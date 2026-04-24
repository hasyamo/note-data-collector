"""
note データ収集スクリプト
creators.txt + testers.txt に登録されたクリエイターのデータを収集・蓄積する
すべて認証不要の公開APIを使用
"""

import os
import csv
import json
import time
import sys
from datetime import datetime, timezone, timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_URL = "https://note.com"

# note-data-collector のルート
COLLECTOR_ROOT = os.path.dirname(os.path.dirname(__file__))
# データは自リポジトリの data/ に保存
DATA_DIR = os.path.join(COLLECTOR_ROOT, "data")

# クリエイターリストのパス
CREATORS_TXT = os.path.join(COLLECTOR_ROOT, "creators.txt")
# テスターリストは環境変数で指定（ツールごとに異なる）
TESTERS_TXT = os.environ.get("TESTERS_TXT", os.path.join(COLLECTOR_ROOT, "fan-board", "testers.txt"))

JST = timezone(timedelta(hours=9))
TODAY = datetime.now(JST).strftime("%Y-%m-%d")

SLEEP_BETWEEN_REQUESTS = 0.75
SLEEP_BETWEEN_ARTICLES = 0.75
LIKES_API_SIZE = 50


# ===== HTTP =====

def fetch_json(url):
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError
    req = Request(url)
    req.add_header("Accept", "application/json, text/plain, */*")
    req.add_header("User-Agent", "Mozilla/5.0")
    req.add_header("Referer", "https://note.com/")
    try:
        with urlopen(req, timeout=30) as res:
            return json.loads(res.read().decode("utf-8"))
    except HTTPError as e:
        print(f"  HTTP error {e.code}: {url}")
        return None
    except URLError as e:
        print(f"  URL error: {e.reason}")
        return None


# ===== Creators =====

def load_txt(filepath):
    """テキストファイルから1行1ユーザー名を読み込む（#でコメント、空行スキップ）
    フォーマット: urlname [YYYY-MM-DD] （日付は省略可、ここでは無視）"""
    names = []
    if not os.path.exists(filepath):
        return names
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line.split()[0])
    return names


def load_creators():
    members = load_txt(CREATORS_TXT)
    testers = load_txt(TESTERS_TXT)
    # 重複を除いて合成（順序維持）
    seen = set()
    creators = []
    for name in members + testers:
        if name not in seen:
            seen.add(name)
            creators.append(name)
    return creators


# ===== Follower =====

def fetch_follower_count(urlname):
    resp = fetch_json(f"{BASE_URL}/api/v2/creators/{urlname}")
    if resp is None:
        return None
    return resp.get("data", {}).get("followerCount")


def save_follower(urlname, count):
    filepath = os.path.join(DATA_DIR, urlname, "followers.csv")
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date", "follower_count"])
        writer.writerow([TODAY, count])


# ===== Articles =====

def fetch_all_articles(urlname):
    articles = []
    page = 1
    while True:
        resp = fetch_json(f"{BASE_URL}/api/v2/creators/{urlname}/contents?kind=note&page={page}&per_page=50")
        if resp is None:
            break
        contents = resp.get("data", {}).get("contents", [])
        if not contents:
            break
        for c in contents:
            articles.append({
                "key": c.get("key", ""),
                "title": c.get("name", ""),
                "published_at": c.get("publishAt", ""),
                "like_count": c.get("likeCount", 0) or 0,
                "comment_count": c.get("commentCount", 0) or 0,
            })
        is_last = resp.get("data", {}).get("isLastPage", True)
        if is_last:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return articles


def save_articles(urlname, articles):
    filepath = os.path.join(DATA_DIR, urlname, "articles.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "key", "title", "published_at", "like_count", "comment_count"])
        for a in articles:
            writer.writerow([TODAY, a["key"], a["title"], a["published_at"], a["like_count"], a["comment_count"]])


# ===== Likes =====

def fetch_all_likes_for_article(note_key):
    all_likes = []
    seen_ids = set()
    page = 1

    while True:
        resp = fetch_json(f"{BASE_URL}/api/v3/notes/{note_key}/likes?page={page}&per={LIKES_API_SIZE}")
        if resp is None:
            break
        data = resp.get("data", {})
        likes = data.get("likes", [])
        if not likes:
            break

        new_in_page = 0
        for like in likes:
            user = like.get("user", {})
            user_id = str(user.get("id", ""))
            if user_id in seen_ids:
                continue
            seen_ids.add(user_id)
            new_in_page += 1
            all_likes.append({
                "note_key": note_key,
                "like_user_id": user_id,
                "like_username": user.get("nickname", ""),
                "like_user_urlname": user.get("urlname", ""),
                "liked_at": like.get("created_at", ""),
                "follower_count": user.get("follower_count", 0),
            })

        if new_in_page == 0:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return all_likes


def load_existing_likes(urlname):
    filepath = os.path.join(DATA_DIR, urlname, "likes.csv")
    if not os.path.exists(filepath):
        return set()
    existing = set()
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if len(row) >= 2:
                existing.add((row[0], row[1]))
    return existing


def append_likes(urlname, new_likes):
    if not new_likes:
        return
    filepath = os.path.join(DATA_DIR, urlname, "likes.csv")
    file_exists = os.path.exists(filepath)
    write_header = not file_exists
    if file_exists:
        with open(filepath, newline="", encoding="utf-8") as f:
            if not f.read().strip():
                write_header = True
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(["note_key", "like_user_id", "like_username", "like_user_urlname", "liked_at", "follower_count"])
        for l in new_likes:
            writer.writerow([l["note_key"], l["like_user_id"], l["like_username"], l["like_user_urlname"], l["liked_at"], l["follower_count"]])


def collect_likes(urlname, articles):
    existing = load_existing_likes(urlname)
    baseline = len(existing) == 0

    if baseline:
        print(f"  Likes: baseline mode ({len(articles)} articles)")
        keys = [a["key"] for a in articles]
    else:
        prev_filepath = os.path.join(DATA_DIR, urlname, "articles_prev.csv")
        prev_likes = {}
        if os.path.exists(prev_filepath):
            with open(prev_filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    prev_likes[row["key"]] = int(row.get("like_count", 0) or 0)

        keys = []
        for a in articles:
            prev = prev_likes.get(a["key"], 0)
            if a["like_count"] > prev:
                keys.append(a["key"])

        if not keys:
            print(f"  Likes: no changes")
            return
        print(f"  Likes: {len(keys)} articles with new likes")

    all_new = []
    for i, key in enumerate(keys, 1):
        likes = fetch_all_likes_for_article(key)
        new = [l for l in likes if (l["note_key"], l["like_user_id"]) not in existing]
        all_new.extend(new)
        for l in new:
            existing.add((l["note_key"], l["like_user_id"]))
        print(f"    {i}/{len(keys)} {key}: {len(likes)} total, {len(new)} new")
        if i < len(keys):
            time.sleep(SLEEP_BETWEEN_ARTICLES)

    append_likes(urlname, all_new)
    print(f"  Likes: {len(all_new)} new likes saved")


def save_articles_prev(urlname, articles):
    """Save current articles as prev for next diff comparison"""
    filepath = os.path.join(DATA_DIR, urlname, "articles_prev.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["key", "like_count", "comment_count"])
        for a in articles:
            writer.writerow([a["key"], a["like_count"], a["comment_count"]])


# ===== Comments =====

COMMENTS_BASELINE_DAYS = 90
COMMENTS_API_PER_PAGE = 100


def fetch_all_comments_for_article(note_key):
    """記事のコメントを全ページ取得（rootのみ。仕様上、返信は別コメントとして同一レスポンスには含まれない）"""
    all_comments = []
    page = 1
    while True:
        url = f"{BASE_URL}/api/v3/notes/{note_key}/note_comments?per_page={COMMENTS_API_PER_PAGE}&page={page}"
        resp = fetch_json(url)
        if resp is None:
            break
        items = resp.get("data", [])
        if not items:
            break
        all_comments.extend(items)
        if not resp.get("next_page"):
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return all_comments


def load_existing_comment_ids(urlname):
    filepath = os.path.join(DATA_DIR, urlname, "comments.csv")
    if not os.path.exists(filepath):
        return set()
    existing = set()
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cid = row.get("comment_id")
            if cid:
                existing.add(cid)
    return existing


def parse_comment_body(body):
    """構造化コメントからプレーンテキストを抽出"""
    if isinstance(body, str):
        return body
    if not body or not isinstance(body, dict):
        return str(body or "")

    def extract_text(node):
        if isinstance(node, str):
            return node
        if not node or not isinstance(node, dict):
            return ""
        if node.get("type") == "text":
            return node.get("value", "")
        if isinstance(node.get("children"), list):
            return "".join(extract_text(c) for c in node["children"])
        return ""

    if isinstance(body.get("children"), list):
        return "\n".join(extract_text(c) for c in body["children"])
    return str(body or "")


def append_comments(urlname, new_comments):
    if not new_comments:
        return
    filepath = os.path.join(DATA_DIR, urlname, "comments.csv")
    file_exists = os.path.exists(filepath)
    write_header = not file_exists
    if file_exists:
        with open(filepath, newline="", encoding="utf-8") as f:
            if not f.read().strip():
                write_header = True
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "comment_id", "note_key", "user_key", "user_name", "user_urlname",
                "user_icon", "commented_at", "body",
            ])
        for c in new_comments:
            writer.writerow([
                c["comment_id"], c["note_key"], c["user_key"], c["user_name"],
                c["user_urlname"], c["user_icon"], c["commented_at"], c["body"],
            ])


def normalize_comment(raw, note_key):
    user = raw.get("user") or {}
    return {
        "comment_id": str(raw.get("key", "")),
        "note_key": note_key,
        "user_key": str(user.get("key", "")),
        "user_name": user.get("nickname") or "",
        "user_urlname": user.get("urlname") or "",
        "user_icon": user.get("profile_image_url") or "",
        "commented_at": raw.get("created_at") or "",
        "body": parse_comment_body(raw.get("comment")),
    }


def collect_comments(urlname, articles):
    """記事のコメントを差分収集"""
    existing_ids = load_existing_comment_ids(urlname)
    baseline = len(existing_ids) == 0

    if baseline:
        # 直近 COMMENTS_BASELINE_DAYS 日以内の記事で comment_count>0 のものだけ
        cutoff = datetime.now(JST) - timedelta(days=COMMENTS_BASELINE_DAYS)

        def is_recent(published_at):
            if not published_at:
                return False
            try:
                s = published_at.strip()
                if "." in s:
                    s = s[: s.index(".")] + s[s.index("+"):]
                dt = datetime.fromisoformat(s)
                return dt.astimezone(JST) >= cutoff
            except (ValueError, IndexError):
                return False

        keys = [a["key"] for a in articles if a["comment_count"] > 0 and is_recent(a["published_at"])]
        print(f"  Comments: baseline mode ({len(keys)} articles in last {COMMENTS_BASELINE_DAYS} days)")
    else:
        # 前回のcomment_countと比較して増えた記事だけ
        prev_filepath = os.path.join(DATA_DIR, urlname, "articles_prev.csv")
        prev_counts = {}
        if os.path.exists(prev_filepath):
            with open(prev_filepath, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    prev_counts[row["key"]] = int(row.get("comment_count", 0) or 0)

        keys = []
        for a in articles:
            prev = prev_counts.get(a["key"], 0)
            if a["comment_count"] > prev:
                keys.append(a["key"])

        if not keys:
            print(f"  Comments: no changes")
            return
        print(f"  Comments: {len(keys)} articles with new comments")

    all_new = []
    for i, key in enumerate(keys, 1):
        raw = fetch_all_comments_for_article(key)
        # クリエイター本人のコメント（返信）は除外
        raw = [c for c in raw if (c.get("user") or {}).get("urlname") != urlname]
        new = []
        for c in raw:
            norm = normalize_comment(c, key)
            if norm["comment_id"] and norm["comment_id"] not in existing_ids:
                new.append(norm)
                existing_ids.add(norm["comment_id"])
        all_new.extend(new)
        print(f"    {i}/{len(keys)} {key}: {len(raw)} fetched, {len(new)} new")
        if i < len(keys):
            time.sleep(SLEEP_BETWEEN_ARTICLES)

    append_comments(urlname, all_new)
    print(f"  Comments: {len(all_new)} new comments saved")


# ===== Magazines =====

def fetch_joined_magazines(urlname):
    """自分が参加している全マガジン（自分作成＋共同運営参加）を取得"""
    magazines = []
    page = 1
    while True:
        url = f"{BASE_URL}/api/v2/creators/{urlname}/contents?kind=magazine&page={page}&per=20&disable_pinned=false&with_notes=false"
        resp = fetch_json(url)
        if resp is None:
            break
        d = resp.get("data", {})
        contents = d.get("contents", [])
        if not contents:
            break
        magazines.extend(contents)
        if d.get("isLastPage"):
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return magazines


def fetch_article_magazine_keys(note_key):
    """記事に紐づくマガジンキー一覧を取得"""
    url = f"{BASE_URL}/api/v3/notes/{note_key}"
    resp = fetch_json(url)
    if resp is None:
        return None
    d = resp.get("data", {})
    return d.get("belonging_magazine_keys", []) or []


def fetch_magazine_detail(mag_key):
    """マガジンの詳細情報を取得"""
    url = f"{BASE_URL}/api/v1/magazines/{mag_key}"
    resp = fetch_json(url)
    if resp is None:
        return None
    d = resp.get("data", resp)
    user = d.get("user", {})
    return {
        "key": d.get("key"),
        "name": d.get("name"),
        "magazine_url": d.get("magazine_url"),
        "cover": d.get("cover"),
        "cover_landscape": d.get("cover_landscape"),
        "is_jointly_managed": d.get("is_jointly_managed"),
        "user": {
            "urlname": user.get("urlname"),
            "nickname": user.get("nickname"),
            "profile_image_path": user.get("user_profile_image_path"),
        },
    }


def load_magazine_memberships(urlname):
    """現在のメンバーシップ状態を読み込む: {(note_key, magazine_key): first_seen_at}"""
    filepath = os.path.join(DATA_DIR, urlname, "magazine_memberships.csv")
    memberships = {}
    if not os.path.exists(filepath):
        return memberships
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            memberships[(row["note_key"], row["magazine_key"])] = row.get("first_seen_at", "")
    return memberships


def save_magazine_memberships(urlname, memberships):
    """メンバーシップ状態を保存"""
    filepath = os.path.join(DATA_DIR, urlname, "magazine_memberships.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["note_key", "magazine_key", "first_seen_at"])
        for (nk, mk), fs in sorted(memberships.items()):
            writer.writerow([nk, mk, fs])


def append_magazine_events(urlname, events):
    """イベント履歴に追記"""
    if not events:
        return
    filepath = os.path.join(DATA_DIR, urlname, "magazine_events.csv")
    file_exists = os.path.exists(filepath)
    with open(filepath, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["detected_at", "event_type", "note_key", "magazine_key"])
        for e in events:
            writer.writerow([e["detected_at"], e["event_type"], e["note_key"], e["magazine_key"]])


def save_joined_magazines(urlname, joined):
    """参加マガジン一覧を保存"""
    filepath = os.path.join(DATA_DIR, urlname, "joined_magazines.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["magazine_key", "owner_urlname", "is_jointly_managed", "name"])
        for m in joined:
            user = m.get("user", {})
            writer.writerow([
                m.get("key"),
                user.get("urlname", ""),
                m.get("isJointlyManaged", False),
                m.get("name", ""),
            ])


def save_magazine_detail(urlname, detail):
    """マガジン詳細情報をJSONで保存"""
    mag_dir = os.path.join(DATA_DIR, urlname, "magazines")
    os.makedirs(mag_dir, exist_ok=True)
    filepath = os.path.join(mag_dir, f"{detail['key']}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)


def collect_magazines(urlname, articles):
    """マガジン情報を収集して差分検出"""
    # 1. 参加マガジンリストを取得・保存
    joined = fetch_joined_magazines(urlname)
    joined_keys = set(m["key"] for m in joined)
    save_joined_magazines(urlname, joined)
    print(f"  Magazines: {len(joined)} joined")
    time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 2. 既存のメンバーシップを読み込む
    memberships_file = os.path.join(DATA_DIR, urlname, "magazine_memberships.csv")
    is_baseline = not os.path.exists(memberships_file)
    prev_memberships = load_magazine_memberships(urlname)
    new_memberships = {}
    events = []
    now_iso = datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S+09:00")

    if is_baseline:
        print(f"  Magazines: baseline mode (using article published_at as detected_at)")

    # 3. 全記事のbelonging_magazine_keysを取得
    for i, a in enumerate(articles, 1):
        note_key = a["key"]
        mag_keys = fetch_article_magazine_keys(note_key)
        if mag_keys is None:
            # 取得失敗時は前回データを維持
            for (nk, mk), fs in prev_memberships.items():
                if nk == note_key:
                    new_memberships[(nk, mk)] = fs
            continue

        # baseline時は記事の公開日時を検出日時として扱う
        event_time = a.get("published_at", now_iso) if is_baseline else now_iso

        for mk in mag_keys:
            prev_seen = prev_memberships.get((note_key, mk))
            if prev_seen:
                # 既存
                new_memberships[(note_key, mk)] = prev_seen
            else:
                # 新規検出
                new_memberships[(note_key, mk)] = event_time
                events.append({
                    "detected_at": event_time,
                    "event_type": "added",
                    "note_key": note_key,
                    "magazine_key": mk,
                })

        # 削除イベント: 前回あって今回ない組み合わせ（baseline時はなし）
        if not is_baseline:
            for (nk, mk) in prev_memberships:
                if nk == note_key and mk not in mag_keys:
                    events.append({
                        "detected_at": now_iso,
                        "event_type": "removed",
                        "note_key": note_key,
                        "magazine_key": mk,
                    })

        if i < len(articles):
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 4. メンバーシップとイベントを保存
    save_magazine_memberships(urlname, new_memberships)
    append_magazine_events(urlname, events)

    added_count = sum(1 for e in events if e["event_type"] == "added")
    removed_count = sum(1 for e in events if e["event_type"] == "removed")
    print(f"  Magazines: {added_count} added, {removed_count} removed")

    # 5. 新規追加されたマガジンの詳細を取得・保存（外部マガジンのみ）
    new_mag_keys = set(e["magazine_key"] for e in events if e["event_type"] == "added")
    new_external = [mk for mk in new_mag_keys if mk not in joined_keys]
    # 既に保存済みのものは除外
    external_to_fetch = []
    for mk in new_external:
        filepath = os.path.join(DATA_DIR, urlname, "magazines", f"{mk}.json")
        if not os.path.exists(filepath):
            external_to_fetch.append(mk)
    if external_to_fetch:
        print(f"  Magazines: fetching {len(external_to_fetch)} new external magazine details")
        for mk in external_to_fetch:
            detail = fetch_magazine_detail(mk)
            if detail:
                save_magazine_detail(urlname, detail)
            time.sleep(SLEEP_BETWEEN_REQUESTS)


# ===== Main =====

def collect_creator(urlname):
    start = time.time()
    print(f"\n--- {urlname} ---")
    user_dir = os.path.join(DATA_DIR, urlname)
    os.makedirs(user_dir, exist_ok=True)

    # 1. Follower count
    fc = fetch_follower_count(urlname)
    if fc is not None:
        save_follower(urlname, fc)
        print(f"  Follower: {fc}")
    else:
        print(f"  Follower: failed")
    time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 2. Articles
    articles = fetch_all_articles(urlname)
    if articles:
        save_articles(urlname, articles)
        print(f"  Articles: {len(articles)}")
    else:
        print(f"  Articles: failed")
        return
    time.sleep(SLEEP_BETWEEN_REQUESTS)

    # 3. Likes
    collect_likes(urlname, articles)

    # 4. Magazines
    collect_magazines(urlname, articles)

    # 5. Comments
    collect_comments(urlname, articles)

    # 6. Save prev for next diff
    save_articles_prev(urlname, articles)

    # 7. Save last updated timestamp
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    with open(os.path.join(user_dir, "last_updated.txt"), "w") as f:
        f.write(now)

    print(f"  Done: {time.time() - start:.1f}s")


MAX_THREADS = 3


def main():
    # 引数: --only urlname1,urlname2,... で特定クリエイターのみ実行
    only_filter = None
    for arg in sys.argv[1:]:
        if arg.startswith("--only="):
            only_filter = set(arg.split("=", 1)[1].split(","))
        elif arg == "--only" and len(sys.argv) > sys.argv.index(arg) + 1:
            idx = sys.argv.index(arg)
            only_filter = set(sys.argv[idx + 1].split(","))

    print(f"=== note data collector ({TODAY}) ===")
    print(f"DATA_DIR: {DATA_DIR}")
    if only_filter:
        print(f"Filter: only {only_filter}")

    creators = load_creators()
    if not creators:
        print("No creators found")
        sys.exit(1)

    if only_filter:
        creators = [c for c in creators if c in only_filter]
        if not creators:
            print(f"No creators matched filter: {only_filter}")
            sys.exit(1)

    print(f"Creators: {len(creators)}, threads: {MAX_THREADS}")

    # ダッシュボードが読み込む creators.csv を生成（--only指定時はスキップ）
    if not only_filter:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(os.path.join(DATA_DIR, "creators.csv"), "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["urlname"])
            for urlname in creators:
                w.writerow([urlname])

    if len(creators) <= MAX_THREADS:
        for urlname in creators:
            try:
                collect_creator(urlname)
            except Exception as e:
                print(f"  Error: {e}")
                import traceback
                traceback.print_exc()
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        groups = [[] for _ in range(MAX_THREADS)]
        for i, urlname in enumerate(creators):
            groups[i % MAX_THREADS].append(urlname)

        def run_group(group_id, urlnames):
            for urlname in urlnames:
                try:
                    collect_creator(urlname)
                except Exception as e:
                    print(f"  Error ({urlname}): {e}")
                    import traceback
                    traceback.print_exc()

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(run_group, i, g) for i, g in enumerate(groups)]
            for f in as_completed(futures):
                f.result()

    print(f"\n=== Done ===")


if __name__ == "__main__":
    main()

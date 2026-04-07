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

SLEEP_BETWEEN_REQUESTS = 1.0
SLEEP_BETWEEN_ARTICLES = 1.5
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
    """テキストファイルから1行1ユーザー名を読み込む（#でコメント、空行スキップ）"""
    names = []
    if not os.path.exists(filepath):
        return names
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                names.append(line)
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
        writer.writerow(["key", "like_count"])
        for a in articles:
            writer.writerow([a["key"], a["like_count"]])


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

    # 4. Save prev for next diff
    save_articles_prev(urlname, articles)

    # 5. Save last updated timestamp
    now = datetime.now(JST).strftime("%Y-%m-%d %H:%M")
    with open(os.path.join(user_dir, "last_updated.txt"), "w") as f:
        f.write(now)

    print(f"  Done: {time.time() - start:.1f}s")


MAX_THREADS = 3


def main():
    print(f"=== note data collector ({TODAY}) ===")
    print(f"DATA_DIR: {DATA_DIR}")

    creators = load_creators()
    if not creators:
        print("No creators found")
        sys.exit(1)

    print(f"Creators: {len(creators)}, threads: {MAX_THREADS}")

    # ダッシュボードが読み込む creators.csv を生成
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

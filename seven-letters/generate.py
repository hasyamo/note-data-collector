"""
seven-letters レター生成スクリプト
note-fan-boardのデータから週次レターJSONを生成する
"""

import os
import csv
import json
import sys
import time
from datetime import datetime, timezone, timedelta, date
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

JST = timezone(timedelta(hours=9))
SLEEP_BETWEEN_REQUESTS = 0.5

# パス設定
COLLECTOR_ROOT = os.path.dirname(os.path.dirname(__file__))
CREATORS_TXT = os.path.join(COLLECTOR_ROOT, "creators.txt")

# 環境変数 or デフォルト
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(COLLECTOR_ROOT, "data"))
SEVEN_LETTERS_DATA = os.environ.get("SEVEN_LETTERS_DATA", os.path.join(COLLECTOR_ROOT, "..", "seven-letters", "data"))


# ===== Utilities =====

# ===== HTTP =====

def fetch_json(url):
    req = Request(url)
    req.add_header("Accept", "application/json, text/plain, */*")
    req.add_header("User-Agent", "Mozilla/5.0")
    req.add_header("Referer", "https://note.com/")
    try:
        with urlopen(req, timeout=30) as res:
            return json.loads(res.read().decode("utf-8"))
    except HTTPError as e:
        print(f"    HTTP error {e.code}: {url}")
        return None
    except URLError as e:
        print(f"    URL error: {e.reason}")
        return None


# ===== Comments =====

def parse_comment_body(body):
    """構造化コメントからプレーンテキストを抽出（ohenji-note/src/utils.js準拠）"""
    if isinstance(body, str):
        return body
    if not body or not isinstance(body, dict):
        return str(body or '')

    def extract_text(node):
        if isinstance(node, str):
            return node
        if not node or not isinstance(node, dict):
            return ''
        if node.get("type") == "text":
            return node.get("value", "")
        if isinstance(node.get("children"), list):
            return ''.join(extract_text(c) for c in node["children"])
        return ''

    if isinstance(body.get("children"), list):
        return '\n'.join(extract_text(c) for c in body["children"])
    if isinstance(body, list):
        return '\n'.join(extract_text(c) for c in body)
    return str(body or '')


def fetch_article_comments(note_key):
    """記事のコメントを全ページ取得（ohenji-note/src/api.js準拠）"""
    comments = []
    page = 1
    while True:
        url = f"https://note.com/api/v3/notes/{note_key}/note_comments?per_page=10&page={page}"
        resp = fetch_json(url)
        if resp is None:
            break
        data = resp.get("data", [])
        if not data:
            break
        comments.extend(data)
        if not resp.get("next_page"):
            break
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return comments


def fetch_week_comments(creator_urlname, week_articles):
    """今週の記事のコメントを取得し、自分のコメントを除外して整形"""
    all_comments = []
    articles_with_comments = [a for a in week_articles if a.get("comment_count", 0) > 0]

    for art in articles_with_comments:
        raw_comments = fetch_article_comments(art["key"])
        for c in raw_comments:
            user = c.get("user", {})
            if user.get("urlname") == creator_urlname:
                continue  # 自分のコメントは除外
            body = parse_comment_body(c.get("comment", ""))
            if len(body) > 80:
                body = body[:80] + "…"
            # 記事タイトルから「｜」以降を除去
            title = art.get("title", "")
            if "｜" in title:
                title = title.split("｜")[0]
            all_comments.append({
                "key": c.get("key", ""),
                "user_name": user.get("nickname", ""),
                "user_icon": user.get("profile_image_url", ""),
                "body": body,
                "article_title": title,
                "note_key": art["key"],
            })
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return all_comments


def load_creators():
    """creators.txtから urlname と joined日付を読み込む
    フォーマット: urlname YYYY-MM-DD"""
    creators = []
    if not os.path.exists(CREATORS_TXT):
        return creators
    with open(CREATORS_TXT, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            urlname = parts[0]
            joined = parts[1] if len(parts) >= 2 else None
            creators.append({"urlname": urlname, "joined": joined})
    return creators


def parse_iso(s):
    """ISO 8601 日時文字列をdatetime(JST)に変換"""
    s = s.strip()
    # 2026-04-07T07:41:25+09:00 or 2026-03-28T13:25:54.000+09:00
    if "." in s:
        s = s[:s.index(".")] + s[s.index("+"):]
    return datetime.fromisoformat(s).astimezone(JST)


def week_start_end(target_date):
    """月曜5:00 JST 〜 日曜28:59 (=翌月曜4:59) の期間を返す"""
    # target_dateを含む週の月曜を求める
    d = target_date
    if isinstance(d, datetime):
        # 5:00境界: 0:00-4:59は前日扱い
        if d.hour < 5:
            d = d - timedelta(days=1)
        d = d.date()

    weekday = d.weekday()  # 0=月
    monday = d - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)

    start = datetime(monday.year, monday.month, monday.day, 5, 0, 0, tzinfo=JST)
    end = datetime(sunday.year, sunday.month, sunday.day, 5, 0, 0, tzinfo=JST) + timedelta(days=1) - timedelta(seconds=1)
    # end = 翌月曜 4:59:59

    return monday, sunday, start, end


def iso_week(d):
    """2026-W14 形式の週番号"""
    return f"{d.isocalendar()[0]}-W{d.isocalendar()[1]:02d}"


# ===== Data Loading =====

def load_articles(creator_dir):
    path = os.path.join(creator_dir, "articles.csv")
    if not os.path.exists(path):
        return []
    articles = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["like_count"] = int(row["like_count"])
            row["comment_count"] = int(row["comment_count"])
            row["published_dt"] = parse_iso(row["published_at"])
            articles.append(row)
    return articles


def load_likes(creator_dir):
    path = os.path.join(creator_dir, "likes.csv")
    if not os.path.exists(path):
        return []
    likes = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["follower_count"] = int(row["follower_count"])
            row["liked_dt"] = parse_iso(row["liked_at"])
            likes.append(row)
    return likes


def load_followers(creator_dir):
    path = os.path.join(creator_dir, "followers.csv")
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["follower_count"] = int(row["follower_count"])
            rows.append(row)
    return rows


# ===== Stats Computation =====

def compute_stats(creator_dir, monday, sunday, start, end, prev_start, prev_end):
    articles = load_articles(creator_dir)
    likes = load_likes(creator_dir)
    followers = load_followers(creator_dir)

    # --- 今週の投稿 ---
    week_articles = [a for a in articles if start <= a["published_dt"] <= end]
    posts_count = len(week_articles)

    # 投稿時間
    post_times = [a["published_dt"].strftime("%H:%M") for a in week_articles]

    # 連続投稿日数
    post_dates = sorted(set(a["published_dt"].date() for a in week_articles))
    consecutive_days = 0
    if post_dates:
        streak = 1
        max_streak = 1
        for i in range(1, len(post_dates)):
            if (post_dates[i] - post_dates[i-1]).days == 1:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 1
        consecutive_days = max_streak

    # --- 今週のスキ ---
    week_likes = [l for l in likes if start <= l["liked_dt"] <= end]
    likes_total = len(week_likes)

    # 先週のスキ
    prev_likes = [l for l in likes if prev_start <= l["liked_dt"] <= prev_end]
    likes_prev = len(prev_likes)

    # 新規ファン (今週スキしてくれた人で、先週以前にスキした記録がない人)
    prev_user_ids = set(l["like_user_id"] for l in likes if l["liked_dt"] < start)
    week_user_ids = set(l["like_user_id"] for l in week_likes)
    new_fan_ids = week_user_ids - prev_user_ids
    new_fans = len(new_fan_ids)

    # コメント合計 (今週の投稿のコメント数合計)
    comments_total = sum(a["comment_count"] for a in week_articles)

    # --- 今週の記事で一番スキされた記事 ---
    top_article = None
    if week_articles:
        best = max(week_articles, key=lambda a: a["like_count"])
        top_article = {
            "title": best["title"],
            "key": best["key"],
            "likes": best["like_count"],
            "comments": best["comment_count"],
        }

    # --- 注目の読者 (フォロワー数上位) ---
    notable_reader = None
    if week_likes:
        top_liker = max(week_likes, key=lambda l: l["follower_count"])
        if top_liker["follower_count"] > 0:
            notable_reader = {
                "name": top_liker["like_username"],
                "urlname": top_liker["like_user_urlname"],
                "follower_count": top_liker["follower_count"],
            }

    # --- フォロワー ---
    monday_str = monday.isoformat()
    sunday_str = sunday.isoformat()

    # 週の開始・終了に最も近いフォロワー数を取得
    followers_start = None
    followers_end = None
    for row in followers:
        d = row["date"]
        if d <= monday_str and (followers_start is None or d >= followers_start_date):
            followers_start = row["follower_count"]
            followers_start_date = d
        if d <= sunday_str:
            followers_end = row["follower_count"]

    if followers_start is None:
        followers_start = followers_end or 0
    if followers_end is None:
        followers_end = followers_start or 0

    # --- 投稿時間の一貫性 (分単位の標準偏差) ---
    time_consistency_minutes = None
    if len(post_times) >= 3:
        minutes = []
        for t in post_times:
            h, m = map(int, t.split(":"))
            minutes.append(h * 60 + m)
        avg = sum(minutes) / len(minutes)
        variance = sum((m - avg) ** 2 for m in minutes) / len(minutes)
        time_consistency_minutes = variance ** 0.5

    # --- 記事特性ポイント用 ---
    # コメント率 (コメント/スキ比)
    comment_ratio = comments_total / max(likes_total, 1)

    # スキのバラつき (記事間の標準偏差 / 平均)
    likes_variance = 0
    if len(week_articles) >= 2:
        like_counts = [a["like_count"] for a in week_articles]
        avg_likes = sum(like_counts) / len(like_counts)
        if avg_likes > 0:
            likes_variance = (sum((x - avg_likes) ** 2 for x in like_counts) / len(like_counts)) ** 0.5 / avg_likes

    # 平均タイトル文字数
    avg_title_len = 0
    if week_articles:
        avg_title_len = sum(len(a["title"]) for a in week_articles) / len(week_articles)

    return {
        "likes_total": likes_total,
        "likes_prev": likes_prev,
        "followers_start": followers_start,
        "followers_end": followers_end,
        "new_fans": new_fans,
        "comments_total": comments_total,
        "posts_count": posts_count,
        "consecutive_days": consecutive_days,
        "post_times": post_times,
        "top_article": top_article,
        "notable_reader": notable_reader,
        "_time_consistency_minutes": time_consistency_minutes,
        "_new_fan_ratio": new_fans / max(likes_total, 1),
        "_comment_ratio": comment_ratio,
        "_likes_variance": likes_variance,
        "_avg_title_len": avg_title_len,
    }


# ===== Sender Selection =====

def select_sender(stats, prev_senders=None):
    """差出人を選出。3層で判定:
    1. ベース条件判定 → マッチしたキャラにポイント付与
    2. 記事特性ポイントで微調整
    3. 直近の差出人は減点
    """
    if prev_senders is None:
        prev_senders = []

    s = stats
    candidates = {}  # sender -> { condition, rare, score }

    # --- 1. ベース条件判定 + レアAND条件 ---
    likes_ratio = (s["likes_total"] - s["likes_prev"]) / s["likes_prev"] if s["likes_prev"] > 0 else 0
    tc = s.get("_time_consistency_minutes")
    follower_diff = s["followers_end"] - s["followers_start"]

    # 月子: 連続投稿（7日）/ レア: 7日連続 かつ スキ先週比+20%以上
    if s["consecutive_days"] >= 7:
        rare = s["consecutive_days"] >= 7 and likes_ratio >= 0.2
        candidates["tsukiko"] = {"condition": "consecutive", "rare": rare, "score": 100}

    # 陽: スキ増 (+20%以上) / レア: +50%以上 かつ 新規ファン5人以上
    if likes_ratio >= 0.2:
        rare = likes_ratio >= 0.5 and s["new_fans"] >= 5
        candidates["you"] = {"condition": "likes_up", "rare": rare, "score": 90}

    # 凛華: 投稿時間一貫 (30分以内) / レア: 5分以内 かつ 7日連続
    if tc is not None and tc <= 30 and s["posts_count"] >= 3:
        rare = tc <= 5 and s["consecutive_days"] >= 7
        candidates["rinka"] = {"condition": "consistent_time", "rare": rare, "score": 80}

    # るな: 新規ファンが多い (3人以上) / レア: 新規ファン率30%以上 かつ フォロワー増
    if s["new_fans"] >= 3:
        rare = s["_new_fan_ratio"] >= 0.3 and follower_diff > 0
        candidates["runa"] = {"condition": "new_fans", "rare": rare, "score": 70}

    # まひる: 投稿頻度が多い (週3本以上) / レア: 週5本以上 かつ コメント率高い
    if s["posts_count"] >= 3:
        rare = s["posts_count"] >= 5 and s.get("_comment_ratio", 0) > 0.3
        candidates["mahiru"] = {"condition": "many_posts", "rare": rare, "score": 60}

    # 日和: フォロワーが伸びた / レア: 切り番達成 かつ 新規ファンもいる
    if follower_diff > 0:
        rare = False
        if s["followers_end"] >= 100:
            if s["followers_end"] // 100 > s["followers_start"] // 100:
                rare = s["new_fans"] >= 1
        candidates["hiyori"] = {"condition": "followers_up", "rare": rare, "score": 50}

    # しずく: 常に候補（最低スコア）
    candidates["shizuku"] = {"condition": "quiet", "rare": False, "score": 10}

    # --- 2. 記事特性ポイント ---
    comment_ratio = s.get("_comment_ratio", 0)
    likes_variance = s.get("_likes_variance", 0)
    avg_title_len = s.get("_avg_title_len", 0)

    # コメント率が高い → まひる（対話）、日和（成長）にボーナス
    if comment_ratio > 0.3:
        for k in ["mahiru", "hiyori"]:
            if k in candidates:
                candidates[k]["score"] += 15

    # スキのバラつきが大きい → 陽（盛り上がり）にボーナス
    if likes_variance > 0.5:
        if "you" in candidates:
            candidates["you"]["score"] += 10

    # タイトルが長い → 凛華（丁寧さ）にボーナス
    if avg_title_len > 30:
        if "rinka" in candidates:
            candidates["rinka"]["score"] += 10

    # タイトルが短い → るな（カジュアル）にボーナス
    if avg_title_len < 20 and avg_title_len > 0:
        if "runa" in candidates:
            candidates["runa"]["score"] += 10

    # --- 3. 直近の差出人は減点 ---
    for i, prev in enumerate(reversed(prev_senders)):
        if prev in candidates:
            # 直近1週前: -50, 2週前: -30, 3週前: -15
            penalty = [50, 30, 15][i] if i < 3 else 5
            candidates[prev]["score"] -= penalty

    # --- 最高スコアのキャラを選出 ---
    best = max(candidates.items(), key=lambda x: x[1]["score"])
    sender = best[0]
    return sender, best[1]["condition"], best[1]["rare"]


# ===== Letter Generation =====

def get_prev_senders(creator, year):
    """既存レターから直近の差出人リストを取得"""
    filepath = os.path.join(SEVEN_LETTERS_DATA, creator, "letters", f"{year}.json")
    if not os.path.exists(filepath):
        return []
    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)
    # 週でソートして差出人リストを返す
    sorted_letters = sorted(data.get("letters", []), key=lambda l: l["week"])
    return [l["sender"] for l in sorted_letters]


def generate_letter(creator, creator_dir, target_date):
    monday, sunday, start, end = week_start_end(target_date)

    # 先週の期間
    prev_monday = monday - timedelta(days=7)
    prev_sunday = sunday - timedelta(days=7)
    prev_start = datetime(prev_monday.year, prev_monday.month, prev_monday.day, 5, 0, 0, tzinfo=JST)
    prev_end = datetime(prev_sunday.year, prev_sunday.month, prev_sunday.day, 5, 0, 0, tzinfo=JST) + timedelta(days=1) - timedelta(seconds=1)

    stats = compute_stats(creator_dir, monday, sunday, start, end, prev_start, prev_end)
    prev_senders = get_prev_senders(creator, monday.year)
    sender, condition, rare = select_sender(stats, prev_senders)

    # 内部フィールドを除去
    clean_stats = {k: v for k, v in stats.items() if not k.startswith("_")}

    # コメント取得
    articles = load_articles(creator_dir)
    week_articles = [a for a in articles if start <= a["published_dt"] <= end]
    comments = fetch_week_comments(creator, week_articles)

    # コメンターのユニーク人数
    commenters = set(c["user_name"] for c in comments)
    clean_stats["commenters_count"] = len(commenters)

    week = iso_week(monday)

    letter = {
        "week": week,
        "period": {
            "start": monday.isoformat(),
            "end": sunday.isoformat(),
        },
        "sender": sender,
        "condition": condition,
        "rare": rare,
        "stats": clean_stats,
    }

    if comments:
        letter["comments"] = comments

    if rare:
        # レア通し番号は既存レターから算出
        letter["rare_no"] = None  # 後で設定

    return letter, monday.year


def save_letter(creator, letter, year):
    out_dir = os.path.join(SEVEN_LETTERS_DATA, creator, "letters")
    os.makedirs(out_dir, exist_ok=True)
    filepath = os.path.join(out_dir, f"{year}.json")

    # 既存データ読み込み
    if os.path.exists(filepath):
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"year": year, "letters": []}

    # 同じ週のレターがあれば上書き
    data["letters"] = [l for l in data["letters"] if l["week"] != letter["week"]]
    data["letters"].append(letter)

    # 週でソート
    data["letters"].sort(key=lambda l: l["week"])

    # レア通し番号を振り直す
    rare_count = 0
    for l in data["letters"]:
        if l.get("rare"):
            rare_count += 1
            l["rare_no"] = rare_count
        elif "rare_no" in l:
            del l["rare_no"]

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return filepath


# ===== Main =====

def main():
    # 引数: 日付指定 (省略時は直近の月曜)
    if len(sys.argv) > 1:
        target = datetime.strptime(sys.argv[1], "%Y-%m-%d").replace(tzinfo=JST)
    else:
        now = datetime.now(JST)
        if now.hour < 5:
            now -= timedelta(days=1)
        # 直近の月曜
        weekday = now.weekday()
        target = now - timedelta(days=weekday)

    creators = load_creators()
    if not creators:
        print("No creators found")
        sys.exit(1)

    # creators.csv を生成
    os.makedirs(SEVEN_LETTERS_DATA, exist_ok=True)
    with open(os.path.join(SEVEN_LETTERS_DATA, "creators.csv"), "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["urlname"])
        for entry in creators:
            w.writerow([entry["urlname"]])

    monday, sunday, start, end = week_start_end(target)
    print(f"=== seven-letters generate ===")
    print(f"Week: {iso_week(monday)} ({monday} ~ {sunday})")
    print(f"Creators: {len(creators)}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"SEVEN_LETTERS_DATA: {SEVEN_LETTERS_DATA}")
    print()

    for entry in creators:
        urlname = entry["urlname"]
        joined = entry["joined"]
        creator_dir = os.path.join(DATA_DIR, urlname)

        if not os.path.exists(creator_dir):
            print(f"  {urlname}: no data dir, skipping")
            continue

        try:
            letter, year = generate_letter(urlname, creator_dir, target)
            filepath = save_letter(urlname, letter, year)
            sender_name = letter["sender"]
            rare_mark = " [RARE]" if letter.get("rare") else ""
            print(f"  {urlname}: {sender_name} ({letter['condition']}){rare_mark} -> {filepath}")
        except Exception as e:
            print(f"  {urlname}: ERROR - {e}")
            import traceback
            traceback.print_exc()

    print()
    print("=== Done ===")


if __name__ == "__main__":
    main()

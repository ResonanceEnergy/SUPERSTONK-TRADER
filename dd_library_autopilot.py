import os
import re
import time
import uuid
import sqlite3
import argparse
import logging
import traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from urllib.parse import urlparse

import praw
from dotenv import load_dotenv

SUBREDDIT = "Superstonk"
DB_PATH = "superstonk_dd.sqlite"

DD_FLAIRS = ["📚 Due Diligence", "📚 Possible DD"]

HUB_QUERIES = [
    '("DD" AND ("library" OR "compilation" OR "directory" OR "index"))',
    '"A Comprehensive Compilation of All Due Diligence"',
    '"Daily Directory" ("DD Library" OR "Library of Due Diligence")',
    '"Due Diligence" "LIBRARY"',
    '"library of DD"',
    '"gme.fyi"',
    '"fliphtml5" "bookcase"',
]

MAX_CRAWL_DEPTH = 3
MAX_QUEUE_BATCH = 50
STOP_DUP_STREAK = 2500

SLEEP_SECONDS = 1.1
HEARTBEAT_EVERY_SECONDS = 30

HUB_COMMENT_SORT = "best"
HUB_TOP_LEVEL_COMMENTS = 75
HUB_REPLY_DEPTH = 1
HUB_MAX_REPLIES_PER_TOP = 20

URL_RE = re.compile(r"(https?://[^\s)>\]]+)", re.IGNORECASE)

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "dd_autopilot.log")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ts_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def extract_urls(text: str):
    if not text:
        return []
    return list({m.group(1).rstrip(".,;!") for m in URL_RE.finditer(text)})


def is_reddit_submission_url(u: str) -> bool:
    try:
        p = urlparse(u)
        host_ok = ("reddit.com" in p.netloc) or ("redd.it" in p.netloc)
        path_ok = ("/comments/" in p.path) or ("redd.it/" in p.path)
        return host_ok and path_ok
    except Exception:
        return False


def normalize_reddit_url(u: str) -> str:
    u = (u or "").strip()
    if u.startswith("http://"):
        u = "https://" + u[len("http://"):]
    if "old.reddit.com" in u:
        u = u.replace("old.reddit.com", "www.reddit.com")
    if "reddit.com" in u and not u.startswith("https://"):
        u = "https://" + u
    return u


def submission_id_from_url(u: str):
    m = re.search(r"/comments/([a-z0-9]{5,8})", u, re.IGNORECASE)
    if m:
        return m.group(1)
    m2 = re.search(r"redd\.it/([a-z0-9]{5,8})", u, re.IGNORECASE)
    if m2:
        return m2.group(1)
    return None


def ensure_dirs():
    os.makedirs(LOG_DIR, exist_ok=True)


def deadline_reached(deadline_ts):
    return deadline_ts is not None and time.time() >= deadline_ts


def setup_logging(verbose=False) -> logging.Logger:
    ensure_dirs()
    logger = logging.getLogger("dd_autopilot")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger

    fmt = logging.Formatter(fmt="%(asctime)sZ | %(levelname)s | %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

    fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=10, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    logger.info("Logger initialized. Log file: %s", LOG_FILE)
    return logger


def init_reddit():
    load_dotenv()
    return praw.Reddit(
        client_id=os.environ["REDDIT_CLIENT_ID"],
        client_secret=os.environ["REDDIT_CLIENT_SECRET"],
        user_agent=os.environ["REDDIT_USER_AGENT"],
    )


def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    return con


def apply_schema(con: sqlite3.Connection):
    con.executescript(
        """
        PRAGMA foreign_keys=ON;
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS posts (
          id TEXT PRIMARY KEY,
          subreddit TEXT NOT NULL,
          created_utc INTEGER NOT NULL,
          created_iso TEXT NOT NULL,
          title TEXT,
          selftext TEXT,
          score INTEGER,
          num_comments INTEGER,
          permalink TEXT,
          url TEXT,
          link_flair_text TEXT,
          author TEXT,
          retrieved_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_posts_created_utc ON posts(created_utc);
        CREATE INDEX IF NOT EXISTS idx_posts_flair ON posts(link_flair_text);

        CREATE TABLE IF NOT EXISTS links (
          post_id TEXT NOT NULL,
          url TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          PRIMARY KEY (post_id, url),
          FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_links_url ON links(url);

        CREATE TABLE IF NOT EXISTS crawl_queue (
          key TEXT PRIMARY KEY,
          url TEXT NOT NULL,
          depth INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'queued',
          last_error TEXT,
          is_hub INTEGER NOT NULL DEFAULT 0,
          max_comment_depth INTEGER NOT NULL DEFAULT 0,
          added_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_queue_status ON crawl_queue(status);

        CREATE TABLE IF NOT EXISTS runs (
          run_id TEXT PRIMARY KEY,
          started_at_utc TEXT NOT NULL,
          ended_at_utc TEXT,
          notes TEXT,
          posts_inserted INTEGER DEFAULT 0,
          hubs_queued INTEGER DEFAULT 0,
          queue_done INTEGER DEFAULT 0,
          errors INTEGER DEFAULT 0
        );
        """
    )
    con.commit()


def insert_post(con, row) -> bool:
    cur = con.execute(
        """
        INSERT OR IGNORE INTO posts
        (id, subreddit, created_utc, created_iso, title, selftext, score, num_comments,
         permalink, url, link_flair_text, author, retrieved_at_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row,
    )
    return cur.rowcount == 1


def upsert_link(con, post_id, url):
    con.execute("INSERT OR IGNORE INTO links(post_id, url, first_seen_utc) VALUES (?, ?, ?)", (post_id, url, now_utc_iso()))


def queue_add(con, key, url, depth, is_hub=0, max_comment_depth=0):
    con.execute(
        """
        INSERT OR IGNORE INTO crawl_queue
        (key, url, depth, status, is_hub, max_comment_depth, added_at_utc, updated_at_utc)
        VALUES (?, ?, ?, 'queued', ?, ?, ?, ?)
        """,
        (key, url, int(depth), int(is_hub), int(max_comment_depth), now_utc_iso(), now_utc_iso()),
    )


def queue_mark(con, key, status, err=None):
    con.execute("UPDATE crawl_queue SET status=?, last_error=?, updated_at_utc=? WHERE key=?", (status, err, now_utc_iso(), key))


def queue_pop_batch(con, batch_size):
    cur = con.execute(
        """
        SELECT key, url, depth, is_hub, max_comment_depth
        FROM crawl_queue
        WHERE status='queued'
        ORDER BY added_at_utc ASC
        LIMIT ?
        """,
        (batch_size,),
    )
    return cur.fetchall()


def queue_reset_hubs(con, logger):
    cur = con.execute("UPDATE crawl_queue SET status='queued', updated_at_utc=? WHERE is_hub=1", (now_utc_iso(),))
    con.commit()
    logger.info("Re-queued hub items: %s", cur.rowcount)


def begin_run(con, notes: str) -> str:
    run_id = str(uuid.uuid4())
    con.execute("INSERT INTO runs(run_id, started_at_utc, notes) VALUES (?, ?, ?)", (run_id, now_utc_iso(), notes))
    con.commit()
    return run_id


def update_run_stats(con, run_id, posts_inserted, hubs_queued, queue_done, errors):
    con.execute(
        """
        UPDATE runs
        SET posts_inserted=?, hubs_queued=?, queue_done=?, errors=?
        WHERE run_id=?
        """,
        (int(posts_inserted), int(hubs_queued), int(queue_done), int(errors), run_id),
    )
    con.commit()


def end_run(con, run_id):
    con.execute("UPDATE runs SET ended_at_utc=? WHERE run_id=?", (now_utc_iso(), run_id))
    con.commit()


def extract_reddit_links_from_best_comments(submission, top_n, reply_depth, max_replies_per_top):
    submission.comment_sort = HUB_COMMENT_SORT
    links = set()
    submission.comments.replace_more(limit=0)
    top_level = list(submission.comments)[:top_n]

    for top in top_level:
        for u in extract_urls(getattr(top, "body", "")):
            if is_reddit_submission_url(u):
                links.add(normalize_reddit_url(u))

        if reply_depth >= 1:
            try:
                top.replies.replace_more(limit=0)
                replies = list(top.replies)[:max_replies_per_top]
                for r in replies:
                    for u in extract_urls(getattr(r, "body", "")):
                        if is_reddit_submission_url(u):
                            links.add(normalize_reddit_url(u))
            except Exception:
                pass

    return sorted(links)


def store_submission_and_discover(con, submission, depth: int):
    _ = submission.title

    post_id = submission.id
    created_utc = int(submission.created_utc)
    permalink = "https://www.reddit.com" + submission.permalink
    author = str(submission.author) if submission.author else None

    row = (
        post_id,
        str(submission.subreddit),
        created_utc,
        ts_to_iso(created_utc),
        submission.title,
        submission.selftext,
        int(submission.score),
        int(submission.num_comments),
        permalink,
        submission.url,
        submission.link_flair_text,
        author,
        now_utc_iso(),
    )

    inserted = insert_post(con, row)

    urls = []
    urls += extract_urls(submission.title or "")
    urls += extract_urls(submission.selftext or "")
    urls += extract_urls(submission.url or "")
    urls = sorted(set(urls))

    for u in urls:
        upsert_link(con, post_id, u)
        if depth < MAX_CRAWL_DEPTH and is_reddit_submission_url(u):
            nu = normalize_reddit_url(u)
            sid = submission_id_from_url(nu)
            key = sid if sid else nu
            queue_add(con, key=key, url=nu, depth=depth + 1)

    return inserted


def ingest_dd_flair_posts(reddit, con, logger, sleep_s, deadline_ts):
    sub = reddit.subreddit(SUBREDDIT)
    inserted_count = 0
    seen = 0
    heartbeat_at = time.time()

    for flair in DD_FLAIRS:
        if deadline_reached(deadline_ts):
            logger.info("Time budget reached during flair ingestion. Stopping safely.")
            break

        query = f'flair:"{flair}"'
        dup_streak = 0
        logger.info("Scanning flair: %s", flair)

        for submission in sub.search(query, sort="new", syntax="lucene", limit=None):
            if deadline_reached(deadline_ts):
                logger.info("Time budget reached mid-flair loop. Stopping safely.")
                return inserted_count

            seen += 1
            inserted = store_submission_and_discover(con, submission, depth=0)
            con.commit()

            if inserted:
                inserted_count += 1
                dup_streak = 0
            else:
                dup_streak += 1

            if time.time() - heartbeat_at >= HEARTBEAT_EVERY_SECONDS:
                logger.info("Progress | phase=flair | flair=%s | seen=%d | inserted=%d | dup_streak=%d", flair, seen, inserted_count, dup_streak)
                heartbeat_at = time.time()

            if dup_streak >= STOP_DUP_STREAK:
                logger.info("Stopping flair scan due to duplicate streak: %d", dup_streak)
                break

            time.sleep(sleep_s)

    return inserted_count


def discover_hub_posts(reddit, con, logger, sleep_s, deadline_ts):
    sub = reddit.subreddit(SUBREDDIT)
    added = 0
    logger.info("Discovering hub posts...")

    for q in HUB_QUERIES:
        if deadline_reached(deadline_ts):
            logger.info("Time budget reached during hub discovery. Stopping safely.")
            break

        logger.info("Hub query: %s", q)
        for submission in sub.search(q, sort="relevance", syntax="lucene", limit=200):
            if deadline_reached(deadline_ts):
                logger.info("Time budget reached mid-hub loop. Stopping safely.")
                return added

            _ = submission.title
            permalink = "https://www.reddit.com" + submission.permalink
            queue_add(con, key=submission.id, url=permalink, depth=0, is_hub=1, max_comment_depth=HUB_REPLY_DEPTH)
            added += 1
            con.commit()
            time.sleep(sleep_s)

    logger.info("Hub discovery done. hubs_queued=%d", added)
    return added


def crawl_queue(reddit, con, logger, sleep_s, deadline_ts):
    done = 0
    errors = 0
    heartbeat_at = time.time()

    while True:
        if deadline_reached(deadline_ts):
            logger.info("Time budget reached during crawl. Stopping safely.")
            break

        batch = queue_pop_batch(con, MAX_QUEUE_BATCH)
        if not batch:
            logger.info("Queue empty. Crawl complete.")
            break

        for key, url, depth, is_hub, max_comment_depth in batch:
            if deadline_reached(deadline_ts):
                logger.info("Time budget reached mid-queue batch. Stopping safely.")
                return done, errors

            try:
                sid = submission_id_from_url(url) or (key if re.fullmatch(r"[a-z0-9]{5,8}", key) else None)
                submission = reddit.submission(id=sid) if sid else reddit.submission(url=url)

                store_submission_and_discover(con, submission, depth=int(depth))

                if int(is_hub) == 1 and int(depth) <= MAX_CRAWL_DEPTH:
                    best_links = extract_reddit_links_from_best_comments(submission, HUB_TOP_LEVEL_COMMENTS, int(max_comment_depth), HUB_MAX_REPLIES_PER_TOP)
                    for nu in best_links:
                        sid2 = submission_id_from_url(nu)
                        qkey = sid2 if sid2 else nu
                        queue_add(con, key=qkey, url=nu, depth=int(depth) + 1)

                queue_mark(con, key, "done")
                done += 1
                con.commit()

            except Exception as e:
                errors += 1
                logger.error("Error processing key=%s url=%s: %s\n%s", key, url, str(e), traceback.format_exc())
                queue_mark(con, key, "error", err=str(e)[:500])
                con.commit()

            if time.time() - heartbeat_at >= HEARTBEAT_EVERY_SECONDS:
                qstats = con.execute("SELECT status, COUNT(*) FROM crawl_queue GROUP BY status").fetchall()
                logger.info("Progress | phase=crawl | done=%d | errors=%d | queue=%s", done, errors, qstats)
                heartbeat_at = time.time()

            time.sleep(sleep_s)

    return done, errors


def main():
    parser = argparse.ArgumentParser(description="Superstonk DD Library Autopilot (SQLite, BEST hub comments)")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--sleep", type=float, default=SLEEP_SECONDS)
    parser.add_argument("--recrawl-hubs", action="store_true")
    parser.add_argument("--max-minutes", type=float, default=None)
    args = parser.parse_args()

    logger = setup_logging(verbose=args.verbose)

    deadline_ts = None
    if args.max_minutes is not None:
        deadline_ts = time.time() + (args.max_minutes * 60.0)
        logger.info("Time budget enabled: max_minutes=%.2f", args.max_minutes)

    logger.info("Starting run. sleep=%.2fs recrawl_hubs=%s", args.sleep, args.recrawl_hubs)

    reddit = init_reddit()
    con = init_db()
    apply_schema(con)

    run_id = begin_run(con, notes="DD Library Autopilot (BEST hub comments, time budget)")
    posts_inserted = 0
    hubs_queued = 0
    queue_done = 0
    errors = 0

    try:
        if args.recrawl_hubs:
            queue_reset_hubs(con, logger)

        posts_inserted = ingest_dd_flair_posts(reddit, con, logger, sleep_s=args.sleep, deadline_ts=deadline_ts)
        update_run_stats(con, run_id, posts_inserted, hubs_queued, queue_done, errors)

        if not deadline_reached(deadline_ts):
            hubs_queued = discover_hub_posts(reddit, con, logger, sleep_s=args.sleep, deadline_ts=deadline_ts)
            update_run_stats(con, run_id, posts_inserted, hubs_queued, queue_done, errors)

        if not deadline_reached(deadline_ts):
            queue_done, crawl_errors = crawl_queue(reddit, con, logger, sleep_s=args.sleep, deadline_ts=deadline_ts)
            errors += crawl_errors
            update_run_stats(con, run_id, posts_inserted, hubs_queued, queue_done, errors)

        logger.info("Run end. posts_inserted=%d hubs_queued=%d queue_done=%d errors=%d", posts_inserted, hubs_queued, queue_done, errors)

        end_run(con, run_id)
        con.close()
        return 0

    except Exception as e:
        logger.critical("Fatal error: %s\n%s", str(e), traceback.format_exc())
        update_run_stats(con, run_id, posts_inserted, hubs_queued, queue_done, errors + 1)
        end_run(con, run_id)
        con.close()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

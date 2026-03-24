import logging
import time
from datetime import datetime, timedelta, timezone

import requests

import config
import db
import metrics

log = logging.getLogger(__name__)


def parse_arctic_shift_post(raw):
    selftext = raw.get("selftext", "")
    removed_cat = raw.get("removed_by_category")
    # no explicit removal flag, gotta infer from body or removed_by_category
    is_removed = 1 if (selftext == "[removed]" or removed_cat) else 0
    return {
        "post_id": raw.get("id", ""),
        "subreddit": raw.get("subreddit", ""),
        "author": raw.get("author"),
        "title": raw.get("title", ""),
        "score": raw.get("score", 0),
        "flair": raw.get("link_flair_text"),
        "created_utc": raw.get("created_utc", 0),
        "domain": raw.get("domain", ""),
        "is_removed": is_removed,
        "removal_reason": removed_cat,
        "fetched_utc": int(datetime.now(timezone.utc).timestamp()),
    }


def fetch_arctic_shift(subreddit, after, before):
    """Paginate Arctic Shift search endpoint"""
    posts = []
    cursor = after

    while True:
        params = {
            "subreddit": subreddit,
            "after": cursor,
            "before": before,
            "limit": config.ARCTIC_SHIFT_LIMIT,
            "sort": "asc",
        }
        try:
            resp = requests.get(config.ARCTIC_SHIFT_BASE, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("data", [])
        except Exception as e:
            log.error("Arctic Shift failed for r/%s: %s", subreddit, e)
            break

        if not data:
            break

        for raw in data:
            posts.append(parse_arctic_shift_post(raw))

        last_utc = data[-1].get("created_utc", 0)
        if last_utc <= cursor or len(data) < config.ARCTIC_SHIFT_LIMIT:
            break
        # +1 or same-second posts at page boundary get skipped
        cursor = last_utc + 1

        time.sleep(config.ARCTIC_SHIFT_SLEEP)

    return posts


def scrape_day(subreddit, date_str, conn):
    """All posts for one sub, one day"""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    day_start = int(dt.timestamp())
    day_end = int((dt + timedelta(days=1)).timestamp())

    posts = fetch_arctic_shift(subreddit, day_start, day_end)
    for p in posts:
        db.upsert_post(p, conn)
    return len(posts)


def get_scraped_dates(conn):
    """Dates already in daily_metrics"""
    return db.get_scraped_dates(conn)


def backfill(days=None, conn=None):
    """Backfill last N days across all tracked subs"""
    days = days or config.BACKFILL_DAYS
    conn = conn or db.get_connection()
    db.init_db(conn)

    existing = get_scraped_dates(conn)
    now = datetime.now(timezone.utc)

    for day_offset in range(days, 0, -1):
        date_str = (now - timedelta(days=day_offset)).strftime("%Y-%m-%d")
        if date_str in existing:
            log.info("Skipping %s (already scraped)", date_str)
            continue

        for sub in config.SUBREDDITS:
            count = scrape_day(sub, date_str, conn)
            log.info("r/%s %s: %d posts", sub, date_str, count)

        metrics.compute_daily_metrics(date_str, conn)
        log.info("Metrics computed for %s", date_str)


def scrape_all(conn=None):
    """Hourly job: scrape today + recompute metrics"""
    conn = conn or db.get_connection()
    db.init_db(conn)

    now = datetime.now(timezone.utc)
    today_str = now.strftime("%Y-%m-%d")

    for sub in config.SUBREDDITS:
        count = scrape_day(sub, today_str, conn)
        log.info("r/%s: %d posts", sub, count)

    metrics.compute_daily_metrics(today_str, conn)
    log.info("Daily metrics computed for %s", today_str)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    conn = db.get_connection()
    db.init_db(conn)
    existing = get_scraped_dates(conn)
    # prob first run
    if len(existing) < 7:
        log.info("First run detected -- backfilling %d days of history...", config.BACKFILL_DAYS)
        backfill()
    else:
        scrape_all()

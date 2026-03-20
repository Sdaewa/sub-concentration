from datetime import datetime, timedelta, timezone

import db


def utc_start_date(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y-%m-%d")


def posts_last_days(conn, subreddit: str, days: int, today: str):
    return db.get_posts(subreddit, utc_start_date(days), today, conn)


def all_posts_last_days(conn, days: int, today: str):
    return db.get_all_posts(utc_start_date(days), today, conn)

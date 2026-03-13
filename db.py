import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd

import config


def get_connection(db_path=None):
    path = db_path or config.DB_PATH
    # streamlit + apscheduler use diff threads
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn=None):
    conn = conn or get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS posts_raw (
            post_id     TEXT PRIMARY KEY,
            subreddit   TEXT NOT NULL,
            author      TEXT,
            title       TEXT,
            score       INTEGER DEFAULT 0,
            flair       TEXT,
            created_utc INTEGER NOT NULL,
            domain      TEXT,
            is_removed  INTEGER DEFAULT 0,
            removal_reason TEXT,
            fetched_utc INTEGER
        );

        CREATE TABLE IF NOT EXISTS daily_metrics (
            date          TEXT NOT NULL,
            subreddit     TEXT NOT NULL,
            total_posts   INTEGER DEFAULT 0,
            top1_pct      REAL DEFAULT 0,
            top3_pct      REAL DEFAULT 0,
            top5_pct      REAL DEFAULT 0,
            burst_count   INTEGER DEFAULT 0,
            removed_pct   REAL DEFAULT 0,
            unique_authors INTEGER DEFAULT 0,
            PRIMARY KEY (date, subreddit)
        );

        CREATE INDEX IF NOT EXISTS idx_posts_sub_date
            ON posts_raw (subreddit, created_utc);
        CREATE INDEX IF NOT EXISTS idx_posts_author
            ON posts_raw (author);
    """)
    conn.commit()
    return conn


def upsert_post(post, conn=None):
    conn = conn or get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO posts_raw
           (post_id, subreddit, author, title, score, flair,
            created_utc, domain, is_removed, removal_reason, fetched_utc)
           VALUES (:post_id, :subreddit, :author, :title, :score, :flair,
                   :created_utc, :domain, :is_removed, :removal_reason, :fetched_utc)
        """,
        post,
    )
    conn.commit()


def upsert_daily_metrics(metrics, conn=None):
    conn = conn or get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO daily_metrics
           (date, subreddit, total_posts, top1_pct, top3_pct, top5_pct,
            burst_count, removed_pct, unique_authors)
           VALUES (:date, :subreddit, :total_posts, :top1_pct, :top3_pct,
                   :top5_pct, :burst_count, :removed_pct, :unique_authors)
        """,
        metrics,
    )
    conn.commit()


def get_posts(subreddit, start_date, end_date, conn=None):
    conn = conn or get_connection()
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    # +86400 so end_date is inclusive
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86400
    return pd.read_sql_query(
        "SELECT * FROM posts_raw WHERE subreddit = ? AND created_utc >= ? AND created_utc < ?",
        conn,
        params=(subreddit, start_ts, end_ts),
    )


def get_daily_metrics(days=30, conn=None):
    conn = conn or get_connection()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    return pd.read_sql_query(
        "SELECT * FROM daily_metrics WHERE date >= ? ORDER BY date",
        conn,
        params=(cutoff,),
    )


def get_all_posts(start_date, end_date, conn=None):
    conn = conn or get_connection()
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86400
    return pd.read_sql_query(
        "SELECT * FROM posts_raw WHERE created_utc >= ? AND created_utc < ?",
        conn,
        params=(start_ts, end_ts),
    )


def export_csv(table, conn=None):
    conn = conn or get_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)  # noqa: S608
    return df.to_csv(index=False)

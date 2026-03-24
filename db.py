import os
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

import config

SQLITE_INIT = """
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
    """

PG_INIT_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS posts_raw (
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
        )""",
    """CREATE TABLE IF NOT EXISTS daily_metrics (
            date          TEXT NOT NULL,
            subreddit     TEXT NOT NULL,
            total_posts   INTEGER DEFAULT 0,
            top1_pct      DOUBLE PRECISION DEFAULT 0,
            top3_pct      DOUBLE PRECISION DEFAULT 0,
            top5_pct      DOUBLE PRECISION DEFAULT 0,
            burst_count   INTEGER DEFAULT 0,
            removed_pct   DOUBLE PRECISION DEFAULT 0,
            unique_authors INTEGER DEFAULT 0,
            PRIMARY KEY (date, subreddit)
        )""",
    "CREATE INDEX IF NOT EXISTS idx_posts_sub_date ON posts_raw (subreddit, created_utc)",
    "CREATE INDEX IF NOT EXISTS idx_posts_author ON posts_raw (author)",
]

SQLITE_UPSERT_POST = """INSERT OR REPLACE INTO posts_raw
           (post_id, subreddit, author, title, score, flair,
            created_utc, domain, is_removed, removal_reason, fetched_utc)
           VALUES (:post_id, :subreddit, :author, :title, :score, :flair,
                   :created_utc, :domain, :is_removed, :removal_reason, :fetched_utc)"""

PG_UPSERT_POST = """INSERT INTO posts_raw
           (post_id, subreddit, author, title, score, flair,
            created_utc, domain, is_removed, removal_reason, fetched_utc)
           VALUES (:post_id, :subreddit, :author, :title, :score, :flair,
                   :created_utc, :domain, :is_removed, :removal_reason, :fetched_utc)
           ON CONFLICT (post_id) DO UPDATE SET
             subreddit = EXCLUDED.subreddit,
             author = EXCLUDED.author,
             title = EXCLUDED.title,
             score = EXCLUDED.score,
             flair = EXCLUDED.flair,
             created_utc = EXCLUDED.created_utc,
             domain = EXCLUDED.domain,
             is_removed = EXCLUDED.is_removed,
             removal_reason = EXCLUDED.removal_reason,
             fetched_utc = EXCLUDED.fetched_utc"""

SQLITE_UPSERT_METRICS = """INSERT OR REPLACE INTO daily_metrics
           (date, subreddit, total_posts, top1_pct, top3_pct, top5_pct,
            burst_count, removed_pct, unique_authors)
           VALUES (:date, :subreddit, :total_posts, :top1_pct, :top3_pct,
                   :top5_pct, :burst_count, :removed_pct, :unique_authors)"""

PG_UPSERT_METRICS = """INSERT INTO daily_metrics
           (date, subreddit, total_posts, top1_pct, top3_pct, top5_pct,
            burst_count, removed_pct, unique_authors)
           VALUES (:date, :subreddit, :total_posts, :top1_pct, :top3_pct,
                   :top5_pct, :burst_count, :removed_pct, :unique_authors)
           ON CONFLICT (date, subreddit) DO UPDATE SET
             total_posts = EXCLUDED.total_posts,
             top1_pct = EXCLUDED.top1_pct,
             top3_pct = EXCLUDED.top3_pct,
             top5_pct = EXCLUDED.top5_pct,
             burst_count = EXCLUDED.burst_count,
             removed_pct = EXCLUDED.removed_pct,
             unique_authors = EXCLUDED.unique_authors"""

_EXPORT_TABLES = frozenset({"posts_raw", "daily_metrics"})

_pg_engine = None


def _database_url():
    u = (os.environ.get("DATABASE_URL") or "").strip()
    return u or None


def _normalize_database_url(url: str) -> str:
    u = url.strip()
    if u.startswith("postgres://"):
        u = "postgresql://" + u[len("postgres://") :]
    if u.startswith("postgresql://"):
        u = "postgresql+psycopg://" + u[len("postgresql://") :]
    return u


def _get_pg_engine():
    global _pg_engine
    if _pg_engine is None:
        from sqlalchemy import create_engine

        raw = _database_url()
        if not raw:
            raise RuntimeError("DATABASE_URL is not set")
        _pg_engine = create_engine(
            _normalize_database_url(raw),
            pool_pre_ping=True,
        )
    return _pg_engine


def _resolved_path(explicit):
    if explicit is not None:
        return explicit
    return (
        os.environ.get("OBSERVATORY_DB_PATH")
        or os.environ.get("DB_PATH_OVERRIDE")
        or config.DB_PATH
    )


def _sqlite_connect(path):
    if path != ":memory:":
        Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def get_connection(db_path=None):
    """SQLite if db_path is set, else Postgres when DATABASE_URL is set, else default SQLite file."""
    if db_path is not None:
        return _sqlite_connect(db_path)
    if _database_url():
        return _get_pg_engine().connect()
    return _sqlite_connect(_resolved_path(None))


def init_db(conn=None):
    conn = conn or get_connection()
    if isinstance(conn, sqlite3.Connection):
        conn.executescript(SQLITE_INIT)
    else:
        for stmt in PG_INIT_STATEMENTS:
            conn.execute(text(stmt))
        conn.commit()
    return conn


def get_scraped_dates(conn):
    """Dates that already have rows in daily_metrics (used by backfill)."""
    if isinstance(conn, sqlite3.Connection):
        cur = conn.execute("SELECT DISTINCT date FROM daily_metrics")
        return {row[0] for row in cur.fetchall()}
    res = conn.execute(text("SELECT DISTINCT date FROM daily_metrics"))
    return {row[0] for row in res}


def upsert_post(post, conn=None):
    conn = conn or get_connection()
    if isinstance(conn, sqlite3.Connection):
        conn.execute(SQLITE_UPSERT_POST, post)
    else:
        conn.execute(text(PG_UPSERT_POST), post)
    conn.commit()


def upsert_daily_metrics(metrics, conn=None):
    conn = conn or get_connection()
    if isinstance(conn, sqlite3.Connection):
        conn.execute(SQLITE_UPSERT_METRICS, metrics)
    else:
        conn.execute(text(PG_UPSERT_METRICS), metrics)
    conn.commit()


def get_posts(subreddit, start_date, end_date, conn=None):
    conn = conn or get_connection()
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86400
    if isinstance(conn, sqlite3.Connection):
        return pd.read_sql_query(
            "SELECT * FROM posts_raw WHERE subreddit = ? AND created_utc >= ? AND created_utc < ?",
            conn,
            params=(subreddit, start_ts, end_ts),
        )
    return pd.read_sql_query(
        text(
            "SELECT * FROM posts_raw WHERE subreddit = :subreddit "
            "AND created_utc >= :start_ts AND created_utc < :end_ts"
        ),
        conn,
        params={"subreddit": subreddit, "start_ts": start_ts, "end_ts": end_ts},
    )


def get_daily_metrics(days=30, conn=None):
    conn = conn or get_connection()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    if isinstance(conn, sqlite3.Connection):
        return pd.read_sql_query(
            "SELECT * FROM daily_metrics WHERE date >= ? ORDER BY date",
            conn,
            params=(cutoff,),
        )
    return pd.read_sql_query(
        text("SELECT * FROM daily_metrics WHERE date >= :cutoff ORDER BY date"),
        conn,
        params={"cutoff": cutoff},
    )


def get_all_posts(start_date, end_date, conn=None):
    conn = conn or get_connection()
    start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) + 86400
    if isinstance(conn, sqlite3.Connection):
        return pd.read_sql_query(
            "SELECT * FROM posts_raw WHERE created_utc >= ? AND created_utc < ?",
            conn,
            params=(start_ts, end_ts),
        )
    return pd.read_sql_query(
        text("SELECT * FROM posts_raw WHERE created_utc >= :start_ts AND created_utc < :end_ts"),
        conn,
        params={"start_ts": start_ts, "end_ts": end_ts},
    )


def export_csv(table, conn=None):
    if table not in _EXPORT_TABLES:
        raise ValueError(f"invalid table: {table!r}")
    conn = conn or get_connection()
    if isinstance(conn, sqlite3.Connection):
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)  # noqa: S608
    else:
        df = pd.read_sql_query(text(f"SELECT * FROM {table}"), conn)  # noqa: S608
    return df.to_csv(index=False)

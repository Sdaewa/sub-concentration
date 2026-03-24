import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

os.environ["DB_PATH_OVERRIDE"] = ":memory:"

import db

YESTERDAY = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


class TestInitDB(unittest.TestCase):
    def setUp(self):
        self.conn = db.get_connection(":memory:")
        db.init_db(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_tables_exist(self):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = [r[0] for r in cur.fetchall()]
        self.assertIn("posts_raw", tables)
        self.assertIn("daily_metrics", tables)


class TestUpsertPost(unittest.TestCase):
    def setUp(self):
        self.conn = db.get_connection(":memory:")
        db.init_db(self.conn)
        self.post = {
            "post_id": "abc123",
            "subreddit": "Conservative",
            "author": "testuser",
            "title": "Test post",
            "score": 42,
            "flair": "News",
            "created_utc": 1710000000,
            "domain": "self.Conservative",
            "is_removed": 0,
            "removal_reason": None,
            "fetched_utc": 1710001000,
        }

    def tearDown(self):
        self.conn.close()

    def test_insert_and_retrieve(self):
        db.upsert_post(self.post, self.conn)
        df = db.get_posts("Conservative", "2024-03-09", "2024-03-10", self.conn)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["author"], "testuser")

    def test_upsert_updates_is_removed(self):
        db.upsert_post(self.post, self.conn)
        self.post["is_removed"] = 1
        db.upsert_post(self.post, self.conn)
        df = db.get_posts("Conservative", "2024-03-09", "2024-03-10", self.conn)
        self.assertEqual(df.iloc[0]["is_removed"], 1)


class TestUpsertDailyMetrics(unittest.TestCase):
    def setUp(self):
        self.conn = db.get_connection(":memory:")
        db.init_db(self.conn)
        self.metrics = {
            "date": YESTERDAY,
            "subreddit": "Conservative",
            "total_posts": 100,
            "top1_pct": 0.15,
            "top3_pct": 0.40,
            "top5_pct": 0.55,
            "burst_count": 3,
            "removed_pct": 0.12,
            "unique_authors": 30,
        }

    def tearDown(self):
        self.conn.close()

    def test_insert_and_retrieve(self):
        db.upsert_daily_metrics(self.metrics, self.conn)
        df = db.get_daily_metrics(days=30, conn=self.conn)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]["top3_pct"], 0.40)

    def test_upsert_replaces(self):
        db.upsert_daily_metrics(self.metrics, self.conn)
        self.metrics["top3_pct"] = 0.50
        db.upsert_daily_metrics(self.metrics, self.conn)
        df = db.get_daily_metrics(days=30, conn=self.conn)
        self.assertEqual(len(df), 1)
        self.assertAlmostEqual(df.iloc[0]["top3_pct"], 0.50)


class TestExportCSV(unittest.TestCase):
    def setUp(self):
        self.conn = db.get_connection(":memory:")
        db.init_db(self.conn)
        db.upsert_daily_metrics(
            {
                "date": "2024-03-09",
                "subreddit": "Conservative",
                "total_posts": 50,
                "top1_pct": 0.10,
                "top3_pct": 0.30,
                "top5_pct": 0.45,
                "burst_count": 1,
                "removed_pct": 0.05,
                "unique_authors": 20,
            },
            self.conn,
        )

    def tearDown(self):
        self.conn.close()

    def test_export_returns_csv_string(self):
        csv_str = db.export_csv("daily_metrics", self.conn)
        self.assertIn("Conservative", csv_str)
        self.assertIn("top3_pct", csv_str)


if __name__ == "__main__":
    unittest.main()

import unittest

import pandas as pd

import metrics


class TestConcentration(unittest.TestCase):
    def test_single_author_is_100pct(self):
        df = pd.DataFrame({"author": ["alice"] * 10})
        self.assertAlmostEqual(metrics.compute_concentration(df, 1), 1.0)

    def test_even_distribution(self):
        df = pd.DataFrame({"author": ["a"] * 5 + ["b"] * 5 + ["c"] * 5 + ["d"] * 5})
        self.assertAlmostEqual(metrics.compute_concentration(df, 1), 0.25)
        self.assertAlmostEqual(metrics.compute_concentration(df, 3), 0.75)

    def test_skewed_distribution(self):
        df = pd.DataFrame({"author": ["heavy"] * 60 + ["light"] * 40})
        self.assertAlmostEqual(metrics.compute_concentration(df, 1), 0.60)

    def test_empty_df_returns_zero(self):
        df = pd.DataFrame({"author": []})
        self.assertAlmostEqual(metrics.compute_concentration(df, 3), 0.0)


class TestBurstDetection(unittest.TestCase):
    def test_no_bursts_slow_posting(self):
        base = 1710000000
        df = pd.DataFrame({
            "author": ["alice"] * 5,
            "created_utc": [base + i * 700 for i in range(5)],
        })
        result = metrics.detect_bursts(df)
        self.assertEqual(result.get("alice", 0), 0)

    def test_burst_detected(self):
        base = 1710000000
        df = pd.DataFrame({
            "author": ["alice"] * 4,
            "created_utc": [base, base + 60, base + 120, base + 300],
        })
        result = metrics.detect_bursts(df)
        self.assertGreaterEqual(result.get("alice", 0), 1)

    def test_multiple_users_independent(self):
        base = 1710000000
        df = pd.DataFrame({
            "author": ["alice"] * 4 + ["bob"] * 2,
            "created_utc": [base, base+60, base+120, base+300, base, base+60],
        })
        result = metrics.detect_bursts(df)
        self.assertGreaterEqual(result.get("alice", 0), 1)
        self.assertEqual(result.get("bob", 0), 0)


class TestRemovalPct(unittest.TestCase):
    def test_no_removals(self):
        df = pd.DataFrame({"is_removed": [0, 0, 0], "removal_reason": [None, None, None]})
        self.assertAlmostEqual(metrics.compute_removal_pct(df), 0.0)

    def test_all_removed(self):
        df = pd.DataFrame({"is_removed": [1, 1, 1],
                           "removal_reason": ["moderator", "moderator", "automod_filtered"]})
        self.assertAlmostEqual(metrics.compute_removal_pct(df), 1.0)

    def test_excludes_self_deletes(self):
        df = pd.DataFrame({"is_removed": [1, 1, 0, 0],
                           "removal_reason": ["moderator", "deleted", None, None]})
        self.assertAlmostEqual(metrics.compute_removal_pct(df), 1 / 3)

    def test_partial(self):
        df = pd.DataFrame({"is_removed": [1, 0, 0, 0],
                           "removal_reason": ["moderator", None, None, None]})
        self.assertAlmostEqual(metrics.compute_removal_pct(df), 0.25)

    def test_empty(self):
        df = pd.DataFrame({"is_removed": []})
        self.assertAlmostEqual(metrics.compute_removal_pct(df), 0.0)

    def test_without_removal_reason_column(self):
        df = pd.DataFrame({"is_removed": [1, 0, 0]})
        self.assertAlmostEqual(metrics.compute_removal_pct(df), 1 / 3)


class TestRemovalBreakdown(unittest.TestCase):
    def test_breakdown(self):
        df = pd.DataFrame({
            "is_removed": [1, 1, 1, 0],
            "removal_reason": ["moderator", "automod_filtered", "moderator", None],
        })
        bd = metrics.removal_breakdown(df)
        self.assertEqual(bd["moderator"], 2)
        self.assertEqual(bd["automod_filtered"], 1)

    def test_empty(self):
        df = pd.DataFrame({"is_removed": [], "removal_reason": []})
        self.assertEqual(metrics.removal_breakdown(df), {})


class TestTopPosters(unittest.TestCase):
    def test_top_posters_ranking(self):
        df = pd.DataFrame({
            "author": ["alice"] * 10 + ["bob"] * 5 + ["carol"] * 2,
            "post_id": [f"p{i}" for i in range(17)],
            "is_removed": [0] * 10 + [1] * 5 + [0, 1],
        })
        result = metrics.top_posters(df, n=2)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["author"], "alice")
        self.assertAlmostEqual(result.iloc[1]["removal_pct"], 100.0)

    def test_empty(self):
        df = pd.DataFrame({"author": [], "post_id": [], "is_removed": []})
        self.assertTrue(metrics.top_posters(df).empty)


class TestCrossSubOverlap(unittest.TestCase):
    def test_finds_overlap(self):
        df = pd.DataFrame({
            "author": ["alice", "alice", "bob"],
            "subreddit": ["A", "B", "A"],
        })
        result = metrics.cross_sub_overlap(df)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["author"], "alice")

    def test_no_overlap(self):
        df = pd.DataFrame({
            "author": ["alice", "bob"],
            "subreddit": ["A", "B"],
        })
        self.assertTrue(metrics.cross_sub_overlap(df).empty)


class TestHourWeekdayMatrix(unittest.TestCase):
    def test_shape(self):
        base = 1710000000
        df = pd.DataFrame({"created_utc": [base + i * 3600 for i in range(168)]})
        result = metrics.hour_weekday_matrix(df)
        self.assertEqual(result.shape[0], 7)
        self.assertGreater(result.shape[1], 0)

    def test_empty(self):
        df = pd.DataFrame({"created_utc": []})
        self.assertTrue(metrics.hour_weekday_matrix(df).empty)


class TestRollingAverage(unittest.TestCase):
    def test_adds_column(self):
        df = pd.DataFrame({
            "date": [f"2024-01-{d:02d}" for d in range(1, 11)],
            "subreddit": ["A"] * 10,
            "top3_pct": [0.1 * i for i in range(10)],
        })
        result, col = metrics.rolling_average(df, "top3_pct", window=3)
        self.assertIn(col, result.columns)
        self.assertTrue(pd.notna(result[col].iloc[-1]))


class TestDomainStats(unittest.TestCase):
    def test_ranking_and_removal(self):
        df = pd.DataFrame({
            "domain": ["cnn.com"] * 5 + ["fox.com"] * 3 + ["bbc.com"] * 2,
            "post_id": [f"p{i}" for i in range(10)],
            "is_removed": [0]*5 + [1]*3 + [0, 1],
        })
        result = metrics.domain_stats(df, n=3)
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]["domain"], "cnn.com")
        self.assertAlmostEqual(result.iloc[1]["removal_pct"], 100.0)

    def test_empty(self):
        df = pd.DataFrame({"domain": [], "post_id": [], "is_removed": []})
        self.assertTrue(metrics.domain_stats(df).empty)


class TestFlairStats(unittest.TestCase):
    def test_ranking_with_no_flair(self):
        df = pd.DataFrame({
            "flair": ["Politics"] * 4 + [None] * 6,
            "post_id": [f"p{i}" for i in range(10)],
            "is_removed": [0]*4 + [1]*3 + [0]*3,
        })
        result = metrics.flair_stats(df, n=5)
        self.assertEqual(result.iloc[0]["flair"], "(no flair)")
        self.assertEqual(result.iloc[0]["posts"], 6)

    def test_empty(self):
        df = pd.DataFrame({"flair": [], "post_id": [], "is_removed": []})
        self.assertTrue(metrics.flair_stats(df).empty)


class TestHourEntropy(unittest.TestCase):
    def test_single_hour_is_zero(self):
        hours = pd.Series([14, 14, 14, 14, 14])
        self.assertAlmostEqual(metrics._hour_entropy(hours), 0.0)

    def test_uniform_is_high(self):
        hours = pd.Series(list(range(24)) * 10)
        self.assertGreater(metrics._hour_entropy(hours), 0.95)

    def test_single_post_is_zero(self):
        self.assertAlmostEqual(metrics._hour_entropy(pd.Series([5])), 0.0)

    def test_two_hours_moderate(self):
        hours = pd.Series([3] * 50 + [15] * 50)
        ent = metrics._hour_entropy(hours)
        self.assertGreater(ent, 0.0)
        self.assertLess(ent, 1.0)


class TestCoordinationPairs(unittest.TestCase):
    def test_finds_coordinated_pair(self):
        base = 1710000000
        authors, times = [], []
        for p in range(5):
            authors += ["alice", "bob"]
            times += [base + p * 3600, base + p * 3600 + 30]
        df = pd.DataFrame({
            "author": authors,
            "created_utc": times,
            "post_id": [f"p{i}" for i in range(10)],
            "domain": ["x.com"] * 10,
        })
        result = metrics.coordination_pairs(df, window_sec=300, min_coincidences=3)
        self.assertEqual(len(result), 1)
        self.assertGreaterEqual(result.iloc[0]["coincidences"], 3)

    def test_no_pairs_when_spread_out(self):
        base = 1710000000
        df = pd.DataFrame({
            "author": ["alice", "bob"] * 3,
            "created_utc": [base + i * 7200 for i in range(6)],
            "post_id": [f"p{i}" for i in range(6)],
            "domain": ["x.com"] * 6,
        })
        result = metrics.coordination_pairs(df, window_sec=300, min_coincidences=3)
        self.assertTrue(result.empty)

    def test_shared_domain_counted(self):
        base = 1710000000
        df = pd.DataFrame({
            "author": ["alice", "bob", "alice", "bob", "alice", "bob"],
            "created_utc": [base, base + 10, base + 3600, base + 3610,
                           base + 7200, base + 7210],
            "post_id": [f"p{i}" for i in range(6)],
            "domain": ["cnn.com", "cnn.com", "fox.com", "fox.com", "cnn.com", "bbc.com"],
        })
        result = metrics.coordination_pairs(df, window_sec=300, min_coincidences=3)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["shared_domains"], 2)

    def test_empty(self):
        df = pd.DataFrame({"author": [], "created_utc": [], "post_id": [], "domain": []})
        self.assertTrue(metrics.coordination_pairs(df).empty)


class TestCampaignScores(unittest.TestCase):
    def _make_df(self, authors_posts):
        """Build a minimal posts_raw-like DataFrame from {author: n_posts}."""
        rows = []
        base = 1710000000
        i = 0
        for author, n in authors_posts.items():
            for j in range(n):
                rows.append({
                    "author": author,
                    "post_id": f"p{i}",
                    "created_utc": base + j * 3600,
                    "is_removed": 0,
                    "removal_reason": None,
                    "domain": "example.com",
                    "subreddit": "TestSub",
                })
                i += 1
        return pd.DataFrame(rows)

    def test_returns_scores_for_eligible_users(self):
        df = self._make_df({"alice": 10, "bob": 6, "carol": 2})
        result = metrics.campaign_scores(df, min_posts=5)
        self.assertEqual(len(result), 2)
        self.assertIn("composite", result.columns)
        self.assertIn("signals_fired", result.columns)

    def test_empty(self):
        df = pd.DataFrame({
            "author": [], "post_id": [], "created_utc": [],
            "is_removed": [], "domain": [], "subreddit": [],
        })
        self.assertTrue(metrics.campaign_scores(df).empty)

    def test_scores_between_0_and_100(self):
        df = self._make_df({"a": 20, "b": 15, "c": 10, "d": 8, "e": 5})
        result = metrics.campaign_scores(df, min_posts=5)
        self.assertTrue((result["composite"] >= 0).all())
        self.assertTrue((result["composite"] <= 100).all())

    def test_multi_sub_signal_uses_df_all(self):
        df_sub = self._make_df({"alice": 10})
        df_all = pd.DataFrame({
            "author": ["alice"] * 15,
            "subreddit": ["A"] * 10 + ["B"] * 5,
            "post_id": [f"p{i}" for i in range(15)],
            "created_utc": [1710000000 + i * 3600 for i in range(15)],
            "is_removed": [0] * 15,
            "removal_reason": [None] * 15,
            "domain": ["x.com"] * 15,
        })
        result = metrics.campaign_scores(df_sub, df_all=df_all, min_posts=5)
        self.assertEqual(result.iloc[0]["multi_sub"], 2)


if __name__ == "__main__":
    unittest.main()

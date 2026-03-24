import unittest
from unittest.mock import patch, MagicMock

import scraper


class TestParseArcticShiftPost(unittest.TestCase):
    def test_normal_post_not_removed(self):
        raw = {
            "id": "t3_abc",
            "subreddit": "Conservative",
            "author": "user1",
            "title": "Hello",
            "score": 10,
            "link_flair_text": "News",
            "created_utc": 1710000000,
            "domain": "self.Conservative",
            "selftext": "Some content",
        }
        parsed = scraper.parse_arctic_shift_post(raw)
        self.assertEqual(parsed["post_id"], "t3_abc")
        self.assertEqual(parsed["is_removed"], 0)

    def test_removed_by_selftext(self):
        raw = {
            "id": "t3_xyz",
            "subreddit": "Conservative",
            "author": "user2",
            "title": "Removed post",
            "score": 5,
            "link_flair_text": None,
            "created_utc": 1710000000,
            "domain": "self.Conservative",
            "selftext": "[removed]",
        }
        parsed = scraper.parse_arctic_shift_post(raw)
        self.assertEqual(parsed["is_removed"], 1)

    def test_removed_by_category(self):
        raw = {
            "id": "t3_def",
            "subreddit": "politics",
            "author": "user3",
            "title": "Another post",
            "score": 1,
            "link_flair_text": "Politics",
            "created_utc": 1710000000,
            "domain": "example.com",
            "selftext": "",
            "removed_by_category": "moderator",
        }
        parsed = scraper.parse_arctic_shift_post(raw)
        self.assertEqual(parsed["is_removed"], 1)
        self.assertEqual(parsed["removal_reason"], "moderator")

    def test_missing_fields_use_defaults(self):
        raw = {"id": "t3_min", "created_utc": 1710000000}
        parsed = scraper.parse_arctic_shift_post(raw)
        self.assertEqual(parsed["post_id"], "t3_min")
        self.assertEqual(parsed["score"], 0)
        self.assertEqual(parsed["is_removed"], 0)


if __name__ == "__main__":
    unittest.main()

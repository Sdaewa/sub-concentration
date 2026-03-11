from pathlib import Path

BASE_DIR = Path(__file__).parent
DB_PATH = str(BASE_DIR / "data" / "observatory.db")

SUBREDDITS = [
    "Conservative",
    "politics",
    "democrats",
    "Libertarian",
    "socialism",
]

ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com/api/posts/search"
ARCTIC_SHIFT_LIMIT = 100
ARCTIC_SHIFT_SLEEP = 2

BACKFILL_DAYS = 180

# 3+ posts in 10min = burst, 5+ bursts/day triggers alert
BURST_WINDOW_MIN = 10
BURST_THRESHOLD = 3
BURST_ALERT = 5

SCRAPE_INTERVAL_HOURS = 1

MIN_POSTS = 20             # anything less and daily stats are garbage
CAMPAIGN_MIN_POSTS = 5     # min posts to show up in campaign scoring
CAMPAIGN_SIGNAL_THRESHOLD = 3
COORD_WINDOW_SEC = 300     # 5 min
COORD_MIN_COINCIDENCES = 3

SUB_COLORS = {
    "Conservative": "#d62728",
    "politics": "#1f77b4",
    "democrats": "#2ca02c",
    "Libertarian": "#ff7f0e",
    "socialism": "#9467bd",
}

REMOVAL_CATEGORIES = {
    "moderator": "Mod removed",
    "automod_filtered": "Automod filtered",
    "reddit": "Reddit spam filter",
    "deleted": "User self-deleted",
    # arctic shift sometimes has no reason, just selftext=[removed]
    None: "Unknown (no category recorded)",
}

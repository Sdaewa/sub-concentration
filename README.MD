# Sub Concentration Observatory

Tracks posting concentration, burst behavior, and moderation patterns across political subreddits. Compares r/Conservative against r/politics, r/democrats, r/Libertarian, and r/socialism.

## What It Measures

| Metric | Definition | Anomaly Threshold |
| ------ | ---------- | ----------------- |
| Top-N % | Posts by top N authors / total posts | Top-3 > 20% |
| Burst Count | 3+ posts by one user in a 10-min window | > 5 bursts/day/user |
| Removal % | Posts removed by mods/automod (self-deletes excluded) | > 30% daily |
| Author Diversity | Unique authors / total posts | < 0.1 = concentrated |

## Setup

### 1. Clone and install

```bash
git clone <repo-url> && cd sub-concentration
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the dashboard

```bash
streamlit run app.py
```

The scheduler auto-scrapes every hour. Use the sidebar buttons for immediate collection or 6-month backfill.

### 3. Run scraper standalone

```bash
python scraper.py
```

### 4. Run tests

```bash
python -m unittest discover tests -v
```

## Data Source

All data comes from [Arctic Shift](https://arctic-shift.photon-reddit.com/), an independent Reddit archive. No Reddit API credentials needed.

- **Endpoint**: `/api/posts/search`, paginated with 2s sleep between requests
- **Removal inference**: `selftext == "[removed]"` or `removed_by_category` field present in the response
- **Self-deletes**: Identified by `removed_by_category == "deleted"` and excluded from removal percentages

## Dashboard Tabs

1. **Who's Posting?** — Concentration trends, rolling averages, z-scores
2. **Top Posters** — Leaderboard + cross-subreddit overlap
3. **Suspicious Speed** — Burst detection heatmap + automation alerts
4. **What Gets Deleted?** — Removal breakdown by category, trend lines
5. **Posting Patterns** — Hour/weekday heatmap, side-by-side comparison
6. **Source Links** — Top domains, removal rates, cross-sub domain overlap
7. **Flair Bias** — Flair distribution, flaired vs unflaired removal rates
8. **Campaign Detector** — 6-signal suspicion scoring + coordination detection
9. **Download Data** — CSV export of raw posts and daily metrics

## Project Structure

```text
config.py         - Subreddit list, thresholds, paths
db.py             - SQLite schema, upsert/query helpers, CSV export
scraper.py        - Arctic Shift historical fetch + backfill
metrics.py        - Concentration, bursts, removal %, campaign scoring
ui/layout.py      - Page config, DB init, scheduler, sidebar, pct, load_metrics
ui/helpers.py     - UTC date range + db.fetch wrappers
ui/tabs/*.py      - One Streamlit tab per module (render(conn, today, cm))
app.py            - Wires tabs only (~45 lines)
tests/            - Unit tests (db, metrics, scraper)
data/             - SQLite DB (gitignored)
```

## Methodology

All metrics are computed daily per subreddit. Concentration uses `groupby('author').size().nlargest(n)`. Bursts use a sliding 10-minute window per author. Z-scores are `(value - 30-day median) / std` per subreddit, flagging statistical outliers across the observation window. Removal percentages exclude user self-deletes so they only reflect moderator and automated actions.

Arctic Shift snapshots posts at creation and later, enabling removal detection without moderator access. This project uses only public, aggregate data.

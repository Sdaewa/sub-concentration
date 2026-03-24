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
git clone https://github.com/Sdaewa/sub-concentration && cd sub-concentration
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

### 5. Local secrets (optional)

Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml` (gitignored). Use **`DATABASE_URL`** for [Neon](https://neon.tech) or any Postgres (connection string from the host, usually with `sslmode=require`). Use **`DB_PATH`** only when you are **not** using `DATABASE_URL`, to override the SQLite file path.

## Deploy on Streamlit Community Cloud

Official flow: [Prep and deploy](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app), [File organization](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/file-organization), [Secrets](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/secrets-management), [Deploy](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy).

Short version:

1. Push this repo to GitHub.
2. Sign in at [share.streamlit.io](https://share.streamlit.io/) and connect the repo.
3. **Create app** → pick the repo, branch, entrypoint **`app.py`** (repo root).
4. **Advanced settings** → Python 3.12 is fine.
5. **Secrets** → set **`DATABASE_URL`** to your Neon (or other Postgres) connection string so data survives restarts. Omit it to fall back to SQLite under `./data/observatory.db` (ephemeral on Community Cloud).
6. Deploy. Logs (right panel) are visible if you have write access to the repo.

**Postgres (Neon):** The app uses SQLAlchemy + `psycopg` when `DATABASE_URL` is set (`ui/layout.py` injects Streamlit secrets into the environment, then `db.py` creates tables on startup). Use Neon’s pooled or direct URI; include `sslmode=require` if your provider expects it.

**SQLite without `DATABASE_URL`:** The container filesystem is **ephemeral**—data can disappear when the app sleeps or redeploys. Use **Backfill** / **Collect Fresh Data** after cold starts, or run the scraper locally against a file DB. Optional secret **`DB_PATH`** overrides the SQLite file path.

**Long backfills:** Community Cloud may time out or sleep during a 6‑month backfill. Safer: run a long backfill locally (with `DATABASE_URL` or a local `.db` file), or backfill in smaller chunks.

## Data Source

All data comes from [Arctic Shift](https://arctic-shift.photon-reddit.com/), an independent Reddit archive. No Reddit API credentials needed.

- **Endpoint**: `/api/posts/search`, paginated with 2s sleep between requests
- **Removal inference**: `selftext == "[removed]"` or `removed_by_category` field present in the response
- **Self-deletes**: Identified by `removed_by_category == "deleted"` and excluded from removal percentages

## Dashboard

Nine tabs: concentration, top posters, burst heatmap, removal breakdown, hour/weekday patterns, domains (incl. cross-sub), flair, campaign heuristics + pair timing, CSV download.

## Project Structure

```text
config.py         - Subreddit list, thresholds, paths
db.py             - SQLite or Postgres (DATABASE_URL), schema, upserts, queries, CSV export
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

## Campaign detector

For each user with enough posts in the selected sub, six raw signals are computed (volume share, bursts per active day, number of tracked subs they use, entropy of posting hours, share of posts on their top domain, gap between sub removal rate and theirs with self-deletes stripped). Each signal is turned into a **percentile rank** among eligible users in that run.

- **Composite** = mean of the six percentiles (0–100 scale on the leaderboard).
- **Signals fired** = count of signals at or above the 75th percentile (`CAMPAIGN_SIGNAL_THRESHOLD` of them triggers the “flagged” row in the UI).

**Coordination pairs** are independent: pairs of authors with many posts falling within `COORD_WINDOW_SEC` of each other. That is timing overlap only, not the same as the composite score.

import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd

import config
import db


def compute_concentration(df, n):
    if df.empty:
        return 0.0
    counts = df.groupby("author").size()
    top_n = counts.nlargest(n).sum()
    return top_n / len(df)


def detect_bursts(df, window_min=None, threshold=None):
    """Return {author: burst_count} for posts in df"""
    window_min = window_min or config.BURST_WINDOW_MIN
    threshold = threshold or config.BURST_THRESHOLD
    window_sec = window_min * 60
    bursts = {}

    if df.empty:
        return bursts

    for author, group in df.groupby("author"):
        times = sorted(group["created_utc"].tolist())
        if len(times) < threshold:
            continue
        author_bursts = 0
        i = 0
        # slide forward, jump past burst once found so it doesnt double-count
        while i < len(times):
            j = i
            while j < len(times) and (times[j] - times[i]) <= window_sec:
                j += 1
            if (j - i) >= threshold:
                author_bursts += 1
                i = j
            else:
                i += 1
        if author_bursts > 0:
            bursts[author] = author_bursts

    return bursts


def compute_removal_pct(df):
    """Removal % excluding user self-deletes."""
    if df.empty:
        return 0.0
    if "removal_reason" not in df.columns:
        return df["is_removed"].mean()
    # skip self-deletes, only care about mod actions
    excluded = df[df["removal_reason"] != "deleted"]
    if excluded.empty:
        return 0.0
    return excluded["is_removed"].mean()


def removal_breakdown(df):
    """Removal counts by category"""
    if df.empty:
        return {}
    removed = df[df["is_removed"] == 1]
    return removed["removal_reason"].value_counts().to_dict()


def daily_removal_breakdown(df, min_posts=None):
    """Per-day removal breakdown for stacked bar chart"""
    min_posts = min_posts or config.MIN_POSTS
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()
    rows = []
    for day, grp in df.groupby("date"):
        total = len(grp)
        if total < min_posts:
            continue
        for reason, count in removal_breakdown(grp).items():
            label = config.REMOVAL_CATEGORIES.get(reason, str(reason) if reason else "Unknown")
            rows.append({"date": day, "category": label,
                         "count": count, "pct": count / total * 100})
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def rolling_average(df_metrics, col, window=7):
    """Rolling avg column per sub, sorted by date"""
    df = df_metrics.sort_values("date")
    new_col = f"{col}_roll{window}"
    df[new_col] = (
        df.groupby("subreddit")[col]
          .transform(lambda s: s.rolling(window, min_periods=3).mean())
    )
    return df, new_col


def top_posters(df, n=10):
    """Top-n authors with count, share, removal rate (excludes self-deletes)"""
    if df.empty:
        return pd.DataFrame()
    excl = df[df["removal_reason"] != "deleted"] if "removal_reason" in df.columns else df
    counts = excl.groupby("author").agg(
        posts=("post_id", "size"),
        removed=("is_removed", "sum"),
    ).sort_values("posts", ascending=False).head(n).reset_index()
    total = len(excl)
    counts["share_pct"] = (counts["posts"] / total * 100).round(1)
    counts["removal_pct"] = (counts["removed"] / counts["posts"] * 100).round(1)
    return counts[["author", "posts", "share_pct", "removal_pct"]]


def cross_sub_overlap(df):
    """Authors active in 2+ tracked subs"""
    if df.empty:
        return pd.DataFrame()
    per_sub = df.groupby(["author", "subreddit"]).size().reset_index(name="posts")
    multi = per_sub.groupby("author").filter(lambda g: g["subreddit"].nunique() >= 2)
    if multi.empty:
        return pd.DataFrame()
    pivot = multi.pivot_table(index="author", columns="subreddit", values="posts", fill_value=0)
    pivot["total"] = pivot.sum(axis=1)
    pivot["subs"] = (pivot.drop(columns="total") > 0).sum(axis=1)
    return pivot.sort_values("total", ascending=False).reset_index()


def hour_weekday_matrix(df):
    """Posts-per-hour-per-weekday matrix"""
    if df.empty:
        return pd.DataFrame()
    ts = pd.to_datetime(df["created_utc"], unit="s", utc=True)
    df2 = pd.DataFrame({"hour": ts.dt.hour, "weekday": ts.dt.day_name()})
    order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    pivot = df2.groupby(["weekday", "hour"]).size().reset_index(name="posts")
    pivot = pivot.pivot_table(index="weekday", columns="hour", values="posts", fill_value=0)
    pivot = pivot.reindex(order)
    return pivot


def domain_stats(df, n=15):
    """Top-n domains by volume + removal rate (excludes self-deletes)"""
    if df.empty or "domain" not in df.columns:
        return pd.DataFrame()
    excl = df[df["removal_reason"] != "deleted"] if "removal_reason" in df.columns else df
    link_posts = excl[excl["domain"].notna() & (excl["domain"] != "")]
    if link_posts.empty:
        return pd.DataFrame()
    stats = link_posts.groupby("domain").agg(
        posts=("post_id", "size"),
        removed=("is_removed", "sum"),
    ).sort_values("posts", ascending=False).head(n).reset_index()
    total_link = len(link_posts)
    stats["share_pct"] = (stats["posts"] / total_link * 100).round(1)
    stats["removal_pct"] = (stats["removed"] / stats["posts"] * 100).round(1)
    return stats


def flair_stats(df, n=15):
    """Top-n flairs by volume + removal rate (excludes self-deletes)"""
    if df.empty or "flair" not in df.columns:
        return pd.DataFrame()
    excl = df[df["removal_reason"] != "deleted"] if "removal_reason" in df.columns else df
    filled = excl.copy()
    filled["flair"] = filled["flair"].fillna("(no flair)")
    stats = filled.groupby("flair").agg(
        posts=("post_id", "size"),
        removed=("is_removed", "sum"),
    ).sort_values("posts", ascending=False).head(n).reset_index()
    total = len(excl)
    stats["share_pct"] = (stats["posts"] / total * 100).round(1)
    stats["removal_pct"] = (stats["removed"] / stats["posts"] * 100).round(1)
    return stats


def _hour_entropy(hours):
    """Normalized Shannon entropy of posting hours.
    0 = single hour (human), 1 = flat across 24h (bot)."""
    if len(hours) <= 1:
        return 0.0
    probs = hours.value_counts(normalize=True).values
    raw = -np.sum(probs * np.log2(probs))
    # 24 possible bins max
    max_ent = np.log2(min(len(hours), 24))
    return float(raw / max_ent) if max_ent > 0 else 0.0


def _percentile_rank(series):
    """Percentile rank 0-100, higher = more unusual"""
    return series.rank(pct=True) * 100


def campaign_scores(df_sub, df_all=None, min_posts=None):
    """Score each author on 6 suspicion signals, return sorted DataFrame."""
    min_posts = min_posts or config.CAMPAIGN_MIN_POSTS
    if df_sub.empty:
        return pd.DataFrame()

    author_counts = df_sub.groupby("author").size()
    eligible = author_counts[author_counts >= min_posts].index
    if len(eligible) == 0:
        return pd.DataFrame()

    df = df_sub[df_sub["author"].isin(eligible)].copy()
    total = len(df_sub)
    ts = pd.to_datetime(df["created_utc"], unit="s", utc=True)
    df["hour"] = ts.dt.hour
    df["date_str"] = ts.dt.strftime("%Y-%m-%d")

    rows = []
    # sub avg removal minus self-deletes
    excl_self = df[df.get("removal_reason", pd.Series()) != "deleted"] if "removal_reason" in df.columns else df
    sub_avg_removal = excl_self["is_removed"].mean() if not excl_self.empty else 0.0

    burst_dict = detect_bursts(df)

    for author in eligible:
        a = df[df["author"] == author]
        post_count = len(a)
        days_active = a["date_str"].nunique() or 1

        volume = post_count / total
        burst_rate = burst_dict.get(author, 0) / days_active
        schedule_score = _hour_entropy(a["hour"])

        domains = a["domain"].dropna()
        domains = domains[domains != ""]
        if len(domains) > 0:
            top_dom_count = domains.value_counts().iloc[0]
            domain_conc = top_dom_count / len(a)
        else:
            domain_conc = 0.0

        a_excl = a[a["removal_reason"] != "deleted"] if "removal_reason" in a.columns else a
        author_removal = a_excl["is_removed"].mean() if not a_excl.empty else 0.0
        # clamped: only care if they get removed LESS than avg
        removal_gap = max(0, sub_avg_removal - author_removal)

        subs_active = 1
        if df_all is not None and not df_all.empty:
            subs_active = df_all[df_all["author"] == author]["subreddit"].nunique()

        rows.append({
            "author": author,
            "posts": post_count,
            "days_active": days_active,
            "volume": round(volume, 4),
            "burst_rate": round(burst_rate, 2),
            "multi_sub": subs_active,
            "schedule_score": round(schedule_score, 4),
            "domain_conc": round(domain_conc, 4),
            "removal_gap": round(removal_gap, 4),
        })

    result = pd.DataFrame(rows)
    if result.empty:
        return result

    signal_cols = ["volume", "burst_rate", "multi_sub",
                   "schedule_score", "domain_conc", "removal_gap"]
    for col in signal_cols:
        result[f"{col}_pct"] = _percentile_rank(result[col])

    pct_cols = [f"{c}_pct" for c in signal_cols]
    result["composite"] = result[pct_cols].mean(axis=1).round(1)
    result["signals_fired"] = (result[pct_cols] >= 75).sum(axis=1)

    return result.sort_values("composite", ascending=False).reset_index(drop=True)


def coordination_pairs(df, window_sec=None, min_coincidences=None):
    """Find user pairs that keep posting within window_sec of each other."""
    window_sec = window_sec or config.COORD_WINDOW_SEC
    min_coincidences = min_coincidences or config.COORD_MIN_COINCIDENCES

    if df.empty or len(df) < 2:
        return pd.DataFrame()

    authors = df["author"].unique()
    if len(authors) < 2:
        return pd.DataFrame()

    sorted_df = df.sort_values("created_utc")
    times = sorted_df["created_utc"].values
    auth = sorted_df["author"].values
    doms = sorted_df["domain"].values if "domain" in sorted_df.columns else [None] * len(sorted_df)

    pair_hits = defaultdict(list)
    pair_domains = defaultdict(lambda: defaultdict(int))

    # TODO: this is O(n*k) with k=window overlap, fine for now but wont scale past ~50k posts
    n = len(times)
    for i in range(n):
        j = i + 1
        while j < n and (times[j] - times[i]) <= window_sec:
            if auth[i] != auth[j]:
                # canonicalize pair order
                key = tuple(sorted([auth[i], auth[j]]))
                pair_hits[key].append(int(times[j] - times[i]))
                d_i, d_j = doms[i], doms[j]
                if d_i and d_j and d_i == d_j and d_i != "":
                    pair_domains[key][d_i] += 1
            j += 1

    rows = []
    for (a, b), gaps in pair_hits.items():
        if len(gaps) < min_coincidences:
            continue
        shared = sum(pair_domains[(a, b)].values())
        rows.append({
            "user_a": a,
            "user_b": b,
            "coincidences": len(gaps),
            "avg_gap_sec": round(sum(gaps) / len(gaps)),
            "shared_domains": shared,
        })

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("coincidences", ascending=False).reset_index(drop=True)


def add_z_scores(df_metrics):
    """Z-scores per metric: how weird each day is vs the baseline."""
    df = df_metrics.copy()
    for col in ["top1_pct", "top3_pct", "top5_pct", "burst_count", "removed_pct"]:
        if col not in df.columns:
            continue
        for sub in config.SUBREDDITS:
            mask = df["subreddit"] == sub
            vals = df.loc[mask, col]
            # median bc mean gets wrecked by outlier days
            med, std = vals.median(), vals.std()
            df.loc[mask, f"{col}_z"] = (vals - med) / std if std > 0 else 0
    return df


def compute_daily_metrics(date_str=None, conn=None):
    """Compute + store daily_metrics for all subs on given date"""
    conn = conn or db.get_connection()
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for sub in config.SUBREDDITS:
        df = db.get_posts(sub, date_str, date_str, conn)
        if df.empty:
            continue

        burst_dict = detect_bursts(df)
        total_bursts = sum(burst_dict.values())

        metrics_row = {
            "date": date_str,
            "subreddit": sub,
            "total_posts": len(df),
            "top1_pct": round(compute_concentration(df, 1), 4),
            "top3_pct": round(compute_concentration(df, 3), 4),
            "top5_pct": round(compute_concentration(df, 5), 4),
            "burst_count": total_bursts,
            "removed_pct": round(compute_removal_pct(df), 4),
            "unique_authors": df["author"].nunique(),
        }
        db.upsert_daily_metrics(metrics_row, conn)

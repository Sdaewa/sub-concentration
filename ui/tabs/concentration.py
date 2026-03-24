import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import metrics
from ui import layout


def render(conn, today: str, cm: dict) -> None:
    st.header("Who's Actually Posting?")
    st.caption(f"Top-N share of daily posts per sub. Days under {config.MIN_POSTS} posts dropped.")
    df_m = layout.load_metrics(conn)
    if df_m.empty:
        st.info("No data yet. Click **Collect Fresh Data** or **Backfill 6 Months**.")
        return

    latest = df_m.sort_values("date", ascending=False).drop_duplicates("subreddit")
    for col, (_, row) in zip(st.columns(len(latest)), latest.iterrows()):
        with col:
            st.metric(f"r/{row['subreddit']}", layout.pct(row["top3_pct"]),
                      help=f"Top-3 share of {int(row['total_posts'])} posts")

    df_roll, roll_col = metrics.rolling_average(df_m, "top3_pct")
    df_roll["top3_pct_100"] = df_roll["top3_pct"] * 100
    df_roll["roll_100"] = df_roll[roll_col] * 100

    fig = go.Figure()
    for sub in config.SUBREDDITS:
        mask = df_roll["subreddit"] == sub
        d = df_roll[mask]
        if d.empty:
            continue
        color = cm.get(sub, "#999")
        fig.add_trace(go.Scatter(
            x=d["date"], y=d["top3_pct_100"], mode="markers",
            marker=dict(color=color, size=4, opacity=0.3),
            name=f"r/{sub} daily", showlegend=False,
        ))
        fig.add_trace(go.Scatter(
            x=d["date"], y=d["roll_100"], mode="lines",
            line=dict(color=color, width=2.5),
            name=f"r/{sub}",
        ))
    fig.update_layout(
        title="What % of posts come from just 3 people? (7-day average)",
        yaxis_title="% of all posts", xaxis_title="",
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, width="stretch")

    df_today = df_m[df_m["date"] == today]
    if not df_today.empty:
        bar = df_today.melt(id_vars=["subreddit", "total_posts"],
                            value_vars=["top1_pct", "top3_pct", "top5_pct"])
        bar["value"] = bar["value"] * 100
        bar["variable"] = bar["variable"].map(
            {"top1_pct": "#1 poster", "top3_pct": "Top 3", "top5_pct": "Top 5"})
        bar["label"] = bar.apply(
            lambda r: f"{r['value']:.0f}% of {int(r['total_posts'])}", axis=1)
        fig2 = px.bar(bar, x="subreddit", y="value", color="variable",
                      barmode="group", text="label",
                      title="Today: how much do the top posters control?",
                      labels={"value": "% of all posts", "variable": "Group"})
        fig2.update_traces(textposition="outside", textfont_size=10)
        st.plotly_chart(fig2, width="stretch")

    st.subheader("Concentration vs removal")
    st.caption("One dot = one sub one day. Up-right cluster = both high.")
    scatter_df = df_m.copy()
    scatter_df["top3_pct"] = scatter_df["top3_pct"] * 100
    scatter_df["removed_pct"] = scatter_df["removed_pct"] * 100
    st.plotly_chart(px.scatter(
        scatter_df, x="top3_pct", y="removed_pct", color="subreddit",
        color_discrete_map=cm, size="total_posts", hover_data=["date"],
        title="Concentration vs. Removal Rate",
        labels={"top3_pct": "Top-3 poster share (%)", "removed_pct": "Removal rate (%)"},
        opacity=0.6,
    ), width="stretch")

    st.subheader("Z-scores vs usual")
    st.caption("Distance from 0 = how weird the day is for that sub.")
    st.dataframe(metrics.add_z_scores(df_m), width="stretch")

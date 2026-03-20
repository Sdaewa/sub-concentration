import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import metrics
from ui import helpers, layout


def render(conn, today: str, cm: dict) -> None:
    st.header("What Gets Deleted?")
    st.markdown(
        "Not all removals are the same. **Mod removed** = human moderator. "
        "**Automod** = automated rule. **Reddit filter** = sitewide spam. "
        "**Self-deleted** = user removed their own post (excluded from removal %)."
    )
    sub_del = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="del_sub")
    days_del = st.slider("Days to look back", 7, 180, 30, key="del_days")
    df_del = helpers.posts_last_days(conn, sub_del, days_del, today)
    if df_del.empty:
        st.info("No data yet.")
        return

    df_del["date"] = pd.to_datetime(df_del["created_utc"], unit="s").dt.strftime("%Y-%m-%d")
    df_bd = metrics.daily_removal_breakdown(df_del)
    if not df_bd.empty:
        totals = df_bd.groupby("category")["count"].sum()
        c1, c2, c3 = st.columns(3)
        c1.metric("Mod removed", f"{totals.get('Mod removed', 0):,}")
        c2.metric("Automod filtered", f"{totals.get('Automod filtered', 0):,}")
        valid = df_del[(df_del["date"].isin(df_bd["date"].unique())) &
                      (df_del["removal_reason"] != "deleted")]
        real = valid[valid["is_removed"] == 1]
        c3.metric("Real removal rate", layout.pct(len(real) / len(valid)) if len(valid) else "N/A")
        st.plotly_chart(px.bar(
            df_bd, x="date", y="pct", color="category",
            title=f"r/{sub_del}: what happens to posts? (% of daily total)",
            labels={"pct": "% of posts", "date": "", "category": "What happened"},
        ), width="stretch")
    else:
        st.info("Not enough data for breakdown.")

    df_m4 = layout.load_metrics(conn)
    df_m4_sub = df_m4[df_m4["subreddit"] == sub_del].copy()
    if len(df_m4_sub) >= 7:
        df_m4_sub, rcol = metrics.rolling_average(df_m4_sub, "removed_pct")
        df_m4_sub["removed_pct_100"] = df_m4_sub["removed_pct"] * 100
        df_m4_sub["roll_100"] = df_m4_sub[rcol] * 100
        fig_r = go.Figure()
        fig_r.add_trace(go.Scatter(
            x=df_m4_sub["date"], y=df_m4_sub["removed_pct_100"],
            mode="markers", marker=dict(size=4, opacity=0.3, color=cm.get(sub_del, "#999")),
            name="Daily", showlegend=True,
        ))
        fig_r.add_trace(go.Scatter(
            x=df_m4_sub["date"], y=df_m4_sub["roll_100"],
            mode="lines", line=dict(width=2.5, color=cm.get(sub_del, "#999")),
            name="7-day avg",
        ))
        fig_r.update_layout(
            title=f"r/{sub_del}: removal rate trend (excluding self-deletes)",
            yaxis_title="% removed", xaxis_title="",
        )
        st.plotly_chart(fig_r, width="stretch")

    st.subheader("Do top posters get protected?")
    st.caption("If top posters get less removed, mods may be clearing the way for them.")
    excl = df_del[df_del["removal_reason"] != "deleted"]
    if not excl.empty:
        top10 = excl.groupby("author").size().nlargest(10).index
        t = excl.loc[excl["author"].isin(top10), "is_removed"].mean() * 100
        o = excl.loc[~excl["author"].isin(top10), "is_removed"].mean() * 100
        st.plotly_chart(px.bar(
            pd.DataFrame({"Group": ["Top 10 posters", "Everyone else"], "% removed": [t, o]}),
            x="Group", y="% removed", text_auto=".1f",
            title="Who gets deleted more? (excluding self-deletes)",
        ), width="stretch")
    st.caption("Source: Arctic Shift independent archive.")

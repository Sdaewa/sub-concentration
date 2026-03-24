import plotly.express as px
import streamlit as st

import config
import metrics
from ui import helpers


def render(conn, today: str, cm: dict) -> None:
    _ = cm
    st.header("Who Are These People?")
    st.caption("Leaderboard for one sub, then who posts in 2+ tracked subs.")
    sub_lb = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="lb_sub")
    days_lb = st.slider("Days to look back", 7, 180, 30, key="lb_days")
    df_lb = helpers.posts_last_days(conn, sub_lb, days_lb, today)
    if df_lb.empty:
        st.info("No post data for this range.")
    else:
        leaders = metrics.top_posters(df_lb, n=15)
        if not leaders.empty:
            st.subheader(f"Top 15 posters in r/{sub_lb} (last {days_lb} days)")
            leaders.columns = ["Author", "Posts", "% of all posts", "% removed"]
            st.dataframe(leaders, hide_index=True, width="stretch")
            fig_lb = px.bar(
                leaders.head(10), x="Author", y="% of all posts",
                color="% removed", color_continuous_scale="RdYlGn_r",
                title="Top 10: post share colored by removal rate (red = more removed)",
            )
            st.plotly_chart(fig_lb, width="stretch")

    st.divider()
    st.subheader("Same accounts, multiple subs")
    days_overlap = st.slider("Days to check", 7, 180, 30, key="overlap_days")
    df_all = helpers.all_posts_last_days(conn, days_overlap, today)
    if df_all.empty:
        st.info("No data.")
    else:
        overlap = metrics.cross_sub_overlap(df_all)
        if overlap.empty:
            st.success("No users found posting in multiple tracked subreddits.")
        else:
            st.metric("Users active in 2+ subs", len(overlap))
            st.dataframe(overlap.head(30), hide_index=True, width="stretch")

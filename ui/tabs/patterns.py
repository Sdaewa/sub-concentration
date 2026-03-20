import plotly.express as px
import streamlit as st

import config
import db
import metrics
from ui import helpers


def render(conn, today: str, cm: dict) -> None:
    _ = cm
    st.header("When Do People Post?")
    st.markdown(
        "Humans follow sleep schedules. Bots don't. "
        "If a subreddit gets a perfectly even spread of posts at 3am and 3pm, "
        "something's off. This shows posting volume by hour (UTC) and day of week."
    )
    sub_hw = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="hw_sub")
    days_hw = st.slider("Days to look back", 7, 180, 30, key="hw_days")
    start_hw = helpers.utc_start_date(days_hw)
    df_hw = db.get_posts(sub_hw, start_hw, today, conn)
    if df_hw.empty:
        st.info("No data yet.")
        return

    matrix = metrics.hour_weekday_matrix(df_hw)
    if not matrix.empty:
        fig_hw = px.imshow(
            matrix, title=f"r/{sub_hw}: when are posts made? (brighter = more posts)",
            labels={"x": "Hour (UTC)", "y": "Day of week", "color": "Posts"},
            aspect="auto", color_continuous_scale="YlOrRd",
        )
        st.plotly_chart(fig_hw, width="stretch")

    st.subheader("Compare: side by side")
    st.caption("Pick a second subreddit to compare posting schedules.")
    sub_hw2 = st.selectbox("Compare with", [s for s in config.SUBREDDITS if s != sub_hw], key="hw_sub2")
    df_hw2 = db.get_posts(sub_hw2, start_hw, today, conn)
    if df_hw2.empty:
        return
    m2 = metrics.hour_weekday_matrix(df_hw2)
    if m2.empty:
        return
    col_a, col_b = st.columns(2)
    with col_a:
        st.plotly_chart(px.imshow(
            matrix, title=f"r/{sub_hw}",
            labels={"x": "Hour", "y": "", "color": "Posts"},
            aspect="auto", color_continuous_scale="YlOrRd",
        ), width="stretch")
    with col_b:
        st.plotly_chart(px.imshow(
            m2, title=f"r/{sub_hw2}",
            labels={"x": "Hour", "y": "", "color": "Posts"},
            aspect="auto", color_continuous_scale="YlOrRd",
        ), width="stretch")

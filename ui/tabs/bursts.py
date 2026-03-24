import pandas as pd
import plotly.express as px
import streamlit as st

import config
import metrics
from ui import helpers


def render(conn, today: str, cm: dict) -> None:
    _ = cm
    st.header("Suspiciously Fast Posting")
    st.caption("3+ posts in 10 min counts as a burst; lots of bursts per day is the red flag.")
    sub_pick = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="burst_sub")
    days_back = st.slider("How many days to look back", 1, 180, 7, key="burst_days")
    df_posts = helpers.posts_last_days(conn, sub_pick, days_back, today)
    if df_posts.empty:
        st.info("No post data for this range.")
        return

    df_posts["date"] = pd.to_datetime(df_posts["created_utc"], unit="s").dt.strftime("%Y-%m-%d")
    burst_rows = []
    for day, grp in df_posts.groupby("date"):
        for author, count in metrics.detect_bursts(grp).items():
            burst_rows.append({"date": day, "author": author, "bursts": count})
    if not burst_rows:
        st.info("No rapid-fire posting detected.")
        return

    df_burst = pd.DataFrame(burst_rows)
    st.metric("Users with suspicious activity", len(df_burst["author"].unique()),
              help=f"Across {len(df_burst['date'].unique())} days")
    top20 = df_burst.groupby("author")["bursts"].sum().nlargest(20).index
    pivot = df_burst[df_burst["author"].isin(top20)].pivot_table(
        index="author", columns="date", values="bursts", fill_value=0)
    st.plotly_chart(px.imshow(
        pivot, title=f"Top repeat offenders in r/{sub_pick} (brighter = more bursts)",
        labels={"x": "Date", "y": "User", "color": "Times caught"}, aspect="auto",
    ), width="stretch")
    alerts = df_burst[df_burst["bursts"] >= config.BURST_ALERT]
    if not alerts.empty:
        st.error(f"These users look automated ({config.BURST_ALERT}+ bursts/day):")
        st.dataframe(alerts, width="stretch")
    else:
        st.success("No one hit the automation alert threshold.")

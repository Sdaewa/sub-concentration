import streamlit as st

import db


def render(conn, today: str, cm: dict) -> None:
    _ = today
    _ = cm
    st.header("Download the Raw Data")
    st.caption("CSV dumps of `daily_metrics` and `posts_raw`.")
    c1, c2 = st.columns(2)
    c1.markdown("**Daily summary** - one row per subreddit per day.")
    c1.download_button("Download daily summary", db.export_csv("daily_metrics", conn),
                       file_name="daily_metrics.csv", mime="text/csv")
    c2.markdown("**Every single post** - full raw data.")
    c2.download_button("Download all posts", db.export_csv("posts_raw", conn),
                       file_name="posts_raw.csv", mime="text/csv")

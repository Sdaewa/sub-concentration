import logging
from datetime import datetime, timezone

import streamlit as st
from apscheduler.schedulers.background import BackgroundScheduler

import config
import db
import scraper

logging.basicConfig(level=logging.INFO)


def configure_page():
    st.set_page_config(page_title="Who Controls the Conversation?", layout="wide")
    st.title("Who Controls the Conversation?")
    st.caption(
        "Comparing how many people actually post in political subreddits. "
        "When a tiny number of users make most of the posts, that's worth paying attention to."
    )
    st.info(
        "**Data source**: Arctic Shift, an independent Reddit archive. "
        "Removal data is a snapshot, not live. "
        "A post might get restored after being flagged removed, or vice versa. "
        "Treat these as estimates, not hard facts.",
        icon="ℹ️",
    )


@st.cache_resource
def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(scraper.scrape_all, "interval", hours=config.SCRAPE_INTERVAL_HOURS)
    sched.start()
    return sched


@st.cache_resource
def setup_db():
    c = db.get_connection()
    db.init_db(c)
    return c


def today_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def pct(v):
    return f"{v * 100:.1f}%"


def load_metrics(conn):
    df = db.get_daily_metrics(days=config.BACKFILL_DAYS, conn=conn)
    # low volume days = garbage stats
    return df[df["total_posts"] >= config.MIN_POSTS] if not df.empty else df


def render_sidebar(conn, scheduler):
    st.sidebar.title("Controls")
    if st.sidebar.button("Collect Fresh Data"):
        with st.spinner("Pulling today's posts..."):
            scraper.scrape_all(conn)
        st.sidebar.success("Done!")
    if st.sidebar.button("Backfill 6 Months"):
        with st.spinner("Pulling 6 months of history..."):
            scraper.backfill(conn=conn)
        st.sidebar.success("Backfill complete!")
    jobs = scheduler.get_jobs()
    if jobs:
        st.sidebar.caption(f"Auto-updates at {jobs[0].next_run_time:%H:%M UTC}")

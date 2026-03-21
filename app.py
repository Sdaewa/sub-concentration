import streamlit as st

import config
from ui import layout
from ui.tabs import (
    bursts,
    campaign,
    concentration,
    domains,
    download_data,
    flair_bias,
    patterns,
    posters,
    removals,
)

layout.configure_page()
conn = layout.setup_db()
scheduler = layout.init_scheduler()
TODAY = layout.today_utc()
CM = config.SUB_COLORS
layout.render_sidebar(conn, scheduler)

tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
    "Who's Posting?", "Top Posters", "Suspicious Speed",
    "What Gets Deleted?", "Posting Patterns",
    "Source Links", "Flair Bias", "Campaign Detector", "Download Data",
])

with tab1:
    concentration.render(conn, TODAY, CM)
with tab2:
    posters.render(conn, TODAY, CM)
with tab3:
    bursts.render(conn, TODAY, CM)
with tab4:
    removals.render(conn, TODAY, CM)
with tab5:
    patterns.render(conn, TODAY, CM)
with tab6:
    domains.render(conn, TODAY, CM)
with tab7:
    flair_bias.render(conn, TODAY, CM)
with tab8:
    campaign.render(conn, TODAY, CM)
with tab9:
    download_data.render(conn, TODAY, CM)

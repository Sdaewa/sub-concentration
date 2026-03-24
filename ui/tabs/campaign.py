import math

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import db
import metrics
from ui import helpers


def render(conn, today: str, cm: dict) -> None:
    _ = cm
    st.header("Campaign Detector")
    st.caption(
        "Heuristic score from 6 signals (percentiles vs other users in the window). "
        "Not proof of anything."
    )
    with st.expander("Signals (short version)"):
        st.markdown(
            "- Volume, burst rate, multi-sub count, hour entropy, domain concentration, removal gap\n"
            "- **Composite** = average of those percentiles. **Signals fired** = how many are >= p75\n"
            "- **Pairs** below = often post within 5 min of each other (separate from the score)\n\n"
            "Full write-up: **README** (Campaign detector section)."
        )

    sub_cd = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="cd_sub")
    days_cd = st.slider("Days to analyze", 7, 180, 30, key="cd_days")
    start_cd = helpers.utc_start_date(days_cd)
    df_cd = db.get_posts(sub_cd, start_cd, today, conn)
    df_cd_all = db.get_all_posts(start_cd, today, conn)

    if df_cd.empty:
        st.info("No data yet.")
        return

    scores = metrics.campaign_scores(df_cd, df_cd_all,
                                     min_posts=config.CAMPAIGN_MIN_POSTS)
    if scores.empty:
        st.info(f"No users with {config.CAMPAIGN_MIN_POSTS}+ posts in this period.")
    else:
        flagged = scores[scores["signals_fired"] >= config.CAMPAIGN_SIGNAL_THRESHOLD]
        c1, c2, c3 = st.columns(3)
        c1.metric("Users analyzed", len(scores))
        c2.metric("Flagged (3+ signals)", len(flagged),
                  help="Users scoring above 75th percentile on 3+ independent signals")
        c3.metric("Highest composite", f"{scores.iloc[0]['composite']:.0f}/100")

        if not flagged.empty:
            st.error(f"{len(flagged)} users triggered 3+ suspicion signals:")
            display_flagged = flagged[["author", "posts", "days_active", "composite",
                                       "signals_fired"]].copy()
            display_flagged.columns = ["Author", "Posts", "Days Active",
                                       "Suspicion Score", "Signals Fired"]
            st.dataframe(display_flagged, hide_index=True, width="stretch")

        st.subheader("Full Leaderboard")
        st.caption("All users with 5+ posts, ranked by composite suspicion score.")
        display_cols = ["author", "posts", "days_active", "composite", "signals_fired",
                       "volume", "burst_rate", "multi_sub", "schedule_score",
                       "domain_conc", "removal_gap"]
        display = scores[display_cols].head(50).copy()
        display.columns = ["Author", "Posts", "Days", "Score", "Signals",
                          "Volume", "Bursts/day", "Subs", "Entropy",
                          "Domain%", "Removal gap"]
        st.dataframe(display, hide_index=True, width="stretch")

        st.subheader("Score Distribution")
        st.plotly_chart(px.histogram(
            scores, x="composite", nbins=20, color_discrete_sequence=["#d62728"],
            title=f"r/{sub_cd}: suspicion score distribution",
            labels={"composite": "Composite Score (0-100)", "count": "Users"},
        ), width="stretch")

        st.subheader("Signal Radar: Top 5 Most Suspicious")
        top5 = scores.head(5)
        signal_labels = ["Volume", "Bursts", "Multi-sub",
                         "Entropy", "Domain", "Immunity"]
        pct_cols = ["volume_pct", "burst_rate_pct", "multi_sub_pct",
                    "schedule_score_pct", "domain_conc_pct", "removal_gap_pct"]
        fig_radar = go.Figure()
        for _, row in top5.iterrows():
            vals = [row[c] for c in pct_cols] + [row[pct_cols[0]]]
            fig_radar.add_trace(go.Scatterpolar(
                r=vals, theta=signal_labels + [signal_labels[0]],
                fill="toself", name=row["author"], opacity=0.6,
            ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            title="Signal profile of top 5 flagged users",
        )
        st.plotly_chart(fig_radar, width="stretch")

    st.divider()
    st.subheader("Coordinated pairs")
    st.caption("Same-window posts across different authors (see config for window).")
    coord = metrics.coordination_pairs(df_cd)
    if coord.empty:
        st.success("No suspicious timing pairs found.")
        return

    st.metric("Pairs with 3+ coincidences", len(coord))
    display_coord = coord.head(20).rename(columns={
        "user_a": "User A", "user_b": "User B",
        "coincidences": "Times within 5 min",
        "avg_gap_sec": "Avg gap (sec)", "shared_domains": "Shared domains",
    })
    st.dataframe(display_coord, hide_index=True, width="stretch")

    if len(coord) < 2:
        return
    nodes = set(coord["user_a"].tolist() + coord["user_b"].tolist())
    node_list = list(nodes)
    node_idx = {n: i for i, n in enumerate(node_list)}
    angles = [2 * 3.14159 * i / len(node_list) for i in range(len(node_list))]
    nx = [math.cos(a) for a in angles]
    ny = [math.sin(a) for a in angles]

    fig_net = go.Figure()
    max_c = coord["coincidences"].max()
    for _, r in coord.head(30).iterrows():
        ia, ib = node_idx[r["user_a"]], node_idx[r["user_b"]]
        width = max(1, r["coincidences"] / max_c * 6)
        fig_net.add_trace(go.Scatter(
            x=[nx[ia], nx[ib]], y=[ny[ia], ny[ib]], mode="lines",
            line=dict(width=width, color="rgba(200,50,50,0.4)"),
            hoverinfo="text",
            text=f"{r['user_a']} <-> {r['user_b']}: {r['coincidences']}x",
            showlegend=False,
        ))
    fig_net.add_trace(go.Scatter(
        x=nx, y=ny, mode="markers+text", text=node_list,
        textposition="top center", textfont_size=9,
        marker=dict(size=12, color="#d62728"),
        showlegend=False,
    ))
    fig_net.update_layout(
        title="Coordination network (thicker line = more coincidences)",
        xaxis=dict(visible=False), yaxis=dict(visible=False, scaleanchor="x"),
        height=500,
    )
    st.plotly_chart(fig_net, width="stretch")

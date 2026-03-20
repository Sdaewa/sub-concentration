import plotly.express as px
import streamlit as st

import config
import db
import metrics
from ui import helpers


def render(conn, today: str, cm: dict) -> None:
    _ = cm
    st.header("Where Do the Links Go?")
    st.markdown(
        "Every link post points to a website. If a subreddit's content comes from "
        "the same 3 sites, that's a sign of narrow sourcing or astroturfing. "
        "Also shows which domains get nuked the most."
    )
    sub_dom = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="dom_sub")
    days_dom = st.slider("Days to look back", 7, 180, 30, key="dom_days")
    start_dom = helpers.utc_start_date(days_dom)
    df_dom = db.get_posts(sub_dom, start_dom, today, conn)
    if df_dom.empty:
        st.info("No data yet.")
    else:
        dom = metrics.domain_stats(df_dom, n=15)
        if not dom.empty:
            c1, c2 = st.columns(2)
            c1.metric("Unique domains", df_dom["domain"].nunique())
            top3_share = dom.head(3)["share_pct"].sum()
            c2.metric("Top 3 domains share", f"{top3_share:.1f}%")

            st.subheader(f"Top 15 link sources in r/{sub_dom}")
            display_dom = dom.rename(columns={
                "domain": "Domain", "posts": "Posts",
                "share_pct": "% of link posts", "removal_pct": "% removed",
            })
            st.dataframe(display_dom[["Domain", "Posts", "% of link posts", "% removed"]],
                         hide_index=True, width="stretch")

            fig_dom = px.bar(
                dom.head(10), x="domain", y="share_pct",
                color="removal_pct", color_continuous_scale="RdYlGn_r",
                title="Top 10 domains: share of link posts (red = more removed)",
                labels={"domain": "", "share_pct": "% of link posts", "removal_pct": "% removed"},
            )
            st.plotly_chart(fig_dom, width="stretch")
        else:
            st.info("No link posts found.")

    st.divider()
    st.subheader("Compare: which domains overlap across subreddits?")
    st.caption("Domains that appear in multiple subs may indicate cross-posted campaigns.")
    df_dom_all = db.get_all_posts(start_dom, today, conn)
    if df_dom_all.empty:
        return
    dom_cross = df_dom_all[df_dom_all["domain"].notna() & (df_dom_all["domain"] != "")]
    dom_subs = dom_cross.groupby("domain")["subreddit"].nunique()
    shared = dom_subs[dom_subs >= 2].index
    if len(shared) == 0:
        st.info("No domains shared across tracked subreddits in this period.")
        return
    shared_df = dom_cross[dom_cross["domain"].isin(shared)]
    pivot_dom = shared_df.groupby(["domain", "subreddit"]).size().reset_index(name="posts")
    pivot_dom = pivot_dom.pivot_table(index="domain", columns="subreddit",
                                      values="posts", fill_value=0)
    pivot_dom["total"] = pivot_dom.sum(axis=1)
    pivot_dom = pivot_dom.sort_values("total", ascending=False).head(20).reset_index()
    st.dataframe(pivot_dom, hide_index=True, width="stretch")

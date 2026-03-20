import pandas as pd
import plotly.express as px
import streamlit as st

import config
import metrics
from ui import helpers


def render(conn, today: str, cm: dict) -> None:
    _ = cm
    st.header("Who Gets to Post?")
    st.markdown(
        "Some subreddits use flair to gatekeep. In r/Conservative, many threads are "
        "'Flaired Users Only' -- only pre-approved users can participate. "
        "This tab shows which flairs dominate and whether unfaired posts get removed more."
    )
    sub_fl = st.selectbox("Pick a subreddit", config.SUBREDDITS, key="fl_sub")
    days_fl = st.slider("Days to look back", 7, 180, 30, key="fl_days")
    df_fl = helpers.posts_last_days(conn, sub_fl, days_fl, today)
    if df_fl.empty:
        st.info("No data yet.")
        return

    fl = metrics.flair_stats(df_fl, n=15)
    if fl.empty:
        st.info("No flair data available.")
        return

    total_fl = len(df_fl)
    no_flair_count = df_fl["flair"].isna().sum() + (df_fl["flair"] == "").sum()
    flaired_pct = (1 - no_flair_count / total_fl) * 100 if total_fl else 0
    c1, c2 = st.columns(2)
    c1.metric("Posts with flair", f"{flaired_pct:.0f}%")
    no_flair_rem = fl.loc[fl["flair"] == "(no flair)", "removal_pct"]
    flaired_rem = fl.loc[fl["flair"] != "(no flair)", "removal_pct"].mean()
    c2.metric("Unflaired removal rate",
              f"{no_flair_rem.values[0]:.1f}%" if len(no_flair_rem) else "N/A",
              delta=f"vs {flaired_rem:.1f}% for flaired" if pd.notna(flaired_rem) else None,
              delta_color="inverse")

    st.subheader(f"Flair breakdown in r/{sub_fl}")
    display_fl = fl.rename(columns={
        "flair": "Flair", "posts": "Posts",
        "share_pct": "% of all posts", "removal_pct": "% removed",
    })
    st.dataframe(display_fl[["Flair", "Posts", "% of all posts", "% removed"]],
                 hide_index=True, width="stretch")

    fig_fl = px.bar(
        fl[fl["flair"] != "(no flair)"].head(10),
        x="flair", y="share_pct",
        color="removal_pct", color_continuous_scale="RdYlGn_r",
        title="Top 10 flairs: share of posts (red = more removed)",
        labels={"flair": "", "share_pct": "% of posts", "removal_pct": "% removed"},
    )
    st.plotly_chart(fig_fl, width="stretch")

    st.subheader("Flair vs. no flair: removal comparison")
    df_fl_copy = df_fl.copy()
    df_fl_copy["has_flair"] = df_fl_copy["flair"].notna() & (df_fl_copy["flair"] != "")
    excl_fl = df_fl_copy[df_fl_copy["removal_reason"] != "deleted"]
    if excl_fl.empty:
        return
    flair_grp = excl_fl.groupby("has_flair")["is_removed"].mean() * 100
    bar_fl = pd.DataFrame({
        "Group": ["Has flair", "No flair"],
        "% removed": [flair_grp.get(True, 0), flair_grp.get(False, 0)],
    })
    st.plotly_chart(px.bar(
        bar_fl, x="Group", y="% removed", text_auto=".1f",
        title="Do flaired posts survive better? (excluding self-deletes)",
    ), width="stretch")

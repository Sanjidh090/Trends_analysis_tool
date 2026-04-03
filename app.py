# app.py  — Streamlit Dashboard (flat import version)
# Run with:  streamlit run app.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import yaml
import streamlit as st
import pandas as pd
import plotly.express as px

from db import TrendsDB
from signal_processor import classify_all, find_correlated_pairs
from targeting_engine import full_platform_brief
from trends_collector import TrendsCollector

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

st.set_page_config(page_title="Trends Intel", page_icon="📊", layout="wide")

@st.cache_resource
def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

@st.cache_resource
def get_db():
    cfg = load_config()
    return TrendsDB(cfg["storage"]["db_path"])

cfg = load_config()
db  = get_db()

st.sidebar.title("📊 Trends Intel")
page = st.sidebar.radio("Navigate", [
    "🏠 Overview", "📈 Trends", "🗺 Geo Map",
    "🎯 Ads Brief", "🔍 Keyword Tool", "⚙️ Settings"
])

all_geos      = [g["code"] for g in cfg["geos"]]
seed_keywords = cfg["collection"]["keywords_seed"]
selected_geo  = st.sidebar.selectbox("Geo Filter", ["ALL"] + all_geos)
selected_kws  = st.sidebar.multiselect("Keywords", seed_keywords, default=seed_keywords[:4])


# ── Overview ──────────────────────────────────────────────────────────────────
if page == "🏠 Overview":
    st.title("🏠 Trends Intelligence Overview")
    breakouts = db.get_breakout_log(days=7)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Breakouts This Week",  len(breakouts))
    c2.metric("Geos Monitored",       len(cfg["geos"]))
    c3.metric("Keywords Tracked",     len(seed_keywords))
    c4.metric("Ad Platforms",         4)
    st.markdown("---")
    if not breakouts.empty:
        st.subheader("🚨 Recent Breakouts")
        st.dataframe(breakouts, use_container_width=True)
    else:
        st.info("No breakouts in the last 7 days. Run: `python main.py --mode collect`")

# ── Trends ────────────────────────────────────────────────────────────────────
elif page == "📈 Trends":
    st.title("📈 Keyword Trends Over Time")
    geo_filter = selected_geo if selected_geo != "ALL" else all_geos[0]
    if not selected_kws:
        st.warning("Select at least one keyword in the sidebar.")
    else:
        dfs = {kw: db.get_interest_history(kw, geo_filter, days=90)["value"]
               for kw in selected_kws
               if not db.get_interest_history(kw, geo_filter, days=90).empty}
        if dfs:
            df_plot = pd.DataFrame(dfs)
            st.plotly_chart(px.line(df_plot, title=f"Interest — {geo_filter}"), use_container_width=True)
            st.subheader("Signal Classifications")
            st.dataframe(classify_all(df_plot, geo=geo_filter), use_container_width=True)
            if len(dfs) > 1:
                pairs = find_correlated_pairs(df_plot, threshold=0.6)
                if pairs:
                    st.subheader("🔗 Correlated Keyword Pairs")
                    st.dataframe(pd.DataFrame(pairs), use_container_width=True)
        else:
            st.info("No data yet. Run a collection first.")

# ── Geo Map ───────────────────────────────────────────────────────────────────
elif page == "🗺 Geo Map":
    st.title("🗺 Geographic Interest Map")
    keyword   = st.selectbox("Keyword", seed_keywords)
    timeframe = st.selectbox("Timeframe", ["now 7-d", "today 1-m", "today 3-m"])
    if st.button("🔄 Fetch Live Geo Data"):
        with st.spinner("Fetching from Google Trends..."):
            try:
                ibr = TrendsCollector(geo="", timeframe=timeframe).interest_by_region([keyword], resolution="COUNTRY")
                if not ibr.empty:
                    ibr = ibr.reset_index()
                    fig = px.choropleth(ibr,
                        locations="geoCode" if "geoCode" in ibr.columns else "geoName",
                        locationmode="ISO-3166-1-alpha-2" if "geoCode" in ibr.columns else "country names",
                        color=keyword, color_continuous_scale="Viridis",
                        title=f"'{keyword}' Interest by Country")
                    st.plotly_chart(fig, use_container_width=True)
                    st.dataframe(ibr.sort_values(keyword, ascending=False).head(20), use_container_width=True)
            except Exception as e:
                st.error(f"Error: {e}")

# ── Ads Brief ─────────────────────────────────────────────────────────────────
elif page == "🎯 Ads Brief":
    st.title("🎯 Platform Ad Recommendations")
    keyword   = st.selectbox("Keyword", seed_keywords)
    geo       = st.selectbox("Geo", all_geos)
    trend_lbl = st.selectbox("Trend Signal", ["breakout","rising","stable","falling","seasonal"])
    momentum  = st.slider("Momentum Score", -100, 100, 25)
    if st.button("⚡ Generate Brief"):
        brief = full_platform_brief(keyword, trend_lbl, momentum, geo)
        db.save_ad_brief(brief)
        for tab, platform in zip(st.tabs(["Google Ads","Meta","TikTok","YouTube"]),
                                 ["google_ads","meta","tiktok","youtube"]):
            with tab:
                for k, v in brief["platforms"][platform].items():
                    label = k.replace("_"," ").title()
                    if isinstance(v, list):  st.write(f"**{label}:** {', '.join(str(i) for i in v)}")
                    elif isinstance(v, bool): st.write(f"**{label}:** {'✅' if v else '❌'}")
                    elif v is not None:       st.write(f"**{label}:** {v}")

# ── Keyword Tool ──────────────────────────────────────────────────────────────
elif page == "🔍 Keyword Tool":
    st.title("🔍 Ad-Hoc Keyword Analysis")
    custom_kw = st.text_input("Keywords (comma-separated)", "fashion, smartphones")
    geo_kw    = st.selectbox("Geo", all_geos)
    tf_kw     = st.selectbox("Timeframe", ["now 7-d","now 1-d","today 1-m","today 3-m"])
    if st.button("🚀 Run Analysis"):
        keywords = [k.strip() for k in custom_kw.split(",")][:5]
        with st.spinner("Fetching from Google Trends..."):
            try:
                collector = TrendsCollector(geo=geo_kw, timeframe=tf_kw)
                iot = collector.interest_over_time(keywords)
                if not iot.empty:
                    st.plotly_chart(px.line(iot[keywords], title=f"Interest | {geo_kw} | {tf_kw}"), use_container_width=True)
                    st.dataframe(classify_all(iot[keywords], geo=geo_kw), use_container_width=True)
                rq = collector.related_queries(keywords)
                for kw, data in rq.items():
                    rising = data.get("rising")
                    if rising is not None and not rising.empty:
                        st.write(f"**{kw}** rising queries:")
                        st.dataframe(rising.head(10), use_container_width=True)
                for kw in keywords:
                    sugg = collector.suggestions(kw)
                    if not sugg.empty:
                        st.write(f"**{kw} suggestions:** {', '.join(sugg['title'].tolist()[:8])}")
            except Exception as e:
                st.error(f"API Error: {e} — wait 60s and retry (Google rate limit)")

# ── Settings ──────────────────────────────────────────────────────────────────
elif page == "⚙️ Settings":
    st.title("⚙️ Settings")
    st.subheader("Active Geos")
    st.dataframe(pd.DataFrame(cfg["geos"]), use_container_width=True)
    st.subheader("Alert Config")
    st.json(cfg["alerts"])
    st.subheader("Rate Limits")
    st.json(cfg["collection"]["rate_limit"])

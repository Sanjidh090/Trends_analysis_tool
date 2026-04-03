# app.py  — Streamlit Dashboard (flat import version)
# Run with:  streamlit run app.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import json
import yaml
import streamlit as st
import pandas as pd
import plotly.express as px

from db import TrendsDB
from signal_processor import classify_all, find_correlated_pairs, classify_share_shift
from targeting_engine import full_platform_brief
from trends_collector import TrendsCollector
from copy_generator import generate_ad_copy, is_configured as gpt_is_configured
from tiktok_enricher import is_configured as tiktok_is_configured

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

st.set_page_config(page_title="Trends Intel", page_icon="📊", layout="wide")

@st.cache_resource
def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

@st.cache_resource
def get_db():
    cfg = load_config()
    return TrendsDB(cfg["storage"]["db_path"], db_config=cfg.get("database", {}))

cfg = load_config()
db  = get_db()

st.sidebar.title("📊 Trends Intel")
page = st.sidebar.radio("Navigate", [
    "🏠 Overview", "📈 Trends", "🗺 Geo Map",
    "🎯 Ads Brief", "🏆 Competitor", "🔍 Keyword Tool", "⚙️ Settings"
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
        brief = full_platform_brief(keyword, trend_lbl, momentum, geo, config=cfg)
        db.save_ad_brief(brief)
        platform_keys  = ["google_ads", "meta", "tiktok", "youtube"]
        platform_names = ["Google Ads", "Meta", "TikTok", "YouTube"]
        gpt_available  = gpt_is_configured(cfg)
        openai_model   = cfg.get("openai", {}).get("model", "gpt-4o")

        for tab, platform, pname in zip(
            st.tabs(platform_names), platform_keys, platform_names
        ):
            with tab:
                pdata = brief["platforms"][platform]
                for k, v in pdata.items():
                    label = k.replace("_", " ").title()
                    if k == "trending_sounds" and isinstance(v, list):
                        st.markdown(f"**🎵 Trending Sounds** (TikTok Creative Center):")
                        for s in v:
                            st.write(f"- {s['title']} — {s['artist']}")
                    elif isinstance(v, list):
                        st.write(f"**{label}:** {', '.join(str(i) for i in v)}")
                    elif isinstance(v, bool):
                        st.write(f"**{label}:** {'✅' if v else '❌'}")
                    elif v is not None:
                        st.write(f"**{label}:** {v}")

                st.markdown("---")
                if gpt_available:
                    if st.button(f"✍️ Generate Copy ({pname})", key=f"copy_{platform}"):
                        with st.spinner("Generating ad copy with GPT..."):
                            copy = generate_ad_copy(pdata, platform, cfg)
                        if copy:
                            db.save_ad_copy(keyword, geo, platform, copy, model=openai_model)
                            st.success("Ad copy generated!")
                            st.text_area("Headline", copy.get("headline", ""), key=f"hl_{platform}")
                            st.text_area("Body",     copy.get("body",     ""), key=f"bd_{platform}")
                            st.text_area("CTA",      copy.get("cta",      ""), key=f"cta_{platform}")
                            tags = copy.get("hashtags", [])
                            if tags:
                                st.write("**Hashtags:** " + "  ".join(f"`{h}`" for h in tags))
                        else:
                            st.error("Copy generation failed. Check OpenAI API key and logs.")
                else:
                    st.caption("💡 Add your OpenAI API key in config.yaml to enable ✍️ Generate Copy")

    # Show ad copy history
    st.markdown("---")
    st.subheader("📚 Ad Copy History")
    hist_kw  = st.selectbox("Keyword (history)", seed_keywords, key="hist_kw")
    hist_geo = st.selectbox("Geo (history)", all_geos, key="hist_geo")
    history  = db.get_ad_copy_history(hist_kw, hist_geo)
    if not history.empty:
        st.dataframe(history, use_container_width=True)
    else:
        st.caption("No copy generated yet for this keyword/geo.")

# ── Competitor ────────────────────────────────────────────────────────────────
elif page == "🏆 Competitor":
    st.title("🏆 Competitor Share-of-Search")
    competitor_map = cfg.get("competitors", {})

    if not competitor_map:
        st.warning("No competitors configured. Add a `competitors:` section to config.yaml.")
    else:
        brand_kw = st.selectbox("Your Brand Keyword", list(competitor_map.keys()))
        geo      = st.selectbox("Geo", all_geos)
        days     = st.slider("History (days)", 30, 180, 90)

        all_kws = [brand_kw] + list(competitor_map[brand_kw])

        st.subheader("📊 Stored Share-of-Search")
        sos_df = db.get_share_of_search_history(all_kws, geo, days=days)

        if not sos_df.empty:
            fig = px.area(sos_df, title=f"Share of Search — {brand_kw} vs competitors ({geo})",
                          labels={"value": "Share (%)", "variable": "Keyword"},
                          color_discrete_sequence=px.colors.qualitative.Set2)
            st.plotly_chart(fig, use_container_width=True)

            events = classify_share_shift(sos_df, brand_kw)
            if events:
                st.subheader("⚠️ Crossover Events")
                st.dataframe(pd.DataFrame(events), use_container_width=True)
            else:
                st.success("No crossover events detected in this period.")

            st.subheader("Per-Geo Competitor Comparison")
            geo_shares = []
            for g in all_geos:
                gdf = db.get_share_of_search_history(all_kws, g, days=days)
                if not gdf.empty:
                    latest = gdf.iloc[-1].rename(g)
                    geo_shares.append(latest)
            if geo_shares:
                geo_share_df = pd.DataFrame(geo_shares)
                st.dataframe(geo_share_df.style.format("{:.1f}%"), use_container_width=True)
        else:
            st.info("No stored share-of-search data yet.")

        st.markdown("---")
        st.subheader("🔄 Fetch Live Share-of-Search")
        timeframe = st.selectbox("Timeframe", ["now 7-d", "today 1-m", "today 3-m"])
        if st.button("📡 Fetch & Save Live Data"):
            with st.spinner("Fetching from Google Trends..."):
                try:
                    collector = TrendsCollector(geo=geo, timeframe=timeframe)
                    sos = collector.get_share_of_search(all_kws)
                    if not sos.empty:
                        db.save_share_of_search(sos, geo, timeframe)
                        st.success(f"Saved {len(sos)} rows of share-of-search data.")
                        fig2 = px.area(sos, title=f"Live Share of Search — {brand_kw} ({geo})",
                                       labels={"value": "Share (%)", "variable": "Keyword"})
                        st.plotly_chart(fig2, use_container_width=True)
                    else:
                        st.warning("No data returned from Google Trends.")
                except Exception as e:
                    st.error(f"API Error: {e}")

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

    # Proxy status
    st.subheader("🔄 Proxy Status")
    proxy_cfg  = cfg.get("proxy", {})
    if proxy_cfg.get("enabled") and proxy_cfg.get("proxies"):
        from trends_collector import ProxyRotator
        rotator = ProxyRotator(proxy_cfg["proxies"], cooldown=proxy_cfg.get("cooldown_seconds", 300))
        proxy_status = rotator.status()
        st.dataframe(pd.DataFrame(proxy_status), use_container_width=True)
    else:
        st.info("Proxy rotation is disabled. Enable it in config.yaml under `proxy:`.")

    # Database info
    st.subheader("🐘 Database Config")
    db_cfg = cfg.get("database", {"type": "sqlite"})
    st.json({"type": db_cfg.get("type", "sqlite"), "host": db_cfg.get("host", "N/A"),
             "name": db_cfg.get("name", "N/A")})

    # GPT status
    st.subheader("🤖 GPT Ad Copy")
    if gpt_is_configured(cfg):
        st.success(f"OpenAI configured | model: {cfg['openai'].get('model','gpt-4o')}")
    else:
        st.warning("OpenAI not configured. Add `openai.api_key` to config.yaml.")

    # TikTok API status
    st.subheader("🎵 TikTok Creative Center API")
    if tiktok_is_configured(cfg):
        st.success(f"TikTok API configured | region: {cfg['tiktok_api'].get('region','US')}")
    else:
        st.warning("TikTok API not configured. Add `tiktok_api.access_token` to config.yaml.")

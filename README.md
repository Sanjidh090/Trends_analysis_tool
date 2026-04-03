# 📊 Google Trends Ads Intelligence Platform

A full-scale, production-grade tool for turning Google Trends data into
actionable ad targeting insights across **Google Ads, Meta, TikTok, and YouTube**.

---

## 🗂️ Project Structure

```
trends_intel/
├── config.yaml              ← Master config: geos, keywords, platforms, alerts
├── main.py                  ← CLI entry point
├── requirements.txt
│
├── collector/
│   ├── trends_collector.py  ← pytrends wrapper (rate limiting, retry, backoff)
│   └── geo_orchestrator.py  ← Multi-geo collection runner
│
├── signals/
│   └── signal_processor.py  ← Momentum, breakout, seasonality, correlation
│
├── ads_intel/
│   └── targeting_engine.py  ← Platform-specific ad recommendations (all 4 platforms)
│
├── reports/
│   └── report_generator.py  ← Excel weekly brief, Slack/email alerting
│
├── scheduler/
│   └── jobs.py              ← APScheduler daily + weekly + breakout jobs
│
├── dashboard/
│   └── app.py               ← Streamlit dashboard (6 pages)
│
└── storage/
    └── db.py                ← SQLite persistence layer
```

---

## ⚡ Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure your targets
Edit `config.yaml`:
- Add/remove geos under `geos:`
- Set your seed keywords under `collection.keywords_seed`
- Configure Slack webhook and email under `alerts:`

### 3. Run a one-off collection
```bash
python main.py --mode collect --keywords "fashion,smartphones,gaming" --geo US
```

### 4. Launch the dashboard
```bash
streamlit run dashboard/app.py
```

### 5. Start the automated scheduler
```bash
python main.py --mode scheduler
```

---

## 🌍 Configured Geos

| Region       | Countries                                         |
|--------------|---------------------------------------------------|
| Middle East  | Saudi Arabia, UAE, Egypt, Qatar, Kuwait           |
| Europe       | Switzerland, Germany, France, UK, Netherlands     |
| Americas     | United States                                     |

Add more geos in `config.yaml` using ISO 2-letter country codes.

---

## 🎯 Ad Platforms Covered

| Platform    | Output                                                          |
|-------------|------------------------------------------------------------------|
| Google Ads  | Match types, bid adjustments, negative KWs, audience layers     |
| Meta        | Creative direction, audience strategy, format, CBO flag         |
| TikTok      | Content hook, hashtags, video length, format, activation gate   |
| YouTube     | Channel targeting, ad format, hook timing, frequency cap        |

---

## 📊 Signal Types

| Signal    | Meaning                                            |
|-----------|----------------------------------------------------|
| 🚀 Breakout | Z-score spike — act within 24-48h                |
| 📈 Rising   | Positive momentum — rising trend                 |
| ➡️ Stable   | No significant change                             |
| 📉 Falling  | Declining interest                               |
| 🔄 Seasonal | Cyclical pattern detected                        |

---

## 📅 Scheduled Jobs

| Job              | Schedule                     | What it does                        |
|------------------|------------------------------|-------------------------------------|
| daily_collection | Daily at 06:00 UTC           | Pulls trends for all geos + keywords|
| breakout_check   | Every 30 minutes             | Detects spikes, fires Slack alerts  |
| weekly_report    | Monday at 07:00 UTC          | Generates Excel brief + emails it   |

---

## 🛠️ Next Steps to Build Out

1. **Proxy rotation** — add `proxies` list to `TrendsCollector` for heavy usage
2. **PostgreSQL** — swap `storage/db.py` SQLite for Postgres for team use
3. **Airflow** — replace APScheduler with Airflow DAGs for production scheduling
4. **Competitor tracking** — add competitor brand keywords to config and compare share-of-search
5. **GPT-powered ad copy** — pipe `full_platform_brief()` output to LLM for auto-draft ad copy
6. **TikTok Creative Center API** — enrich TikTok recommendations with real trending audio data

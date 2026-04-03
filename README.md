# 📊 Google Trends Ads Intelligence Platform

A full-scale, production-grade tool for turning Google Trends data into
actionable ad targeting insights across **Google Ads, Meta, TikTok, and YouTube**.

---

## 🗂️ Project Structure

```
Trends_analysis_tool/
├── config.yaml              ← Master config: geos, keywords, platforms, alerts, proxies, DB, OpenAI, TikTok API
├── main.py                  ← CLI entry point (collect | scheduler | migrate)
├── requirements.txt         ← Core dependencies
├── requirements-airflow.txt ← Airflow-specific dependencies
│
├── trends_collector.py      ← pytrends wrapper + ProxyRotator
├── geo_orchestrator.py      ← Multi-geo collection runner (proxy-aware)
├── signal_processor.py      ← Momentum, breakout, seasonality, correlation, share-shift
├── targeting_engine.py      ← Platform-specific ad recommendations (all 4 platforms + TikTok enrichment)
├── report_generator.py      ← Excel weekly brief, Slack/email alerting
├── jobs.py                  ← APScheduler daily + weekly + breakout jobs
├── app.py                   ← Streamlit dashboard (7 pages)
├── db.py                    ← SQLite / PostgreSQL persistence layer
├── copy_generator.py        ← GPT-powered ad copy generation (OpenAI)
├── tiktok_enricher.py       ← TikTok Creative Center API integration
│
├── dags/                    ← Apache Airflow DAGs (production scheduling)
│   ├── daily_collection_dag.py
│   ├── breakout_check_dag.py
│   └── weekly_report_dag.py
│
└── storage/
    └── trends.db            ← SQLite database (default)
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
streamlit run app.py
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

## 🏆 Competitor Tracking / Share-of-Search

Map your brand keywords to competitors in `config.yaml`:

```yaml
competitors:
  "AI tools": ["chatgpt", "claude ai", "gemini ai"]
  "fitness":  ["gym membership", "home workout", "yoga"]
```

Then visit the **🏆 Competitor** page in the dashboard to:
- Fetch live share-of-search from Google Trends
- View stacked area charts of share over time
- See crossover alerts (when a competitor overtakes your brand)
- Compare share per geo

---

## 🤖 GPT-Powered Ad Copy

Add your OpenAI key to `config.yaml`:

```yaml
openai:
  api_key: "sk-..."
  model: "gpt-4o"
  max_tokens: 512
```

Then click **✍️ Generate Copy** on the **🎯 Ads Brief** page to auto-generate
platform-specific ad copy (headline, body, CTA, hashtags) for each platform.

---

## 🔄 Proxy Rotation

For high-volume scraping across many geos/keywords, configure proxies:

```yaml
proxy:
  enabled: true
  proxies:
    - "http://user:pass@proxy1:8080"
    - "http://user:pass@proxy2:8080"
  cooldown_seconds: 300
```

Proxy health is visible in the **⚙️ Settings** page.

---

## 🐘 PostgreSQL Setup

By default the platform uses SQLite (zero-config). To switch to PostgreSQL:

1. Create a Postgres database:
   ```sql
   CREATE DATABASE trends_intel;
   ```

2. Update `config.yaml`:
   ```yaml
   database:
     type: "postgres"
     host: "localhost"
     port: 5432
     name: "trends_intel"
     user: "postgres"
     password: "your_password"
   ```

3. Run the migration command to create the schema:
   ```bash
   python main.py --mode migrate
   ```

4. Install the Postgres driver (already in `requirements.txt`):
   ```bash
   pip install psycopg2-binary
   ```

---

## ✈️ Airflow Production Scheduling

For production use, replace APScheduler with Apache Airflow:

1. Install Airflow:
   ```bash
   pip install -r requirements-airflow.txt
   airflow db init
   ```

2. Point Airflow at the `dags/` folder:
   ```bash
   export AIRFLOW__CORE__DAGS_FOLDER=/path/to/Trends_analysis_tool/dags
   airflow dags list
   ```

3. Update `config.yaml`:
   ```yaml
   scheduler:
     type: "airflow"
   ```

The APScheduler (`python main.py --mode scheduler`) remains available for local/dev use.

---

## 🎵 TikTok Creative Center API

Enrich TikTok recommendations with live trending hashtags and sounds:

1. Obtain a TikTok Business API access token from [TikTok for Business](https://business.tiktok.com/).

2. Update `config.yaml`:
   ```yaml
   tiktok_api:
     access_token: "your_token"
     region: "US"
   ```

When configured, the **🎯 Ads Brief → TikTok** tab will show live trending sounds
and the hashtag strategy will be populated from real Creative Center data.


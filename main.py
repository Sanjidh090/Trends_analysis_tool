# main.py
"""
Google Trends Intelligence Platform — Main Entry Point

Usage:
    python main.py --mode collect --keywords "fashion,smartphones,gaming" --geo US
    python main.py --mode collect   (uses seed keywords from config.yaml, all geos)
    python main.py --mode scheduler (runs automated daily/weekly jobs)
    python main.py --mode migrate   (create/migrate database schema)
    streamlit run app.py            (launch dashboard)
"""

import sys
import argparse
import logging
import yaml
from pathlib import Path

# Flat structure — all .py files are siblings of main.py in the same folder
sys.path.insert(0, str(Path(__file__).resolve().parent))

Path("logs").mkdir(exist_ok=True)  # must exist before FileHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/trends_intel.log"),
    ],
)
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


def run_collect(keywords: list, geo: str = None):
    """One-shot collection + signal processing + ad brief generation."""
    # Flat imports — every file is in the same directory
    from geo_orchestrator import GeoOrchestrator
    from signal_processor import classify_all
    from targeting_engine import full_platform_brief
    from db import TrendsDB
    import pandas as pd

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    orch    = GeoOrchestrator(config_path=CONFIG_PATH)
    results = orch.run(keywords=keywords, timeframes=["now 7-d"])
    db      = TrendsDB(cfg["storage"]["db_path"], db_config=cfg.get("database", {}))

    logger.info("Running signal processing...")
    geos = [geo] if geo else [g["code"] for g in cfg["geos"]]

    for g in geos:
        kw_series = {}
        for kw in keywords:
            history = db.get_interest_history(kw, g, days=30)
            if not history.empty and "value" in history.columns:
                kw_series[kw] = history["value"]

        if not kw_series:
            logger.warning(f"No stored history yet for geo={g} — will appear after collection completes.")
            continue

        df              = pd.DataFrame(kw_series)
        classifications = classify_all(df, geo=g)
        logger.info(f"\n{'='*50}\nGeo: {g}\n{classifications.to_string()}\n{'='*50}")

        for kw, row in classifications.iterrows():
            brief = full_platform_brief(kw, row["label"], row["momentum"], g)
            db.save_ad_brief(brief)
            logger.info(f"Ad brief saved | keyword={kw} | geo={g} | trend={row['label']}")


def run_scheduler():
    from jobs import run_scheduler as _run
    _run()


def run_migrate():
    """Create / migrate the database schema (useful for initial PostgreSQL setup)."""
    from db import TrendsDB
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    db_config = cfg.get("database", {})
    db_path   = cfg["storage"]["db_path"]
    db = TrendsDB(db_path=db_path, db_config=db_config)
    db_type = db_config.get("type", "sqlite")
    logger.info(f"Migration complete | db_type={db_type}")
    print(f"✅ Database schema created/verified ({db_type})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Trends Intelligence Platform")
    parser.add_argument("--mode",     choices=["collect", "scheduler", "migrate"], default="collect")
    parser.add_argument("--keywords", type=str, help="Comma-separated keywords", default=None)
    parser.add_argument("--geo",      type=str, help="Single geo code e.g. US",  default=None)
    args = parser.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    if args.mode == "collect":
        kws = [k.strip() for k in args.keywords.split(",")] if args.keywords \
              else cfg["collection"]["keywords_seed"]
        run_collect(kws, args.geo)

    elif args.mode == "scheduler":
        run_scheduler()

    elif args.mode == "migrate":
        run_migrate()

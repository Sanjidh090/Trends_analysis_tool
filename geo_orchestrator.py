# geo_orchestrator.py
"""
Runs TrendsCollector across all configured geos in sequence,
stores results, and merges into a unified multi-geo dataset.
"""

import time
import logging
import yaml
from pathlib import Path
from typing import Optional
import pandas as pd

from trends_collector import TrendsCollector   # flat import
from db import TrendsDB                        # flat import

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


class GeoOrchestrator:

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.geos     = self.config["geos"]
        self.db       = TrendsDB(self.config["storage"]["db_path"])
        self.rate_cfg = self.config["collection"]["rate_limit"]

    def run(self, keywords: list, timeframes: Optional[list] = None,
            category: int = 0, skip_geos: Optional[list] = None) -> dict:
        timeframes = timeframes or self.config["collection"]["timeframes"][:2]
        skip_geos  = skip_geos or []
        results    = {"interest_over_time": {}, "related_queries": {}, "interest_by_region": {}}

        for geo in self.geos:
            code = geo["code"]
            if code in skip_geos:
                continue
            logger.info(f"Collecting | geo={code} ({geo['name']}) | keywords={keywords}")

            for tf in timeframes:
                collector = TrendsCollector(geo=code, timeframe=tf, tz=geo.get("tz", 0))

                iot = collector.interest_over_time(keywords, category)
                if not iot.empty:
                    iot["geo"] = code
                    iot["timeframe"] = tf
                    results["interest_over_time"][f"{code}_{tf}"] = iot
                    self.db.save_interest_over_time(iot, geo=code, timeframe=tf)

                if tf == timeframes[0]:
                    rq = collector.related_queries(keywords)
                    results["related_queries"][code] = rq
                    self.db.save_related_queries(rq, geo=code)

                    ibr = collector.interest_by_region(keywords, resolution="REGION")
                    if not ibr.empty:
                        results["interest_by_region"][code] = ibr

                time.sleep(60 / self.rate_cfg["requests_per_minute"])

        logger.info(f"GeoOrchestrator complete | keywords={keywords}")
        return results

    def get_trending_by_region(self) -> dict:
        country_map = {
            "US": "united_states", "GB": "united_kingdom", "DE": "germany",
            "FR": "france", "AE": "united_arab_emirates", "SA": "saudi_arabia",
            "EG": "egypt", "QA": "qatar", "CH": "switzerland", "NL": "netherlands",
        }
        trending = {}
        for geo in self.geos:
            code    = geo["code"]
            country = country_map.get(code)
            if not country:
                continue
            collector = TrendsCollector(geo=code, tz=geo.get("tz", 0))
            df = collector.trending_searches(country=country)
            if not df.empty:
                trending[code] = df
            time.sleep(8)
        return trending

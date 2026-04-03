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

from trends_collector import TrendsCollector, ProxyRotator   # flat import
from db import TrendsDB                                       # flat import

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"


class GeoOrchestrator:

    def __init__(self, config_path: Path = CONFIG_PATH):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.geos     = self.config["geos"]
        self.rate_cfg = self.config["collection"]["rate_limit"]

        db_config = self.config.get("database", {})
        self.db   = TrendsDB(
            db_path=self.config["storage"]["db_path"],
            db_config=db_config,
        )

        # Initialise proxy rotator if proxies are configured
        proxy_cfg  = self.config.get("proxy", {})
        proxy_list = proxy_cfg.get("proxies", []) if proxy_cfg.get("enabled") else []
        cooldown   = proxy_cfg.get("cooldown_seconds", 300)
        self.rotator = ProxyRotator(proxy_list, cooldown=cooldown) if proxy_list else None

    def _make_collector(self, geo_code: str, timeframe: str, tz: int) -> TrendsCollector:
        """Build a TrendsCollector, injecting a proxy from the rotator when available."""
        proxy = self.rotator.get() if self.rotator else None
        proxies = [proxy] if proxy else []
        return TrendsCollector(geo=geo_code, timeframe=timeframe, tz=tz, proxies=proxies)

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
                collector = self._make_collector(code, tf, geo.get("tz", 0))

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
            collector = self._make_collector(code, "now 1-d", geo.get("tz", 0))
            df = collector.trending_searches(country=country)
            if not df.empty:
                trending[code] = df
            time.sleep(8)
        return trending

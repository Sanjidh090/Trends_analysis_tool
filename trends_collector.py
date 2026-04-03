# collector/trends_collector.py
"""
Core pytrends wrapper with:
 - Rate limiting & exponential backoff
 - Proxy rotation support
 - Multi-geo, multi-timeframe collection
 - All pytrends endpoints exposed cleanly
"""

import time
import logging
from typing import Optional
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

logger = logging.getLogger(__name__)


class TrendsCollector:
    """
    Production-grade pytrends wrapper.

    Usage:
        collector = TrendsCollector(geo="US", timeframe="now 7-d")
        df = collector.interest_over_time(["Nike", "Adidas"])
    """

    def __init__(
        self,
        geo: str = "US",
        timeframe: str = "now 7-d",
        language: str = "en-US",
        tz: int = 0,
        proxies: Optional[list] = None,
        retries: int = 3,
        backoff: int = 60,
    ):
        self.geo = geo
        self.timeframe = timeframe
        self.retries = retries
        self.backoff = backoff

        self.pytrends = TrendReq(
            hl=language,
            tz=tz,
            proxies=proxies or [],
            timeout=(10, 25),
            retries=retries,
            backoff_factor=0.1,
        )
        logger.info(f"TrendsCollector initialized | geo={geo} | timeframe={timeframe}")

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _build(self, keywords: list, category: int = 0):
        """Build pytrends payload with retry logic."""
        for attempt in range(self.retries):
            try:
                self.pytrends.build_payload(
                    kw_list=keywords[:5],          # Google max = 5 keywords
                    cat=category,
                    timeframe=self.timeframe,
                    geo=self.geo,
                )
                return
            except TooManyRequestsError:
                wait = self.backoff * (2 ** attempt)
                logger.warning(f"Rate limited. Waiting {wait}s before retry {attempt+1}")
                time.sleep(wait)
        raise RuntimeError(f"Failed to build payload after {self.retries} retries")

    def _safe_fetch(self, fn, *args, **kwargs):
        """Wrap any pytrends call with retry + backoff."""
        for attempt in range(self.retries):
            try:
                result = fn(*args, **kwargs)
                time.sleep(1.5)                    # polite delay between calls
                return result
            except TooManyRequestsError:
                wait = self.backoff * (2 ** attempt)
                logger.warning(f"Rate limited on fetch. Waiting {wait}s")
                time.sleep(wait)
            except Exception as e:
                logger.error(f"Fetch error: {e}")
                return None
        return None

    # ── Public API ────────────────────────────────────────────────────────────

    def interest_over_time(self, keywords: list, category: int = 0) -> pd.DataFrame:
        """
        Returns a DataFrame with weekly/daily interest score (0-100) for each keyword.
        Index = datetime, columns = keywords + isPartial flag.
        """
        self._build(keywords, category)
        df = self._safe_fetch(self.pytrends.interest_over_time)
        if df is not None and not df.empty:
            df = df.drop(columns=["isPartial"], errors="ignore")
        return df or pd.DataFrame()

    def interest_by_region(self, keywords: list, resolution: str = "COUNTRY") -> pd.DataFrame:
        """
        Returns geographic breakdown of interest.
        resolution: 'COUNTRY' | 'REGION' | 'CITY' | 'DMA'
        """
        self._build(keywords)
        return self._safe_fetch(
            self.pytrends.interest_by_region,
            resolution=resolution,
            inc_low_vol=True,
            inc_geo_code=True,
        ) or pd.DataFrame()

    def related_queries(self, keywords: list) -> dict:
        """
        Returns dict of {keyword: {'top': df, 'rising': df}}.
        'rising' queries are breakout signals — high value for ads.
        """
        self._build(keywords)
        result = self._safe_fetch(self.pytrends.related_queries)
        return result or {}

    def related_topics(self, keywords: list) -> dict:
        """Returns dict of {keyword: {'top': df, 'rising': df}} for broader topics."""
        self._build(keywords)
        return self._safe_fetch(self.pytrends.related_topics) or {}

    def trending_searches(self, country: str = "united_states") -> pd.DataFrame:
        """Returns today's real-time trending searches for a country."""
        return self._safe_fetch(
            self.pytrends.trending_searches,
            pn=country,
        ) or pd.DataFrame()

    def top_charts(self, year: int, geo: str = "GLOBAL") -> pd.DataFrame:
        """Returns Google Year in Search top chart for a given year."""
        return self._safe_fetch(
            self.pytrends.top_charts,
            year,
            hl="en-US",
            tz=300,
            geo=geo,
        ) or pd.DataFrame()

    def suggestions(self, keyword: str) -> pd.DataFrame:
        """Returns keyword expansion suggestions from Google autocomplete."""
        raw = self._safe_fetch(self.pytrends.suggestions, keyword=keyword)
        if raw:
            return pd.DataFrame(raw).drop(columns=["mid"], errors="ignore")
        return pd.DataFrame()

    def full_keyword_profile(self, keywords: list, category: int = 0) -> dict:
        """
        Convenience method: runs all relevant endpoints for a keyword list.
        Returns a dict with keys: iot, region, related_queries, related_topics, suggestions.
        """
        logger.info(f"Running full profile | keywords={keywords} | geo={self.geo}")
        profile = {}
        profile["interest_over_time"] = self.interest_over_time(keywords, category)
        profile["interest_by_region"] = self.interest_by_region(keywords)
        profile["related_queries"]    = self.related_queries(keywords)
        profile["related_topics"]     = self.related_topics(keywords)
        profile["suggestions"]        = {kw: self.suggestions(kw) for kw in keywords}
        return profile

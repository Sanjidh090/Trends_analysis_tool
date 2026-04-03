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
import threading
from typing import Optional
import pandas as pd
from pytrends.request import TrendReq
from pytrends.exceptions import TooManyRequestsError

logger = logging.getLogger(__name__)


# ── Proxy Rotator ──────────────────────────────────────────────────────────────

class ProxyRotator:
    """
    Round-robin proxy rotator with per-proxy cooldown after HTTP 429.

    Usage:
        rotator = ProxyRotator(["http://p1:8080", "http://p2:8080"], cooldown=300)
        proxy = rotator.get()          # returns next healthy proxy or None
        rotator.mark_bad(proxy)        # sidelinesthe proxy for cooldown_seconds
    """

    def __init__(self, proxies: list, cooldown: int = 300):
        self._proxies  = list(proxies)
        self._cooldown = cooldown
        self._bad: dict[str, float] = {}   # proxy -> timestamp when it went bad
        self._lock = threading.Lock()
        self._idx  = 0

    def get(self) -> Optional[str]:
        """Return the next healthy proxy, or None if none are available."""
        if not self._proxies:
            return None
        with self._lock:
            now = time.time()
            healthy = [p for p in self._proxies
                       if now - self._bad.get(p, 0) >= self._cooldown]
            if not healthy:
                return None
            proxy = healthy[self._idx % len(healthy)]
            self._idx += 1
        return proxy

    def mark_bad(self, proxy: str):
        """Sidelinesthe proxy for cooldown_seconds."""
        if proxy:
            with self._lock:
                self._bad[proxy] = time.time()
            logger.warning(f"Proxy marked bad (cooling down): {proxy}")

    def status(self) -> list[dict]:
        """Return health status for all proxies (for display in Settings page)."""
        now = time.time()
        result = []
        for p in self._proxies:
            bad_since = self._bad.get(p, 0)
            remaining = max(0, self._cooldown - (now - bad_since))
            result.append({
                "proxy":      p,
                "healthy":    remaining == 0,
                "cooldown_remaining_s": int(remaining),
            })
        return result


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

    def get_share_of_search(
        self,
        keywords: list,
        category: int = 0,
    ) -> pd.DataFrame:
        """
        Compute share-of-search: each keyword's interest as a % of the total
        interest across all keywords in the list.

        Returns a DataFrame indexed by date with one column per keyword,
        values are percentages (0–100).  Returns empty DataFrame on failure.
        """
        df = self.interest_over_time(keywords, category)
        if df is None or df.empty:
            return pd.DataFrame()
        cols = [c for c in df.columns if c in keywords]
        if not cols:
            return pd.DataFrame()
        row_totals = df[cols].sum(axis=1).replace(0, float("nan"))
        share = df[cols].div(row_totals, axis=0).mul(100).round(2)
        logger.info(f"Share-of-search computed | keywords={keywords} | geo={self.geo}")
        return share

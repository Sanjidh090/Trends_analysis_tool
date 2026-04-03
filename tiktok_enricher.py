# tiktok_enricher.py
"""
TikTok Creative Center API integration.

Fetches live trending hashtags and sounds from TikTok's Creative Center API
to enrich TikTok ad recommendations with real data.

Usage:
    from tiktok_enricher import get_trending_hashtags, get_trending_sounds, is_configured

    if is_configured(config):
        hashtags = get_trending_hashtags("fitness", "US", config)
        sounds   = get_trending_sounds("fitness", "US", config)
"""

import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

_BASE_URL = "https://business-api.tiktok.com/open_api/v1.3/creative_center"
_TIMEOUT  = (5, 15)


def is_configured(config: dict) -> bool:
    """Return True if a TikTok Creative Center access token is present in config."""
    return bool(config.get("tiktok_api", {}).get("access_token", "").strip())


def _headers(access_token: str) -> dict:
    return {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }


def get_trending_hashtags(
    keyword: str,
    region: str,
    config: dict,
    limit: int = 10,
) -> list[dict]:
    """
    Fetch trending hashtags related to *keyword* from TikTok Creative Center.

    Returns a list of dicts:
        [{"hashtag_name": str, "hashtag_id": str, "trend_level": str}, ...]

    Falls back to an empty list on any error.
    """
    tiktok_cfg   = config.get("tiktok_api", {})
    access_token = tiktok_cfg.get("access_token", "").strip()
    if not access_token:
        logger.warning("TikTok access token not configured")
        return []

    url    = f"{_BASE_URL}/trending/hashtag/get/"
    params = {
        "region":   region or tiktok_cfg.get("region", "US"),
        "keyword":  keyword,
        "limit":    limit,
    }
    try:
        resp = requests.get(url, headers=_headers(access_token),
                            params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            logger.warning(f"TikTok hashtag API error: {data.get('message')}")
            return []
        items = data.get("data", {}).get("list", [])
        return [
            {
                "hashtag_name": item.get("hashtag_name", ""),
                "hashtag_id":   item.get("hashtag_id", ""),
                "trend_level":  item.get("trend_level", ""),
            }
            for item in items
        ]
    except requests.RequestException as e:
        logger.error(f"TikTok hashtag API request failed: {e}")
        return []


def get_trending_sounds(
    keyword: str,
    region: str,
    config: dict,
    limit: int = 10,
) -> list[dict]:
    """
    Fetch trending sounds/music related to *keyword* from TikTok Creative Center.

    Returns a list of dicts:
        [{"music_id": str, "title": str, "artist": str, "play_url": str}, ...]

    Falls back to an empty list on any error.
    """
    tiktok_cfg   = config.get("tiktok_api", {})
    access_token = tiktok_cfg.get("access_token", "").strip()
    if not access_token:
        logger.warning("TikTok access token not configured")
        return []

    url    = f"{_BASE_URL}/trending/music/get/"
    params = {
        "region":  region or tiktok_cfg.get("region", "US"),
        "keyword": keyword,
        "limit":   limit,
    }
    try:
        resp = requests.get(url, headers=_headers(access_token),
                            params=params, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            logger.warning(f"TikTok music API error: {data.get('message')}")
            return []
        items = data.get("data", {}).get("list", [])
        return [
            {
                "music_id": item.get("music_id", ""),
                "title":    item.get("title", ""),
                "artist":   item.get("author", ""),
                "play_url": item.get("play_url", ""),
            }
            for item in items
        ]
    except requests.RequestException as e:
        logger.error(f"TikTok music API request failed: {e}")
        return []

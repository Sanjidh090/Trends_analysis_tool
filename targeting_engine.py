# ads_intel/targeting_engine.py
"""
Ads Intelligence Layer.

Takes trend signals and produces platform-specific ad targeting recommendations:
  - Google Ads : match types, bid adjustments, audience targeting
  - Meta       : interest clusters, lookalike signals, creative direction
  - TikTok     : hashtag strategy, content hooks, spark ad candidates
  - YouTube    : content category, ad format, audience affinity
"""

import re
import yaml
import logging
from pathlib import Path
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


# ── Intent Classification ─────────────────────────────────────────────────────

INTENT_MAP = {
    "commercial":     ["buy", "price", "shop", "deal", "discount", "order", "cheap", "cost"],
    "informational":  ["how to", "what is", "why", "guide", "tutorial", "learn", "tips"],
    "transactional":  ["near me", "delivery", "book", "reserve", "sign up", "download", "free trial"],
    "navigational":   ["login", "official", "website", "contact", "support"],
    "research":       ["best", "review", "compare", "vs", "top", "ranking", "worth it"],
    "entertainment":  ["trending", "viral", "challenge", "meme", "funny", "tiktok"],
}

def classify_intent(query: str) -> str:
    """Return the dominant intent type for a search query."""
    q = query.lower()
    scores = {intent: sum(1 for sig in signals if sig in q)
              for intent, signals in INTENT_MAP.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "informational"


# ── Google Ads Recommendations ────────────────────────────────────────────────

def google_ads_recommendations(
    keyword: str,
    trend_label: str,
    momentum: float,
    geo: str,
    related_rising: Optional[list] = None,
) -> dict:
    """
    Generate Google Ads targeting recommendations for a trending keyword.
    """
    intent = classify_intent(keyword)

    # Bid adjustment based on momentum
    if momentum > 50:
        bid_adj = "+30%"
        urgency = "HIGH — act within 24–48h"
    elif momentum > 20:
        bid_adj = "+15%"
        urgency = "MEDIUM — act within 1 week"
    elif momentum < -20:
        bid_adj = "-20%"
        urgency = "LOW — trend declining"
    else:
        bid_adj = "0%"
        urgency = "STABLE — monitor"

    # Match type strategy
    match_types = {
        "commercial":    ["Exact", "Phrase"],
        "informational": ["Broad Match (with audience signals)", "Phrase"],
        "transactional": ["Exact", "Phrase"],
        "research":      ["Phrase", "Broad"],
        "entertainment": ["Broad", "Display keyword targeting"],
        "navigational":  ["Exact"],
    }.get(intent, ["Phrase"])

    # Ad format
    formats = {
        "breakout":  ["Responsive Search Ads", "Performance Max"],
        "rising":    ["Responsive Search Ads", "Display", "YouTube"],
        "seasonal":  ["Responsive Search Ads", "Shopping"],
        "stable":    ["Responsive Search Ads"],
        "falling":   ["Display retargeting only"],
    }.get(trend_label, ["Responsive Search Ads"])

    return {
        "platform":        "Google Ads",
        "keyword":         keyword,
        "intent":          intent,
        "trend":           trend_label,
        "geo":             geo,
        "bid_adjustment":  bid_adj,
        "urgency":         urgency,
        "match_types":     match_types,
        "recommended_formats": formats,
        "negative_kws":    _suggest_negatives(intent),
        "rising_queries":  related_rising or [],
        "audience_layers": _google_audience_layers(intent, trend_label),
    }


def _suggest_negatives(intent: str) -> list:
    """Suggest negative keywords based on intent type."""
    neg_map = {
        "commercial":    ["free", "diy", "how to"],
        "informational": ["buy", "price", "near me"],
        "transactional": ["what is", "history", "definition"],
        "entertainment": ["buy", "price", "cost"],
    }
    return neg_map.get(intent, [])


def _google_audience_layers(intent: str, trend: str) -> list:
    layers = ["Custom Intent (keyword-based)"]
    if intent in ["commercial", "transactional"]:
        layers.append("In-Market Audiences")
    if trend in ["breakout", "rising"]:
        layers.append("Life Events")
        layers.append("Similar Audiences to converters")
    return layers


# ── Meta Recommendations ──────────────────────────────────────────────────────

def meta_recommendations(
    keyword: str,
    trend_label: str,
    momentum: float,
    geo: str,
    related_topics: Optional[list] = None,
) -> dict:
    """Generate Meta (Facebook/Instagram) ad targeting recommendations."""

    intent = classify_intent(keyword)

    # Creative direction
    creative_dir = {
        "commercial":    "Product showcase carousel / catalog ad — highlight price/value",
        "informational": "Educational Reel or Story — 'Did you know?' hook",
        "entertainment": "UGC-style Reel — hook in first 2 seconds, trending audio",
        "research":      "Before/After or comparison creative — trust signals",
        "transactional": "Strong CTA with urgency — limited offer, countdown",
    }.get(intent, "Brand awareness video — emotional storytelling")

    # Audience strategy
    if trend_label == "breakout":
        audience_strategy = "Broad + Interest expansion ON — ride the wave"
    elif trend_label == "rising":
        audience_strategy = "Interest targeting + Lookalike 1-3% from converters"
    elif trend_label == "seasonal":
        audience_strategy = "Retargeting + Lookalike — predictable demand spike"
    else:
        audience_strategy = "Retargeting warm audiences + custom intent"

    return {
        "platform":          "Meta (Facebook/Instagram)",
        "keyword":           keyword,
        "intent":            intent,
        "trend":             trend_label,
        "geo":               geo,
        "creative_direction":creative_dir,
        "audience_strategy": audience_strategy,
        "interest_clusters": related_topics or [keyword],
        "ad_formats":        _meta_formats(trend_label, intent),
        "placement_priority":_meta_placements(trend_label),
        "cbo_recommended":   trend_label in ["breakout", "rising"],
    }


def _meta_formats(trend: str, intent: str) -> list:
    if trend == "breakout":
        return ["Reels", "Stories", "In-Feed Video"]
    if intent == "commercial":
        return ["Carousel", "Collection", "Dynamic Product Ads"]
    if intent == "informational":
        return ["Video", "Instant Experience"]
    return ["Single Image", "Carousel", "Video"]


def _meta_placements(trend: str) -> list:
    if trend in ["breakout", "rising"]:
        return ["Instagram Reels", "Facebook Feed", "Stories"]
    return ["Facebook Feed", "Instagram Feed"]


# ── TikTok Recommendations ────────────────────────────────────────────────────

def tiktok_recommendations(
    keyword: str,
    trend_label: str,
    momentum: float,
    geo: str,
) -> dict:
    """Generate TikTok ad and content strategy recommendations."""

    hooks = {
        "breakout":     "POV: [trend] is taking over — jump on this NOW",
        "rising":       "Why everyone is suddenly talking about [keyword]",
        "seasonal":     "It's that time of year — [keyword] season is here",
        "stable":       "The truth about [keyword] no one talks about",
        "falling":      "Is [keyword] actually worth it in 2025?",
    }

    formats = {
        "breakout":  ["TopView", "Branded Hashtag Challenge", "Spark Ads"],
        "rising":    ["In-Feed Ads", "Spark Ads", "Branded Mission"],
        "seasonal":  ["In-Feed Ads", "TopView"],
        "stable":    ["In-Feed Ads"],
        "falling":   ["Spark Ads from organic UGC only"],
    }

    hashtag_strategy = f"#{keyword.replace(' ', '')} + #trending + 2-3 niche hashtags"

    return {
        "platform":         "TikTok",
        "keyword":          keyword,
        "trend":            trend_label,
        "geo":              geo,
        "content_hook":     hooks.get(trend_label, "").replace("[keyword]", keyword).replace("[trend]", keyword),
        "video_length_sec": 15 if trend_label == "breakout" else 30,
        "recommended_formats": formats.get(trend_label, ["In-Feed Ads"]),
        "hashtag_strategy": hashtag_strategy,
        "sound_strategy":   "Use trending audio from TikTok Creative Center",
        "min_momentum_to_activate": 50,
        "activate":         momentum >= 50,
    }


# ── YouTube Recommendations ───────────────────────────────────────────────────

def youtube_recommendations(
    keyword: str,
    trend_label: str,
    momentum: float,
    geo: str,
) -> dict:
    """Generate YouTube ad targeting and content recommendations."""

    intent = classify_intent(keyword)

    content_categories = {
        "commercial":    "Shopping / Product Review channels",
        "informational": "Educational / How-To channels",
        "entertainment": "Entertainment / Vlog channels",
        "research":      "Comparison / Review channels",
        "transactional": "Tutorial / Walkthrough channels",
    }

    ad_formats = {
        "breakout":  ["Masthead", "Non-skippable 15s", "Bumper 6s"],
        "rising":    ["Skippable in-stream", "Non-skippable 15s"],
        "seasonal":  ["Skippable in-stream", "Discovery ads"],
        "stable":    ["Skippable in-stream", "Discovery ads"],
        "falling":   ["Bumper 6s retargeting only"],
    }

    return {
        "platform":          "YouTube",
        "keyword":           keyword,
        "intent":            intent,
        "trend":             trend_label,
        "geo":               geo,
        "target_channels":   content_categories.get(intent, "General interest"),
        "ad_formats":        ad_formats.get(trend_label, ["Skippable in-stream"]),
        "audience_targeting":["Custom Intent", "In-Market", "YouTube Search history"],
        "video_hook_seconds":5,          # must hook before skip button
        "companion_banner":  True,
        "frequency_cap":     "3 per week per user",
    }


# ── Unified Recommendations ───────────────────────────────────────────────────

def full_platform_brief(
    keyword: str,
    trend_label: str,
    momentum: float,
    geo: str,
    related_rising: Optional[list] = None,
    related_topics: Optional[list] = None,
) -> dict:
    """
    Generate a complete cross-platform ad brief for one keyword + geo.

    Returns dict with recommendations for all 4 platforms.
    """
    return {
        "keyword":   keyword,
        "geo":       geo,
        "trend":     trend_label,
        "momentum":  momentum,
        "platforms": {
            "google_ads": google_ads_recommendations(keyword, trend_label, momentum, geo, related_rising),
            "meta":       meta_recommendations(keyword, trend_label, momentum, geo, related_topics),
            "tiktok":     tiktok_recommendations(keyword, trend_label, momentum, geo),
            "youtube":    youtube_recommendations(keyword, trend_label, momentum, geo),
        }
    }

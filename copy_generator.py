# copy_generator.py
"""
GPT-powered ad copy generation.

Takes a full_platform_brief() dict and generates platform-specific ad copy
using the OpenAI Chat Completions API.

Usage:
    from copy_generator import generate_ad_copy, is_configured

    if is_configured(config):
        copy = generate_ad_copy(brief["platforms"]["google_ads"], "google_ads", config)
        # copy = {"headline": ..., "body": ..., "cta": ..., "hashtags": [...]}
"""

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_PLATFORM_INSTRUCTIONS = {
    "google_ads": (
        "You are an expert Google Ads copywriter. "
        "Write a responsive search ad with a punchy headline (max 30 chars), "
        "a compelling description body (max 90 chars), and a strong CTA phrase (max 15 chars). "
        "Focus on the search intent and keyword relevance."
    ),
    "meta": (
        "You are an expert Meta (Facebook/Instagram) ad copywriter. "
        "Write an engaging feed ad with a hook headline (max 40 chars), "
        "a concise body (max 125 chars), and a CTA (max 20 chars). "
        "Match the creative direction provided."
    ),
    "tiktok": (
        "You are an expert TikTok content strategist and ad copywriter. "
        "Write a TikTok in-feed ad script hook (max 50 chars), "
        "a short body caption (max 100 chars), a CTA (max 20 chars), "
        "and 5 relevant hashtags as a JSON list. "
        "Make it feel native, energetic, and trend-aware."
    ),
    "youtube": (
        "You are an expert YouTube ads copywriter. "
        "Write a 5-second unskippable hook (max 60 chars), "
        "a body message (max 120 chars), and a CTA overlay (max 20 chars). "
        "Focus on visual storytelling cues."
    ),
}

_USER_PROMPT_TEMPLATE = """
Generate ad copy for the following trend brief:

Platform: {platform}
Keyword: {keyword}
Geo: {geo}
Trend Signal: {trend}
Momentum Score: {momentum}
Additional context: {context}

Respond in valid JSON with exactly these keys:
  "headline" (string),
  "body"     (string),
  "cta"      (string),
  "hashtags" (list of strings, empty list if not applicable).

Return ONLY the JSON object, no markdown fences, no explanation.
"""


def is_configured(config: dict) -> bool:
    """Return True if an OpenAI API key is present in config."""
    return bool(config.get("openai", {}).get("api_key", "").strip())


def generate_ad_copy(
    platform_brief: dict,
    platform: str,
    config: dict,
) -> Optional[dict]:
    """
    Generate ad copy for a single platform using GPT.

    Parameters
    ----------
    platform_brief : The platform-specific dict from full_platform_brief()["platforms"][platform].
    platform       : One of "google_ads" | "meta" | "tiktok" | "youtube".
    config         : The loaded config.yaml dict (needs config["openai"]).

    Returns a dict with keys: headline, body, cta, hashtags.
    Returns None if OpenAI is not configured or the API call fails.
    """
    openai_cfg = config.get("openai", {})
    api_key    = openai_cfg.get("api_key", "").strip()
    if not api_key:
        logger.warning("OpenAI API key not configured — skipping ad copy generation")
        return None

    try:
        from openai import OpenAI  # lazy import — only needed when this feature is used
    except ImportError:
        logger.error("openai package not installed. Run: pip install openai")
        return None

    client = OpenAI(api_key=api_key)
    model  = openai_cfg.get("model", "gpt-4o")
    max_tokens = int(openai_cfg.get("max_tokens", 512))

    # Build a concise context string from the brief
    context_parts = []
    for k in ("intent", "bid_adjustment", "urgency", "creative_direction",
              "audience_strategy", "content_hook", "hashtag_strategy"):
        val = platform_brief.get(k)
        if val:
            context_parts.append(f"{k}: {val}")
    context = "; ".join(context_parts) if context_parts else "N/A"

    system_msg = _PLATFORM_INSTRUCTIONS.get(platform, _PLATFORM_INSTRUCTIONS["google_ads"])
    user_msg   = _USER_PROMPT_TEMPLATE.format(
        platform=platform.replace("_", " ").title(),
        keyword=platform_brief.get("keyword", ""),
        geo=platform_brief.get("geo", ""),
        trend=platform_brief.get("trend", ""),
        momentum=platform_brief.get("momentum", 0),
        context=context,
    )

    try:
        response = client.chat.completions.create(
            model=model,
            max_tokens=max_tokens,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user",   "content": user_msg},
            ],
            temperature=0.7,
        )
        raw = response.choices[0].message.content.strip()
        copy = json.loads(raw)
        # Normalise: ensure all expected keys exist
        copy.setdefault("headline", "")
        copy.setdefault("body",     "")
        copy.setdefault("cta",      "")
        copy.setdefault("hashtags", [])
        logger.info(f"Ad copy generated | platform={platform} | keyword={platform_brief.get('keyword')}")
        return copy
    except json.JSONDecodeError as e:
        logger.error(f"GPT returned non-JSON response: {e}")
        return None
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return None

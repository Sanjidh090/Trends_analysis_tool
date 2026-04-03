# signals/signal_processor.py
"""
Trend signal processing layer.

Computes:
  - Momentum score    : rate of change over last N days
  - Breakout detection: sudden spike detection (z-score based)
  - Seasonality       : STL decomposition to detect cyclical patterns
  - Cross-correlation : which keywords move together
  - Trend category    : Rising / Falling / Stable / Breakout / Seasonal
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# ── Momentum ──────────────────────────────────────────────────────────────────

def compute_momentum(series: pd.Series, window: int = 7) -> float:
    """
    Momentum = slope of linear regression over last `window` data points.
    Positive = rising trend. Normalized to [-100, 100].
    """
    if len(series) < window:
        return 0.0
    y = series.iloc[-window:].values.astype(float)
    x = np.arange(len(y))
    slope, _, r_value, _, _ = stats.linregress(x, y)
    # Normalize: slope per point, weighted by R² fit quality
    momentum = slope * r_value ** 2 * 10
    return float(np.clip(momentum, -100, 100))


def momentum_all(df: pd.DataFrame, window: int = 7) -> pd.Series:
    """Apply momentum to all keyword columns in a DataFrame."""
    return df.apply(lambda col: compute_momentum(col, window))


# ── Breakout Detection ────────────────────────────────────────────────────────

def detect_breakout(series: pd.Series, threshold: float = 2.0) -> dict:
    """
    Z-score based breakout: is the latest point an outlier vs history?

    Returns:
        {
          "is_breakout": bool,
          "z_score": float,
          "current_value": int,
          "historical_mean": float,
          "pct_above_mean": float,
        }
    """
    if len(series) < 4:
        return {"is_breakout": False, "z_score": 0.0}

    history  = series.iloc[:-1]
    current  = series.iloc[-1]
    mean     = history.mean()
    std      = history.std()

    z = (current - mean) / (std + 1e-9)
    pct_above = ((current - mean) / (mean + 1e-9)) * 100

    return {
        "is_breakout":      bool(z > threshold),
        "z_score":          round(float(z), 2),
        "current_value":    int(current),
        "historical_mean":  round(float(mean), 2),
        "pct_above_mean":   round(float(pct_above), 1),
    }


def breakout_report(df: pd.DataFrame, threshold: float = 2.0) -> pd.DataFrame:
    """Run breakout detection on all keywords in a DataFrame."""
    records = []
    for col in df.columns:
        result = detect_breakout(df[col], threshold)
        result["keyword"] = col
        records.append(result)
    return pd.DataFrame(records).set_index("keyword")


# ── Seasonality ───────────────────────────────────────────────────────────────

def detect_seasonality(series: pd.Series, period: int = 52) -> dict:
    """
    Simple seasonality check using autocorrelation at the seasonal lag.
    For weekly data: period=52 (annual), period=4 (quarterly).

    Returns strength score 0-1 and whether seasonal pattern is significant.
    """
    if len(series) < period * 2:
        return {"seasonal": False, "strength": 0.0, "period": period}

    acf_at_lag = series.autocorr(lag=period)
    acf_at_lag = 0.0 if np.isnan(acf_at_lag) else acf_at_lag

    return {
        "seasonal": bool(acf_at_lag > 0.4),
        "strength": round(float(acf_at_lag), 3),
        "period":   period,
    }


# ── Keyword Correlation ───────────────────────────────────────────────────────

def correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Pearson correlation matrix between all keyword time series."""
    return df.corr(method="pearson").round(3)


def find_correlated_pairs(df: pd.DataFrame, threshold: float = 0.75) -> list:
    """
    Return list of (kw1, kw2, correlation) tuples above threshold.
    Useful for building ad group clusters.
    """
    corr = correlation_matrix(df)
    pairs = []
    cols  = corr.columns.tolist()
    for i, c1 in enumerate(cols):
        for c2 in cols[i+1:]:
            r = corr.loc[c1, c2]
            if abs(r) >= threshold:
                pairs.append({"kw1": c1, "kw2": c2, "correlation": r})
    return sorted(pairs, key=lambda x: abs(x["correlation"]), reverse=True)


# ── Trend Classification ──────────────────────────────────────────────────────

TREND_LABELS = {
    "breakout":  "🚀 Breakout",
    "rising":    "📈 Rising",
    "stable":    "➡️  Stable",
    "falling":   "📉 Falling",
    "seasonal":  "🔄 Seasonal",
}

def classify_trend(series: pd.Series, geo: str = "GLOBAL") -> dict:
    """
    Full classification for a single keyword series.
    Combines momentum + breakout + seasonality into a single signal.
    """
    momentum   = compute_momentum(series)
    breakout   = detect_breakout(series)
    seasonality= detect_seasonality(series)

    if breakout["is_breakout"]:
        label = "breakout"
    elif seasonality["seasonal"]:
        label = "seasonal"
    elif momentum > 10:
        label = "rising"
    elif momentum < -10:
        label = "falling"
    else:
        label = "stable"

    return {
        "geo":          geo,
        "label":        label,
        "display":      TREND_LABELS[label],
        "momentum":     momentum,
        "z_score":      breakout["z_score"],
        "current":      breakout.get("current_value"),
        "pct_change":   breakout.get("pct_above_mean"),
        "seasonal":     seasonality["seasonal"],
        "seasonal_str": seasonality["strength"],
    }


def classify_all(df: pd.DataFrame, geo: str = "GLOBAL") -> pd.DataFrame:
    """Classify every keyword column in a DataFrame."""
    records = []
    for col in df.columns:
        rec = classify_trend(df[col], geo)
        rec["keyword"] = col
        records.append(rec)
    return pd.DataFrame(records).set_index("keyword")


# ── Competitor Share-of-Search ────────────────────────────────────────────────

def classify_share_shift(
    sos_df: pd.DataFrame,
    brand_keyword: str,
    window: int = 7,
) -> list:
    """
    Detect crossover events where a competitor's share surpasses the brand's share.

    Parameters
    ----------
    sos_df         : DataFrame of share-of-search percentages (date index, keyword columns).
    brand_keyword  : The column representing your brand.
    window         : Rolling window (rows) used to smooth before comparison.

    Returns a list of dicts, one per crossover event detected:
        {competitor, crossover_date, brand_share, competitor_share, direction}
    """
    if sos_df.empty or brand_keyword not in sos_df.columns:
        return []

    smoothed = sos_df.rolling(window=window, min_periods=1).mean()
    brand    = smoothed[brand_keyword]
    events   = []

    for competitor in smoothed.columns:
        if competitor == brand_keyword:
            continue
        comp = smoothed[competitor]
        # Detect sign changes: where competitor crosses brand
        diff = comp - brand
        for i in range(1, len(diff)):
            if diff.iloc[i - 1] < 0 and diff.iloc[i] >= 0:
                events.append({
                    "competitor":       competitor,
                    "crossover_date":   str(diff.index[i])[:10],
                    "brand_share":      round(float(brand.iloc[i]), 2),
                    "competitor_share": round(float(comp.iloc[i]), 2),
                    "direction":        "competitor_overtook_brand",
                })
            elif diff.iloc[i - 1] >= 0 and diff.iloc[i] < 0:
                events.append({
                    "competitor":       competitor,
                    "crossover_date":   str(diff.index[i])[:10],
                    "brand_share":      round(float(brand.iloc[i]), 2),
                    "competitor_share": round(float(comp.iloc[i]), 2),
                    "direction":        "brand_recaptured_lead",
                })

    return sorted(events, key=lambda e: e["crossover_date"], reverse=True)

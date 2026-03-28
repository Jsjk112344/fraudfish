"""Risk scoring engine: rank discovered events by fraud/scalping probability."""

from __future__ import annotations

import logging
import re

from agents.event_discovery import HIGH_RISK_CATEGORIES, KPOP_KEYWORDS, HIGH_DEMAND_KEYWORDS

logger = logging.getLogger(__name__)

# ---- Scoring weights (out of 100) ------------------------------------------

# Maximum contribution from each factor
W_SOLD_OUT = 25        # Sold out = highest scalping incentive
W_FACE_VALUE = 20      # Higher face value = more fraud incentive
W_CATEGORY = 15        # Concert/sports > theatre > other
W_DEMAND_KEYWORD = 15  # Known high-demand acts/events
W_KPOP = 10            # K-pop has disproportionate resale fraud in SG
W_POPULARITY_HINT = 10 # "Selling Fast", "Limited" etc.
W_DATE_PROXIMITY = 5   # Closer events = more active fraud window


def _parse_face_value_high(event: dict) -> float:
    """Extract highest face value, handling None/string/number."""
    for key in ("face_value_high", "face_value_low"):
        val = event.get(key)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                continue
    return 0.0


def _popularity_hint_score(hint: str | None) -> float:
    """Score the popularity hint text (0.0 to 1.0)."""
    if not hint:
        return 0.0
    hint_lower = hint.lower()
    if any(kw in hint_lower for kw in ("sold out", "soldout")):
        return 1.0
    if any(kw in hint_lower for kw in ("selling fast", "limited", "few left", "hot")):
        return 0.7
    if any(kw in hint_lower for kw in ("popular", "trending", "recommended")):
        return 0.4
    return 0.1  # Has some hint but not strong


def _date_proximity_score(date_str: str | None) -> float:
    """Score how close the event date is (closer = higher fraud risk).

    Returns 0.0 to 1.0.  Rough parsing — we just need a signal, not precision.
    """
    if not date_str:
        return 0.3  # Unknown date gets moderate score

    date_lower = date_str.lower()

    # Check for month mentions to estimate proximity
    months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december",
    ]
    month_short = [
        "jan", "feb", "mar", "apr", "may", "jun",
        "jul", "aug", "sep", "oct", "nov", "dec",
    ]

    for i, (full, short) in enumerate(zip(months, month_short)):
        if full in date_lower or short in date_lower:
            # Rough check: is it within ~2 months?
            # We don't need to be exact, just need a signal
            from datetime import datetime
            current_month = datetime.now().month
            event_month = i + 1
            # Handle year wrap
            diff = event_month - current_month
            if diff < 0:
                diff += 12
            if diff <= 1:
                return 1.0  # This month or next
            if diff <= 3:
                return 0.7
            if diff <= 6:
                return 0.4
            return 0.2

    return 0.3  # Can't parse, moderate score


def score_event(event: dict) -> float:
    """Compute a fraud risk score (0-100) for a discovered event.

    Higher score = higher probability of scalping/fraud activity.
    """
    score = 0.0
    name_lower = (event.get("event_name") or "").lower()
    category = (event.get("category") or "other").lower()

    # 1. Sold out status (strongest signal)
    sold_out = event.get("sold_out")
    if sold_out is True:
        score += W_SOLD_OUT
    elif sold_out is None:
        score += W_SOLD_OUT * 0.3  # Unknown = some risk

    # 2. Face value (higher = more incentive for fraud)
    face_value = _parse_face_value_high(event)
    if face_value >= 500:
        score += W_FACE_VALUE
    elif face_value >= 300:
        score += W_FACE_VALUE * 0.8
    elif face_value >= 150:
        score += W_FACE_VALUE * 0.5
    elif face_value >= 50:
        score += W_FACE_VALUE * 0.2

    # 3. Event category
    if category in HIGH_RISK_CATEGORIES:
        score += W_CATEGORY
    elif category in ("festival", "theatre"):
        score += W_CATEGORY * 0.4

    # 4. High-demand keyword match
    if any(kw in name_lower for kw in HIGH_DEMAND_KEYWORDS):
        score += W_DEMAND_KEYWORD

    # 5. K-pop (disproportionate resale fraud in SG)
    if any(kw in name_lower for kw in KPOP_KEYWORDS):
        score += W_KPOP

    # 6. Popularity hint
    score += W_POPULARITY_HINT * _popularity_hint_score(event.get("popularity_hint"))

    # 7. Date proximity
    score += W_DATE_PROXIMITY * _date_proximity_score(event.get("date"))

    return round(min(score, 100.0), 1)


def rank_events(events: list[dict]) -> list[dict]:
    """Score and rank events by fraud risk (highest first).

    Mutates each event dict by adding 'risk_score' and 'risk_level' keys.
    """
    for ev in events:
        ev["risk_score"] = score_event(ev)
        if ev["risk_score"] >= 70:
            ev["risk_level"] = "CRITICAL"
        elif ev["risk_score"] >= 50:
            ev["risk_level"] = "HIGH"
        elif ev["risk_score"] >= 30:
            ev["risk_level"] = "MODERATE"
        else:
            ev["risk_level"] = "LOW"

    events.sort(key=lambda e: e["risk_score"], reverse=True)
    return events

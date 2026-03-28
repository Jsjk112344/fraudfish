"""Deterministic rules engine for obvious fraud/scalping classification.

Evaluates pricing rules against evidence and returns a VerdictResult
if a rule matches, or None to fall through to LLM classification.
"""

from typing import Optional

from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity
from classify.config import RULES_CONFIG


def evaluate_rules(evidence: dict) -> Optional[VerdictResult]:
    """Evaluate deterministic rules. Returns VerdictResult if a rule matches, None otherwise."""
    listing = evidence.get("listing", {})
    event = evidence.get("event", {})

    price = listing.get("price")
    face_value = event.get("face_value")
    sold_out = event.get("sold_out", False)

    # Guard: need both price and face_value for pricing rules
    if not price or not face_value or face_value <= 0:
        return None

    price_ratio = price / face_value

    # Rule 1: Extreme underpricing
    threshold = RULES_CONFIG["extreme_underpricing"]["threshold"]
    if price_ratio < threshold:
        return _build_verdict(
            category=ClassificationCategory.LIKELY_SCAM,
            confidence=RULES_CONFIG["extreme_underpricing"]["confidence"],
            reasoning=(
                f"Listed at S${price:.0f} -- {(1 - price_ratio) * 100:.0f}% below face value "
                f"of S${face_value:.0f}. Extreme underpricing on "
                f"{'a sold-out' if sold_out else 'an available'} event is a strong scam indicator."
            ),
            evidence=evidence,
            primary_signal="Pricing Anomaly",
            primary_severity=SignalSeverity.CRITICAL,
        )

    # Rule 2: Extreme markup (context-aware threshold)
    markup_key = "extreme_markup_soldout" if sold_out else "extreme_markup_available"
    markup_threshold = RULES_CONFIG[markup_key]["threshold"]
    if price_ratio > markup_threshold:
        return _build_verdict(
            category=ClassificationCategory.SCALPING_VIOLATION,
            confidence=RULES_CONFIG[markup_key]["confidence"],
            reasoning=(
                f"Listed at S${price:.0f} -- {(price_ratio - 1) * 100:.0f}% above face value "
                f"of S${face_value:.0f}. Excessive markup "
                f"{'despite event still available' if not sold_out else 'on sold-out event'} "
                f"indicates scalping."
            ),
            evidence=evidence,
            primary_signal="Pricing Anomaly",
            primary_severity=SignalSeverity.CRITICAL,
        )

    return None  # No rule matched -- fall through to LLM


def _build_verdict(
    category: ClassificationCategory,
    confidence: float,
    reasoning: str,
    evidence: dict,
    primary_signal: str,
    primary_severity: SignalSeverity,
) -> VerdictResult:
    """Build a full VerdictResult with all 5 signals populated."""
    cross_platform = evidence.get("cross_platform", {})
    event = evidence.get("event", {})

    # Determine secondary signal scores from evidence
    duplicates_found = cross_platform.get("duplicates_found")
    if duplicates_found is True:
        cross_severity = SignalSeverity.WARNING
        cross_segments = 3
    elif duplicates_found is False:
        cross_severity = SignalSeverity.CLEAR
        cross_segments = 1
    else:
        cross_severity = SignalSeverity.NEUTRAL
        cross_segments = 2

    event_has_data = bool(event.get("name") or event.get("face_value"))
    event_severity = SignalSeverity.CLEAR if event_has_data else SignalSeverity.NEUTRAL
    event_segments = 1 if event_has_data else 2

    signals = [
        Signal(name="Pricing Anomaly", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
        Signal(name="Seller Reputation", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
        Signal(name="Event Verification", severity=event_severity, segmentsFilled=event_segments),
        Signal(name="Cross-Platform Duplicates", severity=cross_severity, segmentsFilled=cross_segments),
        Signal(name="Listing Authenticity", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
    ]

    # Override the primary signal
    signals = _update_signal(signals, primary_signal, primary_severity, 5)

    return VerdictResult(
        category=category,
        confidence=confidence,
        reasoning=reasoning,
        signals=signals,
    )


def _update_signal(
    signals: list[Signal], name: str, severity: SignalSeverity, segments: int
) -> list[Signal]:
    """Return new list with the named signal replaced."""
    return [
        Signal(name=s.name, severity=severity, segmentsFilled=segments)
        if s.name == name
        else s
        for s in signals
    ]

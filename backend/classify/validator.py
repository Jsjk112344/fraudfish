"""Post-classification validation layer.

Catches contradictions between verdict and evidence, calibrates
unreasonable confidence scores, and appends visible override notes
to reasoning. Runs after both rules engine and LLM paths.
"""

from typing import List

from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity
from classify.config import RULES_CONFIG


def validate_verdict(verdict: VerdictResult, evidence: dict) -> VerdictResult:
    """Validate verdict against evidence. Returns corrected VerdictResult."""
    overrides: List[str] = []
    updates = {}

    # --- Contradiction check 1: LEGITIMATE + extreme underpricing ---
    listing = evidence.get("listing", {})
    event = evidence.get("event", {})

    price = listing.get("price")
    face_value = event.get("face_value")

    if price and face_value and face_value > 0:
        price_ratio = price / face_value
        underpricing_threshold = RULES_CONFIG["extreme_underpricing"]["threshold"]

        if verdict.category == ClassificationCategory.LEGITIMATE and price_ratio < underpricing_threshold:
            updates["category"] = ClassificationCategory.LIKELY_SCAM
            updates["confidence"] = max(verdict.confidence, 85.0)
            updates["signals"] = _update_signal(
                verdict.signals, "Pricing Anomaly", SignalSeverity.CRITICAL, 5
            )
            overrides.append(
                "Overridden from LEGITIMATE to LIKELY_SCAM -- pricing anomaly contradicts classification"
            )

    # --- Contradiction check 2: LIKELY_SCAM + verified long-standing seller ---
    seller = evidence.get("seller", {})
    account_age_days = seller.get("account_age_days", 0)
    reviews_count = seller.get("reviews_count", 0)

    current_category = updates.get("category", verdict.category)
    current_confidence = updates.get("confidence", verdict.confidence)

    if (
        current_category == ClassificationCategory.LIKELY_SCAM
        and account_age_days > 365
        and reviews_count > 50
    ):
        new_confidence = max(current_confidence - 20, 40.0)
        updates["confidence"] = new_confidence
        overrides.append(
            "Confidence reduced -- verified long-standing seller contradicts LIKELY_SCAM classification"
        )

    # --- Confidence calibration ---
    current_confidence = updates.get("confidence", verdict.confidence)
    current_signals = updates.get("signals", verdict.signals)

    # High confidence + mixed signals -> cap at 82%
    if current_confidence > 95.0 and _has_mixed_signals(current_signals):
        overrides.append(
            f"Confidence calibrated from {current_confidence}% to 82% -- mixed signals"
        )
        updates["confidence"] = 82.0

    # Low confidence + all agreeing signals -> raise to 65%
    elif current_confidence < 40.0 and _all_signals_agree(current_signals):
        overrides.append(
            f"Confidence calibrated from {current_confidence}% to 65% -- consistent signals"
        )
        updates["confidence"] = 65.0

    # --- Apply overrides ---
    if overrides:
        current_reasoning = updates.get("reasoning", verdict.reasoning)
        override_text = " | Validation: " + "; ".join(overrides)
        updates["reasoning"] = current_reasoning + override_text

    if updates:
        return verdict.model_copy(update=updates)
    return verdict


def _has_mixed_signals(signals: List[Signal]) -> bool:
    """Returns True if signals contain both CRITICAL and CLEAR severities."""
    severities = {s.severity for s in signals}
    return SignalSeverity.CRITICAL in severities and SignalSeverity.CLEAR in severities


def _all_signals_agree(signals: List[Signal]) -> bool:
    """Returns True if all signals have the same severity direction."""
    if not signals:
        return False
    severities = {s.severity for s in signals}
    return len(severities) == 1


def _update_signal(
    signals: List[Signal], name: str, severity: SignalSeverity, segments: int
) -> List[Signal]:
    """Return new list with the named signal replaced."""
    return [
        Signal(name=s.name, severity=severity, segmentsFilled=segments)
        if s.name == name
        else s
        for s in signals
    ]

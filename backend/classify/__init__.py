"""Two-tier classification: rules engine -> LLM fallback -> validation."""

import logging
from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity
from classify.rules import evaluate_rules
from classify.llm import classify_with_llm
from classify.validator import validate_verdict

logger = logging.getLogger(__name__)


async def classify(evidence: dict) -> VerdictResult:
    """Classify a listing using two-tier approach.

    1. Rules engine for obvious cases (instant, no API call)
    2. LLM (gpt-4o) for ambiguous cases
    3. Validation on all results (contradiction checks, confidence calibration)

    Falls back to degraded verdict if LLM call fails.
    """
    # Tier 1: Rules engine
    verdict = evaluate_rules(evidence)

    if verdict is None:
        # Tier 2: LLM for ambiguous cases
        try:
            verdict = await classify_with_llm(evidence)
        except Exception as e:
            logger.warning(f"OpenAI classification failed: {e}. Falling back to degraded verdict.")
            verdict = _build_fallback_verdict(evidence)

    # Always validate
    verdict = validate_verdict(verdict, evidence)
    return verdict


def _build_fallback_verdict(evidence: dict) -> VerdictResult:
    """Degraded verdict when LLM is unavailable. Conservative classification."""
    return VerdictResult(
        category=ClassificationCategory.LIKELY_SCAM
            if _has_pricing_red_flags(evidence)
            else ClassificationCategory.LEGITIMATE,
        confidence=50.0,
        reasoning="Automated classification unavailable. Verdict based on limited rule evaluation only. Manual review recommended.",
        signals=[
            Signal(name="Pricing Anomaly", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
            Signal(name="Seller Reputation", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
            Signal(name="Event Verification", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
            Signal(name="Cross-Platform Duplicates", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
            Signal(name="Listing Authenticity", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
        ],
    )


def _has_pricing_red_flags(evidence: dict) -> bool:
    """Quick check for obvious pricing issues even without full rules evaluation."""
    try:
        price = evidence.get("listing", {}).get("price", 0)
        face_value = evidence.get("event", {}).get("face_value", 0)
        if price and face_value and face_value > 0:
            return (price / face_value) < 0.5
    except (TypeError, ZeroDivisionError):
        pass
    return False

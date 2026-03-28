"""Unit tests for the post-classification validation layer."""

import pytest

from classify.validator import validate_verdict
from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity


SIGNAL_NAMES = [
    "Pricing Anomaly",
    "Seller Reputation",
    "Event Verification",
    "Cross-Platform Duplicates",
    "Listing Authenticity",
]


def make_signals(
    pricing=SignalSeverity.NEUTRAL,
    seller=SignalSeverity.NEUTRAL,
    event=SignalSeverity.CLEAR,
    cross=SignalSeverity.NEUTRAL,
    auth=SignalSeverity.NEUTRAL,
):
    """Build default 5-signal list with customizable severities."""
    severity_map = {
        SignalSeverity.CRITICAL: 5,
        SignalSeverity.WARNING: 3,
        SignalSeverity.NEUTRAL: 2,
        SignalSeverity.CLEAR: 1,
    }
    severities = [pricing, seller, event, cross, auth]
    return [
        Signal(name=name, severity=sev, segmentsFilled=severity_map[sev])
        for name, sev in zip(SIGNAL_NAMES, severities)
    ]


def make_verdict(
    category=ClassificationCategory.LEGITIMATE,
    confidence=70.0,
    reasoning="Test reasoning",
    signals=None,
):
    """Build a VerdictResult for testing."""
    if signals is None:
        signals = make_signals()
    return VerdictResult(
        category=category,
        confidence=confidence,
        reasoning=reasoning,
        signals=signals,
    )


def make_evidence(
    price=550.0,
    face_value=698.0,
    sold_out=False,
    account_age_days=12,
    reviews_count=2,
):
    """Build evidence dict for testing."""
    return {
        "listing": {"price": price, "title": "Test", "seller_name": "test"},
        "seller": {
            "username": "test",
            "account_age_days": account_age_days,
            "reviews_count": reviews_count,
        },
        "event": {"name": "Test Event", "face_value": face_value, "sold_out": sold_out},
        "market": {"average_price": 580.0},
        "cross_platform": {"duplicates_found": False},
    }


class TestContradictionOverrides:
    def test_legitimate_with_extreme_underpricing_overridden(self):
        """LEGITIMATE + extreme underpricing -> overridden to LIKELY_SCAM."""
        verdict = make_verdict(
            category=ClassificationCategory.LEGITIMATE, confidence=70.0
        )
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = validate_verdict(verdict, evidence)
        assert result.category == ClassificationCategory.LIKELY_SCAM
        assert result.confidence >= 85.0
        assert "Overridden from LEGITIMATE to LIKELY_SCAM" in result.reasoning

    def test_override_updates_pricing_signal(self):
        """After override, Pricing Anomaly signal is CRITICAL with 5 segments."""
        verdict = make_verdict(
            category=ClassificationCategory.LEGITIMATE, confidence=70.0
        )
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = validate_verdict(verdict, evidence)
        pricing = next(s for s in result.signals if s.name == "Pricing Anomaly")
        assert pricing.severity == SignalSeverity.CRITICAL
        assert pricing.segmentsFilled == 5

    def test_valid_verdict_passes_unchanged(self):
        """LIKELY_SCAM + underpricing evidence -> no override needed."""
        verdict = make_verdict(
            category=ClassificationCategory.LIKELY_SCAM, confidence=90.0
        )
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = validate_verdict(verdict, evidence)
        assert result.category == ClassificationCategory.LIKELY_SCAM
        assert result.confidence == 90.0
        assert "Validation:" not in result.reasoning

    def test_scam_with_verified_seller_override(self):
        """LIKELY_SCAM + long-standing verified seller -> reduced confidence."""
        verdict = make_verdict(
            category=ClassificationCategory.LIKELY_SCAM, confidence=90.0
        )
        evidence = make_evidence(
            price=550.0, face_value=698.0, account_age_days=400, reviews_count=60
        )
        result = validate_verdict(verdict, evidence)
        assert result.confidence < 90.0
        assert "verified long-standing seller" in result.reasoning.lower()


class TestConfidenceCalibration:
    def test_high_confidence_with_mixed_signals_calibrated(self):
        """99% confidence with CRITICAL + CLEAR signals -> calibrated to 82%."""
        signals = make_signals(
            pricing=SignalSeverity.CRITICAL,
            event=SignalSeverity.CLEAR,
        )
        verdict = make_verdict(
            category=ClassificationCategory.LIKELY_SCAM,
            confidence=99.0,
            signals=signals,
        )
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = validate_verdict(verdict, evidence)
        assert result.confidence == 82.0
        assert "Confidence calibrated" in result.reasoning

    def test_low_confidence_with_agreeing_signals_calibrated(self):
        """30% confidence with all CRITICAL signals -> calibrated upward."""
        signals = make_signals(
            pricing=SignalSeverity.CRITICAL,
            seller=SignalSeverity.CRITICAL,
            event=SignalSeverity.CRITICAL,
            cross=SignalSeverity.CRITICAL,
            auth=SignalSeverity.CRITICAL,
        )
        verdict = make_verdict(
            category=ClassificationCategory.LIKELY_SCAM,
            confidence=30.0,
            signals=signals,
        )
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = validate_verdict(verdict, evidence)
        assert result.confidence > 30.0
        assert "calibrated" in result.reasoning.lower()


class TestOverrideNotes:
    def test_override_note_appended_to_reasoning(self):
        """Override appends Validation: note to original reasoning."""
        verdict = make_verdict(
            category=ClassificationCategory.LEGITIMATE,
            confidence=70.0,
            reasoning="Some text",
        )
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = validate_verdict(verdict, evidence)
        assert result.reasoning.startswith("Some text")
        assert "Validation:" in result.reasoning


class TestGracefulHandling:
    def test_no_evidence_data_gracefully_handled(self):
        """Missing evidence keys don't crash the validator."""
        verdict = make_verdict()
        result = validate_verdict(verdict, {})
        assert result is not None

    def test_empty_listing_gracefully_handled(self):
        """Empty listing dict doesn't crash."""
        verdict = make_verdict()
        result = validate_verdict(verdict, {"listing": {}, "seller": {}})
        assert result is not None

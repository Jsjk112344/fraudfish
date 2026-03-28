"""Unit tests for the deterministic rules engine."""

import pytest

from classify.rules import evaluate_rules
from models.enums import ClassificationCategory, SignalSeverity


SIGNAL_NAMES = [
    "Pricing Anomaly",
    "Seller Reputation",
    "Event Verification",
    "Cross-Platform Duplicates",
    "Listing Authenticity",
]


def make_evidence(
    price=550.0,
    face_value=698.0,
    sold_out=False,
    duplicates_found=False,
    account_age_days=12,
    reviews_count=2,
):
    """Build a minimal evidence dict for testing."""
    evidence = {
        "listing": {"price": price, "title": "Test Listing", "seller_name": "test_seller"},
        "seller": {
            "username": "test_seller",
            "account_age_days": account_age_days,
            "reviews_count": reviews_count,
        },
        "event": {
            "name": "Test Event",
            "face_value": face_value,
            "sold_out": sold_out,
        },
        "market": {"average_price": 580.0, "listings_scanned": 24},
        "cross_platform": {"duplicates_found": duplicates_found},
    }
    return evidence


class TestExtremeUnderpricing:
    def test_extreme_underpricing(self):
        """Price at 21.5% of face value -> LIKELY_SCAM."""
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = evaluate_rules(evidence)
        assert result is not None
        assert result.category == ClassificationCategory.LIKELY_SCAM
        assert result.confidence == 92.0
        assert "below face value" in result.reasoning.lower() or "below" in result.reasoning.lower()

    def test_underpricing_verdict_pricing_signal_is_critical(self):
        """Underpricing verdict has Pricing Anomaly signal as CRITICAL with 5 segments."""
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = evaluate_rules(evidence)
        assert result is not None
        pricing_signal = next(s for s in result.signals if s.name == "Pricing Anomaly")
        assert pricing_signal.severity == SignalSeverity.CRITICAL
        assert pricing_signal.segmentsFilled == 5


class TestExtremeMarkup:
    def test_extreme_markup_available(self):
        """Markup 3.58x on available event -> SCALPING_VIOLATION."""
        evidence = make_evidence(price=2500.0, face_value=698.0, sold_out=False)
        result = evaluate_rules(evidence)
        assert result is not None
        assert result.category == ClassificationCategory.SCALPING_VIOLATION
        assert result.confidence == 88.0

    def test_extreme_markup_soldout_below_threshold(self):
        """Markup 3.58x on sold-out event (< 5.0 threshold) -> None."""
        evidence = make_evidence(price=2500.0, face_value=698.0, sold_out=True)
        result = evaluate_rules(evidence)
        assert result is None

    def test_extreme_markup_soldout_above_threshold(self):
        """Markup 5.73x on sold-out event (> 5.0 threshold) -> SCALPING_VIOLATION."""
        evidence = make_evidence(price=4000.0, face_value=698.0, sold_out=True)
        result = evaluate_rules(evidence)
        assert result is not None
        assert result.category == ClassificationCategory.SCALPING_VIOLATION
        assert result.confidence == 75.0


class TestNoRuleMatches:
    def test_normal_price_returns_none(self):
        """Normal price ratio 0.788 -> None (fall through to LLM)."""
        evidence = make_evidence(price=550.0, face_value=698.0)
        result = evaluate_rules(evidence)
        assert result is None

    def test_missing_price_returns_none(self):
        """Missing price -> None (guard clause)."""
        evidence = make_evidence(price=None, face_value=698.0)
        result = evaluate_rules(evidence)
        assert result is None

    def test_missing_face_value_returns_none(self):
        """Missing face_value -> None (guard clause)."""
        evidence = make_evidence(price=550.0, face_value=None)
        result = evaluate_rules(evidence)
        assert result is None

    def test_zero_face_value_returns_none(self):
        """Zero face_value -> None (guard clause, avoid division by zero)."""
        evidence = make_evidence(price=550.0, face_value=0)
        result = evaluate_rules(evidence)
        assert result is None


class TestVerdictSignals:
    def test_rules_verdict_has_all_five_signals(self):
        """Any matched verdict has exactly 5 signals with correct names."""
        evidence = make_evidence(price=150.0, face_value=698.0)
        result = evaluate_rules(evidence)
        assert result is not None
        assert len(result.signals) == 5
        signal_names = [s.name for s in result.signals]
        assert signal_names == SIGNAL_NAMES

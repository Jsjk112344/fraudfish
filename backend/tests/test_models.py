"""Tests for Pydantic models and enums."""

from models.enums import ClassificationCategory, StepStatus, SignalSeverity
from models.events import InvestigationEvent, InvestigateRequest, VerdictResult, Signal


class TestClassificationCategory:
    def test_has_exactly_four_members(self):
        members = list(ClassificationCategory)
        assert len(members) == 4

    def test_member_names(self):
        assert ClassificationCategory.LEGITIMATE == "LEGITIMATE"
        assert ClassificationCategory.SCALPING_VIOLATION == "SCALPING_VIOLATION"
        assert ClassificationCategory.LIKELY_SCAM == "LIKELY_SCAM"
        assert ClassificationCategory.COUNTERFEIT_RISK == "COUNTERFEIT_RISK"


class TestStepStatus:
    def test_has_exactly_four_members(self):
        members = list(StepStatus)
        assert len(members) == 4

    def test_member_values(self):
        assert StepStatus.PENDING == "pending"
        assert StepStatus.ACTIVE == "active"
        assert StepStatus.COMPLETE == "complete"
        assert StepStatus.ERROR == "error"


class TestSignalSeverity:
    def test_has_exactly_four_members(self):
        members = list(SignalSeverity)
        assert len(members) == 4

    def test_member_values(self):
        assert SignalSeverity.CRITICAL == "CRITICAL"
        assert SignalSeverity.WARNING == "WARNING"
        assert SignalSeverity.NEUTRAL == "NEUTRAL"
        assert SignalSeverity.CLEAR == "CLEAR"


class TestInvestigationEvent:
    def test_validates_with_required_fields(self):
        event = InvestigationEvent(
            step="extract_listing",
            status=StepStatus.ACTIVE,
        )
        assert event.step == "extract_listing"
        assert event.status == StepStatus.ACTIVE
        assert event.data is None

    def test_validates_with_data(self):
        event = InvestigationEvent(
            step="extract_listing",
            status=StepStatus.COMPLETE,
            data={"title": "F1 Singapore GP", "price": 150.0},
        )
        assert event.data is not None
        assert event.data["title"] == "F1 Singapore GP"


class TestVerdictResult:
    def test_validates_with_all_fields(self):
        verdict = VerdictResult(
            category=ClassificationCategory.LIKELY_SCAM,
            confidence=94.2,
            reasoning="Test reasoning",
            signals=[
                Signal(name="Pricing Anomaly", severity=SignalSeverity.CRITICAL, segmentsFilled=5),
            ],
        )
        assert verdict.category == ClassificationCategory.LIKELY_SCAM
        assert verdict.confidence == 94.2
        assert verdict.reasoning == "Test reasoning"
        assert len(verdict.signals) == 1
        assert verdict.signals[0].name == "Pricing Anomaly"
        assert verdict.signals[0].severity == SignalSeverity.CRITICAL
        assert verdict.signals[0].segmentsFilled == 5


class TestInvestigateRequest:
    def test_validates_url_string(self):
        request = InvestigateRequest(url="https://carousell.sg/listing/123")
        assert request.url == "https://carousell.sg/listing/123"

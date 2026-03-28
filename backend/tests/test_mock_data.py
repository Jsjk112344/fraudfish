"""Tests for mock data completeness and correctness."""

from mock.data import MOCK_STEPS, MOCK_VERDICT, STEP_DEFINITIONS
from models.enums import ClassificationCategory


EXPECTED_STEP_IDS = [
    "extract_listing",
    "investigate_seller",
    "verify_event",
    "check_market",
    "cross_platform",
    "synthesize",
]


class TestMockSteps:
    def test_contains_all_six_step_ids(self):
        for step_id in EXPECTED_STEP_IDS:
            assert step_id in MOCK_STEPS, f"Missing step: {step_id}"

    def test_each_step_data_is_non_empty(self):
        for step_id in EXPECTED_STEP_IDS:
            data = MOCK_STEPS[step_id]
            assert isinstance(data, dict), f"{step_id} data should be a dict"
            assert len(data) > 0, f"{step_id} data should not be empty"

    def test_extract_listing_has_expected_fields(self):
        listing = MOCK_STEPS["extract_listing"]
        assert "title" in listing
        assert "price" in listing
        assert "seller_name" in listing

    def test_investigate_seller_has_expected_fields(self):
        seller = MOCK_STEPS["investigate_seller"]
        assert "username" in seller
        assert "account_age_days" in seller
        assert "reviews_count" in seller

    def test_verify_event_has_expected_fields(self):
        event = MOCK_STEPS["verify_event"]
        assert "name" in event
        assert "face_value" in event
        assert "sold_out" in event

    def test_check_market_has_expected_fields(self):
        market = MOCK_STEPS["check_market"]
        assert "listings_scanned" in market
        assert "average_price" in market

    def test_cross_platform_has_expected_fields(self):
        cross = MOCK_STEPS["cross_platform"]
        assert "duplicates_found" in cross

    def test_synthesize_has_expected_fields(self):
        synth = MOCK_STEPS["synthesize"]
        assert "signals" in synth or "reasoning" in synth


class TestMockVerdict:
    def test_category_is_likely_scam(self):
        assert MOCK_VERDICT.category == ClassificationCategory.LIKELY_SCAM

    def test_confidence_between_90_and_100(self):
        assert 90 <= MOCK_VERDICT.confidence <= 100

    def test_confidence_is_94_2(self):
        assert MOCK_VERDICT.confidence == 94.2

    def test_reasoning_is_non_empty(self):
        assert len(MOCK_VERDICT.reasoning) > 0

    def test_has_exactly_5_signals(self):
        assert len(MOCK_VERDICT.signals) == 5


class TestStepDefinitions:
    def test_has_6_entries(self):
        assert len(STEP_DEFINITIONS) == 6

    def test_each_has_required_keys(self):
        for step in STEP_DEFINITIONS:
            assert "id" in step
            assert "label" in step
            assert "icon" in step
            assert "mock_delay" in step

    def test_ids_match_expected(self):
        ids = [s["id"] for s in STEP_DEFINITIONS]
        assert ids == EXPECTED_STEP_IDS

"""Mock investigation data for F1 Singapore Grand Prix LIKELY_SCAM scenario."""

import json
import os

from models.enums import ClassificationCategory, SignalSeverity
from models.events import Signal, VerdictResult

# Load mock data from JSON
_DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "f1-gp-mock.json")
with open(_DATA_PATH) as f:
    _raw = json.load(f)

# Step ID -> mock data dict for that step
def get_mock_data(step_id: str) -> dict:
    """Get mock data for a given investigation step.

    Provides cached fallback data when live TinyFish calls fail.
    """
    return MOCK_STEPS.get(step_id, {})


MOCK_STEPS: dict[str, dict] = {
    "extract_listing": _raw["listing"],
    "investigate_seller": _raw["seller"],
    "verify_event": _raw["event"],
    "check_market": _raw["market"],
    "cross_platform": _raw["cross_platform"],
    "synthesize": {
        "signals": _raw["verdict"]["signals"],
        "reasoning": _raw["verdict"]["reasoning"],
    },
}

# Full verdict as a Pydantic model
MOCK_VERDICT = VerdictResult(
    category=ClassificationCategory(_raw["verdict"]["category"]),
    confidence=_raw["verdict"]["confidence"],
    reasoning=_raw["verdict"]["reasoning"],
    signals=[
        Signal(
            name=s["name"],
            severity=SignalSeverity(s["severity"]),
            segmentsFilled=s["segmentsFilled"],
        )
        for s in _raw["verdict"]["signals"]
    ],
)

# Step definitions with labels, icons, and mock delays
STEP_DEFINITIONS: list[dict] = [
    {"id": "extract_listing", "label": "Extracting Listing Data", "icon": "page_info", "mock_delay": 2.5},
    {"id": "investigate_seller", "label": "Investigating Seller Profile", "icon": "person_search", "mock_delay": 3.0},
    {"id": "verify_event", "label": "Verifying Event Details", "icon": "event_available", "mock_delay": 2.0},
    {"id": "check_market", "label": "Checking Market Rates", "icon": "analytics", "mock_delay": 3.5},
    {"id": "cross_platform", "label": "Cross-Platform Search", "icon": "travel_explore", "mock_delay": 2.5},
    {"id": "synthesize", "label": "Synthesizing Verdict", "icon": "psychology", "mock_delay": 2.0},
]

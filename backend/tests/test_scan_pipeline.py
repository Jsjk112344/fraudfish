"""Tests for the batch event scan pipeline."""

import pytest
from unittest.mock import AsyncMock, patch

from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity


def _make_verdict(category: ClassificationCategory, confidence: float = 80.0) -> VerdictResult:
    """Helper to create a VerdictResult with given category."""
    return VerdictResult(
        category=category,
        confidence=confidence,
        reasoning="Test reasoning",
        signals=[
            Signal(name="Test Signal", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
        ],
    )


def _make_listings(count: int, platform: str = "Carousell") -> list[dict]:
    """Helper to create fake listing dicts."""
    return [
        {"title": f"Ticket {i}", "price": 100 + i * 50, "seller": f"seller_{i}"}
        for i in range(count)
    ]


@pytest.mark.asyncio
@patch("agents.scan_pipeline.scan_viagogo_market", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_carousell_market", new_callable=AsyncMock)
async def test_discovery(mock_carousell, mock_viagogo):
    """Discovery phase collects listings from both platforms."""
    mock_carousell.return_value = _make_listings(3)
    mock_viagogo.return_value = _make_listings(2)

    from agents.scan_pipeline import run_event_scan

    events = []
    with patch("agents.scan_pipeline.classify", new_callable=AsyncMock) as mock_classify:
        mock_classify.return_value = _make_verdict(ClassificationCategory.LEGITIMATE)
        async for event in run_event_scan("F1 Singapore GP"):
            events.append(event)

    event_types = [e["event"] for e in events]
    assert event_types[0] == "scan_started"
    assert "listings_found" in event_types

    listings_found = next(e for e in events if e["event"] == "listings_found")
    assert listings_found["data"]["total_count"] == 5
    assert listings_found["data"]["by_platform"]["Carousell"] == 3
    assert listings_found["data"]["by_platform"]["Viagogo"] == 2


@pytest.mark.asyncio
@patch("agents.scan_pipeline.classify", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_viagogo_market", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_carousell_market", new_callable=AsyncMock)
async def test_per_listing(mock_carousell, mock_viagogo, mock_classify):
    """Each listing gets a verdict event."""
    mock_carousell.return_value = [{"title": "Ticket A", "price": 100, "seller": "alice"}]
    mock_viagogo.return_value = [{"title": "Ticket B", "price": 200, "seller": "bob"}]
    mock_classify.return_value = _make_verdict(ClassificationCategory.LEGITIMATE)

    from agents.scan_pipeline import run_event_scan

    events = []
    async for event in run_event_scan("Test Event"):
        events.append(event)

    verdict_events = [e for e in events if e["event"] == "listing_verdict"]
    assert len(verdict_events) == 2

    for ve in verdict_events:
        assert "category" in ve["data"]["verdict"]
        assert "confidence" in ve["data"]["verdict"]


@pytest.mark.asyncio
@patch("agents.scan_pipeline.classify", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_viagogo_market", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_carousell_market", new_callable=AsyncMock)
async def test_stats_aggregation(mock_carousell, mock_viagogo, mock_classify):
    """Stats correctly aggregate flagged, confirmed_scams, fraud_exposure."""
    # All listings same price so order doesn't matter with as_completed
    mock_carousell.return_value = [
        {"title": "Scam Ticket", "price": 100, "seller": "scammer"},
        {"title": "Good Ticket", "price": 100, "seller": "legit"},
    ]
    mock_viagogo.return_value = [
        {"title": "Scalper Ticket", "price": 100, "seller": "scalper"},
    ]

    # Return different categories for each call
    mock_classify.side_effect = [
        _make_verdict(ClassificationCategory.LIKELY_SCAM),
        _make_verdict(ClassificationCategory.LEGITIMATE),
        _make_verdict(ClassificationCategory.SCALPING_VIOLATION),
    ]

    from agents.scan_pipeline import run_event_scan

    events = []
    async for event in run_event_scan("Test Event"):
        events.append(event)

    # Get the final scan_stats (last one before scan_complete)
    stats_events = [e for e in events if e["event"] == "scan_stats"]
    assert len(stats_events) >= 1

    final_stats = stats_events[-1]["data"]
    assert final_stats["total_listings"] == 3
    assert final_stats["investigated"] == 3
    assert final_stats["flagged"] == 2  # LIKELY_SCAM + SCALPING_VIOLATION
    assert final_stats["confirmed_scams"] == 1  # LIKELY_SCAM only
    assert final_stats["fraud_exposure"] == 100.0  # Only LIKELY_SCAM price


@pytest.mark.asyncio
@patch("agents.scan_pipeline.scan_viagogo_market", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_carousell_market", new_callable=AsyncMock)
async def test_empty_discovery(mock_carousell, mock_viagogo):
    """Empty discovery yields scan_complete with zero stats."""
    mock_carousell.return_value = []
    mock_viagogo.return_value = []

    from agents.scan_pipeline import run_event_scan

    events = []
    async for event in run_event_scan("Nonexistent Event"):
        events.append(event)

    event_types = [e["event"] for e in events]
    assert "scan_complete" in event_types

    complete = next(e for e in events if e["event"] == "scan_complete")
    assert complete["data"]["final_stats"]["total_listings"] == 0
    assert complete["data"]["final_stats"]["investigated"] == 0
    assert complete["data"]["final_stats"]["fraud_exposure"] == 0.0


@pytest.mark.asyncio
@patch("agents.scan_pipeline.classify", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_viagogo_market", new_callable=AsyncMock)
@patch("agents.scan_pipeline.scan_carousell_market", new_callable=AsyncMock)
async def test_max_listings_cap(mock_carousell, mock_viagogo, mock_classify):
    """Total listings capped at MAX_LISTINGS (12)."""
    mock_carousell.return_value = _make_listings(15)
    mock_viagogo.return_value = _make_listings(5)
    mock_classify.return_value = _make_verdict(ClassificationCategory.LEGITIMATE)

    from agents.scan_pipeline import run_event_scan

    events = []
    async for event in run_event_scan("Big Event"):
        events.append(event)

    listings_found = next(e for e in events if e["event"] == "listings_found")
    assert listings_found["data"]["total_count"] <= 12

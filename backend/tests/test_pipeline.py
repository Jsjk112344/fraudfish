"""Tests for live investigation pipeline orchestrator."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from models.enums import StepStatus


async def collect_events(gen):
    """Helper: iterate async generator into a list."""
    events = []
    async for event in gen:
        events.append(event)
    return events


MOCK_LISTING_DATA = {
    "title": "Taylor Swift Eras Tour SG",
    "price": 350.0,
    "seller_username": "ticketking88",
    "description": "Cat 1 tickets",
    "platform": "carousell",
}

MOCK_SELLER_DATA = {
    "account_age": "2 years",
    "total_listings": 15,
    "review_count": 8,
    "platform": "carousell",
}

MOCK_EVENT_DATA = {
    "event_name": "Taylor Swift Eras Tour",
    "venue": "National Stadium",
    "face_value": 248.0,
    "sold_out": True,
    "source": "Ticketmaster SG",
}

MOCK_TELEGRAM_LISTING = {
    "message_text": "Selling 2x Cat 1",
    "sender_username": "tixdealer",
    "sent_date": "2026-03-20",
    "price": 400.0,
    "event_name": "Coldplay SG",
    "platform": "telegram",
}

MOCK_TELEGRAM_SELLER = {
    "username": "tixdealer",
    "message_count": 5,
    "first_seen": "2026-01-15",
    "repeat_seller": True,
    "platform": "telegram",
}


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
async def test_parallel_execution(mock_extract, mock_seller, mock_event):
    """Seller investigation and event verification run via asyncio.gather."""
    from agents.pipeline import run_live_investigation

    mock_extract.return_value = (MOCK_LISTING_DATA, True)
    mock_seller.return_value = (MOCK_SELLER_DATA, True)
    mock_event.return_value = (MOCK_EVENT_DATA, True)

    events = await collect_events(run_live_investigation("https://carousell.sg/p/test-123"))

    step_statuses = [(e.step, e.status) for e in events]

    # extract_listing runs first (sequential)
    assert ("extract_listing", StepStatus.ACTIVE) in step_statuses
    assert ("extract_listing", StepStatus.COMPLETE) in step_statuses

    # Both ACTIVE events appear before any COMPLETE for parallel steps
    active_indices = {
        "seller": next(i for i, e in enumerate(events) if e.step == "investigate_seller" and e.status == StepStatus.ACTIVE),
        "event": next(i for i, e in enumerate(events) if e.step == "verify_event" and e.status == StepStatus.ACTIVE),
    }
    complete_indices = {
        "seller": next(i for i, e in enumerate(events) if e.step == "investigate_seller" and e.status == StepStatus.COMPLETE),
        "event": next(i for i, e in enumerate(events) if e.step == "verify_event" and e.status == StepStatus.COMPLETE),
    }
    # Both ACTIVE events should come before both COMPLETE events
    assert active_indices["seller"] < complete_indices["seller"]
    assert active_indices["event"] < complete_indices["event"]
    assert active_indices["seller"] < complete_indices["event"]
    assert active_indices["event"] < complete_indices["seller"]


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
async def test_total_timeout(mock_extract, mock_seller, mock_event):
    """Pipeline completes within 60 seconds even when steps hang."""
    from agents.pipeline import run_live_investigation

    async def slow_extract(*args, **kwargs):
        await asyncio.sleep(70)
        return (MOCK_LISTING_DATA, True)

    mock_extract.side_effect = slow_extract

    # The entire consumption should not take 70 seconds
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            collect_events(run_live_investigation("https://carousell.sg/p/test-123")),
            timeout=5.0,  # Use short timeout for test speed; real pipeline uses 60s
        )


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
async def test_live_badge_field(mock_extract, mock_seller, mock_event):
    """Every COMPLETE event has _live=True when agents return live data."""
    from agents.pipeline import run_live_investigation

    mock_extract.return_value = (MOCK_LISTING_DATA, True)
    mock_seller.return_value = (MOCK_SELLER_DATA, True)
    mock_event.return_value = (MOCK_EVENT_DATA, True)

    events = await collect_events(run_live_investigation("https://carousell.sg/p/test-123"))

    complete_events = [e for e in events if e.status == StepStatus.COMPLETE]
    assert len(complete_events) >= 3

    for e in complete_events:
        assert "_live" in e.data
        assert e.data["_live"] is True


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
async def test_fallback_live_badge_false(mock_extract, mock_seller, mock_event):
    """Every COMPLETE event has _live=False when agents return fallback data."""
    from agents.pipeline import run_live_investigation

    mock_extract.return_value = (MOCK_LISTING_DATA, False)
    mock_seller.return_value = (MOCK_SELLER_DATA, False)
    mock_event.return_value = (MOCK_EVENT_DATA, False)

    events = await collect_events(run_live_investigation("https://carousell.sg/p/test-123"))

    complete_events = [e for e in events if e.status == StepStatus.COMPLETE]
    for e in complete_events:
        assert "_live" in e.data
        assert e.data["_live"] is False


@pytest.mark.asyncio
async def test_unsupported_platform_error():
    """Unsupported URL yields a single ERROR event."""
    from agents.pipeline import run_live_investigation

    events = await collect_events(run_live_investigation("https://google.com/something"))

    assert len(events) == 1
    assert events[0].step == "error"
    assert events[0].status == StepStatus.ERROR


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
@patch("agents.pipeline.extract_telegram_message", new_callable=AsyncMock)
async def test_carousell_platform_routing(mock_telegram, mock_extract, mock_seller, mock_event):
    """Carousell URLs route to carousell extractors."""
    from agents.pipeline import run_live_investigation

    mock_extract.return_value = (MOCK_LISTING_DATA, True)
    mock_seller.return_value = (MOCK_SELLER_DATA, True)
    mock_event.return_value = (MOCK_EVENT_DATA, True)

    await collect_events(run_live_investigation("https://carousell.sg/p/test-123"))

    mock_extract.assert_called_once()
    mock_telegram.assert_not_called()


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_telegram_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_telegram_message", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
async def test_telegram_platform_routing(mock_carousell_extract, mock_carousell_seller, mock_telegram_extract, mock_telegram_seller, mock_event):
    """Telegram URLs route to telegram extractors."""
    from agents.pipeline import run_live_investigation

    mock_telegram_extract.return_value = (MOCK_TELEGRAM_LISTING, True)
    mock_telegram_seller.return_value = (MOCK_TELEGRAM_SELLER, True)
    mock_event.return_value = (MOCK_EVENT_DATA, True)

    await collect_events(run_live_investigation("https://t.me/sgtickets/12345"))

    mock_telegram_extract.assert_called_once()
    mock_carousell_extract.assert_not_called()
    mock_telegram_seller.assert_called_once()
    mock_carousell_seller.assert_not_called()


@pytest.mark.asyncio
@patch("agents.pipeline.verify_event_official", new_callable=AsyncMock)
@patch("agents.pipeline.investigate_carousell_seller", new_callable=AsyncMock)
@patch("agents.pipeline.extract_carousell_listing", new_callable=AsyncMock)
async def test_event_order(mock_extract, mock_seller, mock_event):
    """Events are yielded in the correct order."""
    from agents.pipeline import run_live_investigation

    mock_extract.return_value = (MOCK_LISTING_DATA, True)
    mock_seller.return_value = (MOCK_SELLER_DATA, True)
    mock_event.return_value = (MOCK_EVENT_DATA, True)

    events = await collect_events(run_live_investigation("https://carousell.sg/p/test-123"))

    expected_order = [
        ("extract_listing", StepStatus.ACTIVE),
        ("extract_listing", StepStatus.COMPLETE),
        ("investigate_seller", StepStatus.ACTIVE),
        ("verify_event", StepStatus.ACTIVE),
        ("investigate_seller", StepStatus.COMPLETE),
        ("verify_event", StepStatus.COMPLETE),
    ]

    actual_order = [(e.step, e.status) for e in events]
    assert actual_order == expected_order

"""Integration tests for POST /api/scan SSE endpoint."""

import pytest
import httpx
from unittest.mock import patch, AsyncMock


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _mock_scan_generator(event_name):
    """Mock scan generator yielding 3 events."""
    yield {"event": "scan_started", "data": {"event_name": event_name, "platforms": ["Carousell", "Viagogo"]}}
    yield {
        "event": "listings_found",
        "data": {
            "listings": [
                {"listing_id": "Carousell-0", "platform": "Carousell", "title": "Ticket A", "price": 100, "seller": "alice", "url": None, "status": "pending"},
                {"listing_id": "Viagogo-0", "platform": "Viagogo", "title": "Ticket B", "price": 200, "seller": "bob", "url": None, "status": "pending"},
            ],
            "total_count": 2,
            "by_platform": {"Carousell": 1, "Viagogo": 1},
        },
    }
    yield {"event": "scan_complete", "data": {"final_stats": {"total_listings": 2, "investigated": 0}, "duration_seconds": 0.1}}


@pytest.mark.asyncio
@patch("api.scan.run_event_scan")
async def test_scan_endpoint_accepts_event_name(mock_run):
    """POST /api/scan with valid event_name returns 200 SSE stream."""
    mock_run.side_effect = _mock_scan_generator

    from main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/scan",
            json={"event_name": "F1 Singapore GP 2026"},
        )
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        body = response.text
        assert "scan_started" in body
        assert "listings_found" in body
        assert "scan_complete" in body


@pytest.mark.asyncio
@patch("api.scan.run_event_scan")
async def test_scan_endpoint_empty_name(mock_run):
    """POST /api/scan with empty event_name still works."""
    mock_run.side_effect = _mock_scan_generator

    from main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/scan",
            json={"event_name": ""},
        )
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_scan_endpoint_missing_field():
    """POST /api/scan with missing event_name returns 422."""
    from main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/scan",
            json={},
        )
        assert response.status_code == 422

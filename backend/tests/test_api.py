"""Tests for mock pipeline and SSE endpoint."""

import asyncio
import json

import pytest
import httpx

from models.enums import StepStatus


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.asyncio
async def test_mock_pipeline_yields_13_events():
    """run_mock_investigation yields 6 ACTIVE + 6 COMPLETE + 1 verdict = 13 events."""
    from mock.pipeline import run_mock_investigation

    # Monkeypatch sleep to be instant
    original_sleep = asyncio.sleep
    asyncio.sleep = lambda _: original_sleep(0)

    try:
        events = []
        async for event in run_mock_investigation("https://carousell.sg/p/test-123"):
            events.append(event)

        assert len(events) == 13
    finally:
        asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_mock_pipeline_event_order():
    """Each step yields ACTIVE then COMPLETE, ending with verdict COMPLETE."""
    from mock.pipeline import run_mock_investigation

    original_sleep = asyncio.sleep
    asyncio.sleep = lambda _: original_sleep(0)

    try:
        events = []
        async for event in run_mock_investigation("https://carousell.sg/p/test-123"):
            events.append(event)

        # First 12 events: pairs of ACTIVE/COMPLETE for 6 steps
        for i in range(6):
            active_event = events[i * 2]
            complete_event = events[i * 2 + 1]
            assert active_event.status == StepStatus.ACTIVE
            assert active_event.data is None
            assert complete_event.status == StepStatus.COMPLETE
            assert complete_event.data is not None

        # Last event is verdict
        assert events[12].step == "verdict"
        assert events[12].status == StepStatus.COMPLETE
    finally:
        asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_investigate_endpoint_returns_sse():
    """POST /api/investigate with valid URL returns 200 with text/event-stream."""
    from main import app

    # Monkeypatch sleep to be instant
    original_sleep = asyncio.sleep
    asyncio.sleep = lambda _: original_sleep(0)

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/investigate",
                json={"url": "https://carousell.sg/p/test-123"},
            )
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
    finally:
        asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_investigate_endpoint_invalid_url():
    """POST /api/investigate with invalid URL returns error event."""
    from main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/investigate",
            json={"url": "not-a-url"},
        )
        assert response.status_code == 200
        body = response.text
        assert "error" in body
        assert "Could not detect listing" in body


@pytest.mark.asyncio
async def test_sse_stream_contains_investigation_events():
    """SSE stream contains investigation step events from live pipeline."""
    from main import app

    original_sleep = asyncio.sleep
    asyncio.sleep = lambda _: original_sleep(0)

    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.post(
                "/api/investigate",
                json={"url": "https://carousell.sg/p/test-123"},
            )
            body = response.text

            # Parse SSE data lines
            data_lines = [
                line.removeprefix("data: ")
                for line in body.split("\n")
                if line.startswith("data: ")
            ]

            parsed_events = []
            for line in data_lines:
                try:
                    parsed = json.loads(line)
                    if "step" in parsed:
                        parsed_events.append(parsed)
                except (json.JSONDecodeError, TypeError):
                    continue

            # Live pipeline produces 6 events: 3 steps x (ACTIVE + COMPLETE)
            assert len(parsed_events) >= 6

            # Should have extract_listing, investigate_seller, verify_event steps
            step_names = {e["step"] for e in parsed_events}
            assert "extract_listing" in step_names
            assert "investigate_seller" in step_names
            assert "verify_event" in step_names

            # COMPLETE events should have _live field
            complete_events = [e for e in parsed_events if e.get("status") == "complete"]
            assert len(complete_events) >= 3
            for e in complete_events:
                assert "_live" in e.get("data", {})
    finally:
        asyncio.sleep = original_sleep


@pytest.mark.asyncio
async def test_health_endpoint():
    """GET /api/health returns 200 with status ok."""
    from main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/api/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

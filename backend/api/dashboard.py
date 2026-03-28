"""Dashboard API: event discovery + batch threat scanning."""

import json

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from models.events import DashboardScanRequest
from agents.dashboard_pipeline import run_dashboard_discovery, run_dashboard_scan

router = APIRouter()


@router.post("/api/dashboard/discover")
async def discover_events(request: Request):
    """Discover high-profile events and rank by fraud risk. Returns SSE stream."""
    pipeline = run_dashboard_discovery()

    async def event_generator():
        async for event in pipeline:
            if await request.is_disconnected():
                break
            yield {
                "event": event["event"],
                "data": json.dumps(event["data"]),
            }

    return EventSourceResponse(event_generator())


@router.post("/api/dashboard/scan")
async def dashboard_scan(request: Request, body: DashboardScanRequest):
    """Scan pre-ranked events for fraudulent listings. Returns SSE stream."""
    events = [ev.model_dump() for ev in body.events]
    pipeline = run_dashboard_scan(events)

    async def event_generator():
        async for event in pipeline:
            if await request.is_disconnected():
                break
            yield {
                "event": event["event"],
                "data": json.dumps(event["data"]),
            }

    return EventSourceResponse(event_generator())

"""POST /api/scan SSE endpoint for batch event scanning."""

import json

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from models.events import ScanRequest
from agents.scan_pipeline import run_event_scan

router = APIRouter()


@router.post("/api/scan")
async def scan_event(request: Request, body: ScanRequest):
    pipeline = run_event_scan(body.event_name)

    async def event_generator():
        async for event in pipeline:
            if await request.is_disconnected():
                break
            yield {
                "event": event["event"],
                "data": json.dumps(event["data"]),
            }

    return EventSourceResponse(event_generator())

"""POST /api/investigate SSE endpoint."""

import json
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from models.events import InvestigateRequest

try:
    from agents.pipeline import run_live_investigation
except ImportError:
    run_live_investigation = None

from mock.pipeline import run_mock_investigation

router = APIRouter()


def validate_url(url: str) -> bool:
    """Basic URL validation: must have http(s) scheme and a domain."""
    try:
        result = urlparse(url)
        return all([result.scheme in ("http", "https"), result.netloc])
    except Exception:
        return False


@router.post("/api/investigate")
async def investigate(request: Request, body: InvestigateRequest):
    if not validate_url(body.url):

        async def error_generator():
            yield {
                "event": "error",
                "data": json.dumps(
                    {
                        "error": "Could not detect listing. Please paste a valid marketplace URL."
                    }
                ),
            }

        return EventSourceResponse(error_generator())

    # Use live pipeline if available, fall back to mock
    pipeline = run_live_investigation(body.url) if run_live_investigation else run_mock_investigation(body.url)

    async def event_generator():
        async for event in pipeline:
            if await request.is_disconnected():
                break
            yield {
                "event": event.step,
                "data": json.dumps(event.model_dump()),
            }

    return EventSourceResponse(event_generator())

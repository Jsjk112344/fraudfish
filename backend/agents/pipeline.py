"""Live investigation pipeline orchestrator with parallel fan-out."""

import asyncio
from typing import AsyncGenerator

from agents.platform_detect import detect_platform
from agents.carousell import extract_carousell_listing
from agents.telegram import extract_telegram_message
from agents.seller import investigate_carousell_seller
from agents.seller_telegram import investigate_telegram_seller
from agents.official_price import verify_event_official
from models.events import InvestigationEvent
from models.enums import StepStatus

TOTAL_TIMEOUT = 60.0  # seconds for entire investigation
STEP_TIMEOUT = 15.0   # seconds per TinyFish step


async def run_live_investigation(url: str) -> AsyncGenerator[InvestigationEvent, None]:
    """Live investigation pipeline replacing mock pipeline.
    Same AsyncGenerator interface -- SSE endpoint doesn't change.

    Flow:
    1. Detect platform from URL
    2. Extract listing data (sequential -- provides seller_username and event context)
    3. Fan out: seller investigation + event verification in parallel via asyncio.gather
    4. Each step yields ACTIVE then COMPLETE events with _live flag
    """
    platform = detect_platform(url)
    if platform is None:
        yield InvestigationEvent(
            step="error",
            status=StepStatus.ERROR,
            data={"message": "Unsupported platform. Paste a Carousell (carousell.sg) or Telegram (t.me) listing URL."},
        )
        return

    # Wrap entire investigation in total timeout
    try:
        async for event in _run_pipeline(url, platform):
            yield event
    except asyncio.TimeoutError:
        yield InvestigationEvent(
            step="timeout",
            status=StepStatus.ERROR,
            data={"message": "Investigation timed out after 60 seconds"},
        )


async def _run_pipeline(url: str, platform: str) -> AsyncGenerator[InvestigationEvent, None]:
    """Inner pipeline with the actual investigation steps."""

    # Step 1: Extract listing (sequential -- provides context for parallel steps)
    yield InvestigationEvent(step="extract_listing", status=StepStatus.ACTIVE, data=None)

    if platform == "carousell":
        listing_data, listing_live = await extract_carousell_listing(url, timeout=STEP_TIMEOUT)
    else:  # telegram
        listing_data, listing_live = await extract_telegram_message(url, timeout=STEP_TIMEOUT)

    yield InvestigationEvent(
        step="extract_listing",
        status=StepStatus.COMPLETE,
        data={**listing_data, "_live": listing_live},
    )

    # Extract context for parallel steps
    seller_username = listing_data.get("seller_username") or listing_data.get("sender_username") or "unknown"
    event_name = listing_data.get("title") or listing_data.get("event_name") or "Unknown Event"

    # Steps 2 & 3: Fan out in parallel
    yield InvestigationEvent(step="investigate_seller", status=StepStatus.ACTIVE, data=None)
    yield InvestigationEvent(step="verify_event", status=StepStatus.ACTIVE, data=None)

    if platform == "carousell":
        seller_coro = investigate_carousell_seller(seller_username, timeout=STEP_TIMEOUT)
    else:  # telegram
        seller_coro = investigate_telegram_seller(url, seller_username, timeout=STEP_TIMEOUT)

    event_coro = verify_event_official(event_name, timeout=STEP_TIMEOUT)

    # asyncio.gather runs both in parallel
    seller_result, event_result = await asyncio.gather(seller_coro, event_coro)

    seller_data, seller_live = seller_result
    event_data, event_live = event_result

    yield InvestigationEvent(
        step="investigate_seller",
        status=StepStatus.COMPLETE,
        data={**seller_data, "_live": seller_live},
    )
    yield InvestigationEvent(
        step="verify_event",
        status=StepStatus.COMPLETE,
        data={**event_data, "_live": event_live},
    )

"""Live investigation pipeline orchestrator with parallel fan-out."""

import asyncio
from typing import AsyncGenerator

from agents.platform_detect import detect_platform
from agents.carousell import extract_carousell_listing
from agents.telegram import extract_telegram_message
from agents.seller import investigate_carousell_seller
from agents.seller_telegram import investigate_telegram_seller
from agents.official_price import verify_event_official
from agents.market_scan import check_market_with_fallback
from agents.cross_platform import cross_platform_with_fallback
from classify import classify
from models.events import InvestigationEvent
from models.enums import StepStatus

TOTAL_TIMEOUT = 600.0  # 10 minutes — agents take time on Cloudflare sites
STEP_TIMEOUT = 300.0   # 5 minutes per step — no artificial cutoff


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

    # Steps 4 & 5: Market rate check + cross-platform search in parallel
    yield InvestigationEvent(step="check_market", status=StepStatus.ACTIVE, data=None)
    yield InvestigationEvent(step="cross_platform", status=StepStatus.ACTIVE, data=None)

    xplat_input = {
        "title": listing_data.get("title", ""),
        "seller_name": seller_username,
        "platform": platform.capitalize(),
    }

    market_result, xplat_result = await asyncio.gather(
        check_market_with_fallback(listing_data),
        cross_platform_with_fallback(xplat_input),
    )

    market_data, market_live = market_result
    xplat_data, xplat_live = xplat_result

    yield InvestigationEvent(
        step="check_market",
        status=StepStatus.COMPLETE,
        data={**market_data, "_live": market_live},
    )
    yield InvestigationEvent(
        step="cross_platform",
        status=StepStatus.COMPLETE,
        data={**xplat_data, "_live": xplat_live},
    )

    # Step 6: Synthesize verdict from all evidence
    yield InvestigationEvent(step="synthesize", status=StepStatus.ACTIVE, data=None)

    try:
        price_raw = listing_data.get("price", 0)
        price = float(price_raw) if price_raw else 0.0
    except (TypeError, ValueError):
        price = 0.0

    try:
        face_raw = event_data.get("face_value_low") or event_data.get("face_value") or 0
        face_value = float(face_raw) if face_raw else 0.0
    except (TypeError, ValueError):
        face_value = 0.0

    evidence = {
        "listing": {
            "title": listing_data.get("title", ""),
            "price": price,
            "seller_username": seller_username,
            "platform": platform,
            "description": listing_data.get("description", ""),
        },
        "event": {
            "name": event_data.get("event_name") or event_data.get("name", ""),
            "face_value": face_value,
            "sold_out": event_data.get("sold_out", False),
        },
        "seller": {
            "account_age": seller_data.get("account_age"),
            "total_listings": seller_data.get("total_listings"),
            "overall_rating": seller_data.get("overall_rating"),
            "review_sentiment": seller_data.get("review_sentiment"),
        },
        "market": market_data,
        "cross_platform": xplat_data,
    }

    verdict = await classify(evidence)
    verdict_dump = verdict.model_dump()

    yield InvestigationEvent(
        step="synthesize",
        status=StepStatus.COMPLETE,
        data={"signals": verdict_dump.get("signals", []), "reasoning": verdict_dump.get("reasoning", "")},
    )

    # Final verdict event
    yield InvestigationEvent(
        step="verdict",
        status=StepStatus.COMPLETE,
        data=verdict_dump,
    )

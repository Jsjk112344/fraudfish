"""Batch event scan pipeline: discover listings -> investigate -> classify -> aggregate."""

import asyncio
import logging
import time
from typing import AsyncGenerator

from agents.market_scan import scan_carousell_market, scan_viagogo_market
from classify import classify
from models.events import ScanStats

logger = logging.getLogger(__name__)

MAX_LISTINGS = 12  # Cap total listings for demo timing
CONCURRENCY = 3    # Max concurrent TinyFish sessions


def _assign_ids(listings: list[dict], platform: str) -> list[dict]:
    """Tag each listing with a unique ID and platform."""
    for i, listing in enumerate(listings):
        listing["listing_id"] = f"{platform}-{i}"
        listing["platform"] = platform
    return listings


async def run_event_scan(event_name: str) -> AsyncGenerator[dict, None]:
    """Orchestrate batch scan: discover -> investigate -> aggregate.

    SSE event types yielded:
    - scan_started: {event_name, platforms}
    - listings_found: {listings, total_count, by_platform}
    - listing_update: {listing_id, status, data}
    - listing_verdict: {listing_id, verdict, listing_summary}
    - scan_stats: {total_listings, investigated, flagged, confirmed_scams, fraud_exposure, by_platform, by_category}
    - scan_complete: {final_stats, duration_seconds}
    """
    start_time = time.time()

    yield {"event": "scan_started", "data": {"event_name": event_name, "platforms": ["Carousell", "Viagogo"]}}

    # Phase 1: Discovery -- reuse existing market_scan functions
    try:
        carousell_listings, viagogo_listings = await asyncio.gather(
            scan_carousell_market(event_name, "tickets"),
            scan_viagogo_market(event_name),
        )
    except Exception as e:
        logger.warning("Discovery failed: %s, using empty results", e)
        carousell_listings, viagogo_listings = [], []

    carousell_listings = _assign_ids(carousell_listings, "Carousell")
    viagogo_listings = _assign_ids(viagogo_listings, "Viagogo")
    all_listings = (carousell_listings + viagogo_listings)[:MAX_LISTINGS]

    by_platform = {
        "Carousell": len([l for l in all_listings if l["platform"] == "Carousell"]),
        "Viagogo": len([l for l in all_listings if l["platform"] == "Viagogo"]),
    }

    yield {
        "event": "listings_found",
        "data": {
            "listings": [
                {
                    "listing_id": l["listing_id"],
                    "platform": l["platform"],
                    "title": l.get("title", "Unknown"),
                    "price": float(l.get("price", 0)),
                    "seller": l.get("seller", "Unknown"),
                    "url": l.get("url"),
                    "status": "pending",
                }
                for l in all_listings
            ],
            "total_count": len(all_listings),
            "by_platform": by_platform,
        },
    }

    if not all_listings:
        yield {"event": "scan_complete", "data": {
            "final_stats": ScanStats(by_platform=by_platform).model_dump(),
            "duration_seconds": round(time.time() - start_time, 1),
        }}
        return

    # Phase 2: Investigate with bounded concurrency
    stats = ScanStats(total_listings=len(all_listings), by_platform=by_platform)
    results_lock = asyncio.Lock()

    sem = asyncio.Semaphore(CONCURRENCY)

    async def investigate_one(listing: dict):
        listing_id = listing["listing_id"]
        async with sem:
            yield_events = []
            try:
                yield_events.append({
                    "event": "listing_update",
                    "data": {"listing_id": listing_id, "status": "investigating"},
                })

                # Build evidence from discovery data for rules-based classification
                price = float(listing.get("price", 0))
                evidence = {
                    "listing": {
                        "title": listing.get("title", ""),
                        "price": price,
                        "seller_username": listing.get("seller", ""),
                        "platform": listing.get("platform", ""),
                    },
                    "event": {},
                    "seller": {},
                }

                verdict = await classify(evidence)

                # Update stats
                async with results_lock:
                    stats.investigated += 1
                    cat = verdict.category.value if hasattr(verdict.category, 'value') else str(verdict.category)
                    stats.by_category[cat] = stats.by_category.get(cat, 0) + 1
                    if cat in ("SCALPING_VIOLATION", "LIKELY_SCAM", "COUNTERFEIT_RISK"):
                        stats.flagged += 1
                    if cat in ("LIKELY_SCAM", "COUNTERFEIT_RISK"):
                        stats.confirmed_scams += 1
                        stats.fraud_exposure += price

                verdict_data = verdict.model_dump()
                # Convert enum values to strings for JSON serialization
                if hasattr(verdict_data.get("category"), "value"):
                    verdict_data["category"] = verdict_data["category"].value

                yield_events.append({
                    "event": "listing_verdict",
                    "data": {
                        "listing_id": listing_id,
                        "verdict": verdict_data,
                        "listing_summary": {
                            "title": listing.get("title", ""),
                            "price": price,
                            "seller": listing.get("seller", ""),
                            "platform": listing.get("platform", ""),
                        },
                    },
                })
                yield_events.append({
                    "event": "scan_stats",
                    "data": stats.model_dump(),
                })

            except Exception as e:
                logger.warning("Investigation failed for %s: %s", listing_id, e)
                yield_events.append({
                    "event": "listing_update",
                    "data": {"listing_id": listing_id, "status": "error", "error": str(e)},
                })
                async with results_lock:
                    stats.investigated += 1
                yield_events.append({
                    "event": "scan_stats",
                    "data": stats.model_dump(),
                })

            return yield_events

    # Run investigations with bounded concurrency, yielding events as they complete
    tasks = [investigate_one(l) for l in all_listings]
    for coro in asyncio.as_completed(tasks):
        events = await coro
        for ev in events:
            yield ev

    yield {"event": "scan_complete", "data": {
        "final_stats": stats.model_dump(),
        "duration_seconds": round(time.time() - start_time, 1),
    }}

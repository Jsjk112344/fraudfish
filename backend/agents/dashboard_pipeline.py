"""Dashboard pipeline: discover events -> rank by risk -> scan top threats."""

import asyncio
import logging
import time
from typing import AsyncGenerator

from agents.event_discovery import discover_events, discover_events_with_streaming
from agents.risk_scoring import rank_events
from agents.market_scan import scan_carousell_market, scan_viagogo_market
from classify import classify
from models.events import ScanStats

logger = logging.getLogger(__name__)

# Dashboard-specific limits
MAX_EVENTS_TO_SCAN = 6    # Top N riskiest events to auto-scan
MAX_LISTINGS_PER_EVENT = 8  # Cap per event for dashboard speed
CONCURRENCY = 3             # TinyFish session limit


async def run_dashboard_discovery() -> AsyncGenerator[dict, None]:
    """Phase 1: Discover events and rank by risk.

    SSE events yielded:
    - discovery_started: {sources}
    - discovery_progress: {message}
    - agent_streaming: {step, streaming_url}  — live browser preview from TinyFish
    - agent_progress: {step, message}          — agent narration from TinyFish
    - events_discovered: {events, total_count, is_live}
    - discovery_complete: {duration_seconds}
    """
    start_time = time.time()

    yield {"event": "discovery_started", "data": {
        "sources": ["SISTIC", "Ticketmaster SG"],
    }}

    yield {"event": "discovery_progress", "data": {
        "message": "Launching TinyFish agents to scan SISTIC and Ticketmaster SG...",
    }}

    # Use streaming-aware discovery to forward live browser previews
    events: list[dict] = []
    is_live = False

    async for ev in discover_events_with_streaming():
        if ev["event"] == "discovery_result":
            events = ev["data"]["events"]
            is_live = ev["data"]["is_live"]
        else:
            # Forward agent_streaming and agent_progress events directly
            yield ev

    yield {"event": "discovery_progress", "data": {
        "message": f"Found {len(events)} events, calculating risk scores...",
    }}

    ranked = rank_events(events)

    yield {"event": "events_discovered", "data": {
        "events": [
            {
                "event_id": f"ev-{i}",
                "event_name": ev.get("event_name", "Unknown"),
                "venue": ev.get("venue"),
                "date": ev.get("date"),
                "category": ev.get("category", "other"),
                "face_value_low": ev.get("face_value_low"),
                "face_value_high": ev.get("face_value_high"),
                "sold_out": ev.get("sold_out"),
                "popularity_hint": ev.get("popularity_hint"),
                "source": ev.get("source", "unknown"),
                "risk_score": ev.get("risk_score", 0),
                "risk_level": ev.get("risk_level", "LOW"),
            }
            for i, ev in enumerate(ranked)
        ],
        "total_count": len(ranked),
        "is_live": is_live,
    }}

    yield {"event": "discovery_complete", "data": {
        "duration_seconds": round(time.time() - start_time, 1),
    }}


async def run_dashboard_scan(events: list[dict]) -> AsyncGenerator[dict, None]:
    """Phase 2: Scan top-risk events for fraudulent listings.

    Accepts pre-ranked events (from discovery). Scans the top N.

    SSE events yielded:
    - dashboard_scan_started: {event_count, events}
    - event_scan_started: {event_id, event_name}
    - event_listings_found: {event_id, listings, count}
    - event_listing_verdict: {event_id, listing_id, verdict}
    - event_scan_complete: {event_id, stats}
    - dashboard_scan_complete: {aggregate_stats, duration_seconds}
    """
    start_time = time.time()
    top_events = events[:MAX_EVENTS_TO_SCAN]

    yield {"event": "dashboard_scan_started", "data": {
        "event_count": len(top_events),
        "events": [
            {"event_id": ev["event_id"], "event_name": ev["event_name"]}
            for ev in top_events
        ],
    }}

    # Aggregate stats across all events
    agg = {
        "total_events_scanned": 0,
        "total_listings": 0,
        "total_flagged": 0,
        "total_confirmed_scams": 0,
        "total_fraud_exposure": 0.0,
        "by_event": {},
    }

    sem = asyncio.Semaphore(CONCURRENCY)

    async def scan_one_event(ev: dict) -> list[dict]:
        """Scan a single event — discover listings then classify."""
        event_id = ev["event_id"]
        event_name = ev["event_name"]
        events_to_yield: list[dict] = []

        events_to_yield.append({"event": "event_scan_started", "data": {
            "event_id": event_id,
            "event_name": event_name,
        }})

        # Discover listings for this event
        async with sem:
            try:
                carousell, viagogo = await asyncio.gather(
                    scan_carousell_market(event_name, "tickets"),
                    scan_viagogo_market(event_name),
                )
            except Exception as e:
                logger.warning("Listing discovery failed for %s: %s", event_name, e)
                carousell, viagogo = [], []

        # Tag listings
        all_listings: list[dict] = []
        for i, l in enumerate(carousell):
            l["listing_id"] = f"{event_id}-car-{i}"
            l["platform"] = "Carousell"
            all_listings.append(l)
        for i, l in enumerate(viagogo):
            l["listing_id"] = f"{event_id}-via-{i}"
            l["platform"] = "Viagogo"
            all_listings.append(l)
        all_listings = all_listings[:MAX_LISTINGS_PER_EVENT]

        events_to_yield.append({"event": "event_listings_found", "data": {
            "event_id": event_id,
            "listings": [
                {
                    "listing_id": l["listing_id"],
                    "platform": l.get("platform", ""),
                    "title": l.get("title", "Unknown"),
                    "price": float(l.get("price", 0)),
                    "seller": l.get("seller", "Unknown"),
                }
                for l in all_listings
            ],
            "count": len(all_listings),
        }})

        # Classify each listing
        event_stats = ScanStats(
            total_listings=len(all_listings),
            by_platform={
                "Carousell": len([l for l in all_listings if l["platform"] == "Carousell"]),
                "Viagogo": len([l for l in all_listings if l["platform"] == "Viagogo"]),
            },
        )

        for listing in all_listings:
            async with sem:
                try:
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
                    verdict_data = verdict.model_dump()

                    cat = verdict.category.value if hasattr(verdict.category, "value") else str(verdict.category)
                    event_stats.investigated += 1
                    event_stats.by_category[cat] = event_stats.by_category.get(cat, 0) + 1
                    if cat in ("SCALPING_VIOLATION", "LIKELY_SCAM", "COUNTERFEIT_RISK"):
                        event_stats.flagged += 1
                    if cat in ("LIKELY_SCAM", "COUNTERFEIT_RISK"):
                        event_stats.confirmed_scams += 1
                        event_stats.fraud_exposure += price

                    events_to_yield.append({"event": "event_listing_verdict", "data": {
                        "event_id": event_id,
                        "listing_id": listing["listing_id"],
                        "verdict": verdict_data,
                        "listing_summary": {
                            "title": listing.get("title", ""),
                            "price": price,
                            "seller": listing.get("seller", ""),
                            "platform": listing.get("platform", ""),
                        },
                    }})
                except Exception as e:
                    logger.warning("Classification failed for %s: %s", listing.get("listing_id"), e)
                    event_stats.investigated += 1

        events_to_yield.append({"event": "event_scan_complete", "data": {
            "event_id": event_id,
            "stats": event_stats.model_dump(),
        }})

        return events_to_yield, event_stats

    # Scan events sequentially (each event does parallel listing discovery)
    for ev in top_events:
        event_results, event_stats = await scan_one_event(ev)
        for sse_ev in event_results:
            yield sse_ev

        # Accumulate aggregate
        agg["total_events_scanned"] += 1
        agg["total_listings"] += event_stats.total_listings
        agg["total_flagged"] += event_stats.flagged
        agg["total_confirmed_scams"] += event_stats.confirmed_scams
        agg["total_fraud_exposure"] += event_stats.fraud_exposure
        agg["by_event"][ev["event_id"]] = {
            "event_name": ev["event_name"],
            "risk_score": ev.get("risk_score", 0),
            "stats": event_stats.model_dump(),
        }

    yield {"event": "dashboard_scan_complete", "data": {
        "aggregate_stats": agg,
        "duration_seconds": round(time.time() - start_time, 1),
    }}

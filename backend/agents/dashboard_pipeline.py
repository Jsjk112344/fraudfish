"""Dashboard pipeline: discover events -> rank by risk -> scan top threats.

Uses two-phase parallel discovery:
  Phase 1: Quick agents find event links on SISTIC + Ticketmaster
  Scan:    Batch-fire all marketplace searches upfront, classify as results arrive

TinyFish limits: 2 concurrent sessions, 20 max pending.
We budget: 2 runs for discovery + up to 10 for scanning (5 events × 2 platforms).
"""

import asyncio
import logging
import time
from typing import AsyncGenerator
from urllib.parse import quote

from agents.event_discovery import discover_events_parallel, SEED_EVENTS
from agents.risk_scoring import rank_events
from agents.base import tinyfish_extract_batch, TinyFishResult
from classify import classify
from models.events import ScanStats

logger = logging.getLogger(__name__)

# Dashboard-specific limits — tuned for 2-concurrency / 20-pending TinyFish account
MAX_EVENTS_TO_SCAN = 5     # Top N riskiest events (5 × 2 platforms = 10 runs)
MAX_LISTINGS_PER_EVENT = 8  # Cap per event for demo speed


async def run_dashboard_discovery() -> AsyncGenerator[dict, None]:
    """Phase 1: Discover events and rank by risk.

    Uses two-phase parallel discovery — fast link scan, then batch detail extraction.
    Streaming URLs are captured from polling (thread-safe, no SSE needed).

    SSE events yielded:
    - discovery_started: {sources}
    - discovery_progress: {message}
    - agent_streaming: {step, streaming_url}  -- live browser preview from TinyFish
    - agent_progress: {step, message}          -- agent narration
    - events_discovered: {events, total_count, is_live}
    - discovery_complete: {duration_seconds}
    """
    start_time = time.time()

    yield {"event": "discovery_started", "data": {
        "sources": ["SISTIC", "Ticketmaster SG"],
    }}

    yield {"event": "discovery_progress", "data": {
        "message": "Launching parallel agents to scan SISTIC and Ticketmaster SG...",
    }}

    # Forward all events from the two-phase discovery pipeline
    events: list[dict] = []
    is_live = False

    async for ev in discover_events_parallel():
        etype = ev["event"]

        # Forward streaming and progress events directly to frontend
        if etype in ("agent_streaming", "agent_progress"):
            yield ev
            # Also emit as discovery_progress for the narration log
            if etype == "agent_progress":
                yield {"event": "discovery_progress", "data": {
                    "message": ev["data"].get("message", ""),
                }}

        elif etype == "phase1_complete":
            sistic_n = ev["data"]["sistic_count"]
            tm_n = ev["data"]["ticketmaster_count"]
            yield {"event": "discovery_progress", "data": {
                "message": f"Found {sistic_n} SISTIC + {tm_n} Ticketmaster event links. Extracting details...",
            }}

        elif etype == "phase2_started":
            yield {"event": "discovery_progress", "data": {
                "message": f"Firing {ev['data']['total_events']} parallel agents for detail extraction...",
            }}

        elif etype == "discovery_result":
            events = ev["data"]["events"]
            is_live = ev["data"]["is_live"]

    # Rank by risk
    ranked = await rank_events(events)

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


def _normalize_listings(result) -> list[dict]:
    """Normalize TinyFish extraction result to a list of listing dicts."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("listings") or result.get("results") or []
    return []


async def run_dashboard_scan(events: list[dict]) -> AsyncGenerator[dict, None]:
    """Scan top-risk events for fraudulent listings.

    Fires ALL marketplace searches upfront as one batch (5 events × 2 platforms
    = 10 runs, within the 20-pending TinyFish limit). TinyFish queues them
    and runs 2 at a time. Results are processed as they complete.

    SSE events yielded:
    - dashboard_scan_started: {event_count, events}
    - scan_progress: {message}
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

    # ---- Batch-fire all marketplace searches upfront -----------------------
    # Build tasks: for each event, one Carousell search + one Viagogo search
    batch_tasks: list[dict] = []
    task_map: list[tuple[dict, str]] = []  # (event, platform)

    for ev in top_events:
        event_name = ev["event_name"]
        # Carousell search
        batch_tasks.append({
            "url": f"https://www.carousell.sg/search/{quote(event_name)}",
            "goal": (
                f"Search for '{event_name}' tickets. "
                "Extract a JSON array of listings with keys: title, price, seller."
            ),
        })
        task_map.append((ev, "Carousell"))
        # Viagogo search
        batch_tasks.append({
            "url": "https://www.viagogo.com/",
            "goal": (
                f"Search for '{event_name}' tickets. "
                "Extract a JSON array of listings with keys: title, price, seller."
            ),
        })
        task_map.append((ev, "Viagogo"))

    yield {"event": "scan_progress", "data": {
        "message": f"Firing {len(batch_tasks)} marketplace searches across {len(top_events)} events...",
    }}

    # Fire everything — TinyFish queues and runs 2 at a time
    # Use asyncio.Queue to forward streaming URLs in real-time during polling
    scan_event_queue: asyncio.Queue[dict] = asyncio.Queue()
    streaming_urls_forwarded: set[int] = set()

    def _on_scan_progress(idx: int, status: str, run_data: dict):
        surl = run_data.get("streaming_url")
        if surl and idx not in streaming_urls_forwarded:
            streaming_urls_forwarded.add(idx)
            ev, platform = task_map[idx]
            scan_event_queue.put_nowait({
                "event": "agent_streaming",
                "data": {
                    "step": f"scan_{platform.lower()}",
                    "streaming_url": surl,
                },
            })

    # Run batch in a task so we can drain the queue concurrently
    batch_task = asyncio.create_task(
        tinyfish_extract_batch(
            batch_tasks,
            timeout=600.0,
            poll_interval=4.0,
            on_progress=_on_scan_progress,
        )
    )

    # Drain streaming URL events while batch runs
    while not batch_task.done():
        try:
            ev = await asyncio.wait_for(scan_event_queue.get(), timeout=2.0)
            yield ev
        except asyncio.TimeoutError:
            continue

    # Drain remaining
    while not scan_event_queue.empty():
        yield scan_event_queue.get_nowait()

    batch_results = await batch_task

    yield {"event": "scan_progress", "data": {
        "message": "All searches complete. Classifying listings...",
    }}

    # ---- Group results by event and classify --------------------------------
    event_listings: dict[str, list[dict]] = {}
    for i, result in enumerate(batch_results):
        ev, platform = task_map[i]
        event_id = ev["event_id"]
        if event_id not in event_listings:
            event_listings[event_id] = []

        raw_listings = _normalize_listings(result.data) if result.data else []
        for j, l in enumerate(raw_listings):
            l["listing_id"] = f"{event_id}-{'car' if platform == 'Carousell' else 'via'}-{j}"
            l["platform"] = platform
            event_listings[event_id].append(l)

    # Aggregate stats
    agg = {
        "total_events_scanned": 0,
        "total_listings": 0,
        "total_flagged": 0,
        "total_confirmed_scams": 0,
        "total_fraud_exposure": 0.0,
        "by_event": {},
    }

    # Process each event's listings
    for ev in top_events:
        event_id = ev["event_id"]
        event_name = ev["event_name"]
        all_listings = event_listings.get(event_id, [])[:MAX_LISTINGS_PER_EVENT]

        yield {"event": "event_scan_started", "data": {
            "event_id": event_id,
            "event_name": event_name,
        }}

        yield {"event": "event_listings_found", "data": {
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
        }}

        # Classify each listing
        event_stats = ScanStats(
            total_listings=len(all_listings),
            by_platform={
                "Carousell": len([l for l in all_listings if l["platform"] == "Carousell"]),
                "Viagogo": len([l for l in all_listings if l["platform"] == "Viagogo"]),
            },
        )

        for listing in all_listings:
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

                yield {"event": "event_listing_verdict", "data": {
                    "event_id": event_id,
                    "listing_id": listing["listing_id"],
                    "verdict": verdict_data,
                    "listing_summary": {
                        "title": listing.get("title", ""),
                        "price": price,
                        "seller": listing.get("seller", ""),
                        "platform": listing.get("platform", ""),
                    },
                }}
            except Exception as e:
                logger.warning("Classification failed for %s: %s", listing.get("listing_id"), e)
                event_stats.investigated += 1

        yield {"event": "event_scan_complete", "data": {
            "event_id": event_id,
            "stats": event_stats.model_dump(),
        }}

        # Accumulate aggregate
        agg["total_events_scanned"] += 1
        agg["total_listings"] += event_stats.total_listings
        agg["total_flagged"] += event_stats.flagged
        agg["total_confirmed_scams"] += event_stats.confirmed_scams
        agg["total_fraud_exposure"] += event_stats.fraud_exposure
        agg["by_event"][event_id] = {
            "event_name": event_name,
            "risk_score": ev.get("risk_score", 0),
            "stats": event_stats.model_dump(),
        }

    yield {"event": "dashboard_scan_complete", "data": {
        "aggregate_stats": agg,
        "duration_seconds": round(time.time() - start_time, 1),
    }}

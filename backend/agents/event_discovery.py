"""Event discovery agent: two-phase parallel architecture.

Phase 1 — Link Discovery (fast):
    One agent per source scans the page and extracts event links/names.
    Simple goal = fast, reliable.

Phase 2 — Parallel Detail Extraction:
    Fire N agents via batch API, one per event page.
    Each extracts structured data from a single event page.
    True parallel execution on TinyFish infrastructure.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator

from agents.base import (
    tinyfish_extract_with_streaming,
    tinyfish_extract_batch,
    TinyFishResult,
)

logger = logging.getLogger(__name__)

# ---- Phase 1 Goals: Link Discovery (simple, fast) -------------------------

SISTIC_LINKS_GOAL = (
    "Browse the SISTIC homepage and events listings page. "
    "Find all upcoming event links that are visible. "
    "For each event, extract a JSON array with keys: "
    "event_name (text), url (the link to the event detail page), "
    "popularity_hint (any badge text like 'Selling Fast', 'Limited Tickets', 'Hot' or null). "
    "Return up to 20 events. Focus on getting the URLs right."
)

TICKETMASTER_SG_LINKS_GOAL = (
    "Browse the Ticketmaster Singapore site and find upcoming events. "
    "For each event, extract a JSON array with keys: "
    "event_name (text), url (the link to the event detail page), "
    "popularity_hint (any badge text like 'Sold Out', 'Hot', 'Popular' or null). "
    "Return up to 20 events. Focus on getting the URLs right."
)

# ---- Phase 2 Goal: Single Event Detail Extraction --------------------------

def _build_event_detail_goal(event_name: str) -> str:
    return (
        f"You are on the detail page for '{event_name}'. "
        "Extract a JSON object with these keys: "
        "event_name (full official name), venue (text), date (text), "
        "category (one of: concert, sports, festival, theatre, comedy, other), "
        "face_value_low (number in SGD or null), "
        "face_value_high (number in SGD or null), "
        "sold_out (boolean or null), "
        "popularity_hint (any text like 'Selling Fast', 'Limited Tickets' or null). "
        "Return a single JSON object."
    )


# ---- High-risk category keywords (used by risk_scoring) -------------------

HIGH_RISK_CATEGORIES = {"concert", "sports"}
KPOP_KEYWORDS = {
    "blackpink", "bts", "twice", "stray kids", "day6", "g)i-dle", "gidle",
    "ive", "aespa", "seventeen", "nct", "ateez", "enhypen", "le sserafim",
    "itzy", "txt", "newjeans", "apink",
}
HIGH_DEMAND_KEYWORDS = {
    "taylor swift", "coldplay", "bruno mars", "ed sheeran", "f1",
    "grand prix", "formula 1", "billie eilish", "dua lipa", "adele",
}


# ---- Seed Fallback ---------------------------------------------------------

SEED_EVENTS: list[dict] = [
    {
        "event_name": "F1 Singapore Grand Prix 2026",
        "venue": "Marina Bay Street Circuit",
        "date": "October 2026",
        "category": "sports",
        "face_value_low": 268,
        "face_value_high": 1888,
        "sold_out": False,
        "popularity_hint": "Selling Fast",
        "source": "seed",
    },
    {
        "event_name": "Taylor Swift | The Eras Tour (Singapore)",
        "venue": "National Stadium",
        "date": "2026",
        "category": "concert",
        "face_value_low": 108,
        "face_value_high": 448,
        "sold_out": True,
        "popularity_hint": "Sold Out",
        "source": "seed",
    },
    {
        "event_name": "DAY6 10th Anniversary World Tour",
        "venue": "Singapore Indoor Stadium",
        "date": "April 2026",
        "category": "concert",
        "face_value_low": 128,
        "face_value_high": 328,
        "sold_out": False,
        "popularity_hint": "Selling Fast",
        "source": "seed",
    },
    {
        "event_name": "Coldplay Music of the Spheres Tour",
        "venue": "National Stadium",
        "date": "2026",
        "category": "concert",
        "face_value_low": 108,
        "face_value_high": 498,
        "sold_out": True,
        "popularity_hint": "Sold Out",
        "source": "seed",
    },
    {
        "event_name": "(G)I-DLE World Tour: Syncopation",
        "venue": "Singapore Indoor Stadium",
        "date": "May 2026",
        "category": "concert",
        "face_value_low": 128,
        "face_value_high": 368,
        "sold_out": False,
        "popularity_hint": "Limited Tickets",
        "source": "seed",
    },
    {
        "event_name": "Eric Chou Odyssey World Tour",
        "venue": "Star Theatre",
        "date": "April 2026",
        "category": "concert",
        "face_value_low": 98,
        "face_value_high": 298,
        "sold_out": False,
        "popularity_hint": None,
        "source": "seed",
    },
    {
        "event_name": "Bruno Mars Singapore Concert 2026",
        "venue": "National Stadium",
        "date": "2026",
        "category": "concert",
        "face_value_low": 128,
        "face_value_high": 528,
        "sold_out": True,
        "popularity_hint": "Sold Out",
        "source": "seed",
    },
    {
        "event_name": "Singapore Rugby Sevens 2026",
        "venue": "National Stadium",
        "date": "May 2026",
        "category": "sports",
        "face_value_low": 38,
        "face_value_high": 168,
        "sold_out": False,
        "popularity_hint": None,
        "source": "seed",
    },
    {
        "event_name": "BLACKPINK World Tour Singapore",
        "venue": "National Stadium",
        "date": "2026",
        "category": "concert",
        "face_value_low": 148,
        "face_value_high": 498,
        "sold_out": True,
        "popularity_hint": "Sold Out",
        "source": "seed",
    },
    {
        "event_name": "Billie Eilish HIT ME HARD AND SOFT Tour",
        "venue": "Singapore Indoor Stadium",
        "date": "June 2026",
        "category": "concert",
        "face_value_low": 108,
        "face_value_high": 358,
        "sold_out": False,
        "popularity_hint": "Selling Fast",
        "source": "seed",
    },
]


# ---- Normalization ---------------------------------------------------------

def _normalize_links(raw: dict | list | None) -> list[dict]:
    """Normalize TinyFish extraction to list of {event_name, url, popularity_hint}."""
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("events") or raw.get("results") or raw.get("links") or []
        if not isinstance(items, list):
            return []
    else:
        return []
    # Filter to only items that have a url
    return [item for item in items if isinstance(item, dict) and item.get("url")]


def _normalize_event_detail(raw: dict | list | None, source: str) -> dict | None:
    """Normalize a single event detail extraction."""
    if raw is None:
        return None
    if isinstance(raw, list) and len(raw) > 0:
        raw = raw[0]
    if not isinstance(raw, dict):
        return None
    raw["source"] = source
    return raw


def _normalize_events(raw: dict | list | None, source: str) -> list[dict]:
    """Normalize TinyFish extraction to list of event dicts (legacy compat)."""
    if raw is None:
        return []
    if isinstance(raw, list):
        events = raw
    elif isinstance(raw, dict):
        events = raw.get("events") or raw.get("results") or []
        if not isinstance(events, list):
            return []
    else:
        return []
    for ev in events:
        ev["source"] = source
    return events


# ---- Two-Phase Discovery ---------------------------------------------------

async def discover_events_parallel(
    on_streaming_url: callable | None = None,
    on_progress: callable | None = None,
) -> AsyncGenerator[dict, None]:
    """Two-phase parallel event discovery.

    Phase 1: Fire 2 agents to find event links on SISTIC + Ticketmaster.
    Phase 2: Fire N parallel agents (batch API) to extract details per event.

    Yields SSE events:
    - agent_streaming: {step, streaming_url}
    - agent_progress: {step, message}
    - phase1_complete: {source, link_count, links}
    - phase2_started: {total_events, sources}
    - event_detail: {event, source}
    - discovery_result: {events, is_live}
    """

    # ---- Phase 1: Link Discovery (parallel) --------------------------------
    yield {"event": "agent_progress", "data": {
        "step": "discover_links",
        "message": "Phase 1: Scanning SISTIC and Ticketmaster for event links...",
    }}

    # Fire both link discovery agents concurrently
    sistic_task = asyncio.create_task(
        tinyfish_extract_with_streaming(
            url="https://www.sistic.com.sg",
            goal=SISTIC_LINKS_GOAL,
            timeout=90.0,
        )
    )
    ticketmaster_task = asyncio.create_task(
        tinyfish_extract_with_streaming(
            url="https://www.ticketmaster.sg",
            goal=TICKETMASTER_SG_LINKS_GOAL,
            timeout=90.0,
        )
    )

    # Monitor both tasks and yield streaming_urls as they appear
    streaming_urls_seen: set[str] = set()
    tasks = {"sistic": sistic_task, "ticketmaster": ticketmaster_task}

    while not all(t.done() for t in tasks.values()):
        await asyncio.sleep(2.0)
        # Check for streaming URLs from completed or running tasks
        for name, task in tasks.items():
            if task.done():
                result = task.result()
                if result.streaming_url and result.streaming_url not in streaming_urls_seen:
                    streaming_urls_seen.add(result.streaming_url)
                    yield {"event": "agent_streaming", "data": {
                        "step": f"discover_{name}",
                        "streaming_url": result.streaming_url,
                    }}

    # Collect results
    sistic_result: TinyFishResult = sistic_task.result()
    ticketmaster_result: TinyFishResult = ticketmaster_task.result()

    # Forward any streaming URLs we haven't sent yet
    for name, result in [("sistic", sistic_result), ("ticketmaster", ticketmaster_result)]:
        if result.streaming_url and result.streaming_url not in streaming_urls_seen:
            streaming_urls_seen.add(result.streaming_url)
            yield {"event": "agent_streaming", "data": {
                "step": f"discover_{name}",
                "streaming_url": result.streaming_url,
            }}

    sistic_links = _normalize_links(sistic_result.data)
    ticketmaster_links = _normalize_links(ticketmaster_result.data)

    yield {"event": "agent_progress", "data": {
        "step": "discover_links",
        "message": f"Found {len(sistic_links)} SISTIC links + {len(ticketmaster_links)} Ticketmaster links",
    }}

    yield {"event": "phase1_complete", "data": {
        "sistic_count": len(sistic_links),
        "ticketmaster_count": len(ticketmaster_links),
    }}

    # ---- Phase 2: Parallel Detail Extraction (batch API) -------------------

    # Deduplicate by event name
    seen_names: set[str] = set()
    all_links: list[tuple[dict, str]] = []  # (link_info, source)
    for link in sistic_links:
        name_key = link.get("event_name", "").strip().lower()
        if name_key and name_key not in seen_names:
            seen_names.add(name_key)
            all_links.append((link, "SISTIC"))
    for link in ticketmaster_links:
        name_key = link.get("event_name", "").strip().lower()
        if name_key and name_key not in seen_names:
            seen_names.add(name_key)
            all_links.append((link, "Ticketmaster SG"))

    if not all_links:
        yield {"event": "agent_progress", "data": {
            "step": "discover_links",
            "message": "No event links found from live scraping, using seed data",
        }}
        yield {"event": "discovery_result", "data": {
            "events": list(SEED_EVENTS),
            "is_live": False,
        }}
        return

    yield {"event": "agent_progress", "data": {
        "step": "extract_details",
        "message": f"Phase 2: Extracting details for {len(all_links)} events in parallel...",
    }}

    yield {"event": "phase2_started", "data": {
        "total_events": len(all_links),
    }}

    # Build batch tasks — each extracts details from one event page
    batch_tasks = []
    for link_info, source in all_links:
        event_url = link_info["url"]
        event_name = link_info.get("event_name", "Unknown")
        batch_tasks.append({
            "url": event_url,
            "goal": _build_event_detail_goal(event_name),
        })

    # Fire all via batch API — true parallel on TinyFish infra
    streaming_forwarded: set[int] = set()

    def _on_batch_progress(idx: int, status: str, run_data: dict):
        nonlocal streaming_forwarded
        # We can't yield from a callback, but we track streaming URLs
        surl = run_data.get("streaming_url")
        if surl and idx not in streaming_forwarded:
            streaming_forwarded.add(idx)

    batch_results = await tinyfish_extract_batch(
        batch_tasks,
        timeout=120.0,
        poll_interval=4.0,
        on_progress=_on_batch_progress,
    )

    # Process results
    events: list[dict] = []
    for i, (result, (link_info, source)) in enumerate(zip(batch_results, all_links)):
        detail = _normalize_event_detail(result.data if isinstance(result, TinyFishResult) else result, source)
        if detail:
            # Preserve popularity_hint from Phase 1 if Phase 2 didn't find one
            if not detail.get("popularity_hint") and link_info.get("popularity_hint"):
                detail["popularity_hint"] = link_info["popularity_hint"]
            events.append(detail)

            yield {"event": "agent_progress", "data": {
                "step": "extract_details",
                "message": f"Extracted: {detail.get('event_name', 'Unknown')} ({source})",
            }}

            # Forward streaming URL if available
            if isinstance(result, TinyFishResult) and result.streaming_url:
                yield {"event": "agent_streaming", "data": {
                    "step": "extract_details",
                    "streaming_url": result.streaming_url,
                }}
        else:
            # Fallback: use link info as a minimal event
            events.append({
                "event_name": link_info.get("event_name", "Unknown"),
                "source": source,
                "category": "other",
                "popularity_hint": link_info.get("popularity_hint"),
            })

    is_live = len(events) >= 3
    if not is_live:
        for ev in SEED_EVENTS:
            name_key = ev["event_name"].strip().lower()
            if name_key not in seen_names:
                seen_names.add(name_key)
                events.append(ev)

    yield {"event": "discovery_result", "data": {
        "events": events,
        "is_live": is_live,
    }}


# ---- Legacy compat (used by non-dashboard flows) ---------------------------

async def discover_events() -> tuple[list[dict], bool]:
    """Discover events from SISTIC + Ticketmaster SG in parallel.

    Returns:
        Tuple of (events_list, is_live). Falls back to SEED_EVENTS if scraping fails.
    """
    events: list[dict] = []
    is_live = False

    async for ev in discover_events_parallel():
        if ev["event"] == "discovery_result":
            events = ev["data"]["events"]
            is_live = ev["data"]["is_live"]

    if not events:
        return (list(SEED_EVENTS), False)

    return (events, is_live)

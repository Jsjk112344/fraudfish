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

from agents.base import tinyfish_extract_with_streaming, TinyFishResult

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
    "Step 1: Wait for the page to fully load. If there is a cookie banner, dismiss it first. "
    "Step 2: Browse the Ticketmaster Singapore site and find upcoming events. "
    "Step 3: For each event, extract a JSON array with keys: "
    "event_name (text), url (the full link to the event detail page), "
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
    "i-dle", "idle", "ive", "aespa", "seventeen", "nct", "ateez", "enhypen",
    "le sserafim", "itzy", "txt", "newjeans", "apink", "treasure", "woodz",
    "natori", "zutomayo", "laufey", "bus fancon",
}
HIGH_DEMAND_KEYWORDS = {
    "taylor swift", "coldplay", "bruno mars", "ed sheeran", "f1",
    "grand prix", "formula 1", "billie eilish", "dua lipa", "adele",
    "guns n' roses", "guns n roses", "lany", "harry potter", "cirque du soleil",
    "les mis", "g.e.m", "rainie yang", "eric chou", "周興哲",
    "world tour", "asia tour",
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


def _guess_category(event_name: str) -> str:
    """Guess event category from name keywords. Good enough for risk scoring."""
    name_lower = event_name.lower()
    if any(kw in name_lower for kw in ("f1", "grand prix", "rugby", "football", "basketball", "tennis")):
        return "sports"
    if any(kw in name_lower for kw in ("festival", "fest ")):
        return "festival"
    if any(kw in name_lower for kw in ("comedy", "stand-up", "standup", "masala")):
        return "comedy"
    if any(kw in name_lower for kw in ("theatre", "theater", "musical", "ballet", "opera", "les mis", "bfg", "roald dahl")):
        return "theatre"
    # Immersive experiences / exhibitions — still high-value tickets
    if any(kw in name_lower for kw in ("harry potter", "cirque du soleil", "disney", "visions of magic")):
        return "concert"  # Treat as concert-level risk
    # Default to concert — most ticket events are music
    return "concert"


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

async def discover_events_parallel() -> AsyncGenerator[dict, None]:
    """Two-phase parallel event discovery.

    Phase 1: Fire 2 agents to find event links on SISTIC + Ticketmaster.
             Streaming URLs are forwarded in real-time via asyncio.Queue.
    Phase 2: Fire N parallel agents (batch API) to extract details per event.

    Yields SSE events:
    - agent_streaming: {step, streaming_url}
    - agent_progress: {step, message}
    - phase1_complete: {sistic_count, ticketmaster_count}
    - phase2_started: {total_events}
    - discovery_result: {events, is_live}
    """

    # ---- Phase 1: Link Discovery (parallel) --------------------------------
    yield {"event": "agent_progress", "data": {
        "step": "discover_links",
        "message": "Phase 1: Scanning SISTIC and Ticketmaster for event links...",
    }}

    # Queue for real-time streaming URL and progress forwarding
    event_queue: asyncio.Queue[dict] = asyncio.Queue()

    # Fire both link discovery agents concurrently
    # TinyFish agents can take 2-3 min on complex pages — don't timeout prematurely
    sistic_task = asyncio.create_task(
        tinyfish_extract_with_streaming(
            url="https://www.sistic.com.sg",
            goal=SISTIC_LINKS_GOAL,
            timeout=600.0,
            event_queue=event_queue,
            step_label="discover_sistic",
        )
    )
    # Ticketmaster returns 401 to non-browser requests — stealth + proxy needed
    ticketmaster_task = asyncio.create_task(
        tinyfish_extract_with_streaming(
            url="https://www.ticketmaster.sg",
            goal=TICKETMASTER_SG_LINKS_GOAL,
            stealth=True,
            proxy_country="AU",
            timeout=600.0,
            event_queue=event_queue,
            step_label="discover_ticketmaster",
        )
    )

    # Drain events from both agents in real-time (streaming URLs, progress)
    streaming_count = 0
    while not (sistic_task.done() and ticketmaster_task.done()):
        try:
            ev = await asyncio.wait_for(event_queue.get(), timeout=2.0)
            if ev["event"] == "agent_streaming":
                streaming_count += 1
                logger.info("Yielding agent_streaming #%d: step=%s url=%s",
                           streaming_count, ev["data"]["step"], ev["data"]["streaming_url"][:60])
            yield ev
        except asyncio.TimeoutError:
            continue

    # Drain any remaining queued events
    while not event_queue.empty():
        ev = event_queue.get_nowait()
        if ev["event"] == "agent_streaming":
            streaming_count += 1
            logger.info("Yielding agent_streaming #%d (drain): step=%s url=%s",
                       streaming_count, ev["data"]["step"], ev["data"]["streaming_url"][:60])
        yield ev

    logger.info("Total agent_streaming events yielded: %d", streaming_count)

    # Collect results (await to propagate exceptions)
    sistic_result: TinyFishResult = await sistic_task
    ticketmaster_result: TinyFishResult = await ticketmaster_task

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

    # ---- Build event list from link data (no Phase 2 needed) -----------------
    # Link discovery already gives us event_name + popularity_hint which is
    # enough for risk scoring. Skipping per-event-page extraction saves
    # TinyFish runs (account limit: 20 pending, 2 concurrent).

    seen_names: set[str] = set()
    events: list[dict] = []
    for link in sistic_links:
        name_key = link.get("event_name", "").strip().lower()
        if name_key and name_key not in seen_names:
            seen_names.add(name_key)
            events.append({
                "event_name": link.get("event_name", "Unknown"),
                "url": link.get("url"),
                "source": "SISTIC",
                "category": _guess_category(link.get("event_name", "")),
                "popularity_hint": link.get("popularity_hint"),
            })
    for link in ticketmaster_links:
        name_key = link.get("event_name", "").strip().lower()
        if name_key and name_key not in seen_names:
            seen_names.add(name_key)
            events.append({
                "event_name": link.get("event_name", "Unknown"),
                "url": link.get("url"),
                "source": "Ticketmaster SG",
                "category": _guess_category(link.get("event_name", "")),
                "popularity_hint": link.get("popularity_hint"),
            })

    if not events:
        yield {"event": "agent_progress", "data": {
            "step": "discover_links",
            "message": "No event links found from live scraping, using seed data",
        }}
        yield {"event": "discovery_result", "data": {
            "events": list(SEED_EVENTS),
            "is_live": False,
        }}
        return

    is_live = len(events) >= 3
    if not is_live:
        for ev in SEED_EVENTS:
            name_key = ev["event_name"].strip().lower()
            if name_key not in seen_names:
                seen_names.add(name_key)
                events.append(ev)

    yield {"event": "agent_progress", "data": {
        "step": "discover_links",
        "message": f"Compiled {len(events)} events for risk ranking",
    }}

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

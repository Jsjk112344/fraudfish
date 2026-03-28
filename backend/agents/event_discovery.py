"""Event discovery agent: scrape SISTIC and Ticketmaster SG for upcoming high-profile events."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

from agents.base import tinyfish_extract

logger = logging.getLogger(__name__)

# ---- TinyFish Goals --------------------------------------------------------

SISTIC_DISCOVERY_GOAL = (
    "Browse the SISTIC homepage and events listings. "
    "Find the top upcoming events in Singapore that are popular, high-profile, "
    "or trending — concerts, sports, festivals, shows. "
    "For each event extract a JSON array with keys: "
    "event_name, venue, date (as text), category (one of: concert, sports, "
    "festival, theatre, comedy, other), face_value_low (number in SGD or null), "
    "face_value_high (number in SGD or null), sold_out (boolean or null), "
    "popularity_hint (text snippet like 'Selling Fast', 'Limited Tickets', etc. or null). "
    "Return up to 15 events sorted by perceived popularity."
)

TICKETMASTER_SG_DISCOVERY_GOAL = (
    "Browse the Ticketmaster Singapore site for upcoming events. "
    "Find popular, high-profile, or trending events — concerts, sports, festivals. "
    "For each event extract a JSON array with keys: "
    "event_name, venue, date (as text), category (one of: concert, sports, "
    "festival, theatre, comedy, other), face_value_low (number in SGD or null), "
    "face_value_high (number in SGD or null), sold_out (boolean or null), "
    "popularity_hint (text snippet like 'Sold Out', 'Hot', etc. or null). "
    "Return up to 15 events sorted by perceived popularity."
)

# ---- Seed Fallback ---------------------------------------------------------
# Hard-coded high-profile SG events for when live scraping fails.

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

# ---- High-risk category keywords ------------------------------------------

HIGH_RISK_CATEGORIES = {"concert", "sports"}
KPOP_KEYWORDS = {
    "blackpink", "bts", "twice", "stray kids", "day6", "g)i-dle", "gidle",
    "ive", "aespa", "seventeen", "nct", "ateez", "enhypen", "le sserafim",
    "itzy", "txt", "newjeans",
}
HIGH_DEMAND_KEYWORDS = {
    "taylor swift", "coldplay", "bruno mars", "ed sheeran", "f1",
    "grand prix", "formula 1", "billie eilish", "dua lipa", "adele",
}


# ---- Scraping Functions ----------------------------------------------------

def _normalize_events(raw: dict | list | None, source: str) -> list[dict]:
    """Normalize TinyFish extraction to list of event dicts."""
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


async def discover_sistic_events() -> list[dict]:
    """Scrape SISTIC.com for upcoming popular events."""
    result = await tinyfish_extract(
        url="https://www.sistic.com.sg",
        goal=SISTIC_DISCOVERY_GOAL,
        stealth=True,
        proxy_country="SG",
        timeout=20.0,
    )
    return _normalize_events(result, "SISTIC")


async def discover_ticketmaster_events() -> list[dict]:
    """Scrape Ticketmaster SG for upcoming popular events."""
    result = await tinyfish_extract(
        url="https://www.ticketmaster.sg",
        goal=TICKETMASTER_SG_DISCOVERY_GOAL,
        stealth=True,
        proxy_country="SG",
        timeout=20.0,
    )
    return _normalize_events(result, "Ticketmaster SG")


async def discover_events() -> tuple[list[dict], bool]:
    """Discover events from SISTIC + Ticketmaster SG in parallel.

    Returns:
        Tuple of (events_list, is_live).  Falls back to SEED_EVENTS if scraping fails.
    """
    try:
        sistic, ticketmaster = await asyncio.gather(
            discover_sistic_events(),
            discover_ticketmaster_events(),
        )
    except Exception as e:
        logger.warning("Event discovery failed: %s, using seed list", e)
        return (list(SEED_EVENTS), False)

    # Merge and deduplicate by event name (case-insensitive)
    seen: set[str] = set()
    merged: list[dict] = []
    for ev in sistic + ticketmaster:
        name_key = ev.get("event_name", "").strip().lower()
        if name_key and name_key not in seen:
            seen.add(name_key)
            merged.append(ev)

    if len(merged) < 3:
        logger.info("Only %d live events found, supplementing with seed data", len(merged))
        for ev in SEED_EVENTS:
            name_key = ev["event_name"].strip().lower()
            if name_key not in seen:
                seen.add(name_key)
                merged.append(ev)
        return (merged, len(merged) > len(SEED_EVENTS))

    return (merged, True)


async def discover_events_with_streaming() -> AsyncGenerator[dict, None]:
    """Discover events with live TinyFish streaming events forwarded.

    Yields SSE events:
    - agent_streaming: {step, streaming_url}  — live browser preview
    - agent_progress: {step, message}         — agent narration
    - discovery_result: {events, is_live}     — final merged events
    """
    from agents.live_events import tinyfish_extract_with_events

    sistic_events: list[dict] = []
    ticketmaster_events: list[dict] = []
    sistic_done = False
    ticketmaster_done = False

    async def _discover_sistic():
        nonlocal sistic_events, sistic_done
        async for ev in tinyfish_extract_with_events(
            url="https://www.sistic.com.sg",
            goal=SISTIC_DISCOVERY_GOAL,
            step_label="discover_sistic",
            stealth=True,
            proxy_country="SG",
            timeout=20.0,
        ):
            if ev["event"] == "agent_result":
                sistic_events = _normalize_events(ev["data"].get("data"), "SISTIC")
                sistic_done = True
            else:
                yield ev

    async def _discover_ticketmaster():
        nonlocal ticketmaster_events, ticketmaster_done
        async for ev in tinyfish_extract_with_events(
            url="https://www.ticketmaster.sg",
            goal=TICKETMASTER_SG_DISCOVERY_GOAL,
            step_label="discover_ticketmaster",
            stealth=True,
            proxy_country="SG",
            timeout=20.0,
        ):
            if ev["event"] == "agent_result":
                ticketmaster_events = _normalize_events(ev["data"].get("data"), "Ticketmaster SG")
                ticketmaster_done = True
            else:
                yield ev

    # Run both in sequence (can't easily yield from parallel async generators)
    async for ev in _discover_sistic():
        yield ev
    async for ev in _discover_ticketmaster():
        yield ev

    # Merge and deduplicate
    seen: set[str] = set()
    merged: list[dict] = []
    for ev in sistic_events + ticketmaster_events:
        name_key = ev.get("event_name", "").strip().lower()
        if name_key and name_key not in seen:
            seen.add(name_key)
            merged.append(ev)

    is_live = len(merged) >= 3
    if not is_live:
        for ev in SEED_EVENTS:
            name_key = ev["event_name"].strip().lower()
            if name_key not in seen:
                seen.add(name_key)
                merged.append(ev)

    yield {
        "event": "discovery_result",
        "data": {"events": merged, "is_live": is_live},
    }

"""Official price verification agent for Ticketmaster SG and F1 official site."""

from __future__ import annotations

from urllib.parse import quote_plus

from agents.base import tinyfish_extract
from agents.google_fallback import google_event_search
from mock.data import get_mock_data

F1_OFFICIAL_URL = "https://www.singaporegp.sg"
TICKETMASTER_SG_SEARCH_URL = "https://www.ticketmaster.sg/search?q={query}"

F1_VERIFICATION_GOAL = (
    "Search this F1 Singapore Grand Prix official website for ticket information. "
    "Extract: "
    "1. Full event name, "
    "2. Venue name, "
    "3. Event dates (the race weekend dates), "
    "4. Ticket prices -- find the lowest and highest face value in SGD, "
    "5. Whether tickets are sold out or still available. "
    "Return as JSON with keys: event_name, venue, date, face_value_low, "
    "face_value_high, sold_out (boolean), available_categories (array of ticket types)."
)

TICKETMASTER_VERIFICATION_GOAL = (
    "Search the Ticketmaster Singapore results for '{event_name}'. "
    "Find the matching event and extract: "
    "1. Full event name as listed, "
    "2. Venue name, "
    "3. Event date, "
    "4. Ticket price range (lowest face value in SGD), "
    "5. Whether it is sold out or still available. "
    "Return as JSON with keys: event_name, venue, date, face_value, "
    "sold_out (boolean)."
)

F1_KEYWORDS = {"f1", "grand prix", "gp", "formula 1", "formula one", "singaporegp"}


def is_f1_event(event_name: str) -> bool:
    """Check if event name indicates an F1/Grand Prix event."""
    name_lower = event_name.lower()
    return any(kw in name_lower for kw in F1_KEYWORDS)


def normalize_official_result(raw: dict, source: str) -> dict:
    """Normalize official site result to standard event verification schema."""
    face_value = raw.get("face_value") or raw.get("face_value_low")
    return {
        "event_name": raw.get("event_name", ""),
        "venue": raw.get("venue", ""),
        "date": raw.get("date", ""),
        "face_value": float(face_value) if face_value else None,
        "face_value_high": float(raw["face_value_high"]) if raw.get("face_value_high") else None,
        "sold_out": raw.get("sold_out"),
        "source": source,
        "unverified": False,
        "available_categories": raw.get("available_categories", []),
    }


async def _check_f1_official(timeout: float = 15.0) -> dict | None:
    """Check F1 Singapore GP official site."""
    result = await tinyfish_extract(
        url=F1_OFFICIAL_URL,
        goal=F1_VERIFICATION_GOAL,
        stealth=False,
        proxy_country=None,
        timeout=timeout,
    )
    if result is not None:
        return normalize_official_result(result, "F1 Official (singaporegp.sg)")
    return None


async def _check_ticketmaster(event_name: str, timeout: float = 15.0) -> dict | None:
    """Check Ticketmaster SG for event."""
    search_url = TICKETMASTER_SG_SEARCH_URL.format(query=quote_plus(event_name))
    goal = TICKETMASTER_VERIFICATION_GOAL.format(event_name=event_name)
    result = await tinyfish_extract(
        url=search_url,
        goal=goal,
        stealth=False,
        proxy_country=None,
        timeout=timeout,
    )
    if result is not None:
        return normalize_official_result(result, "Ticketmaster SG")
    return None


async def verify_event_official(
    event_name: str,
    timeout: float = 15.0,
) -> tuple[dict, bool]:
    """Verify event against official ticket sites. Returns (data, is_live).

    Priority: F1 official first for GP events, Ticketmaster SG for others.
    Falls through to Google search if both official sites fail.
    Falls back to mock data if everything fails.
    """
    # Determine check order based on event type
    if is_f1_event(event_name):
        # F1 events: check official site first, then Ticketmaster
        primary = await _check_f1_official(timeout=timeout)
        if primary is not None:
            return (primary, True)
        secondary = await _check_ticketmaster(event_name, timeout=timeout)
        if secondary is not None:
            return (secondary, True)
    else:
        # Other events: check Ticketmaster first, then F1 official (unlikely match)
        primary = await _check_ticketmaster(event_name, timeout=timeout)
        if primary is not None:
            return (primary, True)
        secondary = await _check_f1_official(timeout=timeout)
        if secondary is not None:
            return (secondary, True)

    # Fallback: Google search
    google_result, google_live = await google_event_search(event_name, timeout=timeout)
    if google_result is not None:
        return (google_result, google_live)

    # All sources failed -- return mock data
    return (get_mock_data("verify_event"), False)

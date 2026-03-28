"""Google search fallback for event verification when official sites fail."""

from __future__ import annotations

from urllib.parse import quote_plus

from agents.base import tinyfish_extract

GOOGLE_EVENT_GOAL = (
    "Look at the Google search results on this page and find information about "
    "this event's ticket pricing. Extract: "
    "1. The official event name as shown in search results, "
    "2. The venue name, "
    "3. The event date, "
    "4. Any face value or official ticket price mentioned (number in SGD), "
    "5. The source website where you found the price information. "
    "If you cannot find ticket pricing information for this event, "
    "set found to false. "
    "Return as JSON with keys: event_name, venue, date, face_value, source, found (boolean)."
)


def build_google_search_url(event_name: str) -> str:
    """Build Google search URL for event ticket pricing."""
    query = f"{event_name} singapore tickets price"
    return f"https://www.google.com/search?q={quote_plus(query)}"


def normalize_google_result(raw: dict) -> dict:
    """Normalize Google search result to standard event schema."""
    return {
        "event_name": raw.get("event_name", ""),
        "venue": raw.get("venue", ""),
        "date": raw.get("date", ""),
        "face_value": float(raw.get("face_value", 0)) if raw.get("face_value") else None,
        "sold_out": None,  # Google can't reliably determine sold-out status
        "source": raw.get("source", "Google Search"),
        "found": bool(raw.get("found", False)),
        "unverified": not bool(raw.get("found", False)),
    }


async def google_event_search(
    event_name: str, timeout: float = 15.0
) -> tuple[dict | None, bool]:
    """Search Google for event existence and approximate pricing.

    Returns (data, is_live). Returns (None, False) on TinyFish failure.
    """
    search_url = build_google_search_url(event_name)
    result = await tinyfish_extract(
        url=search_url,
        goal=GOOGLE_EVENT_GOAL,
        stealth=False,
        proxy_country=None,
        timeout=timeout,
    )
    if result is not None:
        return (normalize_google_result(result), True)
    return (None, False)

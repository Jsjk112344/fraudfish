"""Carousell listing extraction with TinyFish and fallback to mock data."""

from __future__ import annotations

from agents.base import tinyfish_extract
from mock.data import get_mock_data

CAROUSELL_LISTING_GOAL = (
    "Extract the following details from this Carousell listing page: "
    "1. Product title (the main listing title), "
    "2. Price in SGD (just the number, no currency symbol), "
    "3. Seller username (the seller's Carousell handle), "
    "4. Full description text (the complete listing description), "
    "5. Transfer/delivery method (e.g., meetup, mail, e-ticket), "
    "6. Posting date or relative time (e.g., '2 days ago'), "
    "7. Condition (new, used, like new, etc.), "
    "8. Number of likes and chats if visible. "
    "Return as a JSON object with keys: title, price, seller_username, "
    "description, transfer_method, posted_date, condition, likes, chats."
)


def normalize_carousell_listing(raw: dict) -> dict:
    """Normalize TinyFish response to standard listing schema."""
    try:
        price = float(raw.get("price", 0))
    except (ValueError, TypeError):
        price = 0.0
    return {
        "title": raw.get("title", ""),
        "price": price,
        "seller_username": raw.get("seller_username", ""),
        "description": raw.get("description", ""),
        "transfer_method": raw.get("transfer_method", "Not specified"),
        "posted_date": raw.get("posted_date", ""),
        "condition": raw.get("condition", ""),
        "likes": raw.get("likes", 0),
        "chats": raw.get("chats", 0),
        "platform": "carousell",
    }


async def extract_carousell_listing(url: str, timeout: float = 15.0) -> tuple[dict, bool]:
    """Extract listing data from Carousell. Returns (data, is_live).

    is_live=True means data came from live TinyFish scrape.
    is_live=False means data is cached mock fallback.
    """
    result = await tinyfish_extract(
        url=url,
        goal=CAROUSELL_LISTING_GOAL,
        stealth=True,
        proxy_country="SG",
        timeout=timeout,
    )
    if result is not None:
        return (normalize_carousell_listing(result), True)
    return (get_mock_data("extract_listing"), False)

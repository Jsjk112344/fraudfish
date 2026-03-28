"""Telegram message extraction with TinyFish and fallback to mock data."""

from __future__ import annotations

from agents.base import tinyfish_extract
from mock.data import get_mock_data

TELEGRAM_MESSAGE_GOAL = (
    "Extract the content of this Telegram message: "
    "1. Message text (the full message content), "
    "2. Sender username or display name, "
    "3. Date/time of the message, "
    "4. Any mentioned price (number in SGD if present), "
    "5. Any mentioned event name. "
    "Return as JSON with keys: message_text, sender_username, "
    "sent_date, price, event_name."
)


def normalize_telegram_message(raw: dict) -> dict:
    """Normalize TinyFish response to standard message schema."""
    try:
        price = float(raw.get("price", 0)) if raw.get("price") else None
    except (ValueError, TypeError):
        price = None
    return {
        "message_text": raw.get("message_text", ""),
        "sender_username": raw.get("sender_username", ""),
        "sent_date": raw.get("sent_date", ""),
        "price": price,
        "event_name": raw.get("event_name", ""),
        "platform": "telegram",
    }


async def extract_telegram_message(url: str, timeout: float = 15.0) -> tuple[dict, bool]:
    """Extract message data from Telegram. Returns (data, is_live).

    is_live=True means data came from live TinyFish extraction.
    is_live=False means data is cached mock fallback.
    """
    result = await tinyfish_extract(
        url=url,
        goal=TELEGRAM_MESSAGE_GOAL,
        stealth=False,
        proxy_country=None,
        timeout=timeout,
    )
    if result is not None:
        return (normalize_telegram_message(result), True)
    # Use listing mock data adapted for telegram format
    mock = get_mock_data("extract_listing")
    telegram_mock = {
        "message_text": mock.get("description", ""),
        "sender_username": mock.get("seller_name", "unknown"),
        "sent_date": mock.get("posted_date", ""),
        "price": mock.get("price"),
        "event_name": mock.get("title", ""),
        "platform": "telegram",
    }
    return (telegram_mock, False)

"""Telegram repeat-seller detection agent."""

from __future__ import annotations

from agents.base import tinyfish_extract
from mock.data import get_mock_data

TELEGRAM_SELLER_GOAL = (
    "Search this Telegram group/channel for other messages from the same user '{username}'. "
    "Extract: "
    "1. Total number of messages from this user in the group, "
    "2. Date of their earliest message in the group, "
    "3. Whether they appear to be a repeat seller (multiple ticket/listing messages). "
    "Return as JSON with keys: username, message_count, first_seen, repeat_seller (boolean)."
)


def normalize_telegram_seller(raw: dict, username: str) -> dict:
    """Normalize TinyFish response to standard Telegram seller schema."""
    message_count = int(raw.get("message_count", 1)) if raw.get("message_count") else 1
    return {
        "username": username,
        "message_count": message_count,
        "first_seen": raw.get("first_seen", "Unknown"),
        "repeat_seller": message_count > 1,
        "platform": "telegram",
    }


def build_telegram_group_url(original_url: str) -> str:
    """Extract the group/channel base URL from a message link.

    e.g., https://t.me/sgtickets/12345 -> https://t.me/sgtickets
    """
    parts = original_url.rstrip("/").split("/")
    # t.me/{group}/{message_id} -> t.me/{group}
    if len(parts) >= 4 and parts[-1].isdigit():
        return "/".join(parts[:-1])
    return original_url


async def investigate_telegram_seller(
    original_url: str,
    sender_username: str,
    timeout: float = 15.0,
) -> tuple[dict, bool]:
    """Investigate Telegram seller by scanning group for repeat messages.

    Returns (data, is_live).
    """
    group_url = build_telegram_group_url(original_url)
    goal = TELEGRAM_SELLER_GOAL.format(username=sender_username)
    result = await tinyfish_extract(
        url=group_url,
        goal=goal,
        stealth=False,
        proxy_country=None,
        timeout=timeout,
    )
    if result is not None:
        return (normalize_telegram_seller(result, sender_username), True)
    # Fallback: assume one-time poster
    fallback = {
        "username": sender_username,
        "message_count": 1,
        "first_seen": "Unknown",
        "repeat_seller": False,
        "platform": "telegram",
    }
    return (fallback, False)

"""Parallel marketplace search for market rate calculation."""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import quote

from agents.base import tinyfish_extract
from agents.stats import compute_market_stats
from mock.data import MOCK_STEPS

logger = logging.getLogger(__name__)


def _normalize_listings(result) -> list[dict]:
    """Normalize TinyFish extraction result to a list of listing dicts."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("listings") or result.get("results") or []
    return []


async def scan_carousell_market(event_name: str, category: str) -> list[dict]:
    """Search Carousell for listings matching the event."""
    search_url = f"https://www.carousell.sg/search/{quote(event_name)}"
    goal = (
        f"Search for '{event_name}' {category} tickets. "
        "Extract a JSON array of listings with keys: title, price, seller."
    )
    result = await tinyfish_extract(
        url=search_url,
        goal=goal,
        stealth=True,
        proxy_country="SG",
        timeout=15.0,
    )
    return _normalize_listings(result)


async def scan_viagogo_market(event_name: str) -> list[dict]:
    """Search Viagogo for listings matching the event."""
    goal = (
        f"Search for '{event_name}' tickets. "
        "Extract a JSON array of listings with keys: title, price, seller."
    )
    result = await tinyfish_extract(
        url="https://www.viagogo.com/",
        goal=goal,
        stealth=True,
        proxy_country="SG",
        timeout=15.0,
    )
    return _normalize_listings(result)


async def check_market_rates(
    event_name: str, category: str, this_listing_price: float
) -> dict:
    """Search Carousell and Viagogo in parallel, compute market statistics.

    Returns dict with listings_scanned, platform_breakdown, and all
    compute_market_stats keys.
    """
    carousell_results, viagogo_results = await asyncio.gather(
        scan_carousell_market(event_name, category),
        scan_viagogo_market(event_name),
    )

    all_listings = carousell_results + viagogo_results

    prices = [
        float(l.get("price", 0))
        for l in all_listings
        if l.get("price") and float(l.get("price", 0)) > 0
    ]

    stats = compute_market_stats(prices, this_listing_price)
    stats["listings_scanned"] = len(all_listings)
    stats["platform_breakdown"] = {
        "Carousell": len(carousell_results),
        "Viagogo": len(viagogo_results),
    }

    return stats


async def check_market_with_fallback(listing_data: dict) -> tuple[dict, bool]:
    """Market rate check with fallback to cached mock data.

    Args:
        listing_data: Dict with at least 'title' and 'price' keys.

    Returns:
        Tuple of (result_dict, is_live).
    """
    try:
        event_name = listing_data.get("title", "")
        category = listing_data.get("category", "")
        price = float(listing_data.get("price", 0))

        result = await check_market_rates(event_name, category, price)

        if result.get("listings_scanned", 0) >= 3:
            return (result, True)

        logger.info("Insufficient market listings (%s), using fallback",
                     result.get("listings_scanned", 0))
        return (MOCK_STEPS["check_market"], False)
    except Exception as e:
        logger.warning("Market rate check failed: %s, using fallback", e)
        return (MOCK_STEPS["check_market"], False)

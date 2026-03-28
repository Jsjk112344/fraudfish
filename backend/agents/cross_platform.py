"""Cross-platform duplicate detection with text similarity."""

from __future__ import annotations

import asyncio
import logging
from difflib import SequenceMatcher

from agents.base import tinyfish_extract
from mock.data import MOCK_STEPS

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.75

# Platform search targets for cross-platform correlation
PLATFORM_TARGETS = {
    "Carousell": [
        ("https://www.viagogo.com/", "Viagogo"),
        ("https://www.facebook.com/marketplace/", "Facebook Marketplace"),
    ],
    "Viagogo": [
        ("https://www.carousell.sg/", "Carousell"),
        ("https://www.facebook.com/marketplace/", "Facebook Marketplace"),
    ],
    "Facebook Marketplace": [
        ("https://www.carousell.sg/", "Carousell"),
        ("https://www.viagogo.com/", "Viagogo"),
    ],
}


def text_similarity(a: str, b: str) -> float:
    """Compute text similarity ratio using SequenceMatcher.

    Args:
        a: First text string.
        b: Second text string.

    Returns:
        Similarity ratio between 0.0 and 1.0.
    """
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _normalize_listings(result) -> list[dict]:
    """Normalize TinyFish extraction result to a list of listing dicts."""
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        return result.get("listings") or result.get("results") or []
    return []


async def search_platform_for_seller(
    platform_url: str, seller_name: str, stealth: bool = True
) -> list[dict]:
    """Search a platform for listings by a seller name."""
    goal = (
        f"Search for seller '{seller_name}' or similar listings. "
        "Extract a JSON array of results with keys: title, seller_name, price."
    )
    result = await tinyfish_extract(
        url=platform_url,
        goal=goal,
        stealth=stealth,
        proxy_country="SG",
        timeout=15.0,
    )
    return _normalize_listings(result)


async def check_cross_platform(
    listing_title: str, seller_name: str, source_platform: str
) -> dict:
    """Search other platforms for duplicate listings.

    Args:
        listing_title: Title of the original listing.
        seller_name: Seller name from the original listing.
        source_platform: Platform where the original listing was found.

    Returns:
        Dict with duplicates_found (bool) and matches (list).
    """
    search_targets = PLATFORM_TARGETS.get(source_platform, [
        ("https://www.carousell.sg/", "Carousell"),
        ("https://www.viagogo.com/", "Viagogo"),
    ])

    search_tasks = [
        search_platform_for_seller(url, seller_name)
        for url, _name in search_targets
    ]
    platform_results = await asyncio.gather(*search_tasks, return_exceptions=True)

    matches = []
    failed_count = sum(1 for r in platform_results if isinstance(r, Exception))
    if failed_count == len(platform_results):
        raise RuntimeError("All cross-platform searches failed")

    for (url, platform_name), results in zip(search_targets, platform_results):
        if isinstance(results, Exception):
            logger.warning("Search failed for %s: %s", platform_name, results)
            continue

        for item in results:
            item_title = item.get("title", "")
            item_seller = item.get("seller_name", "")
            item_price = item.get("price", 0)

            title_sim = text_similarity(listing_title, item_title)
            seller_sim = text_similarity(seller_name, item_seller)
            best_sim = max(title_sim, seller_sim)

            if best_sim >= SIMILARITY_THRESHOLD:
                matches.append({
                    "platform": platform_name,
                    "seller_name": item_seller,
                    "listing_title": item_title,
                    "price": float(item_price) if item_price else 0,
                    "similarity_score": round(best_sim, 2),
                })

    return {
        "duplicates_found": len(matches) > 0,
        "matches": matches,
    }


async def cross_platform_with_fallback(listing_data: dict) -> tuple[dict, bool]:
    """Cross-platform duplicate check with fallback to cached mock data.

    Args:
        listing_data: Dict with title, seller_name, and platform keys.

    Returns:
        Tuple of (result_dict, is_live).
    """
    try:
        result = await check_cross_platform(
            listing_title=listing_data.get("title", ""),
            seller_name=listing_data.get("seller_name", ""),
            source_platform=listing_data.get("platform", "Carousell"),
        )
        return (result, True)
    except Exception as e:
        logger.warning("Cross-platform check failed: %s, using fallback", e)
        return (MOCK_STEPS["cross_platform"], False)

"""IQR-based statistical outlier detection for market price analysis."""

from __future__ import annotations

import statistics


def compute_market_stats(prices: list[float], this_price: float) -> dict:
    """Compute market statistics and IQR-based outlier detection.

    Args:
        prices: List of market prices for similar listings.
        this_price: The price of the listing being investigated.

    Returns:
        Dict with average_price, median_price, min_price, max_price,
        this_listing_percentile, price_deviation_percent, is_outlier,
        outlier_direction.
    """
    if len(prices) < 3:
        return {
            "average_price": None,
            "median_price": None,
            "min_price": None,
            "max_price": None,
            "this_listing_percentile": None,
            "price_deviation_percent": None,
            "is_outlier": False,
            "outlier_direction": None,
        }

    sorted_prices = sorted(prices)
    avg = round(statistics.mean(prices), 2)
    med = round(statistics.median(prices), 2)
    min_p = sorted_prices[0]
    max_p = sorted_prices[-1]

    below_count = sum(1 for p in prices if p < this_price)
    percentile = round((below_count / len(prices)) * 100)

    deviation = round(((this_price - med) / med) * 100, 1)

    # IQR-based outlier detection
    q1, _q2, q3 = statistics.quantiles(prices, n=4)
    iqr = q3 - q1
    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    is_outlier = this_price < lower_fence or this_price > upper_fence

    outlier_direction = None
    if this_price < lower_fence:
        outlier_direction = "suspiciously_low"
    elif this_price > upper_fence:
        outlier_direction = "suspiciously_high"

    return {
        "average_price": avg,
        "median_price": med,
        "min_price": min_p,
        "max_price": max_p,
        "this_listing_percentile": percentile,
        "price_deviation_percent": deviation,
        "is_outlier": is_outlier,
        "outlier_direction": outlier_direction,
    }

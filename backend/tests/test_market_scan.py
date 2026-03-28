"""Tests for market scan agent with parallel platform search and fallback."""

import pytest
from unittest.mock import AsyncMock, patch

from agents.market_scan import check_market_rates, check_market_with_fallback


@pytest.mark.asyncio
async def test_merge_platform_results():
    """check_market_rates merges listings from both platforms and computes stats."""
    carousell_data = [
        {"title": "F1 ticket", "price": 500, "seller": "user1"},
        {"title": "F1 ticket 2", "price": 600, "seller": "user2"},
    ]
    viagogo_data = [
        {"title": "F1 ticket 3", "price": 550, "seller": "user3"},
    ]

    with patch("agents.market_scan.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.side_effect = [carousell_data, viagogo_data]
        result = await check_market_rates("F1 Singapore GP", "Pit Grandstand", 150)

    assert result["listings_scanned"] == 3
    assert result["platform_breakdown"]["Carousell"] == 2
    assert result["platform_breakdown"]["Viagogo"] == 1
    assert "average_price" in result
    assert "median_price" in result
    assert "is_outlier" in result


@pytest.mark.asyncio
async def test_parallel_execution():
    """Both platform searches are launched via asyncio.gather."""
    with patch("agents.market_scan.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.return_value = [{"title": "t", "price": 400, "seller": "s"}]
        await check_market_rates("F1 GP", "General", 300)
        # Should have been called twice: once for Carousell, once for Viagogo
        assert mock_extract.call_count == 2


@pytest.mark.asyncio
async def test_fallback_on_failure():
    """check_market_with_fallback returns mock data when tinyfish_extract raises."""
    with patch("agents.market_scan.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.side_effect = Exception("TinyFish unavailable")
        result, is_live = await check_market_with_fallback({
            "title": "F1 ticket",
            "price": 150,
        })

    assert is_live is False
    assert "listings_scanned" in result


@pytest.mark.asyncio
async def test_fallback_on_insufficient():
    """check_market_with_fallback returns fallback when fewer than 3 listings found."""
    with patch("agents.market_scan.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        # Return only 1 listing total across both platforms
        mock_extract.return_value = [{"title": "t", "price": 400, "seller": "s"}]
        result, is_live = await check_market_with_fallback({
            "title": "F1 ticket",
            "price": 150,
        })

    # With only 2 listings (1 per platform), should fall back
    assert is_live is False
    assert "listings_scanned" in result


@pytest.mark.asyncio
async def test_live_success():
    """check_market_with_fallback returns live data when enough listings found."""
    listings = [
        {"title": "F1 ticket", "price": 500, "seller": "u1"},
        {"title": "F1 ticket 2", "price": 600, "seller": "u2"},
    ]

    with patch("agents.market_scan.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.return_value = listings
        result, is_live = await check_market_with_fallback({
            "title": "F1 Singapore GP Pit Grandstand",
            "price": 150,
        })

    # 2 listings per platform = 4 total >= 3
    assert is_live is True
    assert result["listings_scanned"] == 4

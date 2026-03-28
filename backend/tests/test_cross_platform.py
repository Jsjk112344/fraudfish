"""Tests for cross-platform duplicate detection with text similarity."""

import pytest
from unittest.mock import AsyncMock, patch

from agents.cross_platform import (
    text_similarity,
    check_cross_platform,
    cross_platform_with_fallback,
)


class TestTextSimilarity:
    def test_similarity_high(self):
        score = text_similarity(
            "F1 SG GP 2026 Pit Grandstand",
            "F1 Singapore GP 2026 Pit Grandstand",
        )
        assert score >= 0.75

    def test_similarity_low(self):
        score = text_similarity("F1 tickets", "concert tickets cheap")
        assert score < 0.75

    def test_similarity_exact(self):
        score = text_similarity("hello", "hello")
        assert score == 1.0


@pytest.mark.asyncio
async def test_duplicates_found():
    """check_cross_platform finds duplicates when similarity >= 0.75."""
    mock_results = [
        {
            "title": "F1 SG GP 2026 Pit Grandstand - CHEAP!!",
            "seller_name": "Fast Tickets SG",
            "price": 160,
        }
    ]

    with patch("agents.cross_platform.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.return_value = mock_results
        result = await check_cross_platform(
            listing_title="F1 Singapore GP 2026 Pit Grandstand",
            seller_name="fasttickets_sg",
            source_platform="Carousell",
        )

    assert result["duplicates_found"] is True
    assert len(result["matches"]) >= 1
    assert result["matches"][0]["similarity_score"] >= 0.75


@pytest.mark.asyncio
async def test_no_duplicates():
    """check_cross_platform returns no duplicates when no matches above threshold."""
    mock_results = [
        {
            "title": "Completely unrelated item for sale",
            "seller_name": "different_user",
            "price": 50,
        }
    ]

    with patch("agents.cross_platform.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.return_value = mock_results
        result = await check_cross_platform(
            listing_title="F1 Singapore GP 2026 Pit Grandstand",
            seller_name="fasttickets_sg",
            source_platform="Carousell",
        )

    assert result["duplicates_found"] is False
    assert len(result["matches"]) == 0


@pytest.mark.asyncio
async def test_fallback():
    """cross_platform_with_fallback returns mock data on failure."""
    with patch("agents.cross_platform.tinyfish_extract", new_callable=AsyncMock) as mock_extract:
        mock_extract.side_effect = Exception("TinyFish unavailable")
        result, is_live = await cross_platform_with_fallback({
            "title": "F1 ticket",
            "seller_name": "fasttickets_sg",
            "platform": "Carousell",
        })

    assert is_live is False
    assert "duplicates_found" in result

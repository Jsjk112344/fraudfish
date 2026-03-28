"""Tests for TinyFish base agent wrapper."""

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from agents.base import tinyfish_extract


class TestTinyFishExtract:
    """TinyFish base wrapper handles timeout, success, and error cases."""

    @pytest.mark.asyncio
    async def test_tinyfish_extract_returns_none_on_timeout(self):
        """tinyfish_extract returns None when the call times out."""
        async def slow_thread(*args, **kwargs):
            raise asyncio.TimeoutError()

        with patch("agents.base.asyncio") as mock_asyncio:
            mock_asyncio.TimeoutError = asyncio.TimeoutError
            mock_asyncio.to_thread = AsyncMock(return_value={"title": "test"})
            mock_asyncio.wait_for = AsyncMock(side_effect=asyncio.TimeoutError())
            result = await tinyfish_extract(url="https://example.com", goal="test")
        assert result is None

    @pytest.mark.asyncio
    async def test_tinyfish_extract_returns_result_on_success(self):
        """tinyfish_extract returns result dict when TinyFish call succeeds."""
        expected = {"title": "test"}
        with patch("agents.base._sync_extract", return_value=expected):
            result = await tinyfish_extract(url="https://example.com", goal="test")
        assert result == expected

    @pytest.mark.asyncio
    async def test_tinyfish_extract_returns_none_on_exception(self):
        """tinyfish_extract returns None when TinyFish call raises any exception."""
        with patch("agents.base._sync_extract", side_effect=RuntimeError("fail")):
            result = await tinyfish_extract(url="https://example.com", goal="test")
        assert result is None


# --- Carousell extractor tests ---

from agents.carousell import extract_carousell_listing, normalize_carousell_listing
from agents.telegram import extract_telegram_message


SAMPLE_CAROUSELL_RAW = {
    "title": "F1 Singapore GP 2026 Tickets",
    "price": "350",
    "seller_username": "ticketking99",
    "description": "Selling 2x F1 tickets, Zone 4 walkabout.",
    "transfer_method": "meetup",
    "posted_date": "2 days ago",
    "condition": "new",
    "likes": 5,
    "chats": 3,
}

SAMPLE_TELEGRAM_RAW = {
    "message_text": "Selling F1 tickets $300 each DM me",
    "sender_username": "f1seller",
    "sent_date": "2026-03-20T14:30:00",
    "price": "300",
    "event_name": "F1 Singapore GP 2026",
}


class TestCarousellExtractor:
    """Carousell listing extractor with fallback to mock data."""

    @pytest.mark.asyncio
    async def test_extract_listing_fields(self):
        """extract_carousell_listing returns normalized dict with all fields when TinyFish succeeds."""
        with patch("agents.carousell.tinyfish_extract", new_callable=AsyncMock, return_value=SAMPLE_CAROUSELL_RAW):
            data, is_live = await extract_carousell_listing("https://carousell.sg/p/test-123")
        assert is_live is True
        for key in ("title", "price", "seller_username", "description", "transfer_method", "posted_date", "platform"):
            assert key in data
        assert data["platform"] == "carousell"

    @pytest.mark.asyncio
    async def test_carousell_fallback_on_failure(self):
        """extract_carousell_listing falls back to mock data when TinyFish returns None."""
        with patch("agents.carousell.tinyfish_extract", new_callable=AsyncMock, return_value=None):
            data, is_live = await extract_carousell_listing("https://carousell.sg/p/test-123")
        assert is_live is False
        assert isinstance(data, dict)
        assert len(data) > 0

    @pytest.mark.asyncio
    async def test_carousell_uses_stealth(self):
        """extract_carousell_listing calls tinyfish_extract with stealth=True and proxy_country='SG'."""
        mock_extract = AsyncMock(return_value=SAMPLE_CAROUSELL_RAW)
        with patch("agents.carousell.tinyfish_extract", mock_extract):
            await extract_carousell_listing("https://carousell.sg/p/test-123")
        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args
        assert call_kwargs.kwargs.get("stealth") is True or (len(call_kwargs.args) > 2 and call_kwargs.args[2] is True)
        assert call_kwargs.kwargs.get("proxy_country") == "SG" or (len(call_kwargs.args) > 3 and call_kwargs.args[3] == "SG")

    def test_normalize_carousell_listing(self):
        """normalize_carousell_listing maps raw TinyFish response to standard schema."""
        result = normalize_carousell_listing(SAMPLE_CAROUSELL_RAW)
        assert result["platform"] == "carousell"
        assert isinstance(result["price"], float)
        assert result["title"] == "F1 Singapore GP 2026 Tickets"


class TestTelegramExtractor:
    """Telegram message extractor with fallback to mock data."""

    @pytest.mark.asyncio
    async def test_telegram_extract_fields(self):
        """extract_telegram_message returns normalized dict with all fields when TinyFish succeeds."""
        with patch("agents.telegram.tinyfish_extract", new_callable=AsyncMock, return_value=SAMPLE_TELEGRAM_RAW):
            data, is_live = await extract_telegram_message("https://t.me/channel/123")
        assert is_live is True
        for key in ("message_text", "sender_username", "sent_date", "price", "event_name", "platform"):
            assert key in data
        assert data["platform"] == "telegram"

    @pytest.mark.asyncio
    async def test_telegram_fallback_on_failure(self):
        """extract_telegram_message falls back to mock data when TinyFish returns None."""
        with patch("agents.telegram.tinyfish_extract", new_callable=AsyncMock, return_value=None):
            data, is_live = await extract_telegram_message("https://t.me/channel/123")
        assert is_live is False
        assert isinstance(data, dict)
        assert len(data) > 0

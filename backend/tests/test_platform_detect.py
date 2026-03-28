"""Tests for platform detection from URLs."""

import pytest

from agents.platform_detect import detect_platform


class TestDetectPlatform:
    """Platform detection correctly routes marketplace URLs."""

    def test_carousell_with_www(self):
        assert detect_platform("https://www.carousell.sg/p/some-listing-123456789/") == "carousell"

    def test_carousell_without_www(self):
        assert detect_platform("https://carousell.sg/p/listing-title-123/") == "carousell"

    def test_telegram_tme(self):
        assert detect_platform("https://t.me/channelname/12345") == "telegram"

    def test_telegram_web(self):
        assert detect_platform("https://web.telegram.org/k/#-1001234567890") == "telegram"

    def test_viagogo_returns_none(self):
        assert detect_platform("https://www.viagogo.com/something") is None

    def test_google_returns_none(self):
        assert detect_platform("https://google.com") is None

    def test_not_a_url_returns_none(self):
        assert detect_platform("not-a-url") is None

    def test_empty_string_returns_none(self):
        assert detect_platform("") is None

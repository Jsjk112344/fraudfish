"""Tests for Google fallback and official price verification agents."""

import pytest
from unittest.mock import AsyncMock, patch


# --- Google Fallback Tests ---


@pytest.mark.asyncio
@patch("agents.google_fallback.tinyfish_extract", new_callable=AsyncMock)
async def test_google_search_returns_event_data(mock_extract):
    """google_event_search returns (dict, True) with expected keys when TinyFish succeeds."""
    from agents.google_fallback import google_event_search

    mock_extract.return_value = {
        "event_name": "F1 Singapore GP",
        "venue": "Marina Bay",
        "date": "2026-10-03",
        "face_value": 298,
        "source": "singaporegp.sg",
        "found": True,
    }

    result = await google_event_search("F1 Singapore GP")
    data, is_live = result

    assert is_live is True
    for key in ("event_name", "venue", "date", "face_value", "source", "found"):
        assert key in data
    assert data["found"] is True


@pytest.mark.asyncio
@patch("agents.google_fallback.tinyfish_extract", new_callable=AsyncMock)
async def test_google_search_event_not_found(mock_extract):
    """google_event_search returns found=False and unverified=True when event not in results."""
    from agents.google_fallback import google_event_search

    mock_extract.return_value = {"found": False}

    result = await google_event_search("Nonexistent Event")
    data, is_live = result

    assert data["found"] is False
    assert data["unverified"] is True
    assert is_live is True


@pytest.mark.asyncio
@patch("agents.google_fallback.tinyfish_extract", new_callable=AsyncMock)
async def test_google_search_tinyfish_failure(mock_extract):
    """google_event_search returns (None, False) when TinyFish returns None."""
    from agents.google_fallback import google_event_search

    mock_extract.return_value = None

    result = await google_event_search("F1 Singapore GP")
    data, is_live = result

    assert data is None
    assert is_live is False


def test_build_google_search_url():
    """build_google_search_url constructs proper Google search URL."""
    from agents.google_fallback import build_google_search_url

    result = build_google_search_url("F1 Singapore GP 2026")
    assert result.startswith("https://www.google.com/search?q=")
    assert "singapore+tickets+price" in result


def test_build_google_search_url_encodes_spaces():
    """build_google_search_url properly encodes spaces."""
    from agents.google_fallback import build_google_search_url

    result = build_google_search_url("Taylor Swift Eras Tour")
    assert "Taylor+Swift" in result or "Taylor%20Swift" in result


# --- Official Price Verification Tests ---


@pytest.mark.asyncio
@patch("agents.official_price.tinyfish_extract", new_callable=AsyncMock)
async def test_event_verification_fields(mock_extract):
    """verify_event_official returns dict with all expected keys from Ticketmaster."""
    from agents.official_price import verify_event_official

    mock_extract.return_value = {
        "event_name": "Coldplay",
        "venue": "National Stadium",
        "date": "2026-05-10",
        "face_value": 168,
        "sold_out": False,
    }

    result = await verify_event_official("Coldplay Music of the Spheres")
    data, is_live = result

    for key in ("event_name", "venue", "date", "face_value", "sold_out", "source", "unverified"):
        assert key in data, f"Missing key: {key}"
    assert data["source"] == "Ticketmaster SG"
    assert data["unverified"] is False
    assert is_live is True


@pytest.mark.asyncio
@patch("agents.official_price.tinyfish_extract", new_callable=AsyncMock)
async def test_f1_event_checks_official_first(mock_extract):
    """verify_event_official checks F1 official site first for GP events."""
    from agents.official_price import verify_event_official

    mock_extract.side_effect = [
        {
            "event_name": "F1 Singapore Grand Prix 2026",
            "venue": "Marina Bay Street Circuit",
            "date": "2026-10-03",
            "face_value_low": 298,
            "face_value_high": 1288,
            "sold_out": False,
            "available_categories": ["Zone 4", "Premier Walkabout"],
        },
        None,  # second call not needed
    ]

    result = await verify_event_official("F1 Singapore Grand Prix 2026")
    data, is_live = result

    assert "F1 Official" in data["source"]
    # First call should be to singaporegp.sg
    first_call_url = mock_extract.call_args_list[0][1].get("url", mock_extract.call_args_list[0][0][0] if mock_extract.call_args_list[0][0] else "")
    assert first_call_url == "https://www.singaporegp.sg"


@pytest.mark.asyncio
@patch("agents.official_price.tinyfish_extract", new_callable=AsyncMock)
async def test_non_f1_checks_ticketmaster_first(mock_extract):
    """verify_event_official checks Ticketmaster SG first for non-F1 events."""
    from agents.official_price import verify_event_official

    mock_extract.side_effect = [
        {
            "event_name": "Taylor Swift Eras Tour",
            "venue": "National Stadium",
            "date": "2026-03-15",
            "face_value": 348,
            "sold_out": True,
        },
        None,
    ]

    result = await verify_event_official("Taylor Swift Eras Tour")
    data, is_live = result

    assert data["source"] == "Ticketmaster SG"
    first_call_url = mock_extract.call_args_list[0][1].get("url", mock_extract.call_args_list[0][0][0] if mock_extract.call_args_list[0][0] else "")
    assert first_call_url.startswith("https://www.ticketmaster.sg")


@pytest.mark.asyncio
@patch("agents.official_price.google_event_search", new_callable=AsyncMock)
@patch("agents.official_price.tinyfish_extract", new_callable=AsyncMock)
async def test_falls_through_to_google(mock_extract, mock_google):
    """verify_event_official falls through to Google when official sites return None."""
    from agents.official_price import verify_event_official

    mock_extract.return_value = None
    mock_google.return_value = (
        {
            "event_name": "Test",
            "found": True,
            "source": "Google Search",
            "unverified": False,
            "venue": "",
            "date": "",
            "face_value": 100,
            "sold_out": None,
        },
        True,
    )

    result = await verify_event_official("Unknown Event")
    data, is_live = result

    assert data["source"] == "Google Search"
    mock_google.assert_called()


@pytest.mark.asyncio
@patch("agents.official_price.google_event_search", new_callable=AsyncMock)
@patch("agents.official_price.tinyfish_extract", new_callable=AsyncMock)
async def test_all_sources_fail_returns_mock(mock_extract, mock_google):
    """verify_event_official returns mock data when all sources fail."""
    from agents.official_price import verify_event_official

    mock_extract.return_value = None
    mock_google.return_value = (None, False)

    result = await verify_event_official("Unknown Event")
    data, is_live = result

    assert is_live is False


def test_is_f1_event_true_cases():
    """is_f1_event returns True for F1-related event names."""
    from agents.official_price import is_f1_event

    assert is_f1_event("F1 Singapore Grand Prix") is True
    assert is_f1_event("Singapore GP 2026") is True
    assert is_f1_event("Formula 1 Night Race") is True


def test_is_f1_event_false_cases():
    """is_f1_event returns False for non-F1 event names."""
    from agents.official_price import is_f1_event

    assert is_f1_event("Taylor Swift Eras Tour") is False
    assert is_f1_event("Coldplay Concert") is False

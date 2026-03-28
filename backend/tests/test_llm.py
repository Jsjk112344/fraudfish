"""Unit tests for LLM classification module with mocked OpenAI client."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity


# --- Test data ---

SAMPLE_EVIDENCE = {
    "listing": {"price": 150.0, "title": "Taylor Swift Eras Tour", "seller_name": "ticketking99"},
    "seller": {"username": "ticketking99", "account_age_days": 12, "reviews_count": 0},
    "event": {"face_value": 168.0, "sold_out": False, "name": "Taylor Swift Eras Tour"},
    "market": {"average_price": 180.0, "listings_count": 25},
    "cross_platform": {"duplicates_found": False, "matches": []},
}

SAMPLE_VERDICT = VerdictResult(
    category=ClassificationCategory.LEGITIMATE,
    confidence=72.0,
    reasoning="Price is close to face value. New seller but no red flags found.",
    signals=[
        Signal(name="Pricing Anomaly", severity=SignalSeverity.CLEAR, segmentsFilled=1),
        Signal(name="Seller Reputation", severity=SignalSeverity.WARNING, segmentsFilled=3),
        Signal(name="Event Verification", severity=SignalSeverity.CLEAR, segmentsFilled=1),
        Signal(name="Cross-Platform Duplicates", severity=SignalSeverity.CLEAR, segmentsFilled=1),
        Signal(name="Listing Authenticity", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
    ],
)


def _make_mock_client(verdict: VerdictResult):
    """Build a mock OpenAI client that returns a completion with the given verdict."""
    mock_message = MagicMock()
    mock_message.parsed = verdict
    mock_choice = MagicMock()
    mock_choice.message = mock_message
    mock_completion = MagicMock()
    mock_completion.choices = [mock_choice]

    mock_client = MagicMock()
    mock_client.beta.chat.completions.parse = AsyncMock(return_value=mock_completion)
    return mock_client


@pytest.mark.asyncio
async def test_classify_with_llm_returns_verdict_result():
    """classify_with_llm returns the VerdictResult from OpenAI parse()."""
    mock_client = _make_mock_client(SAMPLE_VERDICT)

    with patch("classify.llm._get_client", return_value=mock_client):
        from classify.llm import classify_with_llm

        result = await classify_with_llm(SAMPLE_EVIDENCE)

    assert isinstance(result, VerdictResult)
    assert result.category == ClassificationCategory.LEGITIMATE
    assert result.confidence == 72.0


@pytest.mark.asyncio
async def test_classify_with_llm_sends_full_evidence():
    """User message contains json.dumps of the full evidence dict."""
    mock_client = _make_mock_client(SAMPLE_VERDICT)

    with patch("classify.llm._get_client", return_value=mock_client):
        from classify.llm import classify_with_llm

        await classify_with_llm(SAMPLE_EVIDENCE)

        call_kwargs = mock_client.beta.chat.completions.parse.call_args
        messages = call_kwargs.kwargs.get("messages") or call_kwargs[1].get("messages")
        user_message = messages[1]["content"]
        assert json.dumps(SAMPLE_EVIDENCE, indent=2, default=str) in user_message


@pytest.mark.asyncio
async def test_classify_with_llm_uses_gpt4o():
    """Model argument is gpt-4o."""
    mock_client = _make_mock_client(SAMPLE_VERDICT)

    with patch("classify.llm._get_client", return_value=mock_client):
        from classify.llm import classify_with_llm

        await classify_with_llm(SAMPLE_EVIDENCE)

        call_kwargs = mock_client.beta.chat.completions.parse.call_args
        model = call_kwargs.kwargs.get("model") or call_kwargs[1].get("model")
        assert model == "gpt-4o"


@pytest.mark.asyncio
async def test_classify_with_llm_sets_max_tokens():
    """max_tokens is set to 2000."""
    mock_client = _make_mock_client(SAMPLE_VERDICT)

    with patch("classify.llm._get_client", return_value=mock_client):
        from classify.llm import classify_with_llm

        await classify_with_llm(SAMPLE_EVIDENCE)

        call_kwargs = mock_client.beta.chat.completions.parse.call_args
        max_tokens = call_kwargs.kwargs.get("max_tokens") or call_kwargs[1].get("max_tokens")
        assert max_tokens == 2000


@pytest.mark.asyncio
async def test_classify_with_llm_uses_verdict_result_format():
    """response_format is VerdictResult."""
    mock_client = _make_mock_client(SAMPLE_VERDICT)

    with patch("classify.llm._get_client", return_value=mock_client):
        from classify.llm import classify_with_llm

        await classify_with_llm(SAMPLE_EVIDENCE)

        call_kwargs = mock_client.beta.chat.completions.parse.call_args
        response_format = call_kwargs.kwargs.get("response_format") or call_kwargs[1].get("response_format")
        assert response_format is VerdictResult


def test_format_evidence_includes_all_sections():
    """format_evidence output contains all 5 evidence sections."""
    from classify.llm import format_evidence

    result = format_evidence(SAMPLE_EVIDENCE)
    for section in ["listing", "seller", "event", "market", "cross_platform"]:
        assert section in result


def test_system_prompt_contains_categories():
    """SYSTEM_PROMPT mentions all 4 classification categories."""
    from classify.llm import SYSTEM_PROMPT

    for category in ["LEGITIMATE", "SCALPING_VIOLATION", "LIKELY_SCAM", "COUNTERFEIT_RISK"]:
        assert category in SYSTEM_PROMPT


def test_system_prompt_contains_signal_names():
    """SYSTEM_PROMPT mentions all 5 signal names."""
    from classify.llm import SYSTEM_PROMPT

    for signal in ["Pricing Anomaly", "Seller Reputation", "Event Verification",
                    "Cross-Platform Duplicates", "Listing Authenticity"]:
        assert signal in SYSTEM_PROMPT

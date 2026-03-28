"""Integration tests for the full classification pipeline (rules -> LLM -> validation)."""

from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity


# --- Test evidence ---

EXTREME_UNDERPRICING_EVIDENCE = {
    "listing": {"price": 50.0, "title": "BTS Concert Cat 1", "seller_name": "cheaptickets"},
    "seller": {"username": "cheaptickets", "account_age_days": 3, "reviews_count": 0},
    "event": {"face_value": 698.0, "sold_out": True, "name": "BTS World Tour"},
    "market": {"average_price": 700.0, "listings_count": 30},
    "cross_platform": {"duplicates_found": True, "matches": ["facebook"]},
}

AMBIGUOUS_EVIDENCE = {
    "listing": {"price": 150.0, "title": "Taylor Swift Eras Tour", "seller_name": "ticketking99"},
    "seller": {"username": "ticketking99", "account_age_days": 45, "reviews_count": 5},
    "event": {"face_value": 168.0, "sold_out": False, "name": "Taylor Swift Eras Tour"},
    "market": {"average_price": 180.0, "listings_count": 25},
    "cross_platform": {"duplicates_found": False, "matches": []},
}

LLM_VERDICT = VerdictResult(
    category=ClassificationCategory.LEGITIMATE,
    confidence=72.0,
    reasoning="Price is close to face value. Seller is relatively new but no red flags.",
    signals=[
        Signal(name="Pricing Anomaly", severity=SignalSeverity.CLEAR, segmentsFilled=1),
        Signal(name="Seller Reputation", severity=SignalSeverity.WARNING, segmentsFilled=3),
        Signal(name="Event Verification", severity=SignalSeverity.CLEAR, segmentsFilled=1),
        Signal(name="Cross-Platform Duplicates", severity=SignalSeverity.CLEAR, segmentsFilled=1),
        Signal(name="Listing Authenticity", severity=SignalSeverity.NEUTRAL, segmentsFilled=2),
    ],
)


@pytest.mark.asyncio
async def test_classify_rules_match_skips_llm():
    """When rules match (extreme underpricing), LLM is never called."""
    mock_llm = AsyncMock(return_value=LLM_VERDICT)

    with patch("classify.classify_with_llm", mock_llm):
        from classify import classify

        result = await classify(EXTREME_UNDERPRICING_EVIDENCE)

    assert result.category == ClassificationCategory.LIKELY_SCAM
    mock_llm.assert_not_called()


@pytest.mark.asyncio
async def test_classify_ambiguous_calls_llm():
    """When rules return None (ambiguous), classify calls LLM."""
    mock_llm = AsyncMock(return_value=LLM_VERDICT)

    with patch("classify.classify_with_llm", mock_llm):
        from classify import classify

        result = await classify(AMBIGUOUS_EVIDENCE)

    mock_llm.assert_called_once_with(AMBIGUOUS_EVIDENCE)
    assert isinstance(result, VerdictResult)


@pytest.mark.asyncio
async def test_classify_always_validates():
    """validate_verdict is called on all paths (rules and LLM)."""
    mock_llm = AsyncMock(return_value=LLM_VERDICT)

    with patch("classify.classify_with_llm", mock_llm), \
         patch("classify.validate_verdict", wraps=lambda v, e: v) as mock_validate:
        from classify import classify

        # Rules path
        await classify(EXTREME_UNDERPRICING_EVIDENCE)
        assert mock_validate.called

        mock_validate.reset_mock()

        # LLM path
        await classify(AMBIGUOUS_EVIDENCE)
        assert mock_validate.called


@pytest.mark.asyncio
async def test_classify_llm_error_falls_back():
    """When LLM raises Exception, classify returns degraded verdict (not crash)."""
    mock_llm = AsyncMock(side_effect=Exception("OpenAI rate limit"))

    with patch("classify.classify_with_llm", mock_llm):
        from classify import classify

        result = await classify(AMBIGUOUS_EVIDENCE)

    assert isinstance(result, VerdictResult)
    assert "unavailable" in result.reasoning.lower() or "classification unavailable" in result.reasoning.lower()


@pytest.mark.asyncio
async def test_classify_fallback_verdict_has_five_signals():
    """Fallback verdict from LLM error still has exactly 5 signals."""
    mock_llm = AsyncMock(side_effect=Exception("API error"))

    with patch("classify.classify_with_llm", mock_llm):
        from classify import classify

        result = await classify(AMBIGUOUS_EVIDENCE)

    assert len(result.signals) == 5
    signal_names = {s.name for s in result.signals}
    assert "Pricing Anomaly" in signal_names
    assert "Seller Reputation" in signal_names
    assert "Event Verification" in signal_names
    assert "Cross-Platform Duplicates" in signal_names
    assert "Listing Authenticity" in signal_names


@pytest.mark.asyncio
async def test_classify_returns_verdict_result_type():
    """classify() always returns a VerdictResult instance."""
    mock_llm = AsyncMock(return_value=LLM_VERDICT)

    with patch("classify.classify_with_llm", mock_llm):
        from classify import classify

        # Rules path
        result1 = await classify(EXTREME_UNDERPRICING_EVIDENCE)
        assert isinstance(result1, VerdictResult)

        # LLM path
        result2 = await classify(AMBIGUOUS_EVIDENCE)
        assert isinstance(result2, VerdictResult)

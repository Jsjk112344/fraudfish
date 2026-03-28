"""TinyFish async wrapper with timeout, streaming URL capture, and progress forwarding."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Callable

try:
    from tinyfish import TinyFish, EventType, RunStatus
except ImportError:
    TinyFish = None
    EventType = None
    RunStatus = None

logger = logging.getLogger(__name__)

_client = None


def get_client() -> "TinyFish":
    """Lazily initialize TinyFish client (reads TINYFISH_API_KEY from env)."""
    global _client
    if _client is None:
        if TinyFish is None:
            raise RuntimeError("tinyfish package is not installed")
        _client = TinyFish()
    return _client


@dataclass
class TinyFishResult:
    """Rich result from a TinyFish extraction, including live stream metadata."""
    data: dict | None = None
    streaming_url: str | None = None
    progress_messages: list[str] = field(default_factory=list)
    success: bool = False


def _sync_extract(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    on_streaming_url: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> TinyFishResult:
    """Synchronous TinyFish extraction -- runs in a thread for async compat.

    Captures STREAMING_URL and PROGRESS events alongside the final result.
    """
    kwargs: dict = {"url": url, "goal": goal}
    if stealth:
        kwargs["browser_profile"] = "stealth"
    if proxy_country:
        kwargs["proxy_config"] = {"enabled": True, "country_code": proxy_country}

    result = TinyFishResult()

    try:
        with get_client().agent.stream(**kwargs) as stream:
            for event in stream:
                # Live browser preview URL
                if event.type == EventType.STREAMING_URL:
                    url_val = getattr(event, 'streaming_url', None)
                    if url_val:
                        result.streaming_url = url_val
                        if on_streaming_url:
                            on_streaming_url(url_val)

                # Agent progress narration
                elif event.type == EventType.PROGRESS:
                    msg = getattr(event, 'purpose', None) or getattr(event, 'message', None) or ''
                    if msg:
                        result.progress_messages.append(msg)
                        if on_progress:
                            on_progress(msg)

                # Final result
                elif event.type == EventType.COMPLETE and event.status == RunStatus.COMPLETED:
                    result.data = event.result_json
                    result.success = True
                    return result

                elif event.type == EventType.ERROR:
                    return result

    except Exception as e:
        logger.warning("TinyFish extraction failed for %s: %s", url, e)
        return result

    return result


async def tinyfish_extract(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    timeout: float = 15.0,
) -> dict | None:
    """Async TinyFish extraction with timeout. Returns result dict or None.

    This is the simple API — returns only the final data. Use
    tinyfish_extract_rich() if you need the streaming URL and progress events.
    """
    try:
        rich = await asyncio.wait_for(
            asyncio.to_thread(
                _sync_extract, url, goal, stealth, proxy_country
            ),
            timeout=timeout,
        )
        return rich.data
    except asyncio.TimeoutError:
        logger.warning("TinyFish extraction timed out for %s", url)
        return None
    except Exception as e:
        logger.warning("TinyFish extraction error for %s: %s", url, e)
        return None


async def tinyfish_extract_rich(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    timeout: float = 15.0,
    on_streaming_url: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> TinyFishResult:
    """Async TinyFish extraction that captures streaming URL and progress events.

    Use this when you want to forward the live browser preview and agent
    narration to the frontend.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                _sync_extract, url, goal, stealth, proxy_country,
                on_streaming_url, on_progress,
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.warning("TinyFish extraction timed out for %s", url)
        return TinyFishResult()
    except Exception as e:
        logger.warning("TinyFish extraction error for %s: %s", url, e)
        return TinyFishResult()

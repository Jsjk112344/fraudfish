"""TinyFish async wrapper with timeout, streaming URL capture, and progress forwarding."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Callable

import httpx

try:
    from tinyfish import TinyFish, EventType, RunStatus
except ImportError:
    TinyFish = None
    EventType = None
    RunStatus = None

logger = logging.getLogger(__name__)

TINYFISH_SSE_URL = "https://agent.tinyfish.ai/v1/automation/run-sse"

_client = None


def get_client() -> "TinyFish":
    """Lazily initialize TinyFish client (reads TINYFISH_API_KEY from env)."""
    global _client
    if _client is None:
        if TinyFish is None:
            raise RuntimeError("tinyfish package is not installed")
        _client = TinyFish()
    return _client


def _get_api_key() -> str:
    """Get TinyFish API key from environment."""
    key = os.getenv("TINYFISH_API_KEY", "")
    if not key:
        raise RuntimeError("TINYFISH_API_KEY not set")
    return key


@dataclass
class TinyFishResult:
    """Rich result from a TinyFish extraction, including live stream metadata."""
    data: dict | None = None
    streaming_url: str | None = None
    progress_messages: list[str] = field(default_factory=list)
    success: bool = False


def _sync_extract_raw(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    on_streaming_url: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> TinyFishResult:
    """Direct REST SSE extraction — bypasses SDK to get correct result field.

    The TinyFish SDK v0.2.4 has a bug where CompleteEvent.result_json is always
    None (the API sends 'result' but the SDK model expects 'result_json').
    This function parses the raw SSE stream directly.
    """
    body: dict = {"url": url, "goal": goal}
    if stealth:
        body["browser_profile"] = "stealth"
    if proxy_country:
        body["proxy_config"] = {"enabled": True, "country_code": proxy_country}

    result = TinyFishResult()

    try:
        with httpx.stream(
            "POST",
            TINYFISH_SSE_URL,
            headers={
                "X-API-Key": _get_api_key(),
                "Content-Type": "application/json",
            },
            json=body,
            timeout=90.0,
        ) as resp:
            if resp.status_code != 200:
                logger.warning("TinyFish returned %s for %s", resp.status_code, url)
                return result

            buffer = ""
            for chunk in resp.iter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    event_block, buffer = buffer.split("\n\n", 1)
                    for line in event_block.strip().split("\n"):
                        if not line.startswith("data:"):
                            continue
                        raw = line[5:].strip()
                        try:
                            data = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        etype = data.get("type", "")

                        if etype == "STREAMING_URL":
                            streaming_url = data.get("streaming_url")
                            if streaming_url:
                                result.streaming_url = streaming_url
                                if on_streaming_url:
                                    on_streaming_url(streaming_url)

                        elif etype == "PROGRESS":
                            msg = data.get("purpose", "")
                            if msg:
                                result.progress_messages.append(msg)
                                if on_progress:
                                    on_progress(msg)

                        elif etype == "COMPLETE":
                            status = data.get("status", "")
                            if status == "COMPLETED":
                                result.data = data.get("result")
                                result.success = result.data is not None
                            return result

    except Exception as e:
        logger.warning("TinyFish extraction failed for %s: %s", url, e)

    return result


async def tinyfish_extract(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    timeout: float = 30.0,
) -> dict | None:
    """Async TinyFish extraction with timeout. Returns result dict or None.

    This is the simple API — returns only the final data. Use
    tinyfish_extract_rich() if you need the streaming URL and progress events.
    """
    try:
        rich = await asyncio.wait_for(
            asyncio.to_thread(
                _sync_extract_raw, url, goal, stealth, proxy_country
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
    timeout: float = 30.0,
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
                _sync_extract_raw, url, goal, stealth, proxy_country,
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

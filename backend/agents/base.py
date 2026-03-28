"""TinyFish async wrapper using run-async + polling for reliable execution.

Uses the async API to fire runs, then polls for status/results. This avoids
the false timeout problem where our code gives up but the TinyFish agent is
still running fine on their servers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from typing import AsyncGenerator, Callable

import httpx

logger = logging.getLogger(__name__)

TINYFISH_BASE = "https://agent.tinyfish.ai"
TINYFISH_SSE_URL = f"{TINYFISH_BASE}/v1/automation/run-sse"
TINYFISH_ASYNC_URL = f"{TINYFISH_BASE}/v1/automation/run-async"
TINYFISH_RUN_URL = f"{TINYFISH_BASE}/v1/runs"

# Default poll settings — generous to avoid false timeouts
DEFAULT_POLL_INTERVAL = 5.0   # seconds between polls
DEFAULT_TIMEOUT = 300.0        # 5 minutes — agents take time on Cloudflare sites


def _get_api_key() -> str:
    key = os.getenv("TINYFISH_API_KEY", "")
    if not key:
        raise RuntimeError("TINYFISH_API_KEY not set")
    return key


def _build_body(url: str, goal: str, stealth: bool = False, proxy_country: str | None = None) -> dict:
    body: dict = {"url": url, "goal": goal}
    if stealth:
        body["browser_profile"] = "stealth"
    if proxy_country:
        body["proxy_config"] = {"enabled": True, "country_code": proxy_country}
    return body


@dataclass
class TinyFishResult:
    """Result from a TinyFish extraction."""
    data: dict | None = None
    streaming_url: str | None = None
    progress_messages: list[str] = field(default_factory=list)
    run_id: str | None = None
    success: bool = False


# ---------------------------------------------------------------------------
# Primary API: async start + poll (no false timeouts)
# ---------------------------------------------------------------------------

async def tinyfish_start_run(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
) -> str | None:
    """Fire a TinyFish run asynchronously. Returns run_id or None."""
    api_key = _get_api_key()
    body = _build_body(url, goal, stealth, proxy_country)
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                TINYFISH_ASYNC_URL,
                headers={"X-API-Key": api_key, "Content-Type": "application/json"},
                json=body,
            )
            if resp.status_code == 200:
                return resp.json().get("run_id")
            else:
                logger.warning("TinyFish async start failed (%s) for %s: %s",
                               resp.status_code, url, resp.text[:200])
    except Exception as e:
        logger.warning("TinyFish async start error for %s: %s", url, e)
    return None


async def tinyfish_poll_run(
    run_id: str,
    timeout: float = DEFAULT_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    on_status: Callable[[str, dict], None] | None = None,
) -> TinyFishResult:
    """Poll a TinyFish run until completion.

    Args:
        on_status: Optional callback(status, run_data) called each poll cycle.
                   Useful for forwarding progress to frontend.
    """
    api_key = _get_api_key()
    result = TinyFishResult(run_id=run_id)
    deadline = asyncio.get_event_loop().time() + timeout

    async with httpx.AsyncClient(timeout=15.0) as client:
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(poll_interval)
            try:
                resp = await client.get(
                    f"{TINYFISH_RUN_URL}/{run_id}",
                    headers={"X-API-Key": api_key},
                )
                if resp.status_code != 200:
                    continue

                run_data = resp.json()
                status = run_data.get("status", "")

                if on_status:
                    on_status(status, run_data)

                if status == "COMPLETED":
                    result.data = run_data.get("result")
                    result.success = result.data is not None
                    return result
                elif status in ("FAILED", "CANCELLED"):
                    logger.warning("TinyFish run %s: %s", run_id, status)
                    return result

            except Exception as e:
                logger.warning("TinyFish poll error for %s: %s", run_id, e)

    logger.warning("TinyFish run %s timed out after %ss", run_id, timeout)
    return result


async def tinyfish_extract(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict | None:
    """Simple extraction: fire async run, poll for result. Returns data or None."""
    run_id = await tinyfish_start_run(url, goal, stealth, proxy_country)
    if not run_id:
        return None
    result = await tinyfish_poll_run(run_id, timeout=timeout)
    return result.data


async def tinyfish_extract_batch(
    tasks: list[dict],
    timeout: float = DEFAULT_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    on_progress: Callable[[int, str, dict], None] | None = None,
) -> list[dict | None]:
    """Fire multiple extractions in parallel, poll all until complete.

    Each task: {url, goal, stealth?, proxy_country?}
    on_progress: callback(task_index, status, run_data) per poll cycle.
    Returns results in same order as tasks.
    """
    api_key = _get_api_key()

    # Phase 1: Fire all runs
    run_ids: list[str | None] = []
    for task in tasks:
        rid = await tinyfish_start_run(
            task["url"], task["goal"],
            task.get("stealth", False),
            task.get("proxy_country"),
        )
        run_ids.append(rid)

    # Phase 2: Poll all pending runs
    results: list[dict | None] = [None] * len(tasks)
    pending = {i: rid for i, rid in enumerate(run_ids) if rid is not None}
    deadline = asyncio.get_event_loop().time() + timeout

    async with httpx.AsyncClient(timeout=15.0) as client:
        while pending and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(poll_interval)
            for i, rid in list(pending.items()):
                try:
                    resp = await client.get(
                        f"{TINYFISH_RUN_URL}/{rid}",
                        headers={"X-API-Key": api_key},
                    )
                    if resp.status_code != 200:
                        continue
                    run_data = resp.json()
                    status = run_data.get("status", "")

                    if on_progress:
                        on_progress(i, status, run_data)

                    if status == "COMPLETED":
                        results[i] = run_data.get("result")
                        del pending[i]
                    elif status in ("FAILED", "CANCELLED"):
                        del pending[i]
                except Exception as e:
                    logger.warning("TinyFish poll error for %s: %s", rid, e)

    if pending:
        logger.warning("TinyFish batch: %d/%d runs still pending after %ss",
                       len(pending), len(tasks), timeout)

    return results


# ---------------------------------------------------------------------------
# SSE streaming API (for live browser preview + progress narration)
# ---------------------------------------------------------------------------

def _sync_extract_sse(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    on_streaming_url: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> TinyFishResult:
    """Direct REST SSE extraction for live browser preview.

    Only use this when you need the streaming_url for the live preview iframe.
    For simple data extraction, use tinyfish_extract() (async + poll) instead.
    """
    body = _build_body(url, goal, stealth, proxy_country)
    result = TinyFishResult()

    try:
        with httpx.stream(
            "POST",
            TINYFISH_SSE_URL,
            headers={"X-API-Key": _get_api_key(), "Content-Type": "application/json"},
            json=body,
            timeout=httpx.Timeout(connect=15.0, read=600.0, write=15.0, pool=15.0),
        ) as resp:
            if resp.status_code != 200:
                logger.warning("TinyFish SSE returned %s for %s", resp.status_code, url)
                return result

            buffer = ""
            for chunk in resp.iter_text():
                buffer += chunk
                while "\n\n" in buffer:
                    event_block, buffer = buffer.split("\n\n", 1)
                    for line in event_block.strip().split("\n"):
                        if not line.startswith("data:"):
                            continue
                        try:
                            data = json.loads(line[5:].strip())
                        except json.JSONDecodeError:
                            continue

                        etype = data.get("type", "")

                        if etype == "STREAMING_URL":
                            surl = data.get("streaming_url")
                            if surl:
                                result.streaming_url = surl
                                if on_streaming_url:
                                    on_streaming_url(surl)

                        elif etype == "PROGRESS":
                            msg = data.get("purpose", "")
                            if msg:
                                result.progress_messages.append(msg)
                                if on_progress:
                                    on_progress(msg)

                        elif etype == "COMPLETE":
                            if data.get("status") == "COMPLETED":
                                result.data = data.get("result")
                                result.success = result.data is not None
                            return result

    except Exception as e:
        logger.warning("TinyFish SSE failed for %s: %s", url, e)

    return result


async def tinyfish_extract_rich(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    on_streaming_url: Callable[[str], None] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> TinyFishResult:
    """SSE extraction with streaming URL and progress callbacks.

    No artificial timeout — waits for TinyFish to finish naturally.
    The SSE read timeout is 10 minutes (httpx.Timeout read=600s).
    """
    return await asyncio.to_thread(
        _sync_extract_sse, url, goal, stealth, proxy_country,
        on_streaming_url, on_progress,
    )

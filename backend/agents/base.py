"""TinyFish async wrapper using run-async + polling for reliable execution.

Uses the async API to fire runs, then polls for status/results. This avoids
the false timeout problem where our code gives up but the TinyFish agent is
still running fine on their servers.

Polling also captures streaming_url from the run object — no need for the
thread-unsafe SSE path to get live browser previews.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Callable

import httpx

logger = logging.getLogger(__name__)

TINYFISH_BASE = "https://agent.tinyfish.ai"
TINYFISH_ASYNC_URL = f"{TINYFISH_BASE}/v1/automation/run-async"
TINYFISH_BATCH_URL = f"{TINYFISH_BASE}/v1/automation/run-batch"
TINYFISH_RUN_URL = f"{TINYFISH_BASE}/v1/runs"
TINYFISH_CANCEL_URL = f"{TINYFISH_BASE}/v1/automation/cancel"

# Default poll settings — generous to avoid false timeouts
DEFAULT_POLL_INTERVAL = 4.0   # seconds between polls
DEFAULT_TIMEOUT = 600.0        # 10 minutes — agents take time on Cloudflare sites, never timeout prematurely


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


async def tinyfish_start_batch(
    tasks: list[dict],
) -> list[str]:
    """Fire multiple TinyFish runs via the batch endpoint. Returns run_ids."""
    api_key = _get_api_key()
    runs = [_build_body(t["url"], t["goal"], t.get("stealth", False), t.get("proxy_country")) for t in tasks]
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                TINYFISH_BATCH_URL,
                headers={"X-API-Key": api_key, "Content-Type": "application/json"},
                json={"runs": runs},
            )
            if resp.status_code == 200:
                return resp.json().get("run_ids", [])
            else:
                logger.warning("TinyFish batch start failed (%s): %s",
                               resp.status_code, resp.text[:200])
    except Exception as e:
        logger.warning("TinyFish batch start error: %s", e)
    # Fallback: fire individually
    ids = []
    for t in tasks:
        rid = await tinyfish_start_run(t["url"], t["goal"], t.get("stealth", False), t.get("proxy_country"))
        ids.append(rid or "")
    return ids


async def tinyfish_poll_run(
    run_id: str,
    timeout: float = DEFAULT_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    on_status: Callable[[str, dict], None] | None = None,
) -> TinyFishResult:
    """Poll a TinyFish run until completion.

    Captures streaming_url from the run object for live browser preview.

    Args:
        on_status: Optional callback(status, run_data) called each poll cycle.
    """
    api_key = _get_api_key()
    result = TinyFishResult(run_id=run_id)
    deadline = asyncio.get_event_loop().time() + timeout

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
    ) as client:
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(poll_interval)
            try:
                resp = await client.get(
                    f"{TINYFISH_RUN_URL}/{run_id}",
                    headers={"X-API-Key": api_key},
                )
                if resp.status_code != 200:
                    logger.debug("TinyFish poll %s: HTTP %s", run_id[:12], resp.status_code)
                    continue

                run_data = resp.json()
                status = run_data.get("status", "")

                # Capture streaming_url as soon as it appears
                surl = run_data.get("streaming_url")
                if surl and not result.streaming_url:
                    result.streaming_url = surl

                if on_status:
                    on_status(status, run_data)

                if status == "COMPLETED":
                    result.data = run_data.get("result")
                    result.success = result.data is not None
                    logger.info("TinyFish run %s completed", run_id[:12])
                    return result
                elif status in ("FAILED", "CANCELLED"):
                    logger.warning("TinyFish run %s: %s", run_id[:12], status)
                    return result

            except Exception as e:
                logger.warning("TinyFish poll error for %s: %s", run_id[:12], e)

    logger.warning("TinyFish run %s timed out after %ss", run_id[:12], timeout)
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


async def tinyfish_extract_with_streaming(
    url: str,
    goal: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    event_queue: asyncio.Queue | None = None,
    step_label: str = "",
) -> TinyFishResult:
    """Extraction that also captures the streaming_url for live preview.

    Uses async+poll (thread-safe) — streaming_url is available on the
    run object via GET /v1/runs/{id}, no SSE needed.

    If event_queue is provided, pushes agent_streaming events as they're
    discovered during polling (useful for real-time UI updates).
    """
    run_id = await tinyfish_start_run(url, goal, stealth, proxy_country)
    if not run_id:
        return TinyFishResult()

    streaming_url_sent = False

    def _on_status(status: str, run_data: dict):
        nonlocal streaming_url_sent
        surl = run_data.get("streaming_url")
        if event_queue and surl and not streaming_url_sent:
            streaming_url_sent = True
            logger.info("[%s] Forwarding streaming_url: %s", step_label, surl[:60])
            event_queue.put_nowait({
                "event": "agent_streaming",
                "data": {"step": step_label, "streaming_url": surl},
            })
        # Push status on first RUNNING only
        if event_queue and status == "RUNNING" and not streaming_url_sent:
            event_queue.put_nowait({
                "event": "agent_progress",
                "data": {"step": step_label, "message": f"Agent starting on {url}..."},
            })

    return await tinyfish_poll_run(run_id, timeout=timeout, on_status=_on_status)


async def tinyfish_extract_batch(
    tasks: list[dict],
    timeout: float = DEFAULT_TIMEOUT,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    on_progress: Callable[[int, str, dict], None] | None = None,
) -> list[TinyFishResult]:
    """Fire multiple extractions in parallel, poll all until complete.

    Each task: {url, goal, stealth?, proxy_country?}
    on_progress: callback(task_index, status, run_data) per poll cycle.
    Returns TinyFishResult list (with streaming_url) in same order as tasks.
    """
    api_key = _get_api_key()

    # Phase 1: Fire all runs via batch API
    run_ids = await tinyfish_start_batch(tasks)
    logger.info("TinyFish batch: fired %d runs, got IDs: %s",
                len(tasks), [rid[:12] + "..." if rid else "NONE" for rid in run_ids])

    # Phase 2: Poll all pending runs CONCURRENTLY (not sequentially)
    results: list[TinyFishResult] = [TinyFishResult(run_id=rid or None) for rid in run_ids]
    pending = {i: rid for i, rid in enumerate(run_ids) if rid}
    deadline = asyncio.get_event_loop().time() + timeout

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0)
    ) as client:
        while pending and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(poll_interval)

            # Poll ALL pending runs concurrently in one batch
            async def _poll_one(i: int, rid: str) -> tuple[int, str | None, dict | None]:
                try:
                    resp = await client.get(
                        f"{TINYFISH_RUN_URL}/{rid}",
                        headers={"X-API-Key": api_key},
                    )
                    if resp.status_code == 200:
                        return (i, rid, resp.json())
                except Exception as e:
                    logger.debug("TinyFish poll error for %s: %s", rid[:12], e)
                return (i, rid, None)

            poll_results = await asyncio.gather(
                *[_poll_one(i, rid) for i, rid in pending.items()]
            )

            for i, rid, run_data in poll_results:
                if run_data is None:
                    continue
                status = run_data.get("status", "")

                # Capture streaming_url
                surl = run_data.get("streaming_url")
                if surl and not results[i].streaming_url:
                    results[i].streaming_url = surl

                if on_progress:
                    on_progress(i, status, run_data)

                if status == "COMPLETED":
                    results[i].data = run_data.get("result")
                    results[i].success = results[i].data is not None
                    pending.pop(i, None)
                    logger.info("TinyFish run %s completed", rid[:12])
                elif status in ("FAILED", "CANCELLED"):
                    pending.pop(i, None)
                    logger.warning("TinyFish run %s: %s", rid[:12], status)

    if pending:
        logger.warning("TinyFish batch: %d/%d runs still pending after %ss",
                       len(pending), len(tasks), timeout)

    logger.info("TinyFish batch complete: %d/%d succeeded",
                sum(1 for r in results if r.success), len(tasks))
    return results

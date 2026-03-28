"""Live event forwarder: captures TinyFish streaming URLs and progress events.

Used by pipelines that want to forward live browser previews and agent
narration to the frontend via SSE.
"""

from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator

from agents.base import tinyfish_extract_rich, TinyFishResult

logger = logging.getLogger(__name__)


async def tinyfish_extract_with_events(
    url: str,
    goal: str,
    step_label: str,
    stealth: bool = False,
    proxy_country: str | None = None,
    timeout: float = 15.0,
) -> AsyncGenerator[dict, None]:
    """TinyFish extraction that yields SSE events for streaming URL and progress.

    Yields dicts of shape {"event": str, "data": dict} which pipelines can
    forward directly to the SSE response.

    Events yielded:
    - agent_streaming: {step, streaming_url}  — live browser preview URL
    - agent_progress: {step, message}          — agent narration text
    - agent_result: {step, data, is_live}      — final extraction result

    Usage in a pipeline:
        async for ev in tinyfish_extract_with_events(url, goal, "extract_listing"):
            yield ev  # forward to SSE
    """
    event_queue: asyncio.Queue[dict] = asyncio.Queue()

    def on_streaming_url(streaming_url: str):
        event_queue.put_nowait({
            "event": "agent_streaming",
            "data": {"step": step_label, "streaming_url": streaming_url},
        })

    def on_progress(message: str):
        event_queue.put_nowait({
            "event": "agent_progress",
            "data": {"step": step_label, "message": message},
        })

    # Run extraction in background, collecting events via queue
    extract_task = asyncio.create_task(
        tinyfish_extract_rich(
            url=url,
            goal=goal,
            stealth=stealth,
            proxy_country=proxy_country,
            timeout=timeout,
            on_streaming_url=on_streaming_url,
            on_progress=on_progress,
        )
    )

    # Drain events from the queue while extraction runs
    while not extract_task.done():
        try:
            ev = await asyncio.wait_for(event_queue.get(), timeout=0.2)
            yield ev
        except asyncio.TimeoutError:
            continue

    # Drain any remaining events
    while not event_queue.empty():
        yield event_queue.get_nowait()

    # Get result
    result: TinyFishResult = extract_task.result()
    yield {
        "event": "agent_result",
        "data": {
            "step": step_label,
            "data": result.data,
            "is_live": result.success,
            "streaming_url": result.streaming_url,
        },
    }

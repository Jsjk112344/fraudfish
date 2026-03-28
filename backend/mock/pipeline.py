"""Mock investigation pipeline -- async generator yielding events with delays."""

import asyncio
from typing import AsyncGenerator

from models.events import InvestigationEvent
from models.enums import StepStatus
from mock.data import STEP_DEFINITIONS, MOCK_STEPS, MOCK_VERDICT


async def run_mock_investigation(url: str) -> AsyncGenerator[InvestigationEvent, None]:
    """Yields investigation events with artificial delays.
    Same interface as live pipeline in Phase 2."""
    for step_def in STEP_DEFINITIONS:
        step_id = step_def["id"]

        # Signal step is now active
        yield InvestigationEvent(step=step_id, status=StepStatus.ACTIVE, data=None)

        # Simulate processing time
        await asyncio.sleep(step_def["mock_delay"])

        # Signal step complete with mock data
        yield InvestigationEvent(
            step=step_id, status=StepStatus.COMPLETE, data=MOCK_STEPS[step_id]
        )

    # Final verdict event
    yield InvestigationEvent(
        step="verdict",
        status=StepStatus.COMPLETE,
        data=MOCK_VERDICT.model_dump(),
    )

"""LLM-based classification for ambiguous fraud cases. Supports OpenAI and Anthropic."""

import json
import os
from models.events import VerdictResult, Signal
from models.enums import ClassificationCategory, SignalSeverity

_openai_client = None
_anthropic_client = None


def _get_provider() -> str:
    """Detect which LLM provider to use based on available API keys."""
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("OPENAI_API_KEY") and os.environ["OPENAI_API_KEY"] != "your-openai-api-key-here":
        return "openai"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    raise RuntimeError("No LLM API key found. Set ANTHROPIC_API_KEY or OPENAI_API_KEY in backend/.env")


def _get_openai_client():
    global _openai_client
    if _openai_client is None:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI()
    return _openai_client


def _get_anthropic_client():
    global _anthropic_client
    if _anthropic_client is None:
        from anthropic import AsyncAnthropic
        _anthropic_client = AsyncAnthropic()
    return _anthropic_client


SYSTEM_PROMPT = """You are a fraud classification expert for Singapore ticket marketplaces.

Analyze the investigation evidence and classify this listing into one of four categories:
- LEGITIMATE: Real ticket, fair price, trustworthy seller
- SCALPING_VIOLATION: Real ticket but significantly marked up beyond face value
- LIKELY_SCAM: Strong indicators of fraud (extreme underpricing, fake seller, suspicious patterns)
- COUNTERFEIT_RISK: Indicators of counterfeit tickets (wrong venue details, suspicious transfer methods)

Evaluate all 5 risk signals:
1. Pricing Anomaly - How does the price compare to face value and market average?
2. Seller Reputation - Account age, review history, listing patterns
3. Event Verification - Does the event exist? Is the ticket category real?
4. Cross-Platform Duplicates - Same listing found elsewhere under different names?
5. Listing Authenticity - Payment method, description quality, transfer method red flags

For each signal, assign:
- severity: CRITICAL, WARNING, NEUTRAL, or CLEAR
- segmentsFilled: 1-5 (1=minimal concern, 5=maximum concern)

Provide a confidence score (0-100) and 2-3 sentence reasoning summary hitting key evidence points.
Focus on Singapore context: Carousell norms, SGD pricing, local events."""

VERDICT_SCHEMA = {
    "type": "object",
    "properties": {
        "category": {"type": "string", "enum": ["LEGITIMATE", "SCALPING_VIOLATION", "LIKELY_SCAM", "COUNTERFEIT_RISK"]},
        "confidence": {"type": "number", "description": "0-100"},
        "reasoning": {"type": "string", "description": "2-3 sentence summary"},
        "signals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "severity": {"type": "string", "enum": ["CRITICAL", "WARNING", "NEUTRAL", "CLEAR"]},
                    "segmentsFilled": {"type": "integer", "minimum": 1, "maximum": 5},
                },
                "required": ["name", "severity", "segmentsFilled"],
            },
        },
    },
    "required": ["category", "confidence", "reasoning", "signals"],
}


def format_evidence(evidence: dict) -> str:
    """Format investigation evidence as structured text for the LLM."""
    return f"Investigation evidence for classification:\n\n{json.dumps(evidence, indent=2, default=str)}"


def _parse_verdict(data: dict) -> VerdictResult:
    """Parse raw LLM JSON into a VerdictResult."""
    return VerdictResult(
        category=ClassificationCategory(data["category"]),
        confidence=float(data["confidence"]),
        reasoning=data["reasoning"],
        signals=[
            Signal(
                name=s["name"],
                severity=SignalSeverity(s["severity"]),
                segmentsFilled=s["segmentsFilled"],
            )
            for s in data.get("signals", [])
        ],
    )


async def _classify_openai(evidence: dict) -> VerdictResult:
    """Classify using OpenAI gpt-4o structured output."""
    client = _get_openai_client()
    completion = await client.beta.chat.completions.parse(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": format_evidence(evidence)},
        ],
        response_format=VerdictResult,
        max_tokens=2000,
        temperature=0.3,
    )
    result = completion.choices[0].message.parsed
    if result is None:
        raise ValueError("OpenAI model refused to classify -- no parsed result")
    return result


async def _classify_anthropic(evidence: dict) -> VerdictResult:
    """Classify using Claude via Anthropic's tool use for structured output."""
    client = _get_anthropic_client()
    response = await client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        tools=[{
            "name": "submit_verdict",
            "description": "Submit the fraud classification verdict with signals and reasoning.",
            "input_schema": VERDICT_SCHEMA,
        }],
        tool_choice={"type": "tool", "name": "submit_verdict"},
        messages=[
            {"role": "user", "content": format_evidence(evidence)},
        ],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == "submit_verdict":
            return _parse_verdict(block.input)
    raise ValueError("Anthropic model did not return a tool use result")


async def classify_with_llm(evidence: dict) -> VerdictResult:
    """Classify a listing using whichever LLM provider has an API key set.

    Checks ANTHROPIC_API_KEY first, then OPENAI_API_KEY.
    """
    provider = _get_provider()
    if provider == "anthropic":
        return await _classify_anthropic(evidence)
    return await _classify_openai(evidence)

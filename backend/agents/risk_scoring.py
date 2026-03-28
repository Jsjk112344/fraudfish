"""Risk scoring engine: rank discovered events by fraud/scalping probability.

Uses OpenAI gpt-4o-mini to intelligently assess fraud risk based on event
characteristics, artist fame, genre, and Singapore market context.
Falls back to rule-based scoring if the API call fails.
"""

from __future__ import annotations

import json
import logging
import os

logger = logging.getLogger(__name__)

# ---- OpenAI client (lazy init) ---------------------------------------------

_openai_client = None


def _get_client():
    global _openai_client
    if _openai_client is None:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI()
    return _openai_client


# ---- LLM-based scoring (primary) -------------------------------------------

SCORING_PROMPT = """\
You are a ticket fraud risk analyst specializing in Singapore's secondary ticket market.

For each event below, estimate a fraud/scalping risk score from 0 to 100 based on:
- **Artist/act fame & demand**: Global superstars and viral K-pop acts score higher
- **Genre risk**: K-pop, pop concerts, and major sports have the highest resale fraud in Singapore
- **Tour type**: World tours and limited Singapore dates create scarcity
- **Ticket price expectations**: Premium events (>S$200) attract more fraud
- **Singapore market context**: Events that Singaporean fans are desperate for
- **Sold-out likelihood**: Acts known to sell out instantly score higher

Score guide:
- 70-100 CRITICAL: Massive global act, certain to sell out, guaranteed scalping (e.g. Taylor Swift, BTS, F1 GP)
- 50-69 HIGH: Very popular act, likely sells out, active resale market (e.g. IVE, Coldplay, Guns N' Roses)
- 30-49 MODERATE: Popular act with some resale activity (e.g. niche K-pop, regional artists)
- 0-29 LOW: Niche/local act, unlikely significant fraud (e.g. comedy shows, small venues)

Return a JSON array with one object per event: {"index": <number>, "score": <number>, "reason": "<brief 5-10 word reason>"}
Return ONLY the JSON array, no markdown formatting."""


async def score_events_with_llm(events: list[dict]) -> list[dict]:
    """Score events using OpenAI gpt-4o-mini for intelligent risk assessment."""
    client = _get_client()

    # Build event list for the prompt
    event_lines = []
    for i, ev in enumerate(events):
        name = ev.get("event_name", "Unknown")
        category = ev.get("category", "other")
        hint = ev.get("popularity_hint") or "none"
        sold_out = ev.get("sold_out")
        face_low = ev.get("face_value_low")
        face_high = ev.get("face_value_high")
        price_str = ""
        if face_low or face_high:
            price_str = f", price: S${face_low or '?'}-S${face_high or '?'}"
        sold_str = ""
        if sold_out is True:
            sold_str = ", SOLD OUT"
        elif sold_out is False:
            sold_str = ", on sale"

        event_lines.append(
            f"{i}. {name} (category: {category}, hint: {hint}{price_str}{sold_str})"
        )

    event_text = "\n".join(event_lines)

    try:
        resp = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": SCORING_PROMPT},
                {"role": "user", "content": f"Score these {len(events)} events in Singapore:\n\n{event_text}"},
            ],
            temperature=0.3,
            max_tokens=2000,
        )

        raw = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()

        scores = json.loads(raw)

        # Apply scores to events
        score_map = {item["index"]: item for item in scores if isinstance(item, dict)}
        for i, ev in enumerate(events):
            if i in score_map:
                ev["risk_score"] = max(0, min(100, float(score_map[i]["score"])))
                ev["ai_reason"] = score_map[i].get("reason", "")
            else:
                # Fallback for missing entries
                ev["risk_score"] = _rule_based_score(ev)

        logger.info("LLM scored %d events (model: gpt-4o-mini)", len(events))
        return events

    except Exception as e:
        logger.warning("LLM scoring failed: %s — falling back to rules", e)
        for ev in events:
            ev["risk_score"] = _rule_based_score(ev)
        return events


# ---- Rule-based fallback ---------------------------------------------------

def _rule_based_score(event: dict) -> float:
    """Quick rule-based score as fallback when LLM is unavailable."""
    score = 0.0
    name_lower = (event.get("event_name") or "").lower()
    category = (event.get("category") or "other").lower()

    # Sold out
    sold_out = event.get("sold_out")
    if sold_out is True:
        score += 25
    elif sold_out is None:
        score += 7.5

    # Face value
    fv = 0.0
    for key in ("face_value_high", "face_value_low"):
        val = event.get(key)
        if val is not None:
            try:
                fv = float(val)
                break
            except (ValueError, TypeError):
                continue
    if fv >= 500:
        score += 20
    elif fv >= 300:
        score += 16
    elif fv >= 150:
        score += 10

    # Category
    if category in ("concert", "sports"):
        score += 15
    elif category in ("festival", "theatre"):
        score += 6

    # Tour
    if any(kw in name_lower for kw in ("world tour", "asia tour")):
        score += 10
    elif "tour" in name_lower:
        score += 5

    # Popularity hint
    hint = (event.get("popularity_hint") or "").lower()
    if "sold out" in hint:
        score += 10
    elif any(kw in hint for kw in ("selling fast", "limited", "hot")):
        score += 7

    return round(min(score, 100.0), 1)


# ---- Public API -----------------------------------------------------------

async def rank_events(events: list[dict]) -> list[dict]:
    """Score and rank events by fraud risk (highest first).

    Uses OpenAI gpt-4o-mini for intelligent scoring, falls back to rules.
    Mutates each event dict by adding 'risk_score' and 'risk_level' keys.
    """
    if not events:
        return events

    # Try LLM scoring if OpenAI key is available
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if api_key and api_key != "your-openai-api-key-here":
        events = await score_events_with_llm(events)
    else:
        for ev in events:
            ev["risk_score"] = _rule_based_score(ev)

    # Assign risk levels
    for ev in events:
        score = ev.get("risk_score", 0)
        if score >= 70:
            ev["risk_level"] = "CRITICAL"
        elif score >= 50:
            ev["risk_level"] = "HIGH"
        elif score >= 30:
            ev["risk_level"] = "MODERATE"
        else:
            ev["risk_level"] = "LOW"

    events.sort(key=lambda e: e["risk_score"], reverse=True)
    return events

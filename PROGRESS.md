# FraudFish — Project Progress

> AI fraud detective for ticket scams — investigates like a human analyst, but in 30 seconds.

---

## Status: All 5 Phases Complete (56 commits)

### Phase 1: Application Skeleton with Mocks
- FastAPI backend with SSE streaming (`POST /api/investigate`)
- Pydantic models and enums for all investigation types
- Mock investigation pipeline — F1 Singapore GP scenario with realistic delays
- React 19 + Vite + Tailwind v4 frontend scaffold
- Animated investigation timeline with 4-state StepCards (pending → active → complete → error)
- VerdictPanel with confidence meter and 5-signal display
- EvidenceSidebar with seller, pricing, and market data

### Phase 2: Core Investigation Pipeline
- **TinyFish agents** for Carousell and Telegram listing extraction (with mock fallback)
- **Seller investigation** — account age, total listings, review parsing with sentiment analysis
- **Event verification** — fallback chain: F1 official site → Ticketmaster SG → Google search
- **Live pipeline orchestrator** — parallel fan-out for seller + event verification
- Per-step 15s timeout, total 60s investigation cap
- LIVE badge indicator on steps using real scraped data

### Phase 3: Classification & Verdict Engine
- **Rules engine** — deterministic pricing rules (< 40% face value = LIKELY_SCAM, > 300% markup = SCALPING_VIOLATION)
- **LLM classification** — OpenAI gpt-4o with structured output for ambiguous cases
- **Validation layer** — post-LLM sanity checks, contradiction detection, confidence calibration
- Two-tier orchestrator: rules first → LLM fallback → validation

### Phase 4: Cross-Platform Intelligence
- **Market rate scanning** — Carousell + Viagogo search, up to 12 listings per event
- **IQR-based statistical outlier detection** — median, percentile rank, price deviation
- **Cross-platform duplicate detection** — text similarity matching (> 90%) across platforms
- UI updates: outlier labels in EvidenceSidebar, formatted StepCard rendering for market/cross-platform steps

### Phase 5: Event Scan Mode (Batch)
- `POST /api/scan` SSE endpoint for batch event scanning
- Scan pipeline orchestrator — discovery → investigation → classification → aggregation
- Concurrency-limited parallel investigation (3 concurrent TinyFish sessions, max 12 listings)
- `useEventScan` hook and SSE client for scan-specific events
- ThreatSummary banner — total listings, flagged count, confirmed scams, fraud exposure (SGD)
- ScanListingRow — expandable rows with inline verdict details
- Mode toggle in InputSection (Investigate | Scan) with demo event chips

---

## Architecture

```
POST /api/investigate (single listing)
POST /api/scan (batch event scan)
    │
    ▼
┌─────────────────────────────────────────┐
│  6-Step Investigation Pipeline          │
│                                         │
│  1. Extract Listing (Carousell/Telegram)│
│  2. Investigate Seller (parallel)       │
│  3. Verify Event (parallel)             │
│  4. Market Rate Scan                    │
│  5. Cross-Platform Duplicate Check      │
│  6. Classify & Synthesize Verdict       │
└─────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────────┐
│  Two-Tier Classification                │
│                                         │
│  Tier 1: Rules engine (obvious cases)   │
│  Tier 2: GPT-4o (ambiguous cases)       │
│  Tier 3: Validation layer (catch errors)│
└─────────────────────────────────────────┘
    │
    ▼  SSE stream
┌─────────────────────────────────────────┐
│  React Frontend                         │
│                                         │
│  Investigation: Timeline → StepCards    │
│  → VerdictPanel → EvidenceSidebar       │
│                                         │
│  Scan: ThreatSummary → ScanListingRows  │
└─────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python + FastAPI + SSE |
| Web Agent | TinyFish SDK (stealth mode) |
| Classification | OpenAI gpt-4o / rules engine |
| Frontend | React 19 + Vite + Tailwind v4 |
| Streaming | POST-based SSE (fetch-event-source) |
| Animations | Framer Motion |

## File Count

| Area | Files |
|------|-------|
| Backend modules | 28 |
| Frontend components | 13 |
| React hooks | 2 |
| API endpoints | 2 |
| Test files | 10+ |

## Classification Categories

| Category | Trigger |
|----------|---------|
| **LEGITIMATE** | No signals triggered, reasonable pricing |
| **SCALPING_VIOLATION** | > 300% markup (available) or > 500% (sold-out) |
| **LIKELY_SCAM** | < 40% face value, cross-platform duplicates, fake seller |
| **COUNTERFEIT_RISK** | Suspicious transfer method, unverifiable tickets |

## What's Needed to Run

1. Set `TINYFISH_API_KEY` in `backend/.env` (done)
2. Set `OPENAI_API_KEY` in `backend/.env` (placeholder added)
3. `pip install -r backend/requirements.txt`
4. `cd frontend && npm install`
5. Start backend: `uvicorn backend.main:app --reload`
6. Start frontend: `cd frontend && npm run dev`
7. Open http://localhost:5173

Without API keys, the app falls back to mock data for all investigation steps.

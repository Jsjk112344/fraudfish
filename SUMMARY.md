# FraudFish — Project Summary & Architecture

## What Is FraudFish?

An autonomous fraud intelligence agent that investigates suspicious ticket listings on Singapore secondary marketplaces. Paste a listing URL → the agent navigates seller profiles, official ticket sites, and other marketplaces → produces an evidence-backed fraud verdict in under 60 seconds.

**Built for:** TinyFish SG Hackathon (March 28, 2026, NUS College)
**Hacking window:** 10:30 AM – 4:30 PM (6 hours, hard code freeze)
**Solo builder**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         USER (Browser)                              │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                    React + Vite Frontend                     │   │
│   │                    (localhost:5173)                           │   │
│   │                                                              │   │
│   │  ┌──────────┐  ┌──────────────────┐  ┌─────────────────┐   │   │
│   │  │  Input    │  │  Investigation   │  │  VerdictPanel   │   │   │
│   │  │  Section  │→ │  Timeline        │→ │  + Evidence     │   │   │
│   │  │  (URL)    │  │  (6 StepCards)   │  │  Sidebar        │   │   │
│   │  └──────────┘  └──────────────────┘  └─────────────────┘   │   │
│   │       │              ▲ SSE events                            │   │
│   │       │              │ (step status + data)                  │   │
│   │  useInvestigation hook ──── fetch-event-source (POST SSE)   │   │
│   └───────┼──────────────┼──────────────────────────────────────┘   │
│           │              │                                          │
└───────────┼──────────────┼──────────────────────────────────────────┘
            │              │
            ▼              │
┌───────────────────────────────────────────────────────────────────────┐
│                    FastAPI Backend (localhost:8000)                    │
│                                                                       │
│   POST /api/investigate { url }                                       │
│         │                                                             │
│         ▼                                                             │
│   ┌─────────────────────────────────────────────────────────┐        │
│   │              Investigation Pipeline                      │        │
│   │              (AsyncGenerator → SSE events)               │        │
│   │                                                          │        │
│   │   Phase 1 (NOW): Mock Pipeline                          │        │
│   │   ┌──────────────────────────────────────────────┐      │        │
│   │   │  run_mock_investigation(url)                  │      │        │
│   │   │  → 6 steps with artificial delays             │      │        │
│   │   │  → F1 Singapore GP LIKELY_SCAM scenario       │      │        │
│   │   │  → Verdict: 94.2% confidence                  │      │        │
│   │   └──────────────────────────────────────────────┘      │        │
│   │                                                          │        │
│   │   Phase 2 (Hackathon): Live Pipeline                    │        │
│   │   ┌──────────────────────────────────────────────┐      │        │
│   │   │  run_live_investigation(url)                  │      │        │
│   │   │                                               │      │        │
│   │   │  1. detect_platform(url)                      │      │        │
│   │   │     └─ carousell.sg | t.me | unsupported      │      │        │
│   │   │                                               │      │        │
│   │   │  2. Extract listing (sequential)              │      │        │
│   │   │     ├─ extract_carousell_listing(url)         │      │        │
│   │   │     └─ extract_telegram_message(url)          │      │        │
│   │   │                                               │      │        │
│   │   │  3. Fan-out: asyncio.gather (parallel)        │      │        │
│   │   │     ├─ investigate_seller(username)            │      │        │
│   │   │     └─ verify_event_official(event_name)      │      │        │
│   │   │         ├─ F1 official (singaporegp.sg)       │      │        │
│   │   │         ├─ Ticketmaster SG                    │      │        │
│   │   │         └─ Google fallback                    │      │        │
│   │   │                                               │      │        │
│   │   │  4. Market rate scan (Phase 4)                │      │        │
│   │   │  5. Cross-platform search (Phase 4)           │      │        │
│   │   │  6. Classification verdict (Phase 3)          │      │        │
│   │   │     ├─ Rules engine (obvious cases)           │      │        │
│   │   │     ├─ OpenAI gpt-4o (ambiguous cases)        │      │        │
│   │   │     └─ Validation layer (catch LLM mistakes)  │      │        │
│   │   └──────────────────────────────────────────────┘      │        │
│   │                                                          │        │
│   │   Every agent: TinyFish call → fallback to mock data    │        │
│   │   Total timeout: 60 seconds                             │        │
│   └─────────────────────────────────────────────────────────┘        │
│                                                                       │
│   Pydantic Models (shared contract):                                  │
│   ┌────────────────────────────────────────────────┐                 │
│   │  InvestigationEvent { step, status, data }      │                 │
│   │  VerdictResult { category, confidence,          │                 │
│   │                  reasoning, signals[] }          │                 │
│   │  Signal { name, severity, segments_filled }     │                 │
│   │  StepStatus: PENDING|ACTIVE|COMPLETE|ERROR      │                 │
│   │  Category: LEGITIMATE|SCALPING|LIKELY_SCAM|     │                 │
│   │            COUNTERFEIT_RISK                      │                 │
│   └────────────────────────────────────────────────┘                 │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
            │                          │
            ▼                          ▼
┌─────────────────────┐   ┌──────────────────────────┐
│   TinyFish API       │   │   OpenAI API              │
│   (Web Agent)        │   │   (Classification)        │
│                      │   │                            │
│  - Stealth browser   │   │  - gpt-4o: full verdicts  │
│  - Proxy config (SG) │   │    (structured JSON)      │
│  - Anti-bot bypass   │   │  - gpt-4o-mini: bulk      │
│  - Multi-step nav    │   │    summaries (event scan)  │
│                      │   │                            │
│  Sites navigated:    │   └──────────────────────────┘
│  - Carousell SG      │
│  - Viagogo           │
│  - Ticketmaster SG   │
│  - singaporegp.sg    │
│  - Google (fallback) │
│  - Facebook (stretch)│
└─────────────────────┘
```

---

## File Structure

```
fraudfish/
├── backend/
│   ├── main.py                    # FastAPI app + CORS
│   ├── requirements.txt
│   ├── api/
│   │   └── investigate.py         # POST /api/investigate → SSE stream
│   ├── models/
│   │   ├── enums.py               # StepStatus, ClassificationCategory, SignalSeverity
│   │   └── events.py              # InvestigationEvent, VerdictResult, Signal
│   ├── mock/
│   │   ├── data.py                # F1 GP mock data + step definitions
│   │   └── pipeline.py            # Mock async generator (Phase 1)
│   ├── agents/                    # [Phase 2] TinyFish investigation agents
│   │   ├── base.py                # TinyFish async wrapper + timeout
│   │   ├── platform_detect.py     # URL → platform routing
│   │   ├── carousell.py           # Listing extraction (stealth + SG proxy)
│   │   ├── telegram.py            # Message extraction
│   │   ├── seller.py              # Carousell seller profile + reviews
│   │   ├── seller_telegram.py     # Telegram repeat-seller detection
│   │   ├── official_price.py      # F1 official / Ticketmaster / Google
│   │   ├── google_fallback.py     # Google search for event pricing
│   │   └── pipeline.py            # Live orchestrator (asyncio.gather)
│   ├── classify/                  # [Phase 3] Verdict engine
│   │   ├── rules.py               # Rules engine (obvious cases)
│   │   ├── llm.py                 # OpenAI gpt-4o classification
│   │   └── validate.py            # Post-LLM sanity checks
│   └── tests/
│       ├── test_models.py
│       ├── test_mock_data.py
│       ├── test_api.py
│       ├── test_agents.py         # [Phase 2]
│       ├── test_seller.py         # [Phase 2]
│       ├── test_official_price.py # [Phase 2]
│       └── test_pipeline.py       # [Phase 2]
├── frontend/
│   ├── package.json               # React 19, Vite, Tailwind v4, Framer Motion
│   ├── vite.config.ts
│   └── src/
│       ├── App.tsx                # Main layout composition
│       ├── app.css                # Tailwind @theme tokens + glass utility
│       ├── types/
│       │   └── investigation.ts   # TypeScript types mirroring backend
│       ├── api/
│       │   └── investigate.ts     # SSE client (fetch-event-source)
│       ├── hooks/
│       │   └── useInvestigation.ts # State machine: idle→running→complete
│       └── components/
│           ├── TopNavBar.tsx       # Fixed glass nav
│           ├── Sidebar.tsx        # Desktop agent identity
│           ├── InputSection.tsx   # URL input + sample chips
│           ├── InvestigationTimeline.tsx  # Animated step container
│           ├── StepCard.tsx       # Step card (4 states) + LIVE badge [P2]
│           ├── VerdictPanel.tsx   # Classification + signal meters
│           ├── EvidenceSidebar.tsx # Seller profile + price chart
│           └── MobileBottomNav.tsx
└── data/
    └── f1-gp-mock.json           # F1 Singapore GP LIKELY_SCAM scenario
```

---

## Investigation Flow (6 Steps)

```
User pastes URL
      │
      ▼
┌─ Step 1: Extract Listing ────────────────────────────────────────┐
│  Pull price, description, seller name, transfer method            │
│  TinyFish: navigate Carousell/Viagogo page, extract structured    │
└───────────────────────────────────────────────────────────────────┘
      │ seller_username, event context
      ├──────────────────────────────┐
      ▼                              ▼
┌─ Step 2: Investigate Seller ┐  ┌─ Step 3: Verify Event ─────────┐
│  Navigate to seller profile  │  │  Check official ticket sites    │
│  Account age, listing count  │  │  F1 official → Ticketmaster →  │
│  Reviews + sentiment         │  │  Google fallback                │
│  (PARALLEL)                  │  │  Face value + sold-out status   │
└──────────────────────────────┘  └────────────────────────────────┘
      │                              │
      ├──────────────────────────────┘
      ▼
┌─ Step 4: Market Rate Check ──────────────────────────────────────┐
│  Scan 10+ listings for same event across platforms                │
│  Calculate median price, percentile, statistical outlier score    │
└───────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─ Step 5: Cross-Platform Search ──────────────────────────────────┐
│  Search seller name / listing text on other marketplaces          │
│  Detect copy-paste scam operations across platforms               │
└───────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─ Step 6: Synthesize Verdict ─────────────────────────────────────┐
│  Rules engine: obvious cases (< 40% face value = LIKELY_SCAM)    │
│  OpenAI gpt-4o: ambiguous cases with full evidence context        │
│  Validation layer: catch LLM mistakes                             │
│                                                                    │
│  Output: Category + Confidence + Reasoning + 5 Signal Meters      │
│  ├── Pricing Anomaly (CRITICAL/WARNING/CLEAR)                     │
│  ├── Seller Reputation                                            │
│  ├── Event Verification                                           │
│  ├── Cross-Platform Duplicates                                    │
│  └── Listing Authenticity                                         │
└───────────────────────────────────────────────────────────────────┘
```

---

## Phase Breakdown & Hackathon Strategy

### Pre-Hackathon (Now → March 27)

| Phase | What | Status | Why Pre-Build |
|-------|------|--------|---------------|
| **Phase 1** | App skeleton with mocks — full UI + SSE streaming + animated timeline + verdict panel | **~90% done** | Eliminates all integration plumbing on hackathon day. The entire app works E2E with mock data. |

### Hackathon Day (March 28, 6 hours)

| Phase | What | Time Est | Hackathon Goal |
|-------|------|----------|----------------|
| **Phase 2** | Live TinyFish agents — listing extraction, seller investigation, event verification | ~2h | Replace mocks with real web scraping. Parallel execution via asyncio.gather. |
| **Phase 3** | Classification engine — rules + OpenAI gpt-4o + validation | ~1.5h | Real verdicts instead of hardcoded ones. Two-tier system shows technical depth. |
| **Phase 4** | Cross-platform intelligence — market rate scan + duplicate detection | ~1.5h | The "wow" moment: agent finds same seller on another platform. |
| **Phase 5** | Event scan mode — batch investigation by event name | ~1h | Threat intelligence: "47 listings, 12 flagged, $3,200 fraud exposure" |

**Strategy:** Each phase leaves the app in a demoable state. If time runs out after Phase 3, we still have a working fraud detector. Phase 4 and 5 are the differentiators for first place.

---

## How We Address Hackathon Goals

### TinyFish Integration Depth (Key Judging Criterion)

This is NOT a thin API wrapper. Each investigation step is a separate TinyFish call with distinct configuration:

| Agent | TinyFish Features Used |
|-------|----------------------|
| **Carousell extractor** | Stealth browser profile + SG proxy (anti-bot bypass) |
| **Seller profile** | Multi-page navigation (profile → reviews section) |
| **Ticketmaster check** | Search result parsing + event page extraction |
| **Google fallback** | Dynamic search result extraction |
| **Market rate scan** | Parallel scraping of 10+ listings across platforms |
| **Cross-platform search** | Multi-site navigation with text similarity matching |

**Total TinyFish calls per investigation:** 5-8 separate API calls orchestrated in parallel.

### Prize Targeting

| Prize | How We Win It |
|-------|---------------|
| **1st Place** | Technical depth (multi-step autonomous agent) + real utility (ticket fraud is a real SG problem) + polished UI (animated evidence reveal) |
| **Deep Sea Architect** | The investigation pipeline is architecturally elegant: platform detection → sequential extraction → parallel fan-out → evidence synthesis. Each agent is isolated, testable, and falls back gracefully. |
| **Most Likely Unicorn** | Clear PMF: sell to Carousell (reduce scam complaints), event organizers (protect fans), consumer protection agencies (automated fraud reports). S$1.6M+ in losses = real market. |

### Demo Moments That Win

1. **Real-time investigation streaming** — steps appear one by one, each revealing new evidence
2. **Cross-platform correlation** — "Same seller found on Facebook with 92% text similarity" (the gasp moment)
3. **Statistical outlier detection** — "This listing is in the 3rd percentile for pricing" (data-driven, not vibes)
4. **Event scan scale** — "47 listings scanned, 12 flagged, $3,200 fraud exposure" (threat intelligence, not just one listing)
5. **Graceful fallback** — if TinyFish is slow, mock data kicks in seamlessly. Demo never breaks.

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **SSE streaming (not WebSocket)** | Simpler, one-directional (server→client), native browser support. Investigation is a one-way stream of evidence. |
| **AsyncGenerator pipeline interface** | Mock and live pipelines share the same interface. Swapping is a one-line change in the SSE endpoint. |
| **Silent fallback to mock data** | Every TinyFish call has cached fallback. Demo never breaks, even if anti-bot blocks us live. |
| **Rules engine + LLM (two-tier)** | Obvious cases (extreme underpricing) don't waste LLM tokens. Ambiguous cases get full reasoning. Shows technical maturity. |
| **Post-LLM validation** | Catches when gpt-4o says "LEGITIMATE" on a listing priced at 20% of face value. Safety net for the demo. |
| **F1 Singapore GP as hero event** | Everyone in Singapore knows it. High face values make fraud obvious. Active listings exist now for live demo. |

---

## Assumptions vs. Current Plans (Delta Notes)

The current GSD plans (Phases 2-5) are well-aligned with the architecture. A few things to note:

1. **Phase 2 plans reference `backend/api/routes.py`** but Phase 1 created `backend/api/investigate.py`. Plan 02-04 will need to update `investigate.py` instead of `routes.py` when wiring the live pipeline.

2. **Telegram is in Phase 2 plans but Carousell is the hackathon priority.** If time is tight on hackathon day, Telegram agents can be deferred — Carousell + Viagogo cover the core demo. The mock fallback ensures the UI still shows all 6 steps regardless.

3. **Phase 3 (Classification) and Phase 4 (Cross-Platform) are independent** and could execute in parallel or be reordered. If cross-platform correlation is harder than expected, Phase 3 alone gives us a working fraud detector.

4. **Phase 5 (Event Scan) is the stretch goal.** The batch investigation mode is impressive but only matters if Phases 2-4 are solid. The plan correctly depends on both Phase 3 and Phase 4.

5. **No Viagogo-specific extractor in Phase 2 plans.** The plans cover Carousell and Telegram, but Viagogo is listed as a target marketplace. Viagogo support would need an additional extractor agent (similar pattern to Carousell but different DOM structure). This could be added as a decimal phase (2.1) or folded into Phase 4's market rate scan.

6. **Frontend changes are minimal after Phase 1.** Phase 2 only adds a LIVE badge to StepCard. The streaming architecture was designed so the frontend doesn't need to know whether data is mock or live — it just renders whatever SSE events arrive.

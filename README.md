# FraudFish

**AI fraud detective for ticket scams — investigates like a human analyst, but in 30 seconds.**

Built for the [TinyFish SG Hackathon](https://tinyfish.io) (March 28, 2026) by Justin Soon.

---

## The Problem

Every week in Singapore, hundreds of people get scammed buying tickets on Carousell, Viagogo, and other secondary marketplaces. Manual scam detection takes 30+ minutes per listing — by then, fans have already lost money. Regulatory bodies lack the tooling to monitor at scale, and marketplace platforms have limited fraud detection for peer-to-peer ticket resales.

## The Solution

FraudFish is an **autonomous investigation agent** that pastes in a listing URL and runs a multi-step investigation — scraping seller profiles, verifying events against official sources, scanning market rates, and detecting cross-platform duplicates — then produces an evidence-backed fraud verdict in under 60 seconds.

It doesn't just classify. It **investigates** — gathering real evidence like a detective, then reasoning over the full evidence file.

## How It Works

### Mode 1: Link Investigation
Paste a suspicious listing URL. FraudFish autonomously:

1. **Extracts the listing** — price, description, seller, transfer method
2. **Investigates the seller** — account age, listing history, review sentiment
3. **Verifies the event** — checks SISTIC, Ticketmaster SG, official sources for face value
4. **Scans market rates** — compares against 12+ listings, detects statistical outliers (IQR-based)
5. **Cross-platform duplicate check** — finds copy-paste scam operations across marketplaces
6. **Classifies with reasoning** — rules engine for obvious cases, GPT-4o for ambiguous ones, validation layer catches errors

### Mode 2: Event Scan (Batch)
Enter an event name. FraudFish searches all platforms, investigates each listing in parallel, and produces a threat intelligence report with fraud exposure totals.

### Classification Categories

| Category | Meaning |
|----------|---------|
| LEGITIMATE | No fraud signals, reasonable pricing |
| SCALPING_VIOLATION | Extreme markup (>300% available, >500% sold-out) |
| LIKELY_SCAM | Severe underpricing, fake seller signals, cross-platform duplicates |
| COUNTERFEIT_RISK | Suspicious transfer method, unverifiable tickets |

## Architecture

```
User pastes URL
    |
    v
+------------------------------------------+
|  6-Step Investigation Pipeline           |
|                                          |
|  1. Extract Listing (Carousell/Telegram) |
|  2. Investigate Seller (parallel)        |
|  3. Verify Event (parallel)              |
|  4. Market Rate Scan                     |
|  5. Cross-Platform Duplicate Check       |
|  6. Classify & Synthesize Verdict        |
+------------------------------------------+
    |
    v
+------------------------------------------+
|  Two-Tier Classification                 |
|                                          |
|  Tier 1: Rules engine (obvious cases)    |
|  Tier 2: GPT-4o (ambiguous cases)        |
|  Tier 3: Validation (catch errors)       |
+------------------------------------------+
    |  SSE stream
    v
+------------------------------------------+
|  React Frontend                          |
|                                          |
|  Investigation: Timeline + StepCards     |
|  -> VerdictPanel -> EvidenceSidebar      |
|                                          |
|  Scan: ThreatSummary + ScanListingRows   |
+------------------------------------------+
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python + FastAPI + SSE streaming |
| Web Agent | **TinyFish SDK** (stealth mode, anti-bot bypass) |
| Classification | OpenAI GPT-4o (evidence reasoning) + rules engine |
| Frontend | React 19 + Vite + Tailwind CSS v4 |
| Streaming | POST-based SSE (fetch-event-source) |
| Animations | Framer Motion |

## TinyFish Integration

FraudFish deeply integrates the TinyFish SDK for autonomous web navigation:

- **Stealth mode** for anti-bot protected marketplaces (Carousell, Viagogo)
- **Multi-step navigation** — listing page -> seller profile -> review pages
- **Parallel scraping** — concurrent TinyFish sessions with concurrency limits
- **Structured extraction** — AgentQL-powered data extraction from unstructured pages
- **Graceful fallback** — mock data when API keys unavailable for demo

## Quick Start

### Prerequisites
- Python 3.11+
- Node.js 18+
- TinyFish API key
- OpenAI API key

### Setup

```bash
# Clone
git clone https://github.com/justinsoon/fraudfish.git
cd fraudfish

# Backend
cp backend/.env.example backend/.env
# Add your TINYFISH_API_KEY and OPENAI_API_KEY to backend/.env
pip install -r backend/requirements.txt

# Frontend
cd frontend && npm install && cd ..
```

### Run

```bash
# Terminal 1: Backend
uvicorn backend.main:app --reload

# Terminal 2: Frontend
cd frontend && npm run dev
```

Open http://localhost:5173

> Without API keys, the app runs with mock data for all investigation steps.

## Target Marketplaces

- **Carousell SG** — Singapore's #1 peer marketplace
- **Viagogo** — Cross-platform correlation
- **Telegram** — Emerging ticket resale channel

## Vision

FraudFish is the **fraud intelligence layer** for every secondary ticket marketplace in Southeast Asia. The same investigation pipeline can be extended to:

- **Marketplace platforms** — automated listing moderation at scale
- **Regulatory bodies** — real-time fraud monitoring dashboards
- **Consumer protection** — browser extension that warns buyers before purchase
- **Event organizers** — protect brand from counterfeit tickets and scalping

---

Built with TinyFish, OpenAI, and determination.

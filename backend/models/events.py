"""Pydantic models for SSE events and investigation data."""

from typing import Optional

from pydantic import BaseModel

from models.enums import ClassificationCategory, SignalSeverity, StepStatus


class InvestigateRequest(BaseModel):
    url: str


class Signal(BaseModel):
    name: str
    severity: SignalSeverity
    segmentsFilled: int


class InvestigationEvent(BaseModel):
    step: str
    status: StepStatus
    data: Optional[dict] = None


class VerdictResult(BaseModel):
    category: ClassificationCategory
    confidence: float
    reasoning: str
    signals: list[Signal]


class ScanRequest(BaseModel):
    event_name: str


class ScanListingResult(BaseModel):
    listing_id: str
    url: Optional[str] = None
    platform: str
    title: str
    price: float
    seller: str
    verdict: Optional[VerdictResult] = None
    status: str  # "pending" | "investigating" | "complete" | "error"


class ScanStats(BaseModel):
    total_listings: int = 0
    investigated: int = 0
    flagged: int = 0
    confirmed_scams: int = 0
    fraud_exposure: float = 0.0
    by_platform: dict = {}
    by_category: dict = {}


# ---- Dashboard models -------------------------------------------------------


class DiscoveredEvent(BaseModel):
    event_id: str
    event_name: str
    venue: Optional[str] = None
    date: Optional[str] = None
    category: str = "other"
    face_value_low: Optional[float] = None
    face_value_high: Optional[float] = None
    sold_out: Optional[bool] = None
    popularity_hint: Optional[str] = None
    source: str = "unknown"
    risk_score: float = 0.0
    risk_level: str = "LOW"  # CRITICAL | HIGH | MODERATE | LOW


class DashboardScanRequest(BaseModel):
    """Request body for dashboard batch scan — accepts pre-ranked event list."""
    events: list[DiscoveredEvent]

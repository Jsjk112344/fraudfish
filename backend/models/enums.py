"""Enums for classification categories, step status, and signal severity."""

from enum import Enum


class StepStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETE = "complete"
    ERROR = "error"


class ClassificationCategory(str, Enum):
    LEGITIMATE = "LEGITIMATE"
    SCALPING_VIOLATION = "SCALPING_VIOLATION"
    LIKELY_SCAM = "LIKELY_SCAM"
    COUNTERFEIT_RISK = "COUNTERFEIT_RISK"


class SignalSeverity(str, Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    NEUTRAL = "NEUTRAL"
    CLEAR = "CLEAR"

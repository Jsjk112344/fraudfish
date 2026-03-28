"""Centralized rules configuration for the classification engine.

All thresholds in one dict -- easy to read and tweak during hackathon.
No magic numbers scattered across functions.
"""

RULES_CONFIG = {
    "extreme_underpricing": {
        "threshold": 0.40,  # < 40% of face value
        "category": "LIKELY_SCAM",
        "confidence": 92.0,
        "description": "Price below {threshold_pct}% of face value indicates likely scam",
    },
    "extreme_markup_available": {
        "threshold": 3.00,  # > 300% markup when NOT sold out
        "category": "SCALPING_VIOLATION",
        "confidence": 88.0,
        "description": "Markup over {threshold_pct}% on available event indicates scalping",
    },
    "extreme_markup_soldout": {
        "threshold": 5.00,  # > 500% markup when sold out (looser)
        "category": "SCALPING_VIOLATION",
        "confidence": 75.0,
        "description": "Markup over {threshold_pct}% even for sold-out event indicates scalping",
    },
}

"""Platform detection from URL domain routing to marketplace extractors."""

from __future__ import annotations

from urllib.parse import urlparse


def detect_platform(url: str) -> str | None:
    """Detect marketplace platform from URL domain.
    Returns 'carousell', 'telegram', or None for unsupported."""
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower().replace("www.", "")
        if not domain:
            return None
        if "carousell.sg" in domain:
            return "carousell"
        elif domain in ("t.me", "web.telegram.org"):
            return "telegram"
        else:
            return None
    except Exception:
        return None

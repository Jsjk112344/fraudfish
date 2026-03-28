"""Carousell seller profile investigation agent with full review parsing."""

from agents.base import tinyfish_extract
from mock.data import get_mock_data

SELLER_PROFILE_GOAL = (
    "Navigate this Carousell seller profile page and extract ALL of the following: "
    "1. Account age (how long they've been on Carousell, e.g., '2 years'), "
    "2. Total number of listings currently active, "
    "3. Categories of their listings (e.g., Tickets, Electronics, Fashion), "
    "4. EVERY review visible on the profile -- for each review extract: "
    "   reviewer name, rating (stars), review text, and date. "
    "5. Overall rating if displayed. "
    "Return as JSON with keys: account_age, total_listings, listing_categories (array), "
    "overall_rating, reviews (array of objects with reviewer, rating, text, date)."
)


def build_seller_profile_url(seller_username: str) -> str:
    """Build Carousell seller profile URL from username."""
    return f"https://www.carousell.sg/u/{seller_username}/"


def analyze_review_sentiment(reviews: list[dict]) -> str:
    """Simple keyword-based sentiment analysis of seller reviews.

    Returns summary like '8 positive, 2 negative, 1 neutral out of 11 reviews'.
    Scans for keywords: 'scam', 'fake', 'never delivered', 'fraud', 'liar' = negative.
    'great', 'fast', 'recommend', 'smooth', 'good', 'excellent', 'trustworthy' = positive.
    Everything else = neutral.
    """
    positive_keywords = {
        "great", "fast", "recommend", "smooth", "good",
        "excellent", "trustworthy", "reliable", "friendly", "quick",
    }
    negative_keywords = {
        "scam", "fake", "never delivered", "fraud", "liar",
        "terrible", "worst", "cheat", "dishonest", "avoid",
    }
    pos = neg = neutral = 0
    for review in reviews:
        text = (review.get("text") or review.get("review_text") or "").lower()
        if any(kw in text for kw in negative_keywords):
            neg += 1
        elif any(kw in text for kw in positive_keywords):
            pos += 1
        else:
            neutral += 1
    total = pos + neg + neutral
    return f"{pos} positive, {neg} negative, {neutral} neutral out of {total} reviews"


def normalize_seller_profile(raw: dict) -> dict:
    """Normalize TinyFish response to standard seller profile schema."""
    reviews = raw.get("reviews", [])
    return {
        "account_age": raw.get("account_age", "Unknown"),
        "total_listings": int(raw.get("total_listings", 0)) if raw.get("total_listings") else 0,
        "listing_categories": raw.get("listing_categories", []),
        "overall_rating": raw.get("overall_rating"),
        "review_count": len(reviews),
        "reviews": reviews,
        "review_sentiment": analyze_review_sentiment(reviews),
        "platform": "carousell",
    }


async def investigate_carousell_seller(seller_username: str, timeout: float = 15.0) -> tuple[dict, bool]:
    """Investigate Carousell seller profile. Returns (data, is_live)."""
    profile_url = build_seller_profile_url(seller_username)
    result = await tinyfish_extract(
        url=profile_url,
        goal=SELLER_PROFILE_GOAL,
        stealth=True,
        proxy_country="SG",
        timeout=timeout,
    )
    if result is not None:
        return (normalize_seller_profile(result), True)
    return (get_mock_data("investigate_seller"), False)

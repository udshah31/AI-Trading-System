"""
Signal accuracy against human labels
====================================
Each labelled report has a human direction (bearish / neutral / bullish). The extractor's
0-1 score is mapped to a direction and compared, grouped by how the score was obtained
(label parse, TypeSafe, or the neutral fallback). The pre-TypeSafe keyword-count method is
re-run on the same text as a baseline. TypeSafe agreement is also split by its confidence.
"""
from typing import Iterable, Optional

LABELS = ("bearish", "neutral", "bullish")
METHODS = ("label", "typesafe", "unavailable", "keyword")
# Confidence bands for TypeSafe answers: [low, medium) [medium, high) [high, 1]
CONFIDENCE_BANDS = (("low", 0.0), ("medium", 1 / 3), ("high", 2 / 3))

# Keyword lists of the method used before TypeSafe (kept only as a comparison baseline)
_BULLISH_WORDS = [
    "strong", "growth", "positive", "bullish", "upside", "outperform",
    "beat", "exceeded", "momentum", "opportunity", "upgrade", "buy",
    "overweight", "impressive", "robust", "accelerating", "expanding",
]
_BEARISH_WORDS = [
    "weak", "decline", "negative", "bearish", "downside", "underperform",
    "miss", "missed", "risk", "concern", "downgrade", "sell",
    "underweight", "disappointing", "slowing", "contracting", "warning",
]


def direction(score: float) -> str:
    """0-1 signal -> direction: below 0.4 bearish, above 0.6 bullish, otherwise neutral."""
    if score < 0.4:
        return "bearish"
    if score > 0.6:
        return "bullish"
    return "neutral"


def keyword_direction(text: str) -> str:
    """Direction from the old keyword-count method: share of bullish vs bearish word hits."""
    lower = (text or "").lower()
    bull = sum(lower.count(w) for w in _BULLISH_WORDS)
    bear = sum(lower.count(w) for w in _BEARISH_WORDS)
    return direction(bull / (bull + bear)) if bull + bear else "neutral"


def _row(method: str, labelled: int, agree: int) -> dict:
    return {"method": method, "labelled": labelled, "agree": agree,
            "agreement": agree / labelled if labelled else None}


def accuracy(rows: Iterable[dict]) -> dict:
    """rows: dicts with label, score, source, confidence (TypeSafe only) and text."""
    rows = list(rows)
    counts = {m: [0, 0] for m in METHODS}  # method -> [labelled, agree]
    bands = {name: [0, 0] for name, _ in CONFIDENCE_BANDS}
    for r in rows:
        extractor_agrees = direction(r["score"]) == r["label"]
        counts[r["source"]][0] += 1
        counts[r["source"]][1] += extractor_agrees
        counts["keyword"][0] += 1
        counts["keyword"][1] += keyword_direction(r["text"]) == r["label"]
        if r["source"] == "typesafe" and r.get("confidence") is not None:
            band = _band(r["confidence"])
            bands[band][0] += 1
            bands[band][1] += extractor_agrees
    return {
        "by_method": [_row(m, *counts[m]) for m in METHODS],
        "typesafe_by_confidence": [
            {"band": name, "from": low, **_row("typesafe", *bands[name])} for name, low in CONFIDENCE_BANDS
        ],
    }


def _band(confidence: float) -> str:
    name: Optional[str] = None
    for band, low in CONFIDENCE_BANDS:
        if confidence >= low:
            name = band
    return name or "low"

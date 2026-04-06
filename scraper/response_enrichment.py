import json
import logging
import os
from collections import Counter
from statistics import mean, median
from typing import Any

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

logger = logging.getLogger(__name__)

VALID_LISTING_TYPES = ("sale", "rent")
VALID_PROPERTY_TYPES = ("residential", "commercial", "land")

TR_ASCII_MAP = str.maketrans("ÇĞİÖŞÜçğıöşü", "CGIOSUcgiosu")


def _ascii_text(value: str | None) -> str:
    if not value:
        return ""
    return str(value).translate(TR_ASCII_MAP)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        number = float(value)
        return number if number > 0 else None
    except (TypeError, ValueError):
        return None


def _bucket_name(listing_type: str | None, property_type: str | None) -> str:
    lt = listing_type if listing_type in VALID_LISTING_TYPES else "unknown_listing"
    pt = property_type if property_type in VALID_PROPERTY_TYPES else "unknown_property"
    return f"{lt}_{pt}"


def _listing_preview(item: dict) -> dict:
    return {
        "source_url": item.get("source_url"),
        "source_site": item.get("source_site"),
        "title": item.get("title"),
        "title_ascii": _ascii_text(item.get("title")),
        "price": item.get("price"),
        "currency": item.get("currency"),
        "area_sqm": item.get("area_sqm"),
        "rooms": item.get("rooms"),
        "listing_type": item.get("listing_type"),
        "property_type": item.get("property_type"),
        "district": item.get("district"),
        "city": item.get("city"),
    }


def categorize_listings(listings: list[dict]) -> dict:
    buckets: dict[str, list[dict]] = {}
    for raw in listings:
        bucket = _bucket_name(raw.get("listing_type"), raw.get("property_type"))
        buckets.setdefault(bucket, []).append(_listing_preview(raw))

    by_listing_type = {
        "sale": 0,
        "rent": 0,
        "unknown": 0,
    }
    by_property_type = {
        "residential": 0,
        "commercial": 0,
        "land": 0,
        "unknown": 0,
    }

    for raw in listings:
        lt = raw.get("listing_type")
        pt = raw.get("property_type")
        if lt in by_listing_type:
            by_listing_type[lt] += 1
        else:
            by_listing_type["unknown"] += 1
        if pt in by_property_type:
            by_property_type[pt] += 1
        else:
            by_property_type["unknown"] += 1

    segment_counts = {key: len(value) for key, value in buckets.items()}
    return {
        "total": len(listings),
        "by_listing_type": by_listing_type,
        "by_property_type": by_property_type,
        "by_segment": segment_counts,
        "segments": buckets,
    }


def build_derived_metrics(listings: list[dict], categorized: dict) -> dict:
    prices = [_safe_float(item.get("price")) for item in listings]
    prices = [value for value in prices if value is not None]

    areas = [_safe_float(item.get("area_sqm")) for item in listings]
    areas = [value for value in areas if value is not None]

    price_per_m2_values: list[float] = []
    for item in listings:
        price = _safe_float(item.get("price"))
        area = _safe_float(item.get("area_sqm"))
        if price is not None and area is not None and area > 0:
            price_per_m2_values.append(price / area)

    room_counter = Counter()
    source_counter = Counter()
    for item in listings:
        rooms = item.get("rooms")
        if rooms:
            room_counter[str(rooms)] += 1
        site = item.get("source_site") or "unknown"
        source_counter[str(site)] += 1

    dominant_segment = None
    if categorized.get("by_segment"):
        dominant_segment = max(categorized["by_segment"], key=categorized["by_segment"].get)

    return {
        "listing_count": len(listings),
        "priced_listing_count": len(prices),
        "area_known_count": len(areas),
        "price_stats_try": {
            "min": min(prices) if prices else None,
            "max": max(prices) if prices else None,
            "avg": round(mean(prices), 2) if prices else None,
            "median": round(median(prices), 2) if prices else None,
        },
        "area_stats_m2": {
            "min": min(areas) if areas else None,
            "max": max(areas) if areas else None,
            "avg": round(mean(areas), 2) if areas else None,
            "median": round(median(areas), 2) if areas else None,
        },
        "price_per_m2_try": {
            "count": len(price_per_m2_values),
            "avg": round(mean(price_per_m2_values), 2) if price_per_m2_values else None,
            "median": round(median(price_per_m2_values), 2) if price_per_m2_values else None,
        },
        "room_distribution": dict(room_counter.most_common(8)),
        "source_distribution": dict(source_counter),
        "dominant_segment": dominant_segment,
    }


def build_rule_based_inference(categorized: dict, metrics: dict) -> list[str]:
    insights: list[str] = []

    sale_count = categorized["by_listing_type"].get("sale", 0)
    rent_count = categorized["by_listing_type"].get("rent", 0)
    commercial_count = categorized["by_property_type"].get("commercial", 0)
    residential_count = categorized["by_property_type"].get("residential", 0)

    if sale_count > rent_count:
        insights.append("Satis ilan agirligi kiraliga gore daha yuksek.")
    elif rent_count > sale_count:
        insights.append("Kiralik ilan agirligi satiliga gore daha yuksek.")
    else:
        insights.append("Satis ve kiralik tarafinda dagilim dengeli.")

    if commercial_count > residential_count:
        insights.append("Ticari ilan yogunlugu konut ilanlarindan fazla.")
    elif residential_count > 0:
        insights.append("Konut ilan yogunlugu ticari ilanlardan fazla.")

    p2 = metrics.get("price_per_m2_try", {})
    if p2.get("avg") is not None:
        insights.append(f"Ortalama m2 fiyati yaklasik {int(p2['avg'])} TRY seviyesinde.")

    if metrics.get("priced_listing_count", 0) == 0:
        insights.append("Fiyat verisi yetersiz oldugu icin fiyat trendi sinirli yorumlanabilir.")
    if metrics.get("area_known_count", 0) == 0:
        insights.append("Metrekare verisi yetersiz oldugu icin m2 bazli analiz sinirli.")

    dominant = metrics.get("dominant_segment")
    if dominant:
        insights.append(f"Baskin segment: {dominant}.")

    return [_ascii_text(item) for item in insights]


def build_market_rows(listings: list[dict], limit: int = 80) -> list[dict]:
    rows: list[dict] = []
    for item in listings:
        price = _safe_float(item.get("price"))
        if price is None:
            continue

        area = _safe_float(item.get("area_sqm"))
        listing_type = item.get("listing_type")
        property_type = item.get("property_type")
        row = {
            "source_url": item.get("source_url"),
            "source_site": item.get("source_site"),
            "title": _ascii_text(item.get("title")),
            "listing_type": listing_type,
            "property_type": property_type,
            "listing_kind": _bucket_name(listing_type, property_type),
            "price_try": round(price, 2),
            "size_m2_try": round(area, 2) if area is not None else None,
            "price_per_m2_try": round(price / area, 2) if area and area > 0 else None,
            "rooms_try": item.get("rooms"),
            "district": item.get("district"),
            "city": item.get("city"),
        }
        rows.append(row)

    rows.sort(
        key=lambda r: (
            1 if r.get("size_m2_try") is not None else 0,
            1 if r.get("price_per_m2_try") is not None else 0,
            r.get("price_try") or 0,
        ),
        reverse=True,
    )
    return rows[: max(1, limit)]


def _extract_json_object(raw_text: str) -> dict | None:
    text = (raw_text or "").strip()
    if not text:
        return None
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    if text.startswith("json"):
        text = text[4:].strip()
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        return None
    return None


def _can_use_ai() -> bool:
    if OpenAI is None:
        return False
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return False
    if api_key.lower() in {"ollama", "none", "null"}:
        return False
    return True


def _ai_client() -> OpenAI:
    if OpenAI is None:
        raise RuntimeError("openai package is not installed")
    return OpenAI(
        api_key=os.getenv("OPENAI_API_KEY", "ollama"),
        base_url=os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1"),
    )


def generate_ai_inference(
    listings: list[dict],
    categorized: dict,
    metrics: dict,
    search_context: dict | None = None,
) -> dict:
    if not _can_use_ai():
        reason = "OPENAI_API_KEY not configured"
        if OpenAI is None:
            reason = "openai package not installed"
        return {
            "status": "skipped",
            "reason": reason,
        }

    model = os.getenv("AI_MODEL", "gpt-4o-mini")
    context_payload = {
        "categorized_summary": {
            "by_listing_type": categorized.get("by_listing_type", {}),
            "by_property_type": categorized.get("by_property_type", {}),
            "by_segment": categorized.get("by_segment", {}),
        },
        "metrics": metrics,
        "search_context": {
            "geo": (search_context or {}).get("geo", {}),
            "query_count": (search_context or {}).get("query_count"),
            "search_strategy": (search_context or {}).get("search_strategy"),
            "google_throttled": (search_context or {}).get("google_throttled"),
        },
        "listing_samples": [
            {
                "title": item.get("title"),
                "price": item.get("price"),
                "area_sqm": item.get("area_sqm"),
                "rooms": item.get("rooms"),
                "listing_type": item.get("listing_type"),
                "property_type": item.get("property_type"),
                "district": item.get("district"),
                "city": item.get("city"),
                "source_site": item.get("source_site"),
            }
            for item in listings[:20]
        ],
    }

    prompt = (
        "You are an analyst for real-estate listing results.\n"
        "Use only provided data. Do not invent fields.\n"
        "Return ASCII-only Turkish text (do not use chars: ç, ğ, ı, ö, ş, ü).\n"
        "Return ONLY JSON with this schema:\n"
        "{\n"
        '  "overall_summary": "short paragraph",\n'
        '  "pricing_insight": "price-level and spread summary",\n'
        '  "segment_insight": "listing type and property type interpretation",\n'
        '  "feature_inference": ["3-6 concise bullet strings"],\n'
        '  "risk_flags": ["0-5 concise bullet strings"],\n'
        '  "next_actions": ["2-5 practical actions"]\n'
        "}\n\n"
        f"Input JSON:\n{json.dumps(context_payload, ensure_ascii=False)}"
    )

    try:
        client = _ai_client()
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        raw = (response.choices[0].message.content or "").strip()
        parsed = _extract_json_object(raw)
        if not parsed:
            return {
                "status": "error",
                "reason": "invalid_ai_json",
                "raw_preview": _ascii_text(raw[:400]),
            }

        sanitized = {
            "overall_summary": _ascii_text(parsed.get("overall_summary")),
            "pricing_insight": _ascii_text(parsed.get("pricing_insight")),
            "segment_insight": _ascii_text(parsed.get("segment_insight")),
            "feature_inference": [_ascii_text(item) for item in parsed.get("feature_inference", []) if item],
            "risk_flags": [_ascii_text(item) for item in parsed.get("risk_flags", []) if item],
            "next_actions": [_ascii_text(item) for item in parsed.get("next_actions", []) if item],
        }
        return {
            "status": "ok",
            "model": model,
            "analysis": sanitized,
        }
    except Exception as exc:
        logger.warning("AI inference failed: %s", exc)
        return {
            "status": "error",
            "reason": _ascii_text(str(exc)),
        }


def enrich_job_payload(payload: dict, include_ai: bool = True) -> dict:
    listings = payload.get("listings") or []
    if not isinstance(listings, list):
        listings = []

    categorized = categorize_listings(listings)
    metrics = build_derived_metrics(listings, categorized)
    rule_based = build_rule_based_inference(categorized, metrics)
    market_rows_limit = int(os.getenv("MARKET_ROWS_LIMIT", "80") or "80")
    market_rows = build_market_rows(listings, limit=market_rows_limit)
    if include_ai:
        ai_result = generate_ai_inference(
            listings=listings,
            categorized=categorized,
            metrics=metrics,
            search_context=payload.get("search_context") or {},
        )
    else:
        ai_result = {
            "status": "skipped",
            "reason": "disabled_for_fast_mode",
        }

    payload["categorized_listings"] = categorized
    payload["analysis"] = {
        "derived_metrics": metrics,
        "rule_based_inference": rule_based,
        "ai": ai_result,
    }
    payload["market_rows"] = market_rows
    payload["market_rows_count"] = len(market_rows)
    return payload

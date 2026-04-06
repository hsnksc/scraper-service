import asyncio
import logging
import os
import re

import aiohttp
from geopy.geocoders import Nominatim
from googlesearch import search
from dotenv import load_dotenv

from scraper.site_detector import is_real_estate_domain, detect_site, is_listing_url

logger = logging.getLogger(__name__)
load_dotenv()

try:
    from ddgs import DDGS
except Exception:
    DDGS = None

LISTING_TYPE_KEYWORDS_TR = {
    "sale": ["satilik", "satilik emlak"],
    "rent": ["kiralik", "kiralik emlak"],
    "all": ["emlak", "satilik", "kiralik"],
}

PROPERTY_TYPE_KEYWORDS_TR = {
    "residential": ["konut", "daire"],
    "commercial": ["ticari", "isyeri"],
    "land": ["arsa", "tarla"],
    "all": ["emlak", "ticari", "isyeri"],
}

KNOWN_SITES = [
    "sahibinden.com",
    "hepsiemlak.com",
    "emlakjet.com",
    "hurriyetemlak.com",
    "emlakcarsi.com",
    "emlakmarketiantalya.com",
    "rentola.com.tr",
    "tr.flatspotter.com",
    "flatspotter.com",
    "emlakgo.net",
    "remax.com.tr",
]

DIRECT_SITE_SOURCES = ["direct_site", "google", "duckduckgo"]
TAVILY_API_URL = "https://api.tavily.com/search"
SERPER_API_URL = "https://google.serper.dev/search"
GOOGLE_CSE_API_URL = "https://www.googleapis.com/customsearch/v1"
VERTEX_SEARCH_API_BASE = "https://discoveryengine.googleapis.com/v1"
EXA_SEARCH_API_URL = "https://api.exa.ai/search"


def parse_coordinates(location: str) -> tuple[str, str] | None:
    """Try to parse 'lat,lng' format. Return (lat, lng) or None."""
    parts = location.strip().split(",")
    if len(parts) != 2:
        return None

    try:
        lat, lng = parts[0].strip(), parts[1].strip()
        float(lat)
        float(lng)
        return lat, lng
    except ValueError:
        return None


async def reverse_geocode(lat: str, lng: str) -> dict:
    """Reverse geocode using Nominatim. Returns neighborhood + district + city + nearby streets."""
    results = {
        "neighborhood": "",
        "district": "",
        "city": "",
        "city_full": "",
        "road": "",
        "suburb": "",
        "county": "",
    }

    def _lookup() -> dict:
        geocode_timeout = float(os.getenv("REVERSE_GEOCODE_TIMEOUT_SECONDS", "8") or "8")
        geolocator = Nominatim(user_agent="emlak-scraper", timeout=geocode_timeout)
        location = geolocator.reverse(f"{lat},{lng}", language="tr", addressdetails=True)
        if not location or not location.raw:
            return results

        addr = location.raw.get("address", {})
        return {
            "neighborhood": addr.get("neighbourhood") or addr.get("quarter") or "",
            "district": addr.get("suburb") or addr.get("city_district") or "",
            "road": addr.get("road") or "",
            "suburb": addr.get("suburb") or "",
            "county": addr.get("county") or "",
            "city": addr.get("city") or addr.get("town") or addr.get("province") or "",
            "city_full": location.address or "",
        }

    try:
        return await asyncio.to_thread(_lookup)
    except Exception as e:
        logger.warning(f"Reverse geocode failed for {lat},{lng}: {e}")
        return results


def _location_variants(geo: dict) -> list[str]:
    variants: list[str] = []

    if geo.get("road"):
        parts = [geo["road"], geo.get("neighborhood", ""), geo.get("district", ""), geo.get("city", "")]
        variants.append(" ".join(part for part in parts if part))

    for keys in [
        ("neighborhood", "district", "city"),
        ("district", "city"),
        ("county", "city"),
        ("city",),
    ]:
        parts = [geo.get(key, "") for key in keys]
        variant = " ".join(part for part in parts if part)
        if variant:
            variants.append(variant)

    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        normalized = " ".join(variant.split())
        if normalized and normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)
    return deduped


def _slugify(text: str) -> str:
    replacements = {
        "ç": "c",
        "ğ": "g",
        "ı": "i",
        "ö": "o",
        "ş": "s",
        "ü": "u",
        "Ç": "c",
        "Ğ": "g",
        "İ": "i",
        "Ö": "o",
        "Ş": "s",
        "Ü": "u",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-")


def _normalize_tavily_query(query: str) -> str:
    query = query.replace('"', "").strip()
    query = re.sub(r"^\d+\s+\w+\s+", "", query, flags=re.IGNORECASE)
    query = re.sub(r"\s+", " ", query).strip()
    return query


def _normalize_text(value: str) -> str:
    mapping = str.maketrans("ÇĞİÖŞÜçğıöşü", "CGIOSUcgiosu")
    return (value or "").translate(mapping).lower()


def _parse_number_token(token: str) -> float | None:
    token = (token or "").strip().replace(" ", "")
    if not token:
        return None

    if "," in token and "." in token:
        # Prefer the last separator as decimal marker.
        if token.rfind(",") > token.rfind("."):
            normalized = token.replace(".", "").replace(",", ".")
        else:
            normalized = token.replace(",", "")
    elif token.count(".") > 1 and "," not in token:
        parts = token.split(".")
        if len(parts[0]) <= 3 and all(len(part) == 3 for part in parts[1:]):
            normalized = "".join(parts)
        else:
            return None
    elif token.count(",") > 1 and "." not in token:
        parts = token.split(",")
        if len(parts[0]) <= 3 and all(len(part) == 3 for part in parts[1:]):
            normalized = "".join(parts)
        else:
            return None
    elif "." in token:
        head, tail = token.split(".", 1)
        if len(tail) == 3 and head:
            normalized = head + tail
        else:
            normalized = token
    elif "," in token:
        head, tail = token.split(",", 1)
        if len(tail) == 3 and head:
            normalized = head + tail
        else:
            normalized = head + "." + tail
    else:
        normalized = token

    try:
        value = float(normalized)
    except ValueError:
        return None
    return value if value > 0 else None


def _extract_price_hint(text: str) -> float | None:
    if not text:
        return None

    # ₺ önde: ₺6.850.000 veya ₺ 6.850.000
    tl_prefix = re.compile(
        r"₺\s*(\d{1,3}(?:[.\s]\d{3})+|\d{4,11})\b",
        re.IGNORECASE,
    )
    for match in tl_prefix.finditer(text):
        value = _parse_number_token(match.group(1))
        if value is not None and value >= 1_000:
            return value

    # Sayı sonda TL/₺: 6.850.000 TL veya 6.850.000₺
    tl_suffix = re.compile(
        r"(?<!\d)(\d{1,3}(?:[.\s]\d{3})+|\d{4,11}|\d+(?:[.,]\d{1,2})?)\s*(?:TL|₺)\b",
        re.IGNORECASE,
    )
    for match in tl_suffix.finditer(text):
        value = _parse_number_token(match.group(1))
        if value is not None and value >= 1_000:
            return value

    # X milyon [TL/₺]
    milyon_match = re.search(r"(\d+(?:[.,]\d+)?)\s*milyon", _normalize_text(text), re.IGNORECASE)
    if milyon_match:
        base = _parse_number_token(milyon_match.group(1))
        if base is not None:
            value = base * 1_000_000
            return value if value > 0 else None

    # Snippet'lerde sadece büyük rakam (fiyat satırları): "30.000" veya "1.273"
    # Sadece noktalı binlik ayıraçlı rakamlar (min 4 hane)
    bare_number = re.compile(r"(?<!\d)(\d{1,3}(?:\.\d{3})+)(?!\s*m[²2]|\s*\d)(?!\w)")
    for match in bare_number.finditer(text):
        value = _parse_number_token(match.group(1))
        if value is not None and 5_000 <= value <= 500_000_000:
            return value

    return None


def _extract_area_hint(text: str) -> float | None:
    if not text:
        return None

    match = re.search(r"(?<!\d)(\d{2,4}(?:[.,]\d{1,2})?)\s*(?:m²|m2|metrekare)\b", text, re.IGNORECASE)
    if not match:
        return None

    value = _parse_number_token(match.group(1))
    if value is None:
        return None

    if 10 <= value <= 5000:
        return value
    return None


def _extract_rooms_hint(text: str) -> str | None:
    if not text:
        return None

    room_match = re.search(r"\b(\d{1,2}\+\d{1,2})\b", text)
    if room_match:
        return room_match.group(1)

    normalized = _normalize_text(text)
    if "studyo" in normalized or "stüdyo" in text.lower():
        return "stüdyo"
    return None


def _infer_listing_type_hint(url: str = "", title: str = "", snippet: str = "") -> str | None:
    primary = _normalize_text(" ".join([url, title]))
    secondary = _normalize_text(snippet)
    if "kiralik" in primary or "for-rent" in primary or "kira" in primary:
        return "rent"
    if "satilik" in primary or "for-sale" in primary or "sale" in primary:
        return "sale"
    if "kiralik" in secondary or "for-rent" in secondary:
        return "rent"
    if "satilik" in secondary or "for-sale" in secondary:
        return "sale"
    return None


def _infer_property_type_hint(url: str = "", title: str = "", snippet: str = "") -> str | None:
    primary = _normalize_text(" ".join([url, title]))
    secondary = _normalize_text(snippet)

    commercial_tokens = ["isyeri", "is-yeri", "ticari", "ofis", "dukkan", "dukkan", "buro", "magaza", "depo", "plaza"]
    land_tokens = ["arsa", "tarla", "zeytinlik", "bag", "bahce"]
    residential_tokens = ["konut", "daire", "ev", "villa", "rezidans", "apartman", "mustakil"]

    if any(token in primary for token in commercial_tokens):
        return "commercial"
    if any(token in primary for token in land_tokens):
        return "land"
    if any(token in primary for token in residential_tokens):
        return "residential"
    if any(token in secondary for token in commercial_tokens):
        return "commercial"
    if any(token in secondary for token in land_tokens):
        return "land"
    if any(token in secondary for token in residential_tokens):
        return "residential"
    return None


def _build_candidate(url: str, source: str, title: str = "", snippet: str = "") -> dict:
    listing_type_hint = _infer_listing_type_hint(url=url, title=title, snippet=snippet)
    property_type_hint = _infer_property_type_hint(url=url, title=title, snippet=snippet)
    price_hint = _extract_price_hint(title) or _extract_price_hint(snippet)
    area_hint = _extract_area_hint(title) or _extract_area_hint(snippet)
    rooms_hint = _extract_rooms_hint(title) or _extract_rooms_hint(snippet)
    return {
        "url": url,
        "sources": [source],
        "title": title or None,
        "snippet": snippet or None,
        "listing_type_hint": listing_type_hint,
        "property_type_hint": property_type_hint,
        "price_hint": price_hint,
        "area_hint": area_hint,
        "rooms_hint": rooms_hint,
    }


def _upsert_candidate(candidate_map: dict, candidate: dict):
    url = candidate.get("url", "")
    if not url:
        return

    current = candidate_map.get(url)
    if not current:
        candidate_map[url] = candidate
        return

    merged_sources = set(current.get("sources", [])) | set(candidate.get("sources", []))
    current["sources"] = sorted(merged_sources)
    if not current.get("title") and candidate.get("title"):
        current["title"] = candidate["title"]
    if not current.get("snippet") and candidate.get("snippet"):
        current["snippet"] = candidate["snippet"]
    if not current.get("listing_type_hint") and candidate.get("listing_type_hint"):
        current["listing_type_hint"] = candidate["listing_type_hint"]
    if not current.get("property_type_hint") and candidate.get("property_type_hint"):
        current["property_type_hint"] = candidate["property_type_hint"]
    if not current.get("price_hint") and candidate.get("price_hint"):
        current["price_hint"] = candidate["price_hint"]
    if not current.get("area_hint") and candidate.get("area_hint"):
        current["area_hint"] = candidate["area_hint"]
    if not current.get("rooms_hint") and candidate.get("rooms_hint"):
        current["rooms_hint"] = candidate["rooms_hint"]


def _extract_admin_district(geo: dict) -> str:
    if geo.get("county"):
        return geo["county"]

    city = geo.get("city", "").strip()
    full = geo.get("city_full", "").strip()
    if not city or not full:
        return ""

    parts = [part.strip() for part in full.split(",") if part.strip()]
    city_indexes = [i for i, part in enumerate(parts) if part == city]
    if len(city_indexes) >= 2:
        candidate_index = city_indexes[-1] - 1
        if 0 <= candidate_index < len(parts):
            candidate = parts[candidate_index]
            if candidate != city and candidate != geo.get("district") and candidate != geo.get("road"):
                return candidate
    return ""


def _query_terms(listing_type: str, property_type: str) -> list[str]:
    listing_terms = LISTING_TYPE_KEYWORDS_TR.get(listing_type, LISTING_TYPE_KEYWORDS_TR["all"])
    property_terms = PROPERTY_TYPE_KEYWORDS_TR.get(property_type, PROPERTY_TYPE_KEYWORDS_TR["all"])

    if listing_type == "all" and property_type == "all":
        # 4 kategori: Konut Satılık, Konut Kiralık, Ticari Satılık, Ticari Kiralık
        return [
            "satilik daire konut",
            "kiralik daire konut",
            "satilik ticari isyeri",
            "kiralik ticari isyeri",
            "satilik emlak",
            "kiralik emlak",
        ]

    if listing_type == "all":
        primary_property = property_terms[0]
        return [f"satilik {primary_property}", f"kiralik {primary_property}", f"{primary_property} emlak"]

    if property_type == "all":
        primary_listing = listing_terms[0]
        return [primary_listing, f"{primary_listing} emlak"]

    return [f"{listing_terms[0]} {property_terms[0]}", f"{listing_terms[0]} emlak {property_terms[0]}"]


def _add_search_strategy(current: str, source: str) -> str:
    if current == "direct_only":
        return f"direct_plus_{source}"
    if source in current:
        return current
    return f"{current}_{source}"


async def _run_provider_with_timeout(name: str, coro, timeout_seconds: float) -> tuple[str, list[dict]]:
    try:
        rows = await asyncio.wait_for(coro, timeout=timeout_seconds)
        if not isinstance(rows, list):
            return name, []
        return name, rows
    except Exception as exc:
        logger.warning(f"{name} provider failed or timed out: {exc}")
        return name, []


def build_pseudo_listings_from_candidates(
    candidates: list[dict],
    geo: dict | None = None,
    limit: int = 60,
) -> list[dict]:
    geo = geo or {}
    district = geo.get("district") or geo.get("neighborhood") or geo.get("suburb") or None
    city = geo.get("city") or None

    pseudo: list[dict] = []
    for candidate in candidates[: max(1, limit)]:
        sources = candidate.get("sources") or []
        source_site = sources[0] if sources else "discovery"
        title = candidate.get("title") or candidate.get("url")
        snippet = candidate.get("snippet") or ""
        pseudo.append(
            {
                "source_url": candidate.get("url"),
                "source_site": source_site,
                "title": title,
                "description": snippet[:1200] if isinstance(snippet, str) else "",
                "price": candidate.get("price_hint"),
                "currency": "TRY",
                "area_sqm": candidate.get("area_hint"),
                "rooms": candidate.get("rooms_hint"),
                "listing_type": candidate.get("listing_type_hint"),
                "property_type": candidate.get("property_type_hint"),
                "district": district,
                "city": city,
            }
        )

    return pseudo


_LOCATION_STOPWORDS = {
    "mahallesi",
    "mahalle",
    "mah",
    "caddesi",
    "cadde",
    "cd",
    "sokak",
    "sk",
    "bulvari",
    "bulvar",
    "blv",
    "sitesi",
}

_ANTALYA_DISTRICTS_ASCII = {
    "muratpasa",
    "kepez",
    "konyaalti",
    "dosemealti",
    "aksu",
    "serik",
    "manavgat",
    "alanya",
    "kas",
    "kumluca",
    "demre",
    "finike",
    "gazipasa",
    "gundogmus",
    "ibradi",
    "korkuteli",
    "elmali",
    "kemer",
}


def _tokenize_location(text: str) -> list[str]:
    tokens = [part for part in re.split(r"[^a-z0-9]+", _normalize_text(text)) if part]
    return [token for token in tokens if token not in _LOCATION_STOPWORDS and len(token) >= 3]


def _district_tokens_ordered() -> list[str]:
    # deterministic order for matching against headers
    return [
        "muratpasa",
        "kepez",
        "konyaalti",
        "dosemealti",
        "aksu",
        "serik",
        "manavgat",
        "alanya",
        "kas",
        "kumluca",
        "demre",
        "finike",
        "gazipasa",
        "gundogmus",
        "ibradi",
        "korkuteli",
        "elmali",
        "kemer",
    ]


def _locality_context(geo: dict) -> dict:
    district = (
        geo.get("district")
        or geo.get("neighborhood")
        or geo.get("suburb")
        or ""
    ).strip()
    city = (geo.get("city") or "").strip()
    road = (geo.get("road") or "").strip()

    district_n = _normalize_text(district)
    city_n = _normalize_text(city)
    road_tokens = _tokenize_location(road)
    district_tokens = _tokenize_location(district)
    city_full_n = _normalize_text(geo.get("city_full", ""))

    admin_district = ""
    for token in _district_tokens_ordered():
        if token in city_full_n:
            admin_district = token
            break

    return {
        "district": district_n,
        "district_tokens": district_tokens,
        "city": city_n,
        "road_tokens": road_tokens,
        "admin_district": admin_district,
    }


def _is_strong_district_match(header_text: str, district_tokens: list[str], admin_district: str) -> bool:
    if not district_tokens:
        return False

    primary = district_tokens[0]
    if not primary:
        return False

    # e.g. "fener mahallesi" pattern
    if re.search(rf"\b{re.escape(primary)}\b[\s\-_/]*mah", header_text):
        return True

    # e.g. "muratpasa fener" / "fener muratpasa"
    if admin_district and admin_district in header_text and primary in header_text:
        return True

    return False


def _locality_score(candidate: dict, geo: dict, strict_district_header: bool | None = None) -> int:
    header_text = _normalize_text(" ".join([
        candidate.get("url", "") or "",
        candidate.get("title", "") or "",
    ]))
    full_text = _normalize_text(" ".join([
        candidate.get("url", "") or "",
        candidate.get("title", "") or "",
        candidate.get("snippet", "") or "",
    ]))
    ctx = _locality_context(geo)
    if strict_district_header is None:
        strict_district_header = os.getenv("STRICT_DISTRICT_HEADER", "true").strip().lower() in {"1", "true", "yes", "on"}

    score = 0
    district_tokens = ctx["district_tokens"]
    district = ctx["district"]
    city = ctx["city"]
    admin_district = ctx.get("admin_district")

    district_hit_header = _is_strong_district_match(header_text, district_tokens, admin_district)
    district_hit_any = any(token in full_text for token in district_tokens) if district_tokens else (district and district in full_text)

    if strict_district_header and district_tokens and not district_hit_header:
        return -99

    if district_hit_header:
        score += 6
    elif district_hit_any:
        score += 3

    road_hits_header = sum(1 for token in ctx["road_tokens"] if token in header_text)
    score += min(road_hits_header, 2)
    if road_hits_header == 0:
        road_hits_full = sum(1 for token in ctx["road_tokens"] if token in full_text)
        score += min(road_hits_full, 1)

    if city and city in header_text:
        score += 1

    if "/ilan/" in candidate.get("url", ""):
        score += 1

    if city == "antalya":
        header_admin_hits = [token for token in _district_tokens_ordered() if token in header_text]
        if admin_district:
            if header_admin_hits and admin_district not in header_admin_hits:
                return -99
        elif district_tokens:
            if header_admin_hits and not district_hit_header:
                return -99

    return score


def _rank_and_filter_candidates(
    urls: list[str],
    candidate_map: dict[str, dict],
    geo: dict,
    strict_detail_only: bool | None = None,
    strict_district_header: bool | None = None,
) -> tuple[list[str], list[dict], dict]:
    scored: list[dict] = []
    if strict_detail_only is None:
        strict_detail_only = os.getenv("STRICT_DETAIL_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"}

    def _is_detail_url(url: str) -> bool:
        site_key = detect_site(url)
        if site_key and is_listing_url(url, site_key):
            return True
        lower_url = (url or "").lower()
        return any(token in lower_url for token in ["/ilan/", "/detay", "/listing/", "/site-"])

    for url in urls:
        candidate = candidate_map.get(url) or _build_candidate(url=url, source="unknown")
        score = _locality_score(candidate, geo, strict_district_header=strict_district_header)
        candidate["locality_score"] = score
        candidate["is_local_match"] = score >= 4
        candidate["is_detail_url"] = _is_detail_url(url)
        scored.append(candidate)

    filtered = [item for item in scored if item.get("is_local_match")]
    used_filtered = len(filtered) >= min(4, max(1, len(scored)))
    items = filtered if used_filtered else scored

    detail_items = [item for item in items if item.get("is_detail_url")]
    detail_filter_applied = False
    if strict_detail_only and len(detail_items) >= min(4, max(1, len(items) // 3)):
        items = detail_items
        detail_filter_applied = True

    items.sort(
        key=lambda item: (
            1 if item.get("is_local_match") else 0,
            1 if item.get("is_detail_url") else 0,
            item.get("locality_score", 0),
            len(item.get("sources", [])),
        ),
        reverse=True,
    )

    ranked_urls = [item["url"] for item in items]
    return ranked_urls, items, {
        "locality_filter_applied": used_filtered,
        "locality_min_score": 4,
        "locality_total_candidates": len(scored),
        "locality_filtered_candidates": len(filtered),
        "detail_filter_applied": detail_filter_applied,
        "strict_detail_only": strict_detail_only,
    }


def build_direct_site_candidates(geo: dict, listing_type: str = "all", property_type: str = "all") -> list[str]:
    city = geo.get("city", "").strip()
    neighborhood = (geo.get("district") or geo.get("neighborhood") or geo.get("suburb") or "").strip()
    admin_district = _extract_admin_district(geo).strip()

    if not city or not neighborhood:
        return []

    city_slug = _slugify(city)
    neighborhood_slug = _slugify(neighborhood)
    admin_slug = _slugify(admin_district) if admin_district else ""

    listing_slugs = {
        "sale": ["satilik"],
        "rent": ["kiralik"],
        "all": ["satilik", "kiralik"],
    }.get(listing_type, ["satilik", "kiralik"])

    property_slugs = {
        "residential": ["daire", "konut"],
        "commercial": ["isyeri", "ticari"],
        "land": ["arsa"],
        "all": ["daire", "konut", "isyeri", "ticari", "arsa"],
    }.get(property_type, ["daire", "konut", "arsa"])

    candidates: list[str] = []

    for listing_slug in listing_slugs:
        for property_slug in property_slugs:
            location_parts = [city_slug]
            if admin_slug:
                location_parts.append(admin_slug)
            location_parts.append(neighborhood_slug)
            location_slug = "-".join(part for part in location_parts if part)

            candidates.append(f"https://www.emlakjet.com/{listing_slug}-{property_slug}/{location_slug}")
            candidates.append(f"https://www.emlakjet.com/{listing_slug}-{property_slug}/{city_slug}-{neighborhood_slug}")

        hepsi_base = "-".join(part for part in [admin_slug, neighborhood_slug] if part)
        if hepsi_base:
            candidates.append(f"https://www.hepsiemlak.com/{hepsi_base}-{listing_slug}")
            if property_type == "residential" or property_type == "all":
                candidates.append(f"https://www.hepsiemlak.com/{hepsi_base}-{listing_slug}/daire")
                candidates.append(f"https://www.hepsiemlak.com/{hepsi_base}-{listing_slug}/konut")
            if property_type == "commercial" or property_type == "all":
                candidates.append(f"https://www.hepsiemlak.com/{hepsi_base}-{listing_slug}/isyeri")
                candidates.append(f"https://www.hepsiemlak.com/{hepsi_base}-{listing_slug}/ticari")
            if property_type == "land" or property_type == "all":
                candidates.append(f"https://www.hepsiemlak.com/{hepsi_base}-{listing_slug}/arsa")

    if listing_type == "all" and property_type == "all":
        candidates.append(f"https://www.hepsiemlak.com/{city_slug}-{neighborhood_slug}")
        candidates.append(f"https://www.sahibinden.com/kategori/emlak?query_text={city_slug}+{neighborhood_slug}")
        candidates.append(f"https://www.sahibinden.com/emlak-isyeri?query_text={city_slug}+{neighborhood_slug}")
        candidates.append(f"https://www.sahibinden.com/satilik-isyeri?query_text={city_slug}+{neighborhood_slug}")
        candidates.append(f"https://www.sahibinden.com/kiralik-isyeri?query_text={city_slug}+{neighborhood_slug}")

    deduped: list[str] = []
    seen: set[str] = set()
    for url in candidates:
        if url not in seen:
            deduped.append(url)
            seen.add(url)
    return deduped[:12]


def build_radius_queries(geo: dict, listing_type: str = "all", property_type: str = "all") -> list[str]:
    """Build hyper-local Google queries targeting the coordinate's immediate area."""
    location_variants = _location_variants(geo)
    query_terms = _query_terms(listing_type, property_type)
    queries: list[str] = []

    for location in location_variants[:2]:
        quoted = f'"{location}"'
        for term in query_terms[:6]:
            queries.append(f"{quoted} {term}")

    primary_location = location_variants[0] if location_variants else geo.get("city", "")
    if primary_location:
        quoted_primary = f'"{primary_location}"'
        for site in KNOWN_SITES:
            for term in query_terms[:3]:
                queries.append(f"{quoted_primary} {term} site:{site}")

    if geo.get("road") and geo.get("district") and geo.get("city"):
        road_focus = f'"{geo["road"]} {geo["district"]} {geo["city"]}"'
        queries.append(f"{road_focus} emlak")
        if listing_type != "all":
            queries.append(f"{road_focus} {LISTING_TYPE_KEYWORDS_TR[listing_type][0]}")

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        normalized = " ".join(query.split())
        if normalized and normalized not in seen:
            deduped.append(normalized)
            seen.add(normalized)

    return deduped[:16]


async def search_google_pages(queries: list[str], num_pages: int = 1, delay: float = 4.0) -> tuple[list[str], bool]:
    """Scrape N pages of Google results per query. Return URLs plus whether Google throttled."""
    all_urls: list[str] = []
    seen: set[str] = set()
    google_throttled = False

    for i, query in enumerate(queries):
        if google_throttled:
            break
        logger.info(f"Google search [{i + 1}/{len(queries)}]: {query}")
        try:
            for start in range(0, num_pages * 10, 10):
                try:
                    results = list(search(query, num_results=10, start_num=start, lang="tr"))
                    for url in results:
                        if url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            all_urls.append(url)
                except Exception as e:
                    error_text = str(e).lower()
                    if "429" in error_text or "too many requests" in error_text or "/sorry/" in error_text:
                        google_throttled = True
                        logger.warning("Google throttled requests; stopping remaining queries")
                    logger.warning(f"Page {start // 10 + 1} failed for '{query}': {e}")
                    break
                await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Query '{query}' failed entirely: {e}")

    logger.info(f"Google search complete: {len(all_urls)} unique real estate URLs found")
    return all_urls, google_throttled


async def search_duckduckgo_pages(queries: list[str], max_results_per_query: int = 8) -> list[str]:
    """Fallback search using DuckDuckGo when Google throttles."""
    if DDGS is None:
        logger.warning("ddgs is not installed; DuckDuckGo fallback unavailable")
        return []

    def _search() -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        with DDGS() as ddgs:
            for query in queries[:6]:
                logger.info(f"DuckDuckGo search: {query}")
                try:
                    for result in ddgs.text(query, region="tr-tr", max_results=max_results_per_query):
                        url = result.get("href", "")
                        if url and url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            urls.append(url)
                except Exception as e:
                    logger.warning(f"DuckDuckGo failed for '{query}': {e}")
        return urls

    return await asyncio.to_thread(_search)


async def search_tavily_candidates(queries: list[str], max_results: int = 8) -> list[dict]:
    """Search with Tavily when an API key is configured."""
    api_key = os.getenv("TAVILY_API_KEY", "").strip()
    if not api_key:
        return []

    candidates: list[dict] = []
    seen: set[str] = set()

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    async with aiohttp.ClientSession() as session:
        normalized_queries: list[str] = []
        seen_queries: set[str] = set()
        for query in queries:
            normalized = _normalize_tavily_query(query)
            if normalized and normalized not in seen_queries:
                seen_queries.add(normalized)
                normalized_queries.append(normalized)

        provider_query_limit = max(1, int(os.getenv("DISCOVERY_PROVIDER_QUERY_LIMIT", "3") or "3"))
        for query in normalized_queries[:provider_query_limit]:
            payload = {
                "query": query,
                "topic": "general",
                "search_depth": "basic",
                "max_results": max_results,
                "include_answer": False,
                "include_raw_content": False,
                "include_domains": KNOWN_SITES,
                "country": "turkey",
            }
            try:
                async with session.post(
                    TAVILY_API_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Tavily returned {resp.status} for query '{query}'")
                        continue

                    data = await resp.json()
                    for result in data.get("results", []):
                        url = result.get("url", "")
                        if url and url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            title = result.get("title", "") or ""
                            snippet = result.get("content", "") or ""
                            candidates.append(_build_candidate(url=url, source="tavily", title=title, snippet=snippet))
            except Exception as e:
                logger.warning(f"Tavily failed for '{query}': {e}")

    return candidates


async def search_tavily_pages(queries: list[str], max_results: int = 8) -> list[str]:
    return [item["url"] for item in await search_tavily_candidates(queries, max_results=max_results)]


async def search_serper_candidates(queries: list[str], max_results: int = 8) -> list[dict]:
    """Search with Serper when an API key is configured."""
    api_key = os.getenv("SERPER_API_KEY", "").strip()
    if not api_key:
        return []

    candidates: list[dict] = []
    seen: set[str] = set()

    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }

    normalized_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        normalized = _normalize_tavily_query(query)
        if normalized and normalized not in seen_queries:
            seen_queries.add(normalized)
            normalized_queries.append(normalized)

    async with aiohttp.ClientSession() as session:
        provider_query_limit = max(1, int(os.getenv("DISCOVERY_PROVIDER_QUERY_LIMIT", "3") or "3"))
        for query in normalized_queries[:provider_query_limit]:
            payload = {
                "q": query,
                "gl": "tr",
                "hl": "tr",
                "num": max_results,
            }
            try:
                async with session.post(
                    SERPER_API_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Serper returned {resp.status} for query '{query}'")
                        continue

                    data = await resp.json()
                    for result in data.get("organic", []):
                        url = result.get("link", "")
                        if url and url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            title = result.get("title", "") or ""
                            snippet_parts = [result.get("snippet", "") or ""]
                            # Serper ek alanlar: attributes ve priceRange fiyat icin zenginlestirir
                            attrs = result.get("attributes") or {}
                            for v in attrs.values():
                                if v and isinstance(v, str):
                                    snippet_parts.append(v)
                            price_range = result.get("priceRange") or ""
                            if price_range:
                                snippet_parts.append(price_range)
                            date_str = result.get("date") or ""
                            if date_str:
                                snippet_parts.append(date_str)
                            snippet = " | ".join(p for p in snippet_parts if p)
                            candidates.append(_build_candidate(url=url, source="serper", title=title, snippet=snippet))
            except Exception as e:
                logger.warning(f"Serper failed for '{query}': {e}")

    return candidates


async def search_serper_pages(queries: list[str], max_results: int = 8) -> list[str]:
    return [item["url"] for item in await search_serper_candidates(queries, max_results=max_results)]


async def search_exa_candidates(queries: list[str], max_results: int = 8) -> list[dict]:
    """
    Search with Exa Search API.
    Requires EXA_API_KEY.
    """
    api_key = os.getenv("EXA_API_KEY", "").strip()
    if not api_key:
        return []

    search_type = os.getenv("EXA_SEARCH_TYPE", "auto").strip() or "auto"
    max_results = max(1, min(max_results, 25))

    candidates: list[dict] = []
    seen: set[str] = set()

    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
    }

    normalized_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        normalized = _normalize_tavily_query(query)
        if normalized and normalized not in seen_queries:
            seen_queries.add(normalized)
            normalized_queries.append(normalized)

    async with aiohttp.ClientSession() as session:
        provider_query_limit = max(1, int(os.getenv("DISCOVERY_PROVIDER_QUERY_LIMIT", "3") or "3"))
        for query in normalized_queries[:provider_query_limit]:
            payload = {
                "query": query,
                "type": search_type,
                "numResults": max_results,
                "includeDomains": KNOWN_SITES,
                "contents": {
                    "highlights": {
                        "maxCharacters": 1200,
                        "query": "fiyat TL metrekare oda ilan",
                    },
                    "maxAgeHours": 2160,
                },
            }
            try:
                async with session.post(
                    EXA_SEARCH_API_URL,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Exa returned {resp.status} for query '{query}'")
                        continue

                    data = await resp.json()
                    for result in data.get("results", []):
                        url = result.get("url", "") or result.get("id", "")
                        if url and url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            title = result.get("title", "") or ""
                            highlights = result.get("highlights") or []
                            highlight_text = ""
                            if isinstance(highlights, list) and highlights:
                                highlight_text = " ".join(str(x) for x in highlights[:2] if x)
                            snippet = (
                                result.get("summary")
                                or highlight_text
                                or ""
                            )
                            if len(snippet) > 900:
                                snippet = snippet[:900]
                            candidates.append(_build_candidate(url=url, source="exa", title=title, snippet=snippet))
            except Exception as e:
                logger.warning(f"Exa failed for '{query}': {e}")

    return candidates


async def search_exa_pages(queries: list[str], max_results: int = 8) -> list[str]:
    return [item["url"] for item in await search_exa_candidates(queries, max_results=max_results)]


async def search_google_cse_candidates(queries: list[str], max_results: int = 10) -> list[dict]:
    """
    Search with Google's Custom Search JSON API.
    Requires GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX.
    """
    api_key = os.getenv("GOOGLE_CSE_API_KEY", "").strip()
    cx = os.getenv("GOOGLE_CSE_CX", "").strip()
    if not api_key or not cx:
        return []

    candidates: list[dict] = []
    seen: set[str] = set()

    normalized_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        normalized = _normalize_tavily_query(query)
        if normalized and normalized not in seen_queries:
            seen_queries.add(normalized)
            normalized_queries.append(normalized)

    async with aiohttp.ClientSession() as session:
        for query in normalized_queries[:3]:
            params = {
                "key": api_key,
                "cx": cx,
                "q": query,
                "num": min(max_results, 10),
                "gl": "tr",
                "hl": "tr",
            }
            try:
                async with session.get(
                    GOOGLE_CSE_API_URL,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Google CSE returned {resp.status} for query '{query}'")
                        continue

                    data = await resp.json()
                    for result in data.get("items", []):
                        url = result.get("link", "")
                        if url and url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            title = result.get("title", "") or ""
                            snippet = result.get("snippet", "") or ""
                            candidates.append(_build_candidate(url=url, source="google_cse", title=title, snippet=snippet))
            except Exception as e:
                logger.warning(f"Google CSE failed for '{query}': {e}")

    return candidates


async def search_google_cse_pages(queries: list[str], max_results: int = 10) -> list[str]:
    return [item["url"] for item in await search_google_cse_candidates(queries, max_results=max_results)]


async def search_vertex_candidates(queries: list[str], max_results: int = 8) -> list[dict]:
    """
    Search via Vertex AI Search (Discovery Engine) searchLite endpoint.
    Requires VERTEX_PROJECT_ID and VERTEX_APP_ID.
    Uses VERTEX_API_KEY if set, otherwise falls back to GOOGLE_CSE_API_KEY.
    """
    project_id = os.getenv("VERTEX_PROJECT_ID", "").strip()
    app_id = os.getenv("VERTEX_APP_ID", "").strip()
    location = os.getenv("VERTEX_LOCATION", "global").strip() or "global"
    api_key = os.getenv("VERTEX_API_KEY", "").strip() or os.getenv("GOOGLE_CSE_API_KEY", "").strip()

    if not project_id or not app_id or not api_key:
        return []

    endpoint = (
        f"{VERTEX_SEARCH_API_BASE}/projects/{project_id}/locations/{location}/collections/default_collection/"
        f"engines/{app_id}/servingConfigs/default_search:searchLite"
    )

    candidates: list[dict] = []
    seen: set[str] = set()

    normalized_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        normalized = _normalize_tavily_query(query)
        if normalized and normalized not in seen_queries:
            seen_queries.add(normalized)
            normalized_queries.append(normalized)

    async with aiohttp.ClientSession() as session:
        for query in normalized_queries[:3]:
            payload = {
                "query": query,
                "pageSize": min(max_results, 10),
            }
            try:
                async with session.post(
                    endpoint,
                    params={"key": api_key},
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Vertex Search returned {resp.status} for query '{query}'")
                        continue

                    data = await resp.json()
                    for result in data.get("results", []):
                        document = result.get("document", {})
                        derived = document.get("derivedStructData", {}) if isinstance(document, dict) else {}
                        struct_data = document.get("structData", {}) if isinstance(document, dict) else {}

                        url = (
                            derived.get("link")
                            or derived.get("uri")
                            or struct_data.get("link")
                            or struct_data.get("uri")
                            or ""
                        )
                        if url and url not in seen and is_real_estate_domain(url):
                            seen.add(url)
                            title = (
                                derived.get("title")
                                or struct_data.get("title")
                                or ""
                            )
                            snippet = (
                                derived.get("snippet")
                                or struct_data.get("snippet")
                                or ""
                            )
                            candidates.append(_build_candidate(url=url, source="vertex_ai_search", title=title, snippet=snippet))
            except Exception as e:
                logger.warning(f"Vertex Search failed for '{query}': {e}")

    return candidates


async def search_vertex_pages(queries: list[str], max_results: int = 8) -> list[str]:
    return [item["url"] for item in await search_vertex_candidates(queries, max_results=max_results)]


async def discover_listing_urls(
    lat: str,
    lng: str,
    listing_type: str = "all",
    property_type: str = "all",
    num_pages: int = 1,
    fast_mode: bool = False,
    strict_detail_only: bool | None = None,
    strict_district_header: bool | None = None,
) -> dict:
    """
    Main entry:
    1. Reverse geocode (lat,lng) -> neighborhood/district/road level
    2. Build hyper-local Google queries
    3. Scrape Google and return deduplicated real estate URLs plus search context
    """
    geo = await reverse_geocode(lat, lng)
    logger.info(
        "Geocoded %s,%s -> neighborhood=%s district=%s city=%s road=%s",
        lat,
        lng,
        geo["neighborhood"],
        geo["district"],
        geo["city"],
        geo["road"],
    )

    queries = build_radius_queries(geo, listing_type=listing_type, property_type=property_type)
    direct_urls = build_direct_site_candidates(geo, listing_type=listing_type, property_type=property_type)
    if not queries:
        fallback = f'"{lat},{lng}" emlak ilan'
        queries = [fallback]
        logger.warning(f"No geocode data, using fallback query: {fallback}")

    if fast_mode:
        fast_query_limit = max(1, int(os.getenv("DISCOVERY_FAST_QUERY_LIMIT", "4") or "4"))
        queries = queries[:fast_query_limit]

    logger.info(f"Built {len(queries)} radius-specific queries")
    logger.info(f"Built {len(direct_urls)} direct site candidate URLs")
    urls = list(direct_urls)
    candidate_map: dict[str, dict] = {}
    for url in direct_urls:
        _upsert_candidate(candidate_map, _build_candidate(url=url, source="direct_site"))

    google_throttled = False
    search_strategy = "direct_only"

    if fast_mode:
        provider_timeout = float(os.getenv("DISCOVERY_PROVIDER_TIMEOUT_SECONDS", "8") or "8")
        provider_query_limit = max(1, int(os.getenv("DISCOVERY_PROVIDER_QUERY_LIMIT", "3") or "3"))
        provider_max_results = num_pages * 10
        provider_queries = queries[:provider_query_limit]
        provider_jobs = [
            ("tavily", search_tavily_candidates(provider_queries, max_results=provider_max_results)),
            ("exa", search_exa_candidates(provider_queries, max_results=provider_max_results)),
            ("serper", search_serper_candidates(provider_queries, max_results=provider_max_results)),
        ]
        provider_results = await asyncio.gather(
            *[
                _run_provider_with_timeout(name, coroutine, timeout_seconds=provider_timeout)
                for name, coroutine in provider_jobs
            ]
        )
        for source_name, rows in provider_results:
            if rows:
                search_strategy = _add_search_strategy(search_strategy, source_name)
            for candidate in rows:
                url = candidate.get("url")
                if not url:
                    continue
                if url not in urls:
                    urls.append(url)
                _upsert_candidate(candidate_map, candidate)
    else:
        if len(urls) < 4:
            tavily_candidates = await search_tavily_candidates(queries)
            if tavily_candidates:
                search_strategy = _add_search_strategy(search_strategy, "tavily")
                for candidate in tavily_candidates:
                    url = candidate["url"]
                    if url not in urls:
                        urls.append(url)
                    _upsert_candidate(candidate_map, candidate)

        if len(urls) < 4:
            exa_candidates = await search_exa_candidates(queries)
            if exa_candidates:
                search_strategy = _add_search_strategy(search_strategy, "exa")
                for candidate in exa_candidates:
                    url = candidate["url"]
                    if url not in urls:
                        urls.append(url)
                    _upsert_candidate(candidate_map, candidate)

        if len(urls) < 4:
            serper_candidates = await search_serper_candidates(queries)
            if serper_candidates:
                search_strategy = _add_search_strategy(search_strategy, "serper")
                for candidate in serper_candidates:
                    url = candidate["url"]
                    if url not in urls:
                        urls.append(url)
                    _upsert_candidate(candidate_map, candidate)

        if not urls:
            search_strategy = "search_engine_fallback"
            google_urls, google_throttled = await search_google_pages(queries, num_pages=num_pages)
            for url in google_urls:
                if url not in urls:
                    urls.append(url)
                _upsert_candidate(candidate_map, _build_candidate(url=url, source="google"))
            if google_throttled or not urls:
                fallback_urls = await search_duckduckgo_pages(queries)
                for url in fallback_urls:
                    if url not in urls:
                        urls.append(url)
                    _upsert_candidate(candidate_map, _build_candidate(url=url, source="duckduckgo"))
        elif len(urls) < 4:
            search_strategy = "direct_plus_search_fallback"
            google_urls, google_throttled = await search_google_pages(queries, num_pages=num_pages)
            for url in google_urls:
                if url not in urls:
                    urls.append(url)
                _upsert_candidate(candidate_map, _build_candidate(url=url, source="google"))
            if google_throttled or len(urls) < 4:
                fallback_urls = await search_duckduckgo_pages(queries)
                for url in fallback_urls:
                    if url not in urls:
                        urls.append(url)
                    _upsert_candidate(candidate_map, _build_candidate(url=url, source="duckduckgo"))

    jina_reader_enriched_count = 0

    ranked_urls, ranked_candidates, locality_info = _rank_and_filter_candidates(
        urls,
        candidate_map,
        geo,
        strict_detail_only=strict_detail_only,
        strict_district_header=strict_district_header,
    )
    return {
        "geo": geo,
        "queries": queries,
        "urls": ranked_urls,
        "candidates": ranked_candidates,
        "direct_urls": direct_urls,
        "listing_type": listing_type,
        "property_type": property_type,
        "google_throttled": google_throttled,
        "discovery_sources": DIRECT_SITE_SOURCES + ["tavily", "exa", "serper"],
        "search_strategy": search_strategy,
        "fast_mode": fast_mode,
        "locality": locality_info,
        "jina_reader_enriched_count": jina_reader_enriched_count,
    }


async def scrape_google_results(
    lat: str,
    lng: str,
    listing_type: str = "all",
    property_type: str = "all",
    num_pages: int = 1,
) -> list[str]:
    """Backward-compatible wrapper returning only URLs."""
    discovery = await discover_listing_urls(
        lat=lat,
        lng=lng,
        listing_type=listing_type,
        property_type=property_type,
        num_pages=num_pages,
    )
    return discovery["urls"]

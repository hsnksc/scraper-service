from abc import ABC, abstractmethod
from playwright.async_api import Page
from typing import Optional
import logging
import re

logger = logging.getLogger(__name__)


async def _safe_text(page: Page, selector: str) -> Optional[str]:
    """Safely extract text from a CSS selector."""
    try:
        el = page.locator(selector).first
        if await el.count() > 0:
            text = await el.inner_text(timeout=3000)
            return text.strip() if text else None
    except Exception:
        pass
    return None


async def _safe_all_texts(page: Page, selector: str) -> list[str]:
    """Safely extract all texts from matching elements."""
    try:
        els = page.locator(selector)
        count = await els.count()
        if count == 0:
            return []
        texts = []
        for i in range(count):
            text = await els.nth(i).inner_text(timeout=3000)
            text = text.strip()
            if text:
                texts.append(text)
        return texts
    except Exception:
        return []


async def _safe_attr(page: Page, selector: str, attr: str) -> Optional[str]:
    """Safely extract an attribute from a CSS selector."""
    try:
        el = page.locator(selector).first
        if await el.count() > 0:
            return await el.get_attribute(attr, timeout=3000)
    except Exception:
        pass
    return None


async def _safe_all_attrs(page: Page, selector: str, attr: str) -> list[str]:
    """Safely extract attributes from matching elements."""
    try:
        els = page.locator(selector)
        count = await els.count()
        if count == 0:
            return []
        result = []
        for i in range(count):
            val = await els.nth(i).get_attribute(attr)
            if val:
                result.append(val)
        return result
    except Exception:
        return []


def extract_price(text: str) -> Optional[float]:
    """Extract numeric price from Turkish formatted text."""
    if not text:
        return None
    # Remove currency symbols and words
    cleaned = re.sub(r'[₺$€£]|TL\s*|USD\s*|EUR\s*|TRY\s*|Bin\s*', '', text, flags=re.IGNORECASE).strip()
    # Turkish format: 1.250.000 or 1,250,000
    cleaned = cleaned.replace('.', '').replace(',', '')
    try:
        val = float(cleaned)
        return val if val > 0 else None
    except ValueError:
        return None


def extract_first_price(text: str) -> Optional[float]:
    if not text:
        return None
    match = re.search(r'(\d[\d\.\,]+)\s*TL', text, re.IGNORECASE)
    if match:
        return extract_price(match.group(0))
    return None


def extract_area(text: str) -> Optional[float]:
    """Extract area in m² from text."""
    if not text:
        return None
    match = re.search(r'(\d+[.,]?\d*)\s*(?:m2|m²|metrekare)\b', text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(',', '.'))
        except ValueError:
            pass
    return None


def extract_rooms(text: str) -> Optional[str]:
    """Extract room count like 3+1 from text."""
    if not text:
        return None
    match = re.search(r'\b(\d{1,2}[+\-]\d{1,2})\b', text)
    if match:
        return match.group(1)
    return None


def normalize_listing_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    normalized = value.strip().lower()
    if any(token in normalized for token in ["kira", "kiralık", "kiralik"]):
        return "rent"
    if any(token in normalized for token in ["sat", "satılık", "satilik", "sale"]):
        return "sale"
    if normalized in {"rent", "sale"}:
        return normalized
    return None


def normalize_property_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    normalized = value.strip().lower()

    commercial_tokens = [
        "ticari", "iş yeri", "is yeri", "ofis", "büro", "buro", "dükkan",
        "dukkan", "mağaza", "magaza", "depo", "plaza",
    ]
    land_tokens = ["arsa", "tarla", "bağ", "bag", "bahçe", "bahce", "zeytinlik"]
    residential_tokens = [
        "konut", "daire", "ev", "villa", "rezidans", "yazlık", "yazlik",
        "apartman", "müstakil", "mustakil", "çatı dubleks", "cati dubleks",
    ]

    if any(token in normalized for token in commercial_tokens):
        return "commercial"
    if any(token in normalized for token in land_tokens):
        return "land"
    if any(token in normalized for token in residential_tokens):
        return "residential"
    if normalized in {"residential", "commercial", "land"}:
        return normalized
    return None


def extract_labeled_values(body_text: str, field_names: set[str]) -> dict[str, str]:
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    result: dict[str, str] = {}
    for i, line in enumerate(lines[:-1]):
        if line in field_names and i + 1 < len(lines):
            result[line] = lines[i + 1]
    return result


def extract_location_line(body_text: str) -> str:
    if not body_text:
        return ""

    patterns = [
        r"([A-ZÇĞİÖŞÜa-zçğıöşü]+)\s*-\s*([A-ZÇĞİÖŞÜa-zçğıöşü]+)\s*-\s*([A-ZÇĞİÖŞÜa-zçğıöşü\s]+Mahallesi)",
        r"([A-ZÇĞİÖŞÜa-zçğıöşü]+),\s*([A-ZÇĞİÖŞÜa-zçğıöşü]+),\s*([A-ZÇĞİÖŞÜa-zçğıöşü\s]+Mahallesi)",
    ]

    for pattern in patterns:
        match = re.search(pattern, body_text)
        if match:
            separator = " - " if " - " in pattern else ", "
            return separator.join(part.strip() for part in match.groups())
    return ""


def parse_location_parts(loc: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not loc:
        return None, None, None

    if " - " in loc:
        parts = [part.strip() for part in loc.split(" - ") if part.strip()]
        if len(parts) >= 3:
            city = parts[0]
            district = parts[1]
            address = f"{parts[2]}, {district}, {city}"
            return city, district, address

    parts = [part.strip() for part in loc.split(",") if part.strip()]
    if len(parts) >= 3:
        city = parts[-3] if len(parts) > 3 else parts[0]
        district = parts[-2]
        address = ", ".join(parts[-3:]) if len(parts) >= 3 else loc
        return city, district, address
    if len(parts) >= 2:
        return parts[-1], parts[-2], loc
    return None, None, loc or None


def infer_agent_type(text: str) -> Optional[str]:
    if not text:
        return None

    normalized = text.lower()
    if any(token in normalized for token in ["sahibinden", "mal sahibi", "mülk sahibi", "mulk sahibi"]):
        return "owner"
    if any(token in normalized for token in ["emlak", "danışman", "danisman", "ofis", "gayrimenkul"]):
        return "agent"
    return None


class BaseScraper(ABC):
    site_name: str = ""

    @abstractmethod
    async def is_listing(self, page: Page) -> bool:
        """Check if the current page is an actual listing."""

    @abstractmethod
    async def extract(self, page: Page) -> Optional[dict]:
        """Extract listing data. Return dict matching Listing model fields."""

    async def scrape(self, page: Page) -> Optional[dict]:
        """Main scrape method: check if listing, then extract."""
        if not await self.is_listing(page):
            return None
        data = await self.extract(page)
        if data:
            data["source_site"] = self.site_name
        return data

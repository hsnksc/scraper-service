from playwright.async_api import Page
from typing import Optional
import re
from scraper.scrapers.base import (
    BaseScraper,
    _safe_text,
    _safe_all_attrs,
    extract_price,
    extract_area,
    extract_rooms,
    normalize_listing_type,
    normalize_property_type,
)


class EmlakJetScraper(BaseScraper):
    site_name = "emlakjet"

    async def is_listing(self, page: Page) -> bool:
        return "/ilan/" in page.url or "/ilan-" in page.url or "-ilan-" in page.url

    async def extract(self, page: Page) -> Optional[dict]:
        try:
            await page.wait_for_selector("h1", timeout=5000)
        except Exception:
            pass

        title = await _safe_text(page, "h1") or await page.title() or ""
        description = await _safe_text(page, ".detail-description, .description, [class*='aciklama']") or ""
        body_text = await self._get_body_text(page)

        # Price
        price_raw = await _safe_text(page, ".info-price, .price, [class*='fiyat']") or ""
        if not price_raw:
            price_raw = await _safe_text(page, "[class*='price-value']") or ""
        price = extract_price(price_raw) or self._extract_price_from_text(body_text) or extract_price(title)

        # Details
        details = await self._extract_details(page, body_text=body_text, title=title)

        # Images
        images = await _safe_all_attrs(page, ".detail-gallery img, .gallery img, [class*='gallery'] img, [class*='slider'] img", "src")
        images = [img for img in images if img and len(img) > 30 and "logo" not in img.lower()]

        # Location
        loc = await _safe_text(page, ".location, .address, [class*='adres'], [class*='konum']") or ""
        if not loc:
            loc = self._extract_location_text(body_text)
        if loc:
            city, district, address = self._parse_location(loc)
            details["city"] = details.get("city") or city
            details["district"] = details.get("district") or district
            details["address"] = details.get("address") or address

        return {
            "title": title,
            "price": price,
            "currency": "TRY",
            "area_sqm": details.get("area_sqm"),
            "rooms": details.get("rooms"),
            "listing_type": normalize_listing_type(details.get("listing_type")),
            "property_type": normalize_property_type(details.get("property_type")),
            "district": details.get("district"),
            "city": details.get("city"),
            "address": details.get("address"),
            "description": description,
            "images": images[:10],
            "date_posted": details.get("date_posted"),
            "agent_type": details.get("agent_type"),
        }

    async def _extract_details(self, page: Page, body_text: str = "", title: str = "") -> dict:
        result = {}
        try:
            # EmlakJet uses detail info cards
            details = page.locator("[class*='detail'] [class*='info'], .info-table li, [class*='feature']")
            count = await details.count()
            for i in range(count):
                text = await details.nth(i).inner_text(timeout=2000)
                text = text.strip()
                if not text:
                    continue
                if "m²" in text or "m2" in text or "metrekare" in text.lower():
                    area = extract_area(text)
                    if area:
                        result["area_sqm"] = area
                elif "oda" in text.lower():
                    rooms = extract_rooms(text)
                    if rooms:
                        result["rooms"] = rooms
                elif "sat" in text.lower():
                    result["listing_type"] = "sale"
                elif "kira" in text.lower():
                    result["listing_type"] = "rent"
                elif "arsa" in text.lower() or "tarla" in text.lower():
                    result["property_type"] = "land"
                elif "ticari" in text.lower() or "is yeri" in text.lower() or "ofis" in text.lower():
                    result["property_type"] = "commercial"
        except Exception:
            pass

        if body_text:
            field_map = self._extract_field_map(body_text)

            result["area_sqm"] = result.get("area_sqm") or extract_area(field_map.get("Metrekare", ""))
            result["rooms"] = result.get("rooms") or extract_rooms(title) or extract_rooms(body_text)
            result["listing_type"] = result.get("listing_type") or field_map.get("Kategorisi")
            result["property_type"] = result.get("property_type") or field_map.get("Türü") or field_map.get("İmar Durumu")
            result["date_posted"] = field_map.get("İlan Güncelleme Tarihi")

            city, district, address = self._parse_location(self._extract_location_text(body_text))
            result["city"] = result.get("city") or city
            result["district"] = result.get("district") or district
            result["address"] = result.get("address") or address

            if not result.get("property_type") and title:
                result["property_type"] = title
            if not result.get("agent_type") and "emlak" in body_text.lower():
                result["agent_type"] = "agent"

        # Listing type from URL
        url = page.url.lower()
        if "kiralik" in url:
            result["listing_type"] = "rent"
        elif "satilik" in url:
            result["listing_type"] = "sale"

        return result

    async def _get_body_text(self, page: Page) -> str:
        try:
            return await page.locator("body").inner_text(timeout=10000)
        except Exception:
            return ""

    def _extract_price_from_text(self, text: str) -> Optional[float]:
        if not text:
            return None
        match = re.search(r"(\d[\d\.\,]+)\s*TL", text, re.IGNORECASE)
        return extract_price(match.group(0)) if match else None

    def _extract_field_map(self, body_text: str) -> dict[str, str]:
        lines = [line.strip() for line in body_text.splitlines() if line.strip()]
        field_names = {
            "İlan Numarası",
            "İlan Güncelleme Tarihi",
            "Türü",
            "Kategorisi",
            "İmar Durumu",
            "Metrekare Birim Fiyatı",
            "Metrekare",
            "Tapu Durumu",
            "Krediye Uygunluk",
            "Takas",
            "Kat Karşılığı",
            "Yatırıma Uygunluk",
            "Fiyat Durumu",
        }

        result: dict[str, str] = {}
        for i, line in enumerate(lines[:-1]):
            if line in field_names and i + 1 < len(lines):
                result[line] = lines[i + 1]
        return result

    def _extract_location_text(self, body_text: str) -> str:
        if not body_text:
            return ""

        match = re.search(r"([A-ZÇĞİÖŞÜa-zçğıöşü]+)\s*-\s*([A-ZÇĞİÖŞÜa-zçğıöşü]+)\s*-\s*([A-ZÇĞİÖŞÜa-zçğıöşü\s]+Mahallesi)", body_text)
        if match:
            return " - ".join(part.strip() for part in match.groups())
        return ""

    def _parse_location(self, loc: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
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
        if len(parts) >= 2:
            return parts[-1], parts[-2], loc
        return None, None, loc or None

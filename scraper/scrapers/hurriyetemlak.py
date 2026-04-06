from playwright.async_api import Page
from typing import Optional
from scraper.scrapers.base import (
    BaseScraper,
    _safe_text,
    _safe_all_attrs,
    extract_price,
    extract_first_price,
    extract_area,
    extract_rooms,
    extract_labeled_values,
    extract_location_line,
    parse_location_parts,
    infer_agent_type,
    normalize_listing_type,
    normalize_property_type,
)


class HurriyetEmlakScraper(BaseScraper):
    site_name = "hurriyetemlak"

    async def is_listing(self, page: Page) -> bool:
        return "/ilan/" in page.url or "/emlak/" in page.url

    async def extract(self, page: Page) -> Optional[dict]:
        try:
            await page.wait_for_selector("h1", timeout=5000)
        except Exception:
            pass

        title = await _safe_text(page, "h1") or await page.title() or ""
        description = await _safe_text(page, ".detail-description, .detailContent, [class*='aciklama']") or ""
        body_text = await self._get_body_text(page)

        # Price
        price_raw = await _safe_text(page, ".price, [class*='fiyat'], [class*='price']") or ""
        if not price_raw:
            price_raw = await _safe_text(page, "h2")  # sometimes price is in h2
        price = extract_price(price_raw) or extract_first_price(body_text) or extract_price(title)

        # Details
        details = await self._extract_details(page, body_text=body_text, title=title)

        # Images
        images = await _safe_all_attrs(page, ".gallery img, .detail-gallery img, [class*='gallery'] img, [data-gallery] img", "src")
        images = [img for img in images if img and len(img) > 30 and "logo" not in img.lower()]

        # Location
        loc = await _safe_text(page, ".address, .location, [class*='adres'], [class*='konum']") or ""
        if not loc:
            loc = extract_location_line(body_text)
        if loc:
            city, district, address = parse_location_parts(loc)
            details["district"] = details.get("district") or district
            details["city"] = details.get("city") or city
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
            detail_items = page.locator("[class*='detail'] li, .detail-table li, [class*='feature'] td, [class*='info'] li")
            count = await detail_items.count()
            for i in range(count):
                text = await detail_items.nth(i).inner_text(timeout=2000)
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
                elif "ticari" in text.lower() or "is yeri" in text.lower():
                    result["property_type"] = "commercial"
                elif "sahibinden" in text.lower():
                    result["agent_type"] = "owner"
        except Exception:
            pass

        if body_text:
            fields = extract_labeled_values(
                body_text,
                {
                    "İlan Tarihi",
                    "Güncelleme Tarihi",
                    "İlan Güncelleme Tarihi",
                    "Brüt Metrekare",
                    "Net Metrekare",
                    "Oda Sayısı",
                    "Kimden",
                    "Konut Tipi",
                    "Kategori",
                    "Türü",
                    "İmar Durumu",
                },
            )
            result["area_sqm"] = result.get("area_sqm") or extract_area(fields.get("Brüt Metrekare", "")) or extract_area(fields.get("Net Metrekare", ""))
            result["rooms"] = result.get("rooms") or extract_rooms(fields.get("Oda Sayısı", "")) or extract_rooms(title)
            result["listing_type"] = result.get("listing_type") or fields.get("Kategori")
            result["property_type"] = result.get("property_type") or fields.get("Konut Tipi") or fields.get("Türü") or fields.get("İmar Durumu") or title
            result["date_posted"] = fields.get("İlan Güncelleme Tarihi") or fields.get("Güncelleme Tarihi") or fields.get("İlan Tarihi")
            result["agent_type"] = result.get("agent_type") or infer_agent_type(fields.get("Kimden", "")) or infer_agent_type(body_text)

            city, district, address = parse_location_parts(extract_location_line(body_text))
            result["city"] = result.get("city") or city
            result["district"] = result.get("district") or district
            result["address"] = result.get("address") or address

        # URL-based type detection
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

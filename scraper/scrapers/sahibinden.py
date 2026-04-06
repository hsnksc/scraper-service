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


class SahibindenScraper(BaseScraper):
    site_name = "sahibinden"

    async def is_listing(self, page: Page) -> bool:
        return "/ilan/" in page.url and "-emlak" in page.url

    async def extract(self, page: Page) -> Optional[dict]:
        # Wait for content to load
        try:
            await page.wait_for_selector(".classifiedDetail", timeout=5000)
        except Exception:
            pass

        title = await _safe_text(page, ".classifiedDetailTitle h1") or await _safe_text(page, "h1") or await page.title()
        description = await _safe_text(page, ".js-classified-description") or await _safe_text(page, ".classifiedDescription")
        body_text = await self._get_body_text(page)

        # Price from info box
        price_text = await _safe_text(page, ".classifiedInfo .infoBox .classifiedInfoRow .value") or ""
        price = extract_price(price_text) or extract_first_price(body_text) or extract_price(title)

        # Details table key-value pairs
        details = await self._extract_details(page, body_text=body_text, title=title)

        images = await _safe_all_attrs(page, ".classifiedGallery img, .gallerySliderView img, .js-galleryImg img", "src")
        # Filter out tiny placeholder images
        images = [img for img in images if img and len(img) > 30 and "logo" not in img.lower()]

        return {
            "title": title or "",
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
            # Sahibinden uses a detail table with dt/dd pairs
            rows = page.locator(".classifiedDetailTableContent tr")
            count = await rows.count()
            for i in range(count):
                key_el = rows.nth(i).locator("td:first-child")
                val_el = rows.nth(i).locator("td:last-child")
                if await key_el.count() == 0:
                    continue
                key = (await key_el.inner_text(timeout=2000)).strip().lower()
                val = (await val_el.inner_text(timeout=2000)).strip()

                if "odasayisi" in key.replace(" ", "") or "oda say" in key:
                    result["rooms"] = val
                elif "net metre" in key or "brut metre" in key or "m²" in key or "metrekare" in key:
                    area = extract_area(val)
                    if area:
                        result["area_sqm"] = area
                elif "emlak tipi" in key:
                    result["property_type"] = val
                elif "kimden" in key:
                    result["agent_type"] = infer_agent_type(val)
                elif "ilan tarihi" in key or "guncelleme tarihi" in key or "güncelleme tarihi" in key:
                    result["date_posted"] = val
                elif "takas" in key:
                    pass  # swap info
                elif "sahibinden" in key.lower():
                    result["agent_type"] = "owner" if "evet" in val.lower() else None
        except Exception:
            pass

        if body_text:
            fields = extract_labeled_values(
                body_text,
                {
                    "İlan Tarihi",
                    "Güncelleme Tarihi",
                    "İlan No",
                    "m² (Brüt)",
                    "m² (Net)",
                    "Oda Sayısı",
                    "Kimden",
                    "Emlak Tipi",
                    "İmar Durumu",
                },
            )
            result["area_sqm"] = result.get("area_sqm") or extract_area(fields.get("m² (Brüt)", "")) or extract_area(fields.get("m² (Net)", ""))
            result["rooms"] = result.get("rooms") or extract_rooms(fields.get("Oda Sayısı", "")) or extract_rooms(title)
            result["property_type"] = result.get("property_type") or fields.get("Emlak Tipi") or fields.get("İmar Durumu") or title
            result["date_posted"] = result.get("date_posted") or fields.get("Güncelleme Tarihi") or fields.get("İlan Tarihi")
            result["agent_type"] = result.get("agent_type") or infer_agent_type(fields.get("Kimden", "")) or infer_agent_type(body_text)

        # Try to extract listing_type from URL or title
        url = page.url.lower()
        if "kiralik" in url or "kiralık" in (await page.title()).lower():
            result["listing_type"] = "rent"
        elif "satilik" in url or "satılık" in (await page.title()).lower():
            result["listing_type"] = "sale"

        # Extract location from classifiedDetailTitle address
        loc_text = await _safe_text(page, ".classifiedInfoRow .classifiedLocation") or ""
        if not loc_text:
            loc_text = extract_location_line(body_text)
        if loc_text:
            city, district, address = parse_location_parts(loc_text)
            result["city"] = result.get("city") or city
            result["district"] = result.get("district") or district
            result["address"] = result.get("address") or address

        return result

    async def _get_body_text(self, page: Page) -> str:
        try:
            return await page.locator("body").inner_text(timeout=10000)
        except Exception:
            return ""

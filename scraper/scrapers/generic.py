from playwright.async_api import Page
from typing import Optional
import json
import logging
from scraper.scrapers.base import BaseScraper, normalize_listing_type, normalize_property_type

logger = logging.getLogger(__name__)

# Lazy import OpenAI only when needed
_client = None


def _get_client():
    global _client
    if _client is None:
        from openai import OpenAI
        from dotenv import load_dotenv
        import os
        load_dotenv()
        _client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY", "ollama"),
            base_url=os.getenv("OPENAI_BASE_URL", "http://localhost:11434/v1"),
        )
    return _client


def _get_model():
    import os
    return os.getenv("AI_MODEL", "gpt-4o-mini")


class GenericScraper(BaseScraper):
    """Fallback scraper: sends page text to OpenAI for structured extraction."""
    site_name = "generic"

    async def is_listing(self, page: Page) -> bool:
        """Heuristic: look for real estate keywords in page text."""
        try:
            text = await page.inner_text(timeout=10000)
            keywords = ["fiyat", "tl", "metrekare", "oda", "satilik", "kiralik", "daire", "emlak", "m2"]
            count = sum(1 for kw in keywords if kw in text.lower())
            return count >= 3
        except Exception:
            return False

    async def extract(self, page: Page) -> Optional[dict]:
        try:
            text = await page.inner_text(timeout=10000)
        except Exception:
            return None

        # Truncate to fit AI context
        text = text[:8000]

        prompt = (
            f"Extract real estate listing information from the following Turkish webpage text. "
            f"If any field cannot be determined, use null.\n\n"
            f'{{\n'
            f'  "title": "page title or headline",\n'
            f'  "price": number or null,\n'
            f'  "area_sqm": number or null,\n'
            f'  "rooms": "3+1" or similar, or null,\n'
            f'  "listing_type": "sale" or "rent" or null,\n'
            f'  "property_type": "residential" or "commercial" or "land" or null,\n'
            f'  "district": "area/district" or null,\n'
            f'  "city": "city name" or null,\n'
            f'  "description": "brief description in Turkish" or null,\n'
            f'  "agent_type": "owner" or "agent" or null\n'
            f'}}\n\n'
            f"Return ONLY the JSON object. Do not include markdown code fences.\n\n"
            f"Page text:\n{text}"
        )

        try:
            client = _get_client()
            model = _get_model()
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
            )
            raw = response.choices[0].message.content.strip()
            # Clean markdown fences if present
            if raw.startswith("```"):
                raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            if raw.startswith("json"):
                raw = raw[4:].strip()
            data = json.loads(raw)
            data["listing_type"] = normalize_listing_type(data.get("listing_type"))
            data["property_type"] = normalize_property_type(data.get("property_type"))
            data["source_url"] = page.url
            return data
        except Exception as e:
            logger.warning(f"Generic AI extraction failed for {page.url}: {e}")
            return None

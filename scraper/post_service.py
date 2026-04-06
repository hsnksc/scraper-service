import asyncio
import logging
import os
import aiohttp
from typing import Optional
from scraper.models import Listing

logger = logging.getLogger(__name__)


class PostService:
    """POST scraped listings to external API with batching and retry."""

    def __init__(self):
        self.base_url = os.getenv("EXTERNAL_API_URL", "")
        self.api_key = os.getenv("EXTERNAL_API_KEY", "")
        self.auth_header = os.getenv("EXTERNAL_API_AUTH_HEADER", "Authorization")
        self.batch_size = int(os.getenv("EXTERNAL_API_BATCH_SIZE", "50"))
        self.headers = {"Content-Type": "application/json"}
        if self.api_key:
            self.headers[self.auth_header] = f"Bearer {self.api_key}"

    async def post_listings(self, listings: list[dict], job_id: str = "") -> dict:
        """POST all listings in batches. Returns stats."""
        if not self.base_url:
            logger.warning("No EXTERNAL_API_URL configured, skipping POST")
            return {"posted": 0, "failed": 0, "skipped": len(listings)}

        total_posted = 0
        total_failed = 0

        for i in range(0, len(listings), self.batch_size):
            batch = listings[i : i + self.batch_size]
            payload = {
                "listings": batch,
                "metadata": {
                    "job_id": job_id,
                    "batch": i // self.batch_size + 1,
                    "count": len(batch),
                },
            }

            success = await self._retry_post(payload)
            if success:
                total_posted += len(batch)
            else:
                total_failed += len(batch)

        return {
            "posted": total_posted,
            "failed": total_failed,
            "total": len(listings),
        }

    async def _retry_post(self, payload: dict, max_retries: int = 3) -> bool:
        """POST with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        self.base_url,
                        json=payload,
                        headers=self.headers,
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        if resp.status in (200, 201, 202):
                            return True
                        logger.warning(f"API returned {resp.status}: {await resp.text()}")
            except Exception as e:
                logger.warning(f"POST attempt {attempt+1} failed: {e}")

            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** (attempt + 1))

        return False

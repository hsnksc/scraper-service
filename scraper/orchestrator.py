import asyncio
import logging
import os
from collections import deque
from datetime import datetime
import re

from scraper.models import Listing, ScrapeJob
from scraper.google_search import discover_listing_urls
from scraper.playwright_manager import PlaywrightManager
from scraper.site_detector import detect_site, is_listing_url
from scraper.post_service import PostService
from scraper.scrapers.base import normalize_listing_type, normalize_property_type
from scraper.scrapers.sahibinden import SahibindenScraper
from scraper.scrapers.hepsiemlak import HepsieEmlakScraper
from scraper.scrapers.emlakjet import EmlakJetScraper
from scraper.scrapers.hurriyetemlak import HurriyetEmlakScraper
from scraper.scrapers.generic import GenericScraper

logger = logging.getLogger(__name__)

SCRAPERS = {
    "sahibinden": SahibindenScraper(),
    "hepsiemlak": HepsieEmlakScraper(),
    "emlakjet": EmlakJetScraper(),
    "hurriyetemlak": HurriyetEmlakScraper(),
}
GENERIC = GenericScraper()


class ScrapeOrchestrator:
    def __init__(self):
        self.jobs: dict[str, ScrapeJob] = {}
        self.post_service = PostService()
        self.max_listing_urls = max(1, int(os.getenv("MAX_LISTING_URLS", "10")))
        self.max_links_per_seed = max(1, int(os.getenv("MAX_LINKS_PER_SEED", "5")))
        self.max_output_listings = max(1, int(os.getenv("MAX_OUTPUT_LISTINGS", "8")))

    async def run_scrape(
        self,
        job_id: str,
        lat: str,
        lng: str,
        listing_type: str = "all",
        property_type: str = "all",
        num_pages: int = 1,
        progress_queue: asyncio.Queue = None,
    ) -> ScrapeJob:
        """Execute full scrape pipeline for a 1km radius around (lat, lng)."""
        job = self.jobs[job_id]
        job.status = "running"
        job.started_at = datetime.now()
        manager = None

        try:
            manager = PlaywrightManager()
            await manager.start()

            # Step 1: Google search (1km radius)
            job.progress = f"1km yarıçapta Google araması yapılıyor ({lat},{lng})..."
            await self._emit_progress(job, progress_queue)
            logger.info(f"Starting Google search for radius around {lat},{lng}")

            discovery = await discover_listing_urls(
                lat=lat,
                lng=lng,
                listing_type=listing_type,
                property_type=property_type,
                num_pages=num_pages,
            )
            urls = discovery["urls"]
            strict_detail_only = os.getenv("STRICT_DETAIL_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"}
            strict_district_header = os.getenv("STRICT_DISTRICT_HEADER", "true").strip().lower() in {"1", "true", "yes", "on"}
            job.search_context = {
                "geo": discovery["geo"],
                "queries": discovery["queries"],
                "discovery_candidates": discovery.get("candidates", [])[:20],
                "discovery_candidate_count": len(discovery.get("candidates", [])),
                "direct_urls": discovery.get("direct_urls", []),
                "query_count": len(discovery["queries"]),
                "direct_url_count": len(discovery.get("direct_urls", [])),
                "google_throttled": discovery.get("google_throttled", False),
                "discovery_sources": discovery.get("discovery_sources", []),
                "search_strategy": discovery.get("search_strategy"),
                "locality": discovery.get("locality", {}),
                "jina_reader_enriched_count": discovery.get("jina_reader_enriched_count", 0),
                "strict_detail_only": strict_detail_only,
                "strict_district_header": strict_district_header,
                "max_listing_urls": self.max_listing_urls,
                "max_links_per_seed": self.max_links_per_seed,
                "max_output_listings": self.max_output_listings,
            }
            job.progress = f"{len(urls)} aday URL bulundu, ilan linkleri ayıklanıyor..."
            await self._emit_progress(job, progress_queue)

            urls = await self._expand_candidate_urls(manager, urls, max_urls=self.max_listing_urls)
            job.total_urls_found = len(urls)
            job.search_context["expanded_listing_url_count"] = len(urls)
            job.progress = f"{len(urls)} ilan URL'si bulundu, kazıma başlıyor..."
            await self._emit_progress(job, progress_queue)

            # Step 2: Scrape listing URLs concurrently
            tasks = [asyncio.create_task(self._scrape_with_url(manager, url)) for url in urls]
            stop_early = False
            for completed_index, task in enumerate(asyncio.as_completed(tasks), start=1):
                url, listing = await task
                job.progress = f"Sayfa {completed_index}/{len(urls)} tarandı: {url}"
                job.total_urls_scraped += 1

                if listing:
                    normalized = self._normalize_listing(listing, source_url=url)
                    if self._matches_filters(normalized, listing_type=listing_type, property_type=property_type):
                        if not self._is_duplicate_listing(job.listings, normalized):
                            job.listings.append(Listing(**normalized))
                            logger.info(f"Listing extracted from {url}: {listing.get('title', '')}")
                        else:
                            logger.info(f"Duplicate listing skipped from {url}: {listing.get('title', '')}")
                else:
                    job.total_errors += 1
                    logger.debug(f"No listing data from {url}")

                await self._emit_progress(job, progress_queue)

                if len(job.listings) >= self.max_output_listings:
                    stop_early = True
                    break

            if stop_early:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                job.progress = f"Hız modu: {self.max_output_listings} ilana ulaşıldı, erken durduruldu"
                await self._emit_progress(job, progress_queue)

            balanced_listings, balance_info = self._balance_listings(
                job.listings,
                listing_type=listing_type,
                property_type=property_type,
            )
            job.listings = balanced_listings
            job.search_context["balance_applied"] = balance_info["applied"]
            job.search_context["balance_strategy"] = balance_info["strategy"]
            job.search_context["balance_bucket_counts"] = balance_info["bucket_counts"]

            # Step 3: POST to external API
            if job.listings:
                job.progress = "API'ye gönderiliyor..."
                await self._emit_progress(job, progress_queue)

                listing_dicts = [l.model_dump(mode="json") for l in job.listings]
                post_result = await self.post_service.post_listings(listing_dicts, job_id=job_id)
                logger.info(f"POST result: {post_result}")
                job.progress = f"{post_result['posted']} ilan API'ye gönderildi"
            else:
                logger.warning("No listings found to post")
                job.progress = "İlan bulunamadı"

            await self._emit_progress(job, progress_queue)

            job.status = "completed"
            job.completed_at = datetime.now()
            await self._emit_progress(job, progress_queue)

        except Exception as e:
            job.status = "failed"
            job.error_message = str(e)
            job.completed_at = datetime.now()
            job.progress = f"Hata: {e}"
            await self._emit_progress(job, progress_queue)
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
        finally:
            if manager:
                try:
                    await manager.stop()
                except Exception:
                    pass
        return job

    async def _scrape_url(self, manager: PlaywrightManager, url: str) -> dict | None:
        """Visit URL, detect site, run appropriate scraper."""
        site_key = detect_site(url)

        page = await manager.visit_page(url)
        if not page:
            return None

        try:
            if site_key and site_key in SCRAPERS:
                scraper = SCRAPERS[site_key]
                if await scraper.is_listing(page):
                    data = await scraper.extract(page)
                    if data:
                        data["source_url"] = url
                        data["source_site"] = site_key
                        return data
                return None
            else:
                # Generic AI-based fallback
                if await GENERIC.is_listing(page):
                    data = await GENERIC.extract(page)
                    if data:
                        data["source_url"] = url
                        return data
                return None
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _scrape_with_url(self, manager: PlaywrightManager, url: str) -> tuple[str, dict | None]:
        return url, await self._scrape_url(manager, url)

    async def _expand_candidate_urls(self, manager: PlaywrightManager, urls: list[str], max_urls: int = 10) -> list[str]:
        expanded: list[str] = []
        seen: set[str] = set()

        for url in urls:
            site_key = detect_site(url)
            if site_key and is_listing_url(url, site_key):
                if url not in seen:
                    seen.add(url)
                    expanded.append(url)
                continue

            if not site_key:
                continue

            listing_links = await self._extract_listing_links(manager, url, site_key)
            for link in listing_links:
                if link not in seen:
                    seen.add(link)
                    expanded.append(link)
                if len(expanded) >= max_urls:
                    return expanded

        return expanded[:max_urls]

    async def _extract_listing_links(self, manager: PlaywrightManager, url: str, site_key: str) -> list[str]:
        page = await manager.visit_page(url)
        if not page:
            return []

        try:
            hrefs = await page.eval_on_selector_all(
                "a[href]",
                "elements => elements.map(el => el.href).filter(Boolean)",
            )
        except Exception:
            hrefs = []
        finally:
            try:
                await page.close()
            except Exception:
                pass

        links: list[str] = []
        seen: set[str] = set()
        for href in hrefs:
            if not isinstance(href, str):
                continue
            href = href.strip()
            if not href or href in seen:
                continue
            if detect_site(href) != site_key:
                continue
            if is_listing_url(href, site_key):
                seen.add(href)
                links.append(href)

        logger.info(f"Expanded {url} into {len(links)} listing URLs")
        return links[: self.max_links_per_seed]

    async def _emit_progress(self, job: ScrapeJob, queue: asyncio.Queue = None):
        if queue:
            await queue.put(job.to_sse_event())

    def _normalize_listing(self, listing: dict, source_url: str) -> dict:
        normalized = dict(listing)
        normalized["source_url"] = normalized.get("source_url") or source_url
        normalized["source_site"] = normalized.get("source_site") or detect_site(source_url) or "generic"
        normalized["listing_type"] = (
            normalize_listing_type(normalized.get("listing_type"))
            or normalize_listing_type(normalized.get("title"))
            or normalize_listing_type(normalized.get("description"))
        )
        normalized["property_type"] = (
            normalize_property_type(normalized.get("property_type"))
            or normalize_property_type(normalized.get("title"))
            or normalize_property_type(normalized.get("description"))
        )
        return normalized

    def _matches_filters(self, listing: dict, listing_type: str = "all", property_type: str = "all") -> bool:
        if listing_type != "all" and listing.get("listing_type") and listing["listing_type"] != listing_type:
            return False
        if property_type != "all" and listing.get("property_type") and listing["property_type"] != property_type:
            return False
        return True

    def _is_duplicate_listing(self, existing: list[Listing], candidate: dict) -> bool:
        candidate_fp = self._listing_fingerprint(candidate)
        return any(self._listing_fingerprint(item.model_dump(mode="json")) == candidate_fp for item in existing)

    def _balance_listings(
        self,
        listings: list[Listing],
        listing_type: str = "all",
        property_type: str = "all",
    ) -> tuple[list[Listing], dict]:
        bucket_specs = self._balance_bucket_specs(listing_type=listing_type, property_type=property_type)
        if len(listings) < 2 or not bucket_specs:
            return listings, {
                "applied": False,
                "strategy": "none",
                "bucket_counts": {},
            }

        buckets = {name: deque() for name, _ in bucket_specs}
        leftovers: list[Listing] = []

        for listing in listings:
            payload = listing.model_dump(mode="json")
            assigned = False
            for bucket_name, matcher in bucket_specs:
                if matcher(payload):
                    buckets[bucket_name].append(listing)
                    assigned = True
                    break
            if not assigned:
                leftovers.append(listing)

        active_bucket_names = [name for name, items in buckets.items() if items]
        bucket_counts = {name: len(items) for name, items in buckets.items() if items}
        if len(active_bucket_names) < 2:
            return listings, {
                "applied": False,
                "strategy": "none",
                "bucket_counts": bucket_counts,
            }

        ordered: list[Listing] = []
        while any(buckets[name] for name in active_bucket_names):
            for bucket_name in active_bucket_names:
                if buckets[bucket_name]:
                    ordered.append(buckets[bucket_name].popleft())

        ordered.extend(leftovers)
        strategy = self._balance_strategy_name(listing_type=listing_type, property_type=property_type)
        return ordered, {
            "applied": True,
            "strategy": strategy,
            "bucket_counts": bucket_counts,
        }

    def _balance_bucket_specs(
        self,
        listing_type: str = "all",
        property_type: str = "all",
    ) -> list[tuple[str, callable]]:
        if listing_type == "all" and property_type == "all":
            return [
                ("sale_residential", lambda item: item.get("listing_type") == "sale" and item.get("property_type") == "residential"),
                ("rent_residential", lambda item: item.get("listing_type") == "rent" and item.get("property_type") == "residential"),
                ("sale_commercial", lambda item: item.get("listing_type") == "sale" and item.get("property_type") == "commercial"),
                ("rent_commercial", lambda item: item.get("listing_type") == "rent" and item.get("property_type") == "commercial"),
                ("sale_land", lambda item: item.get("listing_type") == "sale" and item.get("property_type") == "land"),
                ("rent_land", lambda item: item.get("listing_type") == "rent" and item.get("property_type") == "land"),
            ]

        if listing_type == "all":
            return [
                ("sale", lambda item: item.get("listing_type") == "sale" and item.get("property_type") == property_type),
                ("rent", lambda item: item.get("listing_type") == "rent" and item.get("property_type") == property_type),
            ]

        if property_type == "all":
            return [
                ("residential", lambda item: item.get("listing_type") == listing_type and item.get("property_type") == "residential"),
                ("commercial", lambda item: item.get("listing_type") == listing_type and item.get("property_type") == "commercial"),
                ("land", lambda item: item.get("listing_type") == listing_type and item.get("property_type") == "land"),
            ]

        return []

    def _balance_strategy_name(self, listing_type: str = "all", property_type: str = "all") -> str:
        if listing_type == "all" and property_type == "all":
            return "round_robin_listing_and_property_type"
        if listing_type == "all":
            return "round_robin_listing_type"
        if property_type == "all":
            return "round_robin_property_type"
        return "none"

    def _listing_fingerprint(self, listing: dict) -> str:
        def clean(value: str | None) -> str:
            if not value:
                return ""
            return re.sub(r"\s+", " ", str(value).strip().lower())

        title = clean(listing.get("title"))
        city = clean(listing.get("city"))
        district = clean(listing.get("district"))
        address = clean(listing.get("address"))
        listing_type = clean(listing.get("listing_type"))
        property_type = clean(listing.get("property_type"))

        price = listing.get("price")
        price_key = str(int(float(price))) if price not in (None, "") else ""

        area = listing.get("area_sqm")
        area_key = f"{float(area):.0f}" if area not in (None, "") else ""

        rooms = clean(listing.get("rooms"))

        parts = [title, price_key, area_key, rooms, listing_type, property_type, district, city, address]
        return "|".join(parts)

    def create_job(self, lat: str, lng: str, listing_type: str = "all", property_type: str = "all") -> ScrapeJob:
        import uuid
        job_id = str(uuid.uuid4())[:8]
        job = ScrapeJob(
            job_id=job_id,
            location=f"{lat},{lng}",
            listing_type=listing_type,
            property_type=property_type,
        )
        self.jobs[job_id] = job
        return job

    def get_job(self, job_id: str) -> ScrapeJob | None:
        return self.jobs.get(job_id)


# Global singleton
orchestrator = ScrapeOrchestrator()

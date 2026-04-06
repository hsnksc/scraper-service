import asyncio
import json
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
VENDOR = ROOT / ".vendor"

if str(VENDOR) not in sys.path:
    sys.path.insert(0, str(VENDOR))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper.orchestrator import orchestrator
from scraper.google_search import discover_listing_urls, build_pseudo_listings_from_candidates
from scraper.response_enrichment import enrich_job_payload


async def main() -> None:
    lat = os.getenv("SCRAPE_LAT", "36.856323802189124")
    lng = os.getenv("SCRAPE_LNG", "30.746730472958518")
    listing_type = os.getenv("SCRAPE_LISTING_TYPE", "all")
    property_type = os.getenv("SCRAPE_PROPERTY_TYPE", "all")
    num_pages = int(os.getenv("SCRAPE_NUM_PAGES", "1"))

    os.environ.setdefault("EXTERNAL_API_URL", "")

    t0 = time.perf_counter()
    job = orchestrator.create_job(lat=lat, lng=lng, listing_type=listing_type, property_type=property_type)
    await orchestrator.run_scrape(
        job_id=job.job_id,
        lat=lat,
        lng=lng,
        listing_type=listing_type,
        property_type=property_type,
        num_pages=num_pages,
    )
    elapsed = round(time.perf_counter() - t0, 1)

    discovery_fallback = None
    if job.status == "failed" and "Access is denied" in (job.error_message or ""):
        discovery = await discover_listing_urls(
            lat=lat,
            lng=lng,
            listing_type=listing_type,
            property_type=property_type,
            num_pages=num_pages,
        )
        discovery_fallback = {
            "search_strategy": discovery.get("search_strategy"),
            "query_count": len(discovery.get("queries", [])),
            "candidate_url_count": len(discovery.get("urls", [])),
            "candidates_preview": discovery.get("candidates", [])[:20],
        }

        pseudo_listings = build_pseudo_listings_from_candidates(
            candidates=discovery.get("candidates", []),
            geo=discovery.get("geo", {}),
            limit=60,
        )

        fallback_payload = {
            "job_id": job.job_id,
            "status": "completed",
            "progress": "fallback_discovery",
            "location": job.location,
            "listing_type": listing_type,
            "property_type": property_type,
            "search_context": {
                "geo": discovery.get("geo"),
                "queries": discovery.get("queries"),
                "query_count": len(discovery.get("queries", [])),
                "search_strategy": discovery.get("search_strategy"),
                "google_throttled": discovery.get("google_throttled"),
                "locality": discovery.get("locality", {}),
                "jina_reader_enriched_count": discovery.get("jina_reader_enriched_count", 0),
                "strict_detail_only": os.getenv("STRICT_DETAIL_ONLY", "true").strip().lower() in {"1", "true", "yes", "on"},
                "strict_district_header": os.getenv("STRICT_DISTRICT_HEADER", "true").strip().lower() in {"1", "true", "yes", "on"},
            },
            "total_urls_found": len(discovery.get("urls", [])),
            "total_urls_scraped": 0,
            "total_errors": 0,
            "listings_count": len(pseudo_listings),
            "listings": pseudo_listings,
            "error_message": None,
            "started_at": None,
            "completed_at": None,
        }
        fallback_enriched = enrich_job_payload(fallback_payload)
        discovery_fallback["categorized_candidates"] = fallback_enriched.get("categorized_listings", {})
        discovery_fallback["candidate_analysis"] = fallback_enriched.get("analysis", {})

    output = {
        "job_id": job.job_id,
        "status": job.status,
        "elapsed_seconds": elapsed,
        "progress": job.progress,
        "location": job.location,
        "total_urls_found": job.total_urls_found,
        "total_urls_scraped": job.total_urls_scraped,
        "total_errors": job.total_errors,
        "listings_count": len(job.listings),
        "search_context": job.search_context,
        "listings_preview": [item.model_dump(mode="json") for item in job.listings[:8]],
        "discovery_fallback": discovery_fallback,
    }
    if job.status == "completed":
        enrich_payload = {
            "job_id": job.job_id,
            "status": job.status,
            "progress": job.progress,
            "location": job.location,
            "listing_type": job.listing_type,
            "property_type": job.property_type,
            "search_context": job.search_context,
            "total_urls_found": job.total_urls_found,
            "total_urls_scraped": job.total_urls_scraped,
            "total_errors": job.total_errors,
            "listings_count": len(job.listings),
            "listings": [item.model_dump(mode="json") for item in job.listings],
            "error_message": job.error_message,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
        }
        enriched = enrich_job_payload(enrich_payload)
        output["categorized_listings"] = enriched.get("categorized_listings", {})
        output["analysis"] = enriched.get("analysis", {})

    print(json.dumps(output, ensure_ascii=False, default=str))


if __name__ == "__main__":
    asyncio.run(main())

from pathlib import Path
import sys
import os

ROOT = Path(__file__).resolve().parent
VENDOR = ROOT / ".vendor"
if VENDOR.exists():
    vendor_path = str(VENDOR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import asyncio
import json
import time

from scraper.orchestrator import orchestrator
from scraper.response_enrichment import enrich_job_payload
from scraper.google_search import discover_listing_urls, build_pseudo_listings_from_candidates

app = FastAPI(title="Emlak Radius Scraper", description="Koordinat bazli emlak ilan scraper servisi")
templates = Jinja2Templates(directory="templates")
VALID_LISTING_TYPES = {"sale", "rent", "all"}
VALID_PROPERTY_TYPES = {"residential", "commercial", "land", "all"}


def _serialize_job(job):
    payload = {
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
        "listings": [listing.model_dump(mode="json") for listing in job.listings],
        "error_message": job.error_message,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
    }

    if job.status != "completed":
        return payload

    cached = job.search_context.get("response_enrichment")
    if isinstance(cached, dict):
        payload.update(cached)
        return payload

    enriched = enrich_job_payload(payload)
    job.search_context["response_enrichment"] = {
        "categorized_listings": enriched.get("categorized_listings", {}),
        "analysis": enriched.get("analysis", {}),
    }
    return enriched


def _read_choice(data: dict, key: str, allowed: set[str], default: str) -> str:
    value = str(data.get(key, default)).strip().lower() or default
    if value not in allowed:
        raise HTTPException(400, f"gecersiz {key}: {value}")
    return value


def _read_num_pages(data: dict) -> int:
    try:
        value = int(data.get("num_pages", 1))
    except (TypeError, ValueError):
        raise HTTPException(400, "num_pages sayi olmali")

    if value < 1 or value > 3:
        raise HTTPException(400, "num_pages 1 ile 3 arasinda olmali")
    return value


def _read_discovery_timeout_seconds(data: dict) -> float:
    default_value = float(os.getenv("DISCOVERY_TIMEOUT_SECONDS", "55") or "55")
    raw = data.get("timeout_seconds", default_value)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise HTTPException(400, "timeout_seconds sayisal olmali")
    if value < 5 or value > 60:
        raise HTTPException(400, "timeout_seconds 5 ile 60 arasinda olmali")
    return value


def _read_optional_bool(data: dict, key: str) -> bool | None:
    raw = data.get(key, None)
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)

    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise HTTPException(400, f"{key} true/false olmali")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/api/scrape")
async def start_scrape(request: Request):
    """
    Asenkron scrape job baslat.
    Body: {"lat": "41.0082", "lng": "28.9784", "listing_type": "sale|rent|all", "property_type": "residential|commercial|land|all"}
    Response: {"job_id": "...", "status": "running"}
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "gecersiz veya bos JSON body")
    lat = data.get("lat", "").strip()
    lng = data.get("lng", "").strip()

    if not lat or not lng:
        raise HTTPException(400, "lat ve lng koordinatlari gerekli")

    listing_type = _read_choice(data, "listing_type", VALID_LISTING_TYPES, "all")
    property_type = _read_choice(data, "property_type", VALID_PROPERTY_TYPES, "all")
    num_pages = _read_num_pages(data)

    job = orchestrator.create_job(
        lat=lat,
        lng=lng,
        listing_type=listing_type,
        property_type=property_type,
    )

    asyncio.create_task(
        orchestrator.run_scrape(
            job_id=job.job_id,
            lat=lat,
            lng=lng,
            listing_type=listing_type,
            property_type=property_type,
            num_pages=num_pages,
        )
    )

    return {"job_id": job.job_id, "status": "running"}


@app.post("/api/scrape/stream")
async def start_scrape_stream(request: Request):
    """SSE ile progress stream eden scrape job.

    Body: {"lat": "41.0082", "lng": "28.9784", "listing_type": "sale|rent|all", "property_type": "residential|commercial|land|all"}
    """
    data = await request.json()
    lat = data.get("lat", "").strip()
    lng = data.get("lng", "").strip()

    if not lat or not lng:
        raise HTTPException(400, "lat ve lng koordinatlari gerekli")

    listing_type = _read_choice(data, "listing_type", VALID_LISTING_TYPES, "all")
    property_type = _read_choice(data, "property_type", VALID_PROPERTY_TYPES, "all")
    num_pages = _read_num_pages(data)
    job = orchestrator.create_job(
        lat=lat,
        lng=lng,
        listing_type=listing_type,
        property_type=property_type,
    )
    progress_queue: asyncio.Queue = asyncio.Queue()

    async def event_generator():
        while True:
            event = await progress_queue.get()
            if event is None:
                break
            yield f"data: {json.dumps(event, default=str, ensure_ascii=False)}\n\n"

    async def run_with_progress():
        await orchestrator.run_scrape(
            job_id=job.job_id,
            lat=lat,
            lng=lng,
            listing_type=listing_type,
            property_type=property_type,
            num_pages=num_pages,
            progress_queue=progress_queue,
        )
        await progress_queue.put(None)

    asyncio.create_task(run_with_progress())
    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/api/scrape/run")
async def run_scrape_sync(request: Request):
    """Tek request'te scrape edip ilanlari geri dondur."""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "gecersiz veya bos JSON body")
    lat = data.get("lat", "").strip()
    lng = data.get("lng", "").strip()

    if not lat or not lng:
        raise HTTPException(400, "lat ve lng koordinatlari gerekli")

    listing_type = _read_choice(data, "listing_type", VALID_LISTING_TYPES, "all")
    property_type = _read_choice(data, "property_type", VALID_PROPERTY_TYPES, "all")
    num_pages = _read_num_pages(data)

    job = orchestrator.create_job(
        lat=lat,
        lng=lng,
        listing_type=listing_type,
        property_type=property_type,
    )
    await orchestrator.run_scrape(
        job_id=job.job_id,
        lat=lat,
        lng=lng,
        listing_type=listing_type,
        property_type=property_type,
        num_pages=num_pages,
    )
    return _serialize_job(job)


@app.post("/api/discovery/run")
async def run_discovery_sync(request: Request):
    """
    Scrape yapmadan sadece search API'lerle aday ilanlari topla ve hizli analiz dondur.
    """
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "gecersiz veya bos JSON body")
    if not isinstance(data, dict):
        raise HTTPException(400, "JSON body dict olmali")
    lat = data.get("lat", "").strip()
    lng = data.get("lng", "").strip()

    if not lat or not lng:
        raise HTTPException(400, "lat ve lng koordinatlari gerekli")

    listing_type = _read_choice(data, "listing_type", VALID_LISTING_TYPES, "all")
    property_type = _read_choice(data, "property_type", VALID_PROPERTY_TYPES, "all")
    num_pages = _read_num_pages(data)
    timeout_seconds = _read_discovery_timeout_seconds(data)
    strict_detail_only = _read_optional_bool(data, "strict_detail_only")
    strict_district_header = _read_optional_bool(data, "strict_district_header")

    started = time.perf_counter()
    try:
        discovery = await asyncio.wait_for(
            discover_listing_urls(
                lat=lat,
                lng=lng,
                listing_type=listing_type,
                property_type=property_type,
                num_pages=num_pages,
                fast_mode=True,
                strict_detail_only=strict_detail_only,
                strict_district_header=strict_district_header,
            ),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        raise HTTPException(504, f"discovery timeout: {timeout_seconds:.0f}s")

    pseudo_listings = build_pseudo_listings_from_candidates(
        candidates=discovery.get("candidates", []),
        geo=discovery.get("geo", {}),
        limit=int(os.getenv("DISCOVERY_OUTPUT_LIMIT", "60") or "60"),
    )

    payload = {
        "job_id": f"discovery-{int(time.time())}",
        "status": "completed",
        "progress": "discovery_only",
        "location": f"{lat},{lng}",
        "listing_type": listing_type,
        "property_type": property_type,
        "search_context": {
            "geo": discovery.get("geo"),
            "queries": discovery.get("queries", []),
            "query_count": len(discovery.get("queries", [])),
            "search_strategy": discovery.get("search_strategy"),
            "google_throttled": discovery.get("google_throttled", False),
            "locality": discovery.get("locality", {}),
            "jina_reader_enriched_count": discovery.get("jina_reader_enriched_count", 0),
            "fast_mode": True,
            "strict_detail_only": strict_detail_only,
            "strict_district_header": strict_district_header,
        },
        "total_urls_found": len(discovery.get("urls", [])),
        "total_urls_scraped": 0,
        "total_errors": 0,
        "listings_count": len(pseudo_listings),
        "listings": pseudo_listings,
        "error_message": None,
        "started_at": None,
        "completed_at": None,
        "candidates": discovery.get("candidates", []),
        "candidate_count": len(discovery.get("candidates", [])),
    }
    enriched = enrich_job_payload(payload, include_ai=False)
    enriched["mode"] = "discovery_only"
    enriched["elapsed_seconds"] = round(time.perf_counter() - started, 2)
    enriched["timeout_seconds"] = timeout_seconds
    return enriched


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Job durumu ve tum ilanlarini getir."""
    job = orchestrator.get_job(job_id)
    if not job:
        raise HTTPException(404, "Job bulunamadi")
    return _serialize_job(job)


if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload_enabled = os.getenv("RELOAD", "false").lower() == "true"
    uvicorn.run("main:app", host=host, port=port, reload=reload_enabled)

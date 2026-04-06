from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class Listing(BaseModel):
    source_url: str
    source_site: str                     # "sahibinden", "hepsiemlak", etc.
    title: str
    price: Optional[float] = None
    currency: str = "TRY"
    area_sqm: Optional[float] = None
    rooms: Optional[str] = None          # "3+1", "2+0"
    listing_type: Optional[str] = None   # "sale", "rent"
    property_type: Optional[str] = None  # "residential", "commercial", "land"
    district: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    description: Optional[str] = None
    images: list[str] = Field(default_factory=list)
    date_posted: Optional[str] = None
    agent_type: Optional[str] = None     # "owner", "agent"
    scraped_at: datetime = Field(default_factory=datetime.now)


class ScrapeJob(BaseModel):
    job_id: str
    location: str                        # "41.0138,28.9497" or "Kadikoy Istanbul"
    property_type: str = "all"           # "residential" | "commercial" | "land" | "all"
    listing_type: str = "all"            # "sale" | "rent" | "all"
    status: str = "pending"              # "pending" | "running" | "completed" | "failed"
    listings: list[Listing] = Field(default_factory=list)
    search_context: dict = Field(default_factory=dict)
    total_urls_found: int = 0
    total_urls_scraped: int = 0
    total_errors: int = 0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    progress: str = ""

    def to_sse_event(self) -> dict:
        return {
            "type": "job_progress",
            "job_id": self.job_id,
            "status": self.status,
            "progress": self.progress,
            "property_type": self.property_type,
            "listing_type": self.listing_type,
            "total_urls_found": self.total_urls_found,
            "total_urls_scraped": self.total_urls_scraped,
            "total_errors": self.total_errors,
            "listings_count": len(self.listings),
        }

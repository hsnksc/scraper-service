from urllib.parse import urlparse
from typing import Optional


KNOWN_SITES = {
    "sahibinden.com": "sahibinden",
    "hepsiemlak.com": "hepsiemlak",
    "hurriyetemlak.com": "hurriyetemlak",
    "emlakjet.com": "emlakjet",
    "emlakcarsi.com": "emlakcarsi",
    "emlakmarketiantalya.com": "emlakmarketiantalya",
    "rentola.com.tr": "rentola",
    "flatspotter.com": "flatspotter",
    "tr.flatspotter.com": "flatspotter",
    "emlakgo.net": "emlakgo",
    "remax.com.tr": "remax",
    "zinza.com": "zinza",
}

LISTING_PATTERNS = {
    "sahibinden": ["/ilan/emlak/"],
    "hepsiemlak": ["/ilan/"],
    "hurriyetemlak": ["/ilan/", "/emlak/"],
    "emlakjet": ["/ilan/", "/ilan-", "-ilan-"],
    "emlakcarsi": ["/detay", "/ilan-"],
    "emlakmarketiantalya": ["/ilanlari/"],
    "rentola": ["/listings/", "/for-rent/"],
    "flatspotter": ["/kiralik-", "/satilik-"],
    "emlakgo": ["/site-", "/ilan/"],
    "remax": ["/konut/", "/ticari/"],
    "zinza": ["/ilan/"],
}


def detect_site(url: str) -> Optional[str]:
    """Return site key if URL belongs to a known real estate site."""
    try:
        domain = urlparse(url).netloc.lower()
        domain = domain.replace("www.", "")
        for known_domain, site_key in KNOWN_SITES.items():
            if known_domain in domain:
                return site_key
    except Exception:
        pass
    return None


def is_listing_url(url: str, site_key: str) -> bool:
    """Check if URL looks like a listing page (not homepage/search)."""
    patterns = LISTING_PATTERNS.get(site_key, [])
    url_lower = url.lower()
    return any(p in url_lower for p in patterns)


def is_real_estate_domain(url: str) -> bool:
    """Check if URL belongs to any known Turkish real estate domain."""
    return detect_site(url) is not None

import asyncio
import logging
import os
import random
from pathlib import Path
from playwright.async_api import async_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BROWSERS_PATH = PROJECT_ROOT / ".pw-browsers"
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(DEFAULT_BROWSERS_PATH))


class PlaywrightManager:
    """Manages Playwright browser lifecycle with rate limiting."""

    def __init__(self):
        self.playwright = None
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.delay_min = float(os.getenv("PLAYWRIGHT_DELAY_MIN", "0.0"))
        self.delay_max = float(os.getenv("PLAYWRIGHT_DELAY_MAX", "0.1"))
        self.max_concurrent = int(os.getenv("MAX_CONCURRENT_PAGES", "8"))
        self.nav_timeout_ms = int(os.getenv("PLAYWRIGHT_NAV_TIMEOUT_MS", "15000"))
        self.domcontent_timeout_ms = int(os.getenv("PLAYWRIGHT_DOMCONTENT_TIMEOUT_MS", "4000"))
        self.semaphore = None
        self.headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"

    async def start(self):
        self.playwright = await async_playwright().start()
        launch_kwargs = {
            "headless": self.headless,
            "args": ["--no-sandbox", "--disable-setuid-sandbox"],
        }

        executable_path = self._resolve_executable_path()
        if executable_path:
            launch_kwargs["executable_path"] = executable_path

        self.browser = await self.playwright.chromium.launch(**launch_kwargs)
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        logger.info("Playwright browser started")

    async def stop(self):
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Playwright browser stopped")

    async def visit_page(self, url: str, timeout: int | None = None) -> Page | None:
        """Navigate to URL with random delay, return page or None on failure."""
        if timeout is None:
            timeout = self.nav_timeout_ms
        delay = random.uniform(self.delay_min, self.delay_max)
        logger.debug(f"Waiting {delay:.1f}s before visiting {url}")
        await asyncio.sleep(delay)

        async with self.semaphore:
            try:
                page = await self.context.new_page()
                response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                if response and response.status >= 400:
                    logger.warning(f"HTTP {response.status} for {url}")
                    await page.close()
                    return None
                await page.wait_for_load_state("domcontentloaded", timeout=self.domcontent_timeout_ms)
                return page
            except Exception as e:
                logger.warning(f"Failed to visit {url}: {e}")
                try:
                    await page.close()
                except Exception:
                    pass
                return None

    def _resolve_executable_path(self) -> str | None:
        env_path = os.getenv("PLAYWRIGHT_EXECUTABLE_PATH", "").strip()
        if env_path and Path(env_path).exists():
            return env_path

        local_app_data = Path(os.getenv("LOCALAPPDATA", ""))
        default_root = local_app_data / "ms-playwright"
        if not default_root.exists():
            return None

        candidates = sorted(default_root.glob("chromium-*/chrome-win64/chrome.exe"), reverse=True)
        for candidate in candidates:
            if candidate.exists():
                logger.info(f"Using Chromium executable fallback: {candidate}")
                return str(candidate)
        return None

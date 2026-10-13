import re
import weakref
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

from scrapling.fetchers import ProxyRotator
from scrapling.spiders import Request

from fmkorea_scraper.dom import SEOUL


def safe_endpoint(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.hostname}:{parsed.port}" if parsed.hostname and parsed.port else None


@dataclass
class BrowserIdentityCandidate:
    proxy_id: str
    capture_seq: int
    cookies: list[dict]
    browser_user_agent: str
    browser_chrome_major: int | None
    captured_at: datetime


class BrowserIdentityBridge:
    def __init__(self):
        self.active = {}
        self.sequence = {}
        self.pending = {}

    def next_sequence(self, proxy_id: str) -> int:
        value = self.sequence.get(proxy_id, 0) + 1
        self.sequence[proxy_id] = value
        return value

    def capture(self, candidate: BrowserIdentityCandidate):
        self.sequence[candidate.proxy_id] = max(self.sequence.get(candidate.proxy_id, 0), candidate.capture_seq)
        self.pending[(candidate.proxy_id, candidate.capture_seq)] = candidate

    def promote(self, proxy_id: str, sequence: int, blocked: bool):
        candidate = self.pending.pop((proxy_id, sequence), None)
        if blocked or candidate is None:
            return
        current = self.active.get(proxy_id)
        if current is None or candidate.capture_seq > current.capture_seq:
            self.active[proxy_id] = candidate

    def http_overlay(self, proxy_id: str) -> dict:
        candidate = self.active.get(proxy_id)
        if not candidate:
            return {}
        cookies = {cookie["name"]: cookie["value"] for cookie in candidate.cookies
            if cookie.get("domain", "").lstrip(".").endswith("fmkorea.com")}
        result = {"cookies": cookies} if cookies else {}
        major = candidate.browser_chrome_major
        if major:
            from curl_cffi.requests.impersonate import BrowserType
            exact = f"chrome{major}"
            if exact in BrowserType.__members__:
                result["impersonate"] = exact
                result["headers"] = {"User-Agent": candidate.browser_user_agent}
        return result


_IDENTITY_BRIDGES = weakref.WeakValueDictionary()


@dataclass(frozen=True)
class BrowserIdentityCapture:
    job_id: str
    proxy_id: str
    capture_seq: int

    async def __call__(self, page):
        bridge = _IDENTITY_BRIDGES[self.job_id]
        await page.wait_for_load_state("domcontentloaded")
        cookies = await page.context.cookies()
        ua = await page.evaluate("navigator.userAgent")
        match = re.search(r"Chrome/(\d+)", ua)
        bridge.capture(BrowserIdentityCandidate(self.proxy_id, self.capture_seq, cookies, ua,
            int(match.group(1)) if match else None, datetime.now(SEOUL)))


def register_identity_bridge(job_id: str, bridge: BrowserIdentityBridge):
    _IDENTITY_BRIDGES[job_id] = bridge


class ProxyAttemptCoordinator:
    def __init__(self, proxies: list[str]):
        if len(proxies) != len(set(proxies)) or any(not proxy for proxy in proxies):
            raise ValueError("empty or duplicate proxy")
        self.proxies = proxies
        self.ids = {proxy: f"proxy-{index:04d}" for index, proxy in enumerate(proxies, 1)}
        self.rotator = ProxyRotator(proxies) if proxies else None

    def next_proxy(self) -> str:
        return self.ids[self.rotator.get_proxy()] if self.rotator else "direct"

    def endpoint(self, proxy_id: str) -> str | None:
        if proxy_id == "direct":
            return None
        return safe_endpoint(self.proxies[int(proxy_id.split("-")[-1]) - 1])

    def request(self, url: str, callback, *, meta: dict | None = None,
                priority: int = 0, dont_filter: bool = False) -> Request:
        proxy_id = self.next_proxy()
        context = {key: value for key, value in (meta or {}).items()
            if key in {"board_id_hint", "document_id_hint", "page", "page_kind", "source_type", "comment_state"}}
        context.update(proxy_id=proxy_id, proxy_attempt=1, blocked_attempt=0,
            proxy_log=[self.event(proxy_id, 1, "http", 0, False)])
        sid = f"http:{proxy_id}" if proxy_id != "direct" else "http"
        return Request(url, sid=sid, callback=callback, priority=priority,
            dont_filter=dont_filter, meta=context)

    def event(self, proxy_id: str, attempt: int, phase: str, blocked_attempt: int,
              rotated: bool) -> dict:
        return dict(proxy_id=proxy_id, endpoint=self.endpoint(proxy_id),
            proxy_attempt=attempt, phase=phase, blocked_attempt=blocked_attempt,
            rotated=rotated)

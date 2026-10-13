from datetime import datetime, timedelta
from urllib.parse import urlparse

import anyio
from scrapling import Selector
from scrapling.fetchers import AsyncStealthySession, FetcherSession
from scrapling.spiders import Request, SessionManager, Spider

from fmkorea_scraper.collect.parsers import parse_board_page, parse_integrated_page
from fmkorea_scraper.collect.urls import (allowed_post_url, board_search_url, board_url,
    document_id, integrated_search_url)
from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.job import JobConfig, SpiderRuntimeConfig
from fmkorea_scraper.models.failure import FailedPostKey, FailureRecord
from fmkorea_scraper.models.post import PostResult
from fmkorea_scraper.parse.comments import comment_page_urls, parse_comments
from fmkorea_scraper.parse.service import parse_post
from fmkorea_scraper.outputs.errors import StorageError

from .routing import BrowserIdentityBridge, BrowserIdentityCapture, ProxyAttemptCoordinator, register_identity_bridge, safe_endpoint
from .proxy_bridge import browser_proxy_bridges


SECURITY_TITLE = "\uc5d0\ud3a8\ucf54\ub9ac\uc544 \ubcf4\uc548 \uc2dc\uc2a4\ud15c".encode("utf-8")
UNAVAILABLE_MARKERS = ("deleted", "unavailable", "\uc0ad\uc81c\ub41c \uac8c\uc2dc\uae00", "\uc0ad\uc81c\ub41c \uac8c\uc2dc\ubb3c",
    "\uc874\uc7ac\ud558\uc9c0 \uc54a\ub294 \uac8c\uc2dc\uae00", "\uc874\uc7ac\ud558\uc9c0 \uc54a\ub294 \uac8c\uc2dc\ubb3c", "\uad8c\ud55c\uc774 \uc5c6\uc2b5\ub2c8\ub2e4", "\uc5f4\ub78c\ud560 \uc218 \uc5c6\uc2b5\ub2c8\ub2e4")


def is_semantic_unavailable(response) -> bool:
    if response.status in {404, 410}:
        return True
    if response.request.meta.get("page_kind") != "post":
        return False
    page = Selector(response.body, url=response.url)
    for node in page.css("#message, #error, .message, .error, .alert, .xe_message, .xe_msg, .error-page, .error_page"):
        message = " ".join(node.css("::text").getall()).lower()
        if any(marker in message for marker in UNAVAILABLE_MARKERS):
            return True
    return False


class FMKoreaSpider(Spider):
    name = "fmkorea"
    allowed_domains = ["fmkorea.com"]
    development_mode = False
    logging_level = "WARNING"

    async def parse(self, response):
        raise RuntimeError("request callback missing")
        yield None

    def __init__(self, config: JobConfig, runtime: SpiderRuntimeConfig | None = None,
                 output_store=None, completed_keys: set | None = None,
                 stop_event: anyio.Event | None = None, deadline: datetime | None = None,
                 crawldir=None, debug: bool = False):
        self.config = config
        self.runtime = runtime or SpiderRuntimeConfig()
        self.output_store = output_store
        self.completed_keys = completed_keys if completed_keys is not None else set()
        self.scheduled_keys = set()
        self.unavailable_keys = set()
        self.claim_lock = anyio.Lock()
        self.stop_event = stop_event or anyio.Event()
        self.deadline = deadline
        self.routing = ProxyAttemptCoordinator(self.runtime.proxies)
        self.browser_proxies = {}
        self.identity = BrowserIdentityBridge()
        register_identity_bridge(config.job_id, self.identity)
        self.failures = []
        self.collected_count = 0
        self.stored_count = 0
        self.terminal_blocked = False
        self.terminal_error = False
        self.deep_boards = set()
        self.logging_level = "DEBUG" if debug else "WARNING"
        crawl = config.crawl_config
        self.concurrent_requests = crawl.concurrent_requests
        self.concurrent_requests_per_domain = crawl.concurrent_requests_per_domain
        self.download_delay = crawl.download_delay
        self.max_blocked_retries = crawl.max_blocked_retries
        self.autothrottle_enabled = crawl.autothrottle_enabled
        self.autothrottle_start_delay = crawl.autothrottle_start_delay
        self.autothrottle_max_delay = crawl.autothrottle_max_delay
        self.robots_txt_obey = crawl.robots_txt_obey
        super().__init__(crawldir=crawldir, interval=crawl.checkpoint_interval_seconds)

    async def stream(self):
        async with browser_proxy_bridges(self.runtime.proxies) as self.browser_proxies:
            self._session_manager = SessionManager()
            self.configure_sessions(self._session_manager)
            async for item in super().stream():
                yield item

    def configure_sessions(self, manager):
        endpoints = [("direct", None)] if not self.runtime.proxies else list(self.routing.ids.items())
        for index, (proxy, proxy_id) in enumerate(endpoints):
            if proxy_id is None:
                proxy_id, proxy = "direct", None
            http_id = f"http:{proxy_id}" if proxy_id != "direct" else "http"
            browser_id = f"browser:{proxy_id}" if proxy_id != "direct" else "browser"
            manager.add(http_id, FetcherSession(proxy=proxy, impersonate="chrome", follow_redirects="safe",
                stealthy_headers=True, timeout=self.runtime.http_timeout_seconds,
                retries=self.runtime.http_transport_retries), default=index == 0, lazy=True)
            manager.add(browser_id, AsyncStealthySession(proxy=self.browser_proxies.get(proxy, proxy), headless=True, google_search=True,
                hide_canvas=True, block_webrtc=True, solve_cloudflare=True, allow_webgl=True, disable_resources=False, network_idle=False, load_dom=True, 
                max_pages=self.config.crawl_config.browser_max_pages,
                timeout=self.runtime.browser_timeout_ms,
                retries=self.runtime.browser_transport_retries), lazy=True)

    def _request(self, url: str, callback, *, meta: dict | None = None,
                 dont_filter: bool = False, priority: int = 0):
        request = self.routing.request(url, callback, meta=meta, dont_filter=dont_filter,
            priority=priority)
        request._session_kwargs.update(self.identity.http_overlay(request.meta["proxy_id"]))
        return request

    def _failure(self, stage: str, reason: str, url: str | None = None,
                 board: str | None = None, raw_id: str | None = None):
        raw_id = raw_id or (document_id(url) if url else None)
        key = FailedPostKey(board_id=board, document_srl=int(raw_id) if raw_id else None,
            document_id_raw=raw_id) if raw_id else None
        record = FailureRecord(job_id=self.config.job_id, stage=stage, reason=reason,
            retryable=stage == "post",
            url=url, post_key=key, safe_message=f"{stage} failed: {reason}",
            details={"stage": stage, "reason": reason}, created_at=datetime.now(SEOUL))
        self.failures.append(record.model_dump(mode="json"))
        if self.output_store is not None:
            self.output_store.append_failure(self.failures[-1])

    async def start_requests(self):
        source = self.config.source
        kind = source.type
        if kind in {"direct_post", "parser_failure_retry"}:
            urls = source.urls if kind == "direct_post" else []
            for url in urls:
                if not allowed_post_url(url) or not document_id(url):
                    raise ValueError("invalid direct-post URL")
                if any(key[2] == document_id(url) for key in self.completed_keys):
                    continue
                yield self._request(url, self.parse_detail, meta={"document_id_hint": document_id(url), "page_kind": "post"})
        elif kind in {"board_list", "no_keyword_range", "realtime_all"}:
            yield self._request(board_url(source.board_id, getattr(source, "page_start", 1), source.category_id),
                self.parse_listing, meta={"board_id_hint": source.board_id,
                    "page": getattr(source, "page_start", 1), "page_kind": "board"})
        elif kind in {"board_search", "realtime_keyword"} and (kind != "realtime_keyword" or source.scope == "board"):
            yield self._request(board_search_url(source.board_id, source.keyword, source.search_target,
                getattr(source, "page_start", 1), getattr(source, "category_id", None)), self.parse_listing,
                meta={"board_id_hint": source.board_id, "page": getattr(source, "page_start", 1), "page_kind": "search"})
        else:
            if kind == "deep_search":
                for board in source.board_ids:
                    self.deep_boards.add(board)
                    yield self._request(board_search_url(board, source.keyword,
                        source.seed_search_target), self.parse_listing,
                        meta={"board_id_hint": board, "page": 1, "page_kind": "search"})
            target = getattr(source, "seed_search_target", getattr(source, "search_target", "title_content"))
            yield self._request(integrated_search_url(source.keyword, target,
                getattr(source, "page_start", 1)), self.parse_integrated,
                meta={"page": getattr(source, "page_start", 1), "page_kind": "integrated"})

    async def is_blocked(self, response):
        body = response.body[:4096].lower() if isinstance(response.body, bytes) else str(response.body)[:4096].lower().encode()
        blocked = response.status in {401, 403, 407, 429, 444, 500, 502, 503, 504} or any(
            marker in body for marker in (b"cloudflare", b"just a moment", b"access denied", b"captcha", SECURITY_TITLE))
        request = response.request
        if request.sid.startswith("browser") and is_semantic_unavailable(response):
            blocked = False
        reported_proxy = response.meta.get("proxy")
        expected_endpoint = self.routing.endpoint(request.meta["proxy_id"])
        if request.sid.startswith("browser") and expected_endpoint:
            original = self.routing.proxies[int(request.meta["proxy_id"].split("-")[-1]) - 1]
            expected_endpoint = safe_endpoint(self.browser_proxies.get(original, original))
        if isinstance(reported_proxy, str) and expected_endpoint and safe_endpoint(reported_proxy) != expected_endpoint:
            raise ValueError("response proxy differs from selected proxy")
        history = [event.copy() for event in request.meta.get("proxy_log", [])]
        if history:
            history[-1].update(status_code=response.status, blocked=blocked,
                result="blocked" if blocked else "success")
        response.meta["proxy_log"] = history
        request.meta["proxy_log"] = history
        seq = request.meta.get("identity_capture_seq")
        if seq is not None:
            self.identity.promote(request.meta["proxy_id"], seq, blocked)
        if (blocked and request.sid.startswith("browser")
                and request.meta["blocked_attempt"] >= self.max_blocked_retries
                and not self.terminal_blocked and not self.stop_event.is_set()):
            self.terminal_blocked = True
            self.pause()
        return blocked

    async def retry_blocked_request(self, request: Request, response) -> Request:
        context = request.meta.copy()
        context["blocked_attempt"] += 1
        proxy_id = context["proxy_id"]
        attempt = context["proxy_attempt"]
        if request.sid.startswith("http"):
            sid = f"browser:{proxy_id}" if proxy_id != "direct" else "browser"
            phase = "browser_fallback"
            seq = self.identity.next_sequence(proxy_id)
            context["identity_capture_seq"] = seq

            kwargs = {"page_action": BrowserIdentityCapture(self.config.job_id, proxy_id, seq)}
            referer = request._session_kwargs.get("headers", {}).get("Referer")
            if referer:
                kwargs["extra_headers"] = {"Referer": referer}
        else:
            previous = proxy_id
            proxy_id = self.routing.next_proxy()
            attempt += 1
            context["proxy_id"] = proxy_id
            context["proxy_attempt"] = attempt
            context.pop("identity_capture_seq", None)
            sid = f"http:{proxy_id}" if proxy_id != "direct" else "http"
            phase = "http"
            kwargs = self.identity.http_overlay(proxy_id)
        context["proxy_log"] = [*context.get("proxy_log", []),
            self.routing.event(proxy_id, attempt, phase, context["blocked_attempt"],
                phase == "http" and proxy_id != previous if phase == "http" else False)]
        return Request(request.url, sid=sid, callback=request.callback,
            priority=request.priority, dont_filter=True, meta=context,
            _retry_count=request._retry_count, **kwargs)

    async def _claim(self, board: str | None, raw_id: str) -> bool:
        key = ("fmkorea", board or "", raw_id)
        async with self.claim_lock:
            if key in self.completed_keys or key in self.scheduled_keys or key in self.unavailable_keys:
                return False
            self.scheduled_keys.add(key)
            return True

    def _within_range(self, candidate) -> bool:
        filt = getattr(self.config.source, "range_filter", None)
        if filt is None:
            return True
        if filt.min_document_srl and candidate.document_srl < filt.min_document_srl:
            return False
        if filt.max_document_srl and candidate.document_srl > filt.max_document_srl:
            return False
        if candidate.candidate_created_at:
            day = candidate.candidate_created_at.date()
            if filt.date_from and day < filt.date_from:
                return False
            if filt.date_to and day > filt.date_to:
                return False
        return True

    async def _wait_for_poll(self):
        source = self.config.source
        if not source.type.startswith("realtime_") or self.stop_event.is_set():
            return False
        if self.deadline and datetime.now(SEOUL) >= self.deadline:
            return False
        remaining = (self.deadline - datetime.now(SEOUL)).total_seconds() if self.deadline else source.poll_interval_seconds
        with anyio.move_on_after(min(source.poll_interval_seconds, max(0, remaining))):
            await self.stop_event.wait()
        return not self.stop_event.is_set() and (not self.deadline or datetime.now(SEOUL) < self.deadline)

    async def parse_listing(self, response):
        source = self.config.source
        page = response.meta["page"]
        board = response.meta.get("board_id_hint")
        try:
            parsed = parse_board_page(response, response.url, page, board)
        except ValueError as exc:
            self._failure("board", str(exc), response.url, board)
            return
        for failure in parsed.failures:
            self._failure(failure["stage"], failure["reason"], response.url, board)
        stop_pagination = False
        bounds = getattr(source, "range_filter", None)
        for candidate in parsed.posts:
            if bounds and ((bounds.min_document_srl and candidate.document_srl < bounds.min_document_srl)
                    or (bounds.date_from and candidate.candidate_created_at and candidate.candidate_created_at.date() < bounds.date_from)):
                stop_pagination = True
            if self._within_range(candidate) and await self._claim(candidate.board_id or board, candidate.document_id_raw):
                yield self._request(candidate.url, self.parse_detail,
                    meta={"board_id_hint": candidate.board_id or board,
                        "document_id_hint": candidate.document_id_raw, "page_kind": "post"})
        if source.type.startswith("realtime_"):
            if await self._wait_for_poll():
                next_url = board_url(board, 1, getattr(source, "category_id", None)) if source.type == "realtime_all" else board_search_url(board, source.keyword, source.search_target, 1)
                yield self._request(next_url, self.parse_listing, dont_filter=True,
                    meta={"board_id_hint": board, "page": 1, "page_kind": "board"})
        elif parsed.has_next and not stop_pagination and (getattr(source, "page_end", None) is None or page < source.page_end):
            next_page = page + 1
            next_url = board_url(board, next_page, getattr(source, "category_id", None)) if source.type in {"board_list", "no_keyword_range"} else board_search_url(board, source.keyword, source.search_target, next_page, getattr(source, "category_id", None))
            yield self._request(next_url, self.parse_listing,
                meta={"board_id_hint": board, "page": next_page, "page_kind": response.meta["page_kind"]})

    async def parse_integrated(self, response):
        source = self.config.source
        page = response.meta["page"]
        try:
            parsed = parse_integrated_page(response, response.url, page)
        except ValueError as exc:
            self._failure("integrated", str(exc), response.url)
            return
        for failure in parsed.failures:
            self._failure(failure["stage"], failure["reason"], response.url)
        for candidate in parsed.posts:
            if self._within_range(candidate) and await self._claim(candidate.board_id, candidate.document_id_raw):
                yield self._request(candidate.url, self.parse_detail,
                    meta={"board_id_hint": candidate.board_id,
                        "document_id_hint": candidate.document_id_raw, "page_kind": "post"})
        if source.type == "deep_search":
            for board in parsed.boards:
                if board.board_id not in self.deep_boards:
                    self.deep_boards.add(board.board_id)
                    yield self._request(board_search_url(board.board_id, source.keyword,
                        source.seed_search_target), self.parse_listing,
                        meta={"board_id_hint": board.board_id, "page": 1, "page_kind": "search"})
        if source.type == "realtime_keyword":
            if await self._wait_for_poll():
                yield self._request(integrated_search_url(source.keyword, source.search_target),
                    self.parse_integrated, dont_filter=True,
                    meta={"page": 1, "page_kind": "integrated"})
        elif parsed.has_next and page < min(getattr(source, "page_end", None) or 1000,
                getattr(source, "seed_page_limit", 1000)):
            next_page = page + 1
            target = getattr(source, "seed_search_target", getattr(source, "search_target", "title_content"))
            yield self._request(integrated_search_url(source.keyword, target, next_page),
                self.parse_integrated, meta={"page": next_page, "page_kind": "integrated"})

    async def parse_detail(self, response):
        if is_semantic_unavailable(response):
            raw_id = response.meta.get("document_id_hint") or document_id(response.url)
            self.unavailable_keys.add(("fmkorea", response.meta.get("board_id_hint") or "", raw_id))
            return
        try:
            options = self.config.scrape_options
            if options.max_comment_pages:
                options = options.model_copy(update={"max_comments": None})
            post, warnings = parse_post(response.body, response.url, options,
                response.meta.get("board_id_hint"), response.meta.get("document_id_hint"))
        except (ValueError, TypeError) as exc:
            self._failure("post", type(exc).__name__, response.url,
                response.meta.get("board_id_hint"), response.meta.get("document_id_hint"))
            return
        if warnings:
            post.metadata["parse_warnings"] = warnings
        self.collected_count += 1
        if self.config.source.type == "deep_search" and post.board_id not in self.deep_boards:
            self.deep_boards.add(post.board_id)
            source = self.config.source
            yield self._request(board_search_url(post.board_id, source.keyword,
                source.seed_search_target), self.parse_listing,
                meta={"board_id_hint": post.board_id, "page": 1, "page_kind": "search"})
        pages = comment_page_urls(response, post.canonical_url, post.document_id_raw,
            self.config.scrape_options.max_comment_pages or 0) if self.config.scrape_options.comment_mode != "none" else []
        if pages:
            state = {"post": post.model_dump(mode="json"), "pages": pages, "index": 0}
            yield self._request(pages[0], self.parse_comment_page,
                meta={"board_id_hint": post.board_id, "document_id_hint": post.document_id_raw,
                    "page_kind": "comment", "comment_state": state})
        else:
            max_comments = self.config.scrape_options.max_comments
            if max_comments is not None:
                post.comments = post.comments[:max_comments]
            yield post.model_dump(mode="json")

    async def parse_comment_page(self, response):
        state = response.meta["comment_state"]
        post = PostResult.model_validate(state["post"])
        new, warnings = parse_comments(response, post.canonical_url, "all",
            source_order_start=len(post.comments))
        seen = {comment.comment_srl or comment.source_dom_id for comment in post.comments}
        for comment in new:
            key = comment.comment_srl or comment.source_dom_id
            if key not in seen:
                seen.add(key)
                post.comments.append(comment)
        if warnings:
            post.metadata.setdefault("parse_warnings", []).extend(warnings)
        state["index"] += 1
        if state["index"] < len(state["pages"]):
            state["post"] = post.model_dump(mode="json")
            yield self._request(state["pages"][state["index"]], self.parse_comment_page,
                meta={"board_id_hint": post.board_id, "document_id_hint": post.document_id_raw,
                    "page_kind": "comment", "comment_state": state})
        else:
            if self.config.scrape_options.comment_mode == "best_only":
                post.comments = [comment for comment in post.comments if comment.is_best]
            max_comments = self.config.scrape_options.max_comments
            if max_comments is not None:
                post.comments = post.comments[:max_comments]
            yield post.model_dump(mode="json")

    async def on_scraped_item(self, item):
        if self.output_store is not None:
            try:
                receipt = await self.output_store.append_post(item)
            except Exception as exc:
                if not self.terminal_error:
                    self.terminal_error = True
                    self.pause()
                raise StorageError("output commit failed") from exc
            if receipt.durable:
                committed = receipt.committed_keys or (receipt.post_key,)
                self.completed_keys.update(committed)
                self.stored_count += len(committed)
        return item

    async def on_error(self, request, error):
        if isinstance(error, StorageError):
            raise error
        self._failure(request.meta.get("page_kind", "request"),
            type(error).__name__, request.url,
            request.meta.get("board_id_hint"), request.meta.get("document_id_hint"))


from datetime import datetime
import pickle
from types import SimpleNamespace

import anyio

from fmkorea_scraper.models import DirectPostSource, JobConfig
from fmkorea_scraper.models.job import SpiderRuntimeConfig
from fmkorea_scraper.spider import FMKoreaSpider
from fmkorea_scraper.spider.routing import BrowserIdentityBridge, BrowserIdentityCandidate


def test_blocked_attempt_keeps_proxy_then_rotates():
    async def exercise():
        config = JobConfig(job_id="test", job_type="direct_post",
            source=DirectPostSource(urls=["https://www.fmkorea.com/12345"]))
        runtime = SpiderRuntimeConfig(proxies=["http://user:secret@p1.test:8080",
            "http://user:secret@p2.test:8080"])
        spider = FMKoreaSpider(config, runtime)
        spider.pause = lambda: None
        original = spider._request("https://www.fmkorea.com/12345", spider.parse_detail)
        response = SimpleNamespace(status=403, body=b"blocked", request=original, meta={})
        assert await spider.is_blocked(response)
        browser_retry = original.copy()
        browser_retry._retry_count = 1
        browser = await spider.retry_blocked_request(browser_retry, response)
        restored = pickle.loads(pickle.dumps(browser))
        assert callable(restored._session_kwargs["page_action"])
        assert browser.sid == f"browser:{original.meta['proxy_id']}"
        assert browser.meta["proxy_attempt"] == 1
        assert browser.meta["blocked_attempt"] == browser._retry_count == 1
        assert "secret" not in str(browser.meta["proxy_log"])
        browser_response = SimpleNamespace(status=403, body=b"blocked", request=browser, meta={})
        assert await spider.is_blocked(browser_response)
        next_retry = browser.copy()
        next_retry._retry_count = 2
        next_http = await spider.retry_blocked_request(next_retry, browser_response)
        assert next_http.sid.startswith("http:")
        assert next_http.meta["proxy_id"] != original.meta["proxy_id"]
        assert next_http.meta["proxy_attempt"] == 2
        assert next_http.meta["blocked_attempt"] == next_http._retry_count == 2
        assert next_http.meta["proxy_log"][-1]["rotated"]

        final_retry = next_http.copy()
        final_retry._retry_count = 3
        final_browser = await spider.retry_blocked_request(final_retry,
            SimpleNamespace(status=403, body=b"blocked", request=next_http, meta={}))
        assert final_browser.meta["blocked_attempt"] == final_browser._retry_count == 3
        assert await spider.is_blocked(SimpleNamespace(status=403, body=b"blocked",
            request=final_browser, meta={}))
        assert spider.terminal_blocked

    anyio.run(exercise)


def test_browser_semantic_unavailable_overrides_block_status_only_in_error_ui():
    async def exercise():
        config = JobConfig(job_id="test", job_type="direct_post",
            source=DirectPostSource(urls=["https://www.fmkorea.com/12345"]))
        spider = FMKoreaSpider(config)
        http = spider._request("https://www.fmkorea.com/12345", spider.parse_detail,
            meta={"page_kind": "post"})
        browser = http.copy()
        browser.sid = "browser"
        error_body = b'<div class="message">This post is unavailable</div>'
        assert await spider.is_blocked(SimpleNamespace(status=403, body=error_body,
            request=http, meta={}, url=http.url))
        assert not await spider.is_blocked(SimpleNamespace(status=403, body=error_body,
            request=browser, meta={}, url=browser.url))
        article_body = b'<article class="xe_content">An unavailable post</article>'
        assert await spider.is_blocked(SimpleNamespace(status=403, body=article_body,
            request=browser, meta={}, url=browser.url))

    anyio.run(exercise)


def test_fmkorea_security_page_with_http_200_is_blocked():
    async def exercise():
        config = JobConfig(job_id="test", job_type="direct_post",
            source=DirectPostSource(urls=["https://www.fmkorea.com/12345"]))
        spider = FMKoreaSpider(config)
        request = spider._request("https://www.fmkorea.com/12345", spider.parse_detail,
            meta={"page_kind": "post"})
        body = ("<html><head><title>\uc5d0\ud3a8\ucf54\ub9ac\uc544 \ubcf4\uc548 \uc2dc\uc2a4\ud15c</title></head></html>").encode()
        response = SimpleNamespace(status=200, body=body, request=request,
            meta={}, url=request.url)
        assert await spider.is_blocked(response)

    anyio.run(exercise)


def test_browser_identity_promotes_only_fresh_success():
    bridge = BrowserIdentityBridge()
    cookies = [{"name": "session", "value": "abc", "domain": ".fmkorea.com"}]
    bridge.capture(BrowserIdentityCandidate("direct", 1, cookies,
        "Mozilla/5.0 Chrome/150.0", 150, datetime.now()))
    bridge.promote("direct", 1, True)
    assert bridge.http_overlay("direct") == {}
    bridge.capture(BrowserIdentityCandidate("direct", 2, cookies,
        "Mozilla/5.0 Chrome/150.0", 150, datetime.now()))
    bridge.promote("direct", 2, False)
    assert bridge.http_overlay("direct")["cookies"] == {"session": "abc"}
    bridge.capture(BrowserIdentityCandidate("direct", 1, [], "old", None, datetime.now()))
    bridge.promote("direct", 1, False)
    assert bridge.active["direct"].capture_seq == 2


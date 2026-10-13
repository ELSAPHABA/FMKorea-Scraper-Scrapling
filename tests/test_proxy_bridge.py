import asyncio
from types import SimpleNamespace

import pytest
from scrapling.spiders import Spider

from fmkorea_scraper.models import DirectPostSource, JobConfig
from fmkorea_scraper.models.job import SpiderRuntimeConfig
from fmkorea_scraper.spider import FMKoreaSpider
from fmkorea_scraper.spider.proxy_bridge import browser_proxy_bridges


def test_authenticated_socks5_bridge_connect_and_cleanup():
    async def exercise():
        async def socks(reader, writer):
            assert await reader.readexactly(3) == b"\x05\x01\x02"
            writer.write(b"\x05\x02")
            await writer.drain()
            assert await reader.readexactly(8) == b"\x01\x01u\x04p@ss"
            writer.write(b"\x01\x00")
            await writer.drain()
            assert await reader.readexactly(4) == b"\x05\x01\x00\x03"
            host_length = (await reader.readexactly(1))[0]
            assert await reader.readexactly(host_length + 2) == b"example.com\x01\xbb"
            writer.write(b"\x05\x00\x00\x01\x00\x00\x00\x00\x00\x00")
            await writer.drain()
            assert await reader.readexactly(4) == b"ping"
            writer.write(b"pong")
            await writer.drain()
            writer.close()

        upstream = await asyncio.start_server(socks, "127.0.0.1", 0)
        upstream_port = upstream.sockets[0].getsockname()[1]
        proxy = f"socks5://u:p%40ss@127.0.0.1:{upstream_port}"
        try:
            async with browser_proxy_bridges([proxy]) as mapped:
                assert set(mapped) == {proxy}
                host, port = mapped[proxy].removeprefix("http://").split(":")
                reader, writer = await asyncio.open_connection(host, int(port))
                writer.write(b"CONNECT example.com:443 HTTP/1.1\r\n\r\n")
                await writer.drain()
                assert await reader.readuntil(b"\r\n\r\n") == b"HTTP/1.1 200 Connection Established\r\n\r\n"
                writer.write(b"ping")
                await writer.drain()
                assert await reader.readexactly(4) == b"pong"
                writer.close()
                await writer.wait_closed()
        finally:
            upstream.close()
            await upstream.wait_closed()

    asyncio.run(exercise())


def test_browser_proxy_metadata_uses_transport_address_but_logs_upstream():
    async def exercise():
        proxy = "socks5://u:p@vpn.example:1080"
        config = JobConfig(job_id="test", job_type="direct_post",
            source=DirectPostSource(urls=["https://www.fmkorea.com/12345"]))
        spider = FMKoreaSpider(config, SpiderRuntimeConfig(proxies=[proxy]))
        spider.browser_proxies = {proxy: "http://127.0.0.1:12345"}
        request = spider._request("https://www.fmkorea.com/12345", spider.parse_detail)
        request.sid = "browser:proxy-0001"
        response = SimpleNamespace(status=200, body=b"ok", request=request,
            meta={"proxy": "http://127.0.0.1:12345"})
        assert not await spider.is_blocked(response)
        assert response.meta["proxy_log"][0]["endpoint"] == "socks5://vpn.example:1080"

    asyncio.run(exercise())


def test_spider_stream_configures_browser_with_bridge(monkeypatch):
    proxy = "socks5://u:p@127.0.0.1:1080"
    config = JobConfig(job_id="test", job_type="direct_post",
        source=DirectPostSource(urls=["https://www.fmkorea.com/12345"]))
    spider = FMKoreaSpider(config, SpiderRuntimeConfig(proxies=[proxy]))

    async def fake_stream(self):
        browser = self._session_manager.get("browser:proxy-0001")
        http = self._session_manager.get("http:proxy-0001")
        assert browser._config.proxy["server"].startswith("http://127.0.0.1:")
        assert http._default_proxy == proxy
        if False:
            yield {}

    monkeypatch.setattr(Spider, "stream", fake_stream)

    async def exercise():
        async for _ in spider.stream():
            pass

    asyncio.run(exercise())


@pytest.mark.parametrize("code", [10013, 10048])
def test_windows_reserved_port_bind_is_retried(monkeypatch, code):
    start_server = asyncio.start_server
    attempts = 0

    async def start_with_collisions(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OSError(code, "bind failed")
        return await start_server(*args, **kwargs)

    monkeypatch.setattr(asyncio, "start_server", start_with_collisions)

    async def exercise():
        async with browser_proxy_bridges(["socks5://u:p@vpn.example:1080"]) as mapped:
            assert mapped["socks5://u:p@vpn.example:1080"].startswith("http://127.0.0.1:")
        assert attempts == 3

    asyncio.run(exercise())

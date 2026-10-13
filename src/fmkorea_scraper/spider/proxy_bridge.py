import asyncio
from contextlib import asynccontextmanager
from urllib.parse import unquote, urlparse, urlsplit


async def _relay(source, destination):
    while data := await source.read(65536):
        destination.write(data)
        await destination.drain()


async def _handle(client_reader, client_writer, upstream):
    remote_writer = None
    try:
        header = await client_reader.readuntil(b"\r\n\r\n")
        line, rest = header.split(b"\r\n", 1)
        method, target, version = line.split(b" ", 2)
        destination = urlsplit("//" + target.decode("ascii")) if method == b"CONNECT" else urlsplit(target.decode("ascii"))
        host = destination.hostname
        port = destination.port or (443 if method == b"CONNECT" else 80)
        if not host or len(host.encode("idna")) > 255:
            return
        remote_reader, remote_writer = await asyncio.open_connection(upstream.hostname, upstream.port)
        remote_writer.write(b"\x05\x01\x02")
        await remote_writer.drain()
        if await remote_reader.readexactly(2) != b"\x05\x02":
            return
        username = unquote(upstream.username or "").encode()
        password = unquote(upstream.password or "").encode()
        if len(username) > 255 or len(password) > 255:
            return
        remote_writer.write(b"\x01" + bytes([len(username)]) + username + bytes([len(password)]) + password)
        await remote_writer.drain()
        if await remote_reader.readexactly(2) != b"\x01\x00":
            return
        encoded_host = host.encode("idna")
        remote_writer.write(b"\x05\x01\x00\x03" + bytes([len(encoded_host)]) + encoded_host + port.to_bytes(2, "big"))
        await remote_writer.drain()
        reply = await remote_reader.readexactly(4)
        if reply[:2] != b"\x05\x00":
            return
        address_length = {1: 4, 4: 16}.get(reply[3])
        if address_length is None:
            address_length = (await remote_reader.readexactly(1))[0]
        await remote_reader.readexactly(address_length + 2)
        if method == b"CONNECT":
            client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        else:
            path = (destination.path or "/") + ("?" + destination.query if destination.query else "")
            client_writer.write(method + b" " + path.encode("ascii") + b" " + version + b"\r\n" + rest)
        await client_writer.drain()
        tasks = [asyncio.create_task(_relay(client_reader, remote_writer)),
                 asyncio.create_task(_relay(remote_reader, client_writer))]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    except (OSError, ValueError, UnicodeError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        pass
    finally:
        client_writer.close()
        if remote_writer is not None:
            remote_writer.close()


@asynccontextmanager
async def browser_proxy_bridges(proxies: list[str]):
    """Expose authenticated SOCKS5 proxies to Chromium through loopback HTTP proxies."""
    servers = []
    connections = set()
    browser_proxies = {}
    try:
        for proxy in proxies:
            upstream = urlparse(proxy)
            if upstream.scheme != "socks5" or upstream.username is None:
                continue
            if not upstream.hostname or not upstream.port:
                raise ValueError("invalid SOCKS5 proxy endpoint")

            async def handle(reader, writer, upstream=upstream):
                task = asyncio.current_task()
                connections.add(task)
                try:
                    await _handle(reader, writer, upstream)
                finally:
                    connections.discard(task)

            for attempt in range(5):
                try:
                    server = await asyncio.start_server(handle, "127.0.0.1", 0)
                    break
                except OSError as exc:
                    if attempt == 4 or (getattr(exc, "winerror", None) or exc.errno) not in {10013, 10048}:
                        raise
            servers.append(server)
            browser_proxies[proxy] = f"http://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        yield browser_proxies
    finally:
        for server in servers:
            server.close()
        for server in servers:
            await server.wait_closed()
        for task in tuple(connections):
            task.cancel()
        await asyncio.gather(*connections, return_exceptions=True)

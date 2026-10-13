import argparse
import json
import signal
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, urlparse, urlunparse

import anyio
from dotenv import dotenv_values
from pydantic import ValidationError

from fmkorea_scraper.application.jobs import JobRunner
from fmkorea_scraper.application.resume import resume_job_async
from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.job import (BoardListSource, BoardSearchSource, CrawlConfig,
    DeepSearchSource, DirectPostSource, IntegratedSearchSource, JobConfig,
    NoKeywordRangeSource, RangeFilter, RealtimeAllSource, RealtimeKeywordSource,
    RealtimeTermination, ScrapeOptions, SpiderRuntimeConfig, StorageConfig)


SOURCES = ("direct-post", "board-list", "board-search", "integrated-search",
    "deep-search", "no-keyword-range", "realtime-keyword", "realtime-all")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="fmkorea-scraper")
    root.add_argument("--debug", action="store_true")
    commands = root.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--source", choices=SOURCES, required=True)
    run.add_argument("--job-id")
    run.add_argument("--url", action="append")
    run.add_argument("--board-id")
    run.add_argument("--category-id")
    run.add_argument("--keyword")
    run.add_argument("--search-target", default="title_content")
    run.add_argument("--page-start", type=int, default=1)
    run.add_argument("--page-end", type=int)
    run.add_argument("--seed-page-limit", type=int, default=20)
    run.add_argument("--board-candidate", action="append")
    run.add_argument("--date-from")
    run.add_argument("--date-to")
    run.add_argument("--min-document-srl", "--min-post-number", type=int)
    run.add_argument("--max-document-srl", "--max-post-number", type=int)
    run.add_argument("--scope", choices=["board", "integrated"])
    run.add_argument("--poll-interval-seconds", type=float, default=30.0)
    end = run.add_mutually_exclusive_group()
    end.add_argument("--duration-seconds", type=int)
    end.add_argument("--end-at")
    _common(run)
    resume = commands.add_parser("resume")
    resume.add_argument("--job-id")
    _common(resume)
    retry = commands.add_parser("retry-parser-failures")
    retry.add_argument("--failure-path", required=True)
    retry.add_argument("--existing-success-path")
    from_source = retry.add_mutually_exclusive_group()
    from_source.add_argument("--from-report")
    from_source.add_argument("--from-checkpoint")
    retry.add_argument("--retry-job-id")
    retry.add_argument("--job-id")
    retry.add_argument("--stage", action="append")
    retry.add_argument("--skip-existing-success", dest="skip_existing_success", action="store_true", default=True)
    retry.add_argument("--no-skip-existing-success", dest="skip_existing_success", action="store_false")
    _common(retry)
    proxy = commands.add_parser("configure-proxy")
    proxy.add_argument("--host", action="append")
    proxy.add_argument("--id")
    proxy.add_argument("--password")
    commands.add_parser("wizard")
    return root


def _common(command):
    command.add_argument("--output-path", default=".")
    command.add_argument("--comment-mode", choices=["none", "all", "best_only"], default="all")
    command.add_argument("--media-mode", choices=["url_only", "download"], default="url_only")
    command.add_argument("--include-attachments", dest="include_attachments", action="store_true", default=True)
    command.add_argument("--no-attachments", dest="include_attachments", action="store_false")
    command.add_argument("--download-delay", type=float, default=1.0)
    command.add_argument("--concurrent-requests", type=int, default=4)
    command.add_argument("--concurrent-requests-per-domain", type=int, default=2)
    command.add_argument("--max-blocked-retries", type=int, default=3)
    command.add_argument("--autothrottle", dest="autothrottle", action="store_true", default=False)
    command.add_argument("--no-autothrottle", dest="autothrottle", action="store_false")
    command.add_argument("--autothrottle-start-delay", type=float, default=2.0)
    command.add_argument("--autothrottle-max-delay", type=float, default=30.0)
    command.add_argument("--browser-max-pages", type=int, default=2)
    command.add_argument("--media-download-concurrency", type=int, default=2)
    command.add_argument("--checkpoint-interval-seconds", type=float, default=300.0)
    command.add_argument("--robots-txt-obey", action="store_true")
    command.add_argument("--http-timeout-seconds", type=float, default=30.0)
    command.add_argument("--http-transport-retries", type=int, default=3)
    command.add_argument("--browser-timeout-ms", type=int, default=30000)
    command.add_argument("--browser-transport-retries", type=int, default=3)
    command.add_argument("--max-comment-pages", type=int)
    command.add_argument("--max-comments", type=int)
    command.add_argument("--max-content-blocks", type=int)
    command.add_argument("--proxy-host", action="append")
    command.add_argument("--proxy-id")
    command.add_argument("--proxy-pw")


def _proxies(args) -> list[str]:
    settings = dotenv_values(Path.cwd() / ".env")
    raw = args.proxy_host if args.proxy_host else [settings.get("FMKOREA_PROXY_HOST") or ""]
    entries = [part.strip() for value in raw for part in value.split(";")]
    if entries == [""]:
        return []
    if not all(entries):
        raise ValueError("empty proxy in pool")
    user = args.proxy_id if args.proxy_id is not None else settings.get("FMKOREA_PROXY_ID")
    password = args.proxy_pw if args.proxy_pw is not None else settings.get("FMKOREA_PROXY_PW")
    proxies = []
    for value in entries:
        parsed = urlparse(value if "://" in value else f"http://{value}")
        if not parsed.hostname or not parsed.port:
            raise ValueError("invalid proxy endpoint")
        if parsed.username is None and user:
            authority = f"{quote(user, safe='')}:{quote(password or '', safe='')}@{parsed.hostname}:{parsed.port}"
            parsed = parsed._replace(netloc=authority)
        proxies.append(urlunparse(parsed))
    if len(proxies) != len(set(proxies)):
        raise ValueError("duplicate proxy")
    return proxies


def _runtime(args):
    return SpiderRuntimeConfig(http_timeout_seconds=args.http_timeout_seconds,
        browser_timeout_ms=args.browser_timeout_ms,
        http_transport_retries=args.http_transport_retries,
        browser_transport_retries=args.browser_transport_retries,
        proxies=_proxies(args), execution_dir=Path.cwd())


def _job(args):
    kind = args.source.replace("-", "_")
    bounds = {name: getattr(args, name) for name in
        ("date_from", "date_to", "min_document_srl", "max_document_srl")}
    filter_ = RangeFilter(**bounds) if any(value is not None for value in bounds.values()) else None
    if kind == "direct_post":
        source = DirectPostSource(urls=args.url or [])
    elif kind == "board_list":
        source = BoardListSource(board_id=args.board_id, category_id=args.category_id,
            page_start=args.page_start, page_end=args.page_end, range_filter=filter_)
    elif kind == "board_search":
        source = BoardSearchSource(board_id=args.board_id, category_id=args.category_id,
            keyword=args.keyword, search_target=args.search_target,
            page_start=args.page_start, page_end=args.page_end, range_filter=filter_)
    elif kind == "integrated_search":
        source = IntegratedSearchSource(keyword=args.keyword, search_target=args.search_target,
            page_start=args.page_start, page_end=args.page_end, range_filter=filter_)
    elif kind == "deep_search":
        source = DeepSearchSource(keyword=args.keyword, seed_search_target=args.search_target,
            seed_page_limit=args.seed_page_limit,
            board_ids=[*([args.board_id] if args.board_id else []), *(args.board_candidate or [])],
            range_filter=filter_ or RangeFilter())
    elif kind == "no_keyword_range":
        source = NoKeywordRangeSource(board_id=args.board_id, category_id=args.category_id,
            range_filter=filter_ or RangeFilter())
    else:
        termination = RealtimeTermination(duration_seconds=args.duration_seconds,
            end_at=args.end_at)
        if kind == "realtime_keyword":
            if args.scope == "integrated" and args.category_id:
                raise ValueError("category_id not valid for integrated realtime search")
            source = RealtimeKeywordSource(scope=args.scope, board_id=args.board_id,
                keyword=args.keyword, search_target=args.search_target,
                poll_interval_seconds=args.poll_interval_seconds, termination=termination)
        else:
            source = RealtimeAllSource(board_id=args.board_id, category_id=args.category_id,
                poll_interval_seconds=args.poll_interval_seconds, termination=termination)
    options = ScrapeOptions(comment_mode=args.comment_mode, media_mode=args.media_mode,
        include_attachments=args.include_attachments,
        media_download_concurrency=args.media_download_concurrency,
        max_comment_pages=args.max_comment_pages, max_comments=args.max_comments,
        max_content_blocks=args.max_content_blocks)
    crawl = CrawlConfig(concurrent_requests=args.concurrent_requests,
        concurrent_requests_per_domain=args.concurrent_requests_per_domain,
        download_delay=args.download_delay, max_blocked_retries=args.max_blocked_retries,
        autothrottle_enabled=args.autothrottle,
        autothrottle_start_delay=args.autothrottle_start_delay,
        autothrottle_max_delay=args.autothrottle_max_delay,
        browser_max_pages=args.browser_max_pages,
        checkpoint_interval_seconds=args.checkpoint_interval_seconds,
        robots_txt_obey=args.robots_txt_obey)
    return JobConfig(job_id=args.job_id or f"job_{datetime.now(SEOUL):%Y%m%d_%H%M%S}",
        job_type=kind, source=source, scrape_options=options, crawl_config=crawl,
        storage_config=StorageConfig(base_output_path=args.output_path))


def _configure_proxy(args):
    hosts = args.host or [input("Proxy URL: ").strip()]
    user = args.id if args.id is not None else input("Proxy username (optional): ").strip()
    password = args.password if args.password is not None else input("Proxy password (optional): ").strip()
    _proxies(SimpleNamespace(proxy_host=hosts, proxy_id=user, proxy_pw=password))
    file = Path.cwd() / ".env"
    lines = file.read_text(encoding="utf-8").splitlines() if file.exists() else []
    lines = [line for line in lines if not line.startswith(("FMKOREA_PROXY_HOST=", "FMKOREA_PROXY_ID=", "FMKOREA_PROXY_PW="))]
    values = {"FMKOREA_PROXY_HOST": ";".join(hosts), "FMKOREA_PROXY_ID": user, "FMKOREA_PROXY_PW": password}
    from dotenv import set_key
    file.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    for key, value in values.items():
        set_key(str(file), key, value)


def _wizard():
    options = ["run", "resume", "retry-parser-failures", "configure-proxy"]
    for index, name in enumerate(options, 1):
        print(f"{index}. {name}")
    command = options[int(input("번호: ")) - 1]
    if command == "run":
        for index, name in enumerate(SOURCES, 1):
            print(f"{index}. {name}")
        source = SOURCES[int(input("source 번호: ")) - 1]
        values = ["run", "--source", source]
        if source == "direct-post":
            values.extend(["--url", input("게시글 URL: ").strip()])
        elif source in {"board-list", "no-keyword-range", "realtime-all", "board-search"}:
            values.extend(["--board-id", input("board ID: ").strip()])
        if source in {"board-search", "integrated-search", "deep-search", "realtime-keyword"}:
            values.extend(["--keyword", input("검색어: ").strip()])
        if source == "realtime-keyword":
            scope = input("scope (board/integrated): ").strip()
            values.extend(["--scope", scope])
            if scope == "board":
                values.extend(["--board-id", input("board ID: ").strip()])
        if source.startswith("realtime-"):
            values.extend(["--duration-seconds", input("실행 시간(초): ").strip()])
        if source in {"deep-search", "no-keyword-range"}:
            values.extend(["--date-from", input("시작일 YYYY-MM-DD: ").strip()])
        return values
    if command == "resume":
        return ["resume"]
    if command == "retry-parser-failures":
        return ["retry-parser-failures", "--failure-path", input("failures.jsonl 경로: ").strip()]
    return ["configure-proxy"]


async def _run(args):
    if args.command == "run":
        runner = JobRunner(_job(args), _runtime(args), debug=args.debug)
        previous_handler = signal.getsignal(signal.SIGINT)
        interrupts = 0

        def interrupt(signum, frame):
            nonlocal interrupts
            interrupts += 1
            if interrupts == 1:
                runner.stop()
            else:
                raise KeyboardInterrupt

        signal.signal(signal.SIGINT, interrupt)
        try:
            return await runner.run()
        finally:
            signal.signal(signal.SIGINT, previous_handler)
    if args.command == "resume":
        return await resume_job_async(args.job_id, args.output_path, _runtime(args))
    if args.command == "retry-parser-failures":
        from fmkorea_scraper.retry.parser_failures import retry_parser_failures_async
        return await retry_parser_failures_async(args)
    raise ValueError("unknown command")


def main(argv=None) -> int:
    command = parser()
    args = command.parse_args(argv)
    if args.command == "wizard":
        return main(_wizard())
    if args.command == "configure-proxy":
        try:
            _configure_proxy(args)
            return 0
        except ValueError as exc:
            print(f"invalid input: {exc}", file=sys.stderr)
            return 2
    try:
        report = anyio.run(_run, args)
        print(json.dumps(report.model_dump(mode="json"), ensure_ascii=False))
        return 0 if report.status == "completed" else 1
    except (ValidationError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"runtime error: {type(exc).__name__}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

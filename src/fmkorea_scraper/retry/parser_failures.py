import json
from datetime import datetime
from pathlib import Path

from fmkorea_scraper.application.jobs import JobRunner
from fmkorea_scraper.collect.urls import allowed_post_url, document_id
from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.checkpoint import Checkpoint
from fmkorea_scraper.models.job import CrawlConfig, JobConfig, ParserFailureRetrySource, ScrapeOptions, StorageConfig
from fmkorea_scraper.models.report import RetryReport


def _success_keys(path: str | None) -> set[tuple[str, str, str]]:
    if not path:
        return set()
    result = set()
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            if line.strip():
                item = json.loads(line)
                result.add((item["site_id"], item["board_id"], item["document_id_raw"]))
    return result


async def retry_parser_failures_async(args):
    stages = set(args.stage or ["post"])
    if stages != {"post"}:
        raise ValueError("only parser-stage post failures can be retried")
    original = None
    if args.from_checkpoint:
        with Path(args.from_checkpoint).open(encoding="utf-8") as stream:
            data = json.load(stream)
        if data.get("schema_version") == "fmkorea.local_checkpoint.v1":
            values = data["checkpoints"]
            original = Checkpoint.model_validate(values.get(args.job_id) or max(values.values(), key=lambda x: x["updated_at"])).job_config
        else:
            original = Checkpoint.model_validate(data).job_config
    elif args.from_report:
        with Path(args.from_report).open(encoding="utf-8") as stream:
            report = json.load(stream)
        if "job_config" in report.get("metadata", {}):
            original = JobConfig.model_validate(report["metadata"]["job_config"])
    completed = _success_keys(args.existing_success_path) if args.skip_existing_success else set()
    urls = []
    seen = set()
    selected = skipped = 0
    with Path(args.failure_path).open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            failure = json.loads(line)
            if failure.get("stage") not in stages or args.job_id and failure.get("job_id") != args.job_id:
                skipped += 1
                continue
            url = failure.get("url")
            key = failure.get("post_key") or {}
            raw_id = key.get("document_id_raw") or (document_id(url) if url else None)
            post_key = ("fmkorea", key.get("board_id") or "", raw_id)
            if not url or not raw_id or not allowed_post_url(url) or url in seen or post_key in completed:
                skipped += 1
                continue
            selected += 1
            seen.add(url)
            urls.append(url)
    if not urls:
        return RetryReport(job_id=args.retry_job_id or f"retry_{datetime.now(SEOUL):%Y%m%d_%H%M%S}",
            status="completed", started_at=datetime.now(SEOUL), finished_at=datetime.now(SEOUL),
            selected_count=0, skipped_count=skipped)
    source = ParserFailureRetrySource(failure_input=args.failure_path,
        skip_existing_success=args.skip_existing_success)
    config = JobConfig(job_id=args.retry_job_id or f"retry_{datetime.now(SEOUL):%Y%m%d_%H%M%S}",
        job_type="parser_failure_retry", source=source,
        scrape_options=original.scrape_options if original else ScrapeOptions(),
        crawl_config=original.crawl_config if original else CrawlConfig(),
        storage_config=getattr(args, "storage_config", None) or StorageConfig(base_output_path=args.output_path))
    report = await JobRunner(config, getattr(args, "runtime", None), retry_urls=urls).run()
    return RetryReport.model_validate({**report.model_dump(mode="json"),
        "selected_count": selected, "skipped_count": skipped})

from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import anyio

from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.checkpoint import Checkpoint
from fmkorea_scraper.models.job import JobConfig, SpiderRuntimeConfig
from fmkorea_scraper.models.job import DirectPostSource
from fmkorea_scraper.models.report import JobReport, StoredObjectRef
from fmkorea_scraper.outputs.local import LocalCheckpointStore, LocalOutputStore
from fmkorea_scraper.spider import FMKoreaSpider


class JobRunner:
    def __init__(self, config: JobConfig, runtime: SpiderRuntimeConfig | None = None,
                 checkpoint: Checkpoint | None = None, run_id: str | None = None,
                 debug: bool = False, retry_urls: list[str] | None = None):
        self.config = config
        self.runtime = runtime or SpiderRuntimeConfig()
        self.previous = checkpoint
        self.run_id = run_id or f"run-{datetime.now(SEOUL):%Y%m%d-%H%M%S}-{uuid4().hex[:6]}"
        self.stop_event = anyio.Event()
        self.spider = None
        self.debug = debug
        self.retry_urls = retry_urls

    def stop(self):
        if not self.stop_event.is_set():
            self.stop_event.set()
            if self.spider is not None:
                self.spider.pause()

    async def run(self) -> JobReport:
        config = self.config
        spider_config = config
        if config.source.type == "parser_failure_retry":
            urls = self.retry_urls
            if urls is None:
                with Path(config.source.failure_input).open(encoding="utf-8") as stream:
                    urls = [item["url"] for line in stream if line.strip()
                        if (item := json.loads(line)).get("stage") == "post" and item.get("url")]
            if not urls:
                raise ValueError("no parser-stage post failures to retry")
            spider_config = JobConfig.model_validate({**config.model_dump(mode="json"),
                "job_type": "direct_post", "source": DirectPostSource(urls=urls).model_dump(mode="json")})
        base = Path(config.storage_config.base_output_path)
        if config.storage_config.job_checkpoint_mode == "local":
            checkpoints = LocalCheckpointStore(base)
        else:
            from fmkorea_scraper.outputs.postgres_job_checkpoint import PostgresJobCheckpointStore
            checkpoints = PostgresJobCheckpointStore(config.storage_config.postgres.dsn)
        completed = set(self.previous.completed_keys) if self.previous else set()
        already_completed = completed.copy()
        started = datetime.now(SEOUL)
        if config.storage_config.output_mode == "local":
            output = LocalOutputStore(base, config.job_id, self.run_id, started)
        else:
            from fmkorea_scraper.outputs.s3_output import S3OutputStore
            output = S3OutputStore(config.storage_config, self.runtime, config.job_id,
                self.run_id, started)
        deadline = self.previous.realtime_deadline_at if self.previous else None
        if config.source.type.startswith("realtime_") and deadline is None:
            termination = config.source.termination
            deadline = termination.end_at or started + timedelta(seconds=termination.duration_seconds)
        crawldir = None
        if not config.source.type.startswith("realtime_"):
            if config.storage_config.output_mode == "local":
                crawldir = base.resolve() / ".crawl" / config.job_id
            elif self.runtime.persistent_state_root:
                crawldir = self.runtime.persistent_state_root.resolve() / "crawl" / config.job_id
        if crawldir:
            crawldir.mkdir(parents=True, exist_ok=True)
            map_path = crawldir / "proxy-map.json"
            fingerprints = [hashlib.sha256(proxy.encode()).hexdigest() for proxy in self.runtime.proxies]
            existing = [path for path in crawldir.iterdir() if path.name != "proxy-map.json"]
            if map_path.exists():
                if json.loads(map_path.read_text(encoding="utf-8")) != fingerprints and existing:
                    raise ValueError("proxy pool changed during exact resume")
            elif existing:
                raise ValueError("proxy mapping missing for exact resume")
            map_path.write_text(json.dumps(fingerprints), encoding="utf-8")
        checkpoint = Checkpoint(job_id=config.job_id, job_config=config,
            completed_keys=sorted(completed), active_run_id=self.run_id,
            staging_ref=str(output.staging),
            enrichment_status="pending" if config.scrape_options.media_mode == "download" else "not_requested",
            realtime_deadline_at=deadline,
            crawl_state_ref=str(crawldir.relative_to(self.runtime.persistent_state_root.resolve() if self.runtime.persistent_state_root and config.storage_config.output_mode != "local" else base.resolve())) if crawldir else None,
            updated_at=started)
        checkpoints.save(checkpoint)
        spider = FMKoreaSpider(spider_config, self.runtime, output_store=output,
            completed_keys=completed, stop_event=self.stop_event,
            deadline=deadline, crawldir=crawldir, debug=self.debug)
        self.spider = spider
        reason = None
        flush_stop = anyio.Event()

        async def periodic_flush():
            while not flush_stop.is_set():
                with anyio.move_on_after(config.storage_config.s3_flush_seconds):
                    await flush_stop.wait()
                if flush_stop.is_set():
                    break
                keys = await output.flush_pending()
                if keys:
                    completed.update(keys)
                    checkpoint.completed_keys = sorted(completed)
                    checkpoint.updated_at = datetime.now(SEOUL)
                    checkpoints.save(checkpoint)

        try:
            async with anyio.create_task_group() as tasks:
                if config.storage_config.output_mode == "s3_compatible":
                    tasks.start_soon(periodic_flush)
                try:
                    if not deadline or datetime.now(SEOUL) < deadline or not config.source.type.startswith("realtime_"):
                        async for _ in spider.stream():
                            checkpoint.completed_keys = sorted(completed)
                            checkpoint.updated_at = datetime.now(SEOUL)
                            checkpoints.save(checkpoint)
                finally:
                    flush_stop.set()
            keys = await output.flush_pending()
            if keys:
                completed.update(keys)
                checkpoint.completed_keys = sorted(completed)
                checkpoint.updated_at = datetime.now(SEOUL)
                checkpoints.save(checkpoint)
            if spider.terminal_blocked:
                reason = "blocked"
            elif self.stop_event.is_set():
                reason = "manual_stop"
        except Exception:
            reason = "error"
            raise
        finally:
            output.write_failures(spider.failures)
            checkpoint.completed_keys = sorted(completed)
            checkpoint.updated_at = datetime.now(SEOUL)
            if reason:
                checkpoint.status = "stopped"
                checkpoint.reason = reason
            checkpoints.save(checkpoint)
        try:
            if config.scrape_options.media_mode == "download":
                from .media_enrichment import enrich_local_media
                checkpoint.enrichment_status = "running"
                checkpoints.save(checkpoint)
                output_path, downloaded, failed = await enrich_local_media(output,
                    config.scrape_options.media_download_concurrency)
                checkpoint.enrichment_status = "completed_with_warnings" if failed else "completed"
            else:
                output_path = await output.finalize()
                downloaded = failed = 0
        except Exception:
            if config.scrape_options.media_mode == "download":
                checkpoint.enrichment_status = "failed"
            checkpoint.status = "stopped"
            checkpoint.reason = "error"
            checkpoint.updated_at = datetime.now(SEOUL)
            checkpoints.save(checkpoint)
            raise
        checkpoint.enrichment_status = checkpoint.enrichment_status or "not_requested"
        report = JobReport(job_id=config.job_id, status="stopped" if reason else "completed",
            started_at=started, finished_at=datetime.now(SEOUL),
            collected_count=spider.collected_count, stored_count=len(completed - already_completed),
            unavailable_count=len(spider.unavailable_keys), failure_count=len(spider.failures),
            media_downloaded_count=downloaded, media_failure_count=failed,
            enrichment_status=checkpoint.enrichment_status,
            output_refs=[StoredObjectRef(storage_mode=config.storage_config.output_mode,
                path=output_path if config.storage_config.output_mode == "local" else None,
                bucket=output.bucket if config.storage_config.output_mode != "local" else None,
                object_key=output_path if config.storage_config.output_mode != "local" else None),
                StoredObjectRef(storage_mode=config.storage_config.output_mode,
                    path=str(output.failures) if config.storage_config.output_mode == "local" else None,
                    bucket=output.bucket if config.storage_config.output_mode != "local" else None,
                    object_key=output._key("failures.jsonl") if config.storage_config.output_mode != "local" else None)])
        try:
            output.write_report(report)
        except Exception:
            checkpoint.status = "stopped"
            checkpoint.reason = "error"
            checkpoint.updated_at = datetime.now(SEOUL)
            checkpoints.save(checkpoint)
            raise
        checkpoint.status = report.status
        checkpoint.reason = reason
        checkpoint.active_run_id = None
        checkpoint.staging_ref = None
        checkpoint.updated_at = datetime.now(SEOUL)
        checkpoints.save(checkpoint)
        return report


async def run_job_async(config: JobConfig, runtime: SpiderRuntimeConfig | None = None,
                        run_id: str | None = None) -> JobReport:
    return await JobRunner(config, runtime, run_id=run_id).run()


def run_job(config: JobConfig, runtime: SpiderRuntimeConfig | None = None,
            run_id: str | None = None) -> JobReport:
    return anyio.run(run_job_async, config, runtime, run_id)

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import anyio

from fmkorea_scraper.application.jobs import JobRunner
from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.job import (JobConfig, PostgresJobCheckpointConfig,
    S3StorageConfig, SpiderRuntimeConfig, StorageConfig)
from fmkorea_scraper.models.report import JobReport, StoredObjectRef
from fmkorea_scraper.outputs.postgres_job_checkpoint import PostgresJobCheckpointStore
from fmkorea_scraper.outputs.s3_output import S3OutputStore

from .cli import _job, _proxies, _runtime, parser


def _true(name: str) -> bool:
    value = os.environ.get(name, "false").lower()
    if value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
        raise ValueError(f"invalid boolean environment variable: {name}")
    return value in {"1", "true", "yes", "on"}


def _storage() -> StorageConfig:
    required = ("DATABASE_URL", "S3_ENDPOINT_URL", "S3_BUCKET", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("missing container environment: " + ", ".join(missing))
    return StorageConfig(output_mode="s3_compatible", job_checkpoint_mode="postgres",
        s3=S3StorageConfig(endpoint_url=os.environ["S3_ENDPOINT_URL"],
            bucket=os.environ["S3_BUCKET"], prefix=os.environ.get("S3_PREFIX"),
            region=os.environ.get("S3_REGION")),
        postgres=PostgresJobCheckpointConfig(dsn=os.environ["DATABASE_URL"]))


def _container_runtime(args) -> SpiderRuntimeConfig:
    if os.environ.get("FMKOREA_PROXY_HOST"):
        args.proxy_host = [os.environ["FMKOREA_PROXY_HOST"]]
        args.proxy_id = os.environ.get("FMKOREA_PROXY_ID")
        args.proxy_pw = os.environ.get("FMKOREA_PROXY_PW")
    runtime = _runtime(args)
    root = os.environ.get("FMKOREA_CRAWDIR")
    if root:
        path = Path(root).resolve()
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path):
            pass
        runtime.persistent_state_root = path
    return runtime


async def _resume(storage: StorageConfig, runtime: SpiderRuntimeConfig, job_id: str | None):
    checkpoints = PostgresJobCheckpointStore(storage.postgres.dsn)
    previous = checkpoints.load(job_id) if job_id else checkpoints.load_latest()
    if previous is None:
        raise ValueError("checkpoint not found")
    if previous.status == "completed":
        raise ValueError("job already completed")
    previous.job_config = JobConfig.model_validate({**previous.job_config.model_dump(mode="json"),
        "storage_config": storage.model_dump(mode="json")})
    if previous.active_run_id:
        old = S3OutputStore(storage, runtime, previous.job_id, previous.active_run_id,
            previous.updated_at)
        committed = old.recover_committed_keys()
        committed.update(await old.flush_pending())
        previous.completed_keys = sorted(set(previous.completed_keys) | committed)
        if previous.job_config.scrape_options.media_mode == "download":
            from fmkorea_scraper.application.media_enrichment import enrich_local_media
            output_key, downloaded, failed = await enrich_local_media(old,
                previous.job_config.scrape_options.media_download_concurrency)
        else:
            output_key = await old.finalize()
            downloaded = failed = 0
        previous.active_run_id = None
        previous.staging_ref = None
        previous.updated_at = datetime.now(SEOUL)
        checkpoints.save(previous)
        if previous.realtime_deadline_at and datetime.now(SEOUL) >= previous.realtime_deadline_at:
            return JobReport(job_id=previous.job_id, status="completed",
                started_at=previous.job_config.created_at, finished_at=datetime.now(SEOUL),
                stored_count=len(committed), media_downloaded_count=downloaded,
                media_failure_count=failed,
                enrichment_status="completed_with_warnings" if failed else "completed",
                output_refs=[StoredObjectRef(storage_mode="s3_compatible",
                    bucket=old.bucket, object_key=output_key)])
    if previous.realtime_deadline_at and datetime.now(SEOUL) >= previous.realtime_deadline_at:
        previous.status = "completed"
        previous.reason = None
        previous.updated_at = datetime.now(SEOUL)
        checkpoints.save(previous)
        return JobReport(job_id=previous.job_id, status="completed",
            started_at=previous.job_config.created_at, finished_at=datetime.now(SEOUL),
            stored_count=len(previous.completed_keys), enrichment_status=previous.enrichment_status)
    return await JobRunner(previous.job_config, runtime, previous).run()


async def _run(argv):
    storage = _storage()
    if _true("RESUME"):
        args = parser().parse_args(["resume"])
        return await _resume(storage, _container_runtime(args), os.environ.get("JOB_ID"))
    args = parser().parse_args(argv)
    if args.command == "run":
        config = _job(args)
        config = JobConfig.model_validate({**config.model_dump(mode="json"),
            "storage_config": storage.model_dump(mode="json")})
        return await JobRunner(config, _container_runtime(args)).run()
    if args.command == "retry-parser-failures":
        from fmkorea_scraper.retry.parser_failures import retry_parser_failures_async
        runtime = _container_runtime(args)
        args.storage_config = storage
        args.runtime = runtime
        if not Path(args.failure_path).exists():
            output = S3OutputStore(storage, runtime, args.retry_job_id or "retry",
                "input", datetime.now(SEOUL))
            body = output.client.get_object(Bucket=output.bucket, Key=args.failure_path)["Body"].read()
            input_path = output.root / "failures-input.jsonl"
            input_path.write_bytes(body)
            args.failure_path = str(input_path)
        return await retry_parser_failures_async(args)
    raise ValueError("container supports run, retry-parser-failures or RESUME=true")


def main(argv=None) -> int:
    try:
        report = anyio.run(_run, sys.argv[1:] if argv is None else argv)
        print(json.dumps({"ok": True, "report": report.model_dump(mode="json")}, ensure_ascii=False))
        return 0 if report.status == "completed" else 1
    except Exception as exc:
        print(json.dumps({"ok": False, "error": {"stage": "container",
            "reason": type(exc).__name__, "retryable": False,
            "safe_message": "container job failed"}}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

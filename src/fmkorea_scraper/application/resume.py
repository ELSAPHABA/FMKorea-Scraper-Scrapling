from datetime import datetime
from pathlib import Path

from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.job import SpiderRuntimeConfig
from fmkorea_scraper.models.report import JobReport, StoredObjectRef
from fmkorea_scraper.outputs.local import LocalCheckpointStore, LocalOutputStore

from .jobs import JobRunner


async def resume_job_async(job_id: str | None = None, output_path: str = ".",
                           runtime: SpiderRuntimeConfig | None = None, run_id: str | None = None):
    store = LocalCheckpointStore(output_path)
    previous = store.load(job_id) if job_id else store.load_latest()
    if previous is None:
        raise ValueError("checkpoint not found")
    if previous.status == "completed":
        raise ValueError("job already completed")
    if previous.active_run_id and previous.staging_ref:
        old_run = previous.active_run_id
        output = LocalOutputStore(output_path, previous.job_id, old_run, previous.updated_at)
        if output.staging.exists():
            records = output.read_staging()
            previous.completed_keys = sorted(set(previous.completed_keys) | {
                (item["site_id"], item["board_id"], item["document_id_raw"]) for item in records})
            if not output.final.exists():
                if previous.job_config.scrape_options.media_mode == "download":
                    from .media_enrichment import enrich_local_media
                    await enrich_local_media(output, previous.job_config.scrape_options.media_download_concurrency)
                else:
                    await output.finalize()
        previous.active_run_id = None
        previous.staging_ref = None
        previous.updated_at = datetime.now(SEOUL)
        store.save(previous)
    if previous.realtime_deadline_at and datetime.now(SEOUL) >= previous.realtime_deadline_at:
        paths = sorted((Path(output_path) / "outputs" / previous.job_id).glob("*/job_*.jsonl"))
        latest = paths[-1] if paths else None
        previous.status = "completed"
        previous.reason = None
        previous.updated_at = datetime.now(SEOUL)
        store.save(previous)
        return JobReport(job_id=previous.job_id, status="completed",
            started_at=previous.job_config.created_at, finished_at=datetime.now(SEOUL),
            stored_count=sum(1 for line in latest.open(encoding="utf-8")) if latest else 0,
            enrichment_status=previous.enrichment_status,
            output_refs=[StoredObjectRef(storage_mode="local", path=str(latest))] if latest else [])
    return await JobRunner(previous.job_config, runtime, previous, run_id).run()

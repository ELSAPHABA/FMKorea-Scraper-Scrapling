import json
import os
from pathlib import Path
from urllib.parse import urlparse

import anyio
from scrapling.fetchers import FetcherSession

from fmkorea_scraper.outputs.layout import safe_segment
from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.failure import FailedPostKey, FailureRecord
from datetime import datetime


async def enrich_local_media(output, concurrency: int) -> tuple[str, int, int]:
    downloaded = failed = 0
    counts = {"failed": 0}
    limiter = anyio.Semaphore(concurrency)

    async with FetcherSession(impersonate="chrome", stealthy_headers=True) as session:
        async def enrich_one(item, media):
            nonlocal downloaded, failed
            url = media["url"]
            parsed = urlparse(url)
            if (parsed.hostname or "").lower() != "fmkorea.com" and not (parsed.hostname or "").lower().endswith(".fmkorea.com"):
                failed += 1
                counts["failed"] = failed
                return
            async with limiter:
                try:
                    response = await session.get(url)
                    if response.status >= 400:
                        raise ValueError(f"media HTTP {response.status}")
                    relative = media_relative_path(item, media)
                    if hasattr(output, "bucket"):
                        key = output._key(relative)
                        await anyio.to_thread.run_sync(output._put_immutable, key, response.body)
                        media["object_key"] = key
                    else:
                        target = output.root.joinpath(*relative.split("/"))
                        target.parent.mkdir(parents=True, exist_ok=True)
                        with target.open("wb") as stream:
                            stream.write(response.body)
                            stream.flush()
                            os.fsync(stream.fileno())
                        media["file_path"] = relative
                    downloaded += 1
                except Exception as exc:
                    failed += 1
                    counts["failed"] = failed
                    output.append_failure(FailureRecord(job_id=output.job_id,
                        stage="media", reason=type(exc).__name__, retryable=True, url=url,
                        post_key=FailedPostKey(board_id=item["board_id"],
                            document_srl=item["document_srl"], document_id_raw=item["document_id_raw"]),
                        safe_message="media download failed", created_at=datetime.now(SEOUL)).model_dump(mode="json"))

        async def records():
            if not output.staging.exists():
                return
            with output.staging.open(encoding="utf-8") as source:
                for line in source:
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    async with anyio.create_task_group() as tasks:
                        for media in item.get("media", []):
                            if media.get("media_type") in {"image", "fmk_video"}:
                                tasks.start_soon(enrich_one, item, media)
                    yield item

        output_path = await output.finalize_enriched(records(), counts)
    return output_path, downloaded, failed


def media_relative_path(item: dict, media: dict) -> str:
    filename = safe_segment(Path(urlparse(media["url"]).path).name, fallback="media.bin")
    return (f"media/{safe_segment(item['board_id'])}/{safe_segment(item['document_srl'])}/"
        f"{safe_segment(media['media_type'])}/{media['source_block_order']}-{filename}")

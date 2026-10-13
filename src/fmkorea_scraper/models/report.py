from datetime import datetime
from typing import Literal

from pydantic import Field

from .base import MetadataModel


class StoredObjectRef(MetadataModel):
    storage_mode: Literal["local", "s3_compatible"]
    path: str | None = None
    bucket: str | None = None
    object_key: str | None = None


class JobReport(MetadataModel):
    schema_version: Literal["fmkorea.job_report.v1"] = "fmkorea.job_report.v1"
    job_id: str
    status: Literal["completed", "stopped"]
    started_at: datetime
    finished_at: datetime
    collected_count: int = 0
    stored_count: int = 0
    unavailable_count: int = 0
    failure_count: int = 0
    media_downloaded_count: int = 0
    media_failure_count: int = 0
    enrichment_status: str = "not_requested"
    output_refs: list[StoredObjectRef] = Field(default_factory=list)


class RetryReport(JobReport):
    selected_count: int = 0
    skipped_count: int = 0

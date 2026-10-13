from datetime import datetime
from typing import Literal

from pydantic import Field, model_validator

from .base import StrictModel
from .job import JobConfig


class Checkpoint(StrictModel):
    schema_version: Literal["fmkorea.checkpoint.v1"] = "fmkorea.checkpoint.v1"
    job_id: str
    status: Literal["running", "stopped", "completed"] = "running"
    reason: Literal["blocked", "manual_stop", "error", "timeout", "lease_expired"] | None = None
    job_config: JobConfig
    completed_keys: list[tuple[str, str, str]] = Field(default_factory=list)
    active_run_id: str | None = None
    staging_ref: str | None = None
    enrichment_status: Literal["not_requested", "pending", "running", "completed", "completed_with_warnings", "failed"] = "not_requested"
    realtime_deadline_at: datetime | None = None
    crawl_state_ref: str | None = None
    updated_at: datetime

    @model_validator(mode="after")
    def check_reason(self):
        if (self.status == "stopped") != (self.reason is not None):
            raise ValueError("reason required only for stopped checkpoint")
        return self

from datetime import datetime
from typing import Literal

from pydantic import Field

from .base import MetadataModel, StrictModel


class FailedPostKey(StrictModel):
    site_id: Literal["fmkorea"] = "fmkorea"
    board_id: str | None = None
    document_srl: int | None = None
    document_id_raw: str | None = None


class FailureRecord(MetadataModel):
    schema_version: Literal["fmkorea.failure.v1"] = "fmkorea.failure.v1"
    job_id: str
    stage: str
    reason: str
    retryable: bool
    url: str | None = None
    post_key: FailedPostKey | None = None
    safe_message: str
    details: dict = Field(default_factory=dict)
    created_at: datetime

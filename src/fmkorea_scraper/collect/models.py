from datetime import datetime
from typing import Any

from pydantic import Field

from fmkorea_scraper.models.base import MetadataModel


class PostCandidate(MetadataModel):
    board_id: str | None = None
    document_srl: int
    document_id_raw: str
    url: str
    candidate_title: str | None = None
    candidate_created_at: datetime | None = None


class BoardCandidate(MetadataModel):
    board_id: str
    board_name: str | None = None
    board_url: str | None = None


class CandidatePage(MetadataModel):
    posts: list[PostCandidate] = Field(default_factory=list)
    boards: list[BoardCandidate] = Field(default_factory=list)
    has_next: bool = False
    failures: list[dict[str, Any]] = Field(default_factory=list)

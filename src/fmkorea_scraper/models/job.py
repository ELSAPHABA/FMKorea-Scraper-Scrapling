from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Union

from pydantic import Field, HttpUrl, model_validator

from .base import MetadataModel, StrictModel
from fmkorea_scraper.dom import SEOUL


class RangeFilter(StrictModel):
    date_from: date | None = None
    date_to: date | None = None
    min_document_srl: int | None = Field(default=None, ge=1)
    max_document_srl: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def check_order(self):
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from must be <= date_to")
        if self.min_document_srl and self.max_document_srl and self.min_document_srl > self.max_document_srl:
            raise ValueError("min_document_srl must be <= max_document_srl")
        return self


class DirectPostSource(StrictModel):
    type: Literal["direct_post"] = "direct_post"
    urls: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_urls(self):
        from fmkorea_scraper.collect.urls import allowed_post_url, document_id
        if any(not allowed_post_url(url) or not document_id(url) for url in self.urls):
            raise ValueError("direct-post URLs must be FMKorea post URLs")
        return self


class BoardListSource(StrictModel):
    type: Literal["board_list"] = "board_list"
    board_id: str
    category_id: str | None = None
    page_start: int = Field(default=1, ge=1)
    page_end: int | None = Field(default=None, ge=1)
    range_filter: RangeFilter | None = None

    @model_validator(mode="after")
    def check_pages(self):
        if self.page_end is not None and self.page_end < self.page_start:
            raise ValueError("page_end must be >= page_start")
        return self


class BoardSearchSource(BoardListSource):
    type: Literal["board_search"] = "board_search"
    keyword: str = Field(min_length=1)
    search_target: Literal["title_content", "title_content_summary", "title", "content", "nick_name", "comment"] = "title_content"


class IntegratedSearchSource(StrictModel):
    type: Literal["integrated_search"] = "integrated_search"
    keyword: str = Field(min_length=1)
    search_target: Literal["title_content", "title", "content"] = "title_content"
    page_start: int = Field(default=1, ge=1, le=1000)
    page_end: int | None = Field(default=None, ge=1, le=1000)
    range_filter: RangeFilter | None = None

    @model_validator(mode="after")
    def check_pages(self):
        if self.page_end is not None and self.page_end < self.page_start:
            raise ValueError("page_end must be >= page_start")
        return self


class DeepSearchSource(StrictModel):
    type: Literal["deep_search"] = "deep_search"
    keyword: str = Field(min_length=1)
    seed_search_target: Literal["title_content", "title", "content"] = "title_content"
    seed_page_limit: int = Field(default=20, ge=1, le=1000)
    board_ids: list[str] = Field(default_factory=list)
    range_filter: RangeFilter

    @model_validator(mode="after")
    def require_range(self):
        if not any(v is not None for v in self.range_filter.model_dump().values()):
            raise ValueError("deep_search requires a date or document range")
        return self


class NoKeywordRangeSource(StrictModel):
    type: Literal["no_keyword_range"] = "no_keyword_range"
    board_id: str
    category_id: str | None = None
    range_filter: RangeFilter

    @model_validator(mode="after")
    def require_range(self):
        if not any(v is not None for v in self.range_filter.model_dump().values()):
            raise ValueError("no_keyword_range requires a date or document range")
        return self


class RealtimeTermination(StrictModel):
    duration_seconds: int | None = Field(default=None, ge=1)
    end_at: datetime | None = None

    @model_validator(mode="after")
    def exactly_one(self):
        if (self.duration_seconds is None) == (self.end_at is None):
            raise ValueError("invalid_realtime_termination")
        if self.end_at is not None and self.end_at.tzinfo is None:
            raise ValueError("end_at must include a timezone")
        return self


class RealtimeKeywordSource(StrictModel):
    type: Literal["realtime_keyword"] = "realtime_keyword"
    scope: Literal["board", "integrated"]
    board_id: str | None = None
    keyword: str = Field(min_length=1)
    search_target: Literal["title_content", "title_content_summary", "title", "content", "nick_name", "comment"] = "title_content"
    poll_interval_seconds: float = Field(default=30.0, gt=0)
    termination: RealtimeTermination

    @model_validator(mode="after")
    def check_scope(self):
        if self.scope == "board" and not self.board_id:
            raise ValueError("board_id required for board realtime search")
        if self.scope == "integrated" and self.board_id:
            raise ValueError("board_id not allowed for integrated realtime search")
        if self.scope == "integrated" and self.search_target not in {"title_content", "title", "content"}:
            raise ValueError("unsupported integrated search_target")
        return self


class RealtimeAllSource(StrictModel):
    type: Literal["realtime_all"] = "realtime_all"
    board_id: str
    category_id: str | None = None
    poll_interval_seconds: float = Field(default=30.0, gt=0)
    termination: RealtimeTermination


class ParserFailureRetrySource(StrictModel):
    type: Literal["parser_failure_retry"] = "parser_failure_retry"
    failure_input: str
    skip_existing_success: bool = True


Source = Annotated[Union[DirectPostSource, BoardListSource, BoardSearchSource,
    IntegratedSearchSource, DeepSearchSource, NoKeywordRangeSource,
    RealtimeKeywordSource, RealtimeAllSource, ParserFailureRetrySource], Field(discriminator="type")]


class ScrapeOptions(StrictModel):
    comment_mode: Literal["none", "all", "best_only"] = "all"
    media_mode: Literal["url_only", "download"] = "url_only"
    media_download_concurrency: int = Field(default=2, ge=1)
    include_attachments: bool = True
    max_comment_pages: int | None = Field(default=None, ge=0)
    max_content_blocks: int | None = Field(default=None, ge=1)
    max_comments: int | None = Field(default=None, ge=1)


class CrawlConfig(StrictModel):
    concurrent_requests: int = Field(default=4, ge=1)
    concurrent_requests_per_domain: int = Field(default=2, ge=1)
    download_delay: float = Field(default=1.0, gt=0)
    max_blocked_retries: int = Field(default=3, ge=1)
    autothrottle_enabled: bool = False
    autothrottle_start_delay: float = Field(default=2.0, gt=0)
    autothrottle_max_delay: float = Field(default=30.0, gt=0)
    browser_max_pages: int = Field(default=2, ge=1)
    checkpoint_interval_seconds: float = Field(default=300.0, gt=0)
    robots_txt_obey: bool = False

    @model_validator(mode="after")
    def check_retries(self):
        if self.max_blocked_retries % 2 == 0:
            raise ValueError("max_blocked_retries must be odd")
        return self


class S3StorageConfig(StrictModel):
    endpoint_url: str
    bucket: str
    prefix: str | None = None
    region: str | None = None


class PostgresJobCheckpointConfig(StrictModel):
    dsn: str


class StorageConfig(StrictModel):
    output_mode: Literal["local", "s3_compatible"] = "local"
    job_checkpoint_mode: Literal["local", "postgres"] = "local"
    base_output_path: str = "."
    s3: S3StorageConfig | None = None
    postgres: PostgresJobCheckpointConfig | None = None
    s3_flush_records: int = Field(default=100, ge=1)
    s3_flush_bytes: int = Field(default=8388608, gt=0)
    s3_flush_seconds: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def check_backends(self):
        if self.output_mode == "s3_compatible" and self.s3 is None:
            raise ValueError("s3 configuration required")
        if self.job_checkpoint_mode == "postgres" and self.postgres is None:
            raise ValueError("postgres configuration required")
        return self


class JobConfig(StrictModel):
    job_id: str
    job_type: str
    site_id: Literal["fmkorea"] = "fmkorea"
    source: Source
    scrape_options: ScrapeOptions = Field(default_factory=ScrapeOptions)
    crawl_config: CrawlConfig = Field(default_factory=CrawlConfig)
    storage_config: StorageConfig = Field(default_factory=StorageConfig)
    created_at: datetime = Field(default_factory=lambda: datetime.now(SEOUL))

    @model_validator(mode="after")
    def check_job_type(self):
        if self.job_type != self.source.type:
            raise ValueError("job_type must match source.type")
        if self.source.type.startswith("realtime_") and self.crawl_config.concurrent_requests < 2:
            raise ValueError("realtime requires concurrent_requests >= 2")
        return self


class SpiderRuntimeConfig(StrictModel):
    http_timeout_seconds: float = Field(default=30.0, gt=0)
    browser_timeout_ms: int = Field(default=30000, gt=0)
    http_transport_retries: int = Field(default=3, ge=0)
    browser_transport_retries: int = Field(default=3, ge=0)
    proxies: list[str] = Field(default_factory=list)
    proxy_env_path: Path | None = None
    execution_dir: Path = Field(default_factory=Path.cwd)
    persistent_state_root: Path | None = None

from datetime import datetime
from types import SimpleNamespace
from typing import Literal

import anyio
from pydantic import Field, model_validator

from fmkorea_scraper.application.jobs import run_job as _run_job
from fmkorea_scraper.application.resume import resume_job_async
from fmkorea_scraper.collect.urls import BASE, allowed_post_url, document_id
from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.base import StrictModel
from fmkorea_scraper.models.job import (BoardSearchSource, DirectPostSource,
    IntegratedSearchSource, JobConfig, RangeFilter, ScrapeOptions,
    SpiderRuntimeConfig)
from fmkorea_scraper.models.post import CommentInfo, PostResult
from fmkorea_scraper.spider import FMKoreaSpider


class BoardSearchRequest(StrictModel):
    board_id: str
    keyword: str
    search_target: Literal["title_content", "title_content_summary", "title", "content", "nick_name", "comment"] = "title_content"
    category_id: str | None = None
    page_start: int = Field(default=1, ge=1)
    page_end: int | None = Field(default=None, ge=1)
    range_filter: RangeFilter | None = None
    scrape_options: ScrapeOptions = Field(default_factory=ScrapeOptions)


class IntegratedSearchRequest(StrictModel):
    keyword: str
    search_target: Literal["title_content", "title", "content"] = "title_content"
    page_start: int = Field(default=1, ge=1)
    page_end: int | None = Field(default=None, ge=1)
    range_filter: RangeFilter | None = None
    scrape_options: ScrapeOptions = Field(default_factory=ScrapeOptions)


class PostRequest(StrictModel):
    url: str | None = None
    board_id: str | None = None
    document_srl: int | None = Field(default=None, ge=1)
    scrape_options: ScrapeOptions = Field(default_factory=ScrapeOptions)

    @model_validator(mode="after")
    def check_target(self):
        if self.url:
            if not allowed_post_url(self.url) or not document_id(self.url):
                raise ValueError("invalid FMKorea post URL")
        elif not (self.board_id and self.document_srl):
            raise ValueError("url or board_id + document_srl required")
        return self


class CommentRequest(StrictModel):
    url: str | None = None
    board_id: str | None = None
    document_srl: int | None = None
    comment_mode: Literal["none", "all", "best_only"] = "all"


class ResumeRequest(StrictModel):
    job_id: str | None = None
    output_path: str = "."


class RetryRequest(StrictModel):
    failure_input: str
    skip_existing_success: bool = True
    existing_success_path: str | None = None
    from_report_path: str | None = None
    from_checkpoint_path: str | None = None
    retry_job_id: str | None = None
    job_id: str | None = None
    stage: list[str] = Field(default_factory=lambda: ["post"])
    output_path: str = "."


async def _collect(config: JobConfig, runtime: SpiderRuntimeConfig | None):
    spider = FMKoreaSpider(config, runtime)
    return [PostResult.model_validate(item) async for item in spider.stream()]


def search_board(request: BoardSearchRequest, runtime: SpiderRuntimeConfig | None = None) -> list[PostResult]:
    source = BoardSearchSource(**request.model_dump(exclude={"scrape_options"}))
    config = JobConfig(job_id=f"search_{datetime.now(SEOUL):%Y%m%d_%H%M%S}",
        job_type=source.type, source=source, scrape_options=request.scrape_options)
    return anyio.run(_collect, config, runtime)


def search_integrated(request: IntegratedSearchRequest, runtime: SpiderRuntimeConfig | None = None) -> list[PostResult]:
    source = IntegratedSearchSource(**request.model_dump(exclude={"scrape_options"}))
    config = JobConfig(job_id=f"search_{datetime.now(SEOUL):%Y%m%d_%H%M%S}",
        job_type=source.type, source=source, scrape_options=request.scrape_options)
    return anyio.run(_collect, config, runtime)


def fetch_post(request: PostRequest, runtime: SpiderRuntimeConfig | None = None) -> PostResult | None:
    url = request.url or f"{BASE}/index.php?mid={request.board_id}&document_srl={request.document_srl}"
    source = DirectPostSource(urls=[url])
    config = JobConfig(job_id=f"post_{datetime.now(SEOUL):%Y%m%d_%H%M%S}",
        job_type=source.type, source=source, scrape_options=request.scrape_options)
    posts = anyio.run(_collect, config, runtime)
    return posts[0] if posts else None


def fetch_comments(request: CommentRequest, runtime: SpiderRuntimeConfig | None = None) -> list[CommentInfo]:
    post = fetch_post(PostRequest(url=request.url, board_id=request.board_id,
        document_srl=request.document_srl,
        scrape_options=ScrapeOptions(comment_mode=request.comment_mode)), runtime)
    return post.comments if post else []


def run_job(config: JobConfig, runtime: SpiderRuntimeConfig | None = None, run_id: str | None = None):
    return _run_job(config, runtime, run_id)


def resume_job(request: ResumeRequest, runtime: SpiderRuntimeConfig | None = None,
               run_id: str | None = None):
    return anyio.run(resume_job_async, request.job_id, request.output_path, runtime, run_id)


def retry_parser_failures(request: RetryRequest, retry_service=None):
    if retry_service is not None:
        return retry_service.run(request)
    from fmkorea_scraper.retry.parser_failures import retry_parser_failures_async
    args = SimpleNamespace(failure_path=request.failure_input,
        skip_existing_success=request.skip_existing_success,
        existing_success_path=request.existing_success_path,
        from_report=request.from_report_path, from_checkpoint=request.from_checkpoint_path,
        retry_job_id=request.retry_job_id, job_id=request.job_id,
        stage=request.stage, output_path=request.output_path)
    return anyio.run(retry_parser_failures_async, args)

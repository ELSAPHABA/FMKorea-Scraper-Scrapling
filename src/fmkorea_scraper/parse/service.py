from datetime import datetime

from scrapling import Selector

from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.models.job import ScrapeOptions
from fmkorea_scraper.models.post import PostResult

from .comments import parse_comments
from .content import parse_content
from .metadata import parse_attachments, parse_metadata


def parse_post(html: str | bytes, url: str, options: ScrapeOptions | None = None,
               board_hint: str | None = None, document_id_hint: str | None = None) -> tuple[PostResult, list[str]]:
    options = options or ScrapeOptions()
    selector = Selector(html, url=url)
    metadata, warnings = parse_metadata(selector, url, board_hint, document_id_hint)
    attachments = parse_attachments(selector, url) if options.include_attachments else []
    content, media, content_warnings = parse_content(selector, url, attachments, options.max_content_blocks)
    comments, comment_warnings = parse_comments(selector, metadata["canonical_url"],
        options.comment_mode, options.max_comments)
    post = PostResult(scraped_at=datetime.now(SEOUL), **metadata, content=content,
        attachments=attachments, media=media, comments=comments)
    return post, warnings + content_warnings + comment_warnings

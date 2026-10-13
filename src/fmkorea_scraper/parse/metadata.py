import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from fmkorea_scraper.collect.urls import absolute, allowed_post_url, board_id, document_id
from fmkorea_scraper.dom import attr, date_time, first, number, text
from fmkorea_scraper.models.post import AttachmentInfo, AuthorInfo, CategoryInfo, ReactionInfo


def author(node) -> AuthorInfo:
    classes = attr(node, "class") or ""
    member = re.search(r"(?:^|\s)member_(\d+)", classes)
    icon = first(node, "img.level") if node is not None else None
    level_text = attr(icon, "alt") or attr(icon, "title") or ""
    level = re.search(r"레벨\s*:?\s*(\d+)", level_text)
    return AuthorInfo(nickname=text(node) or "unknown", member_srl=member.group(1) if member else None,
        level=level.group(1) if level else None,
        profile_url=attr(node, "href") if node is not None and node.tag == "a" else None)


def _count(root, label: str) -> int:
    for selector in (".rd_hd .btm_area .side.fr span", ".btm_area span", "span"):
        for node in root.css(selector):
            content = text(node)
            if label in content:
                return number(content.split(label, 1)[1])
    return 0


def parse_metadata(selector, source_url: str, board_hint: str | None = None,
                   document_id_hint: str | None = None) -> tuple[dict, list[str]]:
    warnings = []
    root = first(selector, "#bd_capture")
    if root is None:
        root = selector
        warnings.append("missing_detail_root")
    canonical_node = first(selector, 'link[rel="canonical"][href]', ".document_address a.a[href]")
    canonical = absolute(attr(canonical_node, "href") or source_url, source_url)
    if not allowed_post_url(canonical):
        raise ValueError("required_metadata: invalid canonical URL")
    raw_id = document_id(canonical) or document_id_hint
    title = text(first(root, ".rd_hd h1 .np_18px_span", ".rd_hd h1"))
    if not raw_id or not title:
        raise ValueError("required_metadata")
    board = board_id(canonical) or board_hint
    if not board:
        for link in selector.css('a[href*="document_srl="]'):
            href = absolute(attr(link, "href") or "", source_url)
            if document_id(href) == raw_id and board_id(href):
                board = board_id(href)
                break
    if not board:
        script_text = " ".join(selector.css("script::text").getall())
        match = re.search(r'window\.document_mid\s*=\s*["\']([^"\']+)', script_text)
        board = match.group(1) if match else None
    if not board:
        raise ValueError("required_metadata: board_id")
    category_node = first(root, 'a.category[href*="category="]')
    if category_node is None:
        category_node = first(selector, 'a.category[href*="category="]')
    category_url = absolute(attr(category_node, "href"), source_url) if category_node else None
    category_id = parse_qs(urlparse(category_url).query).get("category", [None])[0] if category_url else None
    created_raw = text(first(root, ".date", ".regdate", ".rd_hd .btm_area")) or None
    board_name = text(first(selector, ".bd_name", ".board_name", "h1 a")) or None
    return dict(board_id=board, board_name=board_name, document_srl=int(raw_id),
        document_id_raw=raw_id, canonical_url=canonical,
        category=CategoryInfo(category_id=category_id, name=text(category_node) or None, url=category_url),
        title=title, author=author(first(root, ".rd_hd .member_plate")),
        created_at=date_time(created_raw), created_at_raw=created_raw,
        views=_count(root, "조회 수"), reactions=ReactionInfo(up_count=_count(root, "추천 수")),
        comment_count=_count(root, "댓글")), warnings


def parse_attachments(selector, base_url: str) -> list[AttachmentInfo]:
    attachments = []
    for link in selector.css(".rd_file a[data-href]"):
        target = absolute(attr(link, "data-href") or attr(link, "href") or "", base_url)
        query = parse_qs(urlparse(target).query)
        attachments.append(AttachmentInfo(file_name=text(link) or attr(link, "title") or "file",
            url=target, file_srl=query.get("file_srl", [None])[0],
            sid=query.get("sid", [None])[0], mid=query.get("mid", [None])[0],
            size_text=attr(link, "title"), download_text=attr(link, "title")))
    return attachments

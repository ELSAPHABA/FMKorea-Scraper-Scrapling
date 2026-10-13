import re

from fmkorea_scraper.collect.urls import BASE, absolute, document_id
from fmkorea_scraper.dom import attr, date_time, first, number, text
from fmkorea_scraper.models.post import CommentInfo, ReactionInfo

from .metadata import author


def parse_comments(selector, post_url: str, mode: str = "all", max_comments: int | None = None,
                   source_order_start: int = 0) -> tuple[list[CommentInfo], list[str]]:
    if mode == "none":
        return [], []
    comments = []
    warnings = []
    for index, item in enumerate(selector.css(".fdb_lst_ul > li")):
        if max_comments is not None and len(comments) >= max_comments:
            warnings.append("max_comments_exceeded")
            break
        try:
            dom_id = attr(item, "id")
            match = re.search(r"comment_(\d+)", dom_id or "")
            raw_id = match.group(1) if match else None
            parent_raw = attr(item, "data-parent") or attr(item, "data-parent-comment-srl")
            parent_match = re.search(r"\d+", parent_raw or "")
            parent_raw = parent_match.group() if parent_match else None
            classes = attr(item, "class") or ""
            best = "comment_best" in classes or text(first(item, ".icon-hit")) == "BEST"
            content_node = first(item, ".xe_content", ".comment_content", ".comment-content")
            content = ""
            if content_node is not None:
                parts = []
                for node in content_node.xpath(".//text()"):
                    parent = node._root.getparent()
                    if any("imagecon-buy-icon" in ancestor.attrib.get("class", "") for ancestor in [parent, *parent.iterancestors()]):
                        continue
                    parts.append(node.get())
                content = " ".join(" ".join(parts).split())
                images = [attr(img, "alt") for img in content_node.css("img[alt]") if attr(img, "alt")]
                if images:
                    content = " ".join([content, *images]).strip()
            date_raw = text(first(item, ".meta .date", ".date")) or None
            post_id = document_id(post_url)
            permalink = f"{BASE}/{post_id}/{raw_id}#comment_{raw_id}" if raw_id and post_id else None
            comment = CommentInfo(comment_srl=int(raw_id) if raw_id else None,
                comment_id_raw=raw_id, source_dom_id=dom_id,
                parent_comment_srl=int(parent_raw) if parent_raw else None,
                parent_comment_id_raw=parent_raw, depth=1 if parent_raw or "reply" in classes else 0,
                source_order=source_order_start + index, is_best=best,
                badges=["BEST"] if best else [],
                author=author(first(item, ".meta .member_plate", ".member_plate")),
                created_at_raw=date_raw, created_at=date_time(date_raw), content=content,
                reactions=ReactionInfo(up_count=number(text(first(item, 'a[title="추천"] .voted_count', ".voted_count"))),
                    down_count=number(text(first(item, 'a[title="비추천"] .blamed_count', ".blamed_count")))),
                permalink=permalink)
            comments.append(comment)
        except (ValueError, TypeError, AttributeError):
            warnings.append("comment_parse_failed")
    if mode == "best_only":
        comments = [comment for comment in comments if comment.is_best]
    return comments, warnings


def comment_page_urls(selector, post_url: str, document_id: str, limit: int) -> list[str]:
    from fmkorea_scraper.collect.urls import document_id as id_from_url

    urls = []
    seen = set()
    for node in selector.css('.fdb_nav.comment_page a[href], .comment_page a[href], .comment-page a[href]'):
        href = attr(node, "href") or ""
        marker = " ".join((href, attr(node, "class") or "", text(node))).lower()
        if href.startswith("#") or not any(word in marker for word in ("cpage", "comment_page", "commentpage")):
            continue
        url = absolute(href, post_url).split("#", 1)[0]
        if url == post_url.split("#", 1)[0] or url in seen or id_from_url(url) != document_id:
            continue
        seen.add(url)
        urls.append(url)
        if len(urls) >= limit:
            break
    return urls

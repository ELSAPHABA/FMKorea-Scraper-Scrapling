import re
from datetime import datetime
from urllib.parse import parse_qs, urlparse

from fmkorea_scraper.collect.urls import absolute
from fmkorea_scraper.dom import attr, first, number, text
from fmkorea_scraper.models.post import (AttachmentBlock, AttachmentInfo, ContentInfo,
    FmkVideoBlock, ImageBlock, LinkBlock, MediaItem, TextBlock, UnknownEmbedBlock,
    XBlock, YouTubeBlock)


def _ancestors(node):
    parent = node._root.getparent()
    while parent is not None:
        yield parent
        parent = parent.getparent()


def _youtube(url: str, order: int):
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        video_id = parse_qs(parsed.query).get("v", [None])[0] if parsed.path == "/watch" else parsed.path.removeprefix("/embed/").split("/")[0] if parsed.path.startswith("/embed/") else None
    elif host == "youtu.be":
        video_id = parsed.path.strip("/").split("/")[0]
    else:
        return None
    if not video_id or not re.fullmatch(r"[A-Za-z0-9_-]{6,}", video_id):
        return None
    start = parse_qs(parsed.query).get("t", parse_qs(parsed.query).get("start", ["0"]))[0]
    return YouTubeBlock(order=order, video_id=video_id, url=url,
        embed_url=f"https://www.youtube.com/embed/{video_id}",
        start_seconds=int(start) if start.isdigit() else 0)


def _x(url: str, order: int):
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() not in {"x.com", "www.x.com", "twitter.com", "www.twitter.com"}:
        return None
    match = re.fullmatch(r"/([^/]+)/status/(\d+)/?", parsed.path)
    if not match:
        return None
    username, tweet = match.groups()
    return XBlock(order=order, username=username, status_id=tweet, tweet_id=tweet,
        url=url, embed_url=f"https://platform.twitter.com/embed/Tweet.html?id={tweet}")


def _media(block):
    if block.type == "image":
        return MediaItem(media_id=block.file_srl or block.url, media_type="image",
            source_block_order=block.order, url=block.url, file_srl=block.file_srl,
            description=block.alt)
    if block.type == "fmk_video":
        return MediaItem(media_id=block.file_srl or block.video_url, media_type="fmk_video",
            source_block_order=block.order, url=block.video_url, file_srl=block.file_srl,
            description=block.description)
    if block.type in {"youtube", "x"}:
        return MediaItem(media_id=block.video_id if block.type == "youtube" else block.tweet_id or block.url,
            media_type=block.type, source_block_order=block.order, url=block.url,
            provider=block.type, embed_url=block.embed_url)
    return None


def parse_content(selector, base_url: str, attachments: list[AttachmentInfo],
                  max_blocks: int | None = None) -> tuple[ContentInfo, list[MediaItem], list[str]]:
    root = first(selector, "#bd_capture .rd_body article > .xe_content",
        "#bd_capture .rd_body .xe_content")
    if root is None:
        return ContentInfo(), [], ["missing_content_container"]
    warnings = []
    blocks = []
    media = []
    seen_media = set()
    by_file = {item.file_srl: item for item in attachments if item.file_srl}
    for node in root.xpath(".//node()"):
        if max_blocks is not None and len(blocks) >= max_blocks:
            warnings.append("max_content_blocks_exceeded")
            break
        try:
            tag = node.tag.lower()
            order = len(blocks)
            block = None
            if tag == "#text":
                ancestors = list(_ancestors(node))
                if not ancestors or any(parent.tag.lower() in {"script", "style", "video", "source", "iframe", "a"} for parent in ancestors):
                    continue
                value = " ".join(node.get().split())
                if not value or value in {"BeforeDocument", "AfterDocument", "__LAZY__"} or value.startswith("serverLog:"):
                    continue
                block = TextBlock(order=order, text=value)
            elif tag == "img":
                if "mejs__poster-img" in (attr(node, "class") or ""):
                    continue
                original = next((parent.attrib.get("href") for parent in _ancestors(node)
                    if parent.tag.lower() == "a" and "highslide" in parent.attrib.get("class", "")), None)
                url = absolute(original or attr(node, "src") or "", base_url)
                if not original and not attr(node, "src"):
                    continue
                file_srl = attr(node, "data-file-srl") or attr(node, "file_srl")
                block = ImageBlock(order=order, url=url, file_srl=file_srl,
                    alt=attr(node, "alt"), width=number(attr(node, "width")) or None,
                    height=number(attr(node, "height")) or None,
                    download_url=by_file[file_srl].url if file_srl in by_file else None)
            elif tag == "video":
                source = first(node, "source[src]")
                url_raw = attr(source, "src") or attr(node, "src")
                if not url_raw:
                    continue
                parent = node._root.getparent()
                video_parent = type(node)(root=parent) if parent is not None else node
                description = text(first(video_parent, ".video_infobox .box div")) or None
                meta = lambda name: attr(first(video_parent, f'meta[itemprop="{name}"][content]'), "content")
                upload_raw = meta("uploadDate")
                try:
                    upload_date = datetime.fromisoformat(upload_raw) if upload_raw else None
                except ValueError:
                    upload_date = None
                block = FmkVideoBlock(order=order, video_url=absolute(url_raw, base_url),
                    poster_url=absolute(attr(node, "poster"), base_url) if attr(node, "poster") else None,
                    download_url=absolute(url_raw, base_url), name=meta("name"),
                    upload_date=upload_date,
                    description=description, width=number(meta("width")) or None,
                    height=number(meta("height")) or None)
            elif tag in {"iframe", "a"}:
                if tag == "a" and "highslide" in (attr(node, "class") or ""):
                    continue
                target = attr(node, "src") if tag == "iframe" else attr(node, "href")
                if not target or target.startswith(("#", "javascript:")):
                    continue
                url = absolute(target, base_url)
                if tag == "a":
                    file_srl = parse_qs(urlparse(attr(node, "data-href") or url).query).get("file_srl", [None])[0]
                    if file_srl in by_file:
                        item = by_file[file_srl]
                        block = AttachmentBlock(order=order, file_name=item.file_name,
                            url=item.url, file_srl=file_srl)
                if block is None:
                    block = _youtube(url, order) or _x(url, order)
                if block is None:
                    block = UnknownEmbedBlock(order=order, url=url) if tag == "iframe" else LinkBlock(order=order, text=text(node) or url, url=url)
            if block is None:
                continue
            if block.type in {"image", "fmk_video", "youtube", "x"}:
                key = (block.type, getattr(block, "file_srl", None) or getattr(block, "url", None) or getattr(block, "video_url", None))
                if key in seen_media:
                    continue
                seen_media.add(key)
            blocks.append(block)
            item = _media(block)
            if item:
                media.append(item)
        except (ValueError, TypeError, AttributeError):
            warnings.append("content_block_parse_failed")
    content_text = "\n".join(block.text for block in blocks if block.type in {"text", "link"})
    return ContentInfo(text=content_text, blocks=blocks), media, warnings

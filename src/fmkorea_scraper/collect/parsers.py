from urllib.parse import parse_qs, urlparse

from fmkorea_scraper.dom import candidate_date, first, text

from .models import BoardCandidate, CandidatePage, PostCandidate
from .urls import absolute, allowed_post_url, board_id, document_id


def _has_next(selector, current: int) -> bool:
    for link in selector.css("a[href*='page=']"):
        try:
            if int(parse_qs(urlparse(link.attrib["href"]).query).get("page", [0])[0]) > current:
                return True
        except (ValueError, KeyError):
            continue
    return False


def parse_board_page(selector, url: str, page: int, board_hint: str | None = None) -> CandidatePage:
    if not selector.css("div.bd"):
        raise ValueError("missing_board_root")
    rows = selector.css("div.bd table tbody tr") or selector.css("tbody tr")
    result = CandidatePage(has_next=_has_next(selector, page))
    for row in rows:
        row_text = text(row)
        classes = row.attrib.get("class", "").lower()
        if any(marker in classes for marker in ("notice", "ad", "power")) or any(marker in row_text for marker in ("공지", "광고")):
            continue
        try:
            link = first(row, "a.hx[href]", "td.title a[href]", "a[href]")
            candidate_url = absolute(link.attrib["href"], url) if link else ""
            raw_id = document_id(candidate_url)
            if not raw_id or not allowed_post_url(candidate_url):
                raise ValueError("invalid_candidate_url")
            result.posts.append(PostCandidate(board_id=board_id(candidate_url) or board_hint,
                document_srl=int(raw_id), document_id_raw=raw_id, url=candidate_url,
                candidate_title=text(link) or None,
                candidate_created_at=candidate_date(text(first(row, "td.time", ".time"))),
                metadata={"source": "board_page", "candidate_text": row_text[:1000]}))
        except (ValueError, KeyError) as exc:
            result.failures.append({"stage": "board_row", "reason": str(exc)})
    return result


def parse_integrated_page(selector, url: str, page: int) -> CandidatePage:
    if not selector.css(".searchResult, .search_result"):
        raise ValueError("missing_integrated_search_root")
    items = selector.css(".searchResult li, .search_result li, ul li") or selector.css("article, tr")
    result = CandidatePage(has_next=_has_next(selector, page))
    seen_boards = set()
    for item in items:
        try:
            links = [(link, absolute(link.attrib["href"], url)) for link in item.css("a[href]")]
            post_link = next(((link, href) for link, href in links if document_id(href) and allowed_post_url(href)), None)
            if post_link is None:
                continue
            link, href = post_link
            raw_id = document_id(href)
            board_link = next(((node, target) for node, target in links if board_id(target) and not document_id(target)), None)
            board = board_id(board_link[1]) if board_link else board_id(href)
            result.posts.append(PostCandidate(board_id=board, document_srl=int(raw_id),
                document_id_raw=raw_id, url=href, candidate_title=text(link) or None,
                candidate_created_at=candidate_date(text(first(item, ".time"))),
                metadata={"source": "integrated_search", "candidate_text": text(item)[:1000]}))
            if board and board not in seen_boards:
                seen_boards.add(board)
                result.boards.append(BoardCandidate(board_id=board,
                    board_name=text(board_link[0]) or None if board_link else None,
                    board_url=board_link[1] if board_link else None))
        except (ValueError, KeyError, StopIteration) as exc:
            result.failures.append({"stage": "integrated_item", "reason": str(exc)})
    return result

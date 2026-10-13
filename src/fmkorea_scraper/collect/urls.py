import re
from urllib.parse import parse_qs, urljoin, urlparse, urlencode


BASE = "https://www.fmkorea.com"


def absolute(url: str, base: str = BASE) -> str:
    return urljoin(base, url)


def allowed_post_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme == "https" and parsed.hostname in {"fmkorea.com", "www.fmkorea.com", "m.fmkorea.com"}


def document_id(url: str) -> str | None:
    parsed = urlparse(url)
    query_id = parse_qs(parsed.query).get("document_srl", [None])[0]
    if query_id and query_id.isdigit():
        return query_id
    match = re.fullmatch(r"/(?:best/)?(\d+)/?", parsed.path)
    return match.group(1) if match else None


def board_id(url: str) -> str | None:
    parsed = urlparse(url)
    mid = parse_qs(parsed.query).get("mid", [None])[0]
    if mid:
        return mid
    match = re.fullmatch(r"/([A-Za-z][A-Za-z0-9_]*)/?", parsed.path)
    return match.group(1) if match and match.group(1) not in {"index", "search", "best"} else None


def board_url(board: str, page: int = 1, category: str | None = None) -> str:
    params = {"mid": board, "page": page}
    if category:
        params["category"] = category
    return f"{BASE}/index.php?{urlencode(params)}"


def board_search_url(board: str, keyword: str, target: str, page: int = 1, category: str | None = None) -> str:
    params = {"mid": board, "search_target": target, "search_keyword": keyword, "page": page}
    if category:
        params["category"] = category
    return f"{BASE}/search.php?{urlencode(params)}"


def integrated_search_url(keyword: str, target: str, page: int = 1) -> str:
    return f"{BASE}/search.php?{urlencode({'act': 'IS', 'is_keyword': keyword, 'where': 'document', 'search_target': target, 'page': page})}"

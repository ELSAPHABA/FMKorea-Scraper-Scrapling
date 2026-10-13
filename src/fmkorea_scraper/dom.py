import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


SEOUL = ZoneInfo("Asia/Seoul")


def first(node, *selectors):
    for selector in selectors:
        found = node.css(selector)
        if found:
            return found[0]
    return None


def text(node) -> str:
    return " ".join(" ".join(node.css("::text").getall()).split()) if node is not None else ""


def attr(node, name: str) -> str | None:
    return node.attrib.get(name) if node is not None else None


def number(value: str | None) -> int:
    match = re.search(r"\d[\d,]*", value or "")
    return int(match.group().replace(",", "")) if match else 0


def date_time(value: str | None) -> datetime | None:
    match = re.search(r"(\d{4})[.-](\d{1,2})[.-](\d{1,2})\s+(\d{1,2}):(\d{2})", value or "")
    if not match:
        return None
    try:
        return datetime(*map(int, match.groups()), tzinfo=SEOUL)
    except ValueError:
        return None


def candidate_date(value: str | None) -> datetime | None:
    parsed = date_time(value)
    if parsed:
        return parsed
    value = (value or "").strip()
    now = datetime.now(SEOUL)
    short = re.fullmatch(r"(\d{2})\.(\d{1,2})\.(\d{1,2})", value)
    if short:
        try:
            return datetime(2000 + int(short.group(1)), int(short.group(2)),
                int(short.group(3)), tzinfo=SEOUL)
        except ValueError:
            return None
    short = re.fullmatch(r"(\d{1,2})\.(\d{1,2})", value)
    if short:
        try:
            return datetime(now.year, int(short.group(1)), int(short.group(2)), tzinfo=SEOUL)
        except ValueError:
            return None
    short = re.fullmatch(r"(\d{1,2}):(\d{2})", value)
    if short:
        try:
            return now.replace(hour=int(short.group(1)), minute=int(short.group(2)),
                second=0, microsecond=0)
        except ValueError:
            return None
    ago = re.fullmatch(r"(\d+)\s*(분|시간|일)\s*전", value)
    if ago:
        unit = {"분": "minutes", "시간": "hours", "일": "days"}[ago.group(2)]
        return now - timedelta(**{unit: int(ago.group(1))})
    return None

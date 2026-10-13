from pathlib import Path

import pytest
from pydantic import ValidationError

from fmkorea_scraper import PostRequest, fetch_post, run_job
from fmkorea_scraper.adapters.cli import main
from fmkorea_scraper.models.job import RealtimeKeywordSource, RealtimeTermination


def test_public_import_and_realtime_search_scope():
    assert callable(fetch_post) and callable(run_job)
    assert PostRequest(url="https://www.fmkorea.com/123").url.endswith("123")
    RealtimeKeywordSource(scope="board", board_id="humor", keyword="test",
        search_target="comment", termination=RealtimeTermination(duration_seconds=1))
    with pytest.raises(ValidationError):
        RealtimeKeywordSource(scope="integrated", keyword="test", search_target="comment",
            termination=RealtimeTermination(duration_seconds=1))


def test_invalid_configure_proxy_does_not_change_env(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    env = Path(".env")
    env.write_text("OTHER=keep\n", encoding="utf-8")
    assert main(["configure-proxy", "--host", "bad-url", "--id", "u", "--password", "p"]) == 2
    assert env.read_text(encoding="utf-8") == "OTHER=keep\n"

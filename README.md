# FMKorea-Scraper-Scrapling
에펨코리아 게시글 및 댓글 스크래퍼 powered by Scrapling 
# FMKorea Scraper

Scrapling based FMKorea post and comment scraper. The product contract is
[`docs/FMKorea-Scraper_Function_Specification.md`](docs/FMKorea-Scraper_Function_Specification.md).
The AI-DLC work record is in [`docs/ai-dlc/delivery-plan.md`](docs/ai-dlc/delivery-plan.md).

## Local setup

Requires Python 3.11 or newer. Install the package, browser runtime, and optional test tools:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -e ".[test]"
.\.venv\Scripts\scrapling install
```

The checked dependency set for container builds is in `requirements.lock`.

## Run

```powershell
fmkorea-scraper run --source direct-post --url https://www.fmkorea.com/10438841266 --output-path .
fmkorea-scraper run --source board-list --board-id humor --page-end 1 --output-path .
fmkorea-scraper resume --job-id <job_id> --output-path .
fmkorea-scraper retry-parser-failures --failure-path .\outputs\<job_id>\<run_id>\failures.jsonl
fmkorea-scraper wizard
```

`python main.py ...` and `python -m fmkorea_scraper.adapters.cli ...` use the
same command interface. Use `--debug` before the subcommand. Outputs and the
local `Checkpoint.json` live under `--output-path`. A first Ctrl+C requests a
graceful stop; the saved job can be resumed.

For a SOCKS5-only proxy pool, set `FMKOREA_PROXY_HOST` in `.env` to a
semicolon-separated list of `socks5://host:port` URLs, with shared credentials
in `FMKOREA_PROXY_ID` and `FMKOREA_PROXY_PW`. HTTP requests use SOCKS5 directly;
browser fallback uses a temporary loopback HTTP bridge to the same proxy.

The Python API is importable from `fmkorea_scraper`:

```python
from fmkorea_scraper import PostRequest, fetch_post

post = fetch_post(PostRequest(url="https://www.fmkorea.com/10438841266"))
```

## Container and infrastructure

`Dockerfile` runs the one shot worker in `src/fmkorea_scraper/adapters/container.py`.
Provide the PostgreSQL and S3 compatible variables listed in specification
section 8.2.11. `RESUME=true` loads a PostgreSQL checkpoint; set
`FMKOREA_CRAWDIR` to a persistent state root when exact non realtime scheduler
resume is required. CDK code and environment examples are in `infra/cdk/`.

## Verify

```powershell
python -m pytest -q
python -m compileall -q src
```

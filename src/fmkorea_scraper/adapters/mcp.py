import anyio

from fmkorea_scraper.application.jobs import run_job_async
from fmkorea_scraper.application.resume import resume_job_async
from fmkorea_scraper.core import api
from fmkorea_scraper.models.job import JobConfig, SpiderRuntimeConfig
from fmkorea_scraper.retry.parser_failures import retry_parser_failures_async

from .safe_errors import safe_error


def create_server():
    from mcp.server.fastmcp import FastMCP
    server = FastMCP("fmkorea-scraper")

    async def invoke(stage, work):
        try:
            data = await work()
            if isinstance(data, list):
                data = [item.model_dump(mode="json") for item in data]
            elif hasattr(data, "model_dump"):
                data = data.model_dump(mode="json")
            return {"ok": True, "data": data}
        except Exception as exc:
            return {"ok": False, "error": safe_error(exc, stage)}

    @server.tool()
    async def search_board(request: dict):
        """Search a FMKorea board and collect matching posts."""
        return await invoke("search_board", lambda: anyio.to_thread.run_sync(api.search_board,
            api.BoardSearchRequest.model_validate(request)))

    @server.tool()
    async def search_integrated(request: dict):
        """Search all FMKorea boards and collect matching posts."""
        return await invoke("search_integrated", lambda: anyio.to_thread.run_sync(api.search_integrated,
            api.IntegratedSearchRequest.model_validate(request)))

    @server.tool()
    async def fetch_post(request: dict):
        """Collect one FMKorea post."""
        return await invoke("fetch_post", lambda: anyio.to_thread.run_sync(api.fetch_post,
            api.PostRequest.model_validate(request)))

    @server.tool()
    async def fetch_comments(request: dict):
        """Collect comments already present on a FMKorea post page."""
        return await invoke("fetch_comments", lambda: anyio.to_thread.run_sync(api.fetch_comments,
            api.CommentRequest.model_validate(request)))

    @server.tool()
    async def run_job(config: dict):
        """Run a one-shot FMKorea collection job."""
        return await invoke("run_job", lambda: run_job_async(JobConfig.model_validate(config)))

    @server.tool()
    async def resume_job(request: dict):
        """Resume a local FMKorea job."""
        return await invoke("resume_job", lambda: resume_job_async(**api.ResumeRequest.model_validate(request).model_dump()))

    @server.tool()
    async def retry_parser_failures(request: dict):
        """Retry parser-stage post failures from a failures JSONL."""
        return await invoke("retry_parser_failures", lambda: anyio.to_thread.run_sync(api.retry_parser_failures,
            api.RetryRequest.model_validate(request)))

    return server


def main():
    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()

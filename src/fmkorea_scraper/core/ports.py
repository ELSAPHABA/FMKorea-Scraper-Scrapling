from dataclasses import dataclass
from typing import Protocol


PostKey = tuple[str, str, str]


@dataclass(frozen=True)
class StorageCommitReceipt:
    post_key: PostKey
    durable: bool
    staging_ref: str
    committed_keys: tuple[PostKey, ...] = ()


class OutputStorePort(Protocol):
    async def append_post(self, item: dict) -> StorageCommitReceipt: ...
    async def flush_pending(self) -> list[PostKey]: ...
    async def finalize(self, records: list[dict] | None = None) -> str: ...


class JobCheckpointStorePort(Protocol):
    def save(self, checkpoint) -> None: ...
    def load(self, job_id: str): ...
    def load_latest(self): ...

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class MetadataModel(StrictModel):
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        def depth(item: Any, level: int = 0) -> int:
            if level > 8:
                raise ValueError("metadata exceeds maximum depth")
            if isinstance(item, dict):
                return max((depth(child, level + 1) for child in item.values()), default=level)
            if isinstance(item, list):
                return max((depth(child, level + 1) for child in item), default=level)
            return level

        depth(value)
        try:
            size = len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
        except (TypeError, ValueError) as exc:
            raise ValueError("metadata must be JSON serializable") from exc
        if size > 65536:
            raise ValueError("metadata exceeds 64 KiB")
        return value

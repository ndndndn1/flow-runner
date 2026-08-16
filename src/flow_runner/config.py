from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    registry_path: str
    mongo_uri: str | None
    mongo_database: str
    lineage_url: str
    lineage_token: str | None
    flowprint_url: str | None
    timeout_seconds: float
    max_payload_bytes: int

    @classmethod
    def from_env(cls, *, registry_path: str | None = None) -> Settings:
        path = registry_path or os.environ.get("FLOW_REGISTRY_PATH")
        if not path:
            raise RuntimeError("FLOW_REGISTRY_PATH is required")
        return cls(
            registry_path=path,
            mongo_uri=(
                os.environ.get("FLOW_MONGODB_URI")
                or os.environ.get("MONGO_AUTO_URI")
                or os.environ.get("MONGODB_URI")
            ),
            mongo_database=os.environ.get("FLOW_MONGO_DATABASE", "flow_runtime"),
            lineage_url=os.environ.get("FLOW_LINEAGE_URL", "http://lineage:8080"),
            lineage_token=os.environ.get("FLOW_LINEAGE_TOKEN"),
            flowprint_url=os.environ.get("FLOWPRINT_URL"),
            timeout_seconds=float(os.environ.get("FLOW_MODULE_TIMEOUT_SECONDS", "30")),
            max_payload_bytes=int(os.environ.get("FLOW_MAX_PAYLOAD_BYTES", str(10 * 1024 * 1024))),
        )

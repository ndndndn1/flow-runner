from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def record_count(value: Any) -> int | None:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in ("records", "items", "rows", "results"):
            if isinstance(value.get(key), list):
                return len(value[key])
        return 1
    return None


def describe(value: Any, schema_ref: str, *, max_bytes: int) -> dict[str, Any]:
    encoded = canonical_bytes(value)
    if len(encoded) > max_bytes:
        raise ValueError(f"payload is {len(encoded)} bytes; limit is {max_bytes}")
    return {
        "hash": f"sha256:{hashlib.sha256(encoded).hexdigest()}",
        "byteCount": len(encoded),
        "recordCount": record_count(value),
        "schemaRef": schema_ref,
    }


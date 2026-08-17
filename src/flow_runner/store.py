from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime
from typing import Any, Protocol

from pymongo import ASCENDING, MongoClient


def now() -> str:
    return datetime.now(UTC).isoformat()


class RunStore(Protocol):
    async def create_run(self, document: dict[str, Any]) -> None: ...
    async def update_run(self, run_id: str, values: dict[str, Any]) -> None: ...
    async def get_run(self, run_id: str) -> dict[str, Any] | None: ...
    async def enqueue(self, event_id: str, event: dict[str, Any]) -> dict[str, Any]: ...
    async def ack(self, event_id: str) -> None: ...
    async def outbox_error(self, event_id: str, error: str) -> None: ...
    async def pending_outbox(self, limit: int = 1000) -> list[dict[str, Any]]: ...


class MemoryStore:
    """Test-only store. Production startup requires MongoStore."""

    def __init__(self) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.outbox: dict[str, dict[str, Any]] = {}

    async def create_run(self, document: dict[str, Any]) -> None:
        self.runs[document["run_id"]] = copy.deepcopy(document)

    async def update_run(self, run_id: str, values: dict[str, Any]) -> None:
        self.runs[run_id].update(copy.deepcopy(values))
        self.runs[run_id]["updated_at"] = now()

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        value = self.runs.get(run_id)
        return copy.deepcopy(value) if value else None

    async def enqueue(self, event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        return self.outbox.setdefault(
            event_id,
            {"event_id": event_id, "event": copy.deepcopy(event), "state": "pending", "attempts": 0},
        )

    async def ack(self, event_id: str) -> None:
        self.outbox[event_id].update({"state": "acked", "acked_at": now()})

    async def outbox_error(self, event_id: str, error: str) -> None:
        item = self.outbox[event_id]
        item.update({"state": "pending", "last_error": error})
        item["attempts"] += 1

    async def pending_outbox(self, limit: int = 1000) -> list[dict[str, Any]]:
        values = [item for item in self.outbox.values() if item["state"] == "pending"]
        values.sort(key=lambda item: item.get("created_at", ""))
        return copy.deepcopy(values[:limit])


class MongoStore:
    DB_NAME = "flow_runtime"
    RUNS = "runs"
    OUTBOX = "lineage_outbox"

    def __init__(self, uri: str, database: str = DB_NAME) -> None:
        self.db = MongoClient(uri)[database]

    async def ensure_indexes(self) -> None:
        def work() -> None:
            self.db[self.RUNS].create_index([("run_id", ASCENDING)], unique=True)
            self.db[self.RUNS].create_index([("status", ASCENDING), ("updated_at", ASCENDING)])
            self.db[self.OUTBOX].create_index([("event_id", ASCENDING)], unique=True)
            self.db[self.OUTBOX].create_index([("state", ASCENDING), ("created_at", ASCENDING)])

        await asyncio.to_thread(work)

    async def create_run(self, document: dict[str, Any]) -> None:
        await asyncio.to_thread(self.db[self.RUNS].insert_one, copy.deepcopy(document))

    async def update_run(self, run_id: str, values: dict[str, Any]) -> None:
        clean = copy.deepcopy(values)
        clean["updated_at"] = now()
        await asyncio.to_thread(
            self.db[self.RUNS].update_one, {"run_id": run_id}, {"$set": clean}
        )

    async def get_run(self, run_id: str) -> dict[str, Any] | None:
        value = await asyncio.to_thread(self.db[self.RUNS].find_one, {"run_id": run_id}, {"_id": 0})
        return value

    async def enqueue(self, event_id: str, event: dict[str, Any]) -> dict[str, Any]:
        created = now()
        await asyncio.to_thread(
            self.db[self.OUTBOX].update_one,
            {"event_id": event_id},
            {
                "$setOnInsert": {
                    "event_id": event_id,
                    "event": copy.deepcopy(event),
                    "state": "pending",
                    "attempts": 0,
                    "created_at": created,
                }
            },
            upsert=True,
        )
        result = await asyncio.to_thread(
            self.db[self.OUTBOX].find_one, {"event_id": event_id}, {"_id": 0}
        )
        assert result is not None
        return result

    async def ack(self, event_id: str) -> None:
        await asyncio.to_thread(
            self.db[self.OUTBOX].update_one,
            {"event_id": event_id},
            {"$set": {"state": "acked", "acked_at": now(), "last_error": None}},
        )

    async def outbox_error(self, event_id: str, error: str) -> None:
        await asyncio.to_thread(
            self.db[self.OUTBOX].update_one,
            {"event_id": event_id},
            {"$set": {"state": "pending", "last_error": error}, "$inc": {"attempts": 1}},
        )

    async def pending_outbox(self, limit: int = 1000) -> list[dict[str, Any]]:
        def work() -> list[dict[str, Any]]:
            return list(
                self.db[self.OUTBOX]
                .find({"state": "pending"}, {"_id": 0})
                .sort("created_at", ASCENDING)
                .limit(limit)
            )

        return await asyncio.to_thread(work)

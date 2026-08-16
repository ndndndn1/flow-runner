from __future__ import annotations

from dataclasses import dataclass

import httpx

from .config import Settings
from .lineage import FlowprintAdapter, LineageSink, OutboxPublisher
from .registry import DeploymentRegistry
from .runner import FlowRunner
from .store import MemoryStore, MongoStore, RunStore


@dataclass
class Services:
    runner: FlowRunner
    store: RunStore
    client: httpx.AsyncClient

    async def close(self) -> None:
        await self.client.aclose()


async def build_services(
    settings: Settings, *, allow_memory: bool = False, transport: httpx.AsyncBaseTransport | None = None
) -> Services:
    client = httpx.AsyncClient(timeout=settings.timeout_seconds, transport=transport)
    if settings.mongo_uri:
        store: RunStore = MongoStore(settings.mongo_uri, settings.mongo_database)
        await store.ensure_indexes()  # type: ignore[attr-defined]
    elif allow_memory:
        store = MemoryStore()
    else:
        await client.aclose()
        raise RuntimeError("MONGO_AUTO_URI or MONGODB_URI is required for durable execution")
    registry = DeploymentRegistry.from_path(settings.registry_path, client)
    sink = LineageSink(client, settings.lineage_url, settings.lineage_token)
    publisher = OutboxPublisher(store, sink)
    runner = FlowRunner(
        store=store,
        registry=registry,
        publisher=publisher,
        client=client,
        flowprint=FlowprintAdapter(client, settings.flowprint_url),
        max_payload_bytes=settings.max_payload_bytes,
    )
    return Services(runner, store, client)


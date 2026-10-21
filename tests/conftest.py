import os
import tempfile
from typing import Any

import pytest

from pi_durable import MemoryStorage, open_jsonl_storage, open_sqlite_storage


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class StorageFactory:
    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.directory = tempfile.mkdtemp(prefix=f"pi-durable-{kind}-")
        self.opened: list[Any] = []

    async def open(self) -> Any:
        if self.kind == "memory":
            storage = MemoryStorage()
        elif self.kind == "jsonl":
            storage = await open_jsonl_storage(os.path.join(self.directory, "store"))
        else:
            storage = await open_sqlite_storage(os.path.join(self.directory, "store.sqlite"))
        self.opened.append(storage)
        return storage


@pytest.fixture(params=["memory", "jsonl", "sqlite"])
async def storage_factory(request: pytest.FixtureRequest) -> StorageFactory:
    factory = StorageFactory(request.param)
    yield factory

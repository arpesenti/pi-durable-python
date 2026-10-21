"""Shared Session test harness: controlled storage, publications, and helpers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from pi_durable import MemoryStorage
from pi_durable.context import BACKGROUND_CONTEXT, Context
from pi_durable.ids import ConversationId, DocumentId, Seq
from pi_durable.json import copy_json
from pi_durable.session.session import SessionImpl
from pi_durable.types import (
    CommitPublication,
    DocumentAddress,
    DocumentPoint,
    DocumentRecord,
    Storage,
    StorageWrite,
    StoredDocument,
)

context: Context = BACKGROUND_CONTEXT


async def flush() -> None:
    """Let queued watch deliveries, commit listeners, and scheduled drain tasks run."""

    await asyncio.sleep(0)
    await asyncio.sleep(0)


class Deferred:
    """A one-shot signal: resolvable from a callback and awaitable by the test."""

    def __init__(self) -> None:
        self._future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    @property
    def promise(self) -> asyncio.Future[None]:
        return self._future

    def resolve(self) -> None:
        if not self._future.done():
            self._future.set_result(None)


async def eventually(check: Callable[[], Awaitable[bool]], timeout: float = 2.0) -> None:
    """Await until ``check`` is true, polling like the upstream test helper does."""

    deadline = time.monotonic() + timeout
    while True:
        if await check():
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.01)


class Gate:
    """A gate that holds calls until released and reports when the first held call arrives."""

    def __init__(self) -> None:
        self._gate = asyncio.Event()
        self.entered = asyncio.Event()

    def release(self) -> None:
        self._gate.set()

    async def wait(self) -> None:
        self.entered.set()
        await self._gate.wait()


class ControlledStorage(MemoryStorage):
    """Memory storage with observable commits, held calls, and injected commit failures."""

    def __init__(self) -> None:
        super().__init__()
        self.admitted_commits: list[Sequence[StorageWrite]] = []
        self.commits: list[list[StorageWrite]] = []
        self.mint_count = 0
        self.document_read_count = 0
        self._commit_gate: Gate | None = None
        self._find_gate: Gate | None = None
        self._commit_failure: Exception | None = None

    def hold_commits(self) -> Gate:
        self._commit_gate = Gate()
        return self._commit_gate

    def hold_find_document(self) -> Gate:
        self._find_gate = Gate()
        return self._find_gate

    def crash(self) -> None:
        self._commit_gate = None

    def fail_next_commit(self, error: Exception) -> None:
        self._commit_failure = error

    def reopen(self) -> ControlledStorage:
        self._closed = False
        return self

    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        self.admitted_commits.append(writes)
        self.commits.append(copy_json(list(writes)))  # type: ignore[arg-type]
        gate = self._commit_gate
        if gate is not None:
            await gate.wait()
        failure = self._commit_failure
        if failure is not None:
            self._commit_failure = None
            raise failure
        return await super().commit(writes, context)

    async def mint_id(self) -> int:
        self.mint_count += 1
        return await super().mint_id()

    async def document(self, id: DocumentId, at: DocumentPoint, context: Context) -> StoredDocument | None:
        self.document_read_count += 1
        return await super().document(id, at, context)

    async def find_document(
        self,
        address: DocumentAddress,
        at: DocumentPoint,
        context: Context,
    ) -> DocumentRecord | None:
        gate = self._find_gate
        if gate is not None:
            await gate.wait()
        return await super().find_document(address, at, context)


def open_test_session() -> tuple[ControlledStorage, SessionImpl, list[CommitPublication]]:
    storage = ControlledStorage()
    session = SessionImpl(cast(Storage, storage), lambda: 1_000, lambda: 600_000)
    publications: list[CommitPublication] = []
    session.subscribe_commits(lambda publication, _context: publications.append(publication))
    return storage, session, publications


def document_changes(publication: CommitPublication) -> list[dict[str, Any]]:
    return [
        cast(dict[str, Any], change)
        for change in publication["changes"]
        if change["type"] == "document"
    ]


async def create_conversation(session: SessionImpl) -> ConversationId:
    async def create(tx: Any) -> ConversationId:
        conversation = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        return conversation["id"]

    return await session.commit(create, context)


__all__ = [
    "ControlledStorage",
    "Deferred",
    "Gate",
    "context",
    "create_conversation",
    "document_changes",
    "eventually",
    "flush",
    "open_test_session",
]

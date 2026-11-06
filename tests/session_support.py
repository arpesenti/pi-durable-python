"""Shared Session test harness: controlled storage, publications, and helpers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from pi_durable import MemoryStorage
from pi_durable.context import BACKGROUND_CONTEXT, Context
from pi_durable.ids import ConversationId, DocumentId
from pi_durable.json import copy_json
from pi_durable.session.session import SessionImpl
from pi_durable.types import (
    CommitPublication,
    CommitReceipt,
    CommitResult,
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

    async def commit_batches(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        commit_context: Context,
    ) -> CommitResult:
        # One Session commit is one call; a batch is one commit, so every batch is recorded.
        for batch in batches:
            self.admitted_commits.append(batch)
            self.commits.append(copy_json(list(batch)))  # type: ignore[arg-type]
        gate = self._commit_gate
        if gate is not None:
            await gate.wait()
        failure = self._commit_failure
        if failure is not None:
            self._commit_failure = None
            raise failure
        return await super().commit_batches(batches, receive, commit_context)

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
    observe_commits(session, lambda publication, _context: publications.append(publication))
    return storage, session, publications


class CommitObserverRecorder:
    """A host ``CommitObserver`` handing each stored publication to a plain callable (spec §9.1).

    A host observer that throws is reported and detached, unlike the Session's own follower, whose
    throw fails the Session: a test that follows what was stored uses :func:`observe_commits`, and a
    test of what the host is handed uses this one.
    """

    def __init__(self, on_publication: Callable[[CommitPublication, Context], Any]) -> None:
        self._on_publication = on_publication
        self.ends: list[Any] = []

    def advance(self, publication: CommitPublication, context: Context) -> Any:
        # What the callable returns goes back to the Session's guard, which awaits a rejected
        # awaitable there and reports it: a host observer is contained, never fatal (§9.1).
        return self._on_publication(publication, context)

    def end(self, end: Any) -> None:
        self.ends.append(end)


def observe_commits_as_host(
    session: Any,
    on_publication: Callable[[CommitPublication, Context], Any],
) -> CommitObserverRecorder:
    """Attach ``on_publication`` as the host's own commit observer of ``session``."""

    observer = CommitObserverRecorder(on_publication)
    session.observe_commits(observer)
    return observer


def observe_commits(
    session: Any,
    on_publication: Callable[[CommitPublication, Context], Any],
) -> Callable[[], None]:
    """Follow what Storage stored, the way the Session's own components do; return its unsubscribe.

    This is the Session's internal follower seam, not the host's :class:`CommitObserver`: it is
    handed the publication on the line, which is what a test that inspects what was committed right
    after the commit needs. A test of the host surface attaches its own ``CommitObserver`` through
    ``Session.observe_commits()`` (spec §9.1).
    """

    return cast(Callable[[], None], session.observe_stored_commits(on_publication))


class Mirror:
    """A host state observer recording every call, its end, and the value it mirrors.

    Tests that only need a reading use it through :func:`view_of` or :func:`graph_of`; a test that
    follows a value over time attaches one directly; a test of the observer surface itself writes
    its own observer and asserts on the calls it was handed (spec §9.1).
    """

    def __init__(self) -> None:
        self.value: Any = None
        self.calls: list[Any] = []
        self.ends: list[Any] = []
        self._observation: Any = None

    def bind(self, observation: Any) -> Mirror:
        """Record the observation this mirror was attached through, so ``dispose`` can detach it."""

        self._observation = observation
        self.value = observation.value
        return self

    def dispose(self) -> None:
        """Detach the attachment; the mirrored value stays for the test to assert on."""

        if self._observation is not None:
            self._observation.detach()

    def advance(self, call: Any, _context: Any = None) -> None:
        self.calls.append(call)
        if call["kind"] == "value" and not call["retired"]:
            self.value = call["value"]

    def end(self, end: Any) -> None:
        self.ends.append(end)


async def attached_graph(harness: Any, context: Context) -> Mirror:
    """A mirror of the task graph that follows every publication until it is disposed."""

    mirror = Mirror()
    return mirror.bind(await harness.observe_task_graph(mirror, context))


async def attached_view(harness: Any, conversation_id: Any, context: Context) -> Mirror:
    """A mirror of one conversation's view that follows every publication until it is disposed."""

    mirror = Mirror()
    return mirror.bind(await harness.observe_conversation(conversation_id, mirror, context))


async def view_of(harness: Any, conversation_id: Any, context: Context) -> Any:
    """One conversation's current view, read the way committed state is read: by attaching."""

    mirror = Mirror()
    observation = await harness.observe_conversation(conversation_id, mirror, context)
    value = observation.value
    observation.detach()
    return value


async def graph_of(harness: Any, context: Context) -> Any:
    """The task graph as it stands, read by attaching an observer of the mount."""

    mirror = Mirror()
    observation = await harness.observe_task_graph(mirror, context)
    value = observation.value
    observation.detach()
    return value


def document_copy_changes(publication: CommitPublication) -> list[dict[str, Any]]:
    return [
        cast(dict[str, Any], change)
        for change in publication["changes"]
        if change["type"] == "document.copy"
    ]


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
    "CommitObserverRecorder",
    "ControlledStorage",
    "Deferred",
    "Gate",
    "context",
    "create_conversation",
    "document_changes",
    "document_copy_changes",
    "eventually",
    "flush",
    "open_test_session",
]

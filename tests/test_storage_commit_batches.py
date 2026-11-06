"""Translated v1.5.0 batched ``Storage.commit_batches()`` contract (#38), over every backend (#40).

One call stores a group of commit batches, each an atomic commit with its own sequence checked
against the state the batches before it left, and resolves with how many were stored and, when one
was refused, the refusal. A refusal is a resolved outcome of Durable's own checks; an error of the
database or driver is a rejected call that stores nothing the backend can roll back -- the two are
told apart in every case below. The batched call is the only commit shape the protocol declares
(#41), so the seeding cases here go through the same helper every other test seeds with.

The SQLite cases also hold the backend to the counted-run member 1.5.0 adds: the commit is the
member's first caller, so an allocation statement that changed no row rejects the call instead of
handing the same sequence out twice, and a driver written against the old member set fails loudly.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from session_support import observe_commits

from pi_durable import MemoryStorage
from pi_durable.context import BACKGROUND_CONTEXT as context
from pi_durable.context import Context
from pi_durable.env import FileError, Result, err
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.harness.types import ReportInfo
from pi_durable.ids import (
    ROOT_CONVERSATION_ID,
    ConversationId,
    DocumentId,
    EntryId,
    Seq,
)
from pi_durable.session.session import SessionImpl
from pi_durable.storage.jsonl import JsonlStorage, JsonlStoragePoisonedError
from pi_durable.storage.sqlite import SqliteStorage
from pi_durable.storage.sqlite.database import SqliteDatabase, SqliteExecutor, SqliteValue
from pi_durable.storage.sqlite.posix import open_posix_sqlite_database
from pi_durable.testing import commit_writes
from pi_durable.types import (
    CommitPublication,
    CommitReceipt,
    CommitResult,
    PublishingStorage,
    Storage,
    StorageWrite,
)

_DIRECTORIES: list[Path] = []
_OPEN: list[Any] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Any:
    yield
    while _OPEN:
        await _OPEN.pop().close(context)
    while _DIRECTORIES:
        shutil.rmtree(_DIRECTORIES.pop(), ignore_errors=True)


def _temp_directory() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-batches-"))
    _DIRECTORIES.append(directory)
    return directory


async def _memory() -> Storage:
    return MemoryStorage()


async def _sqlite() -> Storage:
    database = await open_posix_sqlite_database(str(_temp_directory() / "storage.sqlite"))
    storage = await SqliteStorage.open(database)
    _OPEN.append(storage)
    return storage


async def _jsonl() -> Storage:
    directory = _temp_directory()
    storage = await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)
    _OPEN.append(storage)
    return storage


_BACKENDS = pytest.mark.parametrize(
    "create_storage",
    [_memory, _sqlite, _jsonl],
    ids=["memory", "sqlite", "jsonl"],
)

_RECEIPTS: list[tuple[int, Seq]] = []


def _receive(receipts: list[tuple[int, Seq]]) -> CommitReceipt:
    def receive(index: int, seq: Seq) -> None:
        receipts.append((index, seq))

    return receive


def _entry(id: int, conversation_id: ConversationId, kind: str = "message") -> Any:
    return {"id": id, "conversationId": conversation_id, "kind": kind}


def _document(id: int, kind: str) -> Any:
    return {"id": id, "kind": kind, "scope": {"kind": "session"}}


def _session_document_record(id: int, kind: str) -> Any:
    """A session document record as a plain commit writes it: the newest history, one branch."""

    return {**_document(id, kind), "history": "latest", "fork": "initial"}


async def _create_root(storage: Storage) -> ConversationId:
    await commit_writes(storage, [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context)
    return ROOT_CONVERSATION_ID


# ── The stored group and its receipts ───────────────────────────────────────


@_BACKENDS
async def test_a_call_stores_every_batch_with_its_own_sequence(create_storage: Any) -> None:
    storage = await create_storage()
    root_id = await _create_root(storage)
    first_id = EntryId(await storage.mint_id())
    last_id = EntryId(await storage.mint_id())
    document_id = DocumentId(await storage.mint_id())
    receipts: list[tuple[int, Seq]] = []
    read_at_first_receipt: Any = None

    def receive(index: int, seq: Seq) -> None:
        nonlocal read_at_first_receipt
        receipts.append((index, seq))
        if read_at_first_receipt is None:
            # Receipts follow the last durable batch: a read started at the first observes them all.
            read_at_first_receipt = storage.entry(last_id, context)

    batches: list[list[StorageWrite]] = [
        [
            {"type": "entry", "value": _entry(first_id, root_id)},
            {
                "type": "document.create",
                "record": _session_document_record(document_id, "batch.doc"),
                "content": {"kind": "base", "version": 1, "value": {"count": 1}},
            },
        ],
        # Changes the document the batch before it created: each batch is checked against the
        # state the earlier ones left.
        [
            {
                "type": "document.change",
                "id": document_id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 2]]},
            }
        ],
        [{"type": "entry", "value": _entry(last_id, root_id)}],
    ]

    result = await storage.commit_batches(batches, receive, context)

    assert result == {"stored": 3}
    assert [index for index, _ in receipts] == [0, 1, 2]
    first_seq, second_seq, last_seq = (seq for _, seq in receipts)
    assert second_seq > first_seq
    assert last_seq > second_seq
    read = await read_at_first_receipt
    assert read is not None and read["commitSeq"] == last_seq
    first_read = await storage.entry(first_id, context)
    assert first_read is not None and first_read["commitSeq"] == first_seq
    second_read = await storage.entry(last_id, context)
    assert second_read is not None and second_read["commitSeq"] == last_seq
    stored = await storage.document(document_id, "current", context)
    assert stored is not None and stored["value"] == {"count": 2}
    # The batch after the group allocates above it: every call draws the one sequence.
    next_seq = await commit_writes(
        storage,
        [{"type": "entry", "value": _entry(await storage.mint_id(), root_id)}], context
    )
    assert next_seq > last_seq


@_BACKENDS
async def test_an_empty_group_stores_nothing_and_takes_no_receipt(create_storage: Any) -> None:
    storage = await create_storage()
    await _create_root(storage)
    receipts: list[tuple[int, Seq]] = []

    assert await storage.commit_batches([], _receive(receipts), context) == {"stored": 0}
    assert receipts == []


# ── Refusal: a resolved outcome that keeps the earlier batches ──────────────


@_BACKENDS
async def test_the_first_batch_durable_refuses_leaves_the_earlier_ones_stored(create_storage: Any) -> None:
    storage = await create_storage()
    root_id = await _create_root(storage)
    kept_id = EntryId(await storage.mint_id())
    second_id = EntryId(await storage.mint_id())
    after_id = EntryId(await storage.mint_id())
    receipts: list[tuple[int, Seq]] = []

    result = await storage.commit_batches(
        [
            [{"type": "entry", "value": _entry(kept_id, root_id)}],
            [{"type": "entry", "value": _entry(second_id, root_id)}],
            # Durable's own global-ID rule: one ID belongs to one record, and the batch before it
            # already stored this one.
            [{"type": "entry", "value": _entry(kept_id, root_id)}],
            [{"type": "entry", "value": _entry(after_id, root_id)}],
        ],
        _receive(receipts),
        context,
    )

    assert result["stored"] == 2
    assert str(kept_id) in str(result["refused"])
    # Receipts only for stored batches, in order, one per batch, before the call settled.
    assert [index for index, _ in receipts] == [0, 1]
    kept = await storage.entry(kept_id, context)
    assert kept is not None and kept["commitSeq"] == receipts[0][1]
    second = await storage.entry(second_id, context)
    assert second is not None and second["commitSeq"] == receipts[1][1]
    # The batch after the refusal is not stored either: the call stopped at the refusal.
    assert await storage.entry(after_id, context) is None
    # A refusal is resolved, not thrown: the storage goes on serving batched commits.
    following = await commit_writes(
        storage,
        [{"type": "entry", "value": _entry(after_id, root_id)}], context
    )
    assert following > receipts[1][1]
    assert await storage.entry(after_id, context) is not None


@_BACKENDS
async def test_a_refused_batch_writes_nothing_of_its_own(create_storage: Any) -> None:
    """A refused batch is checked whole before it writes: a partial batch never lands."""

    storage = await create_storage()
    root_id = await _create_root(storage)
    entry_id = EntryId(await storage.mint_id())
    document_id = DocumentId(await storage.mint_id())
    receipts: list[tuple[int, Seq]] = []

    result = await storage.commit_batches(
        [
            [
                {"type": "entry", "value": _entry(entry_id, root_id)},
                # The same document asked for two contents: Durable refuses the whole batch.
                {
                    "type": "document.create",
                    "record": _document(document_id, "batch.doc"),
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
                {
                    "type": "document.change",
                    "id": document_id,
                    "content": {"kind": "delta", "version": 1, "ops": [["s", ["a"], 1]]},
                },
            ]
        ],
        _receive(receipts),
        context,
    )

    assert result["stored"] == 0
    assert "more than one content command" in str(result["refused"])
    assert receipts == []
    assert await storage.entry(entry_id, context) is None
    assert await storage.document(document_id, "current", context) is None


# ── Rejection: a database or driver error, which stores nothing ─────────────


class _FailingCheckHandle:
    """A transaction handle that fails its global-ID reads from the nth one on."""

    def __init__(self, handle: SqliteExecutor, owner: _FailingCheckDatabase) -> None:
        self._handle = handle
        self._owner = owner

    def exec(self, sql: str) -> Awaitable[None]:
        return self._handle.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._handle.run(sql, *params)

    def run_counted(self, sql: str, *params: SqliteValue) -> Awaitable[int]:
        return self._handle.run_counted(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        if "record_type FROM" in sql:
            self._owner.check_reads += 1
            if self._owner.check_reads >= self._owner.fail_from:
                return self._owner.fail()
        return self._handle.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._handle.all(sql, *params)


class _FailingCheckDatabase:
    """A database facade that hands out a handle whose check reads fail, as a locked driver's would.

    ``mode`` says when a failing call reports it: when the caller awaits it, which is what an
    adapter returning a failed future does, or at the call itself, which is what a driver that
    raises outright does. Both are a rejected commit, never a refusal.
    """

    def __init__(self, delegate: SqliteDatabase, fail_from: int, mode: str = "await") -> None:
        self._delegate = delegate
        self.fail_from = fail_from
        self.check_reads = 0
        self._mode = mode

    def fail(self) -> Awaitable[dict[str, Any] | None]:
        if self._mode == "call":
            raise RuntimeError("SQLite is locked")

        async def failed() -> dict[str, Any] | None:
            raise RuntimeError("SQLite is locked")

        return failed()

    def exec(self, sql: str) -> Awaitable[None]:
        return self._delegate.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._delegate.run(sql, *params)

    def run_counted(self, sql: str, *params: SqliteValue) -> Awaitable[int]:
        return self._delegate.run_counted(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        return self._delegate.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._delegate.all(sql, *params)

    def transaction(self, callback: Callable[[SqliteExecutor], Awaitable[Any]]) -> Awaitable[Any]:
        # A commit checks its batches on the transaction handle, so that is the handle that fails.
        return self._delegate.transaction(lambda handle: callback(_FailingCheckHandle(handle, self)))

    def close(self) -> Awaitable[None]:
        return self._delegate.close()


@pytest.mark.parametrize("mode", ["await", "call"])
async def test_a_database_error_during_a_check_rejects_the_call_and_stores_nothing(
    mode: str,
) -> None:
    """The same group the refusal case stored: a failed database call rejects, and nothing stands."""

    directory = _temp_directory()
    path = str(directory / "storage.sqlite")
    seed = await SqliteStorage.open(await open_posix_sqlite_database(path))
    root_id = await _create_root(seed)
    kept_id = EntryId(await seed.mint_id())
    second_id = EntryId(await seed.mint_id())
    await seed.close(context)

    storage = await SqliteStorage.open(
        _FailingCheckDatabase(await open_posix_sqlite_database(path), fail_from=2, mode=mode)
    )
    _OPEN.append(storage)
    receipts: list[tuple[int, Seq]] = []
    with pytest.raises(RuntimeError, match="SQLite is locked"):
        await storage.commit_batches(
            [
                [{"type": "entry", "value": _entry(kept_id, root_id)}],
                [{"type": "entry", "value": _entry(second_id, root_id)}],
            ],
            _receive(receipts),
            context,
        )

    # Unlike a refusal: no receipt at all, and the batch whose check passed rolled back with it.
    assert receipts == []
    assert await storage.entry(kept_id, context) is None
    assert await storage.entry(second_id, context) is None


async def test_a_rejected_batch_call_leaves_the_storage_refusing_only_its_own_checks() -> None:
    """The two outcomes stay told apart: after a rejected call, a refusal still keeps its prefix.

    The failure the checks watch for must not go sticky -- a later group is judged on its own
    database calls, so Durable's own rule still ends it as a resolved refusal.
    """

    directory = _temp_directory()
    path = str(directory / "storage.sqlite")
    seed = await SqliteStorage.open(await open_posix_sqlite_database(path))
    root_id = await _create_root(seed)
    rejected_id = EntryId(await seed.mint_id())
    kept_id = EntryId(await seed.mint_id())
    await seed.close(context)

    failing = _FailingCheckDatabase(await open_posix_sqlite_database(path), fail_from=1)
    storage = await SqliteStorage.open(failing)
    _OPEN.append(storage)
    receipts: list[tuple[int, Seq]] = []
    with pytest.raises(RuntimeError, match="SQLite is locked"):
        await storage.commit_batches(
            [[{"type": "entry", "value": _entry(rejected_id, root_id)}]],
            _receive(receipts),
            context,
        )
    assert receipts == []
    assert await storage.entry(rejected_id, context) is None

    failing.fail_from = 1_000_000
    refused = await storage.commit_batches(
        [
            [{"type": "entry", "value": _entry(kept_id, root_id)}],
            [{"type": "conversation", "value": {"id": root_id}}],
        ],
        _receive(receipts),
        context,
    )

    assert refused["stored"] == 1
    assert "already belongs to conversation" in str(refused["refused"])
    assert [index for index, _ in receipts] == [0]
    assert await storage.entry(kept_id, context) is not None
    assert await storage.entry(rejected_id, context) is None


class _FailingMainEnv(PosixExecutionEnv):
    """A file system whose next ``main.jsonl`` append fails, as a full disk's would."""

    def __init__(self, cwd: str) -> None:
        super().__init__(cwd=cwd)
        self.armed = False

    async def append_file(
        self, path: str, content: str | bytes, append_context: Context
    ) -> Result[None, FileError]:
        if self.armed and path.endswith("main.jsonl"):
            return err(FileError("unknown", "disk is full", path))
        return await super().append_file(path, content, append_context)


async def test_a_file_failure_after_a_batch_applied_rejects_and_poisons_the_storage() -> None:
    """JSONL runs memory ahead of the file, so a failed append rejects and leaves nothing to read."""

    directory = _temp_directory()
    env = _FailingMainEnv(cwd=str(directory))
    storage = await JsonlStorage.open(str(directory), env, context)
    _OPEN.append(storage)
    root_id = await _create_root(storage)
    first_id = EntryId(await storage.mint_id())
    second_id = EntryId(await storage.mint_id())
    receipts: list[tuple[int, Seq]] = []
    env.armed = True

    with pytest.raises(JsonlStoragePoisonedError) as raised:
        await storage.commit_batches(
            [
                [{"type": "entry", "value": _entry(first_id, root_id)}],
                [{"type": "entry", "value": _entry(second_id, root_id)}],
            ],
            _receive(receipts),
            context,
        )

    assert "disk is full" in str(raised.value.__cause__)

    # Unlike a refusal: no receipt, no stored batch, and the storage is poisoned rather than reused
    # with memory that ran ahead of the file.
    assert receipts == []
    with pytest.raises(JsonlStoragePoisonedError):
        await storage.entry(first_id, context)


# ── The testing helper beside the batch form ────────────────────────────────


@_BACKENDS
async def test_commit_writes_commits_one_batch_and_resolves_with_its_sequence(create_storage: Any) -> None:
    storage = await create_storage()
    entry_id = EntryId(await storage.mint_id())

    seq = await commit_writes(
        storage,
        [
            {"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}},
            {"type": "entry", "value": _entry(entry_id, ROOT_CONVERSATION_ID)},
        ],
        context,
    )

    assert seq == 1
    stored = await storage.entry(entry_id, context)
    assert stored is not None and stored["commitSeq"] == seq


@_BACKENDS
async def test_commit_writes_raises_what_the_storage_refused(create_storage: Any) -> None:
    storage = await create_storage()
    await _create_root(storage)
    entry_id = EntryId(await storage.mint_id())
    document_id = DocumentId(await storage.mint_id())

    with pytest.raises(Exception, match="more than one content command"):
        await commit_writes(
            storage,
            [
                {"type": "entry", "value": _entry(entry_id, ROOT_CONVERSATION_ID)},
                {
                    "type": "document.create",
                    "record": _document(document_id, "batch.doc"),
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
                {
                    "type": "document.change",
                    "id": document_id,
                    "content": {"kind": "delta", "version": 1, "ops": []},
                },
            ],
            context,
        )

    assert await storage.entry(entry_id, context) is None
    assert await storage.document(document_id, "current", context) is None


# ── Minting: above every ID minted since opening and every ID stored ────────


@_BACKENDS
async def test_a_minted_id_is_above_every_id_minted_and_every_id_stored(create_storage: Any) -> None:
    storage = await create_storage()
    await _create_root(storage)
    high_id = 9_000
    await commit_writes(
        storage,
        [{"type": "document.create", "record": _document(high_id, "batch.doc"),
          "content": {"kind": "base", "version": 1, "value": {}}}],
        context,
    )

    minted = [await storage.mint_id() for _ in range(3)]

    assert minted == sorted(set(minted))
    assert min(minted) > high_id


async def test_a_minted_id_stays_above_every_stored_id_across_reopen() -> None:
    directory = _temp_directory()
    storage = await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)
    root_id = await _create_root(storage)
    minted_before = [await storage.mint_id() for _ in range(2)]
    stored_high = 5_000
    await commit_writes(
        storage,
        [
            {"type": "entry", "value": _entry(minted_before[0], root_id)},
            {"type": "document.create", "record": _document(stored_high, "batch.doc"),
             "content": {"kind": "base", "version": 1, "value": {}}},
        ],
        context,
    )
    await storage.close(context)

    reopened = await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)
    _OPEN.append(reopened)

    after_reopen = [await reopened.mint_id() for _ in range(2)]
    assert min(after_reopen) > max(minted_before + [stored_high])
    assert after_reopen == sorted(set(after_reopen))


async def test_a_minted_id_stays_above_every_stored_id_across_sqlite_reopen() -> None:
    directory = _temp_directory()
    path = str(directory / "storage.sqlite")
    storage = await SqliteStorage.open(await open_posix_sqlite_database(path))
    await _create_root(storage)
    minted_before = [await storage.mint_id() for _ in range(2)]
    stored_high = 7_000
    await commit_writes(
        storage,
        [{"type": "document.create", "record": _document(stored_high, "batch.doc"),
          "content": {"kind": "base", "version": 1, "value": {}}}],
        context,
    )
    await storage.close(context)

    reopened = await SqliteStorage.open(await open_posix_sqlite_database(path))
    _OPEN.append(reopened)

    after_reopen = [await reopened.mint_id() for _ in range(2)]
    assert min(after_reopen) > max(minted_before + [stored_high])


# ── The optional publication scope ──────────────────────────────────────────


class _PublishingStorage:
    """A storage that owns a batch across a commit and its publication, as a store host's does."""

    def __init__(self) -> None:
        self._inner = MemoryStorage()
        self.events: list[str] = []
        self.open_batches = 0

    def publishing(self, publish: Callable[[], None], publish_context: Context) -> None:
        self.events.append(f"publishing:{self.open_batches}")
        try:
            publish()
        finally:
            self.open_batches -= 1

    async def commit_batches(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        commit_context: Context,
    ) -> CommitResult:
        self.open_batches += 1
        result = await self._inner.commit_batches(batches, receive, commit_context)
        self.events.append("commit")
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class _ThrowingPublishingStorage(_PublishingStorage):
    """A storage whose publication scope throws without ever running the publication."""

    def publishing(self, publish: Callable[[], None], publish_context: Context) -> None:
        self.events.append("publishing")
        try:
            raise RuntimeError("the store's batch is closed")
        finally:
            self.open_batches -= 1


class _ReportingSession(SessionImpl):
    """A Session that keeps the reports it makes, so a case can name what one cost."""

    def __init__(self, storage: Storage) -> None:
        super().__init__(storage, lambda: 1_000, lambda: 600_000)
        self.reports: list[tuple[BaseException, ReportInfo | None]] = []

    def report(self, error: BaseException, info: ReportInfo | None = None) -> None:
        self.reports.append((error, info))


async def test_a_storage_that_owns_a_publication_scope_gets_it_inside_the_commit() -> None:
    storage = _PublishingStorage()
    session = SessionImpl(storage, lambda: 1_000, lambda: 600_000)
    observe_commits(session, 
        lambda publication, _context: storage.events.append(f"publish:{storage.open_batches}")
    )

    async def create(tx: Any) -> None:
        await tx.create_conversation({"ownership": {"kind": "ownerless"}})

    await session.commit(create, context)

    # The publication ran inside the storage's own open batch, after the commit and before the
    # Session's call ended, and the batch was closed again afterwards.
    assert storage.events == ["commit", "publishing:1", "publish:1"]
    assert storage.open_batches == 0


@_BACKENDS
async def test_the_bundled_backends_omit_the_publication_scope(create_storage: Any) -> None:
    """No bundled backend has a publication scope of its own (ADR 0003)."""

    storage = await create_storage()

    assert not isinstance(storage, PublishingStorage)
    assert not hasattr(storage, "publishing")


def test_the_publication_scope_is_a_companion_of_the_storage_contract() -> None:
    """The member is optional: ``Storage`` does not require it, the companion protocol detects it."""

    assert "publishing" not in Storage.__dict__
    assert isinstance(_PublishingStorage(), PublishingStorage)


async def test_a_publication_scope_that_throws_is_reported_and_the_commit_is_still_published() -> None:
    """A stored commit must be published: the Session reports the scope and publishes directly."""

    storage = _ThrowingPublishingStorage()
    session = _ReportingSession(storage)
    published: list[Seq] = []

    def on_publication(publication: CommitPublication, _publish_context: Context) -> None:
        published.append(publication["seq"])
        storage.events.append(f"publish:{storage.open_batches}")

    observe_commits(session, on_publication)

    async def create(tx: Any) -> None:
        await tx.create_conversation({"ownership": {"kind": "ownerless"}})

    await session.commit(create, context)

    # The scope ran once and threw, then the publication ran directly: the commit is stored, so it
    # is published, and the error is a report rather than a failed commit.
    assert storage.events == ["commit", "publishing", "publish:0"]
    assert published == [1]
    assert storage.open_batches == 0
    assert [info for _error, info in session.reports] == [{"kind": "listener_error"}]
    assert isinstance(session.reports[0][0], RuntimeError)


# ── The counted-run executor member ─────────────────────────────────────────


def test_the_executor_protocol_carries_the_counted_run_member() -> None:
    """1.5.0 adds ``runCounted`` to ``SqliteExecutor``; #40 is the member's first caller."""

    assert "run_counted" in SqliteExecutor.__dict__


async def test_the_counted_run_member_reports_the_rows_its_statement_changed() -> None:
    """The bundled driver answers with the number of rows the statement changed."""

    database = await open_posix_sqlite_database(str(_temp_directory() / "counted.sqlite"))

    async def create_table(handle: SqliteExecutor) -> None:
        await handle.exec("CREATE TABLE counted (n INTEGER PRIMARY KEY)")

    await database.transaction(create_table)

    async def insert(handle: SqliteExecutor) -> int:
        return await handle.run_counted("INSERT INTO counted (n) VALUES (?), (?)", 1, 2)

    assert await database.transaction(insert) == 2


async def _count(rows: int) -> int:
    return rows


class _DroppedAllocationHandle:
    """A transaction handle that silently skips the metadata statement and reports no row changed."""

    def __init__(self, handle: SqliteExecutor) -> None:
        self._handle = handle

    def exec(self, sql: str) -> Awaitable[None]:
        return self._handle.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._handle.run(sql, *params)

    def run_counted(self, sql: str, *params: SqliteValue) -> Awaitable[int]:
        if "durable_metadata" in sql:
            return _count(0)
        return self._handle.run_counted(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        return self._handle.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._handle.all(sql, *params)


class _DroppedAllocationDatabase:
    """A database facade whose allocation statement changes no row: the count is the only witness."""

    def __init__(self, delegate: SqliteDatabase) -> None:
        self._delegate = delegate

    def exec(self, sql: str) -> Awaitable[None]:
        return self._delegate.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._delegate.run(sql, *params)

    def run_counted(self, sql: str, *params: SqliteValue) -> Awaitable[int]:
        return self._delegate.run_counted(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        return self._delegate.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._delegate.all(sql, *params)

    def transaction(self, callback: Callable[[SqliteExecutor], Awaitable[Any]]) -> Awaitable[Any]:
        return self._delegate.transaction(lambda handle: callback(_DroppedAllocationHandle(handle)))

    def close(self) -> Awaitable[None]:
        return self._delegate.close()


class _OldMemberSetHandle:
    """A transaction handle carrying the pre-1.5.0 member set: no counted run at all."""

    def __init__(self, handle: SqliteExecutor) -> None:
        self._handle = handle

    def exec(self, sql: str) -> Awaitable[None]:
        return self._handle.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._handle.run(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        return self._handle.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._handle.all(sql, *params)


class _OldMemberSetDatabase:
    """A custom driver written against the old ``SqliteExecutor``: it has no counted run."""

    def __init__(self, delegate: SqliteDatabase) -> None:
        self._delegate = delegate

    def exec(self, sql: str) -> Awaitable[None]:
        return self._delegate.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._delegate.run(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        return self._delegate.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._delegate.all(sql, *params)

    def transaction(self, callback: Callable[[SqliteExecutor], Awaitable[Any]]) -> Awaitable[Any]:
        # The handle deliberately lacks the member, so it no longer satisfies the protocol.
        return self._delegate.transaction(
            lambda handle: callback(cast(SqliteExecutor, _OldMemberSetHandle(handle)))
        )

    def close(self) -> Awaitable[None]:
        return self._delegate.close()


async def test_the_commit_fails_when_its_allocation_statement_changed_no_row() -> None:
    """A commit whose ``next_seq`` write changed nothing is an unknown outcome, not a stored call."""

    directory = _temp_directory()
    path = str(directory / "storage.sqlite")
    seed = await SqliteStorage.open(await open_posix_sqlite_database(path))
    root_id = await _create_root(seed)
    first_id = EntryId(await seed.mint_id())
    second_id = EntryId(await seed.mint_id())
    await seed.close(context)

    storage = await SqliteStorage.open(_DroppedAllocationDatabase(await open_posix_sqlite_database(path)))
    _OPEN.append(storage)
    receipts: list[tuple[int, Seq]] = []
    with pytest.raises(RuntimeError, match="metadata"):
        await storage.commit_batches(
            [
                [{"type": "entry", "value": _entry(first_id, root_id)}],
                [{"type": "entry", "value": _entry(second_id, root_id)}],
            ],
            _receive(receipts),
            context,
        )

    assert receipts == []
    assert await storage.entry(first_id, context) is None
    assert await storage.entry(second_id, context) is None


async def test_a_driver_without_the_counted_run_member_fails_the_commit() -> None:
    """A custom driver of the old member set is refused loudly, not left storing nothing silently."""

    directory = _temp_directory()
    path = str(directory / "storage.sqlite")
    seed = await SqliteStorage.open(await open_posix_sqlite_database(path))
    root_id = await _create_root(seed)
    entry_id = EntryId(await seed.mint_id())
    await seed.close(context)

    storage = await SqliteStorage.open(
        cast(SqliteDatabase, _OldMemberSetDatabase(await open_posix_sqlite_database(path)))
    )
    _OPEN.append(storage)
    receipts: list[tuple[int, Seq]] = []
    with pytest.raises(AttributeError, match="run_counted"):
        await storage.commit_batches(
            [[{"type": "entry", "value": _entry(entry_id, root_id)}]],
            _receive(receipts),
            context,
        )

    assert receipts == []
    assert await storage.entry(entry_id, context) is None


# ── The contract as declared ────────────────────────────────────────────────


def test_the_batched_call_is_the_only_commit_shape() -> None:
    """One commit shape on the protocol: the batched call, and no single-write companion."""

    assert "commit_batches" in Storage.__dict__
    assert "commit" not in Storage.__dict__
    assert CommitResult.__annotations__.keys() == {"stored", "refused"}
    assert isinstance(MemoryStorage(), Storage)


@_BACKENDS
async def test_no_backend_carries_the_retired_single_write_signature(create_storage: Any) -> None:
    """A host that implements the retired form does not satisfy this protocol."""

    assert not hasattr(await create_storage(), "commit")


async def test_the_single_batch_a_seeding_call_writes_carries_on_the_group_sequence() -> None:
    """One contract and one sequence: the helper's batch continues what the batched call left."""

    storage = MemoryStorage()
    root_id = await _create_root(storage)
    first_id = EntryId(await storage.mint_id())
    second_id = EntryId(await storage.mint_id())
    receipts: list[tuple[int, Seq]] = []

    stored = await storage.commit_batches(
        [[{"type": "entry", "value": _entry(first_id, root_id)}]], _receive(receipts), context
    )
    assert stored == {"stored": 1}

    single = await commit_writes(
        storage, [{"type": "entry", "value": _entry(second_id, root_id)}], context
    )
    assert single == receipts[0][1] + 1
    loaded = await storage.entry(second_id, context)
    assert loaded is not None
    assert loaded["entry"]["id"] == second_id
    assert loaded["commitSeq"] == single

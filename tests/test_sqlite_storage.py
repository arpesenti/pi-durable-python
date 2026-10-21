"""Translated v1.3.0 ``sqlite-storage`` suite.

Covers both conformance runs (fresh and across reopen) plus persistence and sequence allocation,
table prefixes, metadata corruption, document history, detach-on-replay, transaction rollback,
index usage, revision reclamation, WAL checkpointing, page reuse, and bounded storage size.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import sqlite3
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
from storage_support import expect

from pi_durable.context import BACKGROUND_CONTEXT, Context
from pi_durable.errors import SessionFailed
from pi_durable.ids import ROOT_CONVERSATION_ID, DocumentId, EntryId, Seq, TaskId
from pi_durable.session.session import create_session
from pi_durable.storage.sqlite import SqliteStorage
from pi_durable.storage.sqlite.posix import (
    PosixSqliteStorageOptions,
    open_posix_sqlite_database,
    open_posix_sqlite_storage,
)
from pi_durable.testing import (
    StorageConformanceCase,
    StorageConformanceOptions,
    create_expect_assertions,
    create_storage_conformance,
)
from pi_durable.types import Storage, StorageWrite

context: Context = BACKGROUND_CONTEXT

_directories: list[Path] = []
_open_storages: list[SqliteStorage] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> AsyncIterator[None]:
    yield
    while _open_storages:
        await _open_storages.pop().close(context)
    while _directories:
        shutil.rmtree(_directories.pop(), ignore_errors=True)


def _temp_directory(prefix: str = "pi-durable-sqlite-") -> Path:
    directory = Path(tempfile.mkdtemp(prefix=prefix))
    _directories.append(directory)
    return directory


async def _open_storage(
    path: Path,
    options: PosixSqliteStorageOptions | None = None,
) -> SqliteStorage:
    storage = await open_posix_sqlite_storage(str(path), options)
    _open_storages.append(storage)
    return storage


async def _create_storage() -> tuple[SqliteStorage, Path]:
    directory = _temp_directory()
    path = directory / "storage.sqlite"
    return await _open_storage(path), path


class ReopeningStorage:
    """A storage that closes and reopens the SQLite file after every commit."""

    def __init__(self, current: SqliteStorage, path: Path) -> None:
        self._current = current
        self._path = path
        self._closed = False

    async def commit(self, writes: Sequence[StorageWrite], commit_context: Context) -> Seq:
        if self._closed:
            return await self._current.commit(writes, commit_context)
        try:
            return await self._current.commit(writes, commit_context)
        finally:
            await self._current.close(context)
            self._current = await open_posix_sqlite_storage(str(self._path))

    async def mint_id(self) -> int:
        return await self._current.mint_id()

    async def conversation(self, id: Any, read_context: Context) -> Any:
        return await self._current.conversation(id, read_context)

    async def scan_conversations(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_conversations(query, limit, cursor, read_context)

    async def entry(self, *args: Any) -> Any:
        return await self._current.entry(*args)

    async def find_latest_head_marker(
        self, conversation_id: Any, at: Any, read_context: Context
    ) -> Any:
        return await self._current.find_latest_head_marker(conversation_id, at, read_context)

    async def scan_entries(self, query: Any, limit: int, cursor: Any, read_context: Context) -> Any:
        return await self._current.scan_entries(query, limit, cursor, read_context)

    async def task(self, id: Any, read_context: Context) -> Any:
        return await self._current.task(id, read_context)

    async def scan_tasks(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_tasks(query, limit, cursor, read_context)

    async def submission(self, id: Any, read_context: Context) -> Any:
        return await self._current.submission(id, read_context)

    async def scan_submissions(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_submissions(query, limit, cursor, read_context)

    async def submission_by_request(
        self, conversation_id: Any, request_id: str, read_context: Context
    ) -> Any:
        return await self._current.submission_by_request(
            conversation_id, request_id, read_context
        )

    async def find_document(self, address: Any, at: Any, read_context: Context) -> Any:
        return await self._current.find_document(address, at, read_context)

    async def document(self, id: Any, at: Any, read_context: Context) -> Any:
        return await self._current.document(id, at, read_context)

    async def scan_documents(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_documents(query, limit, cursor, read_context)

    async def close(self, close_context: Context) -> None:
        if self._closed:
            return
        self._closed = True
        await self._current.close(close_context)


_assertions = create_expect_assertions(expect)


async def _with_storage(use: Callable[[Storage], Awaitable[None]]) -> None:
    await use((await _create_storage())[0])


async def _with_reopening_storage(use: Callable[[Storage], Awaitable[None]]) -> None:
    directory = _temp_directory("pi-durable-sqlite-conformance-")
    path = directory / "storage.sqlite"
    created = await open_posix_sqlite_storage(str(path))
    await created.close(context)
    storage = ReopeningStorage(await _open_storage(path), path)
    try:
        await use(storage)
    finally:
        await storage.close(context)


CASES: list[StorageConformanceCase] = create_storage_conformance(
    StorageConformanceOptions(assertions=_assertions, with_storage=_with_storage)
)
REOPEN_CASES: list[StorageConformanceCase] = create_storage_conformance(
    StorageConformanceOptions(assertions=_assertions, with_storage=_with_reopening_storage)
)


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_storage_conformance(case: StorageConformanceCase) -> None:
    await case.run()


@pytest.mark.parametrize(
    "case",
    REOPEN_CASES,
    ids=[f"{case.name} [across reopen]" for case in REOPEN_CASES],
)
async def test_storage_conformance_across_reopen(case: StorageConformanceCase) -> None:
    await case.run()


def _entry(id: int, conversation_id: Any, data: Any = None) -> dict[str, Any]:
    return {
        "id": id,
        "conversationId": conversation_id,
        "kind": "message",
        **({} if data is None else {"data": data}),
    }


async def _create_root(storage: Storage) -> Seq:
    return await storage.commit(
        [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
    )


def _pending_task(id: int) -> dict[str, Any]:
    return {
        "id": id,
        "conversationId": ROOT_CONVERSATION_ID,
        "kind": "test.task",
        "version": 1,
        "input": None,
        "state": {"status": "pending", "checkpoint": {"phase": "ready"}},
        "background": False,
        "abortRequested": False,
    }


def _read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)
    connection.row_factory = sqlite3.Row
    return connection


def _writable(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, isolation_level=None)
    connection.row_factory = sqlite3.Row
    return connection


def _scalar(connection: sqlite3.Connection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    assert row is not None
    return int(row["value"])


def _revision_count(path: Path, document_id: DocumentId) -> int:
    connection = _read_only(path)
    try:
        row = connection.execute(
            "SELECT count(*) AS count FROM document_revisions WHERE document_id = ?",
            (document_id,),
        ).fetchone()
        assert row is not None
        return int(row["count"])
    finally:
        connection.close()


async def test_persists_records_sequence_allocation_and_global_id_allocation_across_reopen() -> None:
    storage, path = await _create_storage()
    assert await _create_root(storage) == 1
    entry_id = EntryId(await storage.mint_id())
    assert (
        await storage.commit(
            [{"type": "entry", "value": _entry(entry_id, ROOT_CONVERSATION_ID)}], context
        )
        == 2
    )
    await storage.close(context)
    _open_storages.remove(storage)

    reopened = await _open_storage(path)
    assert await reopened.entry(entry_id, context) == {
        "entry": _entry(entry_id, ROOT_CONVERSATION_ID),
        "commitSeq": 2,
    }
    assert await reopened.mint_id() == entry_id + 1
    assert (
        await reopened.commit(
            [{"type": "task", "value": _pending_task(await reopened.mint_id())}], context
        )
        == 3
    )


async def test_namespaces_tables_and_indexes_with_a_prefix_across_reopen() -> None:
    directory = _temp_directory()
    path = directory / "storage.sqlite"
    host = _writable(path)
    try:
        host.execute("CREATE TABLE tasks (text TEXT)")
    finally:
        host.close()
    storage = await _open_storage(path, {"prefix": "pi_"})
    assert await _create_root(storage) == 1
    await storage.close(context)
    _open_storages.remove(storage)

    reopened = await _open_storage(path, {"prefix": "pi_"})
    assert await reopened.conversation(ROOT_CONVERSATION_ID, context) == {
        "id": ROOT_CONVERSATION_ID
    }
    database = _read_only(path)
    try:
        names = [
            str(row["name"])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'index') "
                "AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
        ]
    finally:
        database.close()
    assert [name for name in names if not name.startswith("pi_")] == ["tasks"]
    assert "pi_tasks" in names


async def test_opens_a_database_facade_under_a_table_prefix() -> None:
    directory = _temp_directory()
    path = directory / "storage.sqlite"
    first = await SqliteStorage.open(await open_posix_sqlite_database(str(path)), {"prefix": "first_"})
    second = await SqliteStorage.open(
        await open_posix_sqlite_database(str(path)), {"prefix": "second_"}
    )
    try:
        await first.commit(
            [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
        )
        assert await second.conversation(ROOT_CONVERSATION_ID, context) is None
        await second.commit(
            [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
        )
        assert await first.conversation(ROOT_CONVERSATION_ID, context) == {
            "id": ROOT_CONVERSATION_ID
        }
        assert await second.conversation(ROOT_CONVERSATION_ID, context) == {
            "id": ROOT_CONVERSATION_ID
        }
    finally:
        await first.close(context)
        await second.close(context)


@pytest.mark.parametrize("prefix", ["bad-prefix", "pi_\n"])
async def test_rejects_an_invalid_prefix_before_creating_the_database(prefix: str) -> None:
    directory = _temp_directory()
    database_directory = directory / "nested"
    with pytest.raises(ValueError, match="Invalid SQLite schema prefix"):
        await open_posix_sqlite_storage(
            str(database_directory / "storage.sqlite"), {"prefix": prefix}
        )
    assert database_directory.exists() is False


async def test_rejects_persisted_metadata_corruption_on_reopen() -> None:
    storage, path = await _create_storage()
    await storage.close(context)
    _open_storages.remove(storage)
    database = _writable(path)
    try:
        database.execute("DELETE FROM durable_metadata")
    finally:
        database.close()
    with pytest.raises(RuntimeError, match="Durable SQLite metadata is missing"):
        await open_posix_sqlite_storage(str(path))


async def test_rejects_a_document_whose_required_base_is_missing() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    id = DocumentId(await storage.mint_id())
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": {"id": id, "kind": "corrupt", "scope": {"kind": "session"}},
                "content": {"kind": "base", "version": 1, "value": {"retained": True}},
            }
        ],
        context,
    )
    database = _writable(path)
    try:
        database.execute("DELETE FROM document_revisions WHERE document_id = ?", (id,))
    finally:
        database.close()
    with pytest.raises(RuntimeError, match=f"Document {id} is missing a required base"):
        await storage.document(id, "current", context)


async def test_replays_detached_root_replacements_and_follow_up_edits_while_rejecting_corruption() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    id = DocumentId(await storage.mint_id())
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": {"id": id, "kind": "replay", "scope": {"kind": "session"}},
                "content": {
                    "kind": "base",
                    "version": 1,
                    "value": {"nested": {"value": 1}, "rows": []},
                },
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {
                    "kind": "delta",
                    "version": 1,
                    "ops": [["r", {"nested": {"value": 2}, "rows": [{"id": 1}]}]],
                },
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {
                    "kind": "delta",
                    "version": 1,
                    "ops": [
                        ["s", ["nested", "value"], 3],
                        ["p", ["rows"], 1, 0, [{"id": 2}]],
                        ["m", ["rows"], [1, 0]],
                    ],
                },
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {
                    "kind": "delta",
                    "version": 1,
                    "ops": [["s", ["nested", "value"], 4]],
                },
            }
        ],
        context,
    )

    expected = {"nested": {"value": 4}, "rows": [{"id": 2}, {"id": 1}]}
    first = await storage.document(id, "current", context)
    assert first is not None
    assert first["value"] == expected
    first["value"]["nested"]["value"] = 99
    first["value"]["rows"][0]["id"] = 99
    again = await storage.document(id, "current", context)
    assert again is not None
    assert again["value"] == expected

    database = _writable(path)
    try:
        database.execute(
            "UPDATE document_revisions SET content = ? WHERE document_id = ? AND seq = "
            "(SELECT max(seq) FROM document_revisions WHERE document_id = ?)",
            ('[["unknown"]]', id, id),
        )
    finally:
        database.close()
    with pytest.raises(TypeError, match="unknown op verb"):
        await storage.document(id, "current", context)


async def test_rolls_sql_rows_and_sequence_allocation_back_as_one_transaction() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    transient_id = EntryId(await storage.mint_id())
    transient_task_id = TaskId(await storage.mint_id())
    circular: dict[str, Any] = {}
    circular["self"] = circular
    with pytest.raises(ValueError, match="Circular reference"):
        await storage.commit(
            [
                {"type": "entry", "value": _entry(transient_id, ROOT_CONVERSATION_ID)},
                {
                    "type": "task",
                    "value": {**_pending_task(transient_task_id), "input": circular},
                },
            ],
            context,
        )
    assert await storage.entry(transient_id, context) is None
    assert await storage.task(transient_task_id, context) is None
    committed_id = EntryId(await storage.mint_id())
    assert (
        await storage.commit(
            [{"type": "entry", "value": _entry(committed_id, ROOT_CONVERSATION_ID)}], context
        )
        == 2
    )

    database = _read_only(path)
    try:
        assert _scalar(database, "SELECT count(*) AS value FROM entries") == 1
        assert (
            _scalar(database, "SELECT next_seq AS value FROM durable_metadata WHERE singleton = 1")
            == 3
        )
    finally:
        database.close()


async def test_reconstructs_recent_and_ancient_rewindable_points_after_reopen() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    id = DocumentId(await storage.mint_id())
    record = {
        "id": id,
        "kind": "history",
        "scope": {"kind": "conversation", "conversationId": ROOT_CONVERSATION_ID},
        "history": "rewindable",
        "fork": "asOf",
    }
    created_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": record,
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    ancient_at = created_at
    recent_at = created_at
    for count in range(1, 41):
        recent_at = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": (
                        {"kind": "base", "version": 1, "value": {"count": count}}
                        if count == 20
                        else {
                            "kind": "delta",
                            "version": 1,
                            "ops": [["s", ["count"], count]],
                        }
                    ),
                }
            ],
            context,
        )
        if count == 5:
            ancient_at = recent_at
    await storage.close(context)
    _open_storages.remove(storage)
    reopened = await _open_storage(path)
    ancient = await reopened.document(id, ancient_at, context)
    assert ancient is not None
    assert ancient["value"] == {"count": 5}
    recent = await reopened.document(id, recent_at, context)
    assert recent is not None
    assert recent["value"] == {"count": 40}


async def test_uses_indexes_for_addresses_scopes_entry_history_and_revision_tails() -> None:
    storage, path = await _create_storage()
    await storage.close(context)
    _open_storages.remove(storage)
    database = _read_only(path)
    try:
        plans = [
            database.execute(
                "EXPLAIN QUERY PLAN SELECT record FROM documents "
                "WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? "
                "AND key_value = ? AND retired_at IS NULL ORDER BY created_at DESC LIMIT 1",
                ("kind", "session", 0, 0, ""),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT record FROM documents "
                "WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? "
                "AND key_value = ? AND created_at <= ? AND (retired_at IS NULL OR retired_at > ?) "
                "ORDER BY created_at DESC LIMIT 1",
                ("kind", "conversation", 1, 0, "", 10, 10),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT record FROM documents "
                "WHERE scope_kind = ? AND owner_id = ? AND kind = ? AND id > ? "
                "ORDER BY id LIMIT ?",
                ("task", 1, "kind", 0, 10),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT record FROM entries "
                "WHERE conversation_id = ? AND id <= ? ORDER BY id DESC LIMIT ?",
                (1, 10, 10),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT record FROM entries "
                "WHERE conversation_id = ? AND head IS NOT NULL AND id <= ? "
                "ORDER BY id DESC LIMIT 1",
                (1, 10),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT record FROM tasks "
                "WHERE status = ? AND id > ? ORDER BY id LIMIT ?",
                ("pending", 0, 10),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT seq, kind, version, content FROM document_revisions "
                "WHERE document_id = ? AND kind = 'base' AND seq <= ? ORDER BY seq DESC LIMIT 1",
                (1, 10),
            ).fetchall(),
            database.execute(
                "EXPLAIN QUERY PLAN SELECT seq, kind, version, content FROM document_revisions "
                "WHERE document_id = ? AND seq > ? AND seq <= ? ORDER BY seq",
                (1, 5, 10),
            ).fetchall(),
        ]
        details = ["\n".join(str(row["detail"]) for row in plan) for plan in plans]
    finally:
        database.close()
    assert "documents_by_address" in details[0]
    assert "documents_by_address" in details[1]
    assert "documents_by_scope_kind" in details[2]
    assert "entries_by_conversation" in details[3]
    assert "entry_heads_by_conversation" in details[4]
    assert "tasks_by_status" in details[5]
    assert "document_revisions_by_kind" in details[6]
    assert "sqlite_autoindex_document_revisions_1" in details[7]
    for detail in details[:3]:
        assert "SCAN documents" not in detail
    assert "SCAN document_revisions" not in details[7]


async def test_reclaims_current_only_revisions_only_after_a_base_or_retirement() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    id = DocumentId(await storage.mint_id())
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": {"id": id, "kind": "latest", "scope": {"kind": "session"}},
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    for count in range(1, 11):
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {
                        "kind": "delta",
                        "version": 1,
                        "ops": [["s", ["count"], count]],
                    },
                }
            ],
            context,
        )
    assert _revision_count(path, id) == 11
    revisions = _read_only(path)
    try:
        row = revisions.execute(
            "SELECT content FROM document_revisions WHERE document_id = ? "
            "AND kind = 'delta' ORDER BY seq DESC LIMIT 1",
            (id,),
        ).fetchone()
        assert row is not None
        assert json.loads(row["content"]) == [["s", ["count"], 10]]
    finally:
        revisions.close()
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "base", "version": 1, "value": {"count": 11}},
            }
        ],
        context,
    )
    assert _revision_count(path, id) == 1
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "delta", "version": 1, "ops": [["r", {"count": 12}]]},
            }
        ],
        context,
    )
    assert _revision_count(path, id) == 2
    await storage.commit([{"type": "document.retire", "id": id}], context)
    assert _revision_count(path, id) == 0


async def test_waits_for_a_competing_writer_only_up_to_the_busy_timeout() -> None:
    directory = _temp_directory()
    path = directory / "storage.sqlite"
    storage = await _open_storage(path, {"busy_timeout_ms": 20})
    await _create_root(storage)
    entry_id = EntryId(await storage.mint_id())
    blocker = _writable(path)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            await storage.commit(
                [{"type": "entry", "value": _entry(entry_id, ROOT_CONVERSATION_ID)}], context
            )
        blocker.execute("ROLLBACK")
    finally:
        blocker.close()
    assert (
        await storage.commit(
            [{"type": "entry", "value": _entry(entry_id, ROOT_CONVERSATION_ID)}], context
        )
        == 2
    )


async def test_auto_checkpoints_wal_frames_and_truncates_the_wal_on_close() -> None:
    directory = _temp_directory()
    path = directory / "storage.sqlite"
    storage = await _open_storage(path, {"wal_auto_checkpoint_pages": 1})
    await _create_root(storage)
    for index in range(20):
        await storage.commit(
            [
                {
                    "type": "entry",
                    "value": _entry(
                        await storage.mint_id(),
                        ROOT_CONVERSATION_ID,
                        {"text": "x" * (32 * 1024), "index": index},
                    ),
                }
            ],
            context,
        )
    wal_path = Path(f"{path}-wal")
    assert wal_path.stat().st_size < 512 * 1024
    observer = _read_only(path)
    try:
        assert _scalar(observer, "SELECT count(*) AS value FROM entries") == 20
        await storage.close(context)
        _open_storages.remove(storage)
        assert wal_path.stat().st_size == 0
    finally:
        observer.close()


async def test_reuses_pages_released_by_current_only_checkpoints() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    id = DocumentId(await storage.mint_id())
    large = "x" * (512 * 1024)
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": {"id": id, "kind": "reuse", "scope": {"kind": "session"}},
                "content": {"kind": "base", "version": 1, "value": {"text": large}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "base", "version": 1, "value": {"text": "small"}},
            }
        ],
        context,
    )
    before = _read_only(path)
    try:
        pages_after_delete = _scalar(before, "SELECT page_count AS value FROM pragma_page_count()")
        free_after_delete = _scalar(
            before, "SELECT freelist_count AS value FROM pragma_freelist_count()"
        )
    finally:
        before.close()
    assert free_after_delete > 0
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "base", "version": 1, "value": {"text": large}},
            }
        ],
        context,
    )
    after = _read_only(path)
    try:
        pages_after_reuse = _scalar(after, "SELECT page_count AS value FROM pragma_page_count()")
        free_after_reuse = _scalar(
            after, "SELECT freelist_count AS value FROM pragma_freelist_count()"
        )
    finally:
        after.close()
    assert pages_after_reuse <= pages_after_delete + 2
    assert free_after_reuse < free_after_delete


async def test_keeps_representative_row_and_document_storage_bounded() -> None:
    storage, path = await _create_storage()
    await _create_root(storage)
    for index in range(100):
        await storage.commit(
            [
                {
                    "type": "entry",
                    "value": _entry(
                        await storage.mint_id(),
                        ROOT_CONVERSATION_ID,
                        {"index": index, "text": "x" * 1_024},
                    ),
                }
            ],
            context,
        )
    document_id = DocumentId(await storage.mint_id())
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": {
                    "id": document_id,
                    "kind": "size.history",
                    "scope": {"kind": "conversation", "conversationId": ROOT_CONVERSATION_ID},
                    "history": "rewindable",
                    "fork": "asOf",
                },
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    for count in range(1, 101):
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": document_id,
                    "content": {
                        "kind": "delta",
                        "version": 1,
                        "ops": [["s", ["count"], count]],
                    },
                }
            ],
            context,
        )
    await storage.close(context)
    _open_storages.remove(storage)
    assert path.stat().st_size < 1024 * 1024


async def test_storage_failure_fails_the_session_and_publishes_nothing() -> None:
    storage, _path = await _create_storage()
    session = create_session(storage)
    publications: list[Any] = []
    session.subscribe_commits(lambda publication, _context: publications.append(publication))

    async def create(tx: Any) -> Any:
        return await tx.create_conversation({"ownership": {"kind": "ownerless"}})

    conversation = await session.commit(create, context)
    published = len(publications)
    await storage.close(context)
    _open_storages.remove(storage)

    async def append(tx: Any) -> Any:
        return await tx.append_entry(conversation["id"], {"kind": "lost"})

    with pytest.raises(RuntimeError, match="SqliteStorage is closed"):
        await session.commit(append, context)
    assert len(publications) == published
    assert session.failure is not None
    with pytest.raises(SessionFailed):
        await session.commit(append, context)
    closed = await asyncio.wait_for(session.closed, 1)
    assert closed["reason"] == "failed"

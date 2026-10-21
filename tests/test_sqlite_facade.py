"""Translated v1.3.0 ``sqlite-facade`` suite.

The portable SQLite core is driven through the minimal ``SqliteDatabase`` facade. This suite pins
the facade guarantees it relies on: one prepared statement per connection and SQL text, serialized
transactions, call-ordered operations, stale transaction handles, rollback reporting, settlement
before ID adoption, and single-snapshot document reads.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypeVar, cast

import pytest

from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.ids import ROOT_CONVERSATION_ID, DocumentId, EntryId
from pi_durable.storage.sqlite import (
    SqliteDatabase,
    SqliteExecutor,
    SqliteStorage,
    SqliteValue,
)
from pi_durable.storage.sqlite.posix import PosixSqliteDatabase, open_posix_sqlite_database

context = BACKGROUND_CONTEXT

T = TypeVar("T")
SettlementMode = Literal["immediate", "delay", "reject"]


class ControlledSettlementDatabase:
    """A facade whose next transaction settlement can be delayed or rejected after the callback."""

    def __init__(self, delegate: SqliteDatabase) -> None:
        self._delegate = delegate
        self._mode: SettlementMode = "immediate"
        self._pending: Callable[[], None] | None = None

    def exec(self, sql: str) -> Awaitable[None]:
        return self._delegate.exec(sql)

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        return self._delegate.run(sql, *params)

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        return self._delegate.get(sql, *params)

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        return self._delegate.all(sql, *params)

    def close(self) -> Awaitable[None]:
        return self._delegate.close()

    def transaction(
        self, callback: Callable[[SqliteExecutor], Awaitable[T]]
    ) -> Awaitable[T]:
        mode = self._mode
        self._mode = "immediate"
        if mode == "immediate":
            return self._delegate.transaction(callback)

        async def wrapped(transaction: SqliteExecutor) -> T:
            value = await callback(transaction)
            if mode == "reject":
                raise RuntimeError("controlled settlement rejection")
            return value

        settlement = cast(asyncio.Future[T], self._delegate.transaction(wrapped))
        outer: asyncio.Future[T] = asyncio.get_running_loop().create_future()

        def adopt(_future: asyncio.Future[T]) -> None:
            if outer.done():
                return
            error = settlement.exception()
            if error is not None:
                outer.set_exception(error)
            else:
                outer.set_result(settlement.result())

        def install() -> None:
            settlement.add_done_callback(adopt)

        self._pending = install
        return outer

    def control_next_settlement(self, mode: Literal["delay", "reject"]) -> None:
        if self._pending is not None:
            raise RuntimeError("A settlement is already pending")
        self._mode = mode

    def settle(self) -> None:
        pending = self._pending
        if pending is None:
            raise RuntimeError("No settlement is pending")
        self._pending = None
        pending()


class PrepareCountingDatabase(PosixSqliteDatabase):
    """Counts statement preparation misses, exactly as the Node adapter counts ``prepare``."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        super().__init__(connection)
        self._prepare_counts: dict[str, int] = {}

    def prepare(self, sql: str) -> sqlite3.Cursor:
        self._prepare_counts[sql] = self._prepare_counts.get(sql, 0) + 1
        return super().prepare(sql)

    def repeated_prepares(self) -> list[str]:
        return [sql for sql, count in self._prepare_counts.items() if count > 1]


async def test_prepares_each_storage_statement_once_per_connection_and_reuses_it() -> None:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    database = PrepareCountingDatabase(connection)
    storage = await SqliteStorage.open(database)
    await storage.commit([{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context)
    await storage.commit(
        [
            {
                "type": "entry",
                "value": {
                    "id": EntryId(index + 2),
                    "conversationId": ROOT_CONVERSATION_ID,
                    "kind": "cached",
                },
            }
            for index in range(100)
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "entry",
                "value": {
                    "id": EntryId(102),
                    "conversationId": ROOT_CONVERSATION_ID,
                    "kind": "cached-again",
                },
            }
        ],
        context,
    )
    first = await storage.entry(EntryId(2), context)
    assert first is not None
    assert first["entry"]["kind"] == "cached"
    second = await storage.entry(EntryId(102), context)
    assert second is not None
    assert second["entry"]["kind"] == "cached-again"
    with pytest.raises(RuntimeError, match="ID 1 already belongs to conversation"):
        await storage.commit(
            [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
        )
    still_cached = await storage.entry(EntryId(2), context)
    assert still_cached is not None
    assert still_cached["entry"]["kind"] == "cached"
    assert database.repeated_prepares() == []
    await storage.close(context)


async def test_commits_work_done_through_the_transaction_handle_and_closes_idempotently() -> None:
    database = await open_posix_sqlite_database(":memory:")

    async def create(transaction: Any) -> None:
        await transaction.exec("CREATE TABLE async_probe (value INTEGER)")
        await transaction.run("INSERT INTO async_probe (value) VALUES (?)", 1)

    await database.transaction(create)
    assert await database.get("SELECT value FROM async_probe") == {"value": 1}
    await database.close()
    await database.close()


async def test_runs_multi_statement_scripts_inside_a_transaction() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE script_probe (value INTEGER)")

    async def insert_two(transaction: SqliteExecutor) -> None:
        await transaction.exec(
            "INSERT INTO script_probe (value) VALUES (1); "
            "INSERT INTO script_probe (value) VALUES (2);"
        )

    await database.transaction(insert_two)
    assert await database.all("SELECT value FROM script_probe ORDER BY value") == [
        {"value": 1},
        {"value": 2},
    ]

    async def insert_and_roll_back(transaction: SqliteExecutor) -> None:
        await transaction.exec("INSERT INTO script_probe (value) VALUES (3)")
        raise RuntimeError("roll back script")

    with pytest.raises(RuntimeError, match="roll back script"):
        await database.transaction(insert_and_roll_back)
    assert await database.all("SELECT value FROM script_probe ORDER BY value") == [
        {"value": 1},
        {"value": 2},
    ]
    await database.close()


async def test_serializes_concurrent_transactions() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE transaction_queue (value INTEGER)")
    first_started = asyncio.Event()
    release_first = asyncio.Event()

    async def first_transaction(transaction: Any) -> None:
        await transaction.exec("INSERT INTO transaction_queue (value) VALUES (1)")
        first_started.set()
        await release_first.wait()

    first = asyncio.ensure_future(database.transaction(first_transaction))
    await first_started.wait()

    second_started = False

    async def second_transaction(transaction: Any) -> None:
        nonlocal second_started
        second_started = True
        await transaction.exec("INSERT INTO transaction_queue (value) VALUES (2)")

    second = asyncio.ensure_future(database.transaction(second_transaction))
    await asyncio.sleep(0)
    assert second_started is False

    release_first.set()
    await asyncio.gather(first, second)
    assert await database.all("SELECT value FROM transaction_queue ORDER BY value") == [
        {"value": 1},
        {"value": 2},
    ]
    await database.close()


async def test_runs_operations_in_call_order_whether_they_start_immediately_or_wait() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE call_order (value INTEGER)")

    async def roll_back(handle: Any) -> None:
        await handle.run("INSERT INTO call_order (value) VALUES (?)", 1)
        await asyncio.sleep(0)
        raise RuntimeError("roll back")

    # Operations called during a transaction must neither see its uncommitted rows nor join its
    # rollback.
    transaction = database.transaction(roll_back)
    before_write = database.all("SELECT value FROM call_order ORDER BY value")
    write = database.run("INSERT INTO call_order (value) VALUES (?)", 2)
    after_write = database.all("SELECT value FROM call_order ORDER BY value")
    with pytest.raises(RuntimeError, match="roll back"):
        await transaction
    await write
    assert await before_write == []
    assert await after_write == [{"value": 2}]

    storage = await SqliteStorage.open(database)
    commit = asyncio.ensure_future(
        storage.commit(
            [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
        )
    )
    read = asyncio.ensure_future(storage.conversation(ROOT_CONVERSATION_ID, context))
    await commit
    assert await read == {"id": ROOT_CONVERSATION_ID}
    await storage.close(context)


async def test_queues_ordinary_operations_behind_an_active_transaction() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE operation_queue (value INTEGER)")
    transaction_started = asyncio.Event()
    release_transaction = asyncio.Event()

    async def blocked(transaction: Any) -> None:
        await transaction.exec("INSERT INTO operation_queue (value) VALUES (1)")
        transaction_started.set()
        await release_transaction.wait()

    pending = asyncio.ensure_future(database.transaction(blocked))
    await transaction_started.wait()

    write = database.exec("INSERT INTO operation_queue (value) VALUES (2)")
    read = database.all("SELECT value FROM operation_queue ORDER BY value")
    await asyncio.sleep(0)
    assert write.done() is False
    assert read.done() is False

    release_transaction.set()
    await pending
    await write
    assert await read == [{"value": 1}, {"value": 2}]
    await database.close()


async def test_queues_database_calls_made_synchronously_by_an_immediate_transaction() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE barrier_probe (value INTEGER)")
    outside: asyncio.Future[None] | None = None

    async def roll_back(handle: Any) -> None:
        nonlocal outside
        # Misuse: this call must wait for the transaction instead of joining it.
        outside = database.run("INSERT INTO barrier_probe (value) VALUES (?)", 2)
        await handle.run("INSERT INTO barrier_probe (value) VALUES (?)", 1)
        await asyncio.sleep(0)
        raise RuntimeError("roll back")

    transaction = database.transaction(roll_back)
    with pytest.raises(RuntimeError, match="roll back"):
        await transaction
    assert outside is not None
    await outside
    assert await database.all("SELECT value FROM barrier_probe") == [{"value": 2}]
    await database.close()


async def test_lets_admitted_multi_query_reads_finish_before_storage_closes() -> None:
    storage = await SqliteStorage.open(await open_posix_sqlite_database(":memory:"))
    entry_id = EntryId(2)
    await storage.commit(
        [
            {"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}},
            {
                "type": "entry",
                "value": {"id": entry_id, "conversationId": ROOT_CONVERSATION_ID, "kind": "probe"},
            },
        ],
        context,
    )
    scan = asyncio.ensure_future(
        storage.scan_entries({"conversationId": ROOT_CONVERSATION_ID}, 10, None, context)
    )
    entry = asyncio.ensure_future(storage.entry(ROOT_CONVERSATION_ID, entry_id, context))
    head = asyncio.ensure_future(
        storage.find_latest_head_marker(ROOT_CONVERSATION_ID, None, context)
    )
    await asyncio.sleep(0)
    closed = storage.close(context)
    # A repeated close is the same awaitable and settles only when the database is closed.
    assert storage.close(context) is closed
    assert [item["id"] for item in (await scan)["items"]] == [entry_id]
    scanned_entry = await entry
    assert scanned_entry is not None
    assert scanned_entry["entry"]["kind"] == "probe"
    assert await head is None
    await closed
    with pytest.raises(RuntimeError, match="SqliteStorage is closed"):
        await storage.scan_entries({"conversationId": ROOT_CONVERSATION_ID}, 10, None, context)


async def test_rejects_a_transaction_handle_used_after_its_transaction_settles() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE stale_probe (value INTEGER)")
    captured: list[Any] = []

    async def insert(transaction: Any) -> None:
        captured.append(transaction)
        await transaction.run("INSERT INTO stale_probe (value) VALUES (?)", 1)

    await database.transaction(insert)
    handle = captured[0]
    stale = "SQLite transaction handle is no longer active"
    with pytest.raises(RuntimeError, match=stale):
        await handle.exec("INSERT INTO stale_probe (value) VALUES (2)")
    with pytest.raises(RuntimeError, match=stale):
        await handle.run("INSERT INTO stale_probe (value) VALUES (?)", 3)
    assert await database.all("SELECT value FROM stale_probe") == [{"value": 1}]
    await database.close()


async def test_does_not_preserve_the_callback_error_when_rollback_itself_fails() -> None:
    database = await open_posix_sqlite_database(":memory:")
    await database.exec("CREATE TABLE rollback_probe (value INTEGER)")

    async def escape_commit(transaction: Any) -> None:
        await transaction.exec("INSERT INTO rollback_probe (value) VALUES (1)")
        await transaction.exec("COMMIT")
        raise RuntimeError("rejected after an escaped commit")

    with pytest.raises(ExceptionGroup, match="SQLite transaction failed and rollback failed"):
        await database.transaction(escape_commit)
    assert await database.get("SELECT value FROM rollback_probe") == {"value": 1}
    await database.close()


async def test_awaits_async_transaction_settlement_and_adopts_ids_only_after_success() -> None:
    database = ControlledSettlementDatabase(await open_posix_sqlite_database(":memory:"))
    database.control_next_settlement("delay")
    opening = asyncio.ensure_future(SqliteStorage.open(database))
    await asyncio.sleep(0)
    assert opening.done() is False
    database.settle()
    storage = await opening

    database.control_next_settlement("delay")
    committing = asyncio.ensure_future(
        storage.commit(
            [
                {
                    "type": "entry",
                    "value": {
                        "id": EntryId(100),
                        "conversationId": ROOT_CONVERSATION_ID,
                        "kind": "settled",
                    },
                }
            ],
            context,
        )
    )
    await asyncio.sleep(0)
    assert committing.done() is False
    assert await storage.mint_id() == 2
    database.settle()
    assert await committing == 1
    assert await storage.mint_id() == 101

    database.control_next_settlement("reject")
    rejected = asyncio.ensure_future(
        storage.commit(
            [
                {
                    "type": "entry",
                    "value": {
                        "id": EntryId(200),
                        "conversationId": ROOT_CONVERSATION_ID,
                        "kind": "rejected",
                    },
                }
            ],
            context,
        )
    )
    await asyncio.sleep(0)
    assert rejected.done() is False
    assert await storage.mint_id() == 102
    database.settle()
    with pytest.raises(RuntimeError, match="controlled settlement rejection"):
        await rejected
    assert await storage.mint_id() == 103
    assert await storage.entry(EntryId(200), context) is None
    await storage.close(context)


async def test_reads_a_document_from_one_committed_state_while_a_commit_replaces_its_base() -> None:
    document_id = DocumentId(5)
    # Each yield count starts the commit at a different point of the read's record and revision
    # queries.
    for yields in range(16):
        storage = await SqliteStorage.open(await open_posix_sqlite_database(":memory:"))
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": {
                        "id": document_id,
                        "kind": "replaced",
                        "scope": {"kind": "session"},
                    },
                    "content": {"kind": "base", "version": 1, "value": {"value": 1}},
                }
            ],
            context,
        )
        read = asyncio.ensure_future(storage.document(document_id, "current", context))
        for _ in range(yields):
            await asyncio.sleep(0)
        replace = asyncio.ensure_future(
            storage.commit(
                [
                    {
                        "type": "document.change",
                        "id": document_id,
                        "content": {"kind": "base", "version": 1, "value": {"value": 2}},
                    }
                ],
                context,
            )
        )
        stored, _ = await asyncio.gather(read, replace)
        assert stored is not None
        assert stored["value"] in ({"value": 1}, {"value": 2})
        await storage.close(context)

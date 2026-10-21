"""Stdlib ``sqlite3`` adapter for the portable SQLite core.

Python replaces the host-specific Node driver with this adapter, as it replaces
``NodeExecutionEnv`` with :class:`~pi_durable.env.posix.PosixExecutionEnv`. It supplies the
:class:`~pi_durable.storage.sqlite.database.SqliteDatabase` facade: calls run in call order, a
transaction holds the connection until its callback settles, and prepared statements are reused
across ordinary operations and transaction handles.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, NotRequired, TypedDict, TypeVar

from .database import SqliteExecutor, SqliteValue
from .schema import validate_sqlite_schema_prefix
from .storage import SqliteStorage, SqliteStorageOptions

T = TypeVar("T")

_DEFAULT_WAL_AUTO_CHECKPOINT_PAGES = 1_000
_DEFAULT_BUSY_TIMEOUT_MS = 5_000


class PosixSqliteDatabaseOptions(TypedDict):
    """Connection settings for a stdlib-backed SQLite file."""

    wal_auto_checkpoint_pages: NotRequired[int]
    """SQLite WAL auto-checkpoint threshold in pages; 0 disables it (SQLite default: 1000)."""

    busy_timeout_ms: NotRequired[int]
    """Time SQLite waits for a competing file lock (SQLite default: 0; here: 5000 ms)."""


class PosixSqliteStorageOptions(PosixSqliteDatabaseOptions):
    """Connection settings plus durable storage options for a stdlib-backed file."""

    prefix: NotRequired[str]


class _TransactionScope:
    """Whether a transaction handle still belongs to a live transaction."""

    __slots__ = ("active",)

    def __init__(self) -> None:
        self.active = True


class _SerialOperationQueue:
    """Runs operations in call order; an asynchronous operation holds the queue until it settles."""

    __slots__ = ("_tail", "_tasks")

    def __init__(self) -> None:
        self._tail: asyncio.Future[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    def run(self, operation: Callable[[], Awaitable[T]]) -> asyncio.Future[T]:
        loop = asyncio.get_running_loop()
        result: asyncio.Future[T] = loop.create_future()
        barrier: asyncio.Future[None] = loop.create_future()
        previous, self._tail = self._tail, barrier

        async def runner() -> None:
            if previous is not None:
                await previous
            try:
                value = await operation()
            except BaseException as error:
                if not result.done():
                    result.set_exception(error)
            else:
                if not result.done():
                    result.set_result(value)
            finally:
                if not barrier.done():
                    barrier.set_result(None)

        task = loop.create_task(runner())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return result


class _PosixSqliteExecutor:
    """One statement per ``run``/``get``/``all`` on a shared connection and statement cache."""

    __slots__ = ("_connection", "_statements", "_preparer")

    def __init__(
        self,
        connection: sqlite3.Connection,
        statements: dict[str, sqlite3.Cursor],
        prepare: Callable[[str], sqlite3.Cursor] | None = None,
    ) -> None:
        self._connection = connection
        self._statements = statements
        self._preparer = self.prepare if prepare is None else prepare

    def prepare(self, sql: str) -> sqlite3.Cursor:
        """Return a cursor for ``sql``; a subclass may count preparation misses."""

        return self._connection.cursor()

    def exec(self, sql: str) -> asyncio.Future[None]:
        return self._run(lambda: self._execute_script(sql))

    def run(self, sql: str, *params: SqliteValue) -> asyncio.Future[None]:
        def execute() -> None:
            self._statement(sql).execute(sql, params)

        return self._run(execute)

    def get(self, sql: str, *params: SqliteValue) -> asyncio.Future[dict[str, Any] | None]:
        def execute() -> dict[str, Any] | None:
            cursor = self._statement(sql)
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return None if row is None else dict(row)

        return self._run(execute)

    def all(self, sql: str, *params: SqliteValue) -> asyncio.Future[list[dict[str, Any]]]:
        def execute() -> list[dict[str, Any]]:
            cursor = self._statement(sql)
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

        return self._run(execute)

    def _execute_script(self, sql: str) -> None:
        self._connection.executescript(sql)

    def _statement(self, sql: str) -> sqlite3.Cursor:
        cursor = self._statements.get(sql)
        if cursor is None:
            cursor = self._preparer(sql)
            self._statements[sql] = cursor
        return cursor

    def _run(self, operation: Callable[[], T]) -> asyncio.Future[T]:
        raise NotImplementedError


class _PosixSqliteTransaction(_PosixSqliteExecutor):
    """A transaction handle: runs synchronously inside the owning transaction until it settles."""

    __slots__ = ("_scope",)

    def __init__(
        self,
        connection: sqlite3.Connection,
        statements: dict[str, sqlite3.Cursor],
        prepare: Callable[[str], sqlite3.Cursor],
        scope: _TransactionScope,
    ) -> None:
        super().__init__(connection, statements, prepare)
        self._scope = scope

    def _execute_script(self, sql: str) -> None:
        for statement in _complete_statements(sql):
            self._connection.execute(statement)

    def _run(self, operation: Callable[[], T]) -> asyncio.Future[T]:
        future: asyncio.Future[T] = asyncio.get_running_loop().create_future()
        if not self._scope.active:
            future.set_exception(RuntimeError("SQLite transaction handle is no longer active"))
            return future
        try:
            future.set_result(operation())
        except BaseException as error:
            future.set_exception(error)
        return future


class PosixSqliteDatabase(_PosixSqliteExecutor):
    """``SqliteDatabase`` facade backed by Python's stdlib :mod:`sqlite3`."""

    __slots__ = ("_closing", "_closed", "_queue")

    def __init__(self, connection: sqlite3.Connection) -> None:
        super().__init__(connection, {})
        self._queue = _SerialOperationQueue()
        self._closed = False
        self._closing: asyncio.Future[None] | None = None

    def transaction(self, callback: Callable[[SqliteExecutor], Awaitable[T]]) -> asyncio.Future[T]:
        async def run_transaction() -> T:
            scope = _TransactionScope()
            transaction = _PosixSqliteTransaction(
                self._connection, self._statements, self._preparer, scope
            )
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                result = await callback(transaction)
                scope.active = False
                self._connection.execute("COMMIT")
                return result
            except BaseException as error:
                scope.active = False
                try:
                    self._connection.execute("ROLLBACK")
                except Exception as rollback_error:
                    if isinstance(error, Exception):
                        raise ExceptionGroup(
                            "SQLite transaction failed and rollback failed",
                            [error, rollback_error],
                        ) from None
                    raise rollback_error from None
                raise

        return self._queue.run(run_transaction)

    def close(self) -> asyncio.Future[None]:
        if self._closing is None:
            async def close_database() -> None:
                self._close()

            self._closing = self._queue.run(close_database)
        return self._closing

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._statements.clear()
        try:
            self._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            self._connection.close()

    def _run(self, operation: Callable[[], T]) -> asyncio.Future[T]:
        async def call() -> T:
            return operation()

        return self._queue.run(call)


def _complete_statements(sql: str) -> list[str]:
    """Split SQL text into complete statements, so a transaction handle can run a script."""

    statements: list[str] = []
    pending = ""
    for character in sql:
        pending += character
        if character == ";" and sqlite3.complete_statement(pending):
            statements.append(pending)
            pending = ""
    if pending.strip():
        statements.append(pending)
    return statements


async def open_posix_sqlite_database(
    path: str,
    options: PosixSqliteDatabaseOptions | None = None,
) -> PosixSqliteDatabase:
    """Open and configure a stdlib-backed SQLite database facade."""

    checkpoint_pages = (
        _DEFAULT_WAL_AUTO_CHECKPOINT_PAGES
        if options is None
        else options.get("wal_auto_checkpoint_pages", _DEFAULT_WAL_AUTO_CHECKPOINT_PAGES)
    )
    timeout_ms = (
        _DEFAULT_BUSY_TIMEOUT_MS
        if options is None
        else options.get("busy_timeout_ms", _DEFAULT_BUSY_TIMEOUT_MS)
    )
    if path != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=timeout_ms / 1000, isolation_level=None)
    connection.row_factory = sqlite3.Row
    database = PosixSqliteDatabase(connection)
    try:
        await database.exec("PRAGMA journal_mode = WAL")
        await database.exec("PRAGMA synchronous = NORMAL")
        await database.exec(f"PRAGMA wal_autocheckpoint = {checkpoint_pages}")
        return database
    except BaseException:
        try:
            await database.close()
        except Exception:
            pass
        raise


async def open_posix_sqlite_storage(
    path: str,
    options: PosixSqliteStorageOptions | None = None,
) -> SqliteStorage:
    """Open or create file-backed durable storage, optionally under a table prefix."""

    prefix = None if options is None else options.get("prefix")
    if prefix is not None:
        validate_sqlite_schema_prefix(prefix)
    database = await open_posix_sqlite_database(path, options)
    storage_options: SqliteStorageOptions | None = (
        None if prefix is None else {"prefix": prefix}
    )
    return await SqliteStorage.open(database, storage_options)


__all__ = [
    "PosixSqliteDatabase",
    "PosixSqliteDatabaseOptions",
    "PosixSqliteStorageOptions",
    "open_posix_sqlite_database",
    "open_posix_sqlite_storage",
]

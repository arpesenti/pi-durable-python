"""Portable SQLite facade contracts, translated from v1.3.0 ``sqlite/database.ts``.

The core never imports a driver: it accepts any :class:`SqliteDatabase`, and the stdlib adapter in
:mod:`pi_durable.storage.sqlite.posix` supplies one. Operations return awaitables rather than being
``async`` functions so a caller's call order is the order the adapter observes, matching the
promise-returning upstream facade.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, TypeAlias, TypeVar

SqliteValue: TypeAlias = None | int | float | str | bytes
"""One positional binding accepted by the portable SQLite storage core."""

T = TypeVar("T")


class SqliteExecutor(Protocol):
    """Asynchronous SQL operations shared by a database and its transaction handles.

    ``exec`` runs SQL text without bindings and may contain several statements. ``run``, ``get``,
    and ``all`` execute one statement with positional bindings. Adapters may cache prepared
    statements by SQL text, so callers pass values as bindings instead of interpolating them.
    """

    def exec(self, sql: str) -> Awaitable[None]:
        """Run one or more statements without bindings."""

    def run(self, sql: str, *params: SqliteValue) -> Awaitable[None]:
        """Run one statement with positional bindings."""

    def get(self, sql: str, *params: SqliteValue) -> Awaitable[dict[str, Any] | None]:
        """Return the first row of one statement, or ``None``."""

    def all(self, sql: str, *params: SqliteValue) -> Awaitable[list[dict[str, Any]]]:
        """Return every row of one statement."""


class SqliteDatabase(SqliteExecutor, Protocol):
    """Minimal database facade required by :class:`~pi_durable.storage.sqlite.SqliteStorage`.

    ``transaction`` passes the callback a transaction handle. All work in the transaction must use
    that handle; the handle is invalid after the callback settles. Adapters must queue unrelated
    operations and other transactions until the transaction finishes. The returned awaitable
    settles after commit or rollback. Calling the database itself (including ``transaction`` or
    ``close``) from inside a callback therefore waits for that transaction and never settles.

    When the callback raises, the adapter must roll the transaction back before re-raising that
    same error. If rollback fails, it must raise a different error (here an ``ExceptionGroup``) so
    callers cannot mistake the callback error for a guaranteed rollback.
    """

    def transaction(self, callback: Callable[[SqliteExecutor], Awaitable[T]]) -> Awaitable[T]:
        """Run one callback as one SQL transaction."""

    def close(self) -> Awaitable[None]:
        """Release the connection; later operations reject or fail at the driver."""


__all__ = ["SqliteDatabase", "SqliteExecutor", "SqliteValue"]

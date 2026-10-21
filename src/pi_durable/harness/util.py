"""Scheduler support: pending waits, paginated scans, and closed-Harness errors."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Hashable
from typing import Any, Generic, TypeVar

from ..context import AbortError, AbortSignal, Context
from ..errors import SessionFailed
from ..types import Cursor, Page

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)


def cancellation_error(signal: AbortSignal) -> BaseException:
    """Return what an aborted context rejects with: its reason if it is an error, else ``AbortError``."""

    reason = signal.reason
    if isinstance(reason, BaseException):
        return reason
    return AbortError(reason)


class Waiters(Generic[K, T]):
    """Pending waits by key. Each settles once: through ``resolve``, ``reject_all``, or its cancellation."""

    def __init__(self) -> None:
        self._sets: dict[K, set[asyncio.Future[T]]] = {}

    def add(self, key: K, context: Context) -> Awaitable[T]:
        signal = context.abort_signal
        if signal is not None and signal.aborted:
            return rejected(cancellation_error(signal))
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[T] = loop.create_future()
        waiter.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)
        self._sets.setdefault(key, set()).add(waiter)
        if signal is not None:

            def cancel() -> None:
                values = self._sets.get(key)
                if values is not None:
                    values.discard(waiter)
                    if not values and self._sets.get(key) is values:
                        del self._sets[key]
                if not waiter.done():
                    waiter.set_exception(cancellation_error(signal))

            remove = signal.add_listener(cancel)
            waiter.add_done_callback(lambda _: remove())
        return waiter

    def keys(self) -> list[K]:
        return list(self._sets)

    def resolve(self, key: K, value: T) -> None:
        values = self._sets.pop(key, None)
        for waiter in values or ():
            if not waiter.done():
                waiter.set_result(value)

    def reject_all(self, error: BaseException) -> None:
        sets = list(self._sets.values())
        self._sets.clear()
        for values in sets:
            for waiter in values:
                if not waiter.done():
                    waiter.set_exception(error)


def rejected(error: BaseException) -> asyncio.Future[Any]:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = loop.create_future()
    future.set_exception(error)
    future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
    return future


async def scan_all(scan: Callable[[Cursor | None], Awaitable[Page[Any, Cursor]]]) -> list[Any]:
    """Every item of a paginated scan, in page order."""

    items: list[Any] = []
    cursor: Cursor | None = None
    while True:
        page = await scan(cursor)
        items.extend(page["items"])
        cursor = page.get("next")
        if cursor is None:
            return items


def closed_error(session: Any) -> BaseException:
    """What a closed Harness rejects with: ``SessionFailed`` when it closed because the Session failed."""

    failure = session.failure
    return RuntimeError("Harness is closed") if failure is None else SessionFailed(failure)


__all__ = ["Waiters", "cancellation_error", "closed_error", "rejected", "scan_all"]

"""Scheduler support: pending waits, paginated scans, and closed-Harness errors."""

from __future__ import annotations

import asyncio
import inspect
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
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    future.set_exception(error)
    future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
    return future


def resolved(value: T) -> asyncio.Future[T]:
    future: asyncio.Future[T] = asyncio.get_running_loop().create_future()
    future.set_result(value)
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


def call_with_args(handler: Callable[..., Any], *args: Any) -> Any:
    """Call ``handler`` with only the arguments it declares, as JavaScript ignores extras."""

    try:
        parameters = list(inspect.signature(handler).parameters.values())
    except (TypeError, ValueError):
        return handler(*args)
    if any(parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in parameters):
        return handler(*args)
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    return handler(*args[: len(positional)])


def assign_json(target: Any, key: Any, value: Any) -> None:
    """Assign ``value`` at ``target[key]`` leaf by leaf, so a string leaf becomes an append."""

    slots = target
    current = slots.get(key) if hasattr(slots, "get") else slots[key]
    if _is_record(current) and _is_record(value):
        for name in list(current.keys()):
            if name not in value:
                del current[name]
        for name, child in value.items():
            assign_json(current, name, child)
        return
    if _is_array(current) and _is_array(value) and len(current) <= len(value):
        for index in range(len(value)):
            if index < len(current):
                assign_json(current, index, value[index])
            else:
                current.append(value[index])
        return
    if current != value:
        slots[key] = value


def _is_record(value: Any) -> bool:
    from ..session.drafts import DraftObject

    return isinstance(value, (dict, DraftObject))


def _is_array(value: Any) -> bool:
    from ..session.drafts import DraftList

    return isinstance(value, (list, DraftList))


__all__ = [
    "Waiters",
    "assign_json",
    "call_with_args",
    "cancellation_error",
    "closed_error",
    "rejected",
    "resolved",
    "scan_all",
]

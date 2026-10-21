"""Read-only replicated document state and serialized exact-frame watches.

Only committed revisions are published. A watch buffers at most 100 exact frames behind a
slow listener and then delivers a full-value reset, so callbacks never overlap and memory
stays bounded.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .delta import Op
from .json import copy_json, error_message

MAX_PENDING_WATCH_FRAMES = 100
RETIREMENT_OPERATIONS: list[Op] = [["r", None]]


@dataclass(frozen=True)
class WatchEnd:
    reason: str
    error: BaseException | None = None


StateListener = Callable[[Any, list[Op]], Awaitable[None] | None]
SyncListener = Callable[[Any, list[Op]], None]


class DocumentState:
    """Disposable read-only state bound to one committed document incarnation."""

    def __init__(self, value: Any, detach: Callable[[], None], report: Callable[[BaseException], None]) -> None:
        self._value = copy_json(value)
        self._detach = detach
        self._report = report
        self._listeners: list[SyncListener] = []
        self._closed = False
        self._queue: list[tuple[Any, list[Op]]] = []
        self._scheduled = False
        self._draining = False

    @property
    def value(self) -> Any:
        return copy_json(self._value)

    @property
    def closed(self) -> bool:
        return self._closed

    def subscribe(self, listener: SyncListener) -> Callable[[], None]:
        if self._closed:
            raise RuntimeError("Document state is closed")
        self._listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def advance(self, value: Any, ops: list[Op]) -> None:
        if self._closed:
            return
        self._value = copy_json(value)
        if not self._listeners:
            return
        self._queue.append((copy_json(value), copy_json(ops)))
        if not self._scheduled:
            self._scheduled = True
            asyncio.get_running_loop().call_soon(self._drain)

    def _drain(self) -> None:
        self._scheduled = False
        if self._closed:
            return
        while self._queue:
            value, ops = self._queue.pop(0)
            for listener in list(self._listeners):
                try:
                    listener(value, ops)
                except BaseException as error:  # noqa: BLE001 - listener containment is the contract
                    self._report(error)

    def close(self) -> None:
        """Stop publishing; the state keeps its last value."""
        if self._closed:
            return
        self._closed = True
        self._queue.clear()
        self._listeners.clear()
        self._detach()

    def fail(self) -> None:
        self.close()


class CommittedWatch:
    """Serialized exact-frame watch over one committed value. ``None`` retires it."""

    def __init__(self, value: Any, detach: Callable[[], None], report: Callable[[BaseException], None]) -> None:
        self._value = copy_json(value)
        self._detach = detach
        self._report = report
        self._pending: deque[tuple[Any, list[Op]]] = deque()
        self._listener: StateListener | None = None
        self._started = False
        self._scheduled = False
        self._running = False
        self._end: WatchEnd | None = None
        self._closed: asyncio.Future[WatchEnd] = asyncio.get_running_loop().create_future()
        self._closed.add_done_callback(lambda future: future.exception())

    @property
    def value(self) -> Any:
        return copy_json(self._value)

    @property
    def closed(self) -> Awaitable[WatchEnd]:
        return self._closed

    def start(self, listener: StateListener) -> None:
        if self._started:
            raise RuntimeError("Watch is already started")
        if self._end is not None:
            raise RuntimeError("Watch is stopped")
        self._started = True
        self._listener = listener
        if self._pending:
            self._schedule()

    async def stop(self) -> WatchEnd:
        self._terminate(WatchEnd("stopped"))
        return await self._closed

    def cancel(self) -> None:
        self._terminate(WatchEnd("cancelled"))

    def close_session(self, failure: BaseException | None = None) -> None:
        self._terminate(WatchEnd("session_failed", failure) if failure is not None else WatchEnd("session_closed"))

    def advance(self, value: Any, ops: list[Op]) -> None:
        if self._end is not None:
            return
        if len(self._pending) >= MAX_PENDING_WATCH_FRAMES:
            self._pending.clear()
            replacement = copy_json(value)
            self._pending.append((replacement, [["r", replacement]]))
        else:
            self._pending.append((copy_json(value), copy_json(ops)))
        if self._started:
            self._schedule()

    def _schedule(self) -> None:
        if self._scheduled or self._running or self._end is not None:
            return
        self._scheduled = True
        asyncio.get_running_loop().call_soon(self._spawn_drain)

    def _spawn_drain(self) -> None:
        self._scheduled = False
        if self._end is not None or not self._started:
            return
        asyncio.ensure_future(self._drain())

    async def _drain(self) -> None:
        if self._running or self._end is not None or not self._started:
            return
        self._running = True
        try:
            while self._end is None and self._pending:
                value, ops = self._pending.popleft()
                self._value = value
                listener = self._listener
                if listener is None:
                    break
                try:
                    result = listener(value, ops)
                    if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
                        await result
                except BaseException as error:  # noqa: BLE001 - ends this watch only
                    if self._end is None:
                        self._report(error)
                        self._terminate(WatchEnd("listener_error", _to_error(error)))
                    break
                if value is None:
                    self._terminate(WatchEnd("retired"))
                    break
        finally:
            self._running = False
            if self._end is None and self._pending:
                self._schedule()

    def _terminate(self, end: WatchEnd) -> None:
        if self._end is not None:
            return
        self._end = end
        self._pending.clear()
        try:
            self._detach()
        except Exception as error:  # noqa: BLE001
            self._report(error)
        if not self._closed.done():
            self._closed.set_result(end)


def _to_error(error: BaseException) -> BaseException:
    if isinstance(error, BaseException):
        return error
    return RuntimeError(error_message(error))


__all__ = [
    "MAX_PENDING_WATCH_FRAMES",
    "RETIREMENT_OPERATIONS",
    "CommittedWatch",
    "DocumentState",
    "StateListener",
    "WatchEnd",
]

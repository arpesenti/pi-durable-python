"""Committed observation: exact-frame watches and read-only attached states.

A watch is bound to one committed revision or view mount. It queues every committed change while
its listener runs, so callbacks never overlap, and collapses more than
``MAX_PENDING_WATCH_FRAMES`` pending frames into one root replacement. Retirement and close end
the watch with the v1.3.0 reason. A state publishes every later revision in order through its
listeners and keeps its last value at close.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from ..context import BACKGROUND_CONTEXT, AbortSignal, Context, without_abort_signal
from ..delta import Op
from ..errors import is_failure
from ..json import JsonObject
from ..types import DocumentCommittedChange, WatchEnd

ObservedDocumentValue = JsonObject | None
"""One observed revision; ``None`` is the canonical retirement value."""

MAX_PENDING_WATCH_FRAMES = 100
"""Maximum exact committed frames retained behind one unavailable watch listener."""

RETIREMENT_OPERATIONS: list[Op] = [["r", None]]
"""Canonical terminal update for a retired document incarnation."""


@dataclass(slots=True)
class Delivery:
    """One state delivery: hydration at sequence 0, then one per committed update."""

    kind: str
    sequence: int


class CommittedState:
    """Disposable, read-only committed state over one immutable value.

    A document state binds it to one incarnation; a conversation view or task graph binds it to
    the Harness's mount. Subscribing delivers the acquisition revision immediately and then every
    later committed revision, in order, without another read.
    """

    __slots__ = (
        "_closed",
        "_disposed",
        "_listeners",
        "_release",
        "_released",
        "_report",
        "_sequence",
        "_value",
    )

    def __init__(
        self,
        value: Any,
        release: Callable[[], None],
        report: Callable[[BaseException], None] | None = None,
    ) -> None:
        self._value = value
        self._release = release
        self._report = report
        self._released = False
        self._sequence = 0
        self._listeners: list[Callable[[Any, Context, Delivery], Any]] = []
        self._disposed = False
        self._closed = False

    @property
    def value(self) -> Any:
        return self._value

    def subscribe(self, listener: Callable[[Any, Context, Delivery], Any]) -> Callable[[], None]:
        """Subscribe; hydration is delivered immediately with sequence 0."""

        if self._disposed:
            raise RuntimeError("State is disposed")
        self._listeners.append(listener)
        self._deliver(listener, self._value, BACKGROUND_CONTEXT, Delivery("hydrate", self._sequence))

        def unsubscribe() -> None:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def dispose(self) -> None:
        """Idempotently stop future deliveries; the last value remains readable."""

        if self._disposed:
            return
        self._disposed = True
        self._listeners.clear()
        self._release_pin()

    def _release_pin(self) -> None:
        """Release the observer once; a retired incarnation keeps its last value but pins nothing."""

        if self._released:
            return
        self._released = True
        self._release()

    def advance(self, value: Any, _ops: list[Op], context: Context) -> None:
        """Publish one exact committed revision to every listener."""

        self._advance(value, context)

    def _advance(self, value: Any, context: Context) -> None:
        if self._disposed or self._closed:
            return
        self._value = value
        self._sequence += 1
        delivery = Delivery("update", self._sequence)
        for listener in list(self._listeners):
            self._deliver(listener, value, without_abort_signal(context), delivery)

    def close_session(self, _failure: BaseException | None = None) -> None:
        """An observer's Session-close callback; the last value and this state remain."""

        self._close_session()

    def _close_session(self) -> None:
        """Keep the last value at the Session's seal and receive no commit that settles later."""

        if self._closed:
            return
        self._closed = True
        self._listeners.clear()
        self._release_pin()

    def _deliver(
        self,
        listener: Callable[[Any, Context, Delivery], Any],
        value: Any,
        context: Context,
        delivery: Delivery,
    ) -> None:
        try:
            result = listener(value, context, delivery)
            if hasattr(result, "__await__"):
                asyncio.ensure_future(cast(Awaitable[Any], result))
        except Exception as error:
            if self._report is not None:
                self._report(error)


def observed_operations(
    observed_version: int,
    change: DocumentCommittedChange,
) -> tuple[list[Op], int]:
    """Return the operations one observer applies for a committed change, and its version.

    An observer hydrated under another definition version holds a differently shaped value, so it
    receives the new value as a root replacement instead of operations for that shape.
    """

    if change["value"] is None:
        return RETIREMENT_OPERATIONS, observed_version
    version = cast(int, change["version"])
    if version == observed_version:
        return change["ops"], observed_version
    return [["r", change["value"]]], version


class CommittedWatch:
    """Serialized exact-frame watch bound to one document incarnation (spec §9.2)."""

    __slots__ = (
        "_cancellation_remove",
        "_cancellation_signal",
        "_closed",
        "_terminal",
        "_detach",
        "_end",
        "_listener",
        "_observed_version",
        "_pending",
        "_release",
        "_replace",
        "_report",
        "_retired",
        "_running",
        "_scheduled",
        "_started",
        "_value",
    )

    def __init__(
        self,
        value: Any,
        detach: Callable[[], None],
        report: Callable[[BaseException], None],
        observed_version: int = 0,
        replace: Callable[[], Any] | None = None,
    ) -> None:
        self._value: Any = value
        self._detach = detach
        self._report = report
        self._observed_version = observed_version
        self._replace = replace
        self._closed: asyncio.Future[WatchEnd] = asyncio.get_running_loop().create_future()
        # Awaiting a cancellation must not cancel the watch's own terminal result; `closed` and
        # every `stop()` return this same shielded value, as v1.3.0 returns one common promise.
        self._terminal: asyncio.Future[WatchEnd] = asyncio.shield(self._closed)
        self._pending: list[tuple[Any, list[Op], Context]] = []
        self._listener: Callable[[Any, list[Op], Context], Any] | None = None
        self._started = False
        self._scheduled = False
        self._running = False
        self._retired = False
        self._end: WatchEnd | None = None
        self._cancellation_signal: AbortSignal | None = None
        self._cancellation_remove: Callable[[], None] | None = None

    @property
    def value(self) -> Any:
        """Acquisition revision before start; latest delivered revision afterward."""

        return self._value

    @property
    def closed(self) -> asyncio.Future[WatchEnd]:
        return self._terminal

    @property
    def retired(self) -> bool:
        """Whether the observed incarnation retired; a retired observer pins nothing."""

        return self._retired

    def start(
        self,
        listener: Callable[[Any, list[Op], Context], Awaitable[None] | None],
    ) -> None:
        """Install the sole asynchronous listener. Never invokes it inline."""

        if self._started:
            raise RuntimeError("Watch is already started")
        if self._end is not None:
            raise RuntimeError("Watch is stopped")
        self._started = True
        self._listener = listener
        if self._pending:
            self._schedule()

    def stop(self) -> asyncio.Future[WatchEnd]:
        """Idempotently stop future callbacks and return this watch's terminal result."""

        self._terminate({"reason": "stopped"})
        return self._terminal

    def observe_cancellation(self, signal: AbortSignal) -> None:
        if self._cancellation_signal is not None:
            raise RuntimeError("Watch cancellation is already installed")
        if self._end is not None:
            return
        self._cancellation_signal = signal
        self._cancellation_remove = signal.add_listener(self.cancel)
        if signal.aborted:
            self.cancel()

    def cancel(self) -> None:
        self._terminate({"reason": "cancelled"})

    def close_session(self, failure: BaseException | None = None) -> None:
        """End with ``session_closed``, or with ``session_failed`` and the error on failure."""

        if failure is None:
            self._terminate({"reason": "session_closed"})
        else:
            self._terminate({"reason": "session_failed", "error": failure})

    def advance(self, change: DocumentCommittedChange, context: Context) -> None:
        """Queue the committed document change, skipping one this observer already holds."""

        if self._end is not None or self._retired:
            return
        ops, version = observed_operations(self._observed_version, change)
        if not ops:
            return
        self._observed_version = version
        self.advance_frame(change["value"], ops, context)

    def advance_frame(self, value: Any, ops: list[Op], context: Context) -> None:
        """Queue one exact committed frame for a view, graph, or event stream."""

        if self._end is not None or self._retired:
            return
        if value is None:
            self._retired = True
        if len(self._pending) >= MAX_PENDING_WATCH_FRAMES:
            self._pending.clear()
            replacement = self._replace() if self._replace is not None else value
            self._pending.append((replacement, [["r", replacement]], context))
        else:
            self._pending.append((value, ops, context))
        if self._started:
            self._schedule()

    def _schedule(self) -> None:
        if self._scheduled or self._running or self._end is not None:
            return
        self._scheduled = True
        asyncio.get_running_loop().call_soon(self._launch)

    def _launch(self) -> None:
        self._scheduled = False
        if self._end is not None or self._running or not self._started:
            return
        self._running = True
        asyncio.get_running_loop().create_task(self._drain())

    async def _drain(self) -> None:
        try:
            while self._end is None and self._pending:
                value, ops, context = self._pending.pop(0)
                self._value = value
                listener = self._listener
                assert listener is not None
                try:
                    result = listener(value, ops, without_abort_signal(context))
                    if isinstance(result, Awaitable):
                        await result
                except BaseException as error:
                    if not is_failure(error):
                        raise
                    if self._end is None:
                        self._report(error)
                        self._terminate({"reason": "listener_error", "error": error})
                    break
                if value is None:
                    self._terminate({"reason": "retired"})
                    break
        finally:
            self._running = False
            if self._end is None and self._pending:
                self._schedule()

    def _terminate(self, end: WatchEnd) -> None:
        """End the watch, once: detach, drop pending frames, and settle ``closed``."""

        if self._end is not None:
            return
        self._end = end
        self._pending.clear()
        if self._cancellation_remove is not None:
            remove = self._cancellation_remove
            self._cancellation_remove = None
            remove()
        try:
            self._detach()
        finally:
            if not self._closed.done():
                self._closed.set_result(end)


__all__ = [
    "CommittedState",
    "CommittedWatch",
    "Delivery",
    "MAX_PENDING_WATCH_FRAMES",
    "ObservedDocumentValue",
    "RETIREMENT_OPERATIONS",
    "observed_operations",
]

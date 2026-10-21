"""Committed document observation: exact-frame watches over one incarnation.

A watch is bound to one committed document revision. It queues every committed change while its
listener runs, so callbacks never overlap, and collapses more than
``MAX_PENDING_WATCH_FRAMES`` pending frames into one root replacement. Retirement and close end
the watch with the v1.3.0 reason.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

from ..context import AbortSignal, Context, without_abort_signal
from ..delta import Op
from ..json import JsonObject
from ..types import DocumentCommittedChange, WatchEnd

ObservedDocumentValue = JsonObject | None
"""One observed revision; ``None`` is the canonical retirement value."""

MAX_PENDING_WATCH_FRAMES = 100
"""Maximum exact committed frames retained behind one unavailable watch listener."""

RETIREMENT_OPERATIONS: list[Op] = [["r", None]]
"""Canonical terminal update for a retired document incarnation."""


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
        "_detach",
        "_end",
        "_listener",
        "_observed_version",
        "_pending",
        "_report",
        "_retired",
        "_running",
        "_scheduled",
        "_started",
        "_value",
    )

    def __init__(
        self,
        value: ObservedDocumentValue,
        detach: Callable[[], None],
        report: Callable[[BaseException], None],
        observed_version: int,
    ) -> None:
        self._value = value
        self._detach = detach
        self._report = report
        self._observed_version = observed_version
        self._closed: asyncio.Future[WatchEnd] = asyncio.get_running_loop().create_future()
        self._pending: list[tuple[ObservedDocumentValue, list[Op], Context]] = []
        self._listener: Callable[[ObservedDocumentValue, list[Op], Context], Any] | None = None
        self._started = False
        self._scheduled = False
        self._running = False
        self._retired = False
        self._end: WatchEnd | None = None
        self._cancellation_signal: AbortSignal | None = None
        self._cancellation_remove: Callable[[], None] | None = None

    @property
    def value(self) -> ObservedDocumentValue:
        """Acquisition revision before start; latest delivered revision afterward."""

        return self._value

    @property
    def closed(self) -> asyncio.Future[WatchEnd]:
        return self._closed

    @property
    def retired(self) -> bool:
        """Whether the observed incarnation retired; a retired observer pins nothing."""

        return self._retired

    def start(
        self,
        listener: Callable[[ObservedDocumentValue, list[Op], Context], Awaitable[None] | None],
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

    async def stop(self) -> WatchEnd:
        """Idempotently stop future callbacks and return this watch's terminal result."""

        self._terminate({"reason": "stopped"})
        return await asyncio.shield(self._closed)

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
        """Queue the committed change, skipping one this observer already holds."""

        if self._end is not None or self._retired:
            return
        ops, version = observed_operations(self._observed_version, change)
        if not ops:
            return
        self._observed_version = version
        value = change["value"]
        if value is None:
            self._retired = True
        if len(self._pending) >= MAX_PENDING_WATCH_FRAMES:
            self._pending.clear()
            self._pending.append((value, [["r", value]], context))
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
                except asyncio.CancelledError:
                    raise
                except BaseException as error:
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
    "CommittedWatch",
    "MAX_PENDING_WATCH_FRAMES",
    "ObservedDocumentValue",
    "RETIREMENT_OPERATIONS",
    "observed_operations",
]

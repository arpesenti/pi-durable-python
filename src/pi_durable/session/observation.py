"""Committed observation: coalesced watches and read-only attached states.

A watch is bound to one committed revision or view mount. While its listener runs, later commits
wait in :class:`~pi_durable.delta.PendingOps` and are delivered folded — one call with the newest
value, the folded operations, the newest commit's context, and a retirement flag — so a slow or
unstarted listener costs one call per burst and, past the pending-operation bound, one root
replacement. A watch follows its address rather than one incarnation: a retirement is a change with
the last value and no operations, and a document created at that address later arrives on the same
watch as a root replacement. A watch ends five ways, and only those: ``stopped`` by its own
``stop()``, ``aborted`` by the acquisition context's abort signal, ``ended`` or ``failed`` by its
Session, and ``listener_error`` by the error its listener raised. Termination drops every reference
the watch holds into its Session and its listener, keeping only its last value. A state publishes
every later revision in order through its listeners and keeps its last value at close.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from ..context import BACKGROUND_CONTEXT, AbortSignal, Context, without_abort_signal
from ..delta import Op, PendingOps
from ..errors import is_failure
from ..ids import DocumentId
from ..types import (
    DocumentCommitChange,
    WatchCall,
    WatchEnd,
    WatchValueCall,
    watch_value_call,
)


class _Unchanged:
    """The type of :data:`UNCHANGED`, a value a delivery does not carry."""


UNCHANGED = _Unchanged()
"""The marker of a delivery that brings no new value: a retirement keeps the last one."""

RETIREMENT_OPERATIONS: list[Op] = [["r", None]]
"""Canonical terminal update for a retired document incarnation.

Only an attached :class:`CommittedState` applies it; a 1.5.0 watch is never given ``None``, because
Durable has no value for an absent document (spec §9.1).
"""


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

    def advance(self, call: WatchCall[Any], context: Context) -> None:
        """Publish one committed revision to every listener; a state reads only its value."""

        if call["kind"] != "value":
            return
        self._advance(call["value"], context)

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
    change: DocumentCommitChange,
) -> tuple[list[Op], int]:
    """Return the operations one observer applies for a committed change, and its version.

    An observer hydrated under another definition version holds a differently shaped value, so it
    receives the new value as a root replacement instead of operations for that shape. The change
    carries a single tag: the retirement is the ``kind`` tag, not a ``null`` value.
    """

    if change["type"] != "document":
        # A definition-free copy publishes no adopted value: an observer keeps what it holds.
        return [], observed_version
    if change["kind"] == "retire":
        return RETIREMENT_OPERATIONS, observed_version
    version = change["version"]
    if version == observed_version:
        return change["ops"], observed_version
    return [["r", change["value"]]], version


class CommittedWatch:
    """Serialized coalesced watch of one document address, view mount, or task-graph mount (§9.2).

    Commits that land while the listener runs, or before :meth:`start`, wait folded in one
    :class:`~pi_durable.delta.PendingOps` queue and arrive together: the newest value, the folded
    operations or one root replacement past the bound, the newest commit's context, and a retirement
    flag. A watch is the host's to stop, and never ends on a retirement.
    """

    __slots__ = (
        "__weakref__",
        "_cancellation_remove",
        "_cancellation_signal",
        "_closed",
        "_terminal",
        "_detach",
        "_end",
        "_listener",
        "_observed_version",
        "_pending",
        "_pending_change",
        "_pending_context",
        "_pending_retired",
        "_pending_value",
        "_record_id",
        "_report",
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
        record_id: DocumentId | None = None,
    ) -> None:
        self._value: Any = value
        self._detach: Callable[[], None] | None = detach
        self._report: Callable[[BaseException], None] | None = report
        self._observed_version = observed_version
        self._record_id = record_id
        self._closed: asyncio.Future[WatchEnd] = asyncio.get_running_loop().create_future()
        # Awaiting a cancellation must not cancel the watch's own terminal result; `closed` and
        # every `stop()` return this same shielded value, as v1.3.0 returns one common promise.
        self._terminal: asyncio.Future[WatchEnd] = asyncio.shield(self._closed)
        self._pending = PendingOps()
        self._pending_change = False
        self._pending_value: Any = value
        self._pending_context: Context = BACKGROUND_CONTEXT
        self._pending_retired = False
        self._listener: Callable[[WatchCall[Any], Context], Any] | None = None
        self._started = False
        self._scheduled = False
        self._running = False
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

    def start(
        self,
        listener: Callable[[WatchCall[Any], Context], Awaitable[None] | None],
    ) -> None:
        """Install the sole asynchronous listener. Never invokes it inline."""

        if self._started:
            raise RuntimeError("Watch is already started")
        if self._end is not None:
            raise RuntimeError("Watch is stopped")
        self._started = True
        self._listener = listener
        if self._pending_change:
            self._schedule()

    def stop(self) -> asyncio.Future[WatchEnd]:
        """Idempotently stop future callbacks and return this watch's terminal result.

        This is the host's cancel entry point, and the one the shipped build carries
        (``chord/src/watch.ts:46-55``): it settles ``{"kind": "stopped"}``. The ``aborted`` end is
        separate — only :meth:`abort_acquisition`, the acquisition context's abort signal, emits it.
        """

        self._terminate({"kind": "stopped"})
        return self._terminal

    def observe_cancellation(self, signal: AbortSignal) -> None:
        if self._cancellation_signal is not None:
            raise RuntimeError("Watch cancellation is already installed")
        if self._end is not None:
            return
        self._cancellation_signal = signal
        self._cancellation_remove = signal.add_listener(self.abort_acquisition)
        if signal.aborted:
            self.abort_acquisition()

    def abort_acquisition(self) -> None:
        """End ``aborted``: the context the watch was acquired with was aborted.

        Distinct from :meth:`stop` by upstream's own typing (``chord/src/watch.ts:16-19``): the
        changelog calls this end ``cancelled``, the shipped build calls it ``aborted``.
        """

        self._terminate({"kind": "aborted"})

    def close_session(self, failure: BaseException | None = None) -> None:
        """End with ``ended``, or with ``failed`` and the error that failed the Session."""

        if failure is None:
            self._terminate({"kind": "ended"})
        else:
            self._terminate({"kind": "failed", "error": failure})

    def advance(self, call: WatchCall[Any], context: Context) -> None:
        """Queue one committed call: the seam a mount, or an observer of one, advances.

        A retirement is a call like any other and not an end: it keeps the last value and carries no
        operations, and the watch stays open until :meth:`stop` or the Session's own end.
        """

        if self._end is not None or call["kind"] != "value":
            return
        self._queue(call["value"], call["ops"], call["retired"], context)

    def advance_document(self, change: DocumentCommitChange, context: Context) -> None:
        """Queue the committed document change at this watch's address (spec §9.2).

        A watch follows its address rather than one incarnation: a retirement keeps the last value
        and carries no operations, and a document created at the address later arrives as a root
        replacement of the new incarnation, folded with what is pending.
        """

        if self._end is not None:
            return
        if change["type"] != "document":
            return
        if change["kind"] == "retire":
            # Durable has no value for an absent document: the retirement carries neither a value
            # nor operations, only the flag (spec §9.1).
            self._queue(UNCHANGED, [], True, context)
            return
        ops, version = observed_operations(self._observed_version, change)
        if self._record_id is not None and change["record"]["id"] != self._record_id:
            ops, version = [["r", change["value"]]], change["version"]
        if not ops:
            return
        self._observed_version = version
        self._record_id = change["record"]["id"]
        self._queue(change["value"], ops, False, context)

    def _queue(
        self,
        value: Any,
        ops: list[Op],
        retired: bool,
        context: Context,
    ) -> None:
        """Fold one change into what waits behind the listener, and schedule a call for it."""

        if ops:
            self._pending.push(ops)
        if value is not UNCHANGED:
            self._pending_value = value
        if retired:
            self._pending_retired = True
        # The newest commit's Context is preserved, without the producer's cancellation.
        self._pending_context = without_abort_signal(context)
        self._pending_change = True
        if self._started:
            self._schedule()

    def _take(self) -> WatchValueCall[Any] | None:
        """Take what waited behind the listener as one call, or ``None`` when nothing did."""

        if not self._pending_change:
            return None
        self._pending_change = False
        retired = self._pending_retired
        self._pending_retired = False
        value = self._pending_value
        ops = self._pending.take()
        # Past the pending-operation bound the folded operations give way to one root replacement.
        return watch_value_call(value, [["r", value]] if ops is None else ops, retired)

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
            while self._end is None:
                call = self._take()
                if call is None:
                    break
                # Immediately before the invocation, `value` advances to the delivered revision.
                self._value = call["value"]
                listener = self._listener
                assert listener is not None
                context = self._pending_context
                try:
                    result = listener(call, context)
                    if isinstance(result, Awaitable):
                        await result
                except BaseException as error:
                    if not is_failure(error):
                        raise
                    if self._end is None:
                        report = self._report
                        if report is not None:
                            report(error)
                        self._terminate({"kind": "listener_error", "error": error})
                    break
        finally:
            self._running = False
            if self._end is None and self._pending_change:
                self._schedule()

    def _terminate(self, end: WatchEnd) -> None:
        """End the watch, once: drop every reference into the Session, and settle ``closed``.

        What is still queued is discarded, and the listener, the detach closure, the report sink,
        and the abort hook are released, so a stopped watch that is still referenced keeps nothing of
        its Session or its listener — only its last value (spec §9.1). The end carries the one tag:
        ``closed`` settles with the ``kind`` the port emits for the way this watch ended.
        """

        if self._end is not None:
            return
        self._end = end
        self._pending = PendingOps()
        self._pending_change = False
        self._pending_retired = False
        self._pending_context = BACKGROUND_CONTEXT
        self._listener = None
        self._report = None
        if self._cancellation_remove is not None:
            remove = self._cancellation_remove
            self._cancellation_remove = None
            remove()
        detach, self._detach = self._detach, None
        try:
            if detach is not None:
                detach()
        finally:
            if not self._closed.done():
                self._closed.set_result(self._end)


__all__ = [
    "CommittedState",
    "CommittedWatch",
    "Delivery",
    "RETIREMENT_OPERATIONS",
    "UNCHANGED",
    "observed_operations",
]

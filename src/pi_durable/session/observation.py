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
the watch holds into its Session and its listener, keeping only its last value. An observer of the
same address, view mount, or graph mount is handed the very calls a watch is handed, unfolded and
synchronously, on the mutation line: it mirrors state for a host that follows the Session rather
than rendering it, and its detach — idempotent, like a watch's ``stop()`` — leaves nothing pinned.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, Protocol, cast

from ..context import BACKGROUND_CONTEXT, AbortSignal, Context, without_abort_signal
from ..delta import Op, PendingOps
from ..errors import is_failure
from ..ids import DocumentId
from ..types import (
    CommitObserver,
    DocumentCommitChange,
    Observer,
    WatchCall,
    WatchEnd,
    WatchValueCall,
    watch_value_call,
)


class _Unchanged:
    """The type of :data:`UNCHANGED`, a value a delivery does not carry."""


UNCHANGED = _Unchanged()
"""The marker of a delivery that brings no new value: a retirement keeps the last one."""

SESSION_CLOSED = "session closed"
"""What a Session close ends a synchronous observer with: an adapter follows the next Session.

A watch gets ``ended`` at the close of the Session it rode; an observer mirrors state for a host
that survives the close and reopens, so it gets ``failed`` with this error, and ``ended`` stays what
an observed owner that ended carries — a task document's task (#44, decision "Slice 8 S1").
"""

ENDED: WatchEnd = {"kind": "ended"}
"""The end of an observer whose observed owner ended."""


def session_closed_error() -> RuntimeError:
    """The error one Session close ends its observers with, made fresh for that close."""

    return RuntimeError(SESSION_CLOSED)


def contained(callback: Callable[[], Any], on_error: Callable[[BaseException], None]) -> None:
    """Run a host callback without failing its caller; a throw or rejection goes to ``on_error``."""

    try:
        result = callback()
        if not hasattr(result, "__await__"):
            return
        future = asyncio.ensure_future(cast(Awaitable[Any], result))

        def settle(done: asyncio.Future[Any]) -> None:
            if done.cancelled():
                return
            try:
                error = done.exception()
            except BaseException as caught:
                error = caught
            if error is not None:
                on_error(error)

        future.add_done_callback(settle)
    except BaseException as error:
        on_error(error)


RETIREMENT_OPERATIONS: list[Op] = [["r", None]]
"""Canonical terminal update for a retired document incarnation.

Only an attached :class:`CommittedState` applies it; a 1.5.0 watch is never given ``None``, because
Durable has no value for an absent document (spec §9.1).
"""


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
        "_ending",
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
        self._ending = False
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
    def ending(self) -> bool:
        """Internal: whether the watch is ended by its owner and only owes its last call.

        The Session stops handing a watch of a task document the commits after the one that ended
        its task, which is what unsubscribing it from the line means (§9.1). Hosts have no use for
        it: :meth:`stop` stays the only cancel entry point a caller has.
        """

        return self._ending

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
        """End with ``ended``, or with ``failed`` and the error that failed the Session.

        This ends the watch at once, whatever was still owed to the listener: a retirement the watch
        never delivered — because its listener never started, or was still busy in an earlier call —
        is dropped rather than delivered first (spec §9.1).
        """

        if failure is None:
            self._terminate({"kind": "ended"})
        else:
            self._terminate({"kind": "failed", "error": failure})

    def end_after_delivery(self) -> None:
        """End ``ended`` once the call this watch owes its listener returned (spec §9.1).

        Internal, the Session's own entry point beside :meth:`close_session`: a task document's
        watch is owed the retirement that the commit ending its task published, and that call is its
        last. Three dispositions are distinguished here, and collapsing them is what the upstream
        suite catches:

        - the retirement is queued behind a listener that has not been called yet, or the listener
          is busy in an earlier call: the end comes after that call returns;
        - the listener was never started: the owed call never arrives, so the watch stays open until
          the Session's close or failure ends it at once, dropping the retirement;
        - nothing is owed at all — the retirement was already delivered and returned: the watch ends
          now.

        The end is ``ended``, never ``failed``: the owner ended, the Session did not fail. Only
        :meth:`stop` and the acquisition abort are the host's cancel entry points.
        """

        if self._end is not None or self._ending:
            return
        self._ending = True
        self._end_if_delivered()

    def _end_if_delivered(self) -> None:
        """End the watch its owner ended by, once nothing is owed to the listener any more."""

        if not self._ending or self._end is not None:
            return
        if not self._started or self._running or self._scheduled or self._pending_change:
            # A call is running or still queued; a listener that never started owes one that never
            # arrives, which only the Session's own end discharges.
            return
        self._terminate(ENDED)

    def advance(self, call: WatchCall[Any], context: Context) -> None:
        """Queue one committed call: the seam a mount, or an observer of one, advances.

        A retirement is a call like any other and not an end: it keeps the last value and carries no
        operations, and the watch stays open until :meth:`stop` or the Session's own end.
        """

        if self._end is not None or self._ending or call["kind"] != "value":
            return
        self._queue(call["value"], call["ops"], call["retired"], context)

    def advance_document(self, change: DocumentCommitChange, context: Context) -> None:
        """Queue the committed document change at this watch's address (spec §9.2).

        A watch follows its address rather than one incarnation: a retirement keeps the last value
        and carries no operations, and a document created at the address later arrives as a root
        replacement of the new incarnation, folded with what is pending.
        """

        if self._end is not None or self._ending:
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
            if self._end is None:
                if self._pending_change:
                    self._schedule()
                else:
                    self._end_if_delivered()

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


class _SyncObserver(Protocol):
    """What the guard calls on a host's observer, whichever of the two shapes it is.

    A state observer (:class:`~pi_durable.types.Observer`) and a commit observer
    (:class:`~pi_durable.types.CommitObserver`) differ only in what ``advance`` is handed; the guard
    serves both, and ``advance`` is typed ``Any`` because a host that hands back an awaitable has its
    rejection read as its throw, exactly as :func:`contained` reads one.
    """

    def advance(self, call: Any, context: Context) -> Any: ...

    def end(self, end: WatchEnd) -> None: ...


class GuardedObserver:
    """One host observer behind the rule every synchronous observer runs under (spec §9.1).

    A throw from ``advance`` — or an awaitable it returns that rejects — is reported, detaches the
    observer, and ends it ``listener_error``, so it never reaches the commit underway. Nothing
    reaches the observer after its detach or its end, and from then on this object keeps neither the
    host's observer nor its report sink. ``call`` is the :data:`~pi_durable.types.WatchCall` a state
    observer mirrors or the :class:`~pi_durable.types.CommitPublication` a commit observer mirrors.
    """

    __slots__ = ("_detached", "_detach", "_observer", "_pending", "_report")

    def __init__(
        self,
        observer: Observer | CommitObserver,
        detach: Callable[[], None],
        report: Callable[[BaseException], None],
    ) -> None:
        self._observer: _SyncObserver | None = observer
        self._detach: Callable[[], None] | None = detach
        self._report: Callable[[BaseException], None] | None = report
        self._detached = False
        self._pending = 0

    @property
    def attached(self) -> bool:
        """Whether the observer is still attached: a caller stops handing it work once it is not."""

        return not self._detached

    def advance(self, call: Any, context: Context) -> None:
        """Hand the observer one call, synchronously; its own throw is contained here."""

        if self._detached:
            return
        observer = self._observer
        assert observer is not None
        try:
            result = observer.advance(call, context)
        except BaseException as error:
            if not is_failure(error):
                raise
            self._fault(error)
            return
        if hasattr(result, "__await__"):
            # A rejected awaitable is the observer's throw, after the fact (§9.1). It settles on the
            # loop, so the report sink stays reachable until it does: a close that detaches the
            # observer meanwhile must not swallow the report that call is owed.
            self._pending += 1
            contained(lambda: result, self._rejected)

    def end(self, end: WatchEnd) -> None:
        """End the observer, once; its detach runs first and nothing reaches it afterwards."""

        released = self._release()
        if released is None:
            return
        observer, report = released
        contained(partial(observer.end, end), report)

    def detach(self) -> None:
        """Idempotently detach the observer; no call and no end follows it."""

        self._release()

    def close_session(self, failure: BaseException | None = None) -> None:
        """End the observer as the close of its Session ends it: ``failed``, never ``ended``.

        A failed Session ends the observer with the error that failed it; a clean close ends it
        with a session-closed error, because the host's adapter follows the next Session (§9.1).
        """

        self.end(
            {"kind": "failed", "error": failure if failure is not None else session_closed_error()}
        )

    def _rejected(self, error: BaseException) -> None:
        """Handle the rejection of an awaitable the observer returned, which settles after the call."""

        self._pending -= 1
        self._fault(error)

    def _fault(self, error: BaseException) -> None:
        """Report the observer's throw, then detach it and end it ``listener_error``, once."""

        report = self._report
        if report is not None:
            report(error)
        released = self._release()
        if released is None:
            return
        observer, released_report = released
        contained(
            partial(observer.end, {"kind": "listener_error", "error": error}),
            released_report,
        )

    def _release(self) -> tuple[_SyncObserver, Callable[[BaseException], None]] | None:
        """Detach once, and hand back what the observer was attached with, or ``None`` if already gone.

        The report sink goes with it, except while a call the observer was handed is still awaiting:
        whatever that call settles to is still owed its report.
        """

        if self._detached:
            return None
        self._detached = True
        observer, self._observer = self._observer, None
        detach, self._detach = self._detach, None
        report = self._report
        if self._pending == 0:
            self._report = None
        assert observer is not None
        if detach is not None:
            detach()
        return (observer, report if report is not None else _unreported)


class AttachedObserver(GuardedObserver):
    """The Session's attachment of one host observer to one document address (spec §9.1, §9.2).

    Like a watch it follows the address rather than one incarnation and is handed the same
    :data:`~pi_durable.types.WatchCall` shapes; unlike a watch it is handed every change unfolded
    and synchronously, and it ends ``ended`` when the owner of the address ends — the task of a task
    document, after the retirement that ends it — rather than living until it is stopped.
    """

    __slots__ = ("_observed_version", "_record_id", "_value")

    def __init__(
        self,
        observer: Observer,
        detach: Callable[[], None],
        report: Callable[[BaseException], None],
        value: Any,
        observed_version: int,
        record_id: DocumentId,
    ) -> None:
        super().__init__(observer, detach, report)
        self._value: Any = value
        self._observed_version = observed_version
        self._record_id: DocumentId | None = record_id

    @property
    def value(self) -> Any:
        """The revision the observer is at: what every later call reaches from."""

        return self._value

    def advance_document(self, change: DocumentCommitChange, context: Context) -> None:
        """Deliver one committed change at the followed address, unfolded and in line order.

        A retirement of the followed incarnation is a call with the last value this observer was
        given and no operations, and a change of another definition version — or of a new
        incarnation at the address — is shown whole, because the observer's value does not have that
        shape. The observer may detach in its own call, or throw and be detached: what is left of
        the commit is not its business, which the caller checks through :attr:`attached`.
        """

        if not self.attached:
            return
        if change["type"] != "document":
            return
        if change["kind"] == "retire":
            # Only the retirement of the incarnation followed is a change: Durable has no value for
            # an absent document, so the call keeps the last value and carries no operations.
            if change["record"]["id"] != self._record_id:
                return
            self._record_id = None
            # The retirement hands back the last value and the observer keeps none of it: an absent
            # address has no value to mirror, and a retired one keeps nothing cached for it.
            last = self._value
            self._value = None
            self.advance(watch_value_call(last, [], True), context)
            return
        record_id = change["record"]["id"]
        value = change["value"]
        if record_id != self._record_id or change["version"] != self._observed_version:
            self._record_id = record_id
            self._observed_version = change["version"]
            self._value = value
            self.advance(watch_value_call(value, [["r", value]]), context)
            return
        if not change["ops"]:
            # A migration-only base changes nothing for an observer already at that version.
            return
        self._value = value
        self.advance(watch_value_call(value, change["ops"]), context)


class AttachedObservation:
    """What attaching an observer hands back: the value at attachment, and how to detach it.

    Built outside the attaching closure and holding nothing but the value and the guard, so a
    detached observation keeps neither the Session nor the host's observer alive.
    """

    __slots__ = ("_guarded", "value")

    def __init__(self, value: Any, guarded: GuardedObserver) -> None:
        self.value = value
        self._guarded = guarded

    def detach(self) -> None:
        """Idempotently detach the observer this observation attached."""

        self._guarded.detach()


def _unreported(_error: BaseException) -> None:
    """The sink of an observer whose report was already spent: its own end throw is dropped."""


__all__ = [
    "ENDED",
    "SESSION_CLOSED",
    "AttachedObserver",
    "AttachedObservation",
    "CommittedWatch",
    "GuardedObserver",
    "RETIREMENT_OPERATIONS",
    "UNCHANGED",
    "contained",
    "observed_operations",
    "session_closed_error",
]

"""The Session's writer: commits the line admitted, stored off the line, published in line order.

The line admits each commit as a *part* once its callback and settlement succeeded (logical
admission); the writer sends every part admitted while no call is in flight as one
:meth:`~pi_durable.types.Storage.commit_batches` call off the line, and publishes the parts of a
call one by one, in line order, with a hop between two -- so the continuations of the waits one
resolves start before the next is published. Each part registers the keys it writes; until it is
published it is the newest unpublished writer of each, and a job that touches one waits for its
gate. A key leaves when its newest writer is published, so the map holds one entry per key with an
unpublished writer (group-commit document §3, §4.1, §5.3).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from ..context import Context, without_abort_signal
from ..errors import SessionFailed
from ..ids import Seq
from ..types import CommitReceipt, CommitResult, StorageWrite

#: How one part's own failure at publication came about: memory could not adopt it, or one of the
#: Session's own observers threw.
PublishKind = Literal["adoption", "listener"]


@dataclass(slots=True)
class PublishFailure:
    """What one part's publication reported: the Session's own failure at that part, by origin."""

    kind: PublishKind
    error: BaseException


@dataclass(slots=True)
class PartSpec:
    """One logical commit the line admitted: stored (``writes``), or memory-only."""

    #: Line index of the job that admitted it: a failure it causes makes only later
    #: outcomes ``SessionFailed``.
    index: int
    writes: Sequence[StorageWrite] | None
    #: Keys it writes: a read of one waits for its publication.
    keys: list[str]
    context: Context
    #: Publish it in memory, in line order, once it is durable (with its seq).
    publish: Callable[[Seq | None], PublishFailure | None]
    #: It will never be published: abort what it prepared.
    discard: Callable[[], None]


@dataclass(slots=True)
class Part:
    """An admitted part and its outcome."""

    spec: PartSpec
    #: Resolves once it is published; rejects once it never will be, and also after publication when
    #: its storage call rejected or the Session failed meanwhile.
    outcome: asyncio.Future[None]


@dataclass(slots=True)
class _Admitted:
    """A part, its settle hooks, and the gate every job that touched one of its keys waits on."""

    spec: PartSpec
    outcome: asyncio.Future[None]
    gate: asyncio.Future[None]
    done: bool = False

    @property
    def index(self) -> int:
        return self.spec.index

    @property
    def writes(self) -> Sequence[StorageWrite] | None:
        return self.spec.writes

    @property
    def keys(self) -> list[str]:
        return self.spec.keys

    @property
    def context(self) -> Context:
        return self.spec.context


@dataclass(slots=True)
class _FailedOutcome:
    """How a part that will not resolve settles: with an error of its own, which may be any
    exception, or with ``SessionFailed`` when the Session's failure is its outcome."""

    error: BaseException | None = None

    @property
    def is_own(self) -> bool:
        return self.error is not None


@dataclass(slots=True)
class _Publication:
    """What one part's publication found and returned, written by the publisher the scope ran."""

    #: The Session's failure, checked the moment the scope ran the publication.
    external: BaseException | None = None
    failure: PublishFailure | None = None


class WriterHost(Protocol):
    """What the writer needs of its Session."""

    async def commit(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        context: Context,
    ) -> CommitResult: ...

    def publishing(self, publish: Callable[[], None], context: Context) -> Awaitable[None] | None: ...

    def stored(self, writes: Sequence[StorageWrite]) -> None:
        """Apply a stored batch to the Session's memory of what it committed, before publishing it."""
        ...

    def failure(self) -> BaseException | None: ...

    def fail(self, error: BaseException, at: int, own: Sequence[int]) -> None: ...

    def report(self, error: BaseException, context: Context) -> None: ...


class Writer:
    """The writer of one Session: the pending batch, the unpublished write set, and publication."""

    def __init__(self, host: WriterHost) -> None:
        self._host = host
        # Admitted parts not yet sent, in line order.
        self._queue: list[_Admitted] = []
        self._flight = False
        # Per key, the newest unpublished part that writes it.
        self._unpublished: dict[str, _Admitted] = {}
        # The newest admitted part not yet published; its gate opens after every earlier one's.
        self._newest: _Admitted | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    def admit(self, spec: PartSpec) -> Part:
        """Admit a part: register its keys, queue it, and send it at once unless a call is in flight."""

        loop = asyncio.get_running_loop()
        outcome: asyncio.Future[None] = loop.create_future()
        # Always handled: a part nobody awaits must not report an unhandled rejection.
        outcome.add_done_callback(_consumed)
        gate: asyncio.Future[None] = loop.create_future()
        gate.add_done_callback(_consumed)
        part = _Admitted(spec=spec, outcome=outcome, gate=gate)

        for key in spec.keys:
            self._unpublished[key] = part
        self._newest = part
        self._queue.append(part)
        self._pump()
        return Part(spec=spec, outcome=outcome)

    def gate(self, key: str) -> asyncio.Future[None] | None:
        """The gate of the newest unpublished writer of ``key``, or ``None`` when it has none."""

        part = self._unpublished.get(key)
        return None if part is None else part.gate

    def drained(self) -> asyncio.Future[None] | None:
        """Opens once every part admitted so far is published, or never will be."""

        return None if self._newest is None else self._newest.gate

    @property
    def unpublished(self) -> int:
        """Keys with an unpublished writer, for tests that bound memory."""

        return len(self._unpublished)

    # ── Sending ────────────────────────────────────────────────────────────

    def _pump(self) -> None:
        if self._flight or not self._queue:
            return
        self._flight = True
        parts = self._queue
        self._queue = []
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - the writer runs on the line, inside a loop
            self._flight = False
            return
        task = loop.create_task(self._flush(parts))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _flush(self, parts: Sequence[_Admitted]) -> None:
        """Send and publish ``parts``, then what was admitted meanwhile.

        Never rejects: a throw the writer did not expect fails the Session and settles the parts, so
        no outcome or gate is stranded. A cancellation is not a failure: it unwinds the work.
        """

        try:
            await self._send(parts)
        except Exception as error:
            self._abandon(parts, error)
        finally:
            self._flight = False
        self._pump()

    async def _send(self, parts: Sequence[_Admitted]) -> None:
        """Store ``parts`` in one call, then publish them in order; settles every one of them."""

        # The parts with writes, and their batches, in line order: the call stores these.
        stored: list[_Admitted] = []
        batches: list[Sequence[StorageWrite]] = []
        for part in parts:
            if part.writes is not None:
                stored.append(part)
                batches.append(part.writes)
        seqs: dict[int, Seq] = {}

        def receive(index: int, seq: Seq) -> None:
            seqs[index] = seq

        failed = self._host.failure()
        if failed is not None:
            for part in parts:
                self._unpublish(part, SessionFailed(failed))
            return
        result: CommitResult | None = None
        rejected: BaseException | None = None
        if stored:
            try:
                result = await self._host.commit(
                    batches,
                    receive,
                    without_abort_signal(stored[0].context),
                )
            except Exception as error:  # a rejection: the outcome of the call is unknown
                rejected = error
        # A call that settles once another failure failed the Session reports ``SessionFailed`` with
        # that first error, whatever it returned (spec §10.1), and publishes nothing.
        failed = self._host.failure()
        if failed is not None:
            for part in parts:
                self._unpublish(part, SessionFailed(failed))
            return
        await self._publish(parts, seqs, result, rejected)

    # ── Publication ────────────────────────────────────────────────────────

    async def _publish(
        self,
        parts: Sequence[_Admitted],
        seqs: dict[int, Seq],
        result: CommitResult | None,
        rejected: BaseException | None,
    ) -> None:
        """Publish the received parts in order, a hop apart, and settle every part (§3.4).

        A refusal, a rejection, or a part whose adoption throws fails the Session once the parts
        before it are published and the host listeners got the last one; the parts after it are not
        published, and reject with ``SessionFailed``. What the failure decides -- the outcomes and
        the gates of the parts it concerns -- settles only once the Session failed, so no job waiting
        at such a gate, and no later outcome, goes on as if it had not.
        """

        stop: tuple[BaseException, int, bool] | None = None
        # Settled once the Session failed: each part, whether it was published, and its outcome.
        failing: list[tuple[_Admitted, _FailedOutcome, bool]] = []
        stored_index = 0
        published = 0
        for part in parts:
            index: int | None = None
            if part.writes is not None:
                index = stored_index
                stored_index += 1
            if stop is not None:
                # A part after a rejected call's last receipt has the call's error; any other part
                # after a stop was not sent, nor any part once something else failed the Session.
                own_error = rejected if rejected is not None and index is not None and not stop[2] else None
                failing.append((part, _FailedOutcome(own_error), False))
                continue
            seq = None if index is None else seqs.get(index)
            if index is not None and seq is None:
                error = rejected if rejected is not None else _refusal(index, result)
                failing.append((part, _FailedOutcome(error), False))
                stop = (error, part.index, False)
                continue
            # A hop between two parts: what the first resolves runs before the second is published.
            if published > 0:
                await _hop()
            published += 1
            # Failed by something else meanwhile -- during the hop, or before a storage's scope ran
            # the publication: nothing more of the call is published. Checked right before it would
            # be, after every wait, inside the publication itself.
            done = _Publication()
            running = self._host.publishing(self._publisher(part, seq, done), part.context)
            if running is not None:
                await running
            external, failure = done.external, done.failure
            if external is not None:
                stop = (external, part.index, True)
                failing.append((part, _FailedOutcome(), False))
                continue
            if failure is not None and failure.kind == "adoption":
                failing.append((part, _FailedOutcome(failure.error), True))
                stop = (failure.error, part.index, False)
                continue
            # A part published while its call rejected is durable but rejects (spec §10.1); an own
            # observer's throw is then reported, as the Session fails with the storage error.
            if rejected is not None:
                if failure is not None:
                    self._host.report(failure.error, part.context)
                failing.append((part, _FailedOutcome(rejected), True))
                continue
            # An own observer's throw: the commit stands; the Session fails after it.
            if failure is not None:
                stop = (failure.error, part.index, False)
            failed_meanwhile = self._host.failure()
            if failed_meanwhile is not None and failure is None:
                _reject(part.outcome, SessionFailed(failed_meanwhile), part)
            else:
                _resolve(part.outcome, part)
            self._open(part)

        if rejected is not None or stop is not None:
            # After the host dispatch of the last part published, which is one yield away.
            await _yield()
            # Every part of the call that rejected keeps the storage error; the first of them is the
            # failure's boundary.
            own = [part.index for part, outcome, _ in failing if outcome.is_own]
            if rejected is not None:
                self._host.fail(rejected, own[0] if own else parts[0].index, own)
            else:
                assert stop is not None
                self._host.fail(stop[0], stop[1], own)
        stop_error = None if stop is None else stop[0]
        for part, outcome, was_published in failing:
            error = outcome.error if outcome.error is not None else self._failed(stop_error)
            if was_published:
                _reject(part.outcome, error, part)
                self._open(part)
            else:
                self._unpublish(part, error)
        # Admitted while this call ran, after the failure: never sent.
        failure_error = self._host.failure()
        if failure_error is not None:
            queued = self._queue
            self._queue = []
            for part in queued:
                self._unpublish(part, SessionFailed(failure_error))

    def _publisher(self, part: _Admitted, seq: Seq | None, done: _Publication) -> Callable[[], None]:
        """The publication of one part, bound to it.

        A storage's scope may run it after this loop moved on to the next part, so nothing of the
        part is captured by reference.
        """

        def publish_once() -> None:
            done.external = self._host.failure()
            if done.external is not None:
                return
            # Its keys leave first, so what its publication schedules sees it published; its waiters
            # wake after it.
            self._unregister(part)
            # A throw anywhere in it leaves memory behind what is stored: like an adoption's.
            try:
                if part.spec.writes is not None:
                    self._host.stored(part.spec.writes)
                done.failure = part.spec.publish(seq)
            except Exception as error:
                done.failure = PublishFailure("adoption", error)

        return publish_once

    # ── Settling ───────────────────────────────────────────────────────────

    def _abandon(self, parts: Sequence[_Admitted], error: BaseException) -> None:
        """A throw the writer did not expect while sending ``parts`` (a bug): fail the Session at the
        first of them and settle every one not settled yet, so no outcome, gate, or close waits forever."""

        self._host.fail(error, parts[0].index, [])
        for part in parts:
            if not part.done:
                self._unpublish(part, self._failed(error))
            self._unregister(part)
            self._open(part)

    def _failed(self, error: BaseException | None) -> BaseException:
        """``SessionFailed`` with the Session's failure, or with ``error`` until the Session records it."""

        failure = self._host.failure()
        return SessionFailed(failure if failure is not None else error)

    def _unpublish(self, part: _Admitted, error: BaseException) -> None:
        """Settle a part that will never be published: discard it, reject it, and open its gate."""

        part.spec.discard()
        _reject(part.outcome, error, part)
        self._unregister(part)
        self._open(part)

    def _unregister(self, part: _Admitted) -> None:
        """Drop the keys a part is still the newest writer of: published, or never will be."""

        for key in part.keys:
            if self._unpublished.get(key) is part:
                del self._unpublished[key]

    def _open(self, part: _Admitted) -> None:
        """Open a part's gate: the jobs waiting for it go on. The newest part's opens ``drained()``."""

        if not part.gate.done():
            part.gate.set_result(None)
        if self._newest is part:
            self._newest = None


def _refusal(index: int, result: CommitResult | None) -> BaseException:
    """The error a batch without a receipt carries: the refusal, or a storage that never adopted it."""

    if result is not None and index == result["stored"] and "refused" in result:
        return result["refused"]
    return RuntimeError("Storage committed without adopting the batch")


def _resolve(outcome: asyncio.Future[None], part: _Admitted) -> None:
    part.done = True
    if not outcome.done():
        outcome.set_result(None)


def _reject(outcome: asyncio.Future[None], error: BaseException, part: _Admitted) -> None:
    part.done = True
    if not outcome.done():
        outcome.set_exception(error)


def _consumed(future: asyncio.Future[None]) -> None:
    """Retrieve a settled future's exception: nobody awaits every part's outcome."""

    if not future.cancelled():
        future.exception()


async def _hop() -> None:
    """Yield to the event loop, and drain what the previous part's resolution scheduled.

    Upstream hops a *macrotask* -- ``setImmediate``, or a ``MessageChannel`` message where there is no
    ``setImmediate`` -- whose documented property is that the microtasks queued before it, "such as
    wait continuations, run first" (``writer.ts:348-358``, used at ``writer.ts:240``). One turn of an
    asyncio loop is not that property: the callbacks those continuations themselves queue would still
    be waiting when the next part published, so what a part's resolution *starts* would run after the
    publication that must follow it. So the hop yields once, then drains.
    """

    loop = asyncio.get_running_loop()
    future: asyncio.Future[None] = loop.create_future()

    def release() -> None:
        if not future.done():
            future.set_result(None)

    loop.call_soon(release)
    await future
    await _drain_scheduled(loop)


async def _drain_scheduled(loop: asyncio.AbstractEventLoop) -> None:
    """Run everything the loop still has queued, the way microtasks drain before a macrotask.

    Each round queues one callback of our own at the tail and waits for it: the loop has run every
    callback queued before it, plus whatever those queued, before it reaches ours. It stops when the
    loop has nothing left queued, so the wait is causal -- the writer is woken by the queue emptying,
    never by a number of turns.

    ``loop._ready`` is asyncio's private queue and there is no public way to ask whether the loop has
    work queued (``asyncio.sleep(0)`` yields one turn; it cannot tell an empty queue from a busy one).
    Where that attribute is absent -- an event loop that is not asyncio's own, such as uvloop -- the
    drain does nothing and the hop degrades to a single turn, which is today's behaviour: the parts
    still publish in line order, but a continuation the previous part's resolution started can be
    overtaken by the next publication. Nothing in this project depends on that degradation, because
    neither the supported backends nor CI install an alternative loop (``pyproject.toml`` has no
    runtime dependencies and the CI matrix installs only this project's extras); a port that ever runs
    on another loop must say so here.

    The hazard is inherited from upstream and named here because parity means inheriting it: a
    continuation that re-queues itself forever starves this drain, exactly as a runaway microtask chain
    starves ``setImmediate`` in Node. Upstream has no cap and neither does this -- a cap would restore
    the very turn-counting this drain replaces.
    """

    ready = getattr(loop, "_ready", None)
    if ready is None:
        return
    while ready:
        gate: asyncio.Future[None] = loop.create_future()

        def release_gate(future: asyncio.Future[None] = gate) -> None:
            if not future.done():
                future.set_result(None)

        loop.call_soon(release_gate)
        await gate


async def _yield() -> None:
    """Let the callbacks already queued run, the way one resolved promise defers a ``then``."""

    await asyncio.sleep(0)


__all__ = ["Part", "PartSpec", "PublishFailure", "Writer", "WriterHost"]

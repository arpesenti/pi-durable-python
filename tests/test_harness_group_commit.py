"""The upstream ``test/harness-group-commit.test.ts`` cases this ticket owns (v1.5.0).

Upstream `test/harness-group-commit.test.ts` declares 17 cases and executes 17. This module runs 16
of them -- :123, :159, :197, :226, :259, :281, :324, :363, :404, :446, :491, :608, :626, :699, :714
and :758 -- none skipped, none expected to fail, none stubbed. What it does not run is named below,
and the mutation that breaks each landed case's own mechanism is recorded with the commit that landed
it.

Cases this module does NOT run
------------------------------

* `:646` -- "reads no task record from Storage for more live and awaited tasks than the record cache's
  count": excluded outright, because this port has no read-side record cache and no ``CachedStorage``,
  so the component the case measures does not exist. Recorded the same way in ``PARITY.md``, next to
  its sibling ``session-group-commit.test.ts:635``.

Cases that run but are NOT evidence of the mechanism they name
--------------------------------------------------------------

* `:626` -- "marks the other tasks of a failFast wait from their current records": it stays green with
  the reconcile pass's mark plan taken out of its decision region -- `TaskScheduler._marks` calling
  `_plan_marks` directly, and `mark()` copying the record from the bare `_live` map instead of through
  `_live_record`, both stubs together -- so it is kept for the ``failFast`` coverage of upstream
  `#marks()` (`src/harness/scheduler.ts:1070`), not as proof of that region.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, cast

# --------------------------------------------------------------------------- #
# The four upstream cases #42 keeps: the line, the outcome fence, the hop, and #
# close draining through the writer. Nothing here needs a recorded region.     #
# --------------------------------------------------------------------------- #
from session_support import ControlledStorage, context, flush, observe_commits, observe_commits_as_host
from task_support import aborted, deferred, eventually, open_tasks

from pi_durable.harness.define import define_extension
from pi_durable.harness.registry import create_registry
from pi_durable.session.keys import ROOTS_KEY, owned_key
from pi_durable.session.session import SessionImpl
from pi_durable.tasks import define_task


def _holding(name: str, ran: list[str], gates: dict[str, Any]) -> Any:
    """A task whose one phase waits on a gate the test holds, counting the times it ran."""

    async def wait(_task: Any, _runtime: Any, _ctx: Any) -> None:
        key = _task["input"]["name"]
        ran.append(key)
        await gates[key].promise

    async def abort(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda input: {"phase": "hold", "input": input},
            "phases": {"hold": wait},
            "abort": abort,
        }
    )


async def _running(harness: Any, id: Any) -> bool:
    """Whether the task reads ``running``, for an ``eventually`` check."""

    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == "running"


async def _status(harness: Any, id: Any) -> str:
    record = await harness.get_task(id, context)
    assert record is not None
    return cast(str, record["state"]["status"])


async def test_starts_no_invocation_on_a_reservation_before_the_reservation_is_published() -> None:
    """Upstream ``harness-group-commit.test.ts`` :259: a scheduler commit with writes is never
    delivered before its write is published.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {"r": deferred()}
    Hold = _holding("gc.hold.r", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Hold, {"name": "r"}, {"ownership": {"kind": "conversation"}}), context
    )
    held = storage.hold_commits()
    harness.resume()
    await held.entered.wait()
    await flush()
    # The reservation's write is in flight: no invocation may start on it.
    assert ran == []
    held.release()
    await eventually(lambda: len(ran) == 1)
    assert await _status(harness, id) == "running"
    gates["r"].resolve()
    await harness.wait_for_task(id, context)
    await harness.close(context)


async def test_finds_the_submission_an_unpublished_commit_admitted_with_the_same_request_id() -> None:
    """Upstream ``harness-group-commit.test.ts`` :699: a deduplication read waits for the key the
    line admitted, not for Storage.
    """

    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    harness, root = opened["harness"], await opened["harness"].root(context)
    draft = {"type": "write", "entry": {"kind": "note", "data": {}}, "requestId": "r1"}
    held = storage.hold_commits()
    first = asyncio.ensure_future(root.submit(draft, context))
    await held.entered.wait()
    second = asyncio.ensure_future(root.submit(draft, context))
    await flush()
    held.release()
    # The second submission found the one the unpublished commit had already admitted.
    first_handle = await first
    second_handle = await second
    first_record = await first_handle.status(context)
    second_record = await second_handle.status(context)
    assert second_record["id"] == first_record["id"]
    await harness.close(context)


async def test_runs_a_wait_continuation_before_the_next_commit_is_published() -> (
    None
):
    """The upstream case at :714: the hop between two publications is what the wait continuation runs in."""

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {"one": deferred(), "two": deferred()}
    Hold = _holding("gc.hold.pair", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    other = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    one = await root.commit(
        lambda tx: tx.create_task(Hold, {"name": "one"}, {"ownership": {"kind": "conversation"}}), context
    )
    two = await other.commit(
        lambda tx: tx.create_task(Hold, {"name": "two"}, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()

    async def both_running() -> bool:
        return await _status(harness, one) == "running" and await _status(harness, two) == "running"

    await eventually(both_running)
    ended: set[Any] = set()

    def follow(publication: Any, _ctx: Any) -> None:
        for change in publication["changes"]:
            if change["type"] == "task" and change["value"]["state"]["status"] == "terminal":
                ended.add(change["value"]["id"])

    # The Harness has no publication-observation surface of its own yet (upstream's followCommits is
    # the Session's), so the test subscribes on the Session the task scheduler drives.
    # Upstream's `followCommits(harness, ...)` is the host's own commit observer.
    observe_commits_as_host(harness, follow)
    # Both end in one call, behind a commit in flight.
    held = storage.hold_commits()
    blocker = asyncio.ensure_future(
        root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    )
    await held.entered.wait()
    seen: list[bool] = []

    async def waited() -> None:
        await harness.wait_for_task(one, context)
        seen.append(two in ended)

    waiting = asyncio.ensure_future(waited())
    gates["one"].resolve()
    await flush()
    gates["two"].resolve()
    await flush()
    held.release()
    await blocker
    await waiting
    await harness.wait_for_task(two, context)
    # `one`'s continuation ran in the hop, before `two` was published.
    assert seen == [False]
    await harness.close(context)


async def test_signals_an_invocation_that_closes_during_a_call_in_flight() -> (
    None
):
    """The upstream case at :758: close drains what the call admitted before the backend closes."""

    storage = ControlledStorage()
    signalled = deferred()

    async def listen(_task: Any, runtime: Any, _ctx: Any) -> None:
        try:
            await aborted(runtime.signal)
        except BaseException:
            signalled.resolve()

    Listens = define_task(
        {
            "name": "gc.listens",
            "version": 1,
            "initial": lambda _input: {"phase": "wait"},
            "phases": {"wait": listen},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    opened = await open_tasks(storage, [Listens])
    harness, root = opened["harness"], await opened["harness"].root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Listens, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()

    async def running() -> bool:
        return await _status(harness, id) == "running"

    await eventually(running)
    held = storage.hold_commits()
    entry = asyncio.ensure_future(root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context))
    await held.entered.wait()
    closing = asyncio.ensure_future(harness.close(context))
    # The invocation is signalled while its commit is still in storage, and close waits for that call.
    await signalled.promise
    assert not await _settled(closing)
    held.release()
    await entry
    await closing
    reopened = await open_tasks(storage.reopen(), [Listens])
    conversation = await reopened["harness"].conversation(root.id, context)
    kinds = [item["kind"] for item in (await conversation.entries({}, 10, None, context))["items"]]
    assert "note" in kinds
    # Close wrote the task it stopped back as pending.
    assert await _status(reopened["harness"], id) == "pending"
    await reopened["harness"].close(context)


async def _settled(promise: Any) -> bool:
    task = asyncio.ensure_future(promise)
    await flush()
    return task.done()


def _published_tasks(harness: Any) -> tuple[set[Any], Callable[[], None]]:
    """The task IDs of every stored publication, as upstream's ``followCommits`` collects them."""

    published: set[Any] = set()

    def follow(publication: Any, _context: Any) -> None:
        for change in publication["changes"]:
            if change["type"] == "task":
                published.add(change["value"]["id"])

    return published, observe_commits(harness, follow)


async def test_makes_a_reader_of_the_roots_wait_for_a_creation_under_a_root_a_pending_end_unlists() -> None:
    """Upstream ``harness-group-commit.test.ts`` :363: the end of T's last live task writes ``roots``,
    so a reader of the roots on the line waits for the creation admitted behind it.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {"t": deferred(), "d": deferred()}
    Hold = _hold("gc.hold.roots", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()
    # Root R is listed for its last live task, T.
    last = await root.commit(
        lambda tx: tx.create_task(Hold, {"name": "t"}, {"ownership": {"kind": "conversation"}}), context
    )
    await eventually(lambda: _running(harness, last))
    published, _follow = _published_tasks(harness)
    # T's end is in flight; a creation under R is admitted after it, while R still shows listed.
    held = storage.hold_commits()
    gates["t"].resolve()
    await held.entered.wait()
    created: list[Any] = []
    declared: list[str] = []

    async def create(tx: Any) -> Any:
        id = await tx.create_task(Hold, {"name": "d"}, {"ownership": {"kind": "conversation"}})
        created.append(id)
        declared.extend(await cast(Awaitable[list[str]], harness.membership_keys(tx)))
        return id

    creating = asyncio.ensure_future(root.commit(create, context))
    await flush()

    # A reader of the roots on the line (global idle's key) waits until the creation is published too.
    async def read(_line: Any) -> bool:
        gate = harness.touch(ROOTS_KEY)
        if gate is not None:
            await gate
        return len(created) == 1 and created[0] in published

    reading = asyncio.ensure_future(harness.read_on_line(read))
    held.release()
    await creating
    assert await reading is True
    # The commit declared the roots it walks to: that write is what the reader above waited behind.
    assert ROOTS_KEY in declared
    gates["d"].resolve()
    await harness.wait_for_idle(context)
    await harness.close(context)


async def test_walks_a_creation_s_memberships_past_an_ancestor_whose_pending_end_is_unpublished() -> None:
    """Upstream ``harness-group-commit.test.ts`` :404: a pending end must not stop the membership walk
    early, so the creation's commit writes the nodes above the ancestor that still shows live.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {"p": deferred(), "d": deferred()}
    Hold = _hold("gc.hold.ancestor", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()

    async def tree(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Hold, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": p}})
        return {"p": p, "c": c["id"]}

    # Live P, in the root conversation, owns the empty conversation C.
    built = await root.commit(tree, context)
    await eventually(lambda: _running(harness, built["p"]))
    published, _follow = _published_tasks(harness)
    conversation = await harness.conversation(built["c"], context)
    assert conversation is not None
    # P's end is in flight; a creation in C is admitted after it. Its chain must not stop at P.
    held = storage.hold_commits()
    gates["p"].resolve()
    await held.entered.wait()
    created: list[Any] = []
    declared: list[str] = []

    async def create(tx: Any) -> Any:
        id = await tx.create_task(Hold, {"name": "d"}, {"ownership": {"kind": "conversation"}})
        created.append(id)
        declared.extend(await cast(Awaitable[list[str]], harness.membership_keys(tx)))
        return id

    creating = asyncio.ensure_future(conversation.commit(create, context))
    await flush()

    async def read(_line: Any) -> bool:
        gate = harness.touch(ROOTS_KEY)
        if gate is not None:
            await gate
        return len(created) == 1 and created[0] in published

    reading = asyncio.ensure_future(harness.read_on_line(read))
    held.release()
    await creating
    assert await reading is True
    # The commit declared the roots it walks to: that write is what the reader above waited behind.
    assert ROOTS_KEY in declared
    gates["d"].resolve()
    await harness.wait_for_idle(context)
    await harness.close(context)


async def test_reserves_the_live_work_of_the_owner_above_an_ancestor_whose_end_is_pending() -> None:
    """Upstream ``harness-group-commit.test.ts`` :446: the owner above the ending ancestor is a member
    of the creation's commit, so a reader of that owner's live work waits for the creation.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {"g": deferred(), "p": deferred(), "d": deferred()}
    Hold = _hold("gc.hold.owner", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()

    async def tree(tx: Any) -> dict[str, Any]:
        g = await tx.create_task(Hold, {"name": "g"}, {"ownership": {"kind": "conversation"}})
        p = await tx.create_task(Hold, {"name": "p"}, {"ownership": {"kind": "task", "taskId": g}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": p}})
        return {"g": g, "p": p, "c": c["id"]}

    # Live G owns P, which owns the empty conversation C.
    built = await root.commit(tree, context)
    await eventually(lambda: _running(harness, built["p"]))
    published, _follow = _published_tasks(harness)
    conversation = await harness.conversation(built["c"], context)
    assert conversation is not None
    # P's end is in flight; a creation in C is admitted after it, while P still shows live.
    held = storage.hold_commits()
    gates["p"].resolve()
    await held.entered.wait()
    created: list[Any] = []
    declared: list[str] = []

    async def create(tx: Any) -> Any:
        id = await tx.create_task(Hold, {"name": "d"}, {"ownership": {"kind": "conversation"}})
        created.append(id)
        declared.extend(await cast(Awaitable[list[str]], harness.membership_keys(tx)))
        return id

    creating = asyncio.ensure_future(conversation.commit(create, context))
    await flush()

    # A reader of G's live work (as G's end decision is) waits until the creation is published too.
    async def read(_line: Any) -> bool:
        gate = harness.touch(owned_key(built["g"]))
        if gate is not None:
            await gate
        return len(created) == 1 and created[0] in published

    reading = asyncio.ensure_future(harness.read_on_line(read))
    held.release()
    await creating
    assert await reading is True
    # The walk went past the ancestor whose end is pending, up to G's node: that write is what the
    # reader above waited behind.
    assert owned_key(built["g"]) in declared
    gates["d"].resolve()
    gates["g"].resolve()
    await harness.wait_for_task(built["g"], context)
    await harness.close(context)


async def _race(gate: Any, signal: Any) -> None:
    """Upstream's ``Promise.race([gate.promise, aborted(signal)])``."""

    waiting = asyncio.ensure_future(gate.promise)
    stopping = asyncio.ensure_future(aborted(signal))
    try:
        await asyncio.wait({waiting, stopping}, return_when=asyncio.FIRST_COMPLETED)
        if waiting.done() and not waiting.cancelled():
            waiting.result()
        elif stopping.done() and not stopping.cancelled():
            stopping.result()
    finally:
        waiting.cancel()
        stopping.cancel()
        for pending in (waiting, stopping):
            if pending.done() and not pending.cancelled():
                pending.exception()


def _hold(name: str, ran: list[str], gates: dict[str, Any]) -> Any:
    """Upstream ``Hold``: holds until its named gate opens or it is aborted, then completes; its
    abort handler ends it ``aborted``."""

    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        key = task["input"]["name"]
        ran.append(key)
        await _race(gates[key], runtime.signal)
        outcome = {"status": "terminal", "outcome": {"status": "completed", "result": None}}
        await runtime.commit(lambda: outcome, ctx)

    async def abort(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda input: {"phase": "hold", "input": input},
            "phases": {"hold": wait},
            "abort": abort,
        }
    )


async def _status_is(harness: Any, id: Any, status: str) -> bool:
    """Whether the task reads ``status``, for an ``eventually`` check."""

    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == status


async def _running_phase_is(harness: Any, id: Any, phase: str) -> bool:
    """Whether the task is ``running`` at ``phase``, for an ``eventually`` check."""

    record = await harness.get_task(id, context)
    return (
        record is not None
        and record["state"]["status"] == "running"
        and record["state"]["checkpoint"]["phase"] == phase
    )


def _two_phases(name: str, ran: list[str], gates: dict[str, Any]) -> Any:
    """Upstream ``TwoPhases``: commits its next phase, returns once ``<name>.return`` opens; the
    next phase counts its run and completes."""

    async def one(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "two"}}, ctx)
        await gates[f"{task['input']['name']}.return"].promise

    async def two(task: Any, runtime: Any, ctx: Any) -> None:
        ran.append(f"{task['input']['name']}.two")
        outcome = {"status": "terminal", "outcome": {"status": "completed", "result": None}}
        await runtime.commit(lambda: outcome, ctx)

    async def abort(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda input: {"phase": "one", "input": input},
            "phases": {"one": one, "two": two},
            "abort": abort,
        }
    )


def _conversation_ownership() -> dict[str, Any]:
    return {"ownership": {"kind": "conversation"}}


def _task_ownership(task: Any) -> dict[str, Any]:
    return {"ownership": {"kind": "task", "taskId": task}}


async def test_holds_a_live_owner_whose_end_races_a_creation_below_its_ended_tasks() -> None:
    """Upstream ``harness-group-commit.test.ts`` :123: live P, ended B and A, empty C.

    P's end is decided in a region that reads what the creation of D in C writes, so it waits for
    that publication; in line order D's creation precedes P's decision, and P holds while D is live.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {name: deferred() for name in ("p", "b", "a", "c", "d")}
    Hold = _hold("gc.hold.race", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()

    async def tree(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Hold, {"name": "p"}, _conversation_ownership())
        b = await tx.create_task(Hold, {"name": "b"}, _task_ownership(p))
        a = await tx.create_task(Hold, {"name": "a"}, _task_ownership(b))
        c = await tx.create_conversation(_task_ownership(a))
        return {"p": p, "b": b, "a": a, "c": c["id"]}

    built = await root.commit(tree, context)
    gates["a"].resolve()
    await harness.wait_for_task(built["a"], context)
    gates["b"].resolve()
    await harness.wait_for_task(built["b"], context)
    # The creation of D in C is admitted and in flight when P commits its end.
    held = storage.hold_commits()
    c = await harness.conversation(built["c"], context)
    assert c is not None
    creating = asyncio.ensure_future(
        c.commit(lambda tx: tx.create_task(Hold, {"name": "d"}, _conversation_ownership()), context)
    )
    await held.entered.wait()
    # P's decision reads what the creation writes, so it waits for its publication, holding the line.
    gates["p"].resolve()
    await flush()
    held.release()
    d = await creating
    await eventually(lambda: _status_is(harness, built["p"], "completing"))
    await eventually(lambda: _status_is(harness, d, "running"))
    gates["d"].resolve()
    settled = await harness.wait_for_task(built["p"], context)
    assert settled["state"]["outcome"]["status"] == "completed"
    await harness.close(context)


async def test_holds_an_owner_whose_end_follows_a_creation_below_its_child_pending_end() -> None:
    """Upstream ``harness-group-commit.test.ts`` :159: live P, ending B, conversation C.

    D's creation is admitted before P's end in line order, so P's decision holds while D is live: the
    memberships D wrote reach the walk P's decision takes.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {name: deferred() for name in ("p", "b", "d")}
    Hold = _hold("gc.hold.after", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()

    async def tree(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Hold, {"name": "p"}, _conversation_ownership())
        b = await tx.create_task(Hold, {"name": "b"}, _task_ownership(p))
        c = await tx.create_conversation(_task_ownership(b))
        return {"p": p, "b": b, "c": c["id"]}

    built = await root.commit(tree, context)
    c = await harness.conversation(built["c"], context)
    assert c is not None
    await eventually(lambda: _status_is(harness, built["b"], "running"))
    # A call in flight, so what follows is admitted and stays unpublished, in this order.
    held = storage.hold_commits()
    def note(tx: Any) -> Any:
        return tx.append_entry(root.id, {"kind": "note"})

    blocker = asyncio.ensure_future(root.commit(note, context))
    await held.entered.wait()
    # B ends; then D is created in C while B still shows live; then P ends.
    gates["b"].resolve()
    await flush()
    creating = asyncio.ensure_future(
        c.commit(lambda tx: tx.create_task(Hold, {"name": "d"}, _conversation_ownership()), context)
    )
    await flush()
    gates["p"].resolve()
    await flush()
    held.release()
    await blocker
    d = await creating
    # In line order D's creation precedes P's decision: P holds while D is live.
    await eventually(lambda: _status_is(harness, built["p"], "completing"))
    await eventually(lambda: _status_is(harness, d, "running"))
    gates["d"].resolve()
    settled = await harness.wait_for_task(built["p"], context)
    assert settled["state"]["outcome"]["status"] == "completed"
    await harness.close(context)


async def test_ends_a_child_before_its_next_phase_when_its_owner_s_abort_mark_is_unpublished() -> None:
    """Upstream ``harness-group-commit.test.ts`` :197: the child's step reads the intent set, which
    the owner's still-unpublished abort mark changes, so it ends before phase two ever runs.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {"o": deferred(), "t.return": deferred()}
    Hold = _hold("gc.hold.mark", ran, gates)
    TwoPhases = _two_phases("gc.two.phases.mark", ran, gates)
    opened = await open_tasks(storage, [Hold, TwoPhases])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()

    async def tree(tx: Any) -> dict[str, Any]:
        owner = await tx.create_task(Hold, {"name": "o"}, _conversation_ownership())
        child = await tx.create_task(TwoPhases, {"name": "t"}, _task_ownership(owner))
        return {"owner": owner, "child": child}

    built = await root.commit(tree, context)
    await eventually(lambda: _running_phase_is(harness, built["child"], "two"))
    # The owner's abort mark is admitted and stays unpublished while the child's phase returns.
    held = storage.hold_commits()
    marking = asyncio.ensure_future(harness.abort_task(built["owner"], context))
    await held.entered.wait()
    # The child's phase returns: its step reads the intent set, which the unpublished mark changes.
    gates["t.return"].resolve()
    await flush()
    assert ran.count("t.two") == 0
    held.release()
    assert await marking == "marked"
    child = await harness.wait_for_task(built["child"], context)
    assert child["state"]["outcome"]["status"] == "aborted"
    owner = await harness.wait_for_task(built["owner"], context)
    assert owner["state"]["outcome"]["status"] == "aborted"
    assert ran.count("t.two") == 0
    await harness.close(context)


async def _both_running(harness: Any, x: Any, y: Any) -> bool:
    """Whether both tasks read ``running``, for an ``eventually`` check."""

    return await _status_is(harness, x, "running") and await _status_is(harness, y, "running")


def _waits(name: str, gates: dict[str, Any], refusals: dict[str, BaseException]) -> Any:
    """Upstream ``Waits``: once its gate opens, waits on the task its memo names; records why a
    refused wait was refused, then completes."""

    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        await gates[task["input"]["name"]].promise
        on = await runtime.memo("on", ctx)
        try:
            await runtime.commit(
                lambda: {
                    "status": "waiting",
                    "checkpoint": {"phase": "done"},
                    "on": [on],
                    "policy": "allSettled",
                },
                ctx,
            )
        except BaseException as error:
            refusals[task["input"]["name"]] = error
            outcome = {"status": "terminal", "outcome": {"status": "completed", "result": None}}
            await runtime.commit(lambda: outcome, ctx)

    async def done(_task: Any, runtime: Any, ctx: Any) -> None:
        outcome = {"status": "terminal", "outcome": {"status": "completed", "result": None}}
        await runtime.commit(lambda: outcome, ctx)

    async def abort(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda input: {"phase": "wait", "input": input},
            "phases": {"wait": wait, "done": done},
            "abort": abort,
        }
    )


async def test_refuses_the_second_of_two_opposite_waits_admitted_behind_each_other() -> None:
    """Upstream ``harness-group-commit.test.ts`` :226: the wait-cycle check is a decision region, so
    the second of two opposite waits admitted behind each other sees the first and is refused.
    """

    storage = ControlledStorage()
    gates: dict[str, Any] = {"x": deferred(), "y": deferred()}
    refusals: dict[str, BaseException] = {}
    Waits = _waits("gc.waits.opposite", gates, refusals)
    opened = await open_tasks(storage, [Waits])
    harness, root = opened["harness"], await opened["harness"].root(context)

    async def pair(tx: Any) -> dict[str, Any]:
        x = await tx.create_task(Waits, {"name": "x"}, _conversation_ownership())
        y = await tx.create_task(Waits, {"name": "y"}, _conversation_ownership())
        return {"x": x, "y": y}

    built = await root.commit(pair, context)
    # Each waits on the other, named in a memo.
    async def note(tx: Any) -> None:
        records = [await tx.task(built["x"]), await tx.task(built["y"])]
        tx.set_task({**records[0], "memos": {"on": built["y"]}})
        tx.set_task({**records[1], "memos": {"on": built["x"]}})

    await root.commit(note, context)
    harness.resume()
    await eventually(lambda: _both_running(harness, built["x"], built["y"]))
    # X's wait is admitted and unpublished; then Y's, which must see it and be refused.
    held = storage.hold_commits()
    gates["x"].resolve()
    await held.entered.wait()
    gates["y"].resolve()
    # Both wait commits are on the line and neither is published: Y's is parked at the region's gate
    # on X's write. The Session's own bound (group-commit document §3.4) is what says so.
    session = cast(SessionImpl, harness)
    await eventually(lambda: session.line_sizes == {"outstanding": 2, "unpublished": 1})
    held.release()
    await harness.wait_for_task(built["y"], context)
    assert "wait cycle" in str(refusals.get("y"))
    assert "x" not in refusals
    x = await harness.wait_for_task(built["x"], context)
    assert x["state"]["outcome"]["status"] == "completed"
    await harness.close(context)


async def _until_aborted(signal: Any) -> None:
    """Upstream's ``untilAborted``: settle once the invocation is signalled."""

    if signal is None:
        return
    event = asyncio.Event()
    signal.add_listener(event.set)
    if signal.aborted:
        return
    await event.wait()


async def _gate_fails(gate: Any) -> str:
    await gate.promise
    return "fail"


async def _race_failed(gate: Any, signal: Any) -> str | None:
    """Upstream's ``Promise.race([gate.promise.then(() => "fail"), untilAborted(signal)])``."""

    failing = asyncio.ensure_future(_gate_fails(gate))
    stopping = asyncio.ensure_future(_until_aborted(signal))
    try:
        done, _pending = await asyncio.wait(
            {failing, stopping}, return_when=asyncio.FIRST_COMPLETED
        )
        for finished in done:
            if finished is failing and not failing.cancelled():
                return failing.result()
            if finished is stopping and not stopping.cancelled():
                stopping.result()
                return None
        return None
    finally:
        failing.cancel()
        stopping.cancel()
        for pending in (failing, stopping):
            if pending.done() and not pending.cancelled():
                pending.exception()


def _gate(gates: dict[str, Any], name: str) -> Any:
    """Upstream's ``gate(name)``: the named gate, created once on first use."""

    found = gates.get(name)
    if found is None:
        found = deferred()
        gates[name] = found
    return found


def _probe(name: str, runtimes: dict[str, Any], gates: dict[str, Any]) -> Any:
    """Upstream ``Probe``: holds until aborted, its runtime exposed; fails once its gate opens. Its
    abort handler holds too."""

    async def hold(task: Any, runtime: Any, ctx: Any) -> None:
        key = task["input"]["name"]
        runtimes[key] = runtime
        if await _race_failed(_gate(gates, key), runtime.signal) != "fail":
            return
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "failed", "error": {"message": "gate"}}}, ctx
        )

    async def abort(_task: Any, runtime: Any, _ctx: Any) -> None:
        await _until_aborted(runtime.signal)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda input: {"phase": "hold", "n": 0},
            "phases": {"hold": hold},
            "abort": abort,
        }
    )


def _waiter(name: str, probe: Any) -> Any:
    """Upstream ``Waiter``: creates the probes ``x`` and ``y`` and waits on them ``failFast``."""

    async def spawn(task: Any, runtime: Any, ctx: Any) -> Any:
        async def build(tx: Any) -> dict[str, Any]:
            on: list[Any] = []
            for member in ("x", "y"):
                on.append(await tx.create_task(probe, {"name": member}, _task_ownership(task["id"])))
            return {"status": "waiting", "checkpoint": {"phase": "done"}, "on": on, "policy": "failFast"}

        return await runtime.commit(build, ctx)

    async def done(_task: Any, runtime: Any, ctx: Any) -> None:
        outcome = {"status": "terminal", "outcome": {"status": "completed", "result": None}}
        await runtime.commit(lambda: outcome, ctx)

    async def abort(_task: Any, runtime: Any, _ctx: Any) -> None:
        await _until_aborted(runtime.signal)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda input: {"phase": "spawn"},
            "phases": {"spawn": spawn, "done": done},
            "abort": abort,
        }
    )


async def _race_mark(
    storage: ControlledStorage,
    harness: Any,
    name: str,
    runtimes: dict[str, Any],
    mark: Callable[[], Awaitable[Any]],
) -> None:
    """Upstream ``raceMark``: advance probe ``name`` to ``n = 1`` while the mark-writing commit is in
    flight, and run a reconcile pass meanwhile."""

    held_mark = storage.hold_commits()
    marking = asyncio.ensure_future(mark())
    await held_mark.entered.wait()
    prepared: list[bool] = []

    def advance() -> dict[str, Any]:
        prepared.append(True)
        return {"status": "running", "checkpoint": {"phase": "hold", "n": 1}}

    advancing = runtimes[name].commit(advance, context)
    await eventually(lambda: bool(prepared))
    await flush()
    # Global idle schedules a reconcile pass, which reaches the line while both are unpublished.
    waiting = asyncio.ensure_future(harness.wait_for_idle(context))
    waiting.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    await flush()
    held_advance = storage.hold_commits()
    held_mark.release()
    await held_advance.entered.wait()
    await flush()
    held_advance.release()
    await asyncio.gather(marking, advancing)
    await flush()


async def _abort_requested(harness: Any, id: Any) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record.get("abortRequested") is True


async def test_marks_a_child_in_a_cascade_from_its_current_record() -> None:
    """Upstream ``harness-group-commit.test.ts`` :608: the cascade marks the child from the record it
    has now -- the ``n = 1`` checkpoint its own admitted commit writes -- not the one it replaces.
    """

    storage = ControlledStorage()
    runtimes: dict[str, Any] = {}
    gates: dict[str, Any] = {}
    Probe = _probe("gc.probe.cascade", runtimes, gates)
    opened = await open_tasks(storage, [Probe])
    harness, root = opened["harness"], await opened["harness"].root(context)

    async def tree(tx: Any) -> dict[str, Any]:
        parent = await tx.create_task(Probe, {"name": "p"}, _conversation_ownership())
        child = await tx.create_task(Probe, {"name": "c"}, _task_ownership(parent))
        return {"parent": parent, "child": child}

    built = await root.commit(tree, context)
    harness.resume()
    await eventually(lambda: "c" in runtimes)
    await flush()
    await _race_mark(storage, harness, "c", runtimes, lambda: harness.abort_task(built["parent"], context))
    await eventually(lambda: _abort_requested(harness, built["child"]))
    record = await harness.get_task(built["child"], context)
    assert record is not None
    assert record["state"]["checkpoint"] == {"phase": "hold", "n": 1}
    await harness.close(context)


async def test_marks_the_other_tasks_of_a_fail_fast_wait_from_their_current_records() -> None:
    """Upstream ``harness-group-commit.test.ts`` :626: the ``failFast`` pass marks the waiter's other
    tasks from their current records, not the ones their pending commits replace.
    """

    storage = ControlledStorage()
    runtimes: dict[str, Any] = {}
    gates: dict[str, Any] = {}
    Probe = _probe("gc.probe.fail-fast", runtimes, gates)
    Waiter = _waiter("gc.fail-fast.waiter", Probe)
    opened = await open_tasks(storage, [Probe, Waiter])
    harness, root = opened["harness"], await opened["harness"].root(context)
    await root.commit(
        lambda tx: tx.create_task(Waiter, None, _conversation_ownership()), context
    )
    harness.resume()
    await eventually(lambda: "x" in runtimes and "y" in runtimes)
    await flush()
    y = runtimes["y"].task_id

    async def fail_x() -> None:
        _gate(gates, "x").resolve()

    await _race_mark(storage, harness, "y", runtimes, fail_x)
    await eventually(lambda: _abort_requested(harness, y))
    record = await harness.get_task(y, context)
    assert record is not None
    assert record["state"]["checkpoint"] == {"phase": "hold", "n": 1}
    await harness.close(context)


def _migrating(name: str, migrations: list[int], failure: str | None) -> Any:
    """Upstream ``V2``: a version 2 definition whose ``migrate`` counts its runs, and throws when the
    case wants the failed report."""

    async def hold(_task: Any, runtime: Any, ctx: Any) -> Any:
        outcome = {"status": "terminal", "outcome": {"status": "completed", "result": None}}
        return await runtime.commit(lambda: outcome, ctx)

    def migrate(input: Any, checkpoint: Any, _version: int) -> dict[str, Any]:
        migrations.append(1)
        if failure is not None:
            raise RuntimeError(failure)
        return {"input": input, "checkpoint": checkpoint}

    return define_task(
        {
            "name": name,
            "version": 2,
            "initial": lambda input: {"phase": "hold"},
            "phases": {"hold": hold},
            "abort": lambda _task, _runtime, _ctx: None,
            "migrate": migrate,
        }
    )


async def test_runs_a_migration_once_although_its_reservation_waits_for_an_unpublished_write() -> None:
    """Upstream ``harness-group-commit.test.ts`` :281: the reservation's region reruns, but the task
    is migrated once, because the migration is applied only once the decision stands.
    """

    storage = ControlledStorage()
    migrations: list[int] = []
    V2 = _migrating("gc.migrated", migrations, None)
    registry = create_registry()
    registry.install(define_extension({"name": "v2", "tasks": [V2]}))
    opened = await open_tasks(storage, [], {"registry": registry})
    harness, root = opened["harness"], await opened["harness"].root(context)
    id = await root.commit(
        lambda tx: tx.create_task(V2, {"name": "m"}, _conversation_ownership()), context
    )
    # Stored at version 1, and written again by a commit that is in flight when the reservation reads it.
    held = storage.hold_commits()

    async def write(tx: Any) -> None:
        record = await tx.task(id)
        tx.set_task({**record, "version": 1, "memos": {"touched": True}})

    writing = asyncio.ensure_future(root.commit(write, context))
    await held.entered.wait()
    harness.resume()
    await flush()
    assert migrations == []
    held.release()
    await writing
    settled = await harness.wait_for_task(id, context)
    assert settled["state"]["outcome"]["status"] == "completed"
    assert len(migrations) == 1
    await harness.close(context)


async def test_reports_a_failed_migration_once_although_its_reservation_waits_for_an_unpublished_write() -> (
    None
):
    """Upstream ``harness-group-commit.test.ts`` :324: a ``migrate`` that throws is reported once, even
    though the reservation's region reruns.
    """

    storage = ControlledStorage()
    migrations: list[int] = []
    V2 = _migrating("gc.unmigratable", migrations, "cannot migrate")
    registry = create_registry()
    registry.install(define_extension({"name": "v2", "tasks": [V2]}))
    opened = await open_tasks(storage, [], {"registry": registry})
    harness, root = opened["harness"], await opened["harness"].root(context)
    infos = opened["infos"]
    id = await root.commit(
        lambda tx: tx.create_task(V2, {"name": "m"}, _conversation_ownership()), context
    )
    held = storage.hold_commits()

    async def write(tx: Any) -> None:
        record = await tx.task(id)
        tx.set_task({**record, "version": 1})

    writing = asyncio.ensure_future(root.commit(write, context))
    await held.entered.wait()
    harness.resume()
    await flush()
    held.release()
    await writing
    await eventually(lambda: len(migrations) > 0)
    await flush()
    assert len(migrations) == 1
    assert len([info for info in infos if info["kind"] == "migration_failed"]) == 1
    await harness.close(context)


async def test_keeps_the_nodes_a_chain_load_installed_for_an_unpublished_creation_through_the_sweep() -> None:
    """Upstream ``harness-group-commit.test.ts`` :491: a chain load installs a node for the job on the
    line, and the sweep keeps it -- pinned -- until that job's commit is published or fails.
    """

    storage = ControlledStorage()
    ran: list[str] = []
    gates: dict[str, Any] = {}
    Hold = _hold("gc.hold.pin", ran, gates)
    opened = await open_tasks(storage, [Hold])
    harness, root = opened["harness"], await opened["harness"].root(context)
    harness.resume()

    async def tree(tx: Any) -> dict[str, Any]:
        b = await tx.create_task(Hold, {"name": "b"}, _conversation_ownership())
        c = await tx.create_conversation(_task_ownership(b))
        return {"b": b, "c": c["id"]}

    built = await root.commit(tree, context)
    _gate(gates, "b").resolve()
    await harness.wait_for_task(built["b"], context)
    await harness.close(context)
    # After reopen, B and C's edge are known only once a chain load reads them.
    opened = await open_tasks(storage.reopen(), [Hold])
    harness = opened["harness"]
    def sizes() -> dict[str, int]:
        return harness.scheduler_index_sizes
    assert sizes()["settled"] == 0
    conversation = await harness.conversation(built["c"], context)
    assert conversation is not None
    held = storage.hold_commits()
    creating = asyncio.ensure_future(
        conversation.commit(
            lambda tx: tx.create_task(Hold, {"name": "d"}, _conversation_ownership()), context
        )
    )
    await held.entered.wait()
    # The admission loaded B; the sweep that load scheduled has run on the line since, and kept it.
    await harness.wait_for_idle(context)
    await flush()
    assert sizes()["settled"] == 1
    assert sizes()["pins"] > 0
    held.release()
    await creating
    # Published below a known chain: nothing is left to load, and no pin is left.
    assert sizes()["unloaded"] == 0
    assert sizes()["pins"] == 0
    assert await _settled(harness.wait_for_idle(context)) is False
    harness.resume()
    _gate(gates, "d").resolve()
    await harness.wait_for_idle(context)
    await harness.close(context)

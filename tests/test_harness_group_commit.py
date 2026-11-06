"""The upstream ``test/harness-group-commit.test.ts`` cases this ticket owns (v1.5.0).

The four kept cases are written and pass. The module also carries the deferral record below, so what
the port does not translate is named in the tree rather than only in a commit body, which does not
survive cleaning up the worktrees.

Cases this module does NOT run, named so the record survives the worktrees
------------------------------------------------------------------------

Upstream `test/harness-group-commit.test.ts` declares 17 cases and executes 17. This module translates
9 of them -- :123, :159, :259, :363, :404, :446, :699, :714 and :758 -- and names the rest rather than
stubbing them.

Five are #48's group-commit cases and run here: `:259`, `:699`, `:714`, `:758` (the line, its keys and
its publications) and, since the scheduler's decision region and its `membershipKeys()` landed, `:123`,
`:159`, `:363`, `:404` and `:446`. Four of those are mutation-checked: with `Harness.membership_keys()`
returning no keys, `:123`, `:363`, `:404` and `:446` fail, so none of them passes without the commit
declaring the memberships its task changes alter. `:159` settles either way -- its ordering is decided
by the line, not by a membership key -- and is kept for that ordering, not as evidence of the keys.

Seven remain #48's, named with what each still needs:

* `:197` -- "ends a child before its next phase when its owner's abort mark is unpublished at its step"
  and `:226` -- "refuses the second of two opposite waits admitted behind each other": both need a
  second gate inside a *runtime* commit's region (upstream `commitWith`, `src/harness/scheduler.ts:2939`),
  which this port gates only for the current record, not for the wait graph a commit installs.
* `:281` -- "runs a migration once although its reservation waits for an unpublished write of the task"
  and `:324` -- "reports a failed migration once although its reservation waits for an unpublished
  write of the task": both need the reservation's migration decision to be decided once and applied
  after the region stands; this port resolves and reports inside the pass, so a region rerun would
  migrate or report twice.
* `:491` -- "keeps the nodes a chain load installed for an unpublished creation through the sweep":
  needs upstream's `#pin()` (`src/harness/scheduler.ts:1412`), which holds a node a chain load
  installed until the job that loaded it publishes, and the `pins` size it reports. The port has
  neither, so the sweep of a job whose commit is still unpublished can drop what it just loaded.
* `:608` -- "marks a child in a cascade from its current record, not one its pending commit replaces"
  and `:626` -- "marks the other tasks of a failFast wait from their current records": both need the
  reconcile pass's mark plan in its own region (`#marks()`, `src/harness/scheduler.ts:1074`); this port
  reads the records it marks inside the commit's apply, so a mark can still be built from a record an
  admitted commit replaced.

Excluded outright, with its reason -- no read-side record cache, no `CachedStorage` in this port, so the
component the case measures does not exist (recorded the same way in `PARITY.md`, next to its sibling
`session-group-commit.test.ts:635`):

* `:646` -- "reads no task record from Storage for more live and awaited tasks than the record cache's count"
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

from pi_durable.session.keys import ROOTS_KEY, owned_key
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


def _hold(name: str, ran: list[str], gates: dict[str, Any]) -> Any:
    """Upstream ``Hold``: holds until its named gate opens, then completes; its abort ends ``aborted``."""

    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        key = task["input"]["name"]
        ran.append(key)
        await gates[key].promise
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

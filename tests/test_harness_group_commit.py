"""The upstream ``test/harness-group-commit.test.ts`` cases this ticket owns (v1.5.0).

The four kept cases are written and pass. The module also carries the deferral record below, so what
the port does not translate is named in the tree rather than only in a commit body, which does not
survive cleaning up the worktrees.

Cases this module does NOT run, named so the record survives the worktrees
------------------------------------------------------------------------

Upstream `test/harness-group-commit.test.ts` declares 17 cases and executes 17. This module translates
4 of them -- :259, :699, :714 and :758, the cases that need only the mutation line, its keys and its
publications -- and names the other 13 rather than stubbing them.

Twelve are #48's. Upstream gates a scheduler decision behind what the line admitted by recording the
keys the decision's region read (`docs/upstream/group-commit-1.5.0.md` section 5.4): the records a
region reads -- the decided task, the wait members and their `on`, a cycle walk, the ownership tree --
are keyed, and the region is what makes the reread wait. This port has no recorded region yet, so a
decision rereads its task record ungated, and `owned:<node>` / `roots` / `intent` / `membershipKeys()`
are #48's vocabulary (the last two also #49's), which this ticket does not half-implement:

* `:123` -- "holds a live owner whose end races a creation below its ended tasks: live P, ended B
  and A, empty C"
* `:159` -- "holds an owner whose end follows a creation below its child's pending end: live P,
  ending B, conversation C"
* `:197` -- "ends a child before its next phase when its owner's abort mark is unpublished at its step"
* `:226` -- "refuses the second of two opposite waits admitted behind each other"
* `:281` -- "runs a migration once although its reservation waits for an unpublished write of the task"
* `:324` -- "reports a failed migration once although its reservation waits for an unpublished
  write of the task"
* `:363` -- "makes a reader of the roots wait for a creation under a root a pending end would unlist"
* `:404` -- "walks a creation's memberships past an ancestor whose pending end is unpublished"
* `:446` -- "reserves the live work of the owner above an ancestor whose end is pending, for a
  creation below it"
* `:491` -- "keeps the nodes a chain load installed for an unpublished creation through the sweep"
* `:608` -- "marks a child in a cascade from its current record, not one its pending commit replaces"
* `:626` -- "marks the other tasks of a failFast wait from their current records"

Excluded outright, with its reason -- no read-side record cache, no `CachedStorage` in this port, so the
component the case measures does not exist (recorded the same way in `PARITY.md`, next to its sibling
`session-group-commit.test.ts:635`):

* `:646` -- "reads no task record from Storage for more live and awaited tasks than the record cache's count"
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

# --------------------------------------------------------------------------- #
# The four upstream cases #42 keeps: the line, the outcome fence, the hop, and #
# close draining through the writer. Nothing here needs a recorded region.     #
# --------------------------------------------------------------------------- #
from session_support import ControlledStorage, context, flush, observe_commits_as_host
from task_support import aborted, deferred, eventually, open_tasks

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

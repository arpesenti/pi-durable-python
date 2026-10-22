"""Translated upstream ``harness-scheduler-indexes`` cases.

The scheduler's in-memory indexes: cases where a wrong index changes behaviour, each against what
the committed records imply (spec §5.4, §5.5).
"""

from __future__ import annotations

import asyncio
from typing import Any

from session_support import ControlledStorage, context
from task_support import aborted, deferred, eventually, open_tasks, settled

from pi_durable.ids import TaskId
from pi_durable.session.session import create_session
from pi_durable.tasks import define_task

gates: dict[str, Any] = {}


def gate(name: str) -> Any:
    found = gates.get(name)
    if found is None:
        found = deferred()
        gates[name] = found
    return found


async def _held(signal: Any, name: str) -> bool:
    """Wait for ``gate(name)``; return ``False`` when the signal fires first."""

    signal_task = asyncio.ensure_future(aborted(signal))
    gate_task = asyncio.ensure_future(gate(name).promise)
    done, pending = await asyncio.wait(
        [signal_task, gate_task], return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if gate_task in done and not gate_task.cancelled():
        signal_task.cancel()
        await asyncio.gather(signal_task, return_exceptions=True)
        return True
    return False


async def hold_run(task: Any, runtime: Any, ctx: Any) -> None:
    if not await _held(runtime.signal, task["input"]["name"]):
        return
    await runtime.commit(
        lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
    )


async def hold_abort(task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)


Hold = define_task(
    {
        "name": "test.index.hold",
        "version": 1,
        "initial": lambda _input: {"phase": "hold"},
        "phases": {"hold": hold_run},
        "abort": hold_abort,
    }
)

owned: dict[str, list[TaskId]] = {}
runtimes: dict[str, Any] = {}


async def probe_run(task: Any, runtime: Any, ctx: Any) -> None:
    name = task["input"]["name"]
    runtimes[name] = runtime
    if not await _held(runtime.signal, f"look.{name}"):
        return
    owned[name] = [record["id"] for record in await runtime.owned_tasks(ctx)]
    if not await _held(runtime.signal, name):
        return
    await runtime.commit(
        lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
    )


Probe = define_task(
    {
        "name": "test.index.probe",
        "version": 1,
        "initial": lambda _input: {"phase": "look"},
        "phases": {"look": probe_run},
        "abort": hold_abort,
    }
)


async def test_takes_a_cascades_reason_from_the_nearest_cancelling_owner() -> None:
    # top (abort requested) -> middle (restart-marked) -> leaf: the leaf gets middle's `restart` first, as the
    # nearest cancelling owner, and the request only once middle carries it.
    storage = ControlledStorage()
    session = create_session(storage)

    async def create(tx: Any) -> dict[str, TaskId]:
        conversation = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        top = await tx.create_task(
            Hold,
            {"name": "top"},
            {
                "conversationId": conversation["id"],
                "ownership": {"kind": "conversation"},
            },
        )
        middle = await tx.create_task(
            Hold, {"name": "middle"}, {"ownership": {"kind": "task", "taskId": top}}
        )
        leaf = await tx.create_task(
            Hold, {"name": "leaf"}, {"ownership": {"kind": "task", "taskId": middle}}
        )
        return {"top": top, "middle": middle, "leaf": leaf}

    ids = await session.commit(create, context)

    async def mark(tx: Any) -> None:
        top = await tx.task(ids["top"])
        middle = await tx.task(ids["middle"])
        assert top is not None and middle is not None
        tx.set_task({**top, "abortRequested": True})
        tx.set_task({**middle, "abortRequested": True, "abortReason": "restart"})

    await session.commit(mark, context)
    await session.close(context)

    # Record every mark of the leaf, in commit order.
    reasons: list[Any] = []
    commit = storage.commit

    async def recording_commit(writes: Any, commit_context: Any) -> Any:
        for write in writes:
            value = write.get("value")
            if (
                write["type"] == "task"
                and value is not None
                and value["id"] == ids["leaf"]
                and value.get("abortRequested")
            ):
                reasons.append(value.get("abortReason"))
        return await commit(writes, commit_context)

    storage.commit = recording_commit  # type: ignore[method-assign, assignment]
    opened = await open_tasks(storage.reopen(), [Hold])
    opened["harness"].resume()
    for id in (ids["leaf"], ids["middle"], ids["top"]):
        await opened["harness"].wait_for_task(id, context)
    assert reasons[0] == "restart"
    await opened["harness"].close(context)


async def test_finalizes_a_chain_of_held_owners_in_the_commit_that_frees_the_last() -> None:
    storage = ControlledStorage()
    # The commit, by count, that wrote each terminal record.
    ended_in: dict[TaskId, int] = {}
    commits = [0]
    commit = storage.commit

    async def recording_commit(writes: Any, commit_context: Any) -> Any:
        commits[0] += 1
        for write in writes:
            value = write.get("value")
            if (
                write["type"] == "task"
                and value is not None
                and value["state"]["status"] == "terminal"
            ):
                ended_in[value["id"]] = commits[0]
        return await commit(writes, commit_context)

    storage.commit = recording_commit  # type: ignore[method-assign, assignment]
    opened = await open_tasks(storage, [Hold])
    harness = opened["harness"]
    root = await harness.root(context)
    harness.resume()

    async def create(tx: Any) -> dict[str, TaskId]:
        top = await tx.create_task(Hold, {"name": "top"}, {"ownership": {"kind": "conversation"}})
        middle = await tx.create_task(
            Hold, {"name": "middle"}, {"ownership": {"kind": "task", "taskId": top}}
        )
        leaf = await tx.create_task(
            Hold, {"name": "leaf"}, {"ownership": {"kind": "task", "taskId": middle}}
        )
        return {"top": top, "middle": middle, "leaf": leaf}

    tree = await root.commit(create, context)
    gate("top").resolve()
    gate("middle").resolve()
    await eventually(
        lambda: _status_is(harness, tree["top"], "completing")
    )
    await eventually(lambda: _status_is(harness, tree["middle"], "completing"))
    gate("leaf").resolve()
    assert (await harness.wait_for_task(tree["top"], context))["state"]["outcome"]["status"] == (
        "completed"
    )
    # Both owners end in one finalize commit, after the leaf's.
    assert ended_in[tree["middle"]] == ended_in[tree["top"]]
    assert ended_in[tree["middle"]] > ended_in[tree["leaf"]]
    await harness.close(context)


async def _status_is(harness: Any, id: TaskId, status: str) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == status


async def test_keeps_a_scope_busy_after_reopen_below_an_ended_task_whose_chain_is_unloaded() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [Hold])
    root = await opened["harness"].root(context)
    opened["harness"].resume()

    # root conversation -> owner (ends) -> conversation C -> inner (created after owner ended).
    async def create(tx: Any) -> dict[str, Any]:
        owner = await tx.create_task(
            Hold, {"name": "owner"}, {"ownership": {"kind": "conversation"}}
        )
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": owner}})
        return {"owner": owner, "child": child["id"]}

    tree = await root.commit(create, context)
    gate("owner").resolve()
    await opened["harness"].wait_for_task(tree["owner"], context)
    conversation = await opened["harness"].conversation(tree["child"], context)
    assert conversation is not None
    inner = await conversation.commit(
        lambda tx: tx.create_task(Hold, {"name": "inner"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    await opened["harness"].close(context)
    gates.clear()

    # Hold the read of C's record, so the chain above `inner` stays unloaded while idle is decided.
    held = deferred()
    read = storage.conversation

    async def held_conversation(id: Any, read_context: Any) -> Any:
        if id == tree["child"]:
            await held.promise
        return await read(id, read_context)

    storage.conversation = held_conversation  # type: ignore[method-assign, assignment]
    opened = await open_tasks(storage.reopen(), [Hold])
    # A task whose chain is not loaded counts as inside every scope. The held read blocks the Session
    # line, so the root handle comes after.
    assert await settled(opened["harness"].wait_for_idle(context)) is False
    held.resolve()
    root = await opened["harness"].root(context)
    opened["harness"].resume()
    gate("inner").resolve()
    await opened["harness"].wait_for_task(inner, context)
    await root.wait_for_idle(context)
    await opened["harness"].close(context)


async def test_lists_the_live_tasks_it_owns_directly_in_id_order_also_after_reopen() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [Hold, Probe])
    root = await opened["harness"].root(context)
    opened["harness"].resume()

    async def create(tx: Any) -> dict[str, TaskId]:
        probe = await tx.create_task(
            Probe, {"name": "probe"}, {"ownership": {"kind": "conversation"}}
        )
        first = await tx.create_task(
            Hold, {"name": "first"}, {"ownership": {"kind": "task", "taskId": probe}}
        )
        ends = await tx.create_task(
            Hold, {"name": "ends"}, {"ownership": {"kind": "task", "taskId": probe}}
        )
        second = await tx.create_task(
            Hold, {"name": "second"}, {"ownership": {"kind": "task", "taskId": probe}}
        )
        await tx.create_task(
            Hold, {"name": "grandchild"}, {"ownership": {"kind": "task", "taskId": first}}
        )
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": probe}})
        await tx.create_task(
            Hold,
            {"name": "inside"},
            {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
        )
        return {"probe": probe, "first": first, "ends": ends, "second": second}

    ids = await root.commit(create, context)
    gate("ends").resolve()
    await opened["harness"].wait_for_task(ids["ends"], context)
    # The first ends too, but holds its outcome as `completing` while its grandchild runs: still live.
    gate("first").resolve()
    await eventually(lambda: _status_is(opened["harness"], ids["first"], "completing"))
    gate("look.probe").resolve()
    await eventually(lambda: "probe" in owned)
    assert owned["probe"] == [ids["first"], ids["second"]]
    await opened["harness"].close(context)

    # After a reopen the rerun sees the same tasks, which an earlier invocation created.
    gates.clear()
    owned.clear()
    opened = await open_tasks(storage.reopen(), [Hold, Probe])
    opened["harness"].resume()
    gate("look.probe").resolve()
    await eventually(lambda: "probe" in owned)
    assert owned["probe"] == [ids["first"], ids["second"]]
    await opened["harness"].close(context)


async def test_rejects_owned_tasks_once_its_invocation_has_ended() -> None:
    opened = await open_tasks(ControlledStorage(), [Hold, Probe])
    root = await opened["harness"].root(context)
    opened["harness"].resume()
    probe = await root.commit(
        lambda tx: tx.create_task(Probe, {"name": "probe"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    await eventually(lambda: "probe" in runtimes)
    runtime = runtimes["probe"]
    # Aborting joins the run invocation, which has ended by then.
    await opened["harness"].abort_task(probe, context)
    try:
        await runtime.owned_tasks(context)
    except BaseException as error:
        assert "invocation has ended" in str(error)
    else:
        raise AssertionError("expected a rejection")
    await opened["harness"].close(context)

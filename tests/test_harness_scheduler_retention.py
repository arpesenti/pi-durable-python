"""Translated upstream ``harness-scheduler-retention`` cases.

Scheduler memory is bounded by live work: an ended task or a conversation edge is kept only while
live work is below it, and a chain dropped that way is loaded again from storage when work appears
below it.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import ControlledStorage, context
from task_support import aborted, deferred, eventually, open_tasks, settled

from pi_durable.tasks import define_task

gates: dict[str, Any] = {}
runs: dict[str, int] = {}
rejections: list[BaseException] = []


@pytest.fixture(autouse=True)
async def _reset_globals() -> Any:
    """Clear the shared gates and counters, and stop anything a failed test left running."""

    yield
    gates.clear()
    runs.clear()
    rejections.clear()
    current = asyncio.current_task()
    for _ in range(10):
        leftover = [task for task in asyncio.all_tasks() if task is not current]
        if not leftover:
            break
        for task in leftover:
            task.cancel()
        await asyncio.gather(*leftover, return_exceptions=True)
        await asyncio.sleep(0)


def gate(name: str) -> Any:
    found = gates.get(name)
    if found is None:
        found = deferred()
        gates[name] = found
    return found


async def _gate_open(signal: Any, name: str) -> bool:
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
    name = task["input"]["name"]
    runs[name] = runs.get(name, 0) + 1
    if not await _gate_open(runtime.signal, name):
        return
    await runtime.commit(
        lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
    )


async def hold_abort(task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)


Hold = define_task(
    {
        "name": "test.retention.hold",
        "version": 1,
        "initial": lambda _input: {"phase": "hold"},
        "phases": {"hold": hold_run},
        "abort": hold_abort,
    }
)


async def spawner_run(task: Any, runtime: Any, ctx: Any) -> None:
    name = task["input"]["name"]
    if not await _gate_open(runtime.signal, name):
        return

    async def spawn(tx: Any) -> Any:
        await tx.create_task(
            Hold,
            {"name": f"inside.{name}"},
            {"ownership": {"kind": "conversation"}, "conversationId": task["input"]["into"]},
        )
        return {"status": "terminal", "outcome": {"status": "completed", "result": None}}

    await runtime.commit(spawn, ctx)


Spawner = define_task(
    {
        "name": "test.retention.spawner",
        "version": 1,
        "initial": lambda _input: {"phase": "hold"},
        "phases": {"hold": spawner_run},
        "abort": hold_abort,
    }
)


async def rejected_run(task: Any, runtime: Any, ctx: Any) -> None:
    if not await _gate_open(runtime.signal, "rejected"):
        return
    try:

        async def stage(tx: Any) -> Any:
            await tx.create_task(
                Hold,
                {"name": "staged"},
                {
                    "ownership": {"kind": "conversation"},
                    "conversationId": task["input"]["into"],
                },
            )
            await tx.create_task(
                Hold, {"name": "child"}, {"ownership": {"kind": "task", "taskId": task["id"]}}
            )
            return {"status": "terminal", "outcome": {"status": "completed", "result": None}}

        await runtime.commit(stage, ctx)
    except BaseException as error:
        rejections.append(error)
    await aborted(runtime.signal)


Rejected = define_task(
    {
        "name": "test.retention.rejected",
        "version": 1,
        "initial": lambda _input: {"phase": "hold"},
        "phases": {"hold": rejected_run},
        "abort": hold_abort,
    }
)


class WaiterRun:
    @staticmethod
    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {
                "status": "waiting",
                "checkpoint": {"phase": "done"},
                "on": task["input"]["on"],
                "policy": "allSettled",
            },
            ctx,
        )

    @staticmethod
    async def done(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
        )


Waiter = define_task(
    {
        "name": "test.retention.waiter",
        "version": 1,
        "initial": lambda _input: {"phase": "wait"},
        "phases": {"wait": WaiterRun.wait, "done": WaiterRun.done},
        "abort": hold_abort,
    }
)


def sizes(harness: Any) -> dict[str, int]:
    raw = harness.scheduler_index_sizes
    return {
        "live": raw["live"],
        "settled": raw["settled"],
        "edges": raw["edges"],
        "below": raw["below"],
        "roots": raw["roots"],
        "unloaded": raw["unloaded"],
        "drop_queue": raw["drop_queue"],
    }


async def open_retention(storage: Any | None = None) -> dict[str, Any]:
    opened = await open_tasks(storage or ControlledStorage(), [Hold, Spawner, Rejected])
    root = await opened["harness"].root(context)
    opened["harness"].resume()
    return {**opened, "root": root}


async def status(harness: Any, id: Any) -> dict[str, Any]:
    record = await harness.get_task(id, context)
    assert record is not None
    return record["state"]


async def subagent_cycle(harness: Any, root: Any, parent: Any, n: int) -> Any:
    async def create(tx: Any) -> dict[str, Any]:
        tool = await tx.create_task(
            Hold, {"name": f"tool{n}"}, {"ownership": {"kind": "task", "taskId": parent}}
        )
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": tool}})
        inner = await tx.create_task(
            Hold,
            {"name": f"inner{n}"},
            {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
        )
        return {"tool": tool, "inner": inner, "child": child["id"]}

    tree = await root.commit(create, context)
    gate(f"inner{n}").resolve()
    gate(f"tool{n}").resolve()
    await harness.wait_for_task(tree["inner"], context)
    await harness.wait_for_task(tree["tool"], context)
    return tree["child"]


async def test_forgets_ended_subagent_chains_once_their_work_ends() -> None:
    opened = await open_retention()
    harness, root = opened["harness"], opened["root"]
    parent = await root.commit(
        lambda tx: tx.create_task(
            Hold, {"name": "parent"}, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )
    await subagent_cycle(harness, root, parent, 0)
    baseline = sizes(harness)
    for n in range(1, 201):
        await subagent_cycle(harness, root, parent, n)
    assert sizes(harness) == baseline
    gate("parent").resolve()
    await harness.wait_for_task(parent, context)
    await harness.close(context)


async def test_holds_a_live_owner_for_work_later_created_in_a_forgotten_conversation() -> None:
    storage = ControlledStorage()
    reads = {"tasks": 0, "conversations": 0}
    task_read = storage.task
    conversation_read = storage.conversation

    async def counting_task(id: Any, read_context: Any) -> Any:
        reads["tasks"] += 1
        return await task_read(id, read_context)

    async def counting_conversation(id: Any, read_context: Any) -> Any:
        reads["conversations"] += 1
        return await conversation_read(id, read_context)

    storage.task = counting_task  # type: ignore[method-assign, assignment]
    storage.conversation = counting_conversation  # type: ignore[method-assign, assignment]
    opened = await open_retention(storage)
    harness, root = opened["harness"], opened["root"]

    # Live P -> ended B -> ended A -> conversation C, empty: forgotten.
    async def create(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Hold, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "task", "taskId": p}})
        a = await tx.create_task(Hold, {"name": "a"}, {"ownership": {"kind": "task", "taskId": b}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": a}})
        return {"p": p, "b": b, "a": a, "c": c["id"]}

    tree = await root.commit(create, context)
    gate("a").resolve()
    gate("b").resolve()
    await harness.wait_for_task(tree["b"], context)
    before = sizes(harness)
    loaded = reads["tasks"] + reads["conversations"]
    c = await harness.conversation(tree["c"], context)
    assert c is not None
    d = await c.commit(
        lambda tx: tx.create_task(Hold, {"name": "d"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    gate("p").resolve()
    await eventually(lambda: _status_is(harness, tree["p"], "completing"))
    assert reads["tasks"] + reads["conversations"] > loaded
    gate("d").resolve()
    assert (await harness.wait_for_task(d, context))["state"]["outcome"]["status"] == "completed"
    assert (await harness.wait_for_task(tree["p"], context))["state"]["outcome"]["status"] == (
        "completed"
    )
    assert sizes(harness)["settled"] <= before["settled"]
    await harness.close(context)


async def _status_is(harness: Any, id: Any, status: str) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == status


async def test_holds_an_owner_for_work_its_terminal_commit_creates_below_a_forgotten_chain() -> None:
    opened = await open_retention()
    harness, root = opened["harness"], opened["root"]

    # Live P (a spawner) -> ended B -> ended A -> conversation C, empty: forgotten. P's terminal commit
    # creates D in C.
    async def create(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(
            Spawner, {"name": "p", "into": 0}, {"ownership": {"kind": "conversation"}}
        )
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "task", "taskId": p}})
        a = await tx.create_task(Hold, {"name": "a"}, {"ownership": {"kind": "task", "taskId": b}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": a}})
        return {"p": p, "b": b, "c": c["id"]}

    tree = await root.commit(create, context)

    # Point the spawner at C, now that its ID is known.
    async def point(tx: Any) -> None:
        record = await tx.task(tree["p"])
        assert record is not None
        tx.set_task({**record, "input": {"name": "p", "into": tree["c"]}})

    await root.commit(point, context)
    gate("a").resolve()
    gate("b").resolve()
    await harness.wait_for_task(tree["b"], context)
    gate("p").resolve()
    await eventually(lambda: _not_running(harness, tree["p"]))
    assert await _state_status(harness, tree["p"]) == "completing"
    gate("inside.p").resolve()
    assert (await harness.wait_for_task(tree["p"], context))["state"]["outcome"]["status"] == (
        "completed"
    )
    await harness.close(context)


async def _state_status(harness: Any, id: Any) -> str:
    return (await status(harness, id))["status"]


async def _not_running(harness: Any, id: Any) -> bool:
    return await _state_status(harness, id) != "running"


async def test_aborts_work_created_in_a_forgotten_conversation_below_a_marked_owner() -> None:
    opened = await open_retention()
    harness, root = opened["harness"], opened["root"]

    async def create(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Hold, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "task", "taskId": p}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        keep = await tx.create_task(
            Hold, {"name": "keep"}, {"ownership": {"kind": "task", "taskId": p}}
        )
        return {"p": p, "b": b, "c": c["id"], "keep": keep}

    tree = await root.commit(create, context)
    gate("b").resolve()
    await harness.wait_for_task(tree["b"], context)

    # P cancels; its live child keeps it holding.
    async def mark(tx: Any) -> None:
        record = await tx.task(tree["p"])
        assert record is not None
        tx.set_task({**record, "abortRequested": True})

    await harness.commit(mark, context)
    await eventually(lambda: _abort_requested(harness, tree["keep"]))
    c = await harness.conversation(tree["c"], context)
    assert c is not None
    late = await c.commit(
        lambda tx: tx.create_task(Hold, {"name": "late"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    assert (await harness.wait_for_task(late, context))["state"]["outcome"]["status"] == "aborted"
    assert runs.get("late", 0) == 0
    await harness.close(context)


async def _abort_requested(harness: Any, id: Any) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record.get("abortRequested") is True


async def test_keeps_a_scope_busy_while_the_chain_of_new_work_below_a_forgotten_conversation_loads() -> None:
    storage = ControlledStorage()
    opened = await open_retention(storage)
    harness, root = opened["harness"], opened["root"]

    async def create(tx: Any) -> dict[str, Any]:
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "conversation"}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        return {"b": b, "c": c["id"]}

    tree = await root.commit(create, context)
    gate("b").resolve()
    await harness.wait_for_task(tree["b"], context)

    # Hold the chain load's read of C: the second read once holding, after the creating commit's own.
    read = storage.conversation
    held = deferred()
    reached = deferred()
    state = {"holding": False, "reads": 0}

    async def held_conversation(id: Any, read_context: Any) -> Any:
        if state["holding"] and id == tree["c"]:
            state["reads"] += 1
            if state["reads"] == 2:
                reached.resolve()
                await held.promise
        return await read(id, read_context)

    storage.conversation = held_conversation  # type: ignore[method-assign, assignment]
    c = await harness.conversation(tree["c"], context)
    assert c is not None
    state["holding"] = True
    await c.commit(
        lambda tx: tx.create_task(Hold, {"name": "d"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    await reached.promise
    assert await settled(harness.wait_for_idle(context)) is False
    held.resolve()
    gate("d").resolve()
    await harness.wait_for_idle(context)
    await harness.close(context)


async def test_keeps_a_chain_when_one_commit_ends_its_last_task_and_creates_new_work_below_it() -> None:
    opened = await open_retention()
    harness, root = opened["harness"], opened["root"]

    # Live P -> ended B -> conversation C with background task `old`, which keeps the chain known
    # without holding B.
    async def create(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Hold, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "task", "taskId": p}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        old = await tx.create_task(
            Hold,
            {"name": "old"},
            {
                "ownership": {"kind": "conversation"},
                "conversationId": c["id"],
                "background": True,
            },
        )
        return {"p": p, "b": b, "c": c["id"], "old": old}

    tree = await root.commit(create, context)
    gate("b").resolve()
    await harness.wait_for_task(tree["b"], context)

    # One commit: the old task ends, which leaves nothing live below B, and a replacement starts in C.
    async def replace(tx: Any) -> Any:
        old = await tx.task(tree["old"])
        assert old is not None
        id = await tx.create_task(
            Hold,
            {"name": "new"},
            {"ownership": {"kind": "conversation"}, "conversationId": tree["c"]},
        )
        tx.set_task(
            {
                **old,
                "state": {
                    "status": "terminal",
                    "outcome": {"status": "completed", "result": None},
                },
            }
        )
        return id

    replacement = await harness.commit(replace, context)
    # The chain stayed known through the publication, so P holds for the replacement.
    gate("p").resolve()
    await eventually(lambda: _status_is(harness, tree["p"], "completing"))
    gate("new").resolve()
    await harness.wait_for_task(replacement, context)
    assert (await harness.wait_for_task(tree["p"], context))["state"]["outcome"]["status"] == (
        "completed"
    )
    await harness.close(context)


async def test_drops_what_a_rejected_commits_chain_load_read_without_a_later_publication() -> None:
    opened = await open_retention()
    harness, root = opened["harness"], opened["root"]

    # Live P owns ended B, which owns conversation C: forgotten. P's terminal commit stages work in C,
    # which loads C's chain, and a child of P, which the Transaction rejects, as P is ending.
    async def create(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(
            Rejected, {"into": 0}, {"ownership": {"kind": "conversation"}}
        )
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "task", "taskId": p}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        return {"p": p, "b": b, "c": c["id"]}

    tree = await root.commit(create, context)

    async def point(tx: Any) -> None:
        record = await tx.task(tree["p"])
        assert record is not None
        tx.set_task({**record, "input": {"into": tree["c"]}})

    await root.commit(point, context)
    gate("b").resolve()
    await harness.wait_for_task(tree["b"], context)
    baseline = sizes(harness)
    gate("rejected").resolve()
    await eventually(lambda: len(rejections) > 0)
    # The rejected commit published nothing; the sweep job still cleans up what its chain load read.
    await eventually(lambda: sizes(harness)["drop_queue"] == 0)
    assert sizes(harness) == baseline
    await harness.close(context)


async def test_drops_what_a_read_loaded_after_reopen_without_a_later_publication() -> None:
    storage = ControlledStorage()
    opened = await open_retention(storage)

    # A live task D in conversation C, owned by ended B: after reopen D's chain is not loaded.
    async def create(tx: Any) -> dict[str, Any]:
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "conversation"}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        d = await tx.create_task(
            Hold,
            {"name": "d"},
            {"ownership": {"kind": "conversation"}, "conversationId": c["id"]},
        )
        return {"b": b, "d": d}

    tree = await opened["root"].commit(create, context)
    gate("b").resolve()
    await eventually(lambda: _status_is(opened["harness"], tree["b"], "completing"))
    await opened["harness"].close(context)
    gates.clear()
    opened = await open_retention(storage.reopen())
    await opened["harness"].inspect(context)
    # D is still live, so its chain stays; once D ends, everything below the root goes, published or not.
    assert sizes(opened["harness"])["unloaded"] == 0
    gate("d").resolve()
    await opened["harness"].wait_for_task(tree["d"], context)
    await opened["harness"].wait_for_task(tree["b"], context)
    await eventually(lambda: sizes(opened["harness"])["drop_queue"] == 0)
    assert sizes(opened["harness"])["settled"] == 0
    assert sizes(opened["harness"])["below"] == 0
    assert sizes(opened["harness"])["unloaded"] == 0
    await opened["harness"].close(context)


async def test_forgets_a_deep_chain_of_ended_tasks_without_recursion() -> None:
    opened = await open_retention()
    harness, root = opened["harness"], opened["root"]
    depth = 20_000
    baseline = sizes(harness)

    async def create(tx: Any) -> dict[str, Any]:
        links: list[Any] = []
        owner = await tx.create_task(
            Hold, {"name": "link0"}, {"ownership": {"kind": "conversation"}}
        )
        links.append(owner)
        for n in range(1, depth):
            owner = await tx.create_task(
                Hold, {"name": f"link{n}"}, {"ownership": {"kind": "task", "taskId": owner}}
            )
            links.append(owner)
        leaf = await tx.create_task(
            Hold, {"name": "leaf"}, {"ownership": {"kind": "task", "taskId": owner}}
        )
        return {"links": links, "leaf": leaf}

    tree = await root.commit(create, context)
    for n in range(depth):
        gate(f"link{n}").resolve()
    gate("leaf").resolve()
    await harness.wait_for_task(tree["leaf"], context)
    await harness.wait_for_task(tree["links"][0], context)
    await harness.wait_for_idle(context)
    assert sizes(harness) == baseline
    await harness.close(context)


async def test_keeps_no_more_after_all_work_ended_than_a_fresh_reopen() -> None:
    storage = ControlledStorage()
    opened = await open_retention(storage)
    parent = await opened["root"].commit(
        lambda tx: tx.create_task(
            Hold, {"name": "parent"}, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )
    for n in range(20):
        await subagent_cycle(opened["harness"], opened["root"], parent, n)
    gate("parent").resolve()
    await opened["harness"].wait_for_task(parent, context)
    await opened["harness"].wait_for_idle(context)
    ended = sizes(opened["harness"])
    await opened["harness"].close(context)
    opened = await open_retention(storage.reopen())
    await opened["harness"].wait_for_idle(context)
    assert ended == sizes(opened["harness"])
    await opened["harness"].close(context)


async def test_lets_a_task_wait_on_a_forgotten_ended_owner() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [Hold, Waiter])
    root = await opened["harness"].root(context)
    opened["harness"].resume()

    async def create(tx: Any) -> dict[str, Any]:
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "conversation"}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        return {"b": b, "c": c["id"]}

    tree = await root.commit(create, context)
    gate("b").resolve()
    await opened["harness"].wait_for_task(tree["b"], context)
    c = await opened["harness"].conversation(tree["c"], context)
    assert c is not None
    waiter = await c.commit(
        lambda tx: tx.create_task(
            Waiter, {"on": [tree["b"]]}, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )
    outcome = (await opened["harness"].wait_for_task(waiter, context))["state"]["outcome"]
    assert outcome["status"] == "completed"
    await opened["harness"].close(context)

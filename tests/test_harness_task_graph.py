"""Translated v1.3.0 ``harness-task-graph`` suite.

Covers every live task's statuses, owner edges, and owned conversations; rebuilds from committed
state after reopen; exact replayable frames; cancellation and close; and revisions only for
commits that actually change a node.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from typing import Any

import pytest
from session_support import ControlledStorage, context, flush
from task_support import (
    aborted,
    aborted_with,
    completed,
    deferred,
    eventually,
    open_tasks,
    with_signal,
)

from pi_durable import MemoryStorage, TaskGraph
from pi_durable.context import AbortController
from pi_durable.delta import apply_immutable
from pi_durable.harness.define import define_extension
from pi_durable.ids import ConversationId, TaskId
from pi_durable.storage.sqlite import SqliteStorage
from pi_durable.storage.sqlite.posix import open_posix_sqlite_database
from pi_durable.tasks import define_task


def family(gates: dict[str, Any]) -> dict[str, Any]:
    """A parent that creates children and owned conversations, then holds its outcome."""

    async def child_work(task: Any, runtime: Any, ctx: Any) -> None:
        gate = gates["late"] if task["input"]["late"] else gates["child"]
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    async def child_abort(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    Child = define_task(
        {
            "name": "test.graph-child",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": child_work},
            "abort": child_abort,
        }
    )

    async def spawn(task: Any, runtime: Any, ctx: Any) -> None:
        ownership = {"kind": "task", "taskId": task["id"]}

        async def conversations(tx: Any) -> None:
            await tx.create_conversation({"ownership": ownership})
            await tx.create_conversation({"ownership": ownership})

        await runtime.commit(conversations, ctx)

        async def create(tx: Any) -> dict[str, Any]:
            child = await tx.create_task(
                Child, {"late": False}, {"ownership": ownership}
            )
            return {
                "status": "waiting",
                "checkpoint": {"phase": "join", "child": child},
                "on": [child],
                "policy": "allSettled",
            }

        await runtime.commit(create, ctx)

    async def join(task: Any, runtime: Any, ctx: Any) -> None:
        async def create(tx: Any) -> dict[str, Any]:
            await tx.create_task(
                Child,
                {"late": True},
                {"ownership": {"kind": "task", "taskId": task["id"]}},
            )
            return {"status": "running", "checkpoint": {"phase": "finish"}}

        await runtime.commit(create, ctx)

    async def finish(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    Parent = define_task(
        {
            "name": "test.graph-parent",
            "version": 1,
            "initial": lambda _input: {"phase": "spawn"},
            "phases": {"spawn": spawn, "join": join, "finish": finish},
            "abort": child_abort,
        }
    )
    return {"Parent": Parent, "Child": Child}


def gates_never() -> dict[str, Any]:
    return {"child": deferred(), "late": deferred()}


def statuses(graph: TaskGraph) -> dict[str, str]:
    return {
        f"{node['kind']}#{node['id']}": node["state"]["status"]
        for node in graph["tasks"].values()
    }


async def test_follows_every_live_task_through_statuses_owner_edges_and_owned_conversations() -> None:
    child = deferred()
    late = deferred()
    tasks = family({"child": child, "late": late})
    opened = await open_tasks(MemoryStorage(), [tasks["Parent"], tasks["Child"]])
    harness = opened["harness"]
    root = await harness.root(context)
    seen: list[TaskGraph] = []

    async def observe() -> Any:
        opened_graph = await harness.task_graph(context)
        opened_graph.subscribe(
            lambda value, _context, delivery: seen.append(value)
            if delivery.kind == "update"
            else None
        )
        return opened_graph

    graph = await observe()
    assert graph.value == {"tasks": {}}

    async def rebuild() -> None:
        nonlocal graph
        advanced = graph.value
        graph.dispose()
        graph = await observe()
        assert graph.value is not advanced
        assert graph.value == advanced

    parent = await root.commit(
        lambda tx: tx.create_task(
            tasks["Parent"], None, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )
    await flush()
    assert graph.value["tasks"][str(parent)] == {
        "id": parent,
        "kind": "test.graph-parent",
        "conversationId": root.id,
        "background": False,
        "abortRequested": False,
        "state": {"status": "pending", "phase": "spawn"},
        "conversations": [],
    }

    harness.resume()

    def child_id() -> str:
        return next(
            (
                node["id"]
                for node in graph.value["tasks"].values()
                if node["kind"] == "test.graph-child"
            ),
            "",
        )

    await eventually(
        lambda: len(graph.value["tasks"]) == 2
        and statuses(graph.value).get(f"test.graph-child#{child_id()}") == "running"
    )
    first = TaskId(int(child_id()))
    parent_node = graph.value["tasks"][str(parent)]
    assert parent_node["state"] == {
        "status": "waiting",
        "phase": "join",
        "on": [first],
        "policy": "allSettled",
    }
    assert len(parent_node["conversations"]) == 2
    assert parent_node["conversations"] == sorted(parent_node["conversations"])
    owned = parent_node["conversations"][0]
    assert graph.value["tasks"][str(first)]["owner"] == parent
    assert graph.value["tasks"][str(first)]["conversationId"] == root.id
    assert (await harness.conversation(owned, context)) is not None
    await rebuild()

    child.resolve()
    # The parent completes while the late child lives: its outcome is held.
    await eventually(
        lambda: graph.value["tasks"].get(str(parent), {}).get("state", {}).get("status")
        == "completing"
    )
    assert graph.value["tasks"][str(parent)]["state"] == {
        "status": "completing",
        "outcome": "completed",
    }
    assert str(first) not in graph.value["tasks"]
    # Owned conversations stay listed while the owner lives.
    assert graph.value["tasks"][str(parent)]["conversations"] == parent_node["conversations"]
    await rebuild()

    late.resolve()
    await harness.wait_for_task(parent, context)
    await flush()
    assert graph.value == {"tasks": {}}
    # Every revision is one commit that changed a node; none repeats its predecessor.
    for index in range(1, len(seen)):
        assert seen[index] != seen[index - 1]
    # The commit that created the two conversations published one revision setting them.
    assert any(
        (node := value["tasks"].get(str(parent))) is not None
        and len(node["conversations"]) == 2
        for value in seen
    )
    graph.dispose()
    await harness.close(context)


async def test_builds_from_committed_tasks_shows_surviving_tasks_as_pending_and_marks_aborts() -> None:
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "session.sqlite"
        gate = deferred()

        async def work(_task: Any, runtime: Any, ctx: Any) -> None:
            waiters = [
                asyncio.ensure_future(asyncio.shield(gate.promise)),
                asyncio.ensure_future(aborted(runtime.signal)),
            ]
            for waiter in waiters:
                waiter.add_done_callback(
                    lambda future: future.exception()
                    if not future.cancelled()
                    else None
                )
            done, pending = await asyncio.wait(
                waiters, return_when=asyncio.FIRST_COMPLETED
            )
            for waiter in pending:
                waiter.cancel()
            for waiter in done:
                waiter.result()
            await runtime.commit(lambda: completed(None), ctx)

        async def abort(_task: Any, runtime: Any, ctx: Any) -> None:
            await runtime.commit(lambda: aborted_with("test"), ctx)

        Work = define_task(
            {
                "name": "test.graph-work",
                "version": 1,
                "initial": lambda _input: {"phase": "work"},
                "phases": {"work": work},
                "abort": abort,
            }
        )
        first = await open_tasks(
            await SqliteStorage.open(await open_posix_sqlite_database(str(path))), [Work]
        )
        first_root = await first["harness"].root(context)

        async def create(tx: Any) -> tuple[TaskId, TaskId, Any]:
            foreground = await tx.create_task(
                Work, None, {"ownership": {"kind": "conversation"}}
            )
            background = await tx.create_task(
                Work,
                None,
                {"ownership": {"kind": "conversation"}, "background": True},
            )
            owned = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": foreground}}
            )
            return foreground, background, owned["id"]

        foreground, background, owned = await first_root.commit(create, context)
        first["harness"].resume()
        await eventually(
            lambda: _all_running(first["harness"])
        )
        running = await first["harness"].task_graph(context)
        assert statuses(running.value) == {
            f"test.graph-work#{foreground}": "running",
            f"test.graph-work#{background}": "running",
        }
        await first["harness"].close(context)

        # Acquired after reopen: built from the committed records and owner edges; open reconciled
        # running to pending.
        reopened = await open_tasks(
            await SqliteStorage.open(await open_posix_sqlite_database(str(path))), [Work]
        )
        harness = reopened["harness"]
        watch = await harness.watch_task_graph(context)
        assert statuses(watch.value) == {
            f"test.graph-work#{foreground}": "pending",
            f"test.graph-work#{background}": "pending",
        }
        assert watch.value["tasks"][str(foreground)]["conversations"] == [
            ConversationId(owned)
        ]
        assert watch.value["tasks"][str(background)]["background"] is True

        # Exact frames: replaying their operations from the acquisition revision gives each value.
        replica: Any = watch.value
        frames: list[list[Any]] = []

        async def listener(value: Any, ops: Any, _context: Any) -> None:
            nonlocal replica
            replica = apply_immutable(replica, ops)
            assert replica == value
            frames.append(ops)

        watch.start(listener)
        harness.resume()
        await eventually(
            lambda: watch.value["tasks"]
            .get(str(background), {})
            .get("state", {})
            .get("status")
            == "running"
        )
        assert await harness.abort_task(background, context) == "marked"
        await harness.wait_for_task(background, context)
        await flush()
        assert any(
            op[0] == "s"
            and op[1] == ["tasks", str(background)]
            and op[2]["abortRequested"] is True
            for ops in frames
            for op in ops
        )
        assert frames[-1] == [["d", ["tasks", str(background)]]]
        assert list(replica["tasks"]) == [str(foreground)]
        await watch.stop()
        gate.resolve()
        await harness.wait_for_task(foreground, context)
        await harness.close(context)


async def _all_running(harness: Any) -> bool:
    inspection = await harness.inspect(context)
    return bool(inspection["tasks"]) and all(
        task["state"]["kind"] == "running" for task in inspection["tasks"]
    )


async def test_lists_owned_conversations_in_id_order_whatever_order_one_commit_creates_them() -> None:
    gate = deferred()
    created: list[ConversationId] = []

    async def spawn(task: Any, runtime: Any, ctx: Any) -> None:
        async def create(tx: Any) -> None:
            ownership = {"kind": "task", "taskId": task["id"]}
            forked, fresh = await asyncio.gather(
                tx.fork_conversation(
                    runtime.conversation_id, task["input"]["at"], {"ownership": ownership}
                ),
                tx.create_conversation({"ownership": ownership}),
            )
            created.extend([forked["id"], fresh["id"]])

        await runtime.commit(create, ctx)
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    async def abort(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    Spawner = define_task(
        {
            "name": "test.graph-spawner",
            "version": 1,
            "initial": lambda _input: {"phase": "spawn"},
            "phases": {"spawn": spawn},
            "abort": abort,
        }
    )
    opened = await open_tasks(MemoryStorage(), [Spawner])
    harness = opened["harness"]
    root = await harness.root(context)
    graph = await harness.task_graph(context)

    async def create(tx: Any) -> TaskId:
        entry = await tx.append_entry(root.id, {"kind": "note"})
        return await tx.create_task(
            Spawner,
            {"at": entry["id"]},
            {"ownership": {"kind": "conversation"}},
        )

    id = await root.commit(create, context)
    harness.resume()
    await eventually(
        lambda: len(graph.value["tasks"].get(str(id), {}).get("conversations", [])) == 2
    )
    advanced = graph.value
    assert advanced["tasks"][str(id)]["conversations"] == sorted(created)
    graph.dispose()
    rebuilt = await harness.task_graph(context)
    assert rebuilt.value == advanced
    rebuilt.dispose()
    gate.resolve()
    await harness.wait_for_task(id, context)
    await harness.close(context)


async def test_registers_nothing_for_an_acquisition_cancelled_while_it_waits_for_the_line() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    harness = opened["harness"]
    root = await harness.root(context)
    child = family(gates_never())["Child"]
    await root.commit(
        lambda tx: tx.create_task(
            child, {"late": False}, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )
    held = storage.hold_commits()

    async def blocker(tx: Any) -> None:
        await tx.append_entry(root.id, {"kind": "blocker"})

    blocking = asyncio.ensure_future(harness.commit(blocker, context))
    await held.entered.wait()
    controller = AbortController()
    cancelled = asyncio.ensure_future(
        harness.watch_task_graph(with_signal(controller.signal))
    )
    controller.abort(RuntimeError("cancelled"))
    held.release()
    await blocking
    with pytest.raises(RuntimeError, match="cancelled"):
        await cancelled
    # No observer kept the mount: each new observer builds a new revision.
    first = await harness.task_graph(context)
    value = first.value
    first.dispose()
    second = await harness.task_graph(context)
    assert second.value is not value
    assert second.value == value
    second.dispose()
    await harness.close(context)


async def test_publishes_no_revision_for_a_commit_that_changes_no_node_and_shares_one_mount() -> None:
    gate = deferred()
    reached = deferred()

    async def work(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.memo("seen", True, ctx)
        reached.resolve()
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    async def abort(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    Memo = define_task(
        {
            "name": "test.graph-memo",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": abort,
        }
    )
    opened = await open_tasks(MemoryStorage(), [Memo])
    harness = opened["harness"]
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Memo, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    first = await harness.task_graph(context)
    second = await harness.task_graph(context)
    assert second.value is first.value
    updates: list[str] = []

    def listener(value: Any, _context: Any, delivery: Any) -> None:
        if delivery.kind == "update":
            updates.append(value["tasks"].get(str(id), {}).get("state", {}).get("status", "gone"))

    first.subscribe(listener)
    harness.resume()
    await reached.promise
    await flush()
    # Reservation changed the node; the memo commit did not.
    assert updates == ["running"]
    gate.resolve()
    await harness.wait_for_task(id, context)
    await flush()
    assert updates == ["running", "gone"]
    last = first.value
    first.dispose()
    second.dispose()
    # No observer is left, so the mount was dropped: a new observer builds a new revision.
    rebuilt = await harness.task_graph(context)
    assert rebuilt.value == last
    assert rebuilt.value is not last
    rebuilt.dispose()
    await harness.close(context)


async def test_publishes_a_sleeping_state_and_a_derived_blocked_reason() -> None:
    clock = [1_000]

    async def nap(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.sleep_until(clock[0] + 500, {"phase": "later"}, ctx)

    async def later(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    Nap = define_task(
        {
            "name": "test.graph-nap",
            "version": 1,
            "initial": lambda _input: {"phase": "nap"},
            "phases": {"nap": nap, "later": later},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    Missing = define_task(
        {
            "name": "test.graph-missing",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": nap},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    opened = await open_tasks(MemoryStorage(), [Nap], {"now": lambda: clock[0]})
    harness = opened["harness"]
    root = await harness.root(context)

    async def create(tx: Any) -> tuple[TaskId, TaskId]:
        sleeping = await tx.create_task(Nap, None, {"ownership": {"kind": "conversation"}})
        # Created through a token the registry does not hold: nothing can run it.
        missing = await tx.create_task(Missing, None, {"ownership": {"kind": "conversation"}})
        return sleeping, missing

    sleeping, missing = await root.commit(create, context)
    harness.resume()
    graph = await harness.task_graph(context)
    await eventually(
        lambda: graph.value["tasks"].get(str(sleeping), {}).get("state", {}).get("status")
        == "sleeping"
    )
    assert graph.value["tasks"][str(sleeping)]["state"] == {
        "status": "sleeping",
        "phase": "later",
        "until": 1_500,
    }
    await eventually(
        lambda: graph.value["tasks"].get(str(missing), {}).get("blocked") == "missing_task"
    )
    opened["registry"].install(define_extension({"name": "late", "tasks": [_late_definition()]}))
    # A registry change derives the reason again, without a commit.
    await eventually(
        lambda: graph.value["tasks"].get(str(missing), {}).get("blocked") is None
    )
    graph.dispose()
    await harness.close(context)


def _late_definition() -> Any:
    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    return define_task(
        {
            "name": "test.graph-missing",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )

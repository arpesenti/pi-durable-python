"""Translated wait-graph cases of the upstream ``harness-structured`` suite.

The broader structured-concurrency cases live in ``test_harness_tasks.py`` and
``test_harness_ownership.py``. This file ports the seven wait-graph cases: a two-task cycle, a
cycle through a held owner, a cycle two holds deep, a background wait on its conversation owner,
the orchestrator/worker/reviewer cycle, a diamond, and a longer cycle.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from session_support import context
from task_support import completed, deferred, eventually, open_tasks

from pi_durable import MemoryStorage
from pi_durable.tasks import define_task

BEHAVIORS: dict[str, dict[str, Callable[..., Any]]] = {}
GATES: dict[str, Any] = {}
LOG: list[str] = []
OWN_CONVERSATION = {"ownership": {"kind": "conversation"}}


@pytest.fixture(autouse=True)
def _reset_scripts() -> Any:
    BEHAVIORS.clear()
    GATES.clear()
    LOG.clear()
    yield
    BEHAVIORS.clear()
    GATES.clear()
    LOG.clear()


def script(name: str, behavior: dict[str, Callable[..., Any]]) -> None:
    BEHAVIORS[name] = behavior


def end(ending: str, name: str) -> dict[str, Any]:
    if ending == "completed":
        return {"status": "terminal", "outcome": {"status": "completed", "result": name}}
    return {
        "status": "terminal",
        "outcome": {"status": "failed", "error": {"message": f"{name} failed"}},
    }


def wait_on(on: list[Any], policy: str = "allSettled", round: int = 1) -> dict[str, Any]:
    return {
        "status": "waiting",
        "checkpoint": {"phase": "resume", "round": round},
        "on": on,
        "policy": policy,
    }


async def default_run(task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: end("completed", task["input"]["name"]), ctx)


async def node_run(task: Any, runtime: Any, ctx: Any) -> None:
    name = task["input"]["name"]
    LOG.append(f"run:{name}")
    behavior = BEHAVIORS.get(name, {}).get("run")
    if behavior is None:
        await default_run(task, runtime, ctx)
    else:
        await behavior(task, runtime, ctx)


async def node_resume(task: Any, runtime: Any, ctx: Any) -> None:
    name = task["input"]["name"]
    LOG.append(f"resume:{name}")
    behavior = BEHAVIORS.get(name, {}).get("resume")
    if behavior is None:
        await runtime.commit(lambda: end("completed", name), ctx)
    else:
        await behavior(task, runtime, ctx)


async def node_abort(task: Any, runtime: Any, ctx: Any) -> None:
    name = task["input"]["name"]
    LOG.append(f"abort:{name}")
    behavior = BEHAVIORS.get(name, {}).get("abort")
    if behavior is None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted", "result": name}}, ctx
        )
    else:
        await behavior(task, runtime, ctx)


Node = define_task(
    {
        "name": "test.node",
        "version": 1,
        "initial": lambda _input: {"phase": "run"},
        "phases": {"run": node_run, "resume": node_resume},
        "abort": node_abort,
    }
)


def spawn(tx: Any, owner: Any, name: str) -> Any:
    return tx.create_task(Node, {"name": name}, {"ownership": {"kind": "task", "taskId": owner}})


async def open_nodes() -> dict[str, Any]:
    opened = await open_tasks(MemoryStorage(), [Node])
    root = await opened["harness"].root(context)
    opened["harness"].resume()
    return {**opened, "root": root}


async def outcome_of(harness: Any, id: Any) -> str:
    return (await harness.wait_for_task(id, context))["state"]["outcome"]["status"]


async def state_of(harness: Any, id: Any) -> str:
    record = await harness.get_task(id, context)
    assert record is not None
    return record["state"]["status"]


async def test_rejects_the_wait_that_closes_a_cycle() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}

    def a_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["b"]]), ctx)

    def b_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["a"]]), ctx)

    script("a", {"run": a_run})
    script("b", {"run": b_run})

    async def create(tx: Any) -> None:
        ids["a"] = await tx.create_task(Node, {"name": "a"}, OWN_CONVERSATION)
        ids["b"] = await tx.create_task(Node, {"name": "b"}, OWN_CONVERSATION)

    await root.commit(create, context)
    outcomes = sorted(
        [await outcome_of(harness, ids["a"]), await outcome_of(harness, ids["b"])]
    )
    assert outcomes == ["completed", "faulted"]
    await harness.close(context)


async def test_rejects_the_wait_that_closes_a_cycle_through_a_held_owner() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}

    def spawn_and_finish(parent: str, child: str) -> Any:
        async def run(task: Any, runtime: Any, ctx: Any) -> None:
            async def create(tx: Any) -> Any:
                ids[child] = await spawn(tx, task["id"], child)
                return {"status": "running", "checkpoint": {"phase": "resume", "round": 1}}

            await runtime.commit(create, ctx)

        return run

    def child_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["other"]]), ctx)

    def other_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["held"]]), ctx)

    script("held", {"run": spawn_and_finish("held", "child")})
    script("child", {"run": child_run})
    script("other", {"run": other_run})

    async def create(tx: Any) -> None:
        ids["held"] = await tx.create_task(Node, {"name": "held"}, OWN_CONVERSATION)
        ids["other"] = await tx.create_task(Node, {"name": "other"}, OWN_CONVERSATION)

    await root.commit(create, context)
    assert await outcome_of(harness, ids["held"]) == "completed"
    outcomes = sorted(
        [
            await outcome_of(harness, ids["child"]),
            await outcome_of(harness, ids["other"]),
        ]
    )
    assert outcomes == ["completed", "faulted"]
    await harness.close(context)


async def test_rejects_a_cycle_through_holds_two_levels_deep() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}

    def spawn_and_finish(parent: str, child: str) -> Any:
        async def run(task: Any, runtime: Any, ctx: Any) -> None:
            async def create(tx: Any) -> Any:
                ids[child] = await spawn(tx, task["id"], child)
                return {"status": "running", "checkpoint": {"phase": "resume", "round": 1}}

            await runtime.commit(create, ctx)

        return run

    async def grandchild_run(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: wait_on([ids["other"]]), ctx)

    async def other_run(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: wait_on([ids["held"]]), ctx)

    script("held", {"run": spawn_and_finish("held", "child")})
    script("child", {"run": spawn_and_finish("child", "grandchild")})
    script("grandchild", {"run": grandchild_run})
    script("other", {"run": other_run})

    async def create(tx: Any) -> None:
        ids["held"] = await tx.create_task(Node, {"name": "held"}, OWN_CONVERSATION)
        ids["other"] = await tx.create_task(Node, {"name": "other"}, OWN_CONVERSATION)

    await root.commit(create, context)
    await eventually(lambda: ids.get("grandchild") is not None)
    outcomes = sorted(
        [
            await outcome_of(harness, ids["grandchild"]),
            await outcome_of(harness, ids["other"]),
        ]
    )
    assert outcomes == ["completed", "faulted"]
    assert await outcome_of(harness, ids["child"]) == "completed"
    await harness.close(context)


async def test_lets_a_background_task_wait_on_the_owner_of_its_conversation() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}

    owner_gate = deferred()

    def background_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["owner"]]), ctx)

    async def owner_run(_task: Any, runtime: Any, ctx: Any) -> None:
        await owner_gate.promise
        await runtime.commit(lambda: completed("owner"), ctx)

    script("background", {"run": background_run})
    script("owner", {"run": owner_run})

    async def create(tx: Any) -> Any:
        ids["owner"] = await tx.create_task(Node, {"name": "owner"}, OWN_CONVERSATION)
        conversation = await tx.create_conversation(
            {"ownership": {"kind": "task", "taskId": ids["owner"]}}
        )
        return await tx.create_task(
            Node,
            {"name": "background"},
            {
                "ownership": {"kind": "conversation"},
                "conversationId": conversation["id"],
                "background": True,
            },
        )

    background = await root.commit(create, context)
    await eventually(lambda: _status(harness, background, "waiting"))
    owner_gate.resolve()
    assert await outcome_of(harness, background) == "completed"
    await harness.close(context)


async def _status(harness: Any, id: Any, value: str) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == value


async def test_rejects_a_cycle_of_a_hold_and_two_waits() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}
    reviewer_waits = deferred()

    async def orchestrator_run(task: Any, runtime: Any, ctx: Any) -> None:
        async def create(tx: Any) -> Any:
            ids["worker"] = await spawn(tx, task["id"], "worker")
            return {"status": "running", "checkpoint": {"phase": "resume", "round": 1}}

        await runtime.commit(create, ctx)

    async def reviewer_run(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: wait_on([ids["orchestrator"]]), ctx)
        reviewer_waits.resolve()

    async def worker_run(_task: Any, runtime: Any, ctx: Any) -> None:
        await reviewer_waits.promise
        await runtime.commit(lambda: wait_on([ids["reviewer"]]), ctx)

    script("orchestrator", {"run": orchestrator_run})
    script("reviewer", {"run": reviewer_run})
    script("worker", {"run": worker_run})

    async def start(name: str) -> Any:
        return await root.commit(
            lambda tx: tx.create_task(Node, {"name": name}, OWN_CONVERSATION), context
        )

    ids["orchestrator"] = await start("orchestrator")
    await eventually(lambda: _status(harness, ids["orchestrator"], "completing"))
    ids["reviewer"] = await start("reviewer")
    outcomes = await asyncio.gather(
        outcome_of(harness, ids["orchestrator"]),
        outcome_of(harness, ids["worker"]),
        outcome_of(harness, ids["reviewer"]),
    )
    assert list(outcomes) == ["completed", "faulted", "completed"]
    worker = (await harness.wait_for_task(ids["worker"], context))["state"]["outcome"]
    assert "is a wait cycle" in worker["error"]["message"]
    assert "->" in worker["error"]["message"]
    await harness.close(context)


async def test_allows_a_diamond_of_waits() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}

    def top_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["left"], ids["right"]]), ctx)

    def left_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["bottom"]]), ctx)

    def right_run(_task: Any, runtime: Any, ctx: Any) -> Any:
        return runtime.commit(lambda: wait_on([ids["bottom"]]), ctx)

    script("top", {"run": top_run})
    script("left", {"run": left_run})
    script("right", {"run": right_run})

    async def create(tx: Any) -> None:
        for name in ("bottom", "left", "right", "top"):
            ids[name] = await tx.create_task(Node, {"name": name}, OWN_CONVERSATION)

    await root.commit(create, context)
    assert await outcome_of(harness, ids["top"]) == "completed"
    await harness.close(context)


async def test_rejects_a_longer_cycle_of_waits() -> None:
    opened = await open_nodes()
    harness, root = opened["harness"], opened["root"]
    ids: dict[str, Any] = {}
    names = ["x", "y", "z"]

    for index, name in enumerate(names):
        following = names[(index + 1) % len(names)]

        def run(_task: Any, runtime: Any, ctx: Any, following: str = following) -> Any:
            return runtime.commit(lambda: wait_on([ids[following]]), ctx)

        script(name, {"run": run})

    async def create(tx: Any) -> None:
        for name in names:
            ids[name] = await tx.create_task(Node, {"name": name}, OWN_CONVERSATION)

    await root.commit(create, context)
    outcomes = [await outcome_of(harness, ids[name]) for name in names]
    assert outcomes.count("faulted") == 1
    assert outcomes.count("completed") == 2
    await harness.close(context)

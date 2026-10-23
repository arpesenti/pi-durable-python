"""Translated upstream ``harness-inspect`` cases for the live inspection surface."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import ControlledStorage, context, flush
from task_support import deferred, eventually, open_tasks

from pi_durable import MemoryStorage
from pi_durable.harness.define import define_extension
from pi_durable.tasks import define_task


def one_phase(name: str, run: Any, abort: Any = None) -> Any:
    async def default_abort(_task: Any, runtime: Any, _context: Any) -> None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, _context
        )

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": default_abort if abort is None else abort,
        }
    )


async def wait_states(harness: Any) -> dict[int, dict[str, Any]]:
    inspection = await harness.inspect(context)
    return {task["record"]["id"]: task["state"] for task in inspection["tasks"]}


def task_records(inspection: dict[str, Any]) -> list[int]:
    return [task["record"]["id"] for task in inspection["tasks"]]


async def test_reports_paused_running_and_empty_states_with_id_ordered_submissions() -> None:
    gate = deferred()
    runs = {"held": 0}

    async def hold(_task: Any, runtime: Any, ctx: Any) -> None:
        runs["held"] += 1
        await gate.promise
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
        )

    Held = one_phase("test.inspect-held", hold)

    async def waiter_phase(task: Any, runtime: Any, ctx: Any) -> None:
        if task["state"]["checkpoint"]["phase"] == "wait":
            await runtime.commit(
                lambda: {
                    "status": "waiting",
                    "checkpoint": {"phase": "done"},
                    "on": task["input"]["on"],
                    "policy": "allSettled",
                },
                ctx,
            )
        else:
            await runtime.commit(
                lambda: {
                    "status": "terminal",
                    "outcome": {"status": "completed", "result": None},
                },
                ctx,
            )

    Waiter = define_task(
        {
            "name": "test.inspect-waiter",
            "version": 1,
            "initial": lambda _input: {"phase": "wait"},
            "phases": {"wait": waiter_phase, "done": waiter_phase},
            "abort": lambda _task, _runtime, _context: None,
        }
    )

    opened = await open_tasks(MemoryStorage(), [Held, Waiter])
    harness, root = opened["harness"], await opened["harness"].root(context)
    held = await root.commit(
        lambda tx: tx.create_task(Held, None, {"ownership": {"kind": "conversation"}}), context
    )
    waiter = await root.commit(
        lambda tx: tx.create_task(
            Waiter, {"on": [held]}, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )

    async def submissions(tx: Any) -> list[int]:
        entry = await tx.append_entry(root.id, {"kind": "test.mark"})
        queued = await tx.create_submission(
            {"conversationId": root.id, "type": "input", "status": "queued"}
        )
        placed = await tx.create_submission(
            {
                "conversationId": root.id,
                "type": "input",
                "status": "placed",
                "entry": entry["id"],
            }
        )
        return [queued["id"], placed["id"]]

    submission_ids = await root.commit(submissions, context)

    inspection = await harness.inspect(context)
    assert inspection["scheduling"] == "paused"
    assert task_records(inspection) == [held, waiter]
    assert [task["state"] for task in inspection["tasks"]] == [
        {"kind": "ready", "migrates": False},
        {"kind": "ready", "migrates": False},
    ]
    assert [record["id"] for record in inspection["submissions"]] == submission_ids
    assert [record["status"] for record in inspection["submissions"]] == ["queued", "placed"]
    assert inspection["submissions"][1]["conversationId"] == root.id

    harness.resume()
    await eventually(lambda: runs["held"] == 1)
    inspection = await harness.inspect(context)
    assert inspection["scheduling"] == "running"
    states = await wait_states(harness)
    assert states[held] == {"kind": "running"}
    assert states[waiter] == {"kind": "waiting", "on": [held]}

    gate.resolve()
    await harness.wait_for_idle(context)
    inspection = await harness.inspect(context)
    assert inspection["tasks"] == []
    assert [record["id"] for record in inspection["submissions"]] == submission_ids
    await harness.close(context)


async def test_never_starts_scheduling_for_a_read_only_viewer() -> None:
    runs = {"count": 0}

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        runs["count"] += 1
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
        )

    Watched = one_phase("test.inspect-watched", run)
    opened = await open_tasks(MemoryStorage(), [Watched])
    harness, root = opened["harness"], await opened["harness"].root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Watched, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    for _ in range(3):
        inspection = await harness.inspect(context)
        assert inspection["scheduling"] == "paused"
        assert inspection["tasks"][0]["state"] == {"kind": "ready", "migrates": False}
        assert (await harness.get_task(id, context))["state"]["status"] == "pending"
        await flush()
    assert runs["count"] == 0
    harness.resume()
    await eventually(lambda: runs["count"] == 1)
    await harness.close(context)


async def test_reports_blocked_reasons_without_migrating_or_running_them() -> None:
    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
        )

    first = one_phase("test.inspect-versioned", run)
    later = define_task(
        {
            "name": "test.inspect-versioned",
            "version": 2,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _context: None,
        }
    )

    async def case(installed: Any, replacement: Any, reason: str) -> None:
        opened = await open_tasks(MemoryStorage(), [])
        registry = opened["registry"]
        registry.install(define_extension({"name": "versioned", "tasks": [installed]}))
        root = await opened["harness"].root(context)
        id = await root.commit(
            lambda tx: tx.create_task(installed, None, {"ownership": {"kind": "conversation"}}),
            context,
        )
        registry.uninstall(define_extension({"name": "versioned"}))
        if replacement is not None:
            registry.install(define_extension({"name": "versioned", "tasks": [replacement]}))
        try:
            inspection = await opened["harness"].inspect(context)
            assert inspection["tasks"][0]["record"]["id"] == id
            state = inspection["tasks"][0]["state"]
            assert state["kind"] == "blocked" and state["reason"] == reason
            assert (await opened["harness"].get_task(id, context))["state"]["status"] == "pending"
        finally:
            await opened["harness"].close(context)

    await case(first, None, "missing_task")
    await case(later, first, "task_too_old")
    opened = await open_tasks(MemoryStorage(), [])
    opened["registry"].install(define_extension({"name": "versioned", "tasks": [first]}))
    root = await opened["harness"].root(context)
    await root.commit(
        lambda tx: tx.create_task(first, None, {"ownership": {"kind": "conversation"}}), context
    )
    opened["registry"].uninstall(define_extension({"name": "versioned"}))
    opened["registry"].install(define_extension({"name": "versioned", "tasks": [later]}))
    inspection = await opened["harness"].inspect(context)
    state = inspection["tasks"][0]["state"]
    assert state["kind"] == "blocked" and state["reason"] == "migration_failed"
    assert isinstance(state["error"], RuntimeError)
    assert opened["reports"] == []
    await opened["harness"].close(context)


async def test_reports_a_held_outcome_as_completing_until_owned_work_drains() -> None:
    gate = deferred()

    async def child_abort(_task: Any, runtime: Any, ctx: Any) -> None:
        await gate.promise
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx
        )

    Child = one_phase(
        "test.inspect-child",
        lambda _task, runtime, ctx: runtime.commit(lambda: None, ctx),
        abort=child_abort,
    )

    async def owner_phase(task: Any, runtime: Any, ctx: Any) -> None:
        if task["state"]["checkpoint"]["phase"] == "spawn":
            async def spawn(tx: Any) -> dict[str, Any]:
                child = await tx.create_task(
                    Child, None, {"ownership": {"kind": "task", "taskId": task["id"]}}
                )
                return {"status": "running", "checkpoint": {"phase": "fail", "child": child}}

            await runtime.commit(spawn, ctx)
        else:
            await runtime.commit(
                lambda: {
                    "status": "terminal",
                    "outcome": {"status": "failed", "error": {"message": "boom"}},
                },
                ctx,
            )

    Owner = define_task(
        {
            "name": "test.inspect-owner",
            "version": 1,
            "initial": lambda _input: {"phase": "spawn"},
            "phases": {"spawn": owner_phase, "fail": owner_phase},
            "abort": lambda _task, _runtime, _context: None,
        }
    )

    opened = await open_tasks(MemoryStorage(), [Owner, Child])
    harness, root = opened["harness"], await opened["harness"].root(context)
    owner = await root.commit(
        lambda tx: tx.create_task(Owner, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()

    async def completing() -> bool:
        return (await wait_states(harness)).get(owner) == {"kind": "completing"}

    await eventually(completing)
    inspection = await harness.inspect(context)
    states = {task["record"]["id"]: task["state"] for task in inspection["tasks"]}
    assert states[owner] == {"kind": "completing"}
    child_id = next(identifier for identifier in states if identifier != owner)
    assert states[child_id]["kind"] != "completing"

    gate.resolve()
    settled = await harness.wait_for_task(owner, context)
    assert settled["state"]["outcome"]["status"] == "failed"
    assert [task["record"]["id"] for task in (await harness.inspect(context))["tasks"]] == []
    await harness.close(context)


async def test_reports_closing_to_an_inspection_queued_before_the_seal() -> None:
    gate = deferred()
    started = deferred()

    async def stubborn(_task: Any, runtime: Any, ctx: Any) -> None:
        started.resolve()
        await gate.promise
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
        )

    Stubborn = one_phase("test.inspect-stubborn", stubborn)
    storage = ControlledStorage()
    opened = await open_tasks(storage, [Stubborn])
    harness, root = opened["harness"], await opened["harness"].root(context)
    await root.commit(
        lambda tx: tx.create_task(Stubborn, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    harness.resume()
    await started.promise

    held = storage.hold_commits()
    committing = asyncio.ensure_future(
        root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    )
    await held.entered.wait()
    queued = asyncio.ensure_future(harness.inspect(context))
    await flush()
    closing = asyncio.ensure_future(harness.close(context))
    try:
        await flush()
        held.release()
        await committing
        inspection = await queued
        assert inspection["scheduling"] == "closing"
        assert inspection["tasks"][0]["state"] == {"kind": "running"}
        with pytest.raises(RuntimeError, match="Harness is closed"):
            await harness.inspect(context)
    finally:
        gate.resolve()
        await closing
    with pytest.raises(RuntimeError, match="Harness is closed"):
        await harness.inspect(context)
async def _state_is(harness: Any, value: str) -> bool:
    try:
        inspection = await harness.inspect(context)
    except RuntimeError:
        return False
    return inspection["scheduling"] == value

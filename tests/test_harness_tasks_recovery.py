"""Translated upstream ``harness-tasks-recovery`` cases for the durable task core."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from session_support import ControlledStorage, context, flush
from task_support import (
    aborted,
    aborted_with,
    completed,
    counting_reader,
    deferred,
    eventually,
    open_tasks,
)

from pi_durable import MemoryStorage
from pi_durable.documents import define_doc
from pi_durable.harness.define import define_extension, define_tool
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.ids import TaskId
from pi_durable.tasks import define_task


@pytest.fixture(autouse=True)
async def _drain_crashed_harnesses() -> Any:
    """Crash simulation abandons a Harness on purpose; cancel what it left running at test end."""

    yield
    import gc

    current = asyncio.current_task()
    for _ in range(10):
        leftover = [task for task in asyncio.all_tasks() if task is not current]
        if not leftover:
            break
        for task in leftover:
            task.cancel()
        await asyncio.gather(*leftover, return_exceptions=True)
        await asyncio.sleep(0)
    gc.collect()


async def _create_in(harness: Harness, task: Any) -> TaskId:
    root = await harness.root(context)
    return await root.commit(
        lambda tx: tx.create_task(task, None, {"ownership": {"kind": "conversation"}}),
        context,
    )


class TransferService:
    """Fake external service whose operations are idempotent by request key."""

    def __init__(self) -> None:
        self.applied: dict[str, int] = {}
        self.calls = 0

    async def apply(self, key: str, amount: int) -> int:
        self.calls += 1
        if key not in self.applied:
            self.applied[key] = amount * 10
        return self.applied[key]


class TransferInterrupt:
    def __init__(self, first: bool) -> None:
        self.first = first


def transfer_task(service: TransferService, interrupt: TransferInterrupt) -> Any:
    async def prepare(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.memo("requested", task["input"]["amount"], ctx)
        await runtime.commit(
            lambda: {
                "status": "running",
                "checkpoint": {"phase": "apply", "key": f"transfer-{task['id']}"},
            },
            ctx,
        )

    async def apply(task: Any, runtime: Any, ctx: Any) -> None:
        receipt = await service.apply(
            task["state"]["checkpoint"]["key"], task["input"]["amount"]
        )
        if interrupt.first:
            interrupt.first = False
            await aborted(runtime.signal)
        await runtime.commit(lambda: completed({"receipt": receipt}), ctx)

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx
        )

    return define_task(
        {
            "name": "test.transfer",
            "version": 1,
            "initial": lambda _input: {"phase": "prepare"},
            "phases": {"prepare": prepare, "apply": apply},
            "abort": abort,
        }
    )


async def test_resumes_an_intent_effect_outcome_task_across_close_and_reopen() -> None:
    storage = ControlledStorage()
    service = TransferService()
    Transfer = transfer_task(service, TransferInterrupt(first=True))

    first = await open_tasks(storage, [Transfer])
    root = await first["harness"].root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Transfer, {"amount": 7}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    first["harness"].resume()
    await eventually(lambda: service.calls == 1)
    await first["harness"].close(context)

    second = await open_tasks(storage.reopen(), [Transfer])
    record = await second["harness"].get_task(id, context)
    assert record is not None
    assert record["abortRequested"] is False
    assert record["memos"] == {"requested": 7}
    assert record["state"] == {
        "status": "pending",
        "checkpoint": {"phase": "apply", "key": f"transfer-{id}"},
    }
    assert service.calls == 1
    second["harness"].resume()
    receipt = await second["harness"].wait_for_task(id, context)
    assert receipt["state"]["outcome"] == {"status": "completed", "result": {"receipt": 70}}
    assert service.calls == 2
    assert len(service.applied) == 1
    await second["harness"].close(context)

    third = await open_tasks(storage.reopen(), [Transfer])
    assert await third["harness"].get_task(id, context) == receipt
    await third["harness"].close(context)


async def test_keeps_started_at_across_reopen_and_stamps_ended_at() -> None:
    storage = ControlledStorage()
    service = TransferService()
    Transfer = transfer_task(service, TransferInterrupt(first=True))
    clock = [2_000]

    first = await open_tasks(storage, [Transfer], {"now": lambda: clock[0]})
    root = await first["harness"].root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Transfer, {"amount": 7}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    first["harness"].resume()
    await eventually(lambda: service.calls == 1)
    clock[0] = 3_000
    await first["harness"].close(context)

    clock[0] = 9_000
    second = await open_tasks(storage.reopen(), [Transfer], {"now": lambda: clock[0]})
    reopened = await second["harness"].get_task(id, context)
    assert reopened is not None
    assert [reopened["state"]["status"], reopened.get("startedAt"), reopened.get("endedAt")] == [
        "pending",
        2_000,
        None,
    ]
    second["harness"].resume()
    receipt = await second["harness"].wait_for_task(id, context)
    assert [receipt.get("startedAt"), receipt.get("endedAt")] == [2_000, 9_000]
    await second["harness"].close(context)


async def test_resumes_abort_work_after_close_at_every_direct_task_abort_stage() -> None:
    storage = ControlledStorage()
    log: list[str] = []
    abort_gate = {"block": True}
    abort_reached = deferred()
    run_release = deferred()

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        log.append("run")
        await run_release.promise

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        log.append("abort")
        if abort_gate["block"]:
            abort_reached.resolve()
            await aborted(runtime.signal)
        await runtime.commit(lambda: aborted_with("stop"), ctx)

    Abortable = define_task(
        {
            "name": "test.abortable",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )

    # Stage 1: the mark is committed while the run invocation is still active.
    opened = await open_tasks(storage, [Abortable])
    id = await _create_in(opened["harness"], Abortable)
    opened["harness"].resume()
    await eventually(lambda: len(log) == 1)
    aborting = asyncio.ensure_future(opened["harness"].abort_task(id, context))
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    while not record["abortRequested"]:
        await flush()
        record = await opened["harness"].get_task(id, context)
        assert record is not None
    closing = opened["harness"].close(context)
    run_release.resolve()
    await closing
    assert await aborting == "marked"
    assert log == ["run"]

    # Stage 2: reopen dispatches the abort invocation, never the run; close while it is active.
    opened = await open_tasks(storage.reopen(), [Abortable])
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["abortRequested"] is True
    assert record["state"]["status"] == "pending"
    opened["harness"].resume()
    await abort_reached.promise
    await opened["harness"].close(context)
    assert log == ["run", "abort"]

    # Stage 3: a fresh abort invocation settles the task.
    abort_gate["block"] = False
    opened = await open_tasks(storage.reopen(), [Abortable])
    opened["harness"].resume()
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "stop",
    }
    await opened["harness"].close(context)
    assert log == ["run", "abort", "abort"]

    # Stage 4: the terminal receipt survives reopen and nothing runs again.
    opened = await open_tasks(storage.reopen(), [Abortable])
    opened["harness"].resume()
    await flush()
    assert await opened["harness"].abort_task(id, context) == "terminal"
    await opened["harness"].close(context)
    assert log == ["run", "abort", "abort"]


# ── Crash recovery ──────────────────────────────────────────────────────────


def crash_task(log: list[str], options: dict[str, bool]) -> Any:
    """A task whose run ignores its signal and whose abort handler blocks while ``block_abort`` is set."""

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        log.append("run")
        await asyncio.Event().wait()

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        log.append("abort")
        if options["block_abort"]:
            await asyncio.Event().wait()
        await runtime.commit(lambda: aborted_with("recovered"), ctx)

    return define_task(
        {
            "name": "test.crash",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )


async def _recover(storage: ControlledStorage, log: list[str]) -> tuple[Harness, TaskId]:
    storage.crash()
    opened = await open_tasks(storage, [crash_task(log, {"block_abort": False})])
    page = await opened["harness"].commit(
        lambda tx: tx.scan_tasks({"kind": "test.crash"}, 1), context
    )
    return opened["harness"], cast(Any, page)["items"][0]["id"]


async def _crashed_run(log: list[str]) -> tuple[ControlledStorage, Harness, TaskId]:
    storage = ControlledStorage()
    task = crash_task(log, {"block_abort": True})
    opened = await open_tasks(storage, [task])
    id = await _create_in(opened["harness"], task)
    opened["harness"].resume()
    await eventually(lambda: len(log) == 1)
    return storage, opened["harness"], id


async def test_crash_while_the_mark_commit_is_in_storage_resumes_the_run() -> None:
    log: list[str] = []
    storage, harness, id = await _crashed_run(log)
    held = storage.hold_commits()
    asyncio.ensure_future(harness.abort_task(id, context))
    await held.entered.wait()
    recovered, _ = await _recover(storage, log)
    record = await recovered.get_task(id, context)
    assert record is not None
    assert record["abortRequested"] is False
    assert record["state"]["status"] == "pending"
    recovered.resume()
    await eventually(lambda: len(log) == 2)
    assert log == ["run", "run"]


async def test_crash_after_the_mark_before_the_run_joins_runs_only_the_abort_handler() -> None:
    log: list[str] = []
    storage, harness, id = await _crashed_run(log)
    joined = {"value": False}

    def done(_: Any) -> None:
        joined["value"] = True

    aborting = asyncio.ensure_future(harness.abort_task(id, context))
    aborting.add_done_callback(done)
    record = await harness.get_task(id, context)
    assert record is not None
    while not record["abortRequested"]:
        await flush()
        record = await harness.get_task(id, context)
        assert record is not None
    await flush()
    assert joined["value"] is False
    recovered, _ = await _recover(storage, log)
    record = await recovered.get_task(id, context)
    assert record is not None
    assert record["abortRequested"] is True
    assert record["state"]["status"] == "pending"
    recovered.resume()
    assert (await recovered.wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "recovered",
    }
    assert log == ["run", "abort"]
    await recovered.close(context)


async def test_crash_while_the_abort_handler_runs_settles_it_on_reopen() -> None:
    log: list[str] = []
    storage = ControlledStorage()
    Crash = crash_task(log, {"block_abort": True})
    opened = await open_tasks(storage, [Crash])
    id = await _create_in(opened["harness"], Crash)
    await opened["harness"].abort_task(id, context)
    opened["harness"].resume()
    await eventually(lambda: len(log) == 1)
    assert log == ["abort"]
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["state"]["status"] == "running"
    recovered, _ = await _recover(storage, log)
    recovered.resume()
    assert (await recovered.wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "recovered",
    }
    assert log == ["abort", "abort"]
    await recovered.close(context)


async def test_crash_while_the_abort_outcome_is_in_storage_runs_the_handler_again() -> None:
    log: list[str] = []
    storage = ControlledStorage()
    reached = deferred()
    proceed = deferred()

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        return None

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        log.append("abort")
        reached.resolve()
        await proceed.promise
        await runtime.commit(lambda: aborted_with("lost"), ctx)

    Crash = define_task(
        {
            "name": "test.crash",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )
    opened = await open_tasks(storage, [Crash])
    id = await _create_in(opened["harness"], Crash)
    await opened["harness"].abort_task(id, context)
    opened["harness"].resume()
    await reached.promise
    held = storage.hold_commits()
    proceed.resolve()
    await held.entered.wait()
    recovered, _ = await _recover(storage, log)
    recovered.resume()
    assert (await recovered.wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "recovered",
    }
    assert log == ["abort", "abort"]
    await recovered.close(context)


async def test_crash_after_the_terminal_outcome_runs_nothing_again() -> None:
    log: list[str] = []
    storage = ControlledStorage()
    Crash = crash_task(log, {"block_abort": False})
    opened = await open_tasks(storage, [Crash])
    id = await _create_in(opened["harness"], Crash)
    await opened["harness"].abort_task(id, context)
    opened["harness"].resume()
    await opened["harness"].wait_for_task(id, context)
    recovered, _ = await _recover(storage, log)
    recovered.resume()
    await flush()
    assert log == ["abort"]
    record = await recovered.get_task(id, context)
    assert record is not None
    assert record["state"] == {
        "status": "terminal",
        "outcome": {"status": "aborted", "reason": "recovered"},
    }
    await recovered.close(context)


async def test_crash_while_the_reservation_commit_is_in_storage_leaves_the_task_pending() -> None:
    log: list[str] = []
    storage = ControlledStorage()
    Crash = crash_task(log, {"block_abort": False})
    opened = await open_tasks(storage, [Crash])
    id = await _create_in(opened["harness"], Crash)
    held = storage.hold_commits()
    opened["harness"].resume()
    await held.entered.wait()
    recovered, _ = await _recover(storage, log)
    record = await recovered.get_task(id, context)
    assert record is not None
    assert record["state"]["status"] == "pending"
    assert log == []
    await recovered.abort_task(id, context)
    recovered.resume()
    await recovered.wait_for_task(id, context)
    assert log == ["abort"]
    await recovered.close(context)


# ── Blocked tasks ───────────────────────────────────────────────────────────


def add_task(registry: Any, task: Any) -> Any:
    extension = define_extension({"name": f"tasks.{task.definition.name}", "tasks": [task]})
    registry.install(extension)
    return extension


def add_tool(registry: Any, tool: Any) -> Any:
    extension = define_extension({"name": f"tools.{tool.name}", "tools": [tool]})
    registry.install(extension)
    return extension


def versioned(
    version: int,
    result: str,
    migrate: Any = None,
    *,
    name: str = "test.versioned",
) -> Any:
    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(result), ctx)

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: aborted_with(result), ctx)

    values: dict[str, Any] = {
        "name": name,
        "version": version,
        "initial": lambda _input: {"phase": "run"},
        "phases": {"run": run},
        "abort": abort,
    }
    if migrate is not None:
        values["migrate"] = migrate
    return define_task(values)


async def test_keeps_a_task_with_a_missing_definition_pending_until_registration() -> None:
    V1 = versioned(1, "v1")
    opened = await open_tasks(MemoryStorage(), [])
    id = await _create_in(opened["harness"], V1)
    opened["harness"].resume()
    await flush()
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["state"]["status"] == "pending"
    idle = asyncio.ensure_future(opened["harness"].wait_for_idle(context))
    await flush()
    add_task(opened["registry"], V1)
    await idle
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["state"] == {
        "status": "terminal",
        "outcome": {"status": "completed", "result": "v1"},
    }
    await opened["harness"].close(context)


async def test_keeps_a_task_stored_by_a_newer_version_pending_until_a_fitting_definition() -> None:
    registry = create_registry()
    add_task(registry, versioned(1, "old"))
    opened = await open_tasks(MemoryStorage(), [], {"registry": registry})
    id = await _create_in(opened["harness"], versioned(2, "new"))
    opened["harness"].resume()
    await flush()
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["state"]["status"] == "pending"
    add_task(registry, versioned(2, "new"))
    receipt = await opened["harness"].wait_for_task(id, context)
    assert receipt["state"]["outcome"] == {"status": "completed", "result": "new"}
    await opened["harness"].close(context)


async def test_migrates_at_reservation_and_retries_only_for_a_new_definition() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    id = await _create_in(opened["harness"], versioned(1, "v1"))
    await opened["harness"].close(context)

    registry = create_registry()
    failures = [0]

    def cannot_migrate(_input: Any, _checkpoint: Any, _from: int) -> Any:
        failures[0] += 1
        raise RuntimeError("cannot migrate")

    add_task(registry, versioned(2, "v2", cannot_migrate))
    opened = await open_tasks(storage.reopen(), [], {"registry": registry})
    opened["harness"].resume()
    await eventually(lambda: len(opened["reports"]) == 1)
    # An unrelated registry change wakes the scheduler without retrying the failed definition.
    add_tool(
        registry,
        define_tool({"name": "test.unrelated", "description": "unrelated", "execute": lambda: {}}),
    )
    await flush()
    assert failures[0] == 1
    assert len(opened["reports"]) == 1
    assert "cannot migrate" in str(opened["reports"][0])
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["version"] == 1
    assert record["state"] == {"status": "pending", "checkpoint": {"phase": "run"}}

    migrations: list[int] = []

    def migrate(_input: Any, checkpoint: Any, from_version: int) -> Any:
        migrations.append(from_version)
        return {"input": None, "checkpoint": checkpoint}

    add_task(registry, versioned(2, "v2", migrate))
    receipt = await opened["harness"].wait_for_task(id, context)
    assert receipt["version"] == 2
    assert receipt["state"]["outcome"] == {"status": "completed", "result": "v2"}
    assert migrations == [1]
    await opened["harness"].close(context)


async def test_blocks_an_older_record_whose_newer_definition_has_no_migration() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    id = await _create_in(opened["harness"], versioned(1, "v1"))
    await opened["harness"].close(context)
    opened = await open_tasks(storage.reopen(), [versioned(2, "v2")])
    opened["harness"].resume()
    await eventually(lambda: len(opened["reports"]) == 1)
    assert "has no migration from 1" in str(opened["reports"][0])
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["version"] == 1
    assert record["state"]["status"] == "pending"
    await opened["harness"].close(context)


async def test_settles_an_aborted_blocked_task_as_orphaned_and_retires_its_documents() -> None:
    Scratch = define_doc(
        {
            "kind": "test.orphan-scratch",
            "version": 1,
            "scope": "task",
            "initial": lambda: {"n": 0},
        }
    )
    opened = await open_tasks(MemoryStorage(), [])
    root = await opened["harness"].root(context)

    async def create(tx: Any) -> Any:
        created = await tx.create_task(
            versioned(1, "x"), None, {"ownership": {"kind": "conversation"}}
        )
        (await tx.doc(Scratch, created))["n"] = 1
        return created

    id = await root.commit(create, context)
    # Before resume: the marking commit settles the blocked task directly.
    assert await opened["harness"].abort_task(id, context) == "marked"
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["state"] == {
        "status": "terminal",
        "outcome": {"status": "orphaned", "reason": "missing_task"},
    }
    assert await opened["harness"].snapshot(Scratch, id, context) is None
    await opened["harness"].close(context)


async def test_orphans_a_marked_task_whose_definition_disappeared_while_its_run_was_active() -> None:
    reached = deferred()

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        reached.resolve()
        await aborted(runtime.signal)

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        raise RuntimeError("must not run")

    Running = define_task(
        {
            "name": "test.vanishing",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )
    registry = create_registry()
    registration = add_task(registry, Running)
    opened = await open_tasks(MemoryStorage(), [], {"registry": registry})
    id = await _create_in(opened["harness"], Running)
    opened["harness"].resume()
    await reached.promise
    registry.uninstall(registration)
    # An active run means the mark does not orphan directly; the scheduler orphans once the run has ended.
    assert await opened["harness"].abort_task(id, context) == "marked"
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "orphaned",
        "reason": "missing_task",
    }
    await opened["harness"].close(context)


async def test_orphans_a_reopened_abort_marked_task_without_a_definition_once_scheduling_resumes() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [versioned(1, "x")])
    id = await _create_in(opened["harness"], versioned(1, "x"))
    assert await opened["harness"].abort_task(id, context) == "marked"
    await opened["harness"].close(context)

    opened = await open_tasks(storage.reopen(), [])
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["abortRequested"] is True
    assert record["state"]["status"] == "pending"
    idle = asyncio.ensure_future(opened["harness"].wait_for_idle(context))
    opened["harness"].resume()
    await idle
    record = await opened["harness"].get_task(id, context)
    assert record is not None
    assert record["state"] == {
        "status": "terminal",
        "outcome": {"status": "orphaned", "reason": "missing_task"},
    }
    await opened["harness"].close(context)


# ── Definition handover ─────────────────────────────────────────────────────


def handover_task(
    label: str,
    version: int,
    log: list[str],
    options: dict[str, Any] | None = None,
) -> Any:
    values = options or {}
    gates = values.get("gates") or {}

    def advance(phase: str, following: dict[str, Any]) -> Any:
        async def handler(task: Any, runtime: Any, ctx: Any) -> None:
            log.append(f"{label}:{phase} start")
            gate = gates.get(phase)
            if gate is not None:
                await gate
            await runtime.commit(
                lambda: {"status": "running", "checkpoint": following}, ctx
            )
            # Leave room for a wrongly dispatched successor before this invocation ends.
            await flush()
            on_end = gates.get("on_end")
            if on_end is not None:
                on_end()
            log.append(f"{label}:{phase} end")

        return handler

    async def final(task: Any, runtime: Any, ctx: Any) -> None:
        log.append(f"{label}:c")
        await runtime.commit(lambda: completed(None), ctx)

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        log.append(f"{label}:abort")
        await runtime.commit(lambda: aborted_with(label), ctx)

    definition: dict[str, Any] = {
        "name": "test.handover",
        "version": version,
        "initial": lambda _input: {"phase": "a"},
        "phases": {
            "a": advance("a", {"phase": "b"}),
            "b": advance("b", {"phase": "c"}),
            "c": final,
        },
        "abort": abort,
    }
    if values.get("migrate") is not None:
        definition["migrate"] = values["migrate"]
    return define_task(definition)


async def start_handover(
    log: list[str], gates: dict[str, Any], storage: Any = None
) -> dict[str, Any]:
    registry = create_registry()
    old = add_task(registry, handover_task("old", 1, log, {"gates": gates}))
    opened = await open_tasks(storage or MemoryStorage(), [], {"registry": registry})
    id = await _create_in(opened["harness"], handover_task("old", 1, log))
    opened["harness"].resume()
    await eventually(lambda: len(log) == 1)
    return {**opened, "old": old, "id": id}


async def test_hands_over_at_the_next_phase_boundary_without_overlap() -> None:
    log: list[str] = []
    gate = deferred()
    opened = await start_handover(log, {"a": gate.promise})
    add_task(opened["registry"], handover_task("new", 1, log))
    gate.resolve()
    await opened["harness"].wait_for_task(opened["id"], context)
    assert log == ["old:a start", "old:a end", "new:b start", "new:b end", "new:c"]
    await opened["harness"].close(context)


async def test_keeps_the_memos_across_a_handover() -> None:
    log: list[str] = []
    gate = deferred()
    reached = deferred()

    def memo_task(label: str) -> Any:
        async def a(task: Any, runtime: Any, ctx: Any) -> None:
            await runtime.memo("picked", label, ctx)
            reached.resolve()
            await gate.promise
            await runtime.commit(
                lambda: {"status": "running", "checkpoint": {"phase": "b"}}, ctx
            )

        async def b(task: Any, runtime: Any, ctx: Any) -> None:
            picked = await runtime.memo("picked", label, ctx)
            log.append(f"{label}:b {picked}")
            await runtime.commit(lambda: completed(picked), ctx)

        return define_task(
            {
                "name": "test.handover-memo",
                "version": 1,
                "initial": lambda _input: {"phase": "a"},
                "phases": {"a": a, "b": b},
                "abort": lambda task, runtime, ctx: None,
            }
        )

    registry = create_registry()
    add_task(registry, memo_task("old"))
    opened = await open_tasks(MemoryStorage(), [], {"registry": registry})
    id = await _create_in(opened["harness"], memo_task("old"))
    opened["harness"].resume()
    await reached.promise
    add_task(registry, memo_task("new"))
    gate.resolve()
    receipt = await opened["harness"].wait_for_task(id, context)
    assert receipt["state"]["outcome"] == {"status": "completed", "result": "old"}
    assert log == ["new:b old"]
    await opened["harness"].close(context)


async def test_hands_over_to_a_newer_version_with_a_migration() -> None:
    log: list[str] = []
    gate = deferred()
    opened = await start_handover(log, {"a": gate.promise})
    add_task(
        opened["registry"],
        handover_task(
            "v2",
            2,
            log,
            {"migrate": lambda _input, _checkpoint, _from: {
                "input": None,
                "checkpoint": {"phase": "c"},
            }},
        ),
    )
    gate.resolve()
    receipt = await opened["harness"].wait_for_task(opened["id"], context)
    assert receipt["version"] == 2
    assert log == ["old:a start", "old:a end", "v2:c"]
    await opened["harness"].close(context)


async def test_hands_over_to_a_newer_version_whose_migration_fails_and_stays_blocked() -> None:
    log: list[str] = []
    gate = deferred()
    opened = await start_handover(log, {"a": gate.promise})

    def broken(_input: Any, _checkpoint: Any, _from: int) -> Any:
        raise RuntimeError("broken migration")

    add_task(
        opened["registry"],
        handover_task("broken", 2, log, {"migrate": broken}),
    )
    gate.resolve()
    await eventually(lambda: len(opened["reports"]) == 1)
    await flush()
    record = await opened["harness"].get_task(opened["id"], context)
    assert record is not None
    assert record["version"] == 1
    assert record["state"] == {"status": "pending", "checkpoint": {"phase": "b"}}
    assert log == ["old:a start", "old:a end"]
    await opened["harness"].close(context)


async def test_keeps_running_under_the_old_definition_when_the_replacement_cannot_take_it() -> None:
    log: list[str] = []
    gate_a = deferred()
    gate_b = deferred()
    opened = await start_handover(log, {"a": gate_a.promise, "b": gate_b.promise})
    opened["registry"].uninstall(opened["old"])
    gate_a.resolve()
    await eventually(lambda: "old:b start" in log)
    # A newer definition without a migration cannot take the task either.
    add_task(opened["registry"], handover_task("incompatible", 2, log))
    gate_b.resolve()
    await opened["harness"].wait_for_task(opened["id"], context)
    assert log == ["old:a start", "old:a end", "old:b start", "old:b end", "old:c"]
    assert [getattr(report, "cause", None) for report in opened["reports"]] == [
        "missing_task",
        "incompatible_task",
    ]
    await opened["harness"].close(context)


async def test_rejects_a_runtime_commit_queued_behind_its_handover_commit() -> None:
    log: list[str] = []
    gate = deferred()
    storage = ControlledStorage()
    state: dict[str, Any] = {}
    registry = create_registry()

    async def old_a(task: Any, runtime: Any, ctx: Any) -> None:
        state["runtime"] = runtime
        await gate.promise
        await runtime.commit(
            lambda: {"status": "running", "checkpoint": {"phase": "b"}}, ctx
        )
        # Hold the line with an unrelated commit so the handover commit queues behind it.
        state["held"] = storage.hold_commits()
        asyncio.ensure_future(
            state["harness"].commit(
                lambda tx: tx.append_entry(task["conversationId"], {"kind": "blocker"}),
                ctx,
            )
        )

    Old = define_task(
        {
            "name": "test.handover",
            "version": 1,
            "initial": lambda _input: {"phase": "a"},
            "phases": {
                "a": old_a,
                "b": lambda task, runtime, ctx: None,
                "c": lambda task, runtime, ctx: None,
            },
            "abort": lambda task, runtime, ctx: None,
        }
    )
    add_task(registry, Old)
    opened = await open_tasks(storage, [], {"registry": registry})
    state["harness"] = opened["harness"]
    id = await _create_in(opened["harness"], Old)
    opened["harness"].resume()
    await eventually(lambda: state.get("runtime") is not None)
    add_task(registry, handover_task("new", 1, log))
    gate.resolve()
    await eventually(lambda: state.get("held") is not None)
    await state["held"].entered.wait()
    await flush()
    # Queued behind the handover commit while the invocation has not ended yet.
    late = state["runtime"].commit(lambda: completed(None), context)
    state["held"].release()
    error = await _capture(late)
    assert "invocation has ended" in str(error)
    await opened["harness"].wait_for_task(id, context)
    assert log == ["new:b start", "new:b end", "new:c"]
    await opened["harness"].close(context)


async def test_preserves_an_abort_mark_that_races_the_handover_commit() -> None:
    log: list[str] = []
    gate = deferred()
    storage = ControlledStorage()
    state: dict[str, Any] = {}

    def on_end() -> None:
        if "held" not in state:
            state["held"] = storage.hold_commits()

    opened = await start_handover(
        log, {"a": gate.promise, "on_end": on_end}, storage
    )
    add_task(opened["registry"], handover_task("new", 1, log))
    gate.resolve()
    await eventually(lambda: state.get("held") is not None)
    await state["held"].entered.wait()
    aborting = asyncio.ensure_future(opened["harness"].abort_task(opened["id"], context))
    state["held"].release()
    assert await aborting == "marked"
    assert (await opened["harness"].wait_for_task(opened["id"], context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "new",
    }
    states: list[str] = []
    for writes in storage.commits:
        for write in writes:
            if write["type"] == "task" and write["value"]["id"] == opened["id"]:
                value = write["value"]
                states.append(
                    f"{value['state']['status']}"
                    f"{'+mark' if value.get('abortRequested') else ''}"
                )
    # created, reserved, progress, handover, mark, abort reservation, aborted
    assert states == [
        "pending",
        "running",
        "running",
        "pending",
        "pending+mark",
        "running+mark",
        "terminal+mark",
    ]
    assert log == ["old:a start", "old:a end", "new:abort"]
    await opened["harness"].close(context)


async def _capture(awaitable: Any) -> BaseException:
    try:
        await awaitable
    except BaseException as error:
        return error
    raise AssertionError("expected a rejection")


# ── Harness open ────────────────────────────────────────────────────────────


async def test_open_releases_its_registry_subscription_and_closes_the_session_on_failure() -> None:
    storage = ControlledStorage()
    log: list[str] = []

    async def stuck_run(task: Any, runtime: Any, ctx: Any) -> None:
        log.append("run")
        await asyncio.Event().wait()

    Stuck = define_task(
        {
            "name": "test.stuck",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": stuck_run},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    # Leave a running task behind so open has a reconciliation commit to fail.
    first = await open_tasks(storage, [Stuck])
    await _create_in(first["harness"], Stuck)
    first["harness"].resume()
    await eventually(lambda: len(log) == 1)

    registry = create_registry()
    reader, subscriptions = counting_reader(registry)
    storage.fail_next_commit(RuntimeError("disk full"))
    error = await _capture(
        Harness.open(
            storage,
            {"models": {}, "registry": reader},
            context,
        )
    )
    assert "disk full" in str(error)
    assert subscriptions() == 0
    task_error = await _capture(storage.task(cast(TaskId, 1), context))
    assert isinstance(task_error, BaseException)


async def test_abandons_a_flagged_task_and_its_owned_work_before_any_phase_runs() -> None:
    storage = ControlledStorage()
    ran: list[str] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        ran.append(task["kind"])
        await runtime.commit(lambda: completed(None), ctx)

    async def abort(name: str) -> Any:
        async def handler(task: Any, runtime: Any, ctx: Any) -> None:
            await runtime.commit(lambda: aborted_with(name), ctx)

        return handler

    Parent = define_task(
        {
            "name": "test.abandon-parent",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": await abort("parent"),
        }
    )
    Child = define_task(
        {
            "name": "test.abandon-child",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": await abort("child"),
        }
    )

    first = await open_tasks(storage, [Parent, Child])
    root = await first["harness"].root(context)

    async def create(tx: Any) -> dict[str, Any]:
        parent = await tx.create_task(
            Parent,
            None,
            {"ownership": {"kind": "conversation"}, "abandonOnRestart": True},
        )
        child = await tx.create_task(
            Child, None, {"ownership": {"kind": "task", "taskId": parent}}
        )
        return {"parent": parent, "child": child}

    ids = await root.commit(create, context)
    await first["harness"].close(context)

    second = await open_tasks(storage.reopen(), [Parent, Child])
    # Opening alone marks nothing: the abandonment applies once scheduling resumes.
    pending = await second["harness"].get_task(ids["parent"], context)
    assert pending is not None
    assert pending.get("abortReason") is None
    assert pending["state"]["status"] == "pending"
    assert ran == []
    second["harness"].resume()
    parent = await second["harness"].wait_for_task(ids["parent"], context)
    child = await second["harness"].wait_for_task(ids["child"], context)
    assert ran == []
    assert parent["abortReason"] == "restart"
    assert parent["state"]["outcome"] == {"status": "aborted", "reason": "parent"}
    assert child["abortReason"] == "restart"
    assert child["state"]["outcome"] == {"status": "aborted", "reason": "child"}
    await second["harness"].close(context)

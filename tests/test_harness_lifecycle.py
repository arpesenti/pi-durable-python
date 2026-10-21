"""Translated upstream ``harness-lifecycle`` cases for the durable task core."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import ControlledStorage, context
from task_support import (
    completed,
    counting_reader,
    deferred,
    eventually,
    open_tasks,
    settled,
    with_signal,
)

from pi_durable import MemoryStorage
from pi_durable.context import AbortController
from pi_durable.harness.define import define_extension
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.ids import ConversationId, TaskId
from pi_durable.tasks import define_task


def one_step(name: str, run: Any = None) -> Any:
    async def handler(task: Any, runtime: Any, ctx: Any) -> None:
        if run is not None:
            await run()
        await runtime.commit(lambda: completed(None), ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": handler},
            "abort": lambda task, runtime, ctx: asyncio.sleep(0),
        }
    )


async def start(conversation: Any, task: Any, background: bool = False) -> TaskId:
    return await conversation.commit(
        lambda tx: tx.create_task(
            task, None, {"ownership": {"kind": "conversation"}, "background": background}
        ),
        context,
    )


async def storage_open(storage: Any) -> bool:
    try:
        await storage.task(TaskId(1), context)
        return True
    except Exception:
        return False


async def seed_running_task(storage: Any) -> None:
    conversation_id = ConversationId(await storage.mint_id())
    id = TaskId(await storage.mint_id())
    await storage.commit(
        [
            {"type": "conversation", "value": {"id": conversation_id}},
            {
                "type": "task",
                "value": {
                    "id": id,
                    "conversationId": conversation_id,
                    "kind": "test.seeded",
                    "version": 1,
                    "input": None,
                    "background": False,
                    "abortRequested": False,
                    "state": {"status": "running", "checkpoint": {"phase": "run"}},
                },
            },
        ],
        context,
    )


async def test_open_closes_without_a_cancelled_caller_context_and_rethrows() -> None:
    storage = ControlledStorage()
    await seed_running_task(storage)
    reader, subscriptions = counting_reader(create_registry())
    held = storage.hold_commits()
    storage.fail_next_commit(RuntimeError("disk full"))
    controller = AbortController()
    opening = asyncio.ensure_future(
        Harness.open(
            storage,
            {"models": {}, "registry": reader},
            with_signal(controller.signal),
        )
    )
    await held.entered.wait()
    controller.abort(RuntimeError("caller gave up"))
    held.release()
    with pytest.raises(Exception, match="disk full"):
        await opening
    assert subscriptions() == 0
    assert await storage_open(storage) is False


async def test_rethrows_the_open_error_and_reports_it_once_though_close_fails() -> None:
    failure = RuntimeError("disk full")
    reports: list[BaseException] = []

    class FailingClose(ControlledStorage):
        async def close(self, close_context: Any) -> None:
            await super().close(close_context)
            raise RuntimeError("close failed")

    storage = FailingClose()
    await seed_running_task(storage)
    storage.fail_next_commit(failure)
    opening = asyncio.ensure_future(
        Harness.open(
            storage,
            {"models": {}, "registry": create_registry(), "on_report": reports.append},
            context,
        )
    )
    with pytest.raises(Exception, match="disk full"):
        await opening
    assert reports == [failure]


async def test_joins_a_task_handler_that_ignores_its_signal_before_closing_storage() -> None:
    storage = MemoryStorage()
    reached = deferred()
    gate = deferred()
    state: dict[str, Any] = {}

    async def run() -> None:
        reached.resolve()
        await gate.promise
        state["read_after_release"] = await storage_open(storage)

    Stubborn = one_step("test.close-stubborn", run)
    opened = await open_tasks(storage, [Stubborn])
    await start(await opened["harness"].root(context), Stubborn)
    opened["harness"].resume()
    await reached.promise
    closing = asyncio.ensure_future(opened["harness"].close(context))
    assert await settled(closing) is False
    assert await storage_open(storage) is True
    gate.resolve()
    await closing
    assert state["read_after_release"] is True
    assert await storage_open(storage) is False


async def test_lets_a_new_harness_open_the_same_storage_once_close_resolved() -> None:
    log: list[str] = []
    gate = deferred()
    state = {"generation": 1}

    async def run() -> None:
        mine = state["generation"]
        log.append(f"start {mine}")
        if mine == 1:
            await gate.promise
        log.append(f"end {mine}")

    Stubborn = one_step("test.generations", run)
    storage = ControlledStorage()
    first = await open_tasks(storage, [Stubborn])
    id = await start(await first["harness"].root(context), Stubborn)
    first["harness"].resume()
    await eventually(lambda: len(log) == 1)
    closing = asyncio.ensure_future(first["harness"].close(context))
    closing.add_done_callback(lambda _: log.append("closed"))
    assert await settled(closing) is False
    gate.resolve()
    await closing
    assert log == ["start 1", "end 1", "closed"]
    state["generation"] = 2
    second = await open_tasks(storage.reopen(), [Stubborn])
    second["harness"].resume()
    await second["harness"].wait_for_task(id, context)
    assert log == ["start 1", "end 1", "closed", "start 2", "end 2"]
    await second["harness"].close(context)


async def test_keeps_shutting_down_after_a_cancelled_close_and_a_second_close_shares_it() -> None:
    storage = MemoryStorage()
    reached = deferred()
    gate = deferred()

    async def run() -> None:
        reached.resolve()
        await gate.promise

    Stubborn = one_step("test.close-cancelled", run)
    opened = await open_tasks(storage, [Stubborn])
    await start(await opened["harness"].root(context), Stubborn)
    opened["harness"].resume()
    await reached.promise
    controller = AbortController()
    cancelled = asyncio.ensure_future(opened["harness"].close(with_signal(controller.signal)))
    controller.abort(RuntimeError("stop waiting"))
    with pytest.raises(Exception, match="stop waiting"):
        await cancelled
    with pytest.raises(Exception, match="closed"):
        await opened["harness"].commit(lambda: None, context)
    assert await storage_open(storage) is True
    second = asyncio.ensure_future(opened["harness"].close(context))
    assert await settled(second) is False
    gate.resolve()
    await second
    assert await storage_open(storage) is False


async def test_settles_durably_a_commit_whose_committer_was_cancelled_in_storage() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    root = await opened["harness"].root(context)
    held = storage.hold_commits()
    controller = AbortController()
    committing = asyncio.ensure_future(
        root.commit(
            lambda tx: tx.append_entry(root.id, {"kind": "note"}),
            with_signal(controller.signal),
        )
    )
    await held.entered.wait()
    controller.abort(RuntimeError("committer gave up"))
    held.release()
    entry = await committing
    page = await root.entries({}, 10, None, context)
    assert [item["id"] for item in page["items"]] == [entry["id"]]
    await opened["harness"].close(context)


async def test_runs_what_the_registry_holds_at_resume() -> None:
    log: list[str] = []

    def define(label: str) -> Any:
        async def run() -> None:
            log.append(label)

        return one_step("test.late-definition", run)

    storage = MemoryStorage()
    first = await open_tasks(storage, [])
    root = await first["harness"].root(context)
    missing = await start(root, define("unused"))
    inspection = await first["harness"].inspect(context)
    assert inspection["tasks"][0]["state"] == {"kind": "blocked", "reason": "missing_task"}
    installed = define("v1")
    first["registry"].install(define_extension({"name": "late", "tasks": [installed]}))
    inspection = await first["harness"].inspect(context)
    assert inspection["tasks"][0]["state"] == {"kind": "ready", "migrates": False}
    first["registry"].uninstall(define_extension({"name": "late"}))
    replacement = define("v2")
    first["registry"].install(define_extension({"name": "late", "tasks": [replacement]}))
    first["harness"].resume()
    await first["harness"].wait_for_task(missing, context)
    assert log == ["v2"]
    await first["harness"].close(context)

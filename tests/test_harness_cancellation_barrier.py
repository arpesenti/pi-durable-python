"""Translated upstream ``harness-cancellation-barrier`` cases."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from session_support import context, flush
from task_support import aborted, deferred, eventually, open_tasks

from pi_durable import MemoryStorage
from pi_durable.session.session import create_session
from pi_durable.tasks import define_task


async def dormant_run(task: Any, runtime: Any, ctx: Any) -> None:
    await aborted(runtime.signal)


async def dormant_abort(task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)


Dormant = define_task(
    {
        "name": "test.dormant",
        "version": 1,
        "initial": lambda _input: {"phase": "work"},
        "phases": {"work": dormant_run},
        "abort": dormant_abort,
    }
)


class HoldMarks(MemoryStorage):
    """Holds every commit that abort-marks ``target`` until released."""

    def __init__(self) -> None:
        super().__init__()
        self.target: int | None = None
        self.held = deferred()
        self.release = deferred()

    async def commit(self, writes: Any, commit_context: Any) -> Any:
        if any(
            write["type"] == "task"
            and write["value"]["id"] == self.target
            and write["value"].get("abortRequested")
            for write in writes
        ):
            self.held.resolve()
            await self.release.promise
        return await super().commit(writes, commit_context)


async def test_does_not_start_the_next_phase_while_the_cascade_has_not_committed() -> None:
    storage = HoldMarks()
    entered = deferred()
    advance = deferred()
    state = {"ran_next": False}

    async def first(task: Any, runtime: Any, ctx: Any) -> None:
        entered.resolve()
        await advance.promise
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "next"}}, ctx)

    async def next_phase(task: Any, runtime: Any, ctx: Any) -> None:
        state["ran_next"] = True
        await aborted(runtime.signal)

    Child = define_task(
        {
            "name": "test.child",
            "version": 1,
            "initial": lambda _input: {"phase": "first"},
            "phases": {"first": first, "next": next_phase},
            "abort": dormant_abort,
        }
    )
    opened = await open_tasks(storage, [Dormant, Child])
    root = await opened["harness"].root(context)
    parent = await root.commit(
        lambda tx: tx.create_task(Dormant, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    child = await root.commit(
        lambda tx: tx.create_task(Child, None, {"ownership": {"kind": "task", "taskId": parent}}),
        context,
    )
    storage.target = child
    try:
        opened["harness"].resume()
        await entered.promise
        aborting = asyncio.ensure_future(opened["harness"].abort_task(parent, context))
        await storage.held.promise
        advance.resolve()
        for _ in range(5):
            await flush()
        assert state["ran_next"] is False
        storage.release.resolve()
        assert (await opened["harness"].wait_for_task(child, context))["state"]["outcome"][
            "status"
        ] == "aborted"
        await aborting
        assert state["ran_next"] is False
    finally:
        advance.resolve()
        storage.release.resolve()
        await opened["harness"].close(context)


async def test_lets_a_fail_fast_request_in_the_same_pass_override_a_restart_mark() -> None:
    storage = MemoryStorage()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
    )
    parent = await session.commit(
        lambda tx: tx.create_task(
            Dormant,
            None,
            {"conversationId": conversation["id"], "ownership": {"kind": "conversation"}},
        ),
        context,
    )
    failed = await session.commit(
        lambda tx: tx.create_task(Dormant, None, {"ownership": {"kind": "task", "taskId": parent}}),
        context,
    )
    child = await session.commit(
        lambda tx: tx.create_task(Dormant, None, {"ownership": {"kind": "task", "taskId": parent}}),
        context,
    )
    owner = await storage.task(parent, context)
    failed_record = await storage.task(failed, context)
    assert owner is not None and failed_record is not None
    waiting = {
        "status": "waiting",
        "checkpoint": {"phase": "work"},
        "on": [failed, child],
        "policy": "failFast",
    }
    await storage.commit(
        cast(
            Any,
            [
                {
                    "type": "task",
                    "value": {
                        **owner,
                        "abortRequested": True,
                        "abortReason": "restart",
                        "state": waiting,
                    },
                },
                {
                    "type": "task",
                    "value": {
                        **failed_record,
                        "state": {
                            "status": "terminal",
                            "outcome": {"status": "failed", "error": {"message": "failed"}},
                        },
                    },
                },
            ],
        ),
        context,
    )
    opened = await open_tasks(storage, [])
    try:
        opened["harness"].resume()
        await eventually(lambda: _is_terminal(opened["harness"], child))
        record = await opened["harness"].get_task(child, context)
        assert record["state"] == {
            "status": "terminal",
            "outcome": {"status": "orphaned", "reason": "missing_task"},
        }
    finally:
        await opened["harness"].close(context)


async def _is_terminal(harness: Any, id: int) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == "terminal"


async def test_upgrades_restart_marks_down_the_tree_when_an_abort_is_requested_above() -> None:
    storage = MemoryStorage()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
    )
    top = await session.commit(
        lambda tx: tx.create_task(
            Dormant,
            None,
            {"conversationId": conversation["id"], "ownership": {"kind": "conversation"}},
        ),
        context,
    )
    middle = await session.commit(
        lambda tx: tx.create_task(
            Dormant,
            None,
            {"ownership": {"kind": "task", "taskId": top}, "abandonOnRestart": True},
        ),
        context,
    )
    Missing = define_task(
        {
            "name": "test.missing",
            "version": 1,
            "initial": Dormant.definition.initial,
            "phases": {"work": dormant_run},
            "abort": dormant_abort,
        }
    )
    leaf = await session.commit(
        lambda tx: tx.create_task(Missing, None, {"ownership": {"kind": "task", "taskId": middle}}),
        context,
    )
    opened = await open_tasks(storage, [Dormant])
    try:
        opened["harness"].resume()
        await eventually(lambda: _reason_is(opened["harness"], leaf, "restart"))
        await eventually(lambda: _blocked(opened["harness"], leaf))
        record = await opened["harness"].get_task(leaf, context)
        assert record["state"]["status"] != "terminal"
        await opened["harness"].abort_task(top, context)
        expected = [(leaf, "orphaned"), (middle, "aborted"), (top, "aborted")]
        for id, status in expected:
            assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"][
                "status"
            ] == status
    finally:
        await opened["harness"].close(context)


async def _reason_is(harness: Any, id: int, reason: str) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record.get("abortReason") == reason


async def _blocked(harness: Any, id: int) -> bool:
    inspection = await harness.inspect(context)
    for task in inspection["tasks"]:
        if task["record"]["id"] == id:
            return task["state"]["kind"] == "blocked"
    return False


async def test_marks_a_flagged_task_that_holds_its_outcome_and_keeps_it() -> None:
    storage = MemoryStorage()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
    )
    holder = await session.commit(
        lambda tx: tx.create_task(
            Dormant,
            None,
            {
                "conversationId": conversation["id"],
                "ownership": {"kind": "conversation"},
                "abandonOnRestart": True,
            },
        ),
        context,
    )
    child = await session.commit(
        lambda tx: tx.create_task(Dormant, None, {"ownership": {"kind": "task", "taskId": holder}}),
        context,
    )
    record = await storage.task(holder, context)
    assert record is not None
    await storage.commit(
        cast(
            Any,
            [
                {
                    "type": "task",
                    "value": {
                        **record,
                        "state": {
                            "status": "completing",
                            "outcome": {"status": "completed", "result": None},
                        },
                    },
                }
            ],
        ),
        context,
    )
    opened = await open_tasks(storage, [Dormant])
    try:
        opened["harness"].resume()
        assert (await opened["harness"].wait_for_task(child, context))["state"]["outcome"][
            "status"
        ] == "aborted"
        settled = await opened["harness"].wait_for_task(holder, context)
        assert settled["abortReason"] == "restart"
        assert settled["state"]["outcome"]["status"] == "completed"
    finally:
        await opened["harness"].close(context)

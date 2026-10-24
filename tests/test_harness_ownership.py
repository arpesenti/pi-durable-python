"""Translated upstream ``harness-ownership`` cases for the durable task core.

Also translates the "owned conversations from tools and supervisors" cases: invocation-bound
handles, a queued handle operation that loses its invocation, a subagent run aborted with its
parent, and a running tool aborting its owned child.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import ControlledStorage, context
from task_support import (
    Deferred,
    aborted,
    deferred,
    eventually,
    open_tasks,
    settled,
    with_signal,
)

from pi_durable import LiveDoc, MemoryStorage
from pi_durable.context import AbortController, AbortError
from pi_durable.harness.util import cancellation_error
from pi_durable.ids import TaskId
from pi_durable.tasks import define_task

GATES: dict[str, Deferred] = {}
RUNS: dict[str, int] = {}


def gate(name: str) -> Deferred:
    if name not in GATES:
        GATES[name] = deferred()
    return GATES[name]


def open_gate(name: str, ending: str) -> None:
    gate(name).resolve(ending)


async def _race_gate(waiter: asyncio.Future[Any], signal: Any) -> Any:
    stopped = asyncio.ensure_future(signal.wait())
    done, _ = await asyncio.wait((waiter, stopped), return_when=asyncio.FIRST_COMPLETED)
    if stopped in done:
        stopped.result()
        raise _HoldCancelled()
    stopped.cancel()
    return waiter.result()


class _HoldCancelled(Exception):
    pass


async def hold_run(task: Any, runtime: Any, ctx: Any) -> None:
    name = task["input"]["name"]
    RUNS[name] = RUNS.get(name, 0) + 1
    try:
        ending = await _race_gate(gate(name).promise, runtime.signal)
    except _HoldCancelled:
        return
    outcome: dict[str, Any]
    if ending == "completed":
        outcome = {"status": "completed", "result": None}
    else:
        outcome = {"status": "failed", "error": {"message": "gate failed"}}
    await runtime.commit(lambda: {"status": "terminal", "outcome": outcome}, ctx)


async def hold_abort(task: Any, runtime: Any, ctx: Any) -> None:
    if task["input"].get("slowAbort") is True:
        await gate(f"abort.{task['input']['name']}").promise
    await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)


Hold = define_task(
    {
        "name": "test.hold",
        "version": 1,
        "initial": lambda _input: {"phase": "hold"},
        "phases": {"hold": hold_run},
        "abort": hold_abort,
    }
)


async def waiter_run(task: Any, runtime: Any, ctx: Any) -> None:
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
            lambda: {"status": "terminal", "outcome": {"status": "completed", "result": None}}, ctx
        )


Waiter = define_task(
    {
        "name": "test.waiter",
        "version": 1,
        "initial": lambda _input: {"phase": "wait"},
        "phases": {"wait": waiter_run, "done": waiter_run},
        "abort": lambda task, runtime, ctx: runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx
        ),
    }
)


async def spawner_run(task: Any, runtime: Any, ctx: Any) -> None:
    async def change(tx: Any) -> Any:
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": task["id"]}})
        await tx.create_task(
            Hold,
            {"name": task["input"]["name"]},
            {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
        )
        return {"status": "terminal", "outcome": {"status": "completed", "result": None}}

    await runtime.commit(change, ctx)


Spawner = define_task(
    {
        "name": "test.spawner",
        "version": 1,
        "initial": lambda _input: {"phase": "spawn"},
        "phases": {"spawn": spawner_run},
        "abort": lambda task, runtime, ctx: runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx
        ),
    }
)

Unregistered = define_task(
    {
        "name": "test.unregistered",
        "version": 1,
        "initial": lambda _input: {"phase": "hold"},
        "phases": {"hold": lambda task, runtime, ctx: asyncio.sleep(0)},
        "abort": lambda task, runtime, ctx: asyncio.sleep(0),
    }
)


@pytest.fixture(autouse=True)
def clear_gates() -> Any:
    GATES.clear()
    RUNS.clear()
    yield
    GATES.clear()
    RUNS.clear()


async def open_harness(storage: Any = None) -> dict[str, Any]:
    opened = await open_tasks(storage or MemoryStorage(), [Hold, Waiter, Spawner])
    opened["root"] = await opened["harness"].root(context)
    opened["harness"].resume()
    return opened


async def status(harness: Any, id: TaskId) -> dict[str, Any]:
    record = await harness.get_task(id, context)
    assert record is not None
    return record["state"]


async def wait_until(check: Any) -> None:
    await eventually(check, timeout=5)


async def owned_child(
    parent: Any,
    name: str,
    *,
    background: bool = False,
    slow_inner: bool = False,
    slow_owner: bool = False,
) -> dict[str, Any]:
    async def build(tx: Any) -> dict[str, Any]:
        owner = await tx.create_task(
            Hold,
            {"name": name, **({"slowAbort": True} if slow_owner else {})},
            {"ownership": {"kind": "conversation"}, "background": background},
        )
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": owner}})
        inner = await tx.create_task(
            Hold,
            {"name": f"{name}.inner", **({"slowAbort": True} if slow_inner else {})},
            {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
        )
        return {"owner": owner, "child": child["id"], "inner": inner}

    return await parent.commit(build, context)


async def test_keeps_a_conversation_busy_while_its_owned_foreground_subtree_has_live_work() -> None:
    opened = await open_harness()
    tree = await owned_child(opened["root"], "owner")
    open_gate("owner", "completed")
    await wait_until(lambda: _status_is(opened["harness"], tree["owner"], "completing"))
    idle = asyncio.ensure_future(opened["root"].wait_for_idle(context))
    assert await settled(idle) is False
    assert await settled(opened["harness"].wait_for_idle(context)) is False
    open_gate("owner.inner", "completed")
    await idle
    await opened["harness"].wait_for_idle(context)
    assert (await status(opened["harness"], tree["owner"]))["outcome"]["status"] == "completed"
    await opened["harness"].close(context)


async def _status_is(harness: Any, id: TaskId, value: str) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == value


async def test_holds_an_owner_for_work_in_a_conversation_its_ended_child_owns() -> None:
    opened = await open_harness()

    async def build(tx: Any) -> dict[str, Any]:
        owner = await tx.create_task(Hold, {"name": "owner"}, {"ownership": {"kind": "conversation"}})
        child = await tx.create_task(
            Hold, {"name": "child"}, {"ownership": {"kind": "task", "taskId": owner}}
        )
        conversation = await tx.create_conversation({"ownership": {"kind": "task", "taskId": child}})
        return {"owner": owner, "child": child, "conversation": conversation["id"]}

    tree = await opened["root"].commit(build, context)
    open_gate("child", "completed")
    assert (await opened["harness"].wait_for_task(tree["child"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    handle = await opened["harness"].conversation(tree["conversation"], context)
    await handle.commit(
        lambda tx: tx.create_task(Hold, {"name": "late"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    open_gate("owner", "completed")
    await wait_until(lambda: _status_is(opened["harness"], tree["owner"], "completing"))
    open_gate("late", "completed")
    assert (await opened["harness"].wait_for_task(tree["owner"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    await opened["harness"].close(context)


async def test_cascades_through_ended_tasks_into_an_owned_background_subtree_across_background() -> None:
    opened = await open_harness()

    async def build(tx: Any) -> dict[str, Any]:
        a = await tx.create_task(Hold, {"name": "a"}, {"ownership": {"kind": "conversation"}})
        b = await tx.create_task(Hold, {"name": "b"}, {"ownership": {"kind": "task", "taskId": a}})
        conversation = await tx.create_conversation({"ownership": {"kind": "task", "taskId": b}})
        c = await tx.create_task(
            Hold,
            {"name": "c"},
            {"ownership": {"kind": "conversation"}, "conversationId": conversation["id"], "background": True},
        )
        d = await tx.create_task(Hold, {"name": "d"}, {"ownership": {"kind": "task", "taskId": c}})
        return {"a": a, "b": b, "c": c, "d": d}

    tree = await opened["root"].commit(build, context)
    open_gate("b", "completed")
    open_gate("a", "completed")
    assert (await opened["harness"].wait_for_task(tree["a"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    await opened["root"].abort(context, {"background": True})
    for id in (tree["c"], tree["d"]):
        assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"]["status"] == "aborted"
    await opened["harness"].close(context)


async def test_tracks_ownership_chains_deeper_than_the_call_stack() -> None:
    opened = await open_harness()
    depth = 2_000

    async def build(tx: Any) -> dict[str, Any]:
        top = await tx.create_task(Hold, {"name": "top"}, {"ownership": {"kind": "conversation"}})
        owner = top
        for n in range(depth):
            owner = await tx.create_task(
                Hold, {"name": f"link{n}"}, {"ownership": {"kind": "task", "taskId": owner}}
            )
        leaf = await tx.create_task(Hold, {"name": "leaf"}, {"ownership": {"kind": "task", "taskId": owner}})
        return {"top": top, "leaf": leaf}

    tree = await opened["root"].commit(build, context)
    for n in range(depth):
        open_gate(f"link{n}", "completed")
    open_gate("top", "completed")
    await wait_until(lambda: _status_is(opened["harness"], tree["top"], "completing"))
    open_gate("leaf", "completed")
    assert (await opened["harness"].wait_for_task(tree["leaf"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    assert (await opened["harness"].wait_for_task(tree["top"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    await opened["harness"].close(context)


async def test_finalizes_many_held_siblings_that_end_together() -> None:
    opened = await open_harness()
    count = 100

    async def build(tx: Any) -> dict[str, Any]:
        top = await tx.create_task(Hold, {"name": "top"}, {"ownership": {"kind": "conversation"}})
        siblings: list[TaskId] = []
        for n in range(count):
            siblings.append(
                await tx.create_task(Hold, {"name": f"s{n}"}, {"ownership": {"kind": "task", "taskId": top}})
            )
        leaves: list[TaskId] = []
        for n in range(count - 1, -1, -1):
            leaves.append(
                await tx.create_task(
                    Hold,
                    {"name": f"l{n}"},
                    {"ownership": {"kind": "task", "taskId": siblings[n]}},
                )
            )
        return {"top": top, "leaves": leaves}

    tree = await opened["root"].commit(build, context)
    for n in range(count):
        open_gate(f"s{n}", "completed")
    open_gate("top", "completed")
    await wait_until(lambda: _status_is(opened["harness"], tree["top"], "completing"))

    async def end_leaves(tx: Any) -> None:
        records = [await tx.task(id) for id in tree["leaves"]]
        for record in records:
            tx.set_task(
                {
                    **record,
                    "state": {"status": "terminal", "outcome": {"status": "completed", "result": None}},
                }
            )

    await opened["root"].commit(end_leaves, context)
    assert (await opened["harness"].wait_for_task(tree["top"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    await opened["harness"].close(context)


async def test_rejects_a_conversation_created_in_the_commit_that_ends_its_owner() -> None:
    opened = await open_harness()
    spawner = await opened["root"].commit(
        lambda tx: tx.create_task(Spawner, {"name": "spawned"}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    outcome = (await opened["harness"].wait_for_task(spawner, context))["state"]["outcome"]
    assert outcome["status"] == "faulted"
    assert outcome["error"]["message"].startswith(f"Conversation owner task {spawner} is ")
    await opened["harness"].close(context)


async def test_stops_idle_traversal_at_a_background_owner() -> None:
    opened = await open_harness()
    tree = await owned_child(opened["root"], "background", background=True)
    await opened["root"].wait_for_idle(context)
    await opened["harness"].wait_for_idle(context)
    child = await opened["harness"].conversation(tree["child"], context)
    assert await settled(child.wait_for_idle(context)) is False
    await child.abort(context)
    await opened["harness"].close(context)


async def test_cascades_an_abort_mark_to_the_owned_foreground_subtree() -> None:
    opened = await open_harness()
    tree = await owned_child(opened["root"], "owner")
    nested = await opened["harness"].conversation(tree["child"], context)
    deeper = await owned_child(nested, "deeper")
    shielded = await owned_child(nested, "shielded", background=True)
    assert await opened["harness"].abort_task(tree["owner"], context) == "marked"
    for id in (tree["owner"], tree["inner"], deeper["owner"], deeper["inner"]):
        assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"]["status"] == "aborted"
    assert (await status(opened["harness"], shielded["owner"]))["status"] not in ("terminal", "completing")
    assert (await status(opened["harness"], shielded["inner"]))["status"] not in ("terminal", "completing")
    await opened["harness"].abort_task(shielded["owner"], context)
    await opened["harness"].close(context)


async def test_cascades_a_failed_owner_but_not_a_completed_one() -> None:
    opened = await open_harness()
    failed = await owned_child(opened["root"], "failed")
    completed_tree = await owned_child(opened["root"], "done")
    open_gate("failed", "failed")
    open_gate("done", "completed")
    assert (await opened["harness"].wait_for_task(failed["inner"], context))["state"]["outcome"][
        "status"
    ] == "aborted"
    assert (await opened["harness"].wait_for_task(failed["owner"], context))["state"]["outcome"][
        "status"
    ] == "failed"
    await wait_until(lambda: _status_is(opened["harness"], completed_tree["owner"], "completing"))
    assert (await status(opened["harness"], completed_tree["inner"]))["status"] != "terminal"
    open_gate("done.inner", "completed")
    assert (await opened["harness"].wait_for_task(completed_tree["owner"], context))["state"]["outcome"][
        "status"
    ] == "completed"
    await opened["harness"].close(context)


async def test_aborts_a_background_task_directly_with_its_ordinary_subtree() -> None:
    opened = await open_harness()
    tree = await owned_child(opened["root"], "background", background=True)
    await opened["harness"].abort_task(tree["owner"], context)
    assert (await opened["harness"].wait_for_task(tree["inner"], context))["state"]["outcome"][
        "status"
    ] == "aborted"
    await opened["harness"].close(context)


async def test_aborts_work_below_a_held_failed_owner_but_not_a_terminal_one() -> None:
    opened = await open_harness()
    tree = await owned_child(opened["root"], "owner", slow_inner=True)
    open_gate("owner", "failed")
    await wait_until(lambda: _abort_requested(opened["harness"], tree["inner"]))
    assert (await status(opened["harness"], tree["owner"]))["status"] == "completing"
    child = await opened["harness"].conversation(tree["child"], context)

    async def create(name: str) -> TaskId:
        return await child.commit(
            lambda tx: tx.create_task(Hold, {"name": name}, {"ownership": {"kind": "conversation"}}),
            context,
        )

    during = await create("during")
    assert (await opened["harness"].wait_for_task(during, context))["state"]["outcome"]["status"] == "aborted"
    open_gate("abort.owner.inner", "completed")
    assert (await opened["harness"].wait_for_task(tree["owner"], context))["state"]["outcome"][
        "status"
    ] == "failed"
    after = await create("after")
    open_gate("after", "completed")
    assert (await opened["harness"].wait_for_task(after, context))["state"]["outcome"][
        "status"
    ] == "completed"
    await opened["harness"].close(context)


async def _abort_requested(harness: Any, id: TaskId) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record.get("abortRequested") is True


async def test_keeps_a_nested_background_owners_subtree_when_its_cancelled_owner_is_aborted() -> None:
    opened = await open_harness()
    outer = await owned_child(opened["root"], "outer", background=True)
    conv = await opened["harness"].conversation(outer["child"], context)
    inner = await owned_child(conv, "inner", background=True)
    await opened["harness"].abort_task(outer["owner"], context)
    assert (await opened["harness"].wait_for_task(outer["inner"], context))["state"]["outcome"][
        "status"
    ] == "aborted"
    assert (await status(opened["harness"], inner["owner"]))["status"] not in ("terminal", "completing")
    assert (await status(opened["harness"], inner["inner"]))["status"] not in ("terminal", "completing")
    await opened["harness"].abort_task(inner["owner"], context)
    await opened["harness"].close(context)


async def test_cascades_from_an_owner_the_scheduler_orphans() -> None:
    opened = await open_harness()

    async def build(tx: Any) -> dict[str, Any]:
        owner = await tx.create_task(
            Unregistered, {"name": "unregistered"}, {"ownership": {"kind": "conversation"}}
        )
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": owner}})
        inner = await tx.create_task(
            Hold,
            {"name": "below"},
            {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
        )
        return {"owner": owner, "inner": inner}

    tree = await opened["root"].commit(build, context)
    assert await opened["harness"].abort_task(tree["owner"], context) == "marked"
    assert (await opened["harness"].wait_for_task(tree["owner"], context))["state"]["outcome"] == {
        "status": "orphaned",
        "reason": "missing_task",
    }
    assert (await opened["harness"].wait_for_task(tree["inner"], context))["state"]["outcome"][
        "status"
    ] == "aborted"
    await opened["harness"].close(context)


async def test_cancels_only_the_callers_wait_never_the_shared_work() -> None:
    opened = await open_harness()
    tree = await owned_child(opened["root"], "owner")
    waiting = AbortController()
    idle = asyncio.ensure_future(opened["root"].wait_for_idle(with_signal(waiting.signal)))
    waiting.abort(RuntimeError("stop waiting"))
    with pytest.raises(Exception, match="stop waiting"):
        await idle
    assert (await status(opened["harness"], tree["inner"]))["status"] != "terminal"
    await opened["harness"].abort_task(tree["owner"], context)
    await opened["harness"].close(context)


async def test_aborts_a_waiting_child_whose_awaited_task_completes_in_the_commit_that_marks_its_owner() -> (
    None
):
    opened = await open_harness()

    async def build(tx: Any) -> dict[str, Any]:
        owner = await tx.create_task(Hold, {"name": "owner"}, {"ownership": {"kind": "conversation"}})
        child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": owner}})
        dependency = await tx.create_task(
            Hold, {"name": "dependency"}, {"ownership": {"kind": "conversation"}}
        )
        blocked = await tx.create_task(
            Waiter,
            {"on": [dependency]},
            {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
        )
        return {"owner": owner, "dependency": dependency, "blocked": blocked}

    tree = await opened["root"].commit(build, context)
    await wait_until(lambda: RUNS.get("dependency") == 1 and RUNS.get("owner") == 1)
    await wait_until(lambda: _status_is(opened["harness"], tree["blocked"], "waiting"))

    async def finish_and_mark(tx: Any) -> None:
        dependency = await tx.task(tree["dependency"])
        owner = await tx.task(tree["owner"])
        tx.set_task(
            {
                **dependency,
                "state": {"status": "terminal", "outcome": {"status": "completed", "result": None}},
            }
        )
        tx.set_task({**owner, "abortRequested": True})

    await opened["root"].commit(finish_and_mark, context)
    assert (await opened["harness"].wait_for_task(tree["blocked"], context))["state"]["outcome"][
        "status"
    ] == "aborted"
    await opened["harness"].close(context)


async def test_fails_the_harness_on_a_failed_cascade_commit_and_reopening_applies_it() -> None:
    failure = RuntimeError("disk gone")
    state: dict[str, Any] = {}

    class Failing(ControlledStorage):
        async def commit(self, writes: Any, commit_context: Any) -> Any:
            target = state.get("failMark")
            if target is not None and any(
                write["type"] == "task"
                and write["value"]["id"] == target
                and write["value"].get("abortRequested")
                for write in writes
            ):
                state["failMark"] = None
                raise failure
            return await super().commit(writes, commit_context)

    storage = Failing()
    opened = await open_harness(storage)
    tree = await owned_child(opened["root"], "owner")
    state["failMark"] = tree["inner"]
    open_gate("owner", "failed")
    assert await opened["harness"].closed == {"reason": "failed", "error": failure}
    assert opened["reports"] == [failure]
    await opened["harness"].close(context)
    reopened = await open_harness(storage.reopen())
    assert (await reopened["harness"].wait_for_task(tree["inner"], context))["state"]["outcome"][
        "status"
    ] == "aborted"
    await reopened["harness"].close(context)


async def test_gives_a_tool_an_invocation_bound_handle_whose_submissions_stay_durable() -> None:
    from chat_support import chat_setup, open_chat
    from tool_support import DONE, add_tool, all_entries, calls, tool

    from pi_durable import configure

    setup = chat_setup()
    held: dict[str, Any] = {}

    async def delegate(_args: Any, api: Any, call_context: Any) -> Any:
        async def create(tx: Any) -> Any:
            created = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": api.task_id}}
            )
            await configure(tx, created["id"], {"model": {"provider": "faux", "modelId": "faux-1"}})
            return created["id"]

        child = await api.commit(create, call_context)
        held["missing"] = await api.conversation(99_999, call_context)
        handle = await api.conversation(child, call_context)
        held["handle"] = handle
        try:
            await handle.submit(
                {"type": "write", "entry": {"kind": "test.note", "data": "passive"}}, call_context
            )
        except TypeError as error:
            held["write_error"] = str(error)
        submission = await handle.submit(
            {"type": "input", "content": "child task", "requestId": "child"}, call_context
        )
        held["submission"] = submission
        settled_record = await submission.wait(call_context)
        return {"output": [{"type": "text", "text": settled_record["status"]}]}

    add_tool(setup.registry, tool("delegate", delegate))
    setup.faux.set_responses(
        [
            calls(["delegate", {}, "c1"]),
            assistant_text("child answer"),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert held["missing"] is None
    assert held["write_error"] == "A conversation handle accepts only input submissions"
    # The call ended: the handle and its submission reject, while the submission record remains.
    with pytest.raises(RuntimeError, match="invocation has ended"):
        await held["handle"].submit({"type": "input", "content": "again"}, context)
    with pytest.raises(RuntimeError, match="invocation has ended"):
        await held["handle"].wait_for_idle(context)
    with pytest.raises(RuntimeError, match="invocation has ended"):
        await held["handle"].abort(context)
    with pytest.raises(RuntimeError, match="invocation has ended"):
        await held["submission"].wait(context)
    child = await harness.conversation(held["handle"].id, context)
    child_entries = await all_entries(child)
    assert [entry["kind"] for entry in child_entries].count("pi.user") == 1
    assert all(entry["kind"] != "test.note" for entry in child_entries)
    inspection = await harness.inspect(context)
    assert [
        task for task in inspection["tasks"] if task["record"]["conversationId"] == held["handle"].id
    ] == []
    record = await harness.submission(held["submission"].id, context)
    assert (await record.status(context))["status"] == "done"
    await harness.close(context)


def assistant_text(text: str) -> dict[str, Any]:
    from chat_support import assistant_message, faux_text

    return assistant_message([faux_text(text)])


def gated(message: dict[str, Any], gate: Deferred, reached: Deferred) -> Any:
    """A faux step that waits for ``gate``, or rejects when the request is cancelled."""

    async def respond(_request: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
        reached.resolve()
        signal = options.get("signal")
        waiters = [asyncio.ensure_future(asyncio.shield(gate.promise))]
        if signal is not None:
            waiters.append(asyncio.ensure_future(aborted(signal)))
        done, pending = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        for waiter in pending:
            waiter.cancel()
        for waiter in done:
            waiter.exception()
        if signal is not None and signal.aborted:
            raise cancellation_error(signal)
        return message

    return respond


async def test_rejects_a_queued_handle_operation_when_the_invocation_ends_first() -> None:
    from chat_support import chat_setup, open_chat
    from tool_support import add_tool, calls, tool

    setup = chat_setup()
    ready = deferred()
    go = deferred()
    queued = deferred()
    held: dict[str, Any] = {}

    async def delegate(_args: Any, api: Any, call_context: Any) -> Any:
        async def create(tx: Any) -> Any:
            created = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": api.task_id}}
            )
            return created["id"]

        child = await api.commit(create, call_context)
        handle = await api.conversation(child, call_context)
        held["child"] = child
        ready.resolve({"child": child, "task_id": api.task_id})
        await go.promise
        # Called while the invocation is alive, with a context that is not the call's: the handle
        # binds it to the invocation. It reaches the line only after the abort mark.
        submitting = asyncio.ensure_future(
            handle.submit({"type": "input", "content": "late"}, context)
        )
        submitting.add_done_callback(
            lambda done: done.exception() if not done.cancelled() else None
        )
        held["submitting"] = submitting
        queued.resolve()
        await aborted(call_context.abort_signal)

    add_tool(setup.registry, tool("delegate", delegate))
    setup.faux.set_responses(
        [
            calls(["delegate", {}, "c1"]),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await root.submit({"type": "input", "content": "go"}, context)
    tree = await ready.promise
    # Hold the Session line, queue the abort mark behind it, then let the tool queue its submit.
    release = deferred()
    holding = asyncio.ensure_future(root.commit(lambda _tx: release.promise, context))
    aborting = asyncio.ensure_future(harness.abort_task(tree["task_id"], context))
    go.resolve()
    await queued.promise
    release.resolve()
    await holding
    await aborting
    with pytest.raises(AbortError):
        await held["submitting"]
    child = await harness.conversation(tree["child"], context)
    entries = await child.entries({}, 10, None, context)
    assert entries["items"] == []
    inspection = await harness.inspect(context)
    assert [
        record for record in inspection["submissions"] if record["conversationId"] == tree["child"]
    ] == []
    await harness.close(context)


async def test_aborts_a_subagent_run_and_rejects_its_waiting_tool() -> None:
    from chat_support import assistant_message, chat_setup, faux_text, open_chat
    from tool_support import add_tool, calls, tool

    from pi_durable import configure

    setup = chat_setup()
    child_run = deferred()
    reached = deferred()
    tool_wait = deferred()
    held: dict[str, Any] = {}

    async def delegate(_args: Any, api: Any, call_context: Any) -> Any:
        async def create(tx: Any) -> Any:
            created = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": api.task_id}}
            )
            await configure(tx, created["id"], {"model": {"provider": "faux", "modelId": "faux-1"}})
            return created["id"]

        child = await api.commit(create, call_context)
        held["child"] = child
        handle = await api.conversation(child, call_context)
        submission = await handle.submit({"type": "input", "content": "child task"}, call_context)
        try:
            settled = await submission.wait(call_context)
            return {"output": [{"type": "text", "text": settled["status"]}]}
        except BaseException as error:
            tool_wait.resolve(str(error))
            raise

    add_tool(setup.registry, tool("delegate", delegate))
    child_response = gated(assistant_message([faux_text("never")]), child_run, reached)
    setup.faux.set_responses([calls(["delegate", {}, "c1"]), child_response])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    input = await root.submit({"type": "input", "content": "go"}, context)
    await reached.promise
    await root.abort(context)
    assert (await input.wait(context))["status"] == "unanswered"
    assert (await input.wait(context)).get("reason") == "aborted"
    assert await tool_wait.promise
    child = await harness.conversation(held["child"], context)
    await child.wait_for_idle(context)
    assert await harness.snapshot(LiveDoc, held["child"], context) == {}
    inspection = await harness.inspect(context)
    assert [
        record for record in inspection["submissions"] if record["conversationId"] == held["child"]
    ] == []
    await harness.wait_for_idle(context)
    await harness.close(context)


async def test_lets_a_running_tool_abort_its_owned_child_and_continue() -> None:
    from chat_support import assistant_message, chat_setup, faux_text, open_chat
    from tool_support import DONE, add_tool, calls, tool

    from pi_durable import configure

    setup = chat_setup()
    child_run = deferred()
    reached = deferred()

    async def delegate(_args: Any, api: Any, call_context: Any) -> Any:
        async def create(tx: Any) -> Any:
            created = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": api.task_id}}
            )
            await configure(tx, created["id"], {"model": {"provider": "faux", "modelId": "faux-1"}})
            return created["id"]

        child = await api.commit(create, call_context)
        handle = await api.conversation(child, call_context)
        submission = await handle.submit({"type": "input", "content": "child task"}, call_context)
        await reached.promise
        await handle.abort(call_context)
        settled = await submission.wait(call_context)
        return {"output": [{"type": "text", "text": f"child {settled['status']}"}]}

    add_tool(setup.registry, tool("delegate", delegate))
    child_response = gated(assistant_message([faux_text("never")]), child_run, reached)
    setup.faux.set_responses([calls(["delegate", {}, "c1"]), child_response, DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    await harness.close(context)

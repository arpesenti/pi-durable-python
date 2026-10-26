"""Translated upstream ``harness-tasks`` cases for the durable task core."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import ControlledStorage, context, flush
from task_support import (
    aborted_with,
    completed,
    deferred,
    eventually,
    open_tasks,
    settled,
    with_signal,
)

from pi_durable import MemoryStorage
from pi_durable.context import AbortController
from pi_durable.documents import define_doc
from pi_durable.entries import define_entry
from pi_durable.harness.define import define_extension, define_tool, hook
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.ids import TaskId
from pi_durable.tasks import define_task


def one_step(name: str, run: Any, abort: Any = None) -> Any:
    """A one-phase task; the default abort handler settles ``aborted`` with reason test."""

    async def default_abort(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: aborted_with("test"), ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": default_abort if abort is None else abort,
        }
    )


async def start(conversation: Any, task: Any, background: bool = False, input: Any = None) -> TaskId:
    return await conversation.commit(
        lambda tx: tx.create_task(
            task,
            input,
            {"ownership": {"kind": "conversation"}, "background": background},
        ),
        context,
    )


async def open_root(tasks: Any, options: dict[str, Any] | None = None) -> dict[str, Any]:
    values = dict(options or {})
    opened = await open_tasks(values.pop("storage", None) or MemoryStorage(), tasks, values)
    opened["root"] = await opened["harness"].root(context)
    return opened


async def mark_durably(harness: Any, id: TaskId) -> asyncio.Task[Any]:
    aborting = asyncio.ensure_future(harness.abort_task(id, context))
    while not (await harness.get_task(id, context)).get("abortRequested"):
        await flush()
    return aborting


async def test_continues_one_invocation_through_checkpoint_progress() -> None:
    seen: list[int] = []
    runtimes: set[int] = set()

    async def count(task: Any, runtime: Any, ctx: Any) -> None:
        seen.append(task["state"]["checkpoint"]["n"])
        runtimes.add(id(runtime))

        def change(_tx: Any, current: Any) -> Any:
            n = current["state"]["checkpoint"]["n"] + 1
            return (
                completed(n)
                if n == task["input"]["to"]
                else {
                    "status": "running",
                    "checkpoint": {"phase": "count", "n": n},
                }
            )

        await runtime.commit(change, ctx)

    Counter = define_task(
        {
            "name": "test.counter",
            "version": 1,
            "initial": lambda _input: {"phase": "count", "n": 0},
            "phases": {"count": count},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    opened = await open_root([Counter])
    task_id = await opened["root"].commit(
        lambda tx: tx.create_task(Counter, {"to": 3}, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    receipt = await opened["harness"].wait_for_task(task_id, context)
    assert receipt["state"]["outcome"] == {"status": "completed", "result": 3}
    assert seen == [0, 1, 2]
    assert len(runtimes) == 1
    await opened["harness"].close(context)


async def test_faults_a_phase_without_durable_progress_and_a_throwing_phase() -> None:
    Idle = one_step("test.idle", lambda task, runtime, ctx: asyncio.sleep(0))
    DocumentOnly = one_step(
        "test.document-only", lambda task, runtime, ctx: runtime.commit(lambda: None, ctx)
    )
    Throws = one_step("test.throws", lambda task, runtime, ctx: _raise(RuntimeError("boom")))
    opened = await open_root([Idle, DocumentOnly, Throws])
    ids = [await start(opened["root"], task) for task in (Idle, DocumentOnly, Throws)]
    opened["harness"].resume()
    outcomes = [(await opened["harness"].wait_for_task(id, context))["state"]["outcome"] for id in ids]
    assert outcomes == [
        {
            "status": "faulted",
            "error": {"message": "Task test.idle phase run returned without durable progress"},
        },
        {
            "status": "faulted",
            "error": {"message": "Task test.document-only phase run returned without durable progress"},
        },
        {"status": "faulted", "error": {"message": "boom"}},
    ]
    await opened["harness"].close(context)


async def _raise(error: BaseException) -> None:
    raise error


async def test_keeps_a_committed_terminal_outcome_when_the_handler_throws_afterwards() -> None:
    late: list[Any] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed("ok"), ctx)
        late.append(await _capture(runtime.commit(lambda: None, ctx)))
        raise RuntimeError("after terminal")

    Done = one_step("test.done", run)
    opened = await open_root([Done])
    id = await start(opened["root"], Done)
    opened["harness"].resume()
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "completed",
        "result": "ok",
    }
    await eventually(lambda: _check(lambda: bool(late)))
    assert f"Task {id} is terminal" in str(late[0])
    await opened["harness"].close(context)


async def _capture(awaitable: Any) -> BaseException:
    try:
        await awaitable
    except BaseException as error:
        return error
    raise AssertionError("expected a rejection")


async def _check(predicate: Any) -> bool:
    return bool(await predicate()) if asyncio.iscoroutinefunction(predicate) else bool(predicate())


async def test_compares_checkpoints_by_value_including_arrays() -> None:
    async def collect(task: Any, runtime: Any, ctx: Any) -> None:
        items = task["state"]["checkpoint"]["items"]
        following = [*items, f"item{len(items)}"] if len(items) < 2 else list(items)
        await runtime.commit(
            lambda: {"status": "running", "checkpoint": {"phase": "collect", "items": following}},
            ctx,
        )

    Collect = define_task(
        {
            "name": "test.collect",
            "version": 1,
            "initial": lambda _input: {"phase": "collect", "items": []},
            "phases": {"collect": collect},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    opened = await open_root([Collect])
    id = await opened["root"].commit(
        lambda tx: tx.create_task(Collect, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "faulted",
        "error": {"message": "Task test.collect phase collect returned without durable progress"},
    }
    await opened["harness"].close(context)


async def test_memos_children_and_task_document_retirement() -> None:
    Progress = define_doc(
        {
            "kind": "test.task-progress",
            "version": 1,
            "scope": "task",
            "initial": lambda: {"lines": []},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    Answer: Any = define_entry("answer")
    Child = one_step("test.child", lambda task, runtime, ctx: runtime.commit(lambda: completed(None), ctx))
    seen: dict[str, Any] = {}

    async def write(task: Any, runtime: Any, ctx: Any) -> None:
        assert await runtime.memo("choice", ctx) is None
        winners = await asyncio.gather(runtime.memo("choice", "a", ctx), runtime.memo("choice", "b", ctx))
        assert winners == ["a", "a"]
        assert await runtime.memo("choice", ctx) == "a"
        assert await runtime.memo("toString", ctx) is None
        assert await runtime.memo("toString", "own", ctx) == "own"

        async def change(tx: Any) -> Any:
            draft = await tx.doc(Progress, task["id"])
            draft.lines.append("wrote")
            seen["child"] = await tx.create_task(Child, None, {"ownership": {"kind": "conversation"}})
            return {"status": "running", "checkpoint": {"phase": "answer"}}

        await runtime.commit(change, ctx)

    async def answer(task: Any, runtime: Any, ctx: Any) -> None:
        assert task["memos"] == {"choice": "a", "toString": "own"}

        async def read(tx: Any) -> None:
            seen["progress"] = list((await tx.doc(Progress, task["id"])).lines)

        await runtime.commit(read, ctx)

        async def finish(tx: Any, current: Any) -> Any:
            entry = await tx.append_entry(
                current["conversationId"], {"kind": "answer", "model": [_user("done")]}
            )
            return completed({"entryId": entry["id"]})

        await runtime.commit(finish, ctx)

    Writer = define_task(
        {
            "name": "test.writer",
            "version": 1,
            "initial": lambda _input: {"phase": "write"},
            "phases": {"write": write, "answer": answer},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    opened = await open_root([Writer, Child])
    conversation = await opened["harness"].create_conversation({"ownership": {"kind": "ownerless"}}, context)
    id = await conversation.commit(
        lambda tx: tx.create_task(Writer, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    receipt = await opened["harness"].wait_for_task(id, context)
    assert "checkpoint" not in receipt["state"]
    assert "memos" not in receipt
    entry = await opened["harness"].commit(
        lambda tx: tx.entry(receipt["state"]["outcome"]["result"]["entryId"]), context
    )
    assert Answer.is_(entry)
    assert entry["conversationId"] == conversation.id
    assert seen["progress"] == ["wrote"]
    assert await opened["harness"].snapshot(Progress, id, context) is None
    assert await opened["harness"].get_task(id, context) == receipt
    child = await opened["harness"].wait_for_task(seen["child"], context)
    assert child["conversationId"] == conversation.id
    assert opened["root"].id != conversation.id
    await opened["harness"].close(context)


def _user(text: str) -> dict[str, Any]:
    return {"role": "user", "content": text, "timestamp": 1}


async def test_resumes_a_waiting_task_once_on_is_terminal() -> None:
    gate = deferred()
    order: list[str] = []
    on: list[TaskId] = []
    outcomes: list[str] = []

    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        order.append("wait")
        await runtime.commit(
            lambda: {
                "status": "waiting",
                "checkpoint": {"phase": "resume"},
                "on": on,
                "policy": "allSettled",
            },
            ctx,
        )

    async def resume(task: Any, runtime: Any, ctx: Any) -> None:
        order.append("resume")
        outcomes.extend(outcome["status"] for outcome in await runtime.outcomes(on, ctx))
        await runtime.commit(lambda: completed(None), ctx)

    Waiter = define_task(
        {
            "name": "test.waiter",
            "version": 1,
            "initial": lambda _input: {"phase": "wait"},
            "phases": {"wait": wait, "resume": resume},
            "abort": lambda task, runtime, ctx: runtime.commit(lambda: aborted_with("test"), ctx),
        }
    )

    async def first_run(task: Any, runtime: Any, ctx: Any) -> None:
        order.append("first")
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    First = one_step("test.first", first_run)

    async def faulting_run(task: Any, runtime: Any, ctx: Any) -> None:
        order.append("faulting")
        raise RuntimeError("fails")

    Faulting = one_step("test.faulting", faulting_run)
    opened = await open_root([First, Faulting, Waiter])
    first = await start(opened["root"], First)
    faulting = await start(opened["root"], Faulting)
    on.extend([first, faulting])
    waiter = await opened["root"].commit(
        lambda tx: tx.create_task(Waiter, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    await eventually(lambda: _check(lambda: len(order) == 3))
    await flush()
    assert sorted(order) == ["faulting", "first", "wait"]
    record = await opened["harness"].get_task(waiter, context)
    assert record["state"]["status"] == "waiting" and record["state"]["on"] == on
    gate.resolve()
    await opened["harness"].wait_for_task(waiter, context)
    assert order[-1] == "resume"
    assert outcomes == ["completed", "faulted"]
    await opened["harness"].close(context)


async def test_stamps_started_at_and_ended_at() -> None:
    clock = [1_000]
    gate = deferred()
    on: list[TaskId] = []

    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {
                "status": "waiting",
                "checkpoint": {"phase": "resume"},
                "on": on,
                "policy": "allSettled",
            },
            ctx,
        )

    Waiter = define_task(
        {
            "name": "test.timed-waiter",
            "version": 1,
            "initial": lambda _input: {"phase": "wait"},
            "phases": {
                "wait": wait,
                "resume": lambda task, runtime, ctx: runtime.commit(lambda: completed(None), ctx),
            },
            "abort": lambda task, runtime, ctx: runtime.commit(lambda: aborted_with("test"), ctx),
        }
    )

    async def held(task: Any, runtime: Any, ctx: Any) -> None:
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    Held = one_step("test.timed-held", held)
    opened = await open_root([Waiter, Held], {"now": lambda: clock[0]})
    held_id = await start(opened["root"], Held)
    on.append(held_id)
    waiter = await opened["root"].commit(
        lambda tx: tx.create_task(Waiter, None, {"ownership": {"kind": "conversation"}}),
        context,
    )

    async def times() -> list[list[Any]]:
        inspection = await opened["harness"].inspect(context)
        return sorted(
            [
                [record["id"], record.get("startedAt"), record.get("endedAt")]
                for record in (task["record"] for task in inspection["tasks"])
            ]
        )

    assert await times() == [[held_id, None, None], [waiter, None, None]]
    clock[0] = 2_000
    opened["harness"].resume()
    await eventually(lambda: _waiting_status(opened["harness"], waiter))
    assert await times() == [[held_id, 2_000, None], [waiter, 2_000, None]]
    clock[0] = 5_000
    gate.resolve()
    settled_waiter = await opened["harness"].wait_for_task(waiter, context)
    settled_held = await opened["harness"].get_task(held_id, context)
    assert [settled_held.get("startedAt"), settled_held.get("endedAt")] == [2_000, 5_000]
    assert [settled_waiter.get("startedAt"), settled_waiter.get("endedAt")] == [2_000, 5_000]
    await opened["harness"].close(context)


async def _waiting_status(harness: Any, id: TaskId) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == "waiting"


async def test_keeps_one_registry_snapshot_per_phase() -> None:
    seen: list[tuple[str, list[str], bool]] = []
    phase_gate = deferred()
    entered = deferred()

    async def a(task: Any, runtime: Any, ctx: Any) -> None:
        before = runtime.registry
        entered.resolve()
        await phase_gate.promise
        seen.append(
            (
                "a",
                [item["tool"].name for item in runtime.registry.tools()],
                runtime.registry is before,
            )
        )
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "b"}}, ctx)

    async def b(task: Any, runtime: Any, ctx: Any) -> None:
        seen.append(("b", [item["tool"].name for item in runtime.registry.tools()], True))
        await runtime.commit(lambda: completed(None), ctx)

    Snapshots = define_task(
        {
            "name": "test.snapshots",
            "version": 1,
            "initial": lambda _input: {"phase": "a"},
            "phases": {"a": a, "b": b},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    opened = await open_root([Snapshots])
    id = await start(opened["root"], Snapshots)
    opened["harness"].resume()
    await entered.promise
    late = define_tool({"name": "late", "description": "late", "execute": lambda: {}})
    opened["registry"].install(define_extension({"name": "tools", "tools": [late]}))
    phase_gate.resolve()
    await opened["harness"].wait_for_task(id, context)
    assert seen == [("a", [], True), ("b", ["late"], True)]
    await opened["harness"].close(context)


async def test_resolves_agent_at_first_use_in_a_phase() -> None:
    pings: list[str] = []
    seen: list[dict[str, Any]] = []
    before_use = deferred()
    after_use = deferred()
    waiting_before = deferred()
    waiting_after = deferred()

    async def ping(runtime: Any) -> list[str]:
        pings.clear()
        await runtime.hooks.each("ping", lambda handler: handler())
        return list(pings)

    async def a(task: Any, runtime: Any, ctx: Any) -> None:
        waiting_before.resolve()
        await before_use.promise
        first = await runtime.agent(ctx)
        seen.append({"phase": "a", "pings": await ping(runtime), "thinking": first.thinking_level})
        waiting_after.resolve()
        await after_use.promise
        second = await runtime.agent(ctx)
        seen.append(
            {
                "phase": "a",
                "pings": await ping(runtime),
                "thinking": second.thinking_level,
                "same": second is first,
            }
        )
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "b"}}, ctx)

    async def b(task: Any, runtime: Any, ctx: Any) -> None:
        agent = await runtime.agent(ctx)
        seen.append({"phase": "b", "pings": await ping(runtime), "thinking": agent.thinking_level})
        await runtime.commit(lambda: completed(None), ctx)

    Phases = define_task(
        {
            "name": "test.agent-phases",
            "version": 1,
            "initial": lambda _input: {"phase": "a"},
            "phases": {"a": a, "b": b},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    opened = await open_root([Phases])
    opened["registry"].install(
        define_extension(
            {"name": "hooks:before", "hooks": [hook(Phases, {"ping": lambda: pings.append("before")})]}
        )
    )
    id = await start(opened["root"], Phases)
    opened["harness"].resume()
    await waiting_before.promise
    await opened["root"].configure({"thinking_level": "low"}, context)
    before_use.resolve()
    await waiting_after.promise
    opened["registry"].install(
        define_extension(
            {"name": "hooks:late", "hooks": [hook(Phases, {"ping": lambda: pings.append("late")})]}
        )
    )
    await opened["root"].configure({"thinking_level": "high"}, context)
    after_use.resolve()
    await opened["harness"].wait_for_task(id, context)
    assert seen == [
        {"phase": "a", "pings": ["before"], "thinking": "low"},
        {"phase": "a", "pings": ["before"], "thinking": "low", "same": True},
        {"phase": "b", "pings": ["before", "late"], "thinking": "high"},
    ]
    await opened["harness"].close(context)


async def test_rejects_runtime_operations_after_the_invocation_ends() -> None:
    Notes = define_doc(
        {
            "kind": "test.task-notes",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    captured: list[Any] = []
    handler_context: list[Any] = []
    delivered: list[str] = []
    watch_closed: list[Any] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        captured.append(runtime)
        handler_context.append(ctx)
        watch = await runtime.watch_doc(Notes, ctx)
        watch.start(lambda value, _ops, _ctx: delivered.append((value or {}).get("text", "retired")))
        watch_closed.append(watch.closed)
        await runtime.commit(lambda: completed(None), ctx)

    Watcher = one_step("test.watcher", run)
    opened = await open_root([Watcher])

    async def write(tx: Any) -> None:
        (await tx.doc(Notes)).text = "hello"

    await opened["harness"].commit(write, context)
    id = await start(opened["root"], Watcher)
    opened["harness"].resume()
    await opened["harness"].wait_for_task(id, context)
    assert await watch_closed[0] == {"reason": "stopped"}

    async def later(tx: Any) -> None:
        (await tx.doc(Notes)).text = "after"

    await opened["harness"].commit(later, context)
    await flush()
    assert delivered == []
    runtime = captured[0]
    for awaitable in (
        runtime.commit(lambda: {}, context),
        runtime.memo("x", context),
        runtime.memo("x", 1, context),
        runtime.sleep_until(0, {"phase": "run"}, context),
        runtime.watch_doc(Notes, context),
        runtime.agent(handler_context[0]),
    ):
        with pytest.raises(Exception) as error:
            await awaitable
        assert "invocation has ended" in str(error.value)
    with pytest.raises(Exception, match="invocation has ended"):
        runtime.now()
    with pytest.raises(Exception, match="invocation has ended"):
        runtime.report(RuntimeError("late"))
    await opened["harness"].close(context)


async def test_reads_documents_and_context_through_the_runtime() -> None:
    Notes = define_doc(
        {
            "kind": "test.runtime-notes",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    captured: list[Any] = []
    seen: list[Any] = []
    reports: list[BaseException] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        captured.append(runtime)
        entries = (await runtime.context(runtime.conversation_id, ctx))["entries"]
        first, second = entries[0]["id"], entries[1]["id"]
        seen.append((await runtime.snapshot(Notes, runtime.conversation_id, ctx))["text"])
        seen.append((await runtime.snapshot_as_of(Notes, runtime.conversation_id, first, ctx))["text"])
        seen.append(len((await runtime.context(runtime.conversation_id, ctx, {"at": first}))["entries"]))
        seen.append(len((await runtime.context(runtime.conversation_id, ctx, {"at": second}))["messages"]))
        seen.append(runtime.now())
        runtime.report(RuntimeError("reported"))
        await runtime.commit(lambda: completed(None), ctx)

    Reader = one_step("test.reader", run)
    opened = await open_root([Reader], {"now": lambda: 1234})
    opened["reports"].extend(reports)

    async def write(text: str, tx: Any) -> None:
        (await tx.doc(Notes, opened["root"].id)).text = text
        await tx.append_entry(opened["root"].id, {"kind": "message", "model": [_user(text)]})

    for text in ("one", "two"):
        await opened["root"].commit(lambda tx, text=text: write(text, tx), context)
    id = await start(opened["root"], Reader)
    opened["harness"].resume()
    await opened["harness"].wait_for_task(id, context)
    assert seen == ["two", "one", 1, 2, 1234]
    assert [str(error) for error in opened["reports"]] == ["reported"]
    await flush()
    with pytest.raises(Exception, match="invocation has ended"):
        await captured[0].snapshot(Notes, opened["root"].id, context)
    with pytest.raises(Exception, match="invocation has ended"):
        await captured[0].context(opened["root"].id, context)
    await opened["harness"].close(context)


async def test_orders_runtime_commits_against_the_step() -> None:
    storage = ControlledStorage()
    before: list[Any] = []
    after: list[Any] = []
    held: list[Any] = []
    harness_ref: list[Any] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        gate = storage.hold_commits()
        held.append(gate)
        asyncio.ensure_future(
            harness_ref[0].commit(
                lambda tx: tx.append_entry(runtime.conversation_id, {"kind": "blocker"}), context
            )
        )
        before.append(asyncio.ensure_future(runtime.commit(lambda: completed("before"), context)))

        def later() -> None:
            after.append(asyncio.ensure_future(runtime.commit(lambda: completed("after"), context)))

        asyncio.get_running_loop().call_later(0.002, later)

    Detached = one_step("test.detached", run)
    opened = await open_root([Detached], {"storage": storage})
    harness_ref.append(opened["harness"])
    id = await start(opened["root"], Detached)
    opened["harness"].resume()
    await eventually(lambda: bool(held))
    await held[0].entered.wait()
    await eventually(lambda: bool(after))
    held[0].release()
    await before[0]
    receipt = await opened["harness"].wait_for_task(id, context)
    assert receipt["state"]["outcome"] == {"status": "completed", "result": "before"}
    with pytest.raises(Exception, match="invocation has ended|is terminal"):
        await after[0]
    await opened["harness"].close(context)


@pytest.mark.parametrize("recovery", [False])
async def test_fails_the_harness_on_a_failed_reservation_commit(recovery: bool) -> None:
    runs = [0]

    async def once(task: Any, runtime: Any, ctx: Any) -> None:
        runs[0] += 1
        await runtime.commit(lambda: completed(None), ctx)

    Once = one_step("test.once", once)
    storage = ControlledStorage()
    opened = await open_root([Once], {"storage": storage})
    id = await start(opened["root"], Once)
    waiting = asyncio.ensure_future(opened["harness"].wait_for_task(id, context))
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    opened["harness"].resume()
    assert await opened["harness"].closed == {"reason": "failed", "error": failure}
    assert opened["reports"] == [failure]
    with pytest.raises(Exception) as error:
        await waiting
    assert getattr(error.value, "name", None) == "SessionFailed"
    assert getattr(error.value, "cause", None) is failure
    with pytest.raises(Exception) as error:
        await opened["harness"].get_task(id, context)
    assert getattr(error.value, "name", None) == "SessionFailed"
    assert getattr(error.value, "cause", None) is failure
    assert runs[0] == 0
    await opened["harness"].close(context)

    reopened = await open_root([Once], {"storage": storage.reopen()})
    reopened["harness"].resume()
    assert (await reopened["harness"].wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    assert runs[0] == 1
    await reopened["harness"].close(context)


async def test_fails_the_harness_instead_of_running_a_task_again_when_its_fault_write_fails() -> None:
    runs = [0]
    storage = ControlledStorage()
    failure = RuntimeError("disk gone")

    async def throws(task: Any, runtime: Any, ctx: Any) -> None:
        runs[0] += 1
        if runs[0] == 1:
            storage.fail_next_commit(failure)
        raise RuntimeError("boom")

    Throws = one_step("test.failed-fault", throws)
    opened = await open_root([Throws], {"storage": storage})
    id = await start(opened["root"], Throws)
    opened["harness"].resume()
    assert await opened["harness"].closed == {"reason": "failed", "error": failure}
    assert runs[0] == 1
    assert opened["reports"] == [failure]
    await opened["harness"].close(context)
    reopened = await open_root([Throws], {"storage": storage.reopen()})
    reopened["harness"].resume()
    assert (await reopened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "faulted",
        "error": {"message": "boom"},
    }
    assert runs[0] == 2
    await reopened["harness"].close(context)


async def test_waits_for_idleness_counting_blocked_work_and_ignoring_background() -> None:
    gates: dict[TaskId, Any] = {}

    async def gated(task: Any, runtime: Any, ctx: Any) -> None:
        gate = deferred()
        gates[task["id"]] = gate
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    Gated = one_step("test.gated", gated)
    opened = await open_root([Gated])
    await opened["harness"].wait_for_idle(context)
    other = await opened["harness"].create_conversation({"ownership": {"kind": "ownerless"}}, context)
    foreground = await start(opened["root"], Gated)
    background = await start(opened["root"], Gated, background=True)
    elsewhere = await start(other, Gated)

    controller = AbortController()
    cancelled = asyncio.ensure_future(opened["harness"].wait_for_idle(with_signal(controller.signal)))
    controller.abort(RuntimeError("stop waiting"))
    with pytest.raises(Exception, match="stop waiting"):
        await cancelled
    already = AbortController()
    already.abort(RuntimeError("already cancelled"))
    with pytest.raises(Exception, match="already cancelled"):
        await opened["root"].wait_for_idle(with_signal(already.signal))

    opened["harness"].resume()
    await eventually(lambda: _check(lambda: len(gates) == 3))
    root_idle = asyncio.ensure_future(opened["root"].wait_for_idle(context))
    harness_idle = asyncio.ensure_future(opened["harness"].wait_for_idle(context))
    gates[foreground].resolve()
    await root_idle
    assert await settled(harness_idle) is False
    gates[elsewhere].resolve()
    await harness_idle
    assert (await opened["harness"].get_task(background, context))["state"]["status"] == "running"
    gates[background].resolve()
    await opened["harness"].wait_for_task(background, context)
    await opened["harness"].close(context)
    with pytest.raises(Exception, match="closed"):
        await opened["harness"].wait_for_idle(context)
    with pytest.raises(Exception, match="closed"):
        await opened["root"].wait_for_idle(context)


async def test_rejects_unknown_tasks_and_reports_terminal_tasks() -> None:
    Done = one_step("test.quick", lambda task, runtime, ctx: runtime.commit(lambda: completed(None), ctx))
    opened = await open_root([Done])
    unknown = TaskId(999_999)
    assert await opened["harness"].get_task(unknown, context) is None
    with pytest.raises(Exception, match="does not exist"):
        await opened["harness"].wait_for_task(unknown, context)
    with pytest.raises(Exception, match="does not exist"):
        await opened["harness"].abort_task(unknown, context)
    id = await start(opened["root"], Done)
    opened["harness"].resume()
    await opened["harness"].wait_for_task(id, context)
    assert await opened["harness"].wait_for_task(id, context) is not None
    assert await opened["harness"].abort_task(id, context) == "terminal"
    await opened["harness"].close(context)
    with pytest.raises(Exception, match="closed"):
        opened["harness"].resume()


async def test_abort_rejects_the_runs_commits_and_memo_writes_after_the_mark() -> None:
    reached = deferred()
    errors: list[str] = []
    memo_read: list[Any] = []
    abort_runtimes: list[Any] = []
    run_runtime: list[Any] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        run_runtime.append(runtime)
        await runtime.memo("kept", 1, ctx)
        reached.resolve()
        await _wait_signal(runtime.signal)
        memo_read.append(await runtime.memo("kept", context))
        errors.append(str(await _capture(runtime.memo("late", 2, context))))
        errors.append(str(await _capture(runtime.commit(lambda: completed(None), context))))

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        abort_runtimes.append(runtime)

        def change(tx: Any, current: Any) -> Any:
            assert current["abortRequested"] is True
            assert current["memos"] == {"kept": 1}
            return aborted_with("mark")

        await runtime.commit(change, ctx)

    Marked = one_step("test.marked", run, abort)
    opened = await open_root([Marked])
    id = await start(opened["root"], Marked)
    opened["harness"].resume()
    await reached.promise
    assert await opened["harness"].abort_task(id, context) == "marked"
    outcome = (await opened["harness"].wait_for_task(id, context))["state"]["outcome"]
    assert outcome == {"status": "aborted", "reason": "mark"}
    assert memo_read == [1]
    assert errors == [f"Task {id} has a durable abort mark", f"Task {id} has a durable abort mark"]
    assert len(abort_runtimes) == 1
    assert abort_runtimes[0] is not run_runtime[0]
    await opened["harness"].close(context)


async def _wait_signal(signal: Any) -> None:
    await signal.wait()


async def test_starts_no_further_phase_after_a_mark_with_progress() -> None:
    reached = deferred()
    proceed = deferred()
    phases: list[str] = []

    async def one(task: Any, runtime: Any, ctx: Any) -> None:
        phases.append("one")
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "two"}}, ctx)
        reached.resolve()
        await proceed.promise

    async def two(task: Any, runtime, ctx):
        phases.append("two")

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        phases.append("abort")
        await runtime.commit(lambda: aborted_with("boundary"), ctx)

    Two = define_task(
        {
            "name": "test.mark-boundary",
            "version": 1,
            "initial": lambda _input: {"phase": "one"},
            "phases": {"one": one, "two": two},
            "abort": abort,
        }
    )
    opened = await open_root([Two])
    id = await opened["root"].commit(
        lambda tx: tx.create_task(Two, None, {"ownership": {"kind": "conversation"}}), context
    )
    opened["harness"].resume()
    await reached.promise
    aborting = await mark_durably(opened["harness"], id)
    proceed.resolve()
    assert await aborting == "marked"
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "boundary",
    }
    assert phases == ["one", "abort"]
    await opened["harness"].close(context)


async def test_signals_and_joins_the_run_before_returning() -> None:
    reached = deferred()
    state = {"run_ended": False}

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        reached.resolve()
        try:
            await _wait_signal(runtime.signal)
        finally:
            state["run_ended"] = True

    Signalled = one_step("test.signalled", run)
    opened = await open_root([Signalled])
    id = await start(opened["root"], Signalled)
    opened["harness"].resume()
    await reached.promise
    assert await opened["harness"].abort_task(id, context) == "marked"
    assert state["run_ended"] is True
    assert await opened["harness"].abort_task(id, context) == "marked"
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "test",
    }
    await opened["harness"].close(context)


async def test_aborts_waiting_work_and_faults_bad_abort_handlers() -> None:
    gate = deferred()

    async def first_run(task: Any, runtime: Any, ctx: Any) -> None:
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    First = one_step("test.dependency", first_run)
    first_holder: list[TaskId] = []

    async def wait(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {
                "status": "waiting",
                "checkpoint": {"phase": "run"},
                "on": first_holder,
                "policy": "allSettled",
            },
            ctx,
        )

    Lazy = one_step("test.lazy-abort", wait, lambda task, runtime, ctx: _noop())
    Throwing = one_step(
        "test.throwing-abort",
        wait,
        lambda task, runtime, ctx: _raise(RuntimeError("abort failed")),
    )
    opened = await open_root([First, Lazy, Throwing])
    first_holder.append(await start(opened["root"], First))
    lazy = await start(opened["root"], Lazy)
    throwing = await start(opened["root"], Throwing)
    opened["harness"].resume()
    await eventually(lambda: _state_is(opened["harness"], throwing, "waiting"))
    await eventually(lambda: _state_is(opened["harness"], lazy, "waiting"))
    await opened["harness"].abort_task(lazy, context)
    await opened["harness"].abort_task(throwing, context)
    assert (await opened["harness"].wait_for_task(lazy, context))["state"]["outcome"] == {
        "status": "faulted",
        "error": {"message": f"Abort handler of task {lazy} returned without a terminal outcome"},
    }
    assert (await opened["harness"].wait_for_task(throwing, context))["state"]["outcome"] == {
        "status": "faulted",
        "error": {"message": "abort failed"},
    }
    assert (await opened["harness"].get_task(first_holder[0], context))["state"]["status"] == "running"
    gate.resolve()
    await opened["harness"].wait_for_task(first_holder[0], context)
    await opened["harness"].close(context)


async def _noop() -> None:
    return None


async def _state_is(harness: Any, id: TaskId, status: str) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == status


async def test_does_not_signal_a_running_abort_handler_when_aborted_again() -> None:
    reached = deferred()
    proceed = deferred()
    run_release = deferred()
    state: dict[str, Any] = {"abort_signalled": None, "aborts": 0}

    started = deferred()

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        started.resolve()
        await run_release.promise

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        state["aborts"] += 1
        reached.resolve()
        await proceed.promise
        state["abort_signalled"] = runtime.signal.aborted
        await runtime.commit(lambda: aborted_with("once"), ctx)

    Run = one_step("test.abort-again", run, abort)
    opened = await open_root([Run])
    id = await start(opened["root"], Run)
    opened["harness"].resume()
    await started.promise
    controller = AbortController()
    cancelled = asyncio.ensure_future(opened["harness"].abort_task(id, with_signal(controller.signal)))
    while not (await opened["harness"].get_task(id, context)).get("abortRequested"):
        await flush()
    controller.abort(RuntimeError("caller gave up"))
    with pytest.raises(Exception, match="caller gave up"):
        await cancelled
    run_release.resolve()
    await reached.promise
    assert await opened["harness"].abort_task(id, context) == "marked"
    proceed.resolve()
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "once",
    }
    assert state["abort_signalled"] is False
    assert state["aborts"] == 1
    await opened["harness"].close(context)


async def test_never_starts_phase_one_when_the_abort_lands_while_reservation_settles() -> None:
    state = {"ran": False}

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        state["ran"] = True
        await _wait_signal(runtime.signal)

    Reserved = one_step("test.reserved", run)
    storage = ControlledStorage()
    opened = await open_root([Reserved], {"storage": storage})
    id = await start(opened["root"], Reserved)
    held = storage.hold_commits()
    opened["harness"].resume()
    await held.entered.wait()
    aborting = asyncio.ensure_future(opened["harness"].abort_task(id, context))
    held.release()
    assert await aborting == "marked"
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "test",
    }
    assert state["ran"] is False
    await opened["harness"].close(context)


async def test_abort_mark_wins_over_a_racing_fault() -> None:
    storage = ControlledStorage()
    state: dict[str, Any] = {}
    harness_ref: list[Any] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        gate = storage.hold_commits()
        state["held"] = gate
        state["marking"] = asyncio.ensure_future(harness_ref[0].abort_task(task["id"], context))
        await gate.entered.wait()
        raise RuntimeError("would fault")

    Racing = one_step("test.racing-fault", run)
    opened = await open_root([Racing], {"storage": storage})
    harness_ref.append(opened["harness"])
    id = await start(opened["root"], Racing)
    opened["harness"].resume()
    await eventually(lambda: bool(state))
    await state["held"].entered.wait()
    await flush()
    state["held"].release()
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "test",
    }
    assert await state["marking"] == "marked"
    await opened["harness"].close(context)


async def test_keeps_memos_durable_across_reopen() -> None:
    storage = ControlledStorage()
    gate = deferred()
    child_id: list[TaskId] = []

    async def child_run(task: Any, runtime: Any, ctx: Any) -> None:
        waiting = asyncio.ensure_future(gate.promise)
        stopped = asyncio.ensure_future(runtime.signal.wait())
        done, _ = await asyncio.wait((waiting, stopped), return_when=asyncio.FIRST_COMPLETED)
        if stopped in done:
            stopped.result()
            waiting.cancel()
            return
        stopped.cancel()
        await runtime.commit(lambda: completed(None), ctx)

    Child = one_step("test.memo-child", child_run)

    async def parent_run(task: Any, runtime: Any, ctx: Any) -> None:
        if task["state"]["checkpoint"]["phase"] == "one":
            await runtime.memo("choice", "a", ctx)

            async def wait(tx: Any) -> Any:
                child_id.append(
                    await tx.create_task(Child, None, {"ownership": {"kind": "task", "taskId": task["id"]}})
                )
                return {
                    "status": "waiting",
                    "checkpoint": {"phase": "two"},
                    "on": list(child_id),
                    "policy": "allSettled",
                }

            await runtime.commit(wait, ctx)
        else:
            assert await runtime.memo("choice", ctx) == "a"
            await runtime.commit(lambda: completed("done"), ctx)

    Parent = define_task(
        {
            "name": "test.memo-parent",
            "version": 1,
            "initial": lambda _input: {"phase": "one"},
            "phases": {"one": parent_run, "two": parent_run},
            "abort": lambda task, runtime, ctx: runtime.commit(lambda: aborted_with("test"), ctx),
        }
    )
    opened = await open_root([Parent, Child], {"storage": storage})
    parent = await start(opened["root"], Parent)
    opened["harness"].resume()
    await eventually(lambda: _check(lambda: bool(child_id)))
    await eventually(lambda: _state_is(opened["harness"], parent, "waiting"))
    await opened["harness"].close(context)

    reopened = await open_root([Parent, Child], {"storage": storage.reopen()})
    reopened["harness"].resume()
    await eventually(lambda: _state_is(reopened["harness"], parent, "waiting"))
    gate.resolve()
    assert (await reopened["harness"].wait_for_task(parent, context))["state"]["outcome"] == {
        "status": "completed",
        "result": "done",
    }
    await reopened["harness"].close(context)


async def test_rejects_invalid_waits_and_fail_fast_without_ownership() -> None:
    async def self_wait(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(
            lambda: {
                "status": "waiting",
                "checkpoint": {"phase": "run"},
                "on": [task["id"]],
                "policy": "allSettled",
            },
            ctx,
        )

    SelfWaiter = one_step("test.self-wait", self_wait)
    MissingWaiter = one_step(
        "test.missing-wait",
        lambda task, runtime, ctx: runtime.commit(
            lambda: {
                "status": "waiting",
                "checkpoint": {"phase": "run"},
                "on": [TaskId(999_999)],
                "policy": "allSettled",
            },
            ctx,
        ),
    )
    opened = await open_root([SelfWaiter, MissingWaiter])
    a = await start(opened["root"], SelfWaiter)
    b = await start(opened["root"], MissingWaiter)
    opened["harness"].resume()
    a_outcome = (await opened["harness"].wait_for_task(a, context))["state"]["outcome"]
    b_outcome = (await opened["harness"].wait_for_task(b, context))["state"]["outcome"]
    assert a_outcome["status"] == "faulted"
    assert "wait cycle" in a_outcome["error"]["message"]
    assert b_outcome == {
        "status": "faulted",
        "error": {"message": "Task 999999 does not exist"},
    }
    await opened["harness"].close(context)


async def test_pages_the_newest_tasks_first() -> None:
    Idle = one_step("test.idle", lambda task, runtime, ctx: asyncio.sleep(0))
    opened = await open_root([Idle])
    ids = [await start(opened["root"], Idle) for _ in range(5)]
    first = await opened["harness"].commit(lambda tx: tx.scan_tasks({"order": "descending"}, 2), context)
    assert [item["id"] for item in first["items"]] == [ids[4], ids[3]]
    second = await opened["harness"].commit(lambda tx: tx.scan_tasks({}, 2, first["next"]), context)
    assert [item["id"] for item in second["items"]] == [ids[2], ids[1]]
    await opened["harness"].close(context)


async def test_rejects_task_waits_cancelled_or_closed_while_queued() -> None:
    gate = deferred()

    async def blocking(task: Any, runtime: Any, ctx: Any) -> None:
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    Blocking = one_step("test.wait-close", blocking)
    storage = ControlledStorage()
    opened = await open_root([Blocking], {"storage": storage})
    id = await start(opened["root"], Blocking)

    held = storage.hold_commits()
    blocker = asyncio.ensure_future(
        opened["harness"].commit(lambda tx: tx.append_entry(opened["root"].id, {"kind": "blocker"}), context)
    )
    await held.entered.wait()
    controller = AbortController()
    cancelled_while_queued = asyncio.ensure_future(
        opened["harness"].wait_for_task(id, with_signal(controller.signal))
    )
    controller.abort(RuntimeError("wait cancelled"))
    held.release()
    await blocker
    with pytest.raises(Exception, match="wait cancelled"):
        await cancelled_while_queued

    pending = asyncio.ensure_future(opened["harness"].wait_for_task(id, context))
    idle = asyncio.ensure_future(opened["harness"].wait_for_idle(context))
    conversation_idle = asyncio.ensure_future(opened["root"].wait_for_idle(context))
    await flush()
    held_again = storage.hold_commits()
    blocker_again = asyncio.ensure_future(
        opened["harness"].commit(lambda tx: tx.append_entry(opened["root"].id, {"kind": "blocker"}), context)
    )
    await held_again.entered.wait()
    closed_while_queued = asyncio.ensure_future(opened["harness"].wait_for_task(id, context))
    closing = asyncio.ensure_future(opened["harness"].close(context))
    held_again.release()
    await blocker_again
    for wait in (closed_while_queued, pending, idle, conversation_idle):
        with pytest.raises(Exception, match="closed"):
            await wait
    gate.resolve()
    await closing


async def test_close_stops_without_outcomes_and_starts_no_fresh_phase_or_abort() -> None:
    reached = deferred()
    proceed = deferred()
    phases: list[str] = []
    state = {"abort_ran": False}

    async def one(task: Any, runtime: Any, ctx: Any) -> None:
        phases.append("one")
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "two"}}, ctx)
        reached.resolve()
        await proceed.promise

    async def two(task: Any, runtime: Any, ctx: Any) -> None:
        phases.append("two")

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        state["abort_ran"] = True

    Two = define_task(
        {
            "name": "test.two",
            "version": 1,
            "initial": lambda _input: {"phase": "one"},
            "phases": {"one": one, "two": two},
            "abort": abort,
        }
    )
    storage = ControlledStorage()
    opened = await open_root([Two], {"storage": storage})
    id = await opened["root"].commit(
        lambda tx: tx.create_task(Two, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    await reached.promise
    aborting = await mark_durably(opened["harness"], id)
    record = await opened["harness"].get_task(id, context)
    commits = len(storage.commits)
    closing = asyncio.ensure_future(opened["harness"].close(context))
    proceed.resolve()
    await closing
    assert await aborting == "marked"
    # No outcome: close writes only the stopped task back as pending, so a reopen counts no restart.
    assert len(storage.commits) == commits + 1
    last = [write for write in storage.commits[-1] if write["type"] == "task"]
    assert last == [
        {
            "type": "task",
            "value": {
                **last[0]["value"],
                "abortRequested": True,
                "state": {"status": "pending", "checkpoint": {"phase": "two"}},
            },
        }
    ]
    assert "restarts" not in last[0]["value"]
    assert phases == ["one"]
    assert state["abort_ran"] is False
    assert record["abortRequested"] is True
    assert record["state"] == {"status": "running", "checkpoint": {"phase": "two"}}
    with pytest.raises(Exception, match="closed"):
        await opened["harness"].commit(lambda: None, context)


async def test_close_seals_admission_before_signalling_handlers_and_stops_watches() -> None:
    Notes = define_doc(
        {
            "kind": "test.close-notes",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    reached = deferred()
    state: dict[str, Any] = {}

    async def stubborn(task: Any, runtime: Any, ctx: Any) -> None:
        watch = await runtime.watch_doc(Notes, ctx)
        state["watch"] = watch

        def on_abort() -> None:
            state["from_listener"] = asyncio.ensure_future(opened["harness"].commit(lambda: None, context))

        runtime.signal.add_listener(on_abort)
        reached.resolve()
        state["watch_end"] = await watch.closed

    Stubborn = one_step("test.stubborn", stubborn)
    opened = await open_root([Stubborn])

    async def write(tx: Any) -> None:
        (await tx.doc(Notes)).text = "x"

    await opened["harness"].commit(write, context)
    await start(opened["root"], Stubborn)
    opened["harness"].resume()
    await reached.promise
    await opened["harness"].close(context)
    with pytest.raises(Exception, match="closed"):
        await state["from_listener"]
    assert state["watch_end"] == {"reason": "session_closed"}


async def test_writes_no_fault_when_close_seals_before_the_failed_phase_step() -> None:
    storage = ControlledStorage()
    state: dict[str, Any] = {}

    async def throws(task: Any, runtime: Any, ctx: Any) -> None:
        state["held"] = storage.hold_commits()
        asyncio.ensure_future(
            opened["harness"].commit(
                lambda tx: tx.append_entry(runtime.conversation_id, {"kind": "blocker"}),
                context,
            )
        )
        await state["held"].entered.wait()
        raise RuntimeError("would fault")

    Throws = one_step("test.close-fault", throws)
    opened = await open_root([Throws], {"storage": storage})
    id = await start(opened["root"], Throws)
    opened["harness"].resume()
    await eventually(lambda: bool(state))
    await state["held"].entered.wait()
    await flush()
    closing = asyncio.ensure_future(opened["harness"].close(context))
    state["held"].release()
    await closing
    task_writes = [
        write
        for writes in storage.commits
        for write in writes
        if write["type"] == "task" and write["value"]["id"] == id
    ]
    # Created, reserved, and written back as pending by close: no fault.
    assert [write["value"]["state"]["status"] for write in task_writes] == [
        "pending",
        "running",
        "pending",
    ]


async def test_starts_no_abort_handler_whose_reservation_settles_while_closing() -> None:
    state = {"ran": False}

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        return None

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        state["ran"] = True

    Marked = one_step("test.close-abort-reservation", run, abort)
    storage = ControlledStorage()
    opened = await open_root([Marked], {"storage": storage})
    id = await start(opened["root"], Marked)
    assert await opened["harness"].abort_task(id, context) == "marked"
    held = storage.hold_commits()
    opened["harness"].resume()
    await held.entered.wait()
    closing = asyncio.ensure_future(opened["harness"].close(context))
    held.release()
    await closing
    assert state["ran"] is False
    # The reservation landed; close wrote the task back as pending.
    reserved = [write for write in storage.commits[-2] if write["type"] == "task"]
    assert reserved[0]["value"]["id"] == id
    assert reserved[0]["value"]["abortRequested"] is True
    assert reserved[0]["value"]["state"]["status"] == "running"
    written = [write for write in storage.commits[-1] if write["type"] == "task"]
    assert written[0]["value"]["id"] == id
    assert written[0]["value"]["abortRequested"] is True
    assert written[0]["value"]["state"]["status"] == "pending"


async def test_rejects_a_runtime_commit_queued_on_the_line_when_close_sealed() -> None:
    storage = ControlledStorage()
    state: dict[str, Any] = {}
    proceed = deferred()

    async def queued(task: Any, runtime: Any, ctx: Any) -> None:
        state["held"] = storage.hold_commits()
        asyncio.ensure_future(
            opened["harness"].commit(
                lambda tx: tx.append_entry(runtime.conversation_id, {"kind": "blocker"}),
                context,
            )
        )
        state["queued"] = asyncio.ensure_future(runtime.commit(lambda: completed(None), ctx))
        await proceed.promise

    Queued = one_step("test.close-queued-commit", queued)
    opened = await open_root([Queued], {"storage": storage})
    id = await start(opened["root"], Queued)
    opened["harness"].resume()
    await eventually(lambda: bool(state))
    await state["held"].entered.wait()
    await flush()
    closing = asyncio.ensure_future(opened["harness"].close(context))
    state["held"].release()
    with pytest.raises(Exception, match="Harness is closed"):
        await state["queued"]
    proceed.resolve()
    await closing
    task_writes = [
        write
        for writes in storage.commits
        for write in writes
        if write["type"] == "task" and write["value"]["id"] == id
    ]
    # Created, reserved, and written back as pending by close; the queued outcome never landed.
    assert [write["value"]["state"]["status"] for write in task_writes] == [
        "pending",
        "running",
        "pending",
    ]


async def test_starts_no_next_phase_when_close_seals_during_a_continuing_step() -> None:
    phases: list[str] = []
    base = create_registry()
    state: dict[str, Any] = {"close_on_snapshot": False, "closing": None}

    class Reader:
        def snapshot(self) -> Any:
            if state["close_on_snapshot"]:
                state["close_on_snapshot"] = False
                state["closing"] = asyncio.ensure_future(opened["harness"].close(context))
            return base.snapshot()

        def subscribe(self, listener: Any) -> Any:
            return base.subscribe(listener)

    async def one(task: Any, runtime: Any, ctx: Any) -> None:
        phases.append("one")
        await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "two"}}, ctx)
        state["close_on_snapshot"] = True

    async def two(task: Any, runtime: Any, ctx: Any) -> None:
        phases.append("two")

    Two = define_task(
        {
            "name": "test.close-in-step",
            "version": 1,
            "initial": lambda _input: {"phase": "one"},
            "phases": {"one": one, "two": two},
            "abort": lambda task, runtime, ctx: asyncio.sleep(0),
        }
    )
    base.install(define_extension({"name": "tasks", "tasks": [Two]}))
    opened = {"harness": await Harness.open(MemoryStorage(), {"models": {}, "registry": Reader()}, context)}
    root = await opened["harness"].root(context)
    await root.commit(
        lambda tx: tx.create_task(Two, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    await eventually(lambda: state["closing"] is not None)
    await state["closing"]
    assert phases == ["one"]


async def test_joins_a_reservation_that_settles_while_closing_without_starting_its_handler() -> None:
    state = {"ran": False}

    async def never(task: Any, runtime: Any, ctx: Any) -> None:
        state["ran"] = True

    Never = one_step("test.close-reservation", never)
    storage = ControlledStorage()
    opened = await open_root([Never], {"storage": storage})
    id = await start(opened["root"], Never)
    held = storage.hold_commits()
    opened["harness"].resume()
    await held.entered.wait()
    closing = asyncio.ensure_future(opened["harness"].close(context))
    held.release()
    await closing
    assert state["ran"] is False
    # The reservation landed; close wrote the task back as pending.
    reserved = [write for write in storage.commits[-2] if write["type"] == "task"]
    assert reserved[0]["value"]["id"] == id
    assert reserved[0]["value"]["state"]["status"] == "running"
    written = [write for write in storage.commits[-1] if write["type"] == "task"]
    assert written[0]["value"]["id"] == id
    assert written[0]["value"]["state"]["status"] == "pending"

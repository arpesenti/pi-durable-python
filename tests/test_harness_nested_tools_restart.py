"""Translated upstream ``harness-nested-tools-restart`` cases.

A replay-safe caller reattaches to the nested calls it made before a restart by key; any other
caller never resumes, so its unfinished nested calls and the child tasks they created are abandoned
with ``abandonOnRestart``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

from chat_support import chat_setup, open_chat
from session_support import context
from task_support import aborted, eventually
from tool_support import (
    DONE,
    add_hooks,
    add_task,
    add_tool,
    all_entries,
    calls,
    codes,
    results,
    tool,
)

from pi_durable import LiveDoc, ToolTask, define_extension, define_task
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage


def call(name: str, call_id: str = "c1") -> dict[str, Any]:
    return calls([name, {}, call_id])


async def open_path(path: Path, setup: Any) -> dict[str, Any]:
    storage = await open_posix_sqlite_storage(str(path))
    return await open_chat(storage, setup)


async def nested_tasks(harness: Any) -> list[dict[str, Any]]:
    page = await harness.commit(lambda tx: tx.scan_tasks({"kind": "pi.tool"}, 100), context)
    return [task for task in page["items"] if task["input"].get("kind") == "nested"]


def capture_nested_results(harness: Any) -> dict[str, dict[str, Any]]:
    """Stored nested results by call ID, captured from commit publications."""

    stored: dict[str, dict[str, Any]] = {}

    def capture(publication: Any, _listener_context: Any) -> None:
        for change in publication["changes"]:
            if change["type"] != "document" or change["record"]["kind"] != "pi.tool.nested-call":
                continue
            value = cast(dict[str, Any] | None, change["value"])
            if value is not None and "result" in value:
                stored[str(change["record"]["key"])] = value["result"]

    harness.subscribe_commits(capture)
    return stored


async def test_abandons_a_replay_unsafe_callers_unfinished_nested_calls_before_they_run_again(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    runs = {"started": 0, "gated": 0}
    running = asyncio.Event()
    gated = asyncio.Event()

    async def started(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        runs["started"] += 1
        api.output("partial\n")
        await api.details({"step": 1}, call_context)
        running.set()
        await aborted(call_context.abort_signal)
        return {}

    # Replay-safe, so only the abandonment keeps it from rerunning.
    add_tool(setup.registry, tool("started", started, replay="safe"))

    def gated_tool(_args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        runs["gated"] += 1
        return {}

    add_tool(setup.registry, tool("gated", gated_tool))

    async def before_tool(hook_call: dict[str, Any], _api: Any, hook_context: Any) -> Any:
        if hook_call["name"] != "gated":
            return None
        gated.set()
        await aborted(hook_context.abort_signal)
        return None

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool})

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await asyncio.gather(
            api.execute_tool("started", {}, call_context),
            api.execute_tool("gated", {}, call_context),
        )
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await running.wait()
    await gated.wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    stored = capture_nested_results(harness)
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    await harness.wait_for_idle(context)
    assert runs == {"started": 1, "gated": 0}
    started_task, gate_task = await nested_tasks(harness)
    for task in (started_task, gate_task):
        assert task["abandonOnRestart"] is True
        assert task["abortRequested"] is True
        assert task["abortReason"] == "restart"
        assert task["state"]["status"] == "terminal"
        assert task["state"]["outcome"]["status"] == "aborted"
    assert codes(stored["1"]) == ["interrupted"]
    assert stored["1"]["isError"] is True
    assert stored["1"]["details"] == {"step": 1}
    assert "structuredOutput" not in stored["1"]
    assert codes(stored["2"]) == ["abandoned"]
    # The caller reports the interruption.
    [result] = results(await all_entries(root))
    assert result["toolCallId"] == "c1"
    assert result["isError"] is True
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await harness.close(context)


async def test_abandons_the_work_an_abandoned_nested_call_owns_which_never_runs_a_phase_again(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    phases: list[str] = []
    aborts: list[str | None] = []
    started = asyncio.Event()

    async def work(_task: Any, runtime: Any, _context: Any) -> None:
        phases.append("work")
        started.set()
        await aborted(runtime.signal)

    async def abort(task: Any, runtime: Any, abort_context: Any) -> None:
        aborts.append(task.get("abortReason"))
        await runtime.commit(
            lambda _tx: {"status": "terminal", "outcome": {"status": "aborted"}}, abort_context
        )

    Worker = define_task(
        {
            "name": "test.worker",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": abort,
        }
    )
    setup.registry.install(define_extension({"name": "worker", "tasks": [Worker]}))
    workers: list[Any] = []

    async def spawn(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        workers.append(
            await api.create_task(
                Worker, None, {"ownership": {"kind": "task", "taskId": api.task_id}}, call_context
            )
        )
        await api.wait_for_task(workers[-1], call_context)
        return {}

    add_tool(setup.registry, tool("spawn", spawn))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("spawn", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await started.wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    await harness.wait_for_idle(context)
    assert phases == ["work"]
    # The cascade passed the restart reason on.
    assert aborts == ["restart"]
    worker = await harness.get_task(workers[0], context)
    assert worker["state"]["status"] == "terminal"
    assert worker["state"]["outcome"]["status"] == "aborted"
    await harness.close(context)


async def test_keeps_an_abandoned_task_whose_definition_is_missing_waiting_until_it_is_installed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    aborts: list[str | None] = []
    started = asyncio.Event()

    async def work(_task: Any, runtime: Any, _context: Any) -> None:
        started.set()
        await aborted(runtime.signal)

    async def abort(task: Any, runtime: Any, abort_context: Any) -> None:
        aborts.append(task.get("abortReason"))
        await runtime.commit(
            lambda _tx: {"status": "terminal", "outcome": {"status": "aborted"}}, abort_context
        )

    Worker = define_task(
        {
            "name": "test.late",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": abort,
        }
    )
    extension = add_task(setup.registry, Worker)
    workers: list[Any] = []

    async def spawn(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        workers.append(
            await api.create_task(
                Worker, None, {"ownership": {"kind": "task", "taskId": api.task_id}}, call_context
            )
        )
        await api.wait_for_task(workers[-1], call_context)
        return {}

    add_tool(setup.registry, tool("spawn", spawn))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("spawn", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await started.wait()
    await harness.close(context)

    setup.registry.uninstall(extension)
    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()

    async def blocked() -> bool:
        inspected = await harness.inspect(context)
        found = next((task for task in inspected["tasks"] if task["record"]["id"] == workers[0]), None)
        return found is not None and found["state"]["kind"] == "blocked"

    await eventually(blocked)
    record = await harness.get_task(workers[0], context)
    assert record["state"]["status"] != "terminal"
    assert aborts == []
    setup.registry.install(extension)
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    assert aborts == ["restart"]
    worker = await harness.get_task(workers[0], context)
    assert worker["state"]["status"] == "terminal"
    assert worker["state"]["outcome"]["status"] == "aborted"
    await harness.close(context)


async def test_orphans_an_abandoned_task_whose_definition_is_missing_once_an_abort_is_requested(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    started = asyncio.Event()

    async def work(_task: Any, runtime: Any, _context: Any) -> None:
        started.set()
        await aborted(runtime.signal)

    async def abort(_task: Any, runtime: Any, abort_context: Any) -> None:
        await runtime.commit(
            lambda _tx: {"status": "terminal", "outcome": {"status": "aborted"}},
            abort_context,
        )

    Worker = define_task(
        {
            "name": "test.gone",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": abort,
        }
    )
    extension = add_task(setup.registry, Worker)
    workers: list[Any] = []

    async def spawn(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        workers.append(
            await api.create_task(
                Worker, None, {"ownership": {"kind": "task", "taskId": api.task_id}}, call_context
            )
        )
        await api.wait_for_task(workers[-1], call_context)
        return {}

    add_tool(setup.registry, tool("spawn", spawn))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("spawn", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await started.wait()
    await harness.close(context)

    setup.registry.uninstall(extension)
    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()

    async def marked() -> bool:
        record = await harness.get_task(workers[0], context)
        return record is not None and record.get("abortReason") == "restart"

    await eventually(marked)
    await root_abort(harness, opened["root"])
    worker = await harness.get_task(workers[0], context)
    assert worker["state"]["status"] == "terminal"
    assert worker["state"]["outcome"] == {"status": "orphaned", "reason": "missing_task"}
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "unanswered"
    await harness.close(context)


async def root_abort(harness: Any, root: Any) -> None:
    await root.abort(context)


async def test_marks_nothing_when_the_reopened_harness_never_starts_scheduling(tmp_path: Path) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    running = asyncio.Event()

    async def hang(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        running.set()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("hang", hang))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("hang", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await root.submit({"type": "input", "content": "go"}, context)
    await running.wait()
    await harness.close(context)

    # An inspection-only open: no resume, no submission.
    opened = await open_path(path, setup)
    inspected = await opened["harness"].inspect(context)
    assert inspected["scheduling"] == "paused"
    await opened["harness"].close(context)

    opened = await open_path(path, setup)
    [nested] = await nested_tasks(opened["harness"])
    assert nested["abandonOnRestart"] is True
    assert nested["abortRequested"] is False
    assert nested["state"]["status"] == "pending"
    await opened["harness"].close(context)


async def test_lets_a_replay_safe_callers_unfinished_nested_calls_recover_and_reattaches_by_key(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    runs = {"safe": 0, "unsafe": 0, "caller": 0}
    running = {"safe": asyncio.Event(), "unsafe": asyncio.Event()}

    async def safe(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        runs["safe"] += 1
        if runs["safe"] == 1:
            running["safe"].set()
            await aborted(call_context.abort_signal)
        return {"output": [{"type": "text", "text": f"safe run {runs['safe']}"}]}

    add_tool(setup.registry, tool("safe", safe, replay="safe"))

    async def unsafe(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        runs["unsafe"] += 1
        running["unsafe"].set()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("unsafe", unsafe))
    received: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        runs["caller"] += 1
        received.extend(
            await asyncio.gather(
                api.execute_tool("safe", {}, call_context),
                api.execute_tool("unsafe", {}, call_context),
            )
        )
        return {}

    add_tool(setup.registry, tool("batch", batch, replay="safe"))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await running["safe"].wait()
    await running["unsafe"].wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    assert runs == {"safe": 2, "unsafe": 1, "caller": 2}
    # Default keys follow call order, so the rerun found both calls.
    assert len(await nested_tasks(harness)) == 2
    for task in await nested_tasks(harness):
        assert "abandonOnRestart" not in task
        assert "abortReason" not in task
    assert received[0]["structuredOutput"] == "safe run 2"
    assert codes(received[1]) == ["interrupted"]
    await harness.close(context)


async def test_clears_the_slots_of_nested_calls_an_earlier_attempt_finished_when_the_rerun_makes_none(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    finished = asyncio.Event()
    attempts = {"count": 0}

    def quick(_args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        return {"output": [{"type": "text", "text": "ok"}]}

    add_tool(setup.registry, tool("quick", quick))

    async def caller(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        attempts["count"] += 1
        if attempts["count"] == 1:
            await api.execute_tool("quick", {}, call_context)
            finished.set()
            await aborted(call_context.abort_signal)
        return {"output": [{"type": "text", "text": "done"}]}

    add_tool(setup.registry, tool("caller", caller, replay="safe"))
    setup.faux.set_responses([call("caller"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await finished.wait()
    # The finished nested call's slot is still listed, below its running caller.
    live = await harness.snapshot(LiveDoc, root.id, context)
    assert len(live["nestedTools"]) == 1
    await harness.close(context)

    opened = await open_path(path, setup)
    harness = opened["harness"]
    observed: dict[str, int] = {}

    def capture(publication: Any, _listener_context: Any) -> None:
        for change in publication["changes"]:
            if (
                change["type"] != "task"
                or change["value"]["kind"] != "pi.tool"
                or change["value"]["input"].get("kind") != "model"
                or change["value"]["state"]["status"] != "terminal"
            ):
                continue
            for sibling in publication["changes"]:
                if sibling["type"] == "document" and sibling["record"]["kind"] == "pi.live":
                    value = cast(dict[str, Any] | None, sibling["value"])
                    observed["slots"] = len((value or {}).get("nestedTools") or ())

    harness.subscribe_commits(capture)
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    assert attempts["count"] == 2
    # The caller's terminal commit removed the slot, though the rerun made no nested call.
    assert observed["slots"] == 0
    await harness.close(context)


async def test_aborts_a_nested_call_an_earlier_attempt_left_running_and_the_rerun_never_made_again(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    running = asyncio.Event()
    attempts = {"count": 0}

    async def slow(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        running.set()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("slow", slow, replay="safe"))

    async def caller(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        attempts["count"] += 1
        # Only the first attempt calls ``slow``; the rerun after the restart returns at once.
        if attempts["count"] == 1:
            await api.execute_tool("slow", {}, call_context, {"key": "slow"})
        return {"output": [{"type": "text", "text": "done"}]}

    add_tool(setup.registry, tool("caller", caller, replay="safe"))
    setup.faux.set_responses([call("caller"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await running.wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    assert attempts["count"] == 2
    # The rerun's settlement found the leftover call among the tasks it owns and aborted it.
    [leftover] = await nested_tasks(harness)
    assert leftover["state"]["status"] == "terminal"
    assert leftover["state"]["outcome"]["status"] == "aborted"
    await harness.close(context)


async def reattach_case(order: str, path: Path) -> None:
    setup = chat_setup()
    runs: list[str] = []
    blocked = asyncio.Event()
    caller_runs = {"count": 0}
    seen: list[Any] = []

    def echo(args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        runs.append(args.get("text") or "")
        return {"output": [{"type": "text", "text": f"echo {args.get('text')}"}]}

    add_tool(setup.registry, tool("echo", echo))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        caller_runs["count"] += 1
        first = "b" if caller_runs["count"] == 2 and order == "swapped" else "a"
        one = await api.execute_tool("echo", {"text": first}, call_context)
        named = await api.execute_tool("echo", {"text": "named"}, call_context, {"key": "named"})
        if caller_runs["count"] == 1:
            blocked.set()
            await aborted(call_context.abort_signal)
        seen.extend([one.get("structuredOutput"), named.get("structuredOutput")])
        return {}

    add_tool(setup.registry, tool("batch", batch, replay="safe"))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await blocked.wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    [result] = results(await all_entries(root))
    if order == "same":
        assert runs == ["a", "named"]
        assert seen == ["echo a", "echo named"]
        assert result["isError"] is False
    else:
        # Key 1 was made with other arguments: the rerun throws instead of reusing a different result.
        assert runs == ["a", "named"]
        assert result["isError"] is True
        assert "Nested call c1/1 was already made with another tool" in str(result["content"])
    await harness.close(context)


async def test_reattaches_by_default_and_explicit_keys_and_rejects_a_rerun_that_calls_differently(
    tmp_path: Path,
) -> None:
    for order in ("same", "swapped"):
        await reattach_case(order, tmp_path / f"session-{order}.sqlite")


async def test_abandons_the_child_tasks_a_replay_unsafe_tool_created_and_awaited_in_memory(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    phases: list[str] = []
    started = asyncio.Event()

    async def work(_task: Any, runtime: Any, _context: Any) -> None:
        phases.append("work")
        started.set()
        await aborted(runtime.signal)

    async def abort(task: Any, runtime: Any, abort_context: Any) -> None:
        phases.append(f"abort {task.get('abortReason')}")
        await runtime.commit(
            lambda _tx: {"status": "terminal", "outcome": {"status": "aborted"}},
            abort_context,
        )

    Worker = define_task(
        {
            "name": "test.direct-worker",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": abort,
        }
    )
    setup.registry.install(define_extension({"name": "direct-worker", "tasks": [Worker]}))

    async def work_tool(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        id = await api.create_task(
            Worker, None, {"ownership": {"kind": "task", "taskId": api.task_id}}, call_context
        )
        await api.wait_for_task(id, call_context)
        return {}

    add_tool(setup.registry, tool("work", work_tool))
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await started.wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    assert phases == ["work", "abort restart"]
    await harness.close(context)


async def test_leaves_background_work_under_an_abandoned_nested_call_running_and_settles_without_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    started = asyncio.Event()

    async def work(_task: Any, runtime: Any, _context: Any) -> None:
        started.set()
        await aborted(runtime.signal)

    async def abort(_task: Any, runtime: Any, abort_context: Any) -> None:
        await runtime.commit(
            lambda _tx: {"status": "terminal", "outcome": {"status": "aborted"}},
            abort_context,
        )

    Background = define_task(
        {
            "name": "test.background",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": abort,
        }
    )
    setup.registry.install(define_extension({"name": "background", "tasks": [Background]}))
    backgrounds: list[Any] = []

    async def spawn(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        # A conversation the call owns, with background work in it, as a persistent subagent has.
        async def change(tx: Any) -> Any:
            child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": api.task_id}})
            return await tx.create_task(
                Background,
                None,
                {
                    "conversationId": child["id"],
                    "ownership": {"kind": "conversation"},
                    "background": True,
                },
            )

        backgrounds.append(await api.commit(change, call_context))
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("spawn", spawn))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("spawn", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await started.wait()
    await harness.close(context)

    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    [spawn_task] = await nested_tasks(harness)
    assert spawn_task["abortReason"] == "restart"
    assert spawn_task["state"]["status"] == "terminal"
    background = await harness.get_task(backgrounds[0], context)
    assert background["abortRequested"] is False
    assert background["state"]["status"] == "running"
    await harness.abort_task(backgrounds[0], context)
    await harness.close(context)


async def test_aborts_left_running_nested_calls_of_a_caller_interrupted_while_it_cleaned_them_up(
    tmp_path: Path,
) -> None:
    path = tmp_path / "session.sqlite"
    setup = chat_setup()
    running = asyncio.Event()
    cleaning = asyncio.Event()
    release = asyncio.Event()
    runs = {"hang": 0}

    async def hang(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        runs["hang"] += 1
        api.output("partial\n")
        await api.details({"step": 1}, call_context)
        running.set()
        try:
            await aborted(call_context.abort_signal)
        except Exception:
            pass
        # The caller's cleanup waits for this abort; hold it there until the Harness closes.
        cleaning.set()
        await release.wait()
        raise Exception("aborted")

    add_tool(setup.registry, tool("hang", hang))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        future = asyncio.ensure_future(api.execute_tool("hang", {}, call_context))
        future.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        await running.wait()
        return {"output": [{"type": "text", "text": "returned early"}]}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_path(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission_id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await cleaning.wait()
    closed = asyncio.ensure_future(harness.close(context))
    release.set()
    await closed

    opened = await open_path(path, setup)
    harness = opened["harness"]
    harness.resume()
    submission = await harness.submission(submission_id, context)
    assert submission is not None
    assert (await submission.wait(context))["status"] == "done"
    await harness.wait_for_idle(context)
    # The caller is replay-unsafe: it reports the interruption, and its cleanup aborts the nested call.
    [result] = results(await all_entries(opened["root"]))
    assert result["toolCallId"] == "c1"
    assert result["isError"] is True
    assert "Tool batch was interrupted" in str(result["content"])
    [child] = await nested_tasks(harness)
    assert child["state"]["status"] == "terminal"
    assert child["state"]["outcome"]["status"] == "aborted"
    assert runs["hang"] == 1
    assert await harness.snapshot(LiveDoc, opened["root"].id, context) == {}
    await harness.close(context)

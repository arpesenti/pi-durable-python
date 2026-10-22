"""Translated upstream ``harness-tools-recovery`` cases for tool intent and replay policy.

A tool records its intent — final arguments and replay policy — before it executes, so a restart
either reruns a replay-safe call or answers it with an interrupted result from its durable partial
output. The suite reopens file-backed SQLite storage between attempts.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any, cast

from chat_support import chat_setup as new_setup
from chat_support import open_chat, text_of
from session_support import context
from task_support import aborted, eventually
from tool_support import (
    DONE,
    add_hooks,
    add_tool,
    all_entries,
    calls,
    result_text,
    results,
    tool,
)

from pi_durable import GenerationTask, LiveDoc, ToolTask
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage


def call(name: str, call_id: str = "c1") -> dict[str, Any]:
    return calls([name, {}, call_id])


def make_work(cwds: list[Any], started: asyncio.Event) -> Any:
    async def work(_args: Any, api: Any, call_context: Any) -> Any:
        cwds.append(api.env["cwd"])
        if len(cwds) == 1:
            started.set()
            await aborted(call_context.abort_signal)
        return {}

    return work


def blocking_tool(name: str, **extra: Any) -> tuple[Any, asyncio.Event, dict[str, int]]:
    """A tool that writes output and details, then blocks until its invocation is cancelled."""

    started = asyncio.Event()
    state = {"runs": 0, "blocking": 1}

    async def execute(_args: Any, api: Any, call_context: Any) -> Any:
        state["runs"] += 1
        api.output(f"run {state['runs']}\n")
        await api.details({"run": state["runs"]}, call_context)
        if state["runs"] <= state["blocking"]:
            started.set()
            await aborted(call_context.abort_signal)
        return {}

    return tool(name, execute, **extra), started, state


async def open_path(path: str, setup: Any, env: Any = None) -> dict[str, Any]:
    storage = await open_posix_sqlite_storage(path)
    opened = await open_chat(storage, setup, **({} if env is None else {"env": env}))
    opened["harness"].resume()
    return opened


async def tool_task_id(opened: dict[str, Any]) -> Any:
    found: dict[str, Any] = {}

    async def check() -> bool:
        live = await opened["harness"].snapshot(LiveDoc, opened["root"].id, context)
        slots = (live or {}).get("tools") or []
        if slots and slots[0].get("taskId") is not None:
            found["id"] = slots[0]["taskId"]
            return True
        return False

    await eventually(check)
    return found["id"]


async def test_answers_an_unsafe_tool_interrupted_after_intent_with_its_durable_output(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    registration, started, state = blocking_tool("work")
    add_tool(setup.registry, registration)
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    submission_id = (
        await opened["root"].submit({"type": "input", "content": "go"}, context)
    ).id
    await started.wait()
    task_id = await tool_task_id(opened)
    await opened["harness"].close(context)

    opened = await open_path(path, setup)
    task = await opened["harness"].get_task(task_id, context)
    assert task["state"]["checkpoint"] == {
        "phase": "execute",
        "arguments": {},
        "replay": "unsafe",
    }
    submission = await opened["harness"].submission(submission_id, context)
    assert (await submission.wait(context))["status"] == "done"
    assert state["runs"] == 1
    [tool_result] = results(await all_entries(opened["root"]))
    assert tool_result["isError"] is True
    assert tool_result["details"] == {"run": 1}
    assert result_text(tool_result) == (
        "run 1\n|<harness>\n[error] Tool work was interrupted and may have partially run\n</harness>"
    )
    assert await opened["harness"].snapshot(LiveDoc, opened["root"].id, context) == {}
    await opened["harness"].close(context)


async def test_reruns_a_tool_only_when_both_the_stored_and_current_replay_policy_are_safe(
    tmp_path: Any,
) -> None:
    cases: list[tuple[str, str, bool]] = [
        ("safe", "safe", True),
        ("safe", "unsafe", False),
        ("unsafe", "safe", False),
    ]
    for index, (stored, current, reruns) in enumerate(cases):
        path = str(tmp_path / f"session-{index}.sqlite")
        setup = new_setup()
        registration, started, state = blocking_tool("work", replay=stored)
        installed = add_tool(setup.registry, registration)
        setup.faux.set_responses([call("work"), DONE])
        opened = await open_path(path, setup)
        submission_id = (
            await opened["root"].submit({"type": "input", "content": "go"}, context)
        ).id
        await started.wait()
        await opened["harness"].close(context)

        setup.registry.uninstall(installed)
        add_tool(setup.registry, replace(registration, replay=current))
        opened = await open_path(path, setup)
        submission = await opened["harness"].submission(submission_id, context)
        assert (await submission.wait(context))["status"] == "done"
        [tool_result] = results(await all_entries(opened["root"]))
        assert state["runs"] == (2 if reruns else 1)
        assert tool_result["isError"] is (not reruns)
        if reruns:
            assert result_text(tool_result) == "run 2\n"
        else:
            assert "was interrupted" in result_text(tool_result)
        await opened["harness"].close(context)


async def test_reruns_a_safe_tool_with_the_environment_at_rerun_and_not_once_deselected(
    tmp_path: Any,
) -> None:
    for index, change in enumerate(("cwd", "deselect")):
        path = str(tmp_path / f"session-{index}.sqlite")
        setup = new_setup()
        cwds: list[Any] = []
        started = asyncio.Event()

        add_tool(setup.registry, tool("work", make_work(cwds, started), replay="safe"))
        setup.faux.set_responses([call("work"), DONE])

        def env(target: Any, _context: Any) -> Any:
            return {"cwd": target.get("cwd") or "/"}

        opened = await open_path(path, setup, env)
        await opened["root"].configure({"cwd": "/one"}, context)
        submission_id = (
            await opened["root"].submit({"type": "input", "content": "go"}, context)
        ).id
        await started.wait()
        if change == "cwd":
            await opened["root"].configure({"cwd": "/two"}, context)
        else:
            await opened["root"].configure({"extensions": []}, context)
        await opened["harness"].close(context)

        opened = await open_path(path, setup, env)
        submission = await opened["harness"].submission(submission_id, context)
        assert (await submission.wait(context))["status"] == "done"
        [tool_result] = results(await all_entries(opened["root"]))
        if change == "cwd":
            assert cwds == ["/one", "/two"]
            assert tool_result["isError"] is False
        else:
            # A tool that no longer resolves is treated as unsafe: interrupted, not rerun.
            assert cwds == ["/one"]
            assert "was interrupted" in result_text(tool_result)
        await opened["harness"].close(context)


async def test_reruns_before_tool_when_interrupted_before_intent_and_executes_once(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    runs = {"count": 0}

    def work(*_args: Any) -> Any:
        runs["count"] += 1
        return {"output": []}

    add_tool(setup.registry, tool("work", work))
    reached = asyncio.Event()
    asked = {"count": 0}
    decisions: list[Any] = []

    async def before_tool(_call: Any, api: Any, call_context: Any) -> Any:
        asked["count"] += 1
        # A durable first-writer-wins decision survives the rerun.
        decisions.append(
            await api.memo("test:decision", f"attempt {asked['count']}", call_context)
        )
        if asked["count"] == 1:
            reached.set()
            await aborted(call_context.abort_signal)
        return None

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool})
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    submission_id = (
        await opened["root"].submit({"type": "input", "content": "go"}, context)
    ).id
    await reached.wait()
    await opened["harness"].close(context)

    opened = await open_path(path, setup)
    submission = await opened["harness"].submission(submission_id, context)
    assert (await submission.wait(context))["status"] == "done"
    assert [asked["count"], runs["count"]] == [2, 1]
    assert decisions == ["attempt 1", "attempt 1"]
    await opened["harness"].close(context)


async def test_reruns_the_generation_tools_phase_interrupted_before_its_commit(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    add_tool(setup.registry, tool("work", lambda *_args: {"output": []}))
    reached = asyncio.Event()
    observed = {"count": 0}

    async def after_tools(_assistant: Any, _results: Any, _api: Any, call_context: Any) -> Any:
        observed["count"] += 1
        if observed["count"] == 1:
            reached.set()
            await aborted(call_context.abort_signal)
        return None

    add_hooks(setup.registry, GenerationTask, {"afterTools": after_tools})
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    submission_id = (
        await opened["root"].submit({"type": "input", "content": "go"}, context)
    ).id
    await reached.wait()
    await opened["harness"].close(context)

    opened = await open_path(path, setup)
    submission = await opened["harness"].submission(submission_id, context)
    assert (await submission.wait(context))["status"] == "done"
    assert observed["count"] == 2
    assert [entry["kind"] for entry in await all_entries(opened["root"])] == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "pi.tool-result",
        "pi.assistant",
    ]
    await opened["harness"].close(context)


async def test_answers_an_aborted_tool_with_its_partial_output_and_continues_the_run(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    registration, started, _state = blocking_tool("work")
    add_tool(setup.registry, registration)
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    submission = await opened["root"].submit({"type": "input", "content": "go"}, context)
    await started.wait()
    task_id = await tool_task_id(opened)
    assert await opened["harness"].abort_task(task_id, context) == "marked"
    settled = await opened["harness"].wait_for_task(task_id, context)
    assert settled["state"]["outcome"]["status"] == "aborted"
    assert settled["state"]["outcome"]["result"]["entryId"] is not None
    assert (await submission.wait(context))["status"] == "done"
    [tool_result] = results(await all_entries(opened["root"]))
    assert result_text(tool_result) == (
        "run 1\n|<harness>\n[error] Tool work was aborted\n</harness>"
    )
    await opened["harness"].close(context)


async def test_lets_context_derivation_answer_a_faulted_tool_and_continues_the_run(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    # A non-JSON details value makes the result commit throw, so the scheduler faults the task.
    add_tool(setup.registry, tool("bad", lambda *_args: {"output": [], "details": {"fn": lambda: 1}}))
    requests: list[str] = []

    def next_request(request: Any, _options: Any) -> Any:
        result = next(
            message for message in request["messages"] if message["role"] == "toolResult"
        )
        requests.append(cast(str, text_of(result)))
        return DONE

    setup.faux.set_responses([call("bad"), next_request])
    opened = await open_path(path, setup)
    settled = await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(
        context
    )
    assert settled["status"] == "done"
    assert results(await all_entries(opened["root"])) == []
    assert requests == ["Tool result unavailable: history ends before this call completed."]
    tasks = (
        await opened["harness"].commit(
            lambda tx: tx.scan_tasks({"conversationId": opened["root"].id}, 100), context
        )
    )["items"]
    faulted = next(task for task in tasks if task["kind"] == "pi.tool")
    assert faulted["state"]["status"] == "terminal"
    assert faulted["state"]["outcome"]["status"] == "faulted"
    await opened["harness"].close(context)


async def test_clears_the_interrupted_attempts_progress_before_a_safe_rerun(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    started = [asyncio.Event(), asyncio.Event()]
    runs = {"count": 0}

    async def work(_args: Any, api: Any, call_context: Any) -> Any:
        run = runs["count"]
        runs["count"] += 1
        if run == 0:
            api.diagnostic({"severity": "info", "message": "first a"})
            api.diagnostic({"severity": "info", "message": "first b"})
            await api.details({"run": 1, "extra": True}, call_context)
        else:
            api.diagnostic({"severity": "info", "message": "second"})
            await api.details({"run": 2}, call_context)
        started[run].set()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("work", work, replay="safe"))
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    await opened["root"].submit({"type": "input", "content": "go"}, context)
    await started[0].wait()
    await opened["harness"].close(context)

    opened = await open_path(path, setup)
    await started[1].wait()
    live = await opened["harness"].snapshot(LiveDoc, opened["root"].id, context)
    slot = live["tools"][0]
    assert slot["diagnostics"] == [{"severity": "info", "message": "second"}]
    assert slot["details"] == {"run": 2}
    await opened["harness"].close(context)


async def test_migrates_a_version_1_tool_record_and_answers_it_as_model_issued(tmp_path: Any) -> None:
    path = str(tmp_path / "session.sqlite")
    setup = new_setup()
    registration, started, _state = blocking_tool("work")
    add_tool(setup.registry, registration)
    setup.faux.set_responses([call("work"), DONE])
    opened = await open_path(path, setup)
    submission_id = (
        await opened["root"].submit({"type": "input", "content": "go"}, context)
    ).id
    await started.wait()
    task_id = await tool_task_id(opened)
    await opened["harness"].close(context)

    # Version 1 inputs carried only the assistant entry and the call ID.
    storage = await open_posix_sqlite_storage(path)
    record = await storage.task(task_id, context)
    legacy = dict(record)
    legacy["version"] = 1
    legacy["input"] = {
        "assistant": record["input"]["assistant"],
        "callId": record["input"]["callId"],
    }
    await storage.commit([{"type": "task", "value": legacy}], context)
    await storage.close(context)

    opened = await open_path(path, setup)
    submission = await opened["harness"].submission(submission_id, context)
    assert (await submission.wait(context))["status"] == "done"
    migrated = await opened["harness"].get_task(task_id, context)
    assert migrated["version"] == 2
    assert migrated["input"]["kind"] == "model"
    [tool_result] = results(await all_entries(opened["root"]))
    assert result_text(tool_result) == (
        "run 1\n|<harness>\n[error] Tool work was interrupted and may have partially run\n</harness>"
    )
    await opened["harness"].close(context)

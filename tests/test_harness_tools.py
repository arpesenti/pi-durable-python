"""Translated upstream ``harness-tools`` cases for the tool execution core.

The round, its durable result entries, argument repair and validation, tool hooks, output bounds,
the execution api, and the lifetime rules. Coding tools (#17) and the round over them are covered
by test_tools.py; nested calls and structured output by test_harness_nested_tools.py and
test_harness_structured_output.py; tool search (#20) by test_tool_search.py.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from session_support import context
from task_support import aborted
from tool_support import (
    DONE,
    add_hooks,
    add_task,
    add_tool,
    all_entries,
    calls,
    chat_setup,
    result_text,
    results,
    run,
    tool,
)

from pi_durable import (
    AgentDoc,
    GenerationTask,
    LiveDoc,
    MemoryStorage,
    ToolResultEntry,
    ToolTask,
    UsageDoc,
    configure,
    define_extension,
    define_task,
    hook,
)
from pi_durable.context import AbortError
from pi_durable.harness.harness import Harness as HarnessType
from pi_durable.harness.types import Extension


async def _tool_tasks(harness: HarnessType, conversation_id: Any) -> list[Any]:
    page = await harness.commit(
        lambda tx: tx.scan_tasks({"conversationId": conversation_id}, 100), context
    )
    return [task for task in page["items"] if task["kind"] == "pi.tool"]


async def _close(result: dict[str, Any]) -> None:
    await result["harness"].close(context)


async def test_runs_input_tool_call_tool_result_and_answer_and_settles_the_input() -> None:
    setup = chat_setup()
    def echo(args: Any, _api: Any, _context: Any) -> Any:
        return {"output": [{"type": "text", "text": f"echo {args['text']}"}]}

    add_tool(setup.registry, tool("echo", echo))
    result = await run(setup, [calls(["echo", {"text": "hi"}, "c1"]), DONE])
    harness, root, entries = result["harness"], result["root"], result["entries"]
    assert result["status"] == "done"
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "pi.tool-result",
        "pi.assistant",
    ]
    system = cast(dict[str, Any], entries[1]["model"][0])
    assert system["toolsAdded"] == [
        {
            "name": "echo",
            "description": "The echo tool",
            "parameters": {"type": "object", "properties": {"text": {"type": "string"}}},
        }
    ]
    [tool_result] = results(entries)
    assert (tool_result["toolCallId"], tool_result["toolName"], tool_result["isError"]) == (
        "c1",
        "echo",
        False,
    )
    assert result_text(tool_result) == "echo hi"
    assert entries[3]["data"] == {"diagnostics": []}
    assert entries[3]["byTaskId"] is not None
    # The second request sees the tool result right after its call.
    assert setup.faux.call_count == 2
    messages = cast(list[dict[str, Any]], setup.faux.requests[1][0]["messages"])
    call_index = next(
        index
        for index, message in enumerate(messages)
        if message["role"] == "assistant"
        and any(item.get("type") == "toolCall" for item in message["content"])
    )
    assert messages[call_index + 1]["role"] == "toolResult"
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await harness.close(context)


async def test_gives_tools_and_hooks_the_harness_models() -> None:
    setup = chat_setup()
    seen: list[Any] = []

    def execute(_args: Any, api: Any, _context: Any) -> Any:
        seen.append(api.models)
        return {"output": []}

    add_tool(setup.registry, tool("echo", execute))
    add_hooks(
        setup.registry,
        ToolTask,
        {"beforeTool": lambda _call, api, _context: seen.append(api.models)},
    )
    result = await run(setup, [calls(["echo", {}, "c1"]), DONE])
    assert result["status"] == "done"
    assert seen == [setup.models, setup.models]
    assert all(item is setup.models for item in seen)
    await result["harness"].close(context)


async def test_answers_calls_to_tools_the_request_did_not_offer_without_a_task() -> None:
    setup = chat_setup()
    add_tool(setup.registry, tool("echo", lambda *_args: {"output": []}))
    result = await run(setup, [calls(["ghost", {}, "c1"], ["echo", {}, "c2"]), DONE])
    entries = result["entries"]
    assert result["status"] == "done"
    ghost, echo = results(entries)
    assert (ghost["toolCallId"], ghost["isError"]) == ("c1", True)
    assert result_text(ghost) == "<harness>\n[error] Tool ghost is not available\n</harness>"
    assert (echo["toolCallId"], echo["isError"]) == ("c2", False)
    ghost_entry = next(entry for entry in entries if ToolResultEntry.is_(entry))
    assert ghost_entry["data"] == {
        "diagnostics": [
            {"severity": "error", "code": "tool_unavailable", "message": "Tool ghost is not available"}
        ]
    }
    assert len(await _tool_tasks(result["harness"], result["root"].id)) == 1
    await result["harness"].close(context)


async def test_answers_a_call_to_a_tool_deactivated_after_preparation_with_tool_unavailable() -> None:
    setup = chat_setup()
    seen: list[str] = []
    add_tool(setup.registry, tool("echo", lambda *_args: seen.append("ran") or {"output": []}))
    captured: dict[str, Any] = {}

    async def deactivate(_request: Any, _options: Any) -> Any:
        await captured["root"].configure({"tools": []}, context)
        return calls(["echo", {}, "c1"])

    result = await run(
        setup,
        [deactivate, DONE],
        lambda _harness, root: _capture(captured, root),
    )
    assert result["status"] == "done"
    assert seen == []
    assert (
        result_text(results(result["entries"])[0])
        == "<harness>\n[error] Tool echo is not available\n</harness>"
    )
    systems = [entry for entry in result["entries"] if entry["kind"] == "pi.system"]
    assert cast(dict[str, Any], systems[-1]["model"][0])["toolsRemoved"] == [{"name": "echo"}]
    await result["harness"].close(context)


async def _capture(captured: dict[str, Any], root: Any) -> None:
    captured["root"] = root


async def test_removes_unregistered_active_tools_from_the_offer_and_adds_them_back() -> None:
    setup = chat_setup()
    echo = tool("echo", lambda *_args: {"output": []})
    registration: Extension = add_tool(setup.registry, echo)
    first = await run(setup, [DONE])
    assert first["status"] == "done"
    setup.registry.uninstall(registration)
    setup.faux.set_responses([calls(["echo", {}, "c1"]), DONE])
    second = await (await first["root"].submit({"type": "input", "content": "again"}, context)).wait(context)
    assert second["status"] == "done"
    entries = await all_entries(first["root"])
    removal = cast(dict[str, Any], [e for e in entries if e["kind"] == "pi.system"][-1]["model"][0])
    assert removal["toolsRemoved"] == [{"name": "echo"}]
    assert results(entries)[0]["isError"] is True
    # The stored agent is not rewritten; the tool is only not resolved.
    assert (await first["root"].agent(context)).tools == []

    add_tool(setup.registry, echo)
    setup.faux.set_responses([DONE])
    await (await first["root"].submit({"type": "input", "content": "back"}, context)).wait(context)
    entries = await all_entries(first["root"])
    addition = cast(dict[str, Any], [e for e in entries if e["kind"] == "pi.system"][-1]["model"][0])
    assert [declared["name"] for declared in addition["toolsAdded"]] == ["echo"]
    await first["harness"].close(context)


async def test_produces_tool_unavailable_when_unregistered_before_its_task_runs() -> None:
    setup = chat_setup()
    state: dict[str, Any] = {}

    def unregister_then_run(_args: Any, _api: Any, _context: Any) -> Any:
        setup.registry.uninstall(state["second"])
        return {"output": []}

    add_tool(setup.registry, tool("first", unregister_then_run, execution_mode="sequential"))
    state["second"] = add_tool(
        setup.registry,
        tool("second", lambda *_args: {"output": [{"type": "text", "text": "ran"}]}),
    )
    result = await run(setup, [calls(["first", {}, "c1"], ["second", {}, "c2"]), DONE])
    assert result["status"] == "done"
    _first, late = results(result["entries"])
    assert result_text(late) == "<harness>\n[error] Tool second is not available\n</harness>"
    await result["harness"].close(context)


async def test_reads_the_execution_mode_when_a_round_starts_and_keeps_it_for_the_round() -> None:
    setup = chat_setup()
    events: list[str] = []

    def slow(name: str) -> Any:
        async def execute(_args: Any, _api: Any, _context: Any) -> Any:
            events.append(f"start {name}")
            # Changed after the round started: not seen by this round.
            setup.settings["tool_execution"] = "parallel"
            await asyncio.sleep(0.02)
            events.append(f"end {name}")
            return {"output": []}

        return execute

    add_tool(setup.registry, tool("a", slow("a")))
    add_tool(setup.registry, tool("b", slow("b")))

    def request(_request: Any, _options: Any) -> Any:
        # Changed while the model request runs: the round that follows uses it.
        setup.settings["tool_execution"] = "sequential"
        return calls(["a", {}, "c1"], ["b", {}, "c2"])

    result = await run(setup, [request, DONE])
    assert result["status"] == "done"
    assert events == ["start a", "end a", "start b", "end b"]
    await result["harness"].close(context)


async def test_runs_a_round_in_parallel_by_default_and_sequentially_when_configured_or_required() -> None:
    async def trace(setup: Any, prepare: Any = None) -> tuple[list[str], dict[str, Any]]:
        events: list[str] = []

        def slow(name: str) -> Any:
            async def execute(_args: Any, _api: Any, _context: Any) -> Any:
                events.append(f"start {name}")
                await asyncio.sleep(0.02)
                events.append(f"end {name}")
                return {"output": []}

            return execute

        add_tool(setup.registry, tool("a", slow("a")))
        add_tool(setup.registry, tool("b", slow("b")))
        result = await run(setup, [calls(["a", {}, "c1"], ["b", {}, "c2"]), DONE], prepare)
        return events, result

    parallel, parallel_result = await trace(chat_setup())
    assert parallel[:2] == ["start a", "start b"]
    await parallel_result["harness"].close(context)

    configured = chat_setup()
    configured.settings["tool_execution"] = "sequential"
    sequential, sequential_result = await trace(configured)
    assert sequential == ["start a", "end a", "start b", "end b"]
    await sequential_result["harness"].close(context)

    per_tool = chat_setup()
    completed: list[str] = []

    async def slow_a(_args: Any, _api: Any, _context: Any) -> Any:
        await asyncio.sleep(0.02)
        completed.append("a")
        return {"output": []}

    add_tool(per_tool.registry, tool("a", slow_a, execution_mode="sequential"))
    add_tool(
        per_tool.registry,
        tool("b", lambda *_args: completed.append("b") or {"output": []}),
    )
    result = await run(per_tool, [calls(["a", {}, "c1"], ["b", {}, "c2"]), DONE])
    tasks = (await result["harness"].commit(
        lambda tx: tx.scan_tasks({"conversationId": result["root"].id}, 100), context
    ))["items"]
    tools = sorted((task for task in tasks if task["kind"] == "pi.tool"), key=lambda task: task["id"])
    generation = next(task for task in tasks if task["kind"] == "pi.generation")
    # The generation owns both tools and creates the second only after the first ended.
    assert [task["owner"] for task in tools] == [generation["id"], generation["id"]]
    assert completed == ["a", "b"]
    await result["harness"].close(context)


async def test_uses_retained_output_and_the_last_details_with_diagnostics_in_order() -> None:
    setup = chat_setup()

    async def log(_args: Any, api: Any, _context: Any) -> Any:
        api.output("line 1\n")
        api.output(b"line 2\nline 3\n")
        api.diagnostic({"severity": "info", "message": "from api"})
        await api.details({"step": 1}, context)
        await api.details({"step": 2}, context)
        return {"diagnostics": [{"severity": "warn", "message": "from result"}]}

    add_tool(setup.registry, tool("log", log, output_limits={"max_lines": 2}))
    result = await run(setup, [calls(["log", {}, "c1"]), DONE])
    [tool_result] = results(result["entries"])
    assert tool_result["details"] == {"step": 2}
    assert result_text(tool_result) == (
        "line 1\nline 2\n|<harness>\n[info] from api\n[warn] from result\n"
        "[warn] Output truncated to its beginning: 1 lines, 7 bytes dropped\n</harness>"
    )
    entry = next(candidate for candidate in result["entries"] if ToolResultEntry.is_(candidate))
    assert [diagnostic.get("code") for diagnostic in entry["data"]["diagnostics"]] == [
        None,
        None,
        "truncated",
    ]
    await result["harness"].close(context)


async def test_offers_the_tail_window_with_the_configured_pace_and_accepts_skipped_output() -> None:
    setup = chat_setup()
    setup.settings = {"progress": {"output_interval_ms": 250}}
    windows: list[Any] = []

    def tailed(_args: Any, api: Any, _context: Any) -> Any:
        windows.append(api.output_window)
        api.output("dropped\n")
        api.output("x\ny\n", {"bytes": 8, "newlines": 1, "ends_with_newline": True})
        return {}

    add_tool(setup.registry, tool("tailed", tailed, output_limits={"max_lines": 1, "retain": "tail"}))
    add_tool(
        setup.registry,
        tool("headed", lambda _args, api, _context: windows.append(api.output_window) or {}),
    )
    result = await run(setup, [calls(["tailed", {}, "c1"], ["headed", {}, "c2"]), DONE])
    assert {
        "max_bytes": 50 * 1024,
        "max_lines": 1,
        "min_interval_ms": 250,
        "bytes_per_second": 100 * 1024,
    } in windows
    assert None in windows
    # 8 bytes written, 8 skipped, then "x\n" dropped by the window: 3 lines, 18 bytes in all.
    tailed_result = next(
        item for item in results(result["entries"]) if item["toolCallId"] == "c1"
    )
    assert result_text(tailed_result) == (
        "y\n|<harness>\n[warn] Output truncated to its end: 3 lines, 18 bytes dropped\n</harness>"
    )
    await result["harness"].close(context)


async def test_bounds_explicit_text_content_and_keeps_other_content() -> None:
    setup = chat_setup()
    image = {"type": "image", "data": "AAAA", "mimeType": "image/png"}
    add_tool(
        setup.registry,
        tool(
            "big",
            lambda *_args: {
                "output": [
                    {"type": "text", "text": "a\nb\n"},
                    image,
                    {"type": "text", "text": "c\nd\n"},
                ]
            },
            output_limits={"max_lines": 2, "retain": "tail"},
        ),
    )
    result = await run(setup, [calls(["big", {}, "c1"]), DONE])
    assert result_text(results(result["entries"])[0]) == (
        "[image]|c\nd\n|<harness>\n[warn] Output truncated to its end: 2 lines, 4 bytes dropped\n</harness>"
    )
    await result["harness"].close(context)


async def test_turns_a_throw_into_a_tool_error_result_with_the_partial_output() -> None:
    setup = chat_setup()

    def fail(_args: Any, api: Any, _context: Any) -> Any:
        api.output("partial\n")
        raise RuntimeError("boom")

    add_tool(setup.registry, tool("fail", fail))
    result = await run(setup, [calls(["fail", {}, "c1"]), DONE])
    assert result["status"] == "done"
    [tool_result] = results(result["entries"])
    assert tool_result["isError"] is True
    assert result_text(tool_result) == "partial\n|<harness>\n[error] boom\n</harness>"
    await result["harness"].close(context)


async def test_records_how_long_execute_took_and_nothing_for_calls_that_did_not_run() -> None:
    setup = chat_setup()

    async def slow(_args: Any, _api: Any, _context: Any) -> Any:
        await asyncio.sleep(0.03)
        return {"output": []}

    async def thrower(_args: Any, _api: Any, _context: Any) -> Any:
        await asyncio.sleep(0.03)
        raise RuntimeError("boom")

    add_tool(setup.registry, tool("slow", slow))
    add_tool(setup.registry, tool("thrower", thrower))

    async def before_tool(call: Any, _api: Any, _context: Any) -> Any:
        await asyncio.sleep(0.1)
        return {"block": "no"} if call["id"] == "blocked" else None

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool})
    result = await run(
        setup,
        [calls(["slow", {}, "slow"], ["thrower", {}, "thrower"], ["slow", {}, "blocked"]), DONE],
    )
    by_id = {item["toolCallId"]: item for item in results(result["entries"])}
    for call_id in ("slow", "thrower"):
        assert by_id[call_id]["durationMs"] >= 25
        assert by_id[call_id]["durationMs"] < 100
    assert by_id["thrower"]["isError"] is True
    assert "durationMs" not in by_id["blocked"]
    await result["harness"].close(context)


async def test_validates_arguments_before_and_after_before_tool_and_applies_blocks_and_replacements() -> None:
    setup = chat_setup()
    seen: list[dict[str, Any]] = []

    def execute(args: Any, _api: Any, _context: Any) -> Any:
        seen.append(args)
        return {"output": []}

    add_tool(setup.registry, tool("echo", execute))

    def before_tool(call: Any) -> Any:
        if call["id"] == "block":
            return {"block": "not today"}
        if call["id"] == "throw":
            raise RuntimeError("hook failed")
        if call["id"] == "bad":
            return {"arguments": {"text": {"not": "a string"}}}
        text = call["arguments"].get("text", "")
        return {"arguments": {"text": f"{text}!"}}

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool})
    result = await run(
        setup,
        [
            calls(
                ["echo", {"text": 1}, "coerced"],
                ["echo", {"text": {"not": "a string"}}, "invalid"],
                ["echo", {}, "block"],
                ["echo", {}, "throw"],
                ["echo", {"text": "x"}, "bad"],
                ["echo", {"text": "x"}, "ok"],
            ),
            DONE,
        ],
    )
    # Parallel tools append their results in completion order.
    by_id = {
        item["toolCallId"]: (item["isError"], result_text(item))
        for item in results(result["entries"])
    }
    assert by_id["block"] == (True, "<harness>\n[error] Tool call blocked: not today\n</harness>")
    assert by_id["throw"] == (True, "<harness>\n[error] Tool call blocked: hook failed\n</harness>")
    assert by_id["bad"][0] is True
    assert "Validation failed" in by_id["bad"][1]
    assert by_id["ok"] == (False, "")
    assert by_id["invalid"][0] is True
    assert "Validation failed" in by_id["invalid"][1]
    # Coercion happens before the first validation.
    assert by_id["coerced"] == (False, "")
    assert len(seen) == 2
    assert {entry["text"] for entry in seen} == {"1!", "x!"}
    await result["harness"].close(context)


async def test_repairs_arguments_with_prepare_arguments_and_a_throwing_repair_is_invalid() -> None:
    setup = chat_setup()
    seen: list[dict[str, Any]] = []

    def execute(args: Any, _api: Any, _context: Any) -> Any:
        seen.append(args)
        return {"output": []}

    def prepare(args: Any) -> Any:
        text = args.get("text")
        if text == "throw":
            raise RuntimeError("cannot repair")
        return {"text": f"#{text}"} if isinstance(text, int) else args

    add_tool(setup.registry, tool("echo", execute, prepare_arguments=prepare))
    result = await run(
        setup, [calls(["echo", {"text": 7}, "fixed"], ["echo", {"text": "throw"}, "broken"]), DONE]
    )
    by_id = {item["toolCallId"]: result_text(item) for item in results(result["entries"])}
    assert by_id["fixed"] == ""
    assert by_id["broken"] == "<harness>\n[error] cannot repair\n</harness>"
    assert seen == [{"text": "#7"}]
    # The stored call keeps what the model sent.
    message = cast(dict[str, Any], result["entries"][2]["model"][0])
    call = next(item for item in message["content"] if item["type"] == "toolCall")
    assert call["arguments"] == {"text": 7}
    await result["harness"].close(context)


async def test_lets_the_first_before_tool_block_win_and_skips_later_handlers() -> None:
    setup = chat_setup()
    add_tool(setup.registry, tool("echo", lambda *_args: {"output": []}))
    asked: list[str] = []
    add_hooks(
        setup.registry,
        ToolTask,
        {"beforeTool": lambda _call: asked.append("first") or {"block": "first says no"}},
    )
    add_hooks(
        setup.registry,
        ToolTask,
        {"beforeTool": lambda _call: asked.append("second") or {"block": "second says no"}},
    )
    result = await run(setup, [calls(["echo", {}, "c1"]), DONE])
    assert asked == ["first"]
    assert (
        result_text(results(result["entries"])[0])
        == "<harness>\n[error] Tool call blocked: first says no\n</harness>"
    )
    await result["harness"].close(context)


async def test_chains_after_tool_replacements_and_observes_the_round_with_after_tools() -> None:
    setup = chat_setup()
    add_tool(
        setup.registry,
        tool("echo", lambda *_args: {"output": [{"type": "text", "text": "raw"}]}),
    )
    add_hooks(
        setup.registry,
        ToolTask,
        {
            "afterTool": lambda _call, result: {
                **result,
                "output": [{"type": "text", "text": "first"}],
            }
        },
    )

    def second(_call: Any, result: Any) -> Any:
        output = result.get("output") or []
        return {
            **result,
            "details": {
                "replaced": "|".join(
                    item.get("text", "") for item in output if item.get("type") == "text"
                )
            },
        }

    add_hooks(setup.registry, ToolTask, {"afterTool": second})
    observed: list[Any] = []
    add_hooks(
        setup.registry,
        GenerationTask,
        {"afterTools": lambda assistant, entries, _api, _context: observed.extend([assistant, entries])},
    )
    result = await run(setup, [calls(["echo", {}, "c1"]), DONE])
    entries = result["entries"]
    [tool_result] = results(entries)
    assert result_text(tool_result) == "first"
    assert tool_result["details"] == {"replaced": "first"}
    result_entry = next(entry for entry in entries if ToolResultEntry.is_(entry))
    assert observed == [entries[2]["id"], [result_entry["id"]]]
    await result["harness"].close(context)


async def test_runs_the_hooks_of_selected_extensions_and_a_task_owned_child_copies_its_owner() -> None:
    setup = chat_setup()
    called_in: list[Any] = []
    echo = define_extension({"name": "echo", "tools": [tool("echo", lambda *_args: {"output": []})]})
    audit = define_extension(
        {
            "name": "audit",
            "hooks": [
                hook(
                    ToolTask,
                    {"beforeTool": lambda _call, api, _ctx: called_in.append(api.conversation_id)},
                )
            ],
        }
    )
    setup.registry.install(echo)
    setup.registry.install(audit)
    # Audit is installed but not in the default selection.
    setup.settings["extensions"] = [echo]
    first = await run(setup, [DONE])
    await first["root"].configure({"extensions": {"add": [audit]}}, context)
    # An owner task no registered definition takes stays live and pending.
    owner = define_task(
        {
            "name": "test.owner",
            "version": 1,
            "initial": lambda _input: {"phase": "never"},
            "phases": {"never": _never},
            "abort": _never,
        }
    )

    async def create_child(tx: Any) -> Any:
        task_id = await tx.create_task(
            owner, {}, {"ownership": {"kind": "conversation"}, "conversationId": first["root"].id}
        )
        record = await tx.create_conversation({"ownership": {"kind": "task", "taskId": task_id}})
        return record["id"]

    child_id = await first["harness"].commit(create_child, context)
    child = await first["harness"].conversation(child_id, context)
    other = await first["harness"].create_conversation(
        {"ownership": {"kind": "ownerless"}, "agent": {"model": {"provider": "faux", "modelId": "faux-1"}}},
        context,
    )
    for conversation in (first["root"], child, other):
        setup.faux.set_responses([calls(["echo", {}, "c1"]), DONE])
        await (await conversation.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert called_in == [first["root"].id, child.id]
    await first["harness"].close(context)


async def _never(*_args: Any) -> None:
    return None


async def test_applies_add_tools_and_terminates_only_when_every_result_of_the_round_asks() -> None:
    setup = chat_setup()
    stop = tool("stop", lambda *_args: {"output": [], "control": {"terminate": True}})
    grow = tool("grow", lambda *_args: {"output": [], "control": {"addTools": ["extra", "stop"]}})
    add_tool(setup.registry, stop)
    add_tool(setup.registry, grow)
    add_tool(setup.registry, tool("extra", lambda *_args: {"output": []}))
    first = await run(
        setup,
        [calls(["stop", {}, "c1"])],
        lambda _harness, root: root.configure({"tools": [stop, grow]}, context),
    )
    assert first["status"] == "done"
    assert first["entries"][-1]["kind"] == "pi.tool-result"
    tasks = (await first["harness"].commit(
        lambda tx: tx.scan_tasks({"conversationId": first["root"].id}, 100), context
    ))["items"]
    assert all(task["state"]["status"] == "terminal" for task in tasks)

    setup.faux.set_responses([calls(["stop", {}, "c1"], ["grow", {}, "c2"]), DONE])
    second = await (await first["root"].submit({"type": "input", "content": "again"}, context)).wait(context)
    assert second["status"] == "done"
    assert (await all_entries(first["root"]))[-1]["kind"] == "pi.assistant"
    # addTools appends to the stored tool array, skipping names it already holds.
    assert (await first["harness"].snapshot(AgentDoc, first["root"].id, context))["tools"] == [
        "stop",
        "grow",
        "extra",
    ]
    await first["harness"].close(context)


async def test_reads_the_harness_clock_through_api_now() -> None:
    setup = chat_setup()
    setup.now = lambda: 1_234
    seen: list[int] = []

    def clock(_args: Any, api: Any, _context: Any) -> Any:
        seen.append(api.now())
        return {"output": []}

    add_tool(setup.registry, tool("clock", clock))
    ran = await run(
        setup,
        [calls(["clock", {}, "c1"]), DONE],
        lambda _harness, root: root.configure({"tools": ["clock"]}, context),
    )
    assert ran["status"] == "done"
    # The tool sees the same clock the scheduler stamps task times with, not the wall clock.
    assert seen == [1_234]
    await ran["harness"].close(context)


async def test_does_not_terminate_on_terminate_false() -> None:
    # #10858
    setup = chat_setup()
    keep = tool("keep", lambda *_args: {"output": [], "control": {"terminate": False}})
    add_tool(setup.registry, keep)
    ran = await run(
        setup,
        [calls(["keep", {}, "c1"]), DONE],
        lambda _harness, root: root.configure({"tools": [keep]}, context),
    )
    assert ran["status"] == "done"
    assert ran["entries"][-1]["kind"] == "pi.assistant"
    await ran["harness"].close(context)


async def test_builds_the_environment_per_call_and_runs_commits_memos_and_child_tasks() -> None:
    setup = chat_setup()
    child = define_task(
        {
            "name": "test.child",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": _child_run},
            "abort": _never,
        }
    )
    add_task(setup.registry, child)
    seen: list[Any] = []

    async def probe(_args: Any, api: Any, _context: Any) -> Any:
        seen.append(api.env["cwd"])
        seen.append([each.name for each in (await api.agent(context)).tools])
        seen.append(api.registry.extension("tool:probe") is not None)

        async def change(tx: Any) -> Any:
            # The next call runs in the new directory: its environment is built when it executes.
            await configure(tx, api.conversation_id, {"cwd": "/"})
            return await tx.append_entry(
                api.conversation_id, {"kind": "test.note", "data": api.call_id}
            )

        entry = await api.commit(change, context)
        seen.append(entry["byTaskId"] == api.task_id)
        seen.append(await api.memo("m", 1, context))
        seen.append(await api.memo("m", 2, context))
        task_id = await api.create_task(
            child, {"n": 21}, {"ownership": {"kind": "conversation"}}, context
        )
        done = await api.wait_for_task(task_id, context)
        seen.append(done["state"]["outcome"])
        return {"output": []}

    add_tool(setup.registry, tool("probe", probe))
    setup.faux.set_responses([calls(["probe", {}, "c1"], ["probe", {}, "c2"]), DONE])
    setup.settings["tool_execution"] = "sequential"
    targets: list[Any] = []

    def env(target: Any, _context: Any) -> Any:
        targets.append([target["conversation_id"], target.get("cwd")])
        return {"cwd": target.get("cwd") or "/tmp"}

    from chat_support import open_chat

    opened = await open_chat(MemoryStorage(), setup, env=env)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"cwd": "/tmp"}, context)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    outcome = {"status": "completed", "result": 42}

    def call(cwd: str) -> list[Any]:
        return [cwd, ["probe"], True, True, 1, 1, outcome]

    assert seen == [*call("/tmp"), *call("/")]
    assert [root.id, "/tmp"] in targets
    assert [root.id, "/"] in targets
    await harness.close(context)


async def _child_run(task: Any, runtime: Any, call_context: Any) -> None:
    await runtime.commit(
        lambda: {
            "status": "terminal",
            "outcome": {"status": "completed", "result": task["input"]["n"] * 2},
        },
        call_context,
    )


async def test_answers_a_throwing_environment_with_a_tool_error_and_reports_it_while_preparing() -> None:
    setup = chat_setup()
    add_tool(setup.registry, tool("probe", lambda *_args: {"output": [{"type": "text", "text": "ran"}]}))

    def env(_target: Any, _context: Any) -> Any:
        raise RuntimeError("no sandbox")

    result = await run(setup, [calls(["probe", {}, "c1"]), DONE], options={"env": env})
    [tool_result] = results(result["entries"])
    assert tool_result["isError"] is True
    assert result_text(tool_result) == "<harness>\n[error] no sandbox\n</harness>"
    # Each preparation reports the failure and renders without an environment.
    assert len([error for error in setup.reports if str(error) == "no sandbox"]) == 2
    await result["harness"].close(context)


async def test_applies_the_default_output_limits() -> None:
    setup = chat_setup()

    def lines(_args: Any, api: Any, _context: Any) -> Any:
        for index in range(1, 2501):
            api.output(f"{index}\n")
        return {}

    add_tool(setup.registry, tool("lines", lines))
    result = await run(setup, [calls(["lines", {}, "c1"]), DONE])
    text = result_text(results(result["entries"])[0])
    assert text.startswith("1\n2\n")
    assert text.endswith(
        "\n2000\n|<harness>\n[warn] Output truncated to its beginning: 500 lines, "
        "2500 bytes dropped\n</harness>"
    )
    await result["harness"].close(context)


async def test_sanitizes_running_output_but_keeps_explicit_result_content() -> None:
    setup = chat_setup()
    seen: dict[str, Any] = {}

    async def noisy(_args: Any, api: Any, _context: Any) -> Any:
        api.output("a\x07b\r\n")
        await api.details({"ready": True}, context)
        live = cast(dict[str, Any], await api.snapshot(LiveDoc, api.conversation_id, context))
        seen["slot"] = live["tools"][0]["output"]
        return {}

    add_tool(setup.registry, tool("noisy", noisy))
    add_tool(
        setup.registry,
        tool("explicit", lambda *_args: {"output": [{"type": "text", "text": "c\x1bd"}]}),
    )
    result = await run(setup, [calls(["noisy", {}, "c1"], ["explicit", {}, "c2"]), DONE])
    assert seen["slot"] == "ab\n"
    by_id = {item["toolCallId"]: result_text(item) for item in results(result["entries"])}
    assert by_id["c1"] == "ab\n"
    assert by_id["c2"] == "c\x1bd"
    await result["harness"].close(context)


async def test_treats_a_null_control_field_as_json_null_and_still_applies_add_tools() -> None:
    setup = chat_setup()

    def grow(*_args: Any) -> Any:
        return {"output": [], "control": {"addTools": ["extra"], "terminate": None}}

    add_tool(setup.registry, tool("grow", grow))
    extra = tool("extra", lambda *_args: {"output": []})
    add_tool(setup.registry, extra)
    result = await run(
        setup,
        [calls(["grow", {}, "c1"]), DONE],
        lambda _harness, root: root.configure({"tools": {"remove": [extra]}}, context),
    )
    assert result["status"] == "done"
    # addTools deletes the name from a stored `{ remove }` filter.
    assert (await result["harness"].snapshot(AgentDoc, result["root"].id, context))["tools"] == {
        "remove": []
    }
    await result["harness"].close(context)


async def test_uses_explicit_null_details_instead_of_the_last_reported_value() -> None:
    setup = chat_setup()

    async def null_details(_args: Any, api: Any, _context: Any) -> Any:
        await api.details({"old": 1}, context)
        return {"output": [], "details": None}

    add_tool(setup.registry, tool("null", null_details))
    result = await run(setup, [calls(["null", {}, "c1"]), DONE])
    assert results(result["entries"])[0]["details"] is None
    await result["harness"].close(context)


async def test_settles_details_promises_with_coalesced_progress_commits_and_the_terminal_commit() -> None:
    setup = chat_setup()
    settled: list[str] = []

    async def details_tool(_args: Any, api: Any, _context: Any) -> Any:
        async def track(coroutine: Any, name: str) -> None:
            await coroutine
            settled.append(name)

        # Three updates in one throttle window coalesce; the last is still pending when execute returns.
        first = asyncio.ensure_future(track(api.details({"n": 1}, context), "first"))
        second = asyncio.ensure_future(track(api.details({"n": 2}, context), "second"))
        await first
        third = asyncio.ensure_future(track(api.details({"n": 3}, context), "third"))
        second.add_done_callback(lambda _: None)
        third.add_done_callback(lambda _: None)
        # Let the last update register its waiter: it is still pending when execute() returns.
        await asyncio.sleep(0)
        return {"output": []}

    add_tool(setup.registry, tool("details", details_tool))
    result = await run(setup, [calls(["details", {}, "c1"]), DONE])
    assert settled == ["first", "second", "third"]
    assert results(result["entries"])[0]["details"] == {"n": 3}
    await result["harness"].close(context)


async def test_finishes_a_call_under_the_implementation_it_resolved_when_the_tool_is_replaced() -> None:
    setup = chat_setup()
    started = asyncio.Event()
    finished = asyncio.Event()

    async def v1(_args: Any, _api: Any, _context: Any) -> Any:
        started.set()
        await finished.wait()
        return {"output": [{"type": "text", "text": "v1"}]}

    add_tool(setup.registry, tool("work", v1))
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    from chat_support import open_chat

    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await started.wait()
    # The same extension name replaces the old one in place.
    add_tool(setup.registry, tool("work", lambda *_args: {"output": [{"type": "text", "text": "v2"}]}))
    finished.set()
    await submission.wait(context)
    assert result_text(results(await all_entries(root))[0]) == "v1"
    await harness.close(context)


async def test_rejects_invocation_bound_waits_and_stops_watches_when_the_tool_invocation_ends() -> None:
    setup = chat_setup()
    never = define_task(
        {
            "name": "test.never",
            "version": 1,
            "initial": lambda _input: {"phase": "never"},
            "phases": {"never": _never},
            "abort": _never,
        }
    )
    held: dict[str, Any] = {}

    async def detach(_args: Any, api: Any, _context: Any) -> Any:
        # The child's definition is not registered, so it stays pending.
        child = await api.create_task(never, {}, {"ownership": {"kind": "conversation"}}, context)
        held["wait"] = asyncio.ensure_future(api.wait_for_task(child, context))
        watch = await api.watch_doc(LiveDoc, api.conversation_id, context)
        held["closed"] = watch.closed
        return {"output": []}

    add_tool(setup.registry, tool("detach", detach))
    result = await run(setup, [calls(["detach", {}, "c1"]), DONE])
    with pytest.raises(RuntimeError, match="invocation has ended"):
        await held["wait"]
    await held["closed"]
    await result["harness"].close(context)


async def test_rejects_details_still_waiting_when_the_call_is_aborted_during_after_tool() -> None:
    setup = chat_setup()
    held: dict[str, Any] = {}
    in_after_tool = asyncio.Event()

    async def slow(_args: Any, api: Any, _context: Any) -> Any:
        api.output("first\n")
        # The output commit is in flight, so these details wait for the next throttle window.
        held["details"] = asyncio.ensure_future(api.details({"step": 1}, context))
        held["details"].add_done_callback(lambda _: None)
        return {}

    add_tool(setup.registry, tool("slow", slow))

    async def after_tool(_call: Any, _result: Any, _api: Any, call_context: Any) -> Any:
        in_after_tool.set()
        await aborted(call_context.abort_signal)
        return None

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["slow", {}, "c1"]), DONE])
    from chat_support import open_chat

    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await in_after_tool.wait()
    live = await harness.snapshot(LiveDoc, root.id, context)
    task_id = live["tools"][0]["taskId"]
    await harness.abort_task(task_id, context)
    with pytest.raises(AbortError):
        await held["details"]
    await submission.wait(context)
    await harness.close(context)


async def test_answers_an_aborted_tool_with_only_its_durable_output() -> None:
    setup = chat_setup()
    reached = asyncio.Event()

    async def slow(_args: Any, api: Any, call_context: Any) -> Any:
        api.output("durable\n")
        # The first output commits at once; this one waits for the next throttle window.
        await asyncio.sleep(0.02)
        api.output("buffered\n")
        reached.set()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("slow", slow))
    setup.faux.set_responses([calls(["slow", {}, "c1"]), DONE])
    from chat_support import open_chat

    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await reached.wait()
    live = await harness.snapshot(LiveDoc, root.id, context)
    task_id = live["tools"][0]["taskId"]
    await harness.abort_task(task_id, context)
    await submission.wait(context)
    assert result_text(results(await all_entries(root))[0]) == (
        "durable\n|<harness>\n[error] Tool slow was aborted\n</harness>"
    )
    await harness.close(context)


async def test_records_a_results_usage_under_its_tool_and_on_the_message() -> None:
    from chat_support import usage as chat_usage

    setup = chat_setup()
    spent = chat_usage(input=3, output=2)
    add_tool(
        setup.registry,
        tool("spend", lambda *_args: {"output": [], "usage": spent}),
    )
    result = await run(setup, [calls(["spend", {}, "c1"]), DONE])
    [tool_result] = results(result["entries"])
    assert tool_result["usage"] == spent
    ledger = await result["harness"].snapshot(UsageDoc, result["root"].id, context)
    assert ledger["tools"]["spend"] == spent
    await result["harness"].close(context)


async def test_a_round_handoff_appends_a_reset_entry_and_ends_the_run() -> None:
    setup = chat_setup()
    add_tool(
        setup.registry,
        tool(
            "handoff",
            lambda *_args: {
                "output": [{"type": "text", "text": "bye"}],
                "control": {"handoff": "take over"},
            },
        ),
    )
    result = await run(setup, [calls(["handoff", {}, "c1"])])
    assert result["status"] == "done"
    entries = result["entries"]
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "pi.tool-result",
        "pi.reset",
    ]
    reset = entries[-1]
    assert reset["head"] == reset["id"]
    assert reset["model"][0]["content"] == "take over"
    # The handoff ends the run: no successor generation asks the model again.
    assert setup.faux.call_count == 1
    await result["harness"].close(context)

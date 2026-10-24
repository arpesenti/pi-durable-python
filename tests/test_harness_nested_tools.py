"""Translated upstream ``harness-nested-tools`` cases.

A running tool calls another tool as its own ``pi.tool`` child task, linked to the parent, with
replay keys and a ``NestedCallDoc``; its result returns to the caller and never enters the
transcript. Structured output is covered by the ``harness-structured-output`` suite.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, cast

from session_support import context
from task_support import aborted, deferred, eventually
from tool_support import (
    DONE,
    add_hooks,
    add_tool,
    all_entries,
    calls,
    chat_setup,
    open_chat,
    results,
    run,
    tool,
)

from pi_durable import LiveDoc, MemoryStorage, NestedCallDoc, ToolTask, UsageDoc, watch_events
from pi_durable.context import AbortController, with_abort_signal


def call(name: str, args: dict[str, Any] | None = None, call_id: str = "c1") -> dict[str, Any]:
    return calls([name, args or {}, call_id])


def text(result: Mapping[str, Any] | None) -> str:
    """The text of a result message, or the value a schema-less nested call gives programs."""

    if result is None:
        return ""
    if "role" not in result:
        value = result.get("structuredOutput")
        return value if isinstance(value, str) else ""
    return "|".join(
        cast(str, item.get("text")) if item.get("type") == "text" else f"[{item.get('type')}]"
        for item in cast(list[dict[str, Any]], result.get("content") or ())
    )


async def tool_tasks(harness: Any) -> list[dict[str, Any]]:
    page = await harness.commit(lambda tx: tx.scan_tasks({"kind": "pi.tool"}, 100), context)
    return list(page["items"])


def echo_tool(**extra: Any) -> tuple[Any, dict[str, int]]:
    state = {"runs": 0}

    def execute(args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        state["runs"] += 1
        return {"output": [{"type": "text", "text": f"echo {args.get('text') or ''}"}]}

    return tool("echo", execute, **extra), state


async def test_runs_a_nested_call_as_its_own_tool_task_and_returns_its_result_to_the_caller() -> None:
    setup = chat_setup()
    echo, _state = echo_tool()
    add_tool(setup.registry, echo)
    seen: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        nested = await api.execute_tool("echo", {"text": "a"}, call_context)
        seen.append(nested)
        return {"output": [{"type": "text", "text": f"got {text(nested)}"}]}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    harness, root, entries = result["harness"], result["root"], result["entries"]
    assert result["status"] == "done"
    parent, child = await tool_tasks(harness)
    [nested] = seen
    assert nested["taskId"] == child["id"]
    assert nested["structuredOutput"] == "echo a"
    assert nested["isError"] is False
    assert nested["diagnostics"] == []
    assert isinstance(nested["durationMs"], int)
    # One result entry: the model-issued call's, which records nothing of its nested calls.
    assert [(entry["toolCallId"], text(entry)) for entry in results(entries)] == [("c1", "got echo a")]
    assert "nestedCalls" not in results(entries)[0]
    assert (parent["input"]["kind"], parent["input"]["callId"]) == ("model", "c1")
    assert child["owner"] == parent["id"]
    assert child["input"] == {
        "kind": "nested",
        "parent": parent["id"],
        "parentCallId": "c1",
        "key": "1",
        "call": {"type": "toolCall", "id": "c1/1", "name": "echo", "arguments": {"text": "a"}},
    }
    # The receipt stays small: the result lived in the caller's documents, which retired with it.
    assert child["state"] == {
        "status": "terminal",
        "outcome": {"status": "completed", "result": {"kind": "nested"}},
    }
    assert await harness.snapshot(NestedCallDoc, parent["id"], "1", context) is None
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await harness.close(context)


async def test_stores_one_document_per_nested_call_keyed_by_its_key() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("echo", {"text": "a"}, call_context)
        await api.execute_tool("echo", {"text": "b"}, call_context, {"key": "named"})
        return {}

    add_tool(setup.registry, tool("batch", batch))
    written: list[tuple[str, str | None]] = []

    class RecordingStorage(MemoryStorage):
        async def commit(self, writes: Any, commit_context: Any) -> Any:
            for write in writes:
                if write["type"] == "document.create":
                    written.append((write["record"]["kind"], write["record"].get("key")))
            return await super().commit(writes, commit_context)

    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(RecordingStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert [entry for entry in written if entry[0].startswith("pi.tool.")] == [
        ("pi.tool.nested-call", "1"),
        ("pi.tool.nested-call", "named"),
    ]
    await harness.close(context)


async def test_gives_each_nested_call_its_own_task_and_call_id() -> None:
    setup = chat_setup()
    seen: list[tuple[Any, str]] = []

    def who(_args: dict[str, Any], api: Any, _context: Any) -> dict[str, Any]:
        seen.append((api.task_id, api.call_id))
        return {}

    add_tool(setup.registry, tool("who", who))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await asyncio.gather(
            api.execute_tool("who", {}, call_context, {"key": "a"}),
            api.execute_tool("who", {}, call_context, {"key": "b"}),
        )
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert sorted(call_id for _task_id, call_id in seen) == ["c1/a", "c1/b"]
    assert len({task_id for task_id, _call_id in seen}) == 2
    await result["harness"].close(context)


async def test_runs_the_tool_hooks_on_nested_calls_which_see_their_parent() -> None:
    setup = chat_setup()
    echo, state = echo_tool()
    add_tool(setup.registry, echo)
    parents: list[dict[str, Any] | None] = []

    def before_tool(hook_call: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any] | None:
        parents.append(hook_call.get("parent"))
        if hook_call.get("parent") is not None and hook_call["arguments"].get("text") == "secret":
            return {"block": "no secrets"}
        return None

    def after_tool(hook_call: dict[str, Any], hook_result: dict[str, Any], _api: Any, _context: Any) -> Any:
        if hook_call.get("parent") is None:
            return None
        return {**hook_result, "output": [{"type": "text", "text": "rewritten"}]}

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool, "afterTool": after_tool})
    nested: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        nested.append(await api.execute_tool("echo", {"text": "secret"}, call_context))
        nested.append(await api.execute_tool("echo", {"text": "public"}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    parent = (await tool_tasks(result["harness"]))[0]
    assert parents == [
        None,
        {"taskId": parent["id"], "callId": "c1"},
        {"taskId": parent["id"], "callId": "c1"},
    ]
    assert nested[0]["isError"] is True
    assert nested[0]["diagnostics"] == [
        {"severity": "error", "code": "blocked", "message": "Tool call blocked: no secrets"}
    ]
    assert text(nested[1]) == "rewritten"
    assert state["runs"] == 1
    await result["harness"].close(context)


async def test_returns_error_results_for_an_unavailable_tool_and_invalid_arguments() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])
    nested: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        nested.append(await api.execute_tool("missing", {}, call_context))
        nested.append(await api.execute_tool("echo", {"text": {"not": "a string"}}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert [(item["isError"], item["diagnostics"][0]["code"]) for item in nested] == [
        (True, "tool_unavailable"),
        (True, "invalid_arguments"),
    ]
    await result["harness"].close(context)


async def test_returns_the_call_a_key_already_names_and_rejects_reusing_it_for_another_call() -> None:
    setup = chat_setup()
    echo, state = echo_tool()
    add_tool(setup.registry, echo)
    reused: dict[str, Any] = {}

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("echo", {"text": "a"}, call_context, {"key": "k"})
        reused.update(await api.execute_tool("echo", {"text": "a"}, call_context, {"key": "k"}))
        await api.execute_tool("echo", {"text": "b"}, call_context, {"key": "k"})
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert state["runs"] == 1
    assert text(reused) == "echo a"
    assert "Nested call c1/k was already made with another tool or other arguments" in text(
        results(result["entries"])[0]
    )
    await result["harness"].close(context)


async def test_matches_a_reused_keys_arguments_regardless_of_key_order() -> None:
    setup = chat_setup()
    runs = {"count": 0}

    def pair(args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        runs["count"] += 1
        return {"output": [{"type": "text", "text": f"{args['a']}{args['b']}"}]}

    add_tool(
        setup.registry,
        tool(
            "pair",
            pair,
            parameters={
                "type": "object",
                "properties": {"a": {"type": "string"}, "b": {"type": "string"}},
            },
        ),
    )
    nested: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        nested.append(await api.execute_tool("pair", {"a": "x", "b": "y"}, call_context, {"key": "k"}))
        nested.append(await api.execute_tool("pair", {"b": "y", "a": "x"}, call_context, {"key": "k"}))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert runs["count"] == 1
    assert [text(item) for item in nested] == ["xy", "xy"]
    await result["harness"].close(context)


async def test_aborts_the_nested_calls_a_caller_left_running_when_it_returns() -> None:
    setup = chat_setup()
    running = deferred()
    stored: list[dict[str, Any]] = []

    async def hang(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        api.output("partial\n")
        await api.details({"step": 1}, call_context)
        running.resolve()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("hang", hang))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        abandoned = asyncio.ensure_future(api.execute_tool("hang", {}, call_context))
        abandoned.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)
        await running.promise
        return {"output": [{"type": "text", "text": "returned early"}]}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]

    def capture(publication: Any, _listener_context: Any) -> None:
        for change in publication["changes"]:
            if change["type"] != "document" or change["record"]["kind"] != "pi.tool.nested-call":
                continue
            value = cast(dict[str, Any] | None, change["value"])
            if value is not None and "result" in value:
                stored.append(value["result"])

    harness.subscribe_commits(capture)
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    await harness.wait_for_idle(context)
    assert text(results(await all_entries(root))[0]) == "returned early"
    _parent, child = await tool_tasks(harness)
    assert child["state"]["status"] == "terminal"
    assert child["state"]["outcome"] == {"status": "aborted", "result": {"kind": "nested"}}
    # The aborted call's result keeps its details; programs get no partial output, which only the model read.
    [result] = stored
    assert result["isError"] is True
    assert result["details"] == {"step": 1}
    assert "structuredOutput" not in result
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await harness.close(context)


async def test_records_a_nested_calls_usage_under_its_tool() -> None:
    setup = chat_setup()
    usage = {
        "input": 1,
        "output": 2,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 3,
        "cost": {"input": 0.1, "output": 0.2, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.3},
    }
    add_tool(setup.registry, tool("paid", lambda _args, _api, _context: {"usage": usage}))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("paid", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    usage_state = await result["harness"].snapshot(UsageDoc, result["root"].id, context)
    assert usage_state["tools"] == {"paid": usage}
    await result["harness"].close(context)


async def test_aborts_nested_calls_with_their_caller() -> None:
    setup = chat_setup()
    running = deferred()

    async def hang(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        running.resolve()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("hang", hang))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("hang", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await running.promise
    await root.abort(context)
    assert (await submission.wait(context))["status"] == "unanswered"
    parent, child = await tool_tasks(harness)
    assert child["state"]["outcome"] == {"status": "aborted", "result": {"kind": "nested"}}
    assert parent["state"]["outcome"]["status"] == "aborted"
    assert parent["state"]["outcome"]["result"]["kind"] == "model"
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await harness.close(context)


async def test_aborts_a_running_nested_call_when_its_caller_throws() -> None:
    setup = chat_setup()
    running = deferred()

    async def hang(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        running.resolve()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("hang", hang))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        future = asyncio.ensure_future(api.execute_tool("hang", {}, call_context))
        future.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        await running.promise
        raise Exception("caller failed")

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert result["status"] == "done"
    assert "caller failed" in text(results(result["entries"])[0])
    _parent, child = await tool_tasks(result["harness"])
    assert child["state"]["status"] == "terminal"
    assert child["state"]["outcome"]["status"] == "aborted"
    await result["harness"].close(context)


async def test_rejects_explicit_keys_that_are_empty_slashed_proto_or_positive_integers() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])
    errors: list[str] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        for key in ("", "a/b", "__proto__", "7"):
            try:
                await api.execute_tool("echo", {}, call_context, {"key": key})
            except Exception as error:
                errors.append(str(error))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    rule = 'must be non-empty, without "/", not "__proto__", and not a positive integer'
    assert errors == [
        f'Nested call key "" {rule}',
        f'Nested call key "a/b" {rule}',
        f'Nested call key "__proto__" {rule}',
        f'Nested call key "7" {rule}',
    ]
    await result["harness"].close(context)


async def test_runs_nested_calls_of_nested_calls() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])

    async def inner(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        result = await api.execute_tool("echo", {"text": "deep"}, call_context, {"key": "x"})
        return {"output": [{"type": "text", "text": text(result)}]}

    add_tool(setup.registry, tool("inner", inner))

    async def outer(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        result = await api.execute_tool("inner", {}, call_context)
        return {"output": [{"type": "text", "text": text(result)}]}

    add_tool(setup.registry, tool("outer", outer))
    result = await run(setup, [call("outer"), DONE])
    assert text(results(result["entries"])[0]) == "echo deep"
    outer_task, inner_task, deep_task = await tool_tasks(result["harness"])
    assert (inner_task["owner"], deep_task["owner"]) == (outer_task["id"], inner_task["id"])
    assert await result["harness"].snapshot(LiveDoc, result["root"].id, context) == {}
    await result["harness"].close(context)


async def test_commits_only_the_status_of_a_nested_call_made_without_progress() -> None:
    setup = chat_setup()
    running = deferred()
    release = deferred()

    async def chatty(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        api.output("line\n")
        api.diagnostic({"severity": "info", "message": "note"})
        await api.details({"step": 1}, call_context)
        running.resolve()
        await release.promise
        return {}

    add_tool(setup.registry, tool("chatty", chatty))
    received: dict[str, Any] = {}

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.update(await api.execute_tool("chatty", {}, call_context, {"progress": False}))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await running.promise
    live = await harness.snapshot(LiveDoc, root.id, context)
    [slot] = live["nestedTools"]
    assert slot == {
        "callId": "c1/1",
        "parentCallId": "c1",
        "parentTaskId": slot["parentTaskId"],
        "name": "chatty",
        "taskId": slot["taskId"],
        "arguments": {},
        "status": "running",
    }
    release.resolve()
    await submission.wait(context)
    assert received["structuredOutput"] == "line\n"
    assert received["details"] == {"step": 1}
    assert received["diagnostics"] == [{"severity": "info", "message": "note"}]
    await harness.close(context)


async def test_loses_the_running_output_of_a_nested_call_without_progress_when_it_is_aborted() -> None:
    setup = chat_setup()
    running = deferred()
    stored: list[dict[str, Any]] = []

    async def chatty(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        api.output("line\n")
        api.diagnostic({"severity": "info", "message": "note"})
        await api.details({"step": 1}, call_context)
        running.resolve()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("chatty", chatty))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        future = asyncio.ensure_future(api.execute_tool("chatty", {}, call_context, {"progress": False}))
        future.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        await running.promise
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]

    def capture(publication: Any, _listener_context: Any) -> None:
        for change in publication["changes"]:
            if change["type"] != "document" or change["record"]["kind"] != "pi.tool.nested-call":
                continue
            value = cast(dict[str, Any] | None, change["value"])
            if value is not None and "result" in value:
                stored.append(value["result"])

    harness.subscribe_commits(capture)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await harness.wait_for_idle(context)
    # The abort's result is built from the slot, which never got the output.
    [result] = stored
    assert result["isError"] is True
    assert "structuredOutput" not in result
    assert "details" not in result
    assert [diagnostic["code"] for diagnostic in result["diagnostics"]] == ["aborted"]
    await harness.close(context)


async def test_shows_a_nested_calls_arguments_as_made_then_as_it_runs_with_them() -> None:
    setup = chat_setup()
    entered = deferred()
    released = deferred()
    running = deferred()
    finish = deferred()
    large = "x" * (64 * 1024)

    async def slow(_args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        running.resolve()
        await finish.promise
        return {}

    add_tool(setup.registry, tool("slow", slow))

    async def before_tool(hook_call: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any] | None:
        if hook_call.get("parent") is None:
            return None
        entered.resolve()
        await released.promise
        return {"arguments": {"text": large}}

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool})

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("slow", {"text": "asked"}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)

    async def slot() -> dict[str, Any]:
        live = await harness.snapshot(LiveDoc, root.id, context)
        return live["nestedTools"][0]

    await entered.promise
    assert await slot() == {
        "callId": "c1/1",
        "parentCallId": "c1",
        "parentTaskId": (await slot())["parentTaskId"],
        "name": "slow",
        "taskId": (await slot())["taskId"],
        "arguments": {"text": "asked"},
        "status": "pending",
    }
    released.resolve()
    await running.promise
    running_slot = await slot()
    assert (running_slot["status"], running_slot["arguments"]) == ("running", {"text": large})
    finish.resolve()
    await submission.wait(context)
    await harness.wait_for_idle(context)
    nested = next(task for task in await tool_tasks(harness) if task["input"]["kind"] == "nested")
    assert nested["input"]["call"]["arguments"] == {"text": "asked"}
    await harness.close(context)


async def test_runs_a_nested_call_whose_repaired_arguments_clear_an_optional_property() -> None:
    setup = chat_setup()
    seen: list[Any] = []

    def execute(args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        seen.append(args)
        return {}

    add_tool(setup.registry, tool("repaired", execute, prepare_arguments=lambda _args: {"text": None}))
    received: dict[str, Any] = {}

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.update(await api.execute_tool("repaired", {"text": "x"}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert received["isError"] is False
    assert seen == [{}]
    await result["harness"].close(context)


async def test_finds_each_nested_calls_slot_after_slots_in_the_middle_were_removed() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])

    async def inner(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("echo", {"text": "m1"}, call_context)
        await api.execute_tool("echo", {"text": "m2"}, call_context)
        return {}

    add_tool(setup.registry, tool("inner", inner))
    before = deferred()
    checked = deferred()
    observed: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("echo", {"text": "n1"}, call_context)
        await api.execute_tool("inner", {}, call_context)
        await api.execute_tool("echo", {"text": "n3"}, call_context)
        await api.execute_tool("echo", {"text": "n4"}, call_context)
        live = await api.snapshot(LiveDoc, api.conversation_id, call_context)
        observed.extend(
            {"callId": slot["callId"], "status": slot["status"], "summary": slot.get("summary")}
            for slot in live.get("nestedTools") or ()
        )
        before.resolve()
        await checked.promise
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await before.promise
    for slot in observed:
        assert slot["status"] == "done"
        summary = slot["summary"]
        assert summary["isError"] is False
        assert isinstance(summary["durationMs"], int)
    assert [slot["callId"] for slot in observed] == ["c1/1", "c1/2", "c1/3", "c1/4"]
    checked.resolve()
    await submission.wait(context)
    await harness.close(context)


async def test_gives_callers_a_synthesized_result_for_a_nested_call_the_scheduler_faulted() -> None:
    setup = chat_setup()
    # Details that are not JSON make the result commit throw, so the task faults and stores no result.
    add_tool(setup.registry, tool("echo", lambda _args, _api, _context: {"details": object()}))
    received: dict[str, Any] = {}

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.update(await api.execute_tool("echo", {"text": "a"}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    _parent, child = await tool_tasks(harness)
    assert child["state"]["status"] == "terminal"
    assert child["state"]["outcome"]["status"] == "faulted"
    assert received["taskId"] == child["id"]
    assert received["isError"] is True
    assert received["diagnostics"][0]["code"] == "faulted"
    assert "Tool echo failed" in received["diagnostics"][0]["message"]
    await harness.close(context)


async def test_gives_callers_what_a_schema_less_tool_streamed_before_it_threw() -> None:
    setup = chat_setup()

    def fails(_args: dict[str, Any], api: Any, _context: Any) -> dict[str, Any]:
        api.output("half way\n")
        raise Exception("broke")

    add_tool(setup.registry, tool("fails", fails))
    received: dict[str, Any] = {}

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.update(await api.execute_tool("fails", {}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert received["isError"] is True
    assert received["structuredOutput"] == "half way\n"
    assert received["diagnostics"] == [{"severity": "error", "code": "tool_error", "message": "broke"}]
    await result["harness"].close(context)


async def test_shows_a_finished_nested_call_as_a_done_slot_with_its_summary() -> None:
    setup = chat_setup()
    checked = deferred()
    release = deferred()
    usage = {
        "input": 1,
        "output": 2,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 3,
        "cost": {"input": 0.1, "output": 0.2, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.3},
    }
    add_tool(
        setup.registry,
        tool(
            "verbose",
            lambda _args, _api, _context: {
                "output": [{"type": "text", "text": "ignored"}],
                "isError": True,
                "diagnostics": [{"severity": "error", "message": "e" * 600}],
            },
        ),
    )
    add_tool(setup.registry, echo_tool()[0])
    add_tool(setup.registry, tool("paid", lambda _args, _api, _context: {"usage": usage}))
    add_tool(
        setup.registry,
        tool(
            "broken",
            lambda _args, _api, _context: {
                "output": [
                    {"type": "text", "text": "bad image"},
                    {"type": "image", "data": "AAAA", "mimeType": "image/png"},
                ],
                "isError": True,
            },
        ),
    )

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("echo", {"text": "a"}, call_context)
        await api.execute_tool("missing", {}, call_context)
        await api.execute_tool("paid", {}, call_context)
        await api.execute_tool("broken", {}, call_context)
        await api.execute_tool("verbose", {}, call_context)
        checked.resolve()
        await release.promise
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await checked.promise
    slots = (await harness.snapshot(LiveDoc, root.id, context))["nestedTools"]
    summaries = [
        {"callId": slot["callId"], "status": slot["status"], "summary": slot.get("summary")} for slot in slots
    ]
    assert [(item["callId"], item["status"], item["summary"]["isError"]) for item in summaries] == [
        ("c1/1", "done", False),
        ("c1/2", "done", True),
        ("c1/3", "done", False),
        ("c1/4", "done", True),
        ("c1/5", "done", True),
    ]
    assert summaries[1]["summary"] == {"isError": True, "error": "Tool missing is not available"}
    assert summaries[2]["summary"]["usage"] == usage
    assert summaries[3]["summary"]["error"] == "bad image"
    assert isinstance(summaries[3]["summary"]["durationMs"], int)
    assert summaries[4]["summary"]["error"] == "e" * 500
    release.resolve()
    await submission.wait(context)
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await harness.close(context)


def test_migrates_version_1_tool_task_input_to_a_model_issued_call() -> None:
    migrate = ToolTask.definition.migrate
    assert migrate is not None
    assert migrate({"assistant": 3, "callId": "c1"}, {"phase": "call"}, 1) == {
        "input": {"kind": "model", "assistant": 3, "callId": "c1"},
        "checkpoint": {"phase": "call"},
    }


async def test_rejects_a_nested_call_of_an_aborted_caller_and_leaves_no_document_for_it() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])
    started = deferred()
    marked = deferred()
    outcome: dict[str, Any] = {}
    caller: list[Any] = []

    async def late(_args: dict[str, Any], api: Any, _context: Any) -> dict[str, Any]:
        caller.append(api.task_id)
        started.resolve()
        await marked.promise
        # Its context is aborted too, so pass one that is not: the admission itself must refuse.
        try:
            await api.execute_tool("echo", {"text": "a"}, context)
        except Exception as error:
            outcome["rejected"] = str(error)
        return {}

    add_tool(setup.registry, tool("late", late))
    setup.faux.set_responses([call("late"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise

    async def mark(tx: Any) -> None:
        record = await tx.task(caller[0])
        tx.set_task({**record, "abortRequested": True})

    await harness.commit(mark, context)
    marked.resolve()
    await submission.wait(context)
    assert "abort mark" in outcome["rejected"] or "aborted" in outcome["rejected"]
    assert await harness.snapshot(NestedCallDoc, caller[0], "1", context) is None
    await harness.close(context)


async def test_stops_only_the_wait_when_a_caller_cancels_it() -> None:
    setup = chat_setup()
    controller = AbortController()
    release = deferred()
    attempts: list[str] = []
    retried: dict[str, Any] = {}
    runs = {"count": 0}

    async def slow(_args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        runs["count"] += 1
        controller.abort(Exception("cancelled"))
        await release.promise
        return {"output": [{"type": "text", "text": "slow done"}]}

    add_tool(setup.registry, tool("slow", slow))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        cancelled = with_abort_signal(controller.signal, call_context)
        try:
            await api.execute_tool("slow", {}, cancelled, {"key": "same"})
            attempts.append("first done")
        except Exception:
            attempts.append("first cancelled")
        release.resolve()
        retried.update(await api.execute_tool("slow", {}, call_context, {"key": "same"}))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert attempts == ["first cancelled"]
    # The call ran once; the retry reattached to it and got its result.
    assert runs["count"] == 1
    assert retried["isError"] is False
    assert retried["structuredOutput"] == "slow done"
    assert len(await tool_tasks(result["harness"])) == 2
    await result["harness"].close(context)


async def test_admits_one_nested_call_for_a_key_used_by_concurrent_calls() -> None:
    setup = chat_setup()
    echo, state = echo_tool()
    add_tool(setup.registry, echo)
    both: list[dict[str, Any]] = []

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        both.extend(
            await asyncio.gather(
                api.execute_tool("echo", {"text": "a"}, call_context, {"key": "k"}),
                api.execute_tool("echo", {"text": "a"}, call_context, {"key": "k"}),
            )
        )
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert state["runs"] == 1
    assert [text(item) for item in both] == ["echo a", "echo a"]
    assert len(await tool_tasks(result["harness"])) == 2
    await result["harness"].close(context)


async def test_aborts_a_nested_call_whose_admission_was_still_committing_when_its_caller_returned() -> None:
    setup = chat_setup()
    add_tool(setup.registry, echo_tool()[0])

    async def hang(_args: dict[str, Any], _api: Any, call_context: Any) -> dict[str, Any]:
        await aborted(call_context.abort_signal)
        return {}

    add_tool(setup.registry, tool("hang", hang))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await api.execute_tool("echo", {"text": "warm"}, call_context, {"key": "warm"})
        future = asyncio.ensure_future(
            api.execute_tool("hang", {}, call_context, {"key": "late"})
        )
        future.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert result["status"] == "done"
    late = next(
        task
        for task in await tool_tasks(result["harness"])
        if task["input"]["kind"] == "nested" and task["input"]["call"]["name"] == "hang"
    )
    assert late["state"]["status"] == "terminal"
    assert late["state"]["outcome"]["status"] == "aborted"
    assert await result["harness"].snapshot(LiveDoc, result["root"].id, context) == {}
    await result["harness"].close(context)


async def test_aborts_every_left_running_nested_call_before_waiting_for_any() -> None:
    setup = chat_setup()
    b_aborted = deferred()
    started = {"a": deferred(), "b": deferred()}

    async def a_stops_only_once_b_is_told(
        _args: dict[str, Any], _api: Any, _context: Any
    ) -> dict[str, Any]:
        started["a"].resolve()
        await b_aborted.promise
        return {}

    async def b_aborts_then_resolves(
        _args: dict[str, Any], _api: Any, call_context: Any
    ) -> dict[str, Any]:
        started["b"].resolve()
        try:
            await aborted(call_context.abort_signal)
        except Exception:
            pass
        b_aborted.resolve()
        raise Exception("aborted")

    add_tool(setup.registry, tool("a", a_stops_only_once_b_is_told))
    add_tool(setup.registry, tool("b", b_aborts_then_resolves))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        first = asyncio.ensure_future(api.execute_tool("a", {}, call_context, {"key": "a"}))
        second = asyncio.ensure_future(api.execute_tool("b", {}, call_context, {"key": "b"}))
        for future in (first, second):
            future.add_done_callback(
                lambda done: done.exception() if not done.cancelled() else None
            )
        await asyncio.gather(started["a"].promise, started["b"].promise)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    result = await run(setup, [call("batch"), DONE])
    assert result["status"] == "done"
    nested = [task for task in await tool_tasks(result["harness"]) if task["input"]["kind"] == "nested"]
    assert [task["state"]["status"] for task in nested] == ["terminal", "terminal"]
    await result["harness"].close(context)


async def test_reports_several_running_nested_calls_in_their_slots() -> None:
    setup = chat_setup()
    release = deferred()
    running = {"one": deferred(), "two": deferred()}
    wrote = {"one": deferred(), "two": deferred()}

    async def slow(args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        name = args["text"]
        running[name].resolve()
        await wrote[name].promise
        api.output(f"{name}\n")
        await api.details({"name": name}, call_context)
        await release.promise
        return {}

    add_tool(setup.registry, tool("slow", slow))

    async def batch(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        await asyncio.gather(
            api.execute_tool("slow", {"text": "one"}, call_context),
            api.execute_tool("slow", {"text": "two"}, call_context),
        )
        return {}

    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses([call("batch"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await asyncio.gather(running["one"].promise, running["two"].promise)
    stream = await watch_events(harness, root.id, context)
    assert [
        (slot["callId"], slot["status"]) for slot in stream.snapshot["nestedTools"]
    ] == [("c1/1", "running"), ("c1/2", "running")]
    updates: list[tuple[str, Any]] = []

    def listen(batch: Any, _context: Any) -> None:
        for event in batch:
            if event["type"] == "tool_execution_update" and "output" in event:
                updates.append((event["toolCallId"], event["output"]))

    stream.start(listen)
    # The second slot writes first, so a wrong slot index in the update path would show.
    wrote["two"].resolve()
    await wrote["two"].promise
    await eventually(lambda: len(updates) == 1)

    async def two_shows() -> bool:
        live = await harness.snapshot(LiveDoc, root.id, context)
        slots = live["nestedTools"]
        return slots[1].get("output") == "two\n" and slots[1].get("details") == {"name": "two"}

    await eventually(two_shows)
    wrote["one"].resolve()
    await wrote["one"].promise
    await eventually(lambda: len(updates) == 2)

    async def both_show() -> bool:
        live = await harness.snapshot(LiveDoc, root.id, context)
        return [slot.get("output") for slot in live["nestedTools"]] == ["one\n", "two\n"]

    await eventually(both_show)
    release.resolve()
    await submission.wait(context)
    await stream.stop()
    assert updates == [("c1/2", {"set": "two\n"}), ("c1/1", {"set": "one\n"})]
    await harness.close(context)


async def test_resolves_nested_calls_among_the_tools_that_may_be_called_by_tools() -> None:
    setup = chat_setup()
    add_tool(setup.registry, tool("script", lambda _args, _api, _context: {}, callers=["model"]))
    add_tool(
        setup.registry,
        tool(
            "hidden",
            lambda _args, _api, _context: {"output": [{"type": "text", "text": "hidden ran"}]},
            callers=["tools"],
        ),
    )
    add_tool(
        setup.registry,
        tool(
            "both",
            lambda _args, _api, _context: {"output": [{"type": "text", "text": "both ran"}]},
        ),
    )
    received: dict[str, dict[str, Any]] = {}

    async def probe(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        for name in ("script", "hidden", "both"):
            received[name] = await api.execute_tool(name, {}, call_context)
        return {}

    add_tool(setup.registry, tool("probe", probe))
    setup.faux.set_responses([call("probe"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    assert [item.name for item in (await root.agent(context)).tools] == ["script", "both", "probe"]
    assert [item.name for item in (await root.agent(context)).callable] == ["hidden", "both", "probe"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert received["script"]["diagnostics"][0]["code"] == "tool_unavailable"
    assert received["hidden"]["structuredOutput"] == "hidden ran"
    assert received["both"]["structuredOutput"] == "both ran"
    await harness.close(context)


async def test_narrows_what_the_model_is_offered_leaving_the_rest_callable_by_tools() -> None:
    setup = chat_setup()
    add_tool(setup.registry, tool("read", lambda _args, _api, _context: {}))
    add_tool(setup.registry, tool("bash", lambda _args, _api, _context: {}))
    received: dict[str, dict[str, Any]] = {}

    async def probe(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received["read"] = await api.execute_tool("read", {}, call_context)
        received["bash"] = await api.execute_tool("bash", {}, call_context)
        return {}

    add_tool(setup.registry, tool("probe", probe))
    setup.faux.set_responses([call("probe"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"modelTools": ["probe"]}, context)
    assert [item.name for item in (await root.agent(context)).tools] == ["probe"]
    assert [item.name for item in (await root.agent(context)).callable] == ["read", "bash", "probe"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert received["read"]["isError"] is False
    assert received["bash"]["isError"] is False
    await harness.close(context)

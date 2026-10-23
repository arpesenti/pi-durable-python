"""Translated v1.3.0 ``harness-events`` suite.

Covers the experimental agent event adapter: lifecycle events, streamed text and thinking deltas,
tool-call argument assembly, output windows, queued submissions and retries, boundaries, overflow
snapshots, document changes, aborts, cancellation, and close.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import AsyncIterator
from typing import Any, cast

import pytest
from chat_support import (
    AssistantStream,
    Step,
    assistant_message,
    chat_setup,
    faux_text,
    open_chat,
    slow_message,
    text_of,
)
from session_support import context, document_changes, flush
from task_support import aborted, deferred, eventually
from tool_support import DONE, add_tool, tool

from pi_durable import (
    AgentDoc,
    AgentEvent,
    AgentEventStream,
    LiveDoc,
    MemoryStorage,
    MessageChange,
    UsageDoc,
    watch_events,
)
from pi_durable.json import copy_json


async def listen(harness: Any, conversation: Any) -> dict[str, Any]:
    """Attach and start an event stream that records every delivered batch."""

    stream = await watch_events(harness, conversation.id, context)
    batches: list[list[AgentEvent]] = []

    def listener(events: list[AgentEvent], _context: Any) -> None:
        batches.append(list(events))

    stream.start(listener)
    return {
        "stream": stream,
        "batches": batches,
        "events": lambda: [event for batch in batches for event in batch],
    }


async def drained() -> None:
    await flush()


def streamed_text(events: list[AgentEvent]) -> str:
    text = ""
    for event in events:
        if event["type"] == "message_start" and event["message"].get("role") == "assistant":
            block = (event["message"].get("content") or [None])[0]
            text = block.get("text", "") if block and block.get("type") == "text" else ""
        if event["type"] != "message_update":
            continue
        for change in event["changes"]:
            if change["type"] == "text_start" and change["contentIndex"] == 0:
                text = change["block"].get("text", "")
            if change["type"] == "text_delta" and change["contentIndex"] == 0:
                text += change["delta"]
    return text


def apply_changes(message: Any, changes: list[MessageChange]) -> Any:
    """Apply message changes to a copy of ``message``, as an events-only consumer would."""

    current = copy.deepcopy(message)
    for change in changes:
        kind = change["type"]
        if kind == "message":
            current = copy.deepcopy(change["message"])
        elif kind == "block":
            current["content"][change["contentIndex"]] = copy.deepcopy(change["block"])
        elif kind.endswith("_start") and "block" in change:
            current["content"].insert(
                change["contentIndex"], copy.deepcopy(change["block"])
            )
        elif kind in ("text_delta", "thinking_delta"):
            block = current["content"][change["contentIndex"]]
            field = "text" if kind == "text_delta" else "thinking"
            block[field] = f"{block.get(field, '')}{change['delta']}"
        elif kind == "toolcall_delta":
            target = current["content"][change["contentIndex"]]["arguments"]
            for segment in change["path"][:-1]:
                target = target[segment]
            last = change["path"][-1]
            target[last] = f"{target.get(last, '')}{change['delta']}"
    return current


def partials_of(harness: Any) -> list[Any]:
    partials: list[Any] = []

    def observer(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            value = change["value"]
            message = (
                (value or {}).get("generation", {}).get("message")
                if isinstance(value, dict)
                else None
            )
            if change["record"]["kind"] == "pi.live" and message is not None:
                partials.append(message)

    harness.subscribe_commits(observer)
    return partials


def last_snapshot(harness: Any, conversation: Any) -> dict[str, Any]:
    return {"run": None}


async def live(harness: Any, conversation: Any) -> dict[str, Any]:
    return dict((await harness.snapshot(LiveDoc, conversation.id, context)) or {})


async def test_streams_a_run_as_lifecycle_events_and_text_deltas_that_rebuild_the_answer() -> None:
    setup = chat_setup()
    text = "streamed answer text " * 20
    setup.faux.set_responses(
        [slow_message(assistant_message([faux_text(text)]), chunks=20, interval=0.01)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream = listener["stream"]
    assert stream.snapshot["type"] == "snapshot"
    assert stream.snapshot["entries"] == []
    assert stream.snapshot["tools"] == []
    assert stream.snapshot["inbox"] == []
    partials = partials_of(harness)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await submission.wait(context)
    await drained()
    all_events = listener["events"]()
    types = [
        event["type"]
        for index, event in enumerate(all_events)
        if index == 0 or event["type"] != all_events[index - 1]["type"]
    ]
    assert types == [
        "message_start",
        "message_end",
        "submission",
        "run_start",
        "turn_start",
        "message_start",
        "message_update",
        "message_end",
        "turn_end",
        "run_end",
        "submission",
        "usage_changed",
    ]
    assert next(e for e in all_events if e["type"] == "run_start") == {
        "type": "run_start",
        "inputs": [submission.id],
    }
    # After each event, the rebuilt text equals the committed partial of that commit.
    rebuilt = [
        streamed_text(all_events[: index + 1])
        for index, event in enumerate(all_events)
        if (event["type"] == "message_start" and event["message"].get("role") == "assistant")
        or event["type"] == "message_update"
    ]
    assert rebuilt == [text_of(partial) or "" for partial in partials]
    assert len(partials) > 1
    end = [event for event in all_events if event["type"] == "message_end"][-1]
    assert text_of(end["entry"]["model"][0]) == text
    await stream.stop()
    await harness.close(context)


async def test_reports_tool_start_output_appends_and_the_result_entry() -> None:
    setup = chat_setup()
    gate = deferred()

    async def execute(_args: Any, api: Any, _context: Any) -> dict[str, Any]:
        api.output("one\n")
        await asyncio.sleep(0.15)
        api.output("two\n")
        await gate.promise
        return {}

    add_tool(setup.registry, tool("print", execute))
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "print", "arguments": {"n": 1}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)

    def output() -> str:
        text = ""
        for event in events():
            if event["type"] != "tool_execution_update" or event.get("output") is None:
                continue
            value = event["output"]
            if "set" in value:
                text = value["set"]
            else:
                text = text[value.get("trimStart", 0):] + value.get("append", "")
        return text

    await eventually(lambda: output() == "one\ntwo\n", timeout=5)
    gate.resolve()
    await submission.wait(context)
    await drained()
    execution = [
        event
        for event in events()
        if event["type"].startswith("tool_execution")
    ]
    first = execution[0]
    assert first["type"] == "tool_execution_start"
    assert first["toolCallId"] == "c1"
    assert first["toolName"] == "print"
    assert isinstance(first["taskId"], int)
    assert first["args"] == {"n": 1}
    end = execution[-1]
    assert end["type"] == "tool_execution_end"
    assert end["toolCallId"] == "c1"
    assert end["entry"]["kind"] == "pi.tool-result"
    # As in the coding agent, the tool ends directly before its result message.
    all_events = events()
    end_index = all_events.index(end)
    assert [event["type"] for event in all_events[end_index:end_index + 3]] == [
        "tool_execution_end",
        "message_start",
        "message_end",
    ]
    assert all_events[end_index + 1]["message"]["role"] == "toolResult"
    assert all_events[end_index + 1]["message"]["toolCallId"] == "c1"
    # Two turns: the tool round, and the answer.
    assert len([event for event in events() if event["type"] == "turn_start"]) == 2
    assert len([event for event in events() if event["type"] == "turn_end"]) == 2
    await stream.stop()
    await harness.close(context)


async def test_reports_a_nested_call_with_its_parent_task_and_result() -> None:
    setup = chat_setup()
    release = deferred()
    running = deferred()

    async def slow(_args: Any, api: Any, call_context: Any) -> dict[str, Any]:
        api.output("working\n")
        await api.details({"step": 1}, call_context)
        running.resolve()
        await release.promise
        return {"output": [{"type": "text", "text": "slow done"}]}

    async def batch(_args: Any, api: Any, call_context: Any) -> dict[str, Any]:
        nested = await api.execute_tool("slow", {}, call_context)
        return {
            "output": [
                {"type": "text", "text": nested.get("structuredOutput") or ""}
            ]
        }

    add_tool(setup.registry, tool("slow", slow))
    add_tool(setup.registry, tool("batch", batch))
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "batch", "arguments": {}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await running.promise
    live_value = await live(harness, root)
    [nested_slot] = live_value["nestedTools"]
    assert nested_slot["callId"] == "c1/1"
    assert nested_slot["parentCallId"] == "c1"
    assert nested_slot["status"] == "running"
    release.resolve()
    await submission.wait(context)
    await harness.wait_for_idle(context)
    await drained()
    await stream.stop()
    calls = [
        (event["type"], event["toolCallId"], event.get("parentToolCallId"))
        for event in events()
        if event["type"] in ("tool_execution_start", "tool_execution_end")
    ]
    assert calls == [
        ("tool_execution_start", "c1", None),
        ("tool_execution_start", "c1/1", "c1"),
        ("tool_execution_end", "c1/1", "c1"),
        ("tool_execution_end", "c1", None),
    ]
    nested_end = next(
        event
        for event in events()
        if event["type"] == "tool_execution_end"
        and event["toolCallId"] == "c1/1"
    )
    assert nested_end["result"]["structuredOutput"] == "slow done"
    assert nested_end["result"]["isError"] is False
    assert nested_end["taskId"] == nested_slot["taskId"]
    assert nested_end["parentTaskId"] == nested_slot["parentTaskId"]
    parent_end = next(
        event
        for event in events()
        if event["type"] == "tool_execution_end" and event["toolCallId"] == "c1"
    )
    assert "parentTaskId" not in parent_end
    assert any(
        event["type"] == "tool_execution_update"
        and event["toolCallId"] == "c1/1"
        and event.get("parentToolCallId") == "c1"
        for event in events()
    )
    assert (await live(harness, root)) == {}
    await harness.close(context)


async def test_reports_deeply_nested_calls_children_before_their_caller() -> None:
    setup = chat_setup()

    async def echo(args: Any, _api: Any, _context: Any) -> dict[str, Any]:
        return {"output": [{"type": "text", "text": f"echo {args.get('text', '')}"}]}

    async def inner(_args: Any, api: Any, call_context: Any) -> dict[str, Any]:
        result = await api.execute_tool(
            "echo", {"text": "deep"}, call_context, {"key": "x"}
        )
        return {"output": [{"type": "text", "text": result.get("structuredOutput") or ""}]}

    async def outer(_args: Any, api: Any, call_context: Any) -> dict[str, Any]:
        result = await api.execute_tool("inner", {}, call_context)
        return {"output": [{"type": "text", "text": result.get("structuredOutput") or ""}]}

    add_tool(setup.registry, tool("echo", echo))
    add_tool(setup.registry, tool("inner", inner))
    add_tool(setup.registry, tool("outer", outer))
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "outer", "arguments": {}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await harness.wait_for_idle(context)
    await drained()
    await stream.stop()
    assert [
        (event["type"], event["toolCallId"], event.get("parentToolCallId"))
        for event in events()
        if event["type"] in ("tool_execution_start", "tool_execution_end")
    ] == [
        ("tool_execution_start", "c1", None),
        ("tool_execution_start", "c1/1", "c1"),
        ("tool_execution_start", "c1/1/x", "c1/1"),
        ("tool_execution_end", "c1/1/x", "c1/1"),
        ("tool_execution_end", "c1/1", "c1"),
        ("tool_execution_end", "c1", None),
    ]
    assert (await live(harness, root)) == {}
    await harness.close(context)


async def test_reports_queued_submissions_inbox_changes_and_retries() -> None:
    setup = chat_setup()
    release = deferred()

    async def held(_request: Any, _options: Any) -> Any:
        await release.promise
        return assistant_message([faux_text("first")])

    error = assistant_message(
        (), stop_reason="error", error_message="503 Service Unavailable"
    )
    setup.faux.set_responses(
        [held, error, assistant_message([faux_text("second")])]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    setup.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    await root.submit({"type": "input", "content": "a"}, context)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    await drained()
    assert [event for event in events() if event["type"] == "inbox_update"] == [
        {"type": "inbox_update", "items": [{"id": follow_up.id, "mode": "followUp"}]}
    ]
    release.resolve()
    await follow_up.wait(context)
    await drained()
    types = [event["type"] for event in events()]
    assert "auto_retry_start" in types
    assert "auto_retry_end" in types
    assert len([event for event in events() if event["type"] == "run_start"]) == 2
    assert [
        event["record"]["status"]
        for event in events()
        if event["type"] == "submission"
    ] == ["placed", "queued", "done", "placed", "done"]
    await stream.stop()
    await harness.close(context)


async def test_emits_a_batch_for_a_commit_that_changes_only_a_submission_record() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, batches = listener["stream"], listener["batches"]

    async def stale(tx: Any) -> None:
        await tx.create_submission(
            {
                "conversationId": root.id,
                "type": "write",
                "status": "unanswered",
                "reason": "stale",
                "entry": {"kind": "note"},
            }
        )

    await root.commit(stale, context)
    await drained()
    assert len(batches) == 1
    [event] = batches[0]
    assert event["type"] == "submission"
    assert event["record"]["status"] == "unanswered"
    assert event["record"]["reason"] == "stale"
    await stream.stop()
    await harness.close(context)


async def test_replaces_undelivered_batches_with_one_snapshot_after_100_pending_batches() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    stream = await watch_events(harness, root.id, context)
    for _index in range(101):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    batches: list[list[AgentEvent]] = []

    def listener(events: list[AgentEvent], _context: Any) -> None:
        batches.append(list(events))

    stream.start(listener)
    await drained()
    assert len(batches) == 1
    assert len(batches[0]) == 1
    assert batches[0][0]["type"] == "snapshot"
    assert len(batches[0][0]["entries"]) == 101
    await stream.stop()
    await harness.close(context)


def _stream_of(message: dict[str, Any]) -> Step:
    """A stream whose committed partials grow thinking, text, and tool-call arguments."""

    def build(_request: Any, _options: Any) -> AssistantStream:
        async def events() -> AsyncIterator[dict[str, Any]]:
            base = copy.deepcopy(message)
            base["content"] = []
            base["stopReason"] = "pending"
            yield {"type": "start", "partial": base}
            await asyncio.sleep(0.005)
            thinking = message["content"][0]
            text = message["content"][1]
            call = message["content"][2]
            blocks: list[dict[str, Any]] = [
                {"type": "thinking", "thinking": ""},
                {"type": "text", "text": ""},
                {
                    "type": "toolCall",
                    "id": call["id"],
                    "name": call["name"],
                    "arguments": {},
                },
            ]
            partial = copy.deepcopy(base)
            partial["content"] = copy.deepcopy(blocks)
            yield {"type": "thinking_delta", "partial": copy.deepcopy(partial)}
            await asyncio.sleep(0.005)
            for end in range(10, len(thinking["thinking"]) + 10, 10):
                partial["content"][0]["thinking"] = thinking["thinking"][:end]
                yield {"type": "thinking_delta", "partial": copy.deepcopy(partial)}
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.005)
            for end in range(10, len(text["text"]) + 10, 10):
                partial["content"][1]["text"] = text["text"][:end]
                yield {"type": "text_delta", "partial": copy.deepcopy(partial)}
                await asyncio.sleep(0.005)
            path = call["arguments"]["path"]
            for end in range(10, len(path) + 10, 10):
                partial["content"][2]["arguments"] = {
                    "path": path[:end],
                    "note": "",
                }
                yield {
                    "type": "toolcall_delta",
                    "partial": copy.deepcopy(partial),
                }
                await asyncio.sleep(0.005)
            note = call["arguments"]["note"]
            for end in range(10, len(note) + 10, 10):
                partial["content"][2]["arguments"] = {
                    "path": path,
                    "note": note[:end],
                }
                yield {
                    "type": "toolcall_delta",
                    "partial": copy.deepcopy(partial),
                }
                await asyncio.sleep(0.005)

        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        future.set_result(message)
        return AssistantStream(events(), future)

    return Step(build)


async def test_rebuilds_thinking_text_and_toolcall_arguments_from_message_changes() -> None:
    setup = chat_setup()
    message = assistant_message(
        [
            {"type": "thinking", "thinking": "thinking about it " * 10},
            {"type": "text", "text": "some text " * 10},
            {
                "type": "toolCall",
                "id": "c1",
                "name": "missing",
                "arguments": {
                    "path": "a/long/path/" * 10,
                    "note": "x" * 60,
                },
            },
        ],
        stop_reason="toolUse",
    )
    setup.settings["progress"] = {"partial_interval_ms": 0}
    setup.faux.set_responses([_stream_of(message), assistant_message([faux_text("done")])])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    partials = partials_of(harness)
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await drained()
    rebuilt: list[Any] = []
    current: Any = None
    # The streamed tool-calling message, up to its end; the short final answer commits no partial.
    for event in events():
        if event["type"] == "message_end" and event.get("entry", {}).get("kind") == "pi.assistant":
            break
        if event["type"] == "message_start" and event["message"].get("role") == "assistant":
            current = event["message"]
        elif event["type"] == "message_update":
            current = apply_changes(current, event["changes"])
        else:
            continue
        rebuilt.append(current)
    assert len(partials) > 2
    assert [partial["content"] for partial in rebuilt] == [
        partial["content"] for partial in partials
    ]
    kinds = {
        change["type"]
        for event in events()
        if event["type"] == "message_update"
        for change in event["changes"]
    }
    assert "thinking_delta" in kinds or "text_delta" in kinds
    await stream.stop()
    await harness.close(context)


async def test_rebuilds_a_sliding_tail_window_from_output_trims_and_appends() -> None:
    setup = chat_setup()
    gate = deferred()

    async def execute(_args: Any, api: Any, _context: Any) -> dict[str, Any]:
        for line in range(6):
            api.output(f"line {line}\n")
            await asyncio.sleep(0.02)
        await gate.promise
        return {}

    add_tool(
        setup.registry,
        tool("tail", execute, output_limits={"max_lines": 3, "retain": "tail"}),
    )
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "tail", "arguments": {}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    outputs: list[str] = []

    def observer(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            value = change["value"]
            slots = (value or {}).get("tools") if isinstance(value, dict) else None
            output = slots[0].get("output") if slots else None
            if (
                change["record"]["kind"] == "pi.live"
                and output is not None
                and output != (outputs[-1] if outputs else None)
            ):
                outputs.append(output)

    harness.subscribe_commits(observer)
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await eventually(
        lambda: bool(outputs) and outputs[-1].endswith("line 5\n"), timeout=5
    )
    gate.resolve()
    await submission.wait(context)
    await drained()
    rebuilt: list[str] = []
    text = ""
    for event in events():
        if event["type"] != "tool_execution_update" or event.get("output") is None:
            continue
        value = event["output"]
        if "set" in value:
            text = value["set"]
        else:
            text = text[value.get("trimStart", 0):] + value.get("append", "")
        rebuilt.append(text)
    assert rebuilt == outputs
    assert any(
        event["type"] == "tool_execution_update"
        and "trimStart" in (event.get("output") or {})
        for event in events()
    )
    await stream.stop()
    await harness.close(context)


async def test_emits_one_exact_batch_when_a_run_ends_and_a_queued_followup_starts_the_next() -> None:
    setup = chat_setup()
    release = deferred()

    async def held(_request: Any, _options: Any) -> Any:
        await release.promise
        return assistant_message([faux_text("first")])

    setup.faux.set_responses([held, assistant_message([faux_text("second")])])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    input_submission = await root.submit({"type": "input", "content": "a"}, context)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    listener = await listen(harness, root)
    stream, batches = listener["stream"], listener["batches"]
    release.resolve()
    await follow_up.wait(context)
    await drained()
    boundary = next(
        batch
        for batch in batches
        if any(event["type"] == "run_end" for event in batch)
    )
    assert [event["type"] for event in boundary] == [
        "message_start",
        "message_end",
        "message_start",
        "message_end",
        "turn_end",
        "run_end",
        "submission",
        "submission",
        "inbox_update",
        "usage_changed",
        "run_start",
        "turn_start",
    ]
    assert [
        event["record"]["id"]
        for event in boundary
        if event["type"] == "submission"
    ] == [input_submission.id, follow_up.id]
    await stream.stop()
    await harness.close(context)


async def test_ends_a_call_that_never_runs_and_a_tool_aborted_with_its_generation() -> None:
    setup = chat_setup()

    async def wait(_args: Any, _api: Any, call_context: Any) -> Any:
        await aborted(call_context.abort_signal)

    add_tool(setup.registry, tool("wait", wait))
    setup.faux.set_responses(
        [
            assistant_message(
                [
                    {"type": "toolCall", "id": "c1", "name": "ghost", "arguments": {}},
                    {"type": "toolCall", "id": "c2", "name": "wait", "arguments": {}},
                ],
                stop_reason="toolUse",
            )
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await eventually(
        lambda: any(event["type"] == "tool_execution_start" for event in events()),
        timeout=5,
    )
    # Aborting the generation aborts its round: the tool ends with its aborted result first.
    live_value = await live(harness, root)
    await harness.abort_task(live_value["run"]["taskId"], context)
    await submission.wait(context)
    await drained()
    round_events = [
        f"end:{event['entry']['kind']}"
        if event["type"] == "message_end"
        else f"start:{event['message']['role']}"
        if event["type"] == "message_start"
        else event["type"]
        for event in events()
    ]
    ghost_end = round_events.index("tool_execution_end")
    assert round_events[ghost_end - 1:ghost_end + 3] == [
        "end:pi.assistant",
        "tool_execution_end",
        "start:toolResult",
        "end:pi.tool-result",
    ]
    tool_events = [
        event for event in events() if event["type"].startswith("tool_execution")
    ]
    assert [
        (
            event["type"],
            event.get("toolCallId"),
            "entry" in event,
        )
        for event in tool_events
    ] == [
        ("tool_execution_end", "c1", True),
        ("tool_execution_start", "c2", False),
        ("tool_execution_end", "c2", True),
    ]
    await stream.stop()
    await harness.close(context)


async def test_reports_a_steer_reset_and_nothing_for_other_conversations() -> None:
    setup = chat_setup()
    gate = deferred()

    async def hold(_args: Any, _api: Any, _context: Any) -> dict[str, Any]:
        await gate.promise
        return {}

    add_tool(setup.registry, tool("hold", hold))
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "hold", "arguments": {}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    other = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    listener = await listen(harness, root)
    stream, events, batches = (
        listener["stream"],
        listener["events"],
        listener["batches"],
    )
    input_submission = await root.submit({"type": "input", "content": "a"}, context)
    await eventually(
        lambda: any(event["type"] == "tool_execution_start" for event in events()),
        timeout=5,
    )
    await root.submit(
        {"type": "input", "content": "s", "whenBusy": "steer"}, context
    )
    await drained()
    before = len(batches)
    await other.commit(lambda tx: tx.append_entry(other.id, {"kind": "note"}), context)
    await drained()
    assert len(batches) == before
    gate.resolve()
    await input_submission.wait(context)
    await root.reset(None, context)
    await drained()
    assert len([event for event in events() if event["type"] == "run_start"]) == 1
    assert len([event for event in events() if event["type"] == "run_end"]) == 1
    assert [event["type"] for event in batches[-1]] == [
        "entry_appended",
        "submission",
    ]
    await stream.stop()
    await harness.close(context)


async def test_rejects_an_attachment_cancelled_while_it_waits_for_the_session_line() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    release = deferred()

    async def blocked(_tx: Any) -> None:
        await release.promise

    blocking = asyncio.ensure_future(root.commit(blocked, context))
    await asyncio.sleep(0)
    from task_support import with_signal

    from pi_durable.context import AbortController

    controller = AbortController()
    attaching = asyncio.ensure_future(
        watch_events(harness, root.id, with_signal(controller.signal))
    )
    controller.abort(RuntimeError("cancelled"))
    release.resolve()
    await blocking
    with pytest.raises(RuntimeError, match="cancelled"):
        await attaching
    await harness.close(context)


async def test_applies_deltas_after_an_overflow_snapshot_that_holds_an_inflight_partial() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    partial = assistant_message([faux_text("hel")])
    stream = await watch_events(harness, root.id, context)

    async def start_partial(tx: Any) -> None:
        doc = await tx.doc(LiveDoc, root.id)
        doc["generation"] = {"attempt": 1, "message": copy_json(partial)}

    await root.commit(start_partial, context)
    for _index in range(101):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    batches: list[list[AgentEvent]] = []

    def listener(events: list[AgentEvent], _context: Any) -> None:
        batches.append(list(events))

    stream.start(listener)
    await drained()
    snapshot = batches[0][0]
    assert snapshot["type"] == "snapshot"

    async def extend(tx: Any) -> None:
        doc = await tx.doc(LiveDoc, root.id)
        doc["generation"]["message"]["content"][0]["text"] += "lo"

    await root.commit(extend, context)
    await drained()
    update = batches[-1][0]
    assert update["type"] == "message_update"
    assert update["changes"] == [
        {"type": "text_delta", "contentIndex": 0, "delta": "lo"}
    ]
    rebuilt = apply_changes(snapshot["generation"]["message"], update["changes"])
    assert rebuilt["content"] == [{"type": "text", "text": "hello"}]
    await stream.stop()
    await harness.close(context)


async def test_reports_usage_only_cleared_progress_and_retired_documents() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, batches = listener["stream"], listener["batches"]
    partial = cast(dict[str, Any], copy_json(assistant_message([faux_text("partial")])))

    async def change(edit: Any) -> None:
        async def apply(tx: Any) -> None:
            live_doc = await tx.doc(LiveDoc, root.id)
            edit(live_doc)

        await root.commit(apply, context)

    def set_message(live_value: Any) -> None:
        live_value["generation"] = {"attempt": 1, "message": copy.deepcopy(partial)}

    await change(set_message)

    def set_usage(live_value: Any) -> None:
        live_value["generation"]["message"]["usage"]["input"] = 42

    await change(set_usage)

    def set_deferred(live_value: Any) -> None:
        live_value["generation"] = {"attempt": 1, "deferred": {"pollAt": 1}}

    await change(set_deferred)

    def move_poll(live_value: Any) -> None:
        live_value["generation"]["deferred"]["pollAt"] = 2

    await change(move_poll)

    def start_tool(live_value: Any) -> None:
        live_value["tools"] = [
            {
                "callId": "c1",
                "name": "t",
                "status": "running",
                "details": {"n": 1},
                "diagnostics": [],
            }
        ]

    await change(start_tool)

    def clear_progress(live_value: Any) -> None:
        del live_value["tools"][0]["details"]
        del live_value["tools"][0]["diagnostics"]

    await change(clear_progress)

    def finish_tool(live_value: Any) -> None:
        live_value["tools"][0]["status"] = "done"

    await change(finish_tool)
    await root.commit(lambda tx: tx.retire_doc(UsageDoc, root.id), context)
    await root.commit(lambda tx: tx.retire_doc(AgentDoc, root.id), context)
    await drained()
    assert [
        [event for event in batch if event["type"] != "task_failed"]
        for batch in batches
    ] == [
        [{"type": "message_start", "message": partial}],
        [
            {
                "type": "message_update",
                "usage": {**partial["usage"], "input": 42},
                "changes": [],
            }
        ],
        [{"type": "deferred_poll", "pollAt": 1}],
        [{"type": "deferred_poll", "pollAt": 2}],
        [
            {
                "type": "tool_execution_start",
                "toolCallId": "c1",
                "toolName": "t",
                "args": {},
            }
        ],
        [
            {
                "type": "tool_execution_update",
                "toolCallId": "c1",
                "toolName": "t",
                "details": None,
                "diagnostics": [],
            }
        ],
        [
            {
                "type": "tool_execution_end",
                "toolCallId": "c1",
                "toolName": "t",
            }
        ],
        # Retired documents read as their initial values.
        [{"type": "usage_changed", "usage": {"models": {}, "tools": {}}}],
        [{"type": "agent_changed", "agent": {}}],
    ]
    await stream.stop()
    await harness.close(context)


async def test_ends_the_stream_with_the_harness() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    stream: AgentEventStream = await watch_events(harness, root.id, context)
    await harness.close(context)
    assert await stream.closed == {"reason": "session_closed"}


async def test_starts_a_message_at_the_first_committed_partial_and_ends_it_on_abort() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [slow_message(assistant_message([faux_text("x" * 400)]), chunks=20, interval=0.01)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    listener = await listen(harness, root)
    stream, events = listener["stream"], listener["events"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await eventually(
        lambda: any(
            event["type"] == "message_start"
            and event["message"].get("role") == "assistant"
            for event in events()
        ),
        timeout=5,
    )
    live_value = await live(harness, root)
    await harness.abort_task(live_value["run"]["taskId"], context)
    await submission.wait(context)
    await drained()
    assistant_ends = [
        event
        for event in events()
        if event["type"] == "message_end" and event.get("entry", {}).get("kind") == "pi.assistant"
    ]
    assert len(assistant_ends) == 1
    assert len([
        event
        for event in events()
        if event["type"] == "message_start" and event["message"].get("role") == "assistant"
    ]) == 1
    await stream.stop()
    await harness.close(context)

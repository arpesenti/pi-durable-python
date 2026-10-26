"""Translated upstream ``harness-compaction`` cases: range selection, summaries, and starts."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from chat_support import (
    ChatSetup,
    all_entries,
    assistant_message,
    chat_setup,
    faux_tool_call,
    live_of,
    open_chat,
    text_of,
    usage,
)
from harness_support import add_section
from session_support import ControlledStorage, context
from task_support import Deferred, aborted, completed, deferred, eventually
from tool_support import add_hooks, add_task, add_tool

from pi_durable import (
    AgentEvent,
    CompactionTask,
    LiveDoc,
    MemoryStorage,
    ProviderDoc,
    UsageDoc,
    define_task,
    define_tool,
    watch_events,
)
from pi_durable.harness.compaction import select_cut, serialize_conversation
from pi_durable.harness.context import order_tool_results
from pi_durable.harness.types import ContextView
from pi_durable.harness.util import cancellation_error
from pi_durable.providers import Model

# ─── Helpers ──────────────────────────────────────────────────────────────


def text(label: str, tokens: int) -> str:
    """Text of about ``tokens`` estimated tokens, starting with ``label``."""

    return f"{label} {'x' * max(0, tokens * 4 - len(label) - 1)}"


_next_id = 1


def entry(kind: str, model: list[dict[str, Any]], head: int | None = None) -> dict[str, Any]:
    """A committed entry record with its model messages."""

    global _next_id
    record: dict[str, Any] = {"id": _next_id, "conversationId": 1, "kind": kind, "model": model}
    _next_id += 1
    if head is not None:
        record["head"] = head
    return record


def user(content: str) -> dict[str, Any]:
    return {"role": "user", "content": content, "timestamp": 0}


def assistant(
    content: str,
    calls: list[str] | None = None,
    stop_reason: str = "stop",
) -> dict[str, Any]:
    blocks = [{"type": "text", "text": content}]
    blocks.extend(faux_tool_call("read", {}, id=call_id) for call_id in calls or ())
    return assistant_message(blocks, stop_reason=stop_reason)


def tool_result(call_id: str, content: str) -> dict[str, Any]:
    return {
        "role": "toolResult",
        "toolCallId": call_id,
        "toolName": "read",
        "content": [{"type": "text", "text": content}],
        "isError": False,
        "timestamp": 0,
    }


def view(entries: list[dict[str, Any]], head: dict[str, Any] | None = None) -> ContextView:
    """A view over ``entries`` whose contributions are their models, excluded assistants removed."""

    all_entries = entries if head is None else [head, *entries]
    contributions = [
        [
            message
            for message in record.get("model") or []
            if message["role"] != "assistant"
            or message.get("stopReason") not in ("error", "aborted", "deferred")
        ]
        for record in all_entries
    ]
    return cast(
        ContextView,
        {
            "head": head,
            "entries": all_entries,
            "contributions": contributions,
            "messages": order_tool_results([message for messages in contributions for message in messages]),
        },
    )


# ─── Range selection ──────────────────────────────────────────────────────


def test_keeps_about_keep_recent_tokens_and_cuts_at_the_first_candidate_at_or_after_the_budget() -> None:
    # Spec section 8.7 example: the walk from 6 reaches 20k at 3; the first candidate after 3 is 4.
    entries = [
        entry("pi.user", [user(text("1", 10))]),
        entry("pi.assistant", [assistant("2", ["c1"])]),
        entry("pi.tool-result", [tool_result("c1", text("3", 3000))]),
        entry("pi.assistant", [assistant(text("4", 10))]),
        entry("pi.user", [user(text("5", 10))]),
        entry("pi.assistant", [assistant(text("6", 10))]),
    ]
    assert select_cut(view(entries), 2000) == 3


def test_cuts_at_a_user_entry() -> None:
    entries = [
        entry("pi.user", [user(text("u1", 100))]),
        entry("pi.assistant", [assistant(text("a1", 100))]),
        entry("pi.user", [user(text("u2", 100))]),
        entry("pi.assistant", [assistant(text("a2", 100))]),
    ]
    assert select_cut(view(entries), 150) == 2


def test_cuts_at_an_assistant_in_the_middle_of_one_long_run_and_never_at_a_tool_result() -> None:
    entries = [entry("pi.user", [user("do it")])]
    for index in range(5):
        entries.append(entry("pi.assistant", [assistant(f"step {index}", [f"c{index}"])]))
        entries.append(entry("pi.tool-result", [tool_result(f"c{index}", text(f"r{index}", 100))]))
    cut = select_cut(view(entries), 150)
    assert cut is not None
    # The budget is reached at the fourth result; the cut is the last call, whose result it keeps.
    assert entries[cut]["kind"] == "pi.assistant"
    assert cut == 9


def test_never_cuts_at_a_display_only_entry_where_the_budget_is_reached() -> None:
    entries = [
        entry("pi.user", [user(text("u1", 100))]),
        entry("pi.assistant", [assistant(text("a1", 100))]),
        entry("chat.message", []),
        entry("pi.assistant", [assistant(text("a2", 100))]),
    ]
    # The budget is reached at a1; the display-only entry after it is skipped for the assistant a2.
    assert select_cut(view(entries), 150) == 1
    assert select_cut(view(entries), 50) == 3


def test_keeps_a_huge_last_tool_result_together_with_its_assistant() -> None:
    entries = [
        entry("pi.user", [user("u")]),
        entry("pi.assistant", [assistant("a", ["c"])]),
        entry("pi.tool-result", [tool_result("c", text("big", 5000))]),
    ]
    assert select_cut(view(entries), 100) == 1


def test_never_cuts_at_a_system_entry_or_an_excluded_error_or_aborted_answer() -> None:
    entries = [
        entry("pi.user", [user(text("u1", 100))]),
        entry("pi.assistant", [assistant(text("a1", 100))]),
        entry(
            "pi.system",
            [{"role": "system", "content": "", "sections": {"s": text("s", 100)}, "timestamp": 0}],
        ),
        entry("pi.assistant", [assistant(text("err", 100), [], "error")]),
        entry("pi.assistant", [assistant(text("stopped", 100), [], "aborted")]),
        entry("pi.assistant", [assistant(text("a2", 100))]),
    ]
    # The walk reaches 150 at the system entry; the excluded answers after it contribute nothing.
    assert select_cut(view(entries), 150) == 5


def test_follows_edited_contributions_so_an_omitted_entry_adds_nothing_and_is_no_candidate() -> None:
    entries = [
        entry("pi.user", [user(text("u1", 100))]),
        entry("pi.assistant", [assistant(text("a1", 100))]),
        entry("pi.user", [user(text("u2", 100))]),
        entry("pi.assistant", [assistant(text("a2", 100))]),
    ]
    plain = view(entries)
    assert select_cut(plain, 150) == 2
    contributions = [[] if index == 2 else messages for index, messages in enumerate(plain["contributions"])]
    omitted = cast(
        ContextView,
        {
            **plain,
            "contributions": contributions,
            "messages": order_tool_results([message for messages in contributions for message in messages]),
        },
    )
    assert select_cut(omitted, 150) == 1


def test_does_not_cut_at_a_user_entry_that_a_result_of_the_preceding_call_still_follows() -> None:
    entries = [
        entry("pi.user", [user(text("u1", 100))]),
        entry("pi.assistant", [assistant("a", ["c"])]),
        entry("pi.user", [user(text("steer", 100))]),
        entry("pi.tool-result", [tool_result("c", text("r", 100))]),
        entry("pi.assistant", [assistant(text("a2", 100))]),
    ]
    # The budget is reached at the steer; its result follows it, so the cut moves to the next assistant.
    assert select_cut(view(entries), 250) == 4


def test_finds_nothing_when_the_budget_is_never_reached_or_only_the_marker_precedes_the_cut() -> None:
    small = [entry("pi.user", [user("hi")]), entry("pi.assistant", [assistant("hello")])]
    assert select_cut(view(small), 150) is None
    marker = entry("pi.compaction", [user("summary")], 0)
    # The budget is reached at the only entry after the marker, so the marker alone would be summarized.
    assert select_cut(view([entry("pi.user", [user(text("u", 200))])], marker), 150) is None


def test_summarizes_an_earlier_summary_marker_first() -> None:
    marker = entry("pi.compaction", [user("EARLIER")], 0)
    kept = [
        entry("pi.user", [user(text("u1", 100))]),
        entry("pi.assistant", [assistant(text("a1", 100))]),
        entry("pi.user", [user(text("u2", 100))]),
        entry("pi.assistant", [assistant(text("a2", 100))]),
    ]
    selected = view(kept, marker)
    assert select_cut(selected, 150) == 3
    prefix = [message for messages in selected["contributions"][:3] for message in messages]
    assert serialize_conversation(prefix).startswith("[User]: EARLIER")


# ─── Serialization ────────────────────────────────────────────────────────


def test_writes_a_transcript_truncates_tool_results_and_omits_system_messages_and_thinking() -> None:
    call = faux_tool_call("read", {"path": "a.ts"}, id="c")
    messages = [
        {"role": "system", "content": "", "sections": {"s": "hidden"}, "timestamp": 0},
        user("hello"),
        assistant_message(
            [{"type": "thinking", "thinking": "hmm"}, {"type": "text", "text": "sure"}, call]
        ),
        tool_result("c", "y" * 2500),
    ]
    serialized = serialize_conversation(messages)
    assert "hidden" not in serialized
    assert "[User]: hello" in serialized
    assert "hmm" not in serialized
    assert "[Assistant thinking]" not in serialized
    assert "[Assistant]: sure" in serialized
    assert '[Assistant tool calls]: read(path="a.ts")' in serialized
    assert f"[Tool result]: {'y' * 2000}\n\n[... 500 more characters truncated]" in serialized


# ─── Scripted faux model ──────────────────────────────────────────────────


@dataclass
class Script:
    """Queues of agent and summarization responses plus the requests each saw."""

    agent: list[Any] = field(default_factory=list)
    summaries: list[Any] = field(default_factory=list)
    agent_requests: list[dict[str, Any]] = field(default_factory=list)
    summary_requests: list[dict[str, Any]] = field(default_factory=list)


def is_summary_request(messages: Sequence[Mapping[str, Any]]) -> bool:
    first = messages[0] if messages else None
    return (
        first is not None
        and first.get("role") == "system"
        and isinstance(first.get("content"), str)
        and first["content"].startswith("You are a context summarization assistant")
    )


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return "\n".join(
        block["text"]
        if block.get("type") == "text"
        else f"[image:{block.get('mimeType')}:{len(block.get('data') or '')}]"
        for block in content or ()
    )


def _assistant_content_to_text(content: Sequence[Mapping[str, Any]]) -> str:
    parts = []
    for block in content:
        kind = block.get("type")
        if kind == "text":
            parts.append(cast(str, block["text"]))
        elif kind == "thinking":
            parts.append(cast(str, block["thinking"]))
        else:
            parts.append(f"{block['name']}:{json.dumps(block['arguments'], separators=(',', ':'))}")
    return "\n".join(parts)


def _message_to_text(message: Mapping[str, Any]) -> str:
    role = message.get("role")
    if role == "system":
        sections = cast(Mapping[str, str | None], message.get("sections") or {})
        parts = [cast(str, message.get("content") or "")]
        parts.extend(value for value in sections.values() if value is not None)
        parts.extend(
            f"tool-:{json.dumps(tool, separators=(',', ':'))}"
            for tool in message.get("toolsRemoved") or ()
        )
        parts.extend(
            f"tool+:{json.dumps(tool, separators=(',', ':'))}"
            for tool in message.get("toolsAdded") or ()
        )
        return "\n".join(part for part in parts if part)
    if role == "toolResult":
        return "\n".join(
            [cast(str, message.get("toolName") or ""), _content_to_text(message.get("content"))]
        )
    return _content_to_text(message.get("content"))


def _with_usage_estimate(
    message: dict[str, Any],
    messages: Sequence[Mapping[str, Any]],
    model: Model | None,
) -> dict[str, Any]:
    """pi-ai's faux usage: prompt characters over four, plus the answer's own characters over four."""

    prompt = "\n\n".join(f"{message['role']}:{_message_to_text(message)}" for message in messages)
    spent = usage(
        input=math.ceil(len(prompt) / 4),
        output=math.ceil(len(_assistant_content_to_text(message.get("content") or ())) / 4),
    )
    value = dict(message)
    value["usage"] = spent
    if model is not None:
        value["provider"] = model.provider
        value["model"] = model.model_id
    return value


def script(setup: ChatSetup) -> Script:
    result = Script()

    async def responder(request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        messages = request["messages"]
        summary = is_summary_request(messages)
        model = setup.faux.current_model
        (result.summary_requests if summary else result.agent_requests).append(
            {
                "messages": messages,
                "options": _options,
                "model": None if model is None else model.model_id,
            }
        )
        queue = result.summaries if summary else result.agent
        if not queue:
            raise RuntimeError(f"No scripted {'summary' if summary else 'agent'} response")
        step = queue.pop(0)
        message = step(request, _options) if callable(step) else step
        if hasattr(message, "__await__"):
            message = await message
        return _with_usage_estimate(dict(message), messages, setup.faux.current_model)

    setup.faux.set_responses([responder for _ in range(500)])
    return result


def gated(gate: Deferred, message: dict[str, Any], reached: Deferred | None = None) -> Any:
    """A step that waits for ``gate``, or rejects when the request is cancelled."""

    async def respond(request: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
        del request
        if reached is not None:
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


def answer(content: str) -> dict[str, Any]:
    return assistant_message(content)


def failure(error_message: str) -> dict[str, Any]:
    return assistant_message((), stop_reason="error", error_message=error_message)


OVERFLOW = "prompt is too long: 250000 tokens > 200000 maximum"

#: Small thresholds: no automatic compaction unless a test enables it.
MANUAL = {"enabled": False, "reserve_tokens": 1000, "keep_recent_tokens": 150, "background_tokens": 0}
#: Background threshold at 500 and blocking threshold at 1500 tokens of a 2000-token window.
BACKGROUND = {"enabled": True, "reserve_tokens": 500, "keep_recent_tokens": 150, "background_tokens": 1000}
#: Blocking threshold at 700 tokens of a 1000-token window, no background compaction.
BLOCKING = {"enabled": True, "reserve_tokens": 300, "keep_recent_tokens": 150, "background_tokens": 0}


@dataclass
class Chat:
    harness: Any
    root: Any
    setup: ChatSetup
    faux: Script


async def open_compaction(
    policy: Mapping[str, Any] | None = None,
    context_window: int = 100_000,
    storage: Any = None,
    setup: ChatSetup | None = None,
    faux: Script | None = None,
) -> Chat:
    selected = setup or chat_setup(
        {"models": [{"id": "faux-1", "contextWindow": context_window, "maxTokens": 900}]}
    )
    scripted = faux or script(selected)
    add_section(selected.registry, "preamble", lambda *_args: "You are helpful.", {"tag": False})
    opened = await open_chat(storage or MemoryStorage(), selected)
    # These tests exercise compaction accounting and thresholds, not the faux provider's cache simulation.
    selected.settings["stream"] = {"cache_retention": "none"}
    selected.settings["compaction"] = dict(policy or MANUAL)
    selected.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 1}
    opened["harness"].resume()
    return Chat(opened["harness"], opened["root"], selected, scripted)


async def turn(chat: Chat, user_text: str, reply: str) -> None:
    chat.faux.agent.append(answer(reply))
    submission = await chat.root.submit({"type": "input", "content": user_text}, context)
    assert (await submission.wait(context))["status"] == "done"


async def history(chat: Chat) -> None:
    await turn(chat, text("u1", 100), text("a1", 100))
    await turn(chat, text("u2", 100), text("a2", 100))
    await turn(chat, text("u3", 100), text("a3", 100))


def summary(content: str = "SUMMARY") -> dict[str, Any]:
    return answer(content)


async def result(chat: Chat, id: Any) -> dict[str, Any]:
    record = await chat.harness.wait_for_task(id, context)
    return cast(dict[str, Any], record["state"]["outcome"])


async def kinds(conversation: Any) -> list[str]:
    return [entry["kind"] for entry in await all_entries(conversation)]


async def live(chat: Chat) -> dict[str, Any]:
    return live_of(await chat.harness.snapshot(LiveDoc, chat.root.id, context))


def user_text(message: Mapping[str, Any] | None) -> str:
    return text_of(message) or ""


def first_user_text(messages: Sequence[Mapping[str, Any]]) -> str:
    return user_text(next((message for message in messages if message["role"] == "user"), None))


async def compaction_tasks(chat: Chat) -> list[dict[str, Any]]:
    inspection = await chat.harness.inspect(context)
    return [
        task["record"]
        for task in inspection["tasks"]
        if task["record"]["kind"] == "pi.compaction"
    ]


# ─── Manual compaction ────────────────────────────────────────────────────


async def test_places_the_summary_at_once_when_idle_and_keeps_raw_history() -> None:
    chat = await open_compaction()
    await history(chat)
    before = await all_entries(chat.root)
    usage_before = (await chat.harness.snapshot(UsageDoc, chat.root.id, context))["models"]["faux/faux-1"]
    chat.faux.summaries.append(summary())

    id = await chat.root.compact("focus on files", context)
    outcome = await result(chat, id)
    assert outcome["status"] == "completed"
    submission_id = outcome.get("result", {}).get("submissionId")
    placed = await (await chat.harness.submission(submission_id, context)).wait(context)
    assert placed["status"] == "done"

    # Raw history is unchanged; one summary entry heads the first kept entry, u3.
    after = await all_entries(chat.root)
    assert after[: len(before)] == before
    marker = after[-1]
    u3 = next(record for record in before if user_text(record["model"][0]).startswith("u3"))
    assert marker["kind"] == "pi.compaction"
    assert marker["head"] == u3["id"]
    assert marker["data"] == {"reason": "manual"}
    assert placed.get("entry") == marker["id"]
    assert user_text(marker["model"][0]) == (
        "The conversation history before this point was compacted into the following summary:"
        "\n\n<summary>\nSUMMARY\n</summary>"
    )

    # The model context is the summary followed by the kept entries.
    view = await chat.root.context(context)
    assert [user_text(message) for message in view["messages"]] == [
        user_text(marker["model"][0]),
        text("u3", 100),
        text("a3", 100),
    ]

    # The summarizer saw the serialized prefix, the prompt, and the instructions, without tools or caching.
    request = chat.faux.summary_requests[0]
    assert len(request["messages"]) == 2
    prompt = user_text(request["messages"][1])
    assert prompt.startswith("<conversation>\n[User]: u1 ")
    assert "[Assistant]: a2 " in prompt
    assert "u3 " not in prompt
    assert "## Goal" in prompt
    assert prompt.endswith("\n\nAdditional focus: focus on files")
    provider = await chat.harness.snapshot(ProviderDoc, chat.root.id, context)
    assert request["options"]["cacheRetention"] == "none"
    assert request["options"]["maxTokens"] == 800
    assert request["options"]["sessionId"] == provider["sessionId"]
    assert "deferred" not in request["options"]

    # The summarizer's spend is in the ledger, and nothing counts it again later.
    spent = (await chat.harness.snapshot(UsageDoc, chat.root.id, context))["models"]["faux/faux-1"]
    assert spent["input"] > usage_before["input"]
    await turn(chat, "next", "done")
    agent = chat.faux.agent_requests[-1]["messages"]
    # The next request: one complete system baseline leading the summary, the kept turn, and the new input.
    assert [message["role"] for message in agent] == ["system", "user", "user", "assistant", "user"]
    assert agent[0]["sections"] == {"preamble": "You are helpful."}
    await chat.harness.close(context)


async def test_creates_provider_state_before_a_legacy_conversations_summarization_request() -> None:
    chat = await open_compaction()
    await history(chat)
    await chat.root.commit(lambda tx: tx.retire_doc(ProviderDoc, chat.root.id), context)
    assert await chat.harness.snapshot(ProviderDoc, chat.root.id, context) is None
    chat.faux.summaries.append(summary())
    assert (await result(chat, await chat.root.compact(None, context)))["status"] == "completed"
    stored = await chat.harness.snapshot(ProviderDoc, chat.root.id, context)
    assert re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", stored["sessionId"]
    )
    assert chat.faux.summary_requests[0]["options"]["sessionId"] == stored["sessionId"]
    await chat.harness.close(context)


async def test_keeps_working_while_busy_and_places_the_summary_at_the_next_final_boundary() -> None:
    chat = await open_compaction()
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.agent.append(gated(gate, answer("late answer"), reached))
    input = await chat.root.submit({"type": "input", "content": "busy"}, context)
    await reached.promise
    chat.faux.summaries.append(summary())
    outcome = await result(chat, await chat.root.compact(None, context))
    submission_id = outcome["result"]["submissionId"]
    submission = await chat.harness.submission(submission_id, context)
    assert (await submission.status(context))["status"] == "queued"
    gate.resolve()
    assert (await input.wait(context))["status"] == "done"
    assert (await submission.wait(context))["status"] == "done"
    # The summary follows the answer; the kept range still includes the busy turn.
    assert (await kinds(chat.root))[-3:] == ["pi.user", "pi.assistant", "pi.compaction"]
    await chat.harness.close(context)


async def test_places_a_queued_summary_at_post_tools_and_the_run_continues_in_the_compacted_context() -> None:
    chat = await open_compaction()
    wait_tool = define_tool(
        {
            "name": "wait",
            "description": "wait",
            "parameters": {"type": "object", "properties": {}},
            "execute": lambda *_args: {"output": [{"type": "text", "text": "waited"}]},
        }
    )
    add_tool(chat.setup.registry, wait_tool)
    await chat.root.configure({"tools": ["wait"]}, context)
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.agent.append(
        gated(
            gate,
            assistant_message([faux_tool_call("wait", {}, id="call")], stop_reason="toolUse"),
            reached,
        )
    )
    chat.faux.agent.append(answer("after tools"))
    input = await chat.root.submit({"type": "input", "content": "use a tool"}, context)
    await reached.promise
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    gate.resolve()
    assert (await input.wait(context))["status"] == "done"
    # The continuation request starts with the new baseline, then the summary, ahead of the kept tool call.
    continuation = chat.faux.agent_requests[-1]["messages"]
    assert continuation[0]["role"] == "system"
    assert "<summary>\nSUMMARY\n</summary>" in user_text(continuation[1])
    assert (await kinds(chat.root))[-4:] == [
        "pi.tool-result",
        "pi.compaction",
        "pi.system",
        "pi.assistant",
    ]
    await chat.harness.close(context)


async def test_runs_follow_ups_left_by_a_failed_run_after_placing_the_summary() -> None:
    chat = await open_compaction()
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.agent.append(gated(gate, failure("bad request"), reached))
    failed = await chat.root.submit({"type": "input", "content": "fails"}, context)
    await reached.promise
    follow_up = await chat.root.submit({"type": "input", "content": "follow-up"}, context)
    gate.resolve()
    assert (await failed.wait(context))["status"] == "unanswered"
    assert (await follow_up.status(context))["status"] == "queued"

    chat.faux.summaries.append(summary())
    chat.faux.agent.append(answer("followed"))
    await result(chat, await chat.root.compact(None, context))
    assert (await follow_up.wait(context))["status"] == "done"
    request = chat.faux.agent_requests[-1]["messages"]
    assert "SUMMARY" in first_user_text(request)
    assert any(user_text(message) == "follow-up" for message in request)
    await chat.harness.close(context)


async def test_settles_stale_when_a_reset_lands_while_it_summarizes() -> None:
    chat = await open_compaction()
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    await chat.root.reset(None, context)
    gate.resolve()
    outcome = await result(chat, id)
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    status = await submission.status(context)
    assert status["status"] == "unanswered" and status["reason"] == "stale"
    assert (await kinds(chat.root))[-1] == "pi.reset"
    await chat.harness.close(context)


async def test_does_not_make_the_conversation_busy_during_summarization() -> None:
    chat = await open_compaction()
    await history(chat)
    summary_gate, summary_reached = deferred(), deferred()
    chat.faux.summaries.append(gated(summary_gate, summary(), summary_reached))
    id = await chat.root.compact(None, context)
    await summary_reached.promise
    answer_gate, answer_reached = deferred(), deferred()
    chat.faux.agent.append(gated(answer_gate, answer("a4"), answer_reached))
    input = await chat.root.submit({"type": "input", "content": "u4"}, context)
    # Placed and answered immediately with the uncompacted context, not queued behind the compaction.
    assert (await input.status(context))["status"] == "placed"
    await answer_reached.promise
    assert first_user_text(chat.faux.agent_requests[-1]["messages"]) == text("u1", 100)
    # The summary is ready while the run is busy, so it queues and lands after the answer.
    summary_gate.resolve()
    outcome = await result(chat, id)
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    assert (await submission.status(context))["status"] == "queued"
    answer_gate.resolve()
    assert (await input.wait(context))["status"] == "done"
    assert (await submission.wait(context))["status"] == "done"
    assert (await kinds(chat.root))[-3:] == ["pi.user", "pi.assistant", "pi.compaction"]
    await chat.harness.close(context)


async def test_counts_the_spend_of_a_summary_that_ends_stale_and_writes_no_entry_for_it() -> None:
    chat = await open_compaction()
    await history(chat)

    async def input_tokens() -> int:
        state = await chat.harness.snapshot(UsageDoc, chat.root.id, context)
        return state["models"]["faux/faux-1"]["input"]

    before = await input_tokens()
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    await chat.root.reset(None, context)
    gate.resolve()
    outcome = await result(chat, id)
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    status = await submission.status(context)
    assert status["status"] == "unanswered" and status["reason"] == "stale"
    assert await input_tokens() > before
    assert "pi.compaction" not in await kinds(chat.root)
    await chat.harness.close(context)


async def test_lets_the_compaction_that_cuts_furthest_win_whatever_finishes_first() -> None:
    chat = await open_compaction()
    await history(chat)
    first, first_reached = deferred(), deferred()
    chat.faux.summaries.append(gated(first, summary("FIRST"), first_reached))
    early = await chat.root.compact(None, context)
    await first_reached.promise
    await turn(chat, text("u4", 100), text("a4", 100))
    chat.faux.summaries.append(summary("SECOND"))
    later = await result(chat, await chat.root.compact(None, context))
    assert later["status"] == "completed"
    first.resolve()
    outcome = await result(chat, early)
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    status = await submission.status(context)
    # The early compaction cut at u3, before the later cut at u4.
    assert status["status"] == "unanswered" and status["reason"] == "stale"
    assert "SECOND" in first_user_text((await chat.root.context(context))["messages"])
    await chat.harness.close(context)


async def test_places_an_older_selected_summary_that_cuts_later_than_the_newer_one() -> None:
    chat = await open_compaction()
    await history(chat)
    # A: small budget, late cut; selected first, finishes last.
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary("A"), reached))
    a = await chat.root.compact(None, context)
    await reached.promise
    # B: larger budget, earlier cut; placed first.
    chat.setup.settings["compaction"] = {**MANUAL, "keep_recent_tokens": 350}
    chat.faux.summaries.append(summary("B"))
    await result(chat, await chat.root.compact(None, context))
    assert "B" in first_user_text((await chat.root.context(context))["messages"])
    gate.resolve()
    outcome = await result(chat, a)
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    assert (await submission.status(context))["status"] == "done"
    messages = (await chat.root.context(context))["messages"]
    assert "<summary>\nA\n</summary>" in first_user_text(messages)
    assert [user_text(message) for message in messages[1:]] == [text("u3", 100), text("a3", 100)]
    await chat.harness.close(context)


@pytest.mark.parametrize(
    ("keeps", "stale"),
    [([150, 350], True), ([150, 150], False), ([350, 150], False)],
)
async def test_places_two_queued_summaries_in_one_boundary_when_the_second_cuts_differently(
    keeps: list[int], stale: bool
) -> None:
    chat = await open_compaction()
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.agent.append(gated(gate, answer("done"), reached))
    input = await chat.root.submit({"type": "input", "content": "busy"}, context)
    await reached.promise
    submissions = []
    for index, keep in enumerate(keeps):
        chat.setup.settings["compaction"] = {**MANUAL, "keep_recent_tokens": keep}
        chat.faux.summaries.append(summary(f"S{index}"))
        outcome = await result(chat, await chat.root.compact(None, context))
        submissions.append(outcome["result"]["submissionId"])
    gate.resolve()
    await input.wait(context)
    statuses = []
    for id in submissions:
        submission = await chat.harness.submission(id, context)
        statuses.append((await submission.wait(context))["status"])
    assert statuses == ["done", "unanswered" if stale else "done"]
    await chat.harness.close(context)


async def test_is_aborted_by_conversation_abort_and_an_already_queued_summary_survives_it() -> None:
    chat = await open_compaction()
    await history(chat)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    assert len((await live(chat))["compactions"]) == 1
    await chat.root.abort(context)
    assert (await result(chat, id))["status"] == "aborted"
    assert (await live(chat)).get("compactions") is None
    assert "pi.compaction" not in await kinds(chat.root)

    # Queued while busy, then Esc: the queued write stays and lands with the next run's boundary.
    gate, busy = deferred(), deferred()
    chat.faux.agent.append(gated(gate, answer("never"), busy))
    await chat.root.submit({"type": "input", "content": "busy"}, context)
    await busy.promise
    chat.faux.summaries.append(summary())
    queued = await result(chat, await chat.root.compact(None, context))
    await chat.root.abort(context)
    submission = await chat.harness.submission(queued["result"]["submissionId"], context)
    assert (await submission.status(context))["status"] == "queued"
    assert await submission.abort(context) == "aborted"
    await turn(chat, "next", "ok")
    status = await submission.status(context)
    assert status["status"] == "unanswered" and status["reason"] == "aborted"
    assert "pi.compaction" not in await kinds(chat.root)
    await chat.harness.close(context)


async def test_is_ordinary_work_so_idle_waits_include_it() -> None:
    chat = await open_compaction()
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    idle = False

    async def wait() -> None:
        nonlocal idle
        await chat.root.wait_for_idle(context)
        idle = True

    waiting = asyncio.ensure_future(wait())
    await asyncio.sleep(0.02)
    assert idle is False
    gate.resolve()
    await waiting
    assert (await chat.harness.get_task(id, context))["state"]["status"] == "terminal"
    await chat.harness.close(context)


async def test_enables_scheduling_right_after_open() -> None:
    setup = chat_setup()
    faux = script(setup)
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    setup.settings["compaction"] = {**MANUAL, "keep_recent_tokens": 10}
    id = await root.compact(None, context)
    # get_task() only reads, so progress here comes from compact() itself.
    await eventually(lambda: _terminal(harness, id))
    record = await harness.get_task(id, context)
    assert record["state"] == {"status": "terminal", "outcome": {"status": "completed", "result": {}}}
    assert faux.summary_requests == []
    await harness.close(context)


async def _terminal(harness: Any, id: Any) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == "terminal"


# ─── Compaction outcomes ──────────────────────────────────────────────────


async def test_completes_without_a_summary_when_there_is_nothing_to_compact() -> None:
    chat = await open_compaction()
    await turn(chat, "hi", "hello")
    outcome = await result(chat, await chat.root.compact(None, context))
    assert outcome == {"status": "completed", "result": {}}
    assert chat.faux.summary_requests == []
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


async def test_asks_before_compact_and_the_first_decision_wins() -> None:
    chat = await open_compaction()
    seen: list[Any] = []

    def broken(*_args: Any) -> Any:
        raise RuntimeError("hook broke")

    def decide(compaction: Any, *_args: Any) -> Any:
        seen.append(compaction)
        return {"summary": "FROM HOOK"}

    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": broken})
    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": decide})
    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": lambda *_args: {"decline": True}})
    await history(chat)
    outcome = await result(chat, await chat.root.compact("why", context))
    assert outcome["status"] == "completed"
    assert chat.faux.summary_requests == []
    messages = (await chat.root.context(context))["messages"]
    assert "<summary>\nFROM HOOK\n</summary>" in first_user_text(messages)
    assert [str(report) for report in chat.setup.reports] == ["hook broke"]
    compaction = seen[0]
    assert compaction["reason"] == "manual" and compaction["instructions"] == "why"
    assert [record["kind"] for record in compaction["entries"]] == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "pi.user",
        "pi.assistant",
    ]
    assert [user_text(message) for message in compaction["messages"] if message["role"] != "system"] == [
        text("u1", 100),
        text("a1", 100),
        text("u2", 100),
        text("a2", 100),
    ]
    await chat.harness.close(context)


async def test_completes_without_a_summary_when_a_hook_declines() -> None:
    chat = await open_compaction()
    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": lambda *_args: {"decline": True}})
    await history(chat)
    assert await result(chat, await chat.root.compact(None, context)) == {
        "status": "completed",
        "result": {},
    }
    assert chat.faux.summary_requests == []
    await chat.harness.close(context)


async def test_fails_with_no_model_without_a_configured_model() -> None:
    chat = await open_compaction()
    await history(chat)
    await chat.root.configure({"model": None}, context)
    outcome = await result(chat, await chat.root.compact(None, context))
    assert outcome["status"] == "failed"
    assert outcome["error"]["detail"] == {"reason": "no_model"}
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


async def _compaction_input(chat: Chat, run: Any) -> int:
    """Input tokens a compaction added to the ledger."""

    async def read() -> int:
        state = await chat.harness.snapshot(UsageDoc, chat.root.id, context)
        return cast(int, state["models"]["faux/faux-1"]["input"])

    before = await read()
    await run()
    return await read() - before


async def test_retries_a_retryable_error_with_the_pinned_request_and_counts_every_attempt_once() -> None:
    single = await open_compaction()
    await history(single)
    await single.root.configure({"thinkingLevel": "high"}, context)
    single.faux.summaries.append(summary())
    async def run_once() -> None:
        await result(single, await single.root.compact(None, context))

    once = await _compaction_input(single, run_once)
    assert once > 0
    await single.harness.close(context)

    chat = await open_compaction()
    await history(chat)
    await chat.root.configure({"thinkingLevel": "high"}, context)
    chat.setup.settings["stream"] = {"timeout_ms": 1234, "deferred": True}

    async def retry_once(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        # Changed during the attempt: the retry still uses the pinned request.
        await chat.root.configure({"thinkingLevel": "low"}, context)
        chat.setup.settings["stream"] = {"timeout_ms": 1}
        return failure("overloaded")

    chat.faux.summaries.append(retry_once)
    chat.faux.summaries.append(summary())
    settled: dict[str, Any] = {}

    async def run() -> None:
        settled.update(await result(chat, await chat.root.compact(None, context)))

    twice = await _compaction_input(chat, run)
    assert settled["status"] == "completed"
    assert len(chat.faux.summary_requests) == 2
    assert twice == 2 * once
    provider = await chat.harness.snapshot(ProviderDoc, chat.root.id, context)
    for request in chat.faux.summary_requests:
        assert request["options"]["reasoning"] == "high"
        assert request["options"]["timeoutMs"] == 1234
        assert request["options"]["cacheRetention"] == "none"
        assert request["options"]["sessionId"] == provider["sessionId"]
        assert "deferred" not in request["options"]
    await chat.harness.close(context)


@pytest.mark.parametrize("decision", [{"decline": True}, {"summary": "HOOK"}])
async def test_adds_no_usage_when_a_hook_declines_or_supplies_the_summary(
    decision: dict[str, Any]
) -> None:
    chat = await open_compaction()
    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": lambda *_args: decision})
    await history(chat)

    async def run() -> None:
        await result(chat, await chat.root.compact(None, context))

    assert await _compaction_input(chat, run) == 0
    await chat.harness.close(context)


async def test_caps_max_tokens_at_the_models_output_limit_and_sends_no_tools() -> None:
    chat = await open_compaction()
    read_tool = define_tool(
        {
            "name": "read",
            "description": "read",
            "parameters": {"type": "object", "properties": {}},
            "execute": lambda *_args: {"output": []},
        }
    )
    add_tool(chat.setup.registry, read_tool)
    await chat.root.configure({"tools": ["read"]}, context)
    await history(chat)
    chat.setup.settings["compaction"] = {**MANUAL, "reserve_tokens": 2000}
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    request = chat.faux.summary_requests[0]
    # 0.8 * 2000 = 1600, above the model's 900.
    assert request["options"]["maxTokens"] == 900
    assert len(request["messages"]) == 2
    assert not any(
        message["role"] == "system" and message.get("toolsAdded") is not None
        for message in request["messages"]
    )
    await chat.harness.close(context)


@pytest.mark.parametrize(
    ("name", "response", "message", "requests"),
    [
        ("retries run out", failure("overloaded"), "Summarization failed: overloaded", 3),
        ("a non-retryable error", failure("bad request"), "Summarization failed: bad request", 1),
        (
            "a length stop",
            assistant_message("partial", stop_reason="length"),
            "Summarization hit the token limit; the summary is incomplete",
            1,
        ),
        (
            "a tool call",
            assistant_message([faux_tool_call("read", {}, id="c")], stop_reason="stop"),
            "Summarization attempted to call a tool",
            1,
        ),
        ("empty text", answer("  "), "Summarization produced no text", 1),
    ],
)
async def test_fails_with_model_error_on_a_bad_summary_response(
    name: str, response: dict[str, Any], message: str, requests: int
) -> None:
    chat = await open_compaction()
    await history(chat)
    for _ in range(3):
        chat.faux.summaries.append(response)
    outcome = await result(chat, await chat.root.compact(None, context))
    assert outcome["status"] == "failed"
    assert outcome["error"]["message"] == message
    assert outcome["error"]["detail"] == {"reason": "model_error"}
    assert len(chat.faux.summary_requests) == requests
    assert (await live(chat)).get("compactions") is None
    assert "pi.compaction" not in await kinds(chat.root)
    await chat.harness.close(context)


# ─── Automatic compaction ─────────────────────────────────────────────────


async def test_background_compaction_starts_above_the_threshold_without_blocking_the_run() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    await turn(chat, text("u4", 100), text("a4", 100))
    await reached.promise
    [task] = await compaction_tasks(chat)
    assert task["background"] is True
    assert task["input"] == {"reason": "threshold"}
    assert task.get("owner") is None
    assert (await live(chat))["compactions"] == [
        {"taskId": task["id"], "reason": "threshold", "blocking": False, "attempt": 1}
    ]
    # Background work: neither conversation idle nor Esc waits for or stops it.
    await chat.root.wait_for_idle(context)
    await chat.root.abort(context)
    assert (await chat.harness.get_task(task["id"], context))["state"]["status"] == "running"
    gate.resolve()
    assert (await result(chat, task["id"]))["status"] == "completed"
    assert (await kinds(chat.root))[-1] == "pi.compaction"
    assert "SUMMARY" in first_user_text((await chat.root.context(context))["messages"])
    await chat.harness.close(context)


@pytest.mark.parametrize(
    "policy",
    [
        {**BACKGROUND, "enabled": False},
        {**BACKGROUND, "background_tokens": 0},
        {**BACKGROUND, "keep_recent_tokens": 100_000},
    ],
)
async def test_background_compaction_does_not_start_when_the_policy_excludes_it(
    policy: dict[str, Any]
) -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = policy
    await turn(chat, text("u4", 100), text("a4", 100))
    assert await compaction_tasks(chat) == []
    assert chat.faux.summary_requests == []
    await chat.harness.close(context)


async def test_background_compaction_does_not_start_while_another_compaction_is_listed() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    manual = await chat.root.compact(None, context)
    await reached.promise
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    await turn(chat, text("u4", 100), text("a4", 100))
    assert [task["id"] for task in await compaction_tasks(chat)] == [manual]
    await chat.harness.abort_task(manual, context)
    await chat.harness.close(context)


async def test_records_a_compaction_failure_in_pi_live_until_a_summary_is_produced() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    events: list[AgentEvent] = []
    stream = await watch_events(chat.harness, chat.root.id, context)
    stream.start(lambda batch, _context: events.extend(batch))
    chat.faux.summaries.append(failure("bad request"))
    await turn(chat, text("u4", 100), text("a4", 100))
    await eventually(lambda: _has_failure(chat))
    recorded = (await live(chat))["compactionFailure"]
    assert recorded["reason"] == "threshold"
    assert recorded["error"] == "Summarization failed: bad request"
    await eventually(
        lambda: any(event["type"] == "compaction_end" for event in events)
    )
    ended = next(event for event in events if event["type"] == "compaction_end")
    assert ended["taskId"] == recorded["taskId"]
    assert ended["reason"] == "threshold"
    assert ended["error"] == "Summarization failed: bad request"
    # A compaction that produces a summary removes the failure.
    chat.setup.settings["compaction"] = dict(MANUAL)
    chat.faux.summaries.append(summary())
    manual = await chat.root.compact(None, context)
    assert (await result(chat, manual))["status"] == "completed"
    assert (await live(chat)).get("compactionFailure") is None
    await chat.harness.close(context)


async def _has_failure(chat: Chat) -> bool:
    return (await live(chat)).get("compactionFailure") is not None


@pytest.mark.parametrize("stop", ["task", "conversation"])
async def test_background_compaction_stops_through_abort_task_or_a_background_conversation_abort(
    stop: str,
) -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    await turn(chat, text("u4", 100), text("a4", 100))
    await reached.promise
    [task] = await compaction_tasks(chat)
    if stop == "task":
        await chat.harness.abort_task(task["id"], context)
    else:
        await chat.root.abort(context, {"background": True})
    assert (await result(chat, task["id"]))["status"] == "aborted"
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


# ─── Blocking threshold compaction ────────────────────────────────────────


async def test_blocking_compaction_appends_the_summary_before_the_request() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    # A new section: preparation has a system entry to append, but must not append it before the wait.
    add_section(chat.setup.registry, "extra", lambda *_args: "EXTRA")
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    chat.faux.agent.append(answer("a4"))
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    await reached.promise
    [child] = await compaction_tasks(chat)
    generation = (await live(chat))["run"]["taskId"]
    assert child["owner"] == generation
    assert child["background"] is False
    assert child["input"] == {"reason": "threshold"}
    record = await chat.harness.get_task(generation, context)
    assert record["state"]["status"] == "waiting"
    assert record["state"]["on"] == [child["id"]]
    assert record["state"]["checkpoint"]["phase"] == "prepare"
    assert record["state"]["checkpoint"]["attempt"] == 1
    assert record["state"]["checkpoint"]["compacted"] == child["id"]
    assert record["state"]["checkpoint"]["threshold"] is True
    assert (await live(chat))["compactions"] == [
        {"taskId": child["id"], "reason": "threshold", "blocking": True, "attempt": 1}
    ]
    # Nothing was appended before the wait.
    assert (await kinds(chat.root))[-1] == "pi.user"
    gate.resolve()
    assert (await input.wait(context))["status"] == "done"
    assert (await result(chat, child["id"]))["status"] == "completed"
    assert (await kinds(chat.root))[-3:] == ["pi.compaction", "pi.system", "pi.assistant"]
    request = chat.faux.agent_requests[-1]["messages"]
    assert "SUMMARY" in first_user_text(request)
    systems = [message for message in request if message["role"] == "system"]
    assert len(systems) == 1
    assert systems[0]["sections"] == {
        "preamble": "You are helpful.",
        "extra": "<extra>\nEXTRA\n</extra>",
    }
    await chat.harness.close(context)


async def test_sends_the_request_once_when_the_kept_part_stays_above_the_threshold() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = {**BLOCKING, "keep_recent_tokens": 700}
    chat.faux.summaries.append(summary())
    await turn(chat, text("u4", 400), "a4")
    assert len(chat.faux.summary_requests) == 1
    assert (await kinds(chat.root)).count("pi.compaction") == 1
    await chat.harness.close(context)


async def test_sends_the_request_anyway_when_its_blocking_compaction_declines() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": lambda *_args: {"decline": True}})
    await turn(chat, text("u4", 200), "a4")
    assert "pi.compaction" not in await kinds(chat.root)
    assert first_user_text(chat.faux.agent_requests[-1]["messages"]) == text("u1", 100)
    assert (await live(chat)).get("compactionFailure") is None
    await chat.harness.close(context)


async def test_fails_the_run_with_its_blocking_compactions_error_without_a_request() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    chat.faux.summaries.append(failure("bad request"))
    requests = len(chat.faux.agent_requests)
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == "Compaction failed: Summarization failed: bad request"
    assert len(chat.faux.agent_requests) == requests
    assert (await kinds(chat.root))[-1] == "pi.user"
    state = await live(chat)
    assert state.get("compactions") is None
    assert state["compactionFailure"]["reason"] == "threshold"
    assert state["compactionFailure"]["error"] == "Summarization failed: bad request"
    await chat.harness.close(context)


async def test_fails_the_run_when_its_blocking_compaction_is_aborted_directly() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    requests = len(chat.faux.agent_requests)
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    await reached.promise
    [child] = await compaction_tasks(chat)
    await chat.harness.abort_task(child["id"], context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == "Compaction was aborted"
    assert len(chat.faux.agent_requests) == requests
    assert "pi.compaction" not in await kinds(chat.root)
    # Aborting is not a failure.
    assert (await live(chat)).get("compactionFailure") is None
    await chat.harness.close(context)


async def test_blocking_compaction_is_aborted_with_its_generation_by_esc() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    events: list[AgentEvent] = []
    stream = await watch_events(chat.harness, chat.root.id, context)
    stream.start(lambda batch, _context: events.extend(batch))
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    await reached.promise
    [child] = await compaction_tasks(chat)
    await chat.root.abort(context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "aborted"
    record = await chat.harness.get_task(child["id"], context)
    assert record["state"]["outcome"] == {"status": "aborted"}
    await eventually(lambda: any(event["type"] == "run_end" for event in events))
    types = [event["type"] for event in events]
    assert types.index("compaction_end") < types.index("run_end")
    await stream.stop()
    await chat.harness.close(context)


async def test_blocking_compaction_wins_over_a_background_one_still_in_flight() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary("BACKGROUND"), reached))
    await turn(chat, text("u4", 100), text("a4", 100))
    await reached.promise
    [background] = await compaction_tasks(chat)
    chat.faux.summaries.append(summary("BLOCKING"))
    await turn(chat, text("u5", 1000), "a5")
    assert "BLOCKING" in first_user_text((await chat.root.context(context))["messages"])
    gate.resolve()
    outcome = await result(chat, background["id"])
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    status = await submission.status(context)
    assert status["status"] == "unanswered" and status["reason"] == "stale"
    await chat.harness.close(context)


# ─── Overflow compaction ──────────────────────────────────────────────────

ENABLED = {**MANUAL, "enabled": True}


async def test_compacts_an_overflow_and_retries_with_the_same_attempt() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = dict(ENABLED)
    chat.faux.summaries.append(summary())
    seen: dict[str, Any] = {}

    async def retry(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        state = await live(chat)
        seen["attempt"] = state.get("generation", {}).get("attempt")
        return answer("fits")

    chat.faux.agent.append(failure(OVERFLOW))
    chat.faux.agent.append(retry)
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    assert (await input.wait(context))["status"] == "done"
    assert seen["attempt"] == 1
    assert (await kinds(chat.root))[-5:] == [
        "pi.user",
        "pi.assistant",
        "pi.compaction",
        "pi.system",
        "pi.assistant",
    ]
    [compaction] = [
        record for record in await all_entries(chat.root) if record["kind"] == "pi.compaction"
    ]
    assert compaction["data"] == {"reason": "overflow"}
    retry_request = chat.faux.agent_requests[-1]["messages"]
    assert retry_request[0]["role"] == "system"
    assert "SUMMARY" in user_text(retry_request[1])
    assert not any(
        message["role"] == "assistant" and message.get("stopReason") == "error"
        for message in retry_request
    )
    await chat.harness.close(context)


async def test_fails_a_second_overflow_with_its_error_entry() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = dict(ENABLED)
    chat.faux.summaries.append(summary())
    chat.faux.agent.extend([failure(OVERFLOW), failure(OVERFLOW)])
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == OVERFLOW
    assert (await kinds(chat.root)).count("pi.compaction") == 1
    assert (await kinds(chat.root))[-1] == "pi.assistant"
    await chat.harness.close(context)


async def test_fails_without_compacting_when_compaction_is_disabled() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.faux.agent.append(failure(f"overloaded: {OVERFLOW}"))
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert len(chat.faux.agent_requests) == 4
    assert chat.faux.summary_requests == []
    await chat.harness.close(context)


async def test_fails_an_overflow_after_a_blocking_threshold_compaction_in_the_same_generation() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    chat.faux.summaries.append(summary())
    chat.faux.agent.append(failure(OVERFLOW))
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert len(chat.faux.summary_requests) == 1
    await chat.harness.close(context)


@pytest.mark.parametrize(
    ("name", "prepare", "requests"),
    [
        ("declines", "decline", 0),
        ("fails", "fail", 1),
        ("cannot cut", "cannot-cut", 0),
    ],
)
async def test_fails_with_the_overflow_text_when_compaction_cannot_help(
    name: str, prepare: str, requests: int
) -> None:
    chat = await open_compaction()
    await turn(chat, text("u1", 100), text("a1", 100))
    chat.setup.settings["compaction"] = dict(ENABLED)
    if prepare == "decline":
        add_hooks(
            chat.setup.registry, CompactionTask, {"beforeCompact": lambda *_args: {"decline": True}}
        )
    elif prepare == "fail":
        chat.faux.summaries.append(failure("bad request"))
    else:
        # Classification finds no cut, so no compaction starts and the ordinary failure carries the text.
        chat.setup.settings["compaction"] = {**ENABLED, "keep_recent_tokens": 100_000}
    chat.faux.agent.append(failure(OVERFLOW))
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == OVERFLOW
    assert len(chat.faux.summary_requests) == requests
    await chat.harness.close(context)


# ─── Compaction estimates and interactions ────────────────────────────────


@pytest.mark.parametrize("clock", ["real", "fixed"])
async def test_ignores_usage_measured_before_a_summary_placed_mid_run(clock: str) -> None:
    setup = chat_setup({"models": [{"id": "faux-1", "contextWindow": 2000, "maxTokens": 900}]})
    if clock == "fixed":
        setup.now = lambda: 1_000
    chat = await open_compaction(context_window=2000, setup=setup)
    tool_gate, tool_reached = deferred(), deferred()

    async def slow(*_args: Any) -> dict[str, Any]:
        tool_reached.resolve()
        await tool_gate.promise
        return {"output": [{"type": "text", "text": text("result", 200)}]}

    add_tool(
        chat.setup.registry,
        define_tool(
            {
                "name": "slow",
                "description": "slow",
                "parameters": {"type": "object", "properties": {}},
                "execute": slow,
            }
        ),
    )
    await chat.root.configure({"tools": ["slow"]}, context)
    await turn(chat, text("u1", 220), text("a1", 220))
    await turn(chat, text("u2", 220), text("a2", 220))
    await turn(chat, text("u3", 220), text("a3", 220))
    # Background at 900, blocking at 1500: the tool call's usage plus its result would cross 1500.
    chat.setup.settings["compaction"] = {**BACKGROUND, "background_tokens": 600}
    # The request starts a background compaction; its tool call's usage measures the whole context.
    chat.faux.agent.append(
        assistant_message([faux_tool_call("slow", {}, id="slow-1")], stop_reason="toolUse")
    )
    chat.faux.agent.append(answer("done"))
    summary_gate, summary_reached = deferred(), deferred()
    chat.faux.summaries.append(gated(summary_gate, summary(), summary_reached))
    input = await chat.root.submit({"type": "input", "content": text("u4", 50)}, context)
    await asyncio.gather(tool_reached.promise, summary_reached.promise)
    [background] = await compaction_tasks(chat)
    summary_gate.resolve()
    # Queued: the run is busy in its tool round.
    queued = await result(chat, background["id"])
    submission = await chat.harness.submission(queued["result"]["submissionId"], context)
    assert (await submission.status(context))["status"] == "queued"
    tool_gate.resolve()
    assert (await input.wait(context))["status"] == "done"
    # The summary landed at postTools; the successor saw a small context and did not compact again.
    assert len(chat.faux.summary_requests) == 1
    assert await compaction_tasks(chat) == []
    assert "SUMMARY" in first_user_text(chat.faux.agent_requests[-1]["messages"])
    await chat.harness.close(context)


async def test_rebaselines_the_system_prompt_over_kept_system_deltas() -> None:
    chat = await open_compaction()
    mood = {"value": "cheerful"}
    add_section(chat.setup.registry, "mood", lambda *_args: mood["value"], {"tag": False})
    await turn(chat, text("u1", 100), text("a1", 100))
    await turn(chat, text("u2", 100), text("a2", 100))
    mood["value"] = "terse"
    await turn(chat, text("u3", 100), text("a3", 100))
    chat.setup.settings["compaction"] = {**MANUAL, "keep_recent_tokens": 250}
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    # The kept range holds the delta for the terse mood; the next request has one complete baseline.
    assert any(entry["kind"] == "pi.system" for entry in (await chat.root.context(context))["entries"])
    await turn(chat, "next", "ok")
    systems = [
        message for message in chat.faux.agent_requests[-1]["messages"] if message["role"] == "system"
    ]
    assert len(systems) == 1
    assert systems[0]["sections"] == {"preamble": "You are helpful.", "mood": "terse"}
    await chat.harness.close(context)


async def test_summarizes_a_replaced_entrys_replacement_and_shows_it_to_the_hook() -> None:
    chat = await open_compaction()
    await history(chat)
    [u1, *_rest] = await all_entries(chat.root)
    await chat.root.submit(
        {
            "type": "write",
            "entry": {
                "kind": "app.redact",
                "edits": [
                    {
                        "target": u1["id"],
                        "action": "replace",
                        "messages": [{"role": "user", "content": "REDACTED", "timestamp": 0}],
                    }
                ],
            },
        },
        context,
    )
    seen: dict[str, Any] = {}

    def record(compaction: Any, *_args: Any) -> None:
        seen["messages"] = compaction["messages"]

    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": record})
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    assert first_user_text(seen["messages"]) == "REDACTED"
    assert "[User]: REDACTED" in user_text(chat.faux.summary_requests[0]["messages"][1])
    await chat.harness.close(context)


async def test_compacts_a_fork_whose_cut_falls_on_a_parent_entry() -> None:
    chat = await open_compaction()
    await history(chat)
    entries = await all_entries(chat.root)
    fork = await chat.root.fork(entries[-1]["id"], {"ownership": {"kind": "ownerless"}}, context)
    chat.faux.summaries.append(summary())
    outcome = await result(chat, await fork.compact(None, context))
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    assert (await submission.wait(context))["status"] == "done"
    u3 = next(entry for entry in entries if user_text(entry["model"][0]).startswith("u3"))
    view = await fork.context(context)
    assert view["head"]["kind"] == "pi.compaction"
    assert view["head"]["head"] == u3["id"]
    assert view["head"]["conversationId"] == fork.id
    kept = [user_text(message) for message in view["messages"][1:]]
    assert kept == [text("u3", 100), text("a3", 100)]
    # The parent is untouched.
    assert "pi.compaction" not in await kinds(chat.root)
    # The fork's view keeps the parent entries the summary kept.
    state = await fork.view_state(context)
    assert state.value["entries"] == view["entries"]
    state.dispose()
    await chat.harness.close(context)


async def test_settles_stale_in_a_fork_reset_while_it_summarizes() -> None:
    chat = await open_compaction()
    await history(chat)
    entries = await all_entries(chat.root)
    fork = await chat.root.fork(entries[-1]["id"], {"ownership": {"kind": "ownerless"}}, context)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    id = await fork.compact(None, context)
    await reached.promise
    await fork.reset(None, context)
    gate.resolve()
    outcome = await result(chat, id)
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    status = await submission.status(context)
    assert status["status"] == "unanswered" and status["reason"] == "stale"
    assert (await fork.context(context))["head"]["kind"] == "pi.reset"
    await chat.harness.close(context)


async def test_places_an_older_queued_summary_and_the_current_one_together() -> None:
    chat = await open_compaction()
    await history(chat)
    gate, reached = deferred(), deferred()
    chat.faux.agent.append(gated(gate, failure("bad request"), reached))
    failed = await chat.root.submit({"type": "input", "content": "fails"}, context)
    await reached.promise
    chat.faux.summaries.append(summary("OLDER"))
    older = await result(chat, await chat.root.compact(None, context))
    gate.resolve()
    await failed.wait(context)
    # Idle now, with the older summary still queued; the current one queues behind it and a
    # final boundary runs.
    chat.faux.summaries.append(summary("CURRENT"))
    current = await result(chat, await chat.root.compact(None, context))
    statuses = []
    for outcome in (older, current):
        submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
        statuses.append((await submission.status(context))["status"])
    assert statuses == ["done", "done"]
    assert "CURRENT" in first_user_text((await chat.root.context(context))["messages"])
    await chat.harness.close(context)


async def test_places_a_hooks_summary_and_holds_while_work_the_hook_created_runs() -> None:
    chat = await open_compaction()
    child_gate = deferred()

    async def run(_task: Any, runtime: Any, hook_context: Any) -> None:
        await child_gate.promise
        await runtime.commit(lambda: completed(), hook_context)

    async def abort(_task: Any, runtime: Any, hook_context: Any) -> None:
        await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, hook_context)

    child = define_task(
        {
            "name": "test.child",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )
    add_task(chat.setup.registry, child)

    async def hook(_compaction: Any, api: Any, hook_context: Any) -> dict[str, Any]:
        async def create(tx: Any) -> Any:
            return await tx.create_task(
                child,
                {},
                {"ownership": {"kind": "task", "taskId": api.task_id}, "conversationId": chat.root.id},
            )

        await chat.harness.commit(create, hook_context)
        return {"summary": "HOOK"}

    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": hook})
    await history(chat)
    id = await chat.root.compact(None, context)
    await eventually(lambda: _completing(chat.harness, id))
    # The summary and the status removal landed at the hold.
    assert (await kinds(chat.root))[-1] == "pi.compaction"
    assert (await live(chat)).get("compactions") is None
    child_gate.resolve()
    assert (await result(chat, id))["status"] == "completed"
    await chat.harness.close(context)


async def _completing(harness: Any, id: Any) -> bool:
    record = await harness.get_task(id, context)
    return record is not None and record["state"]["status"] == "completing"


async def test_removes_the_status_of_a_faulted_compaction() -> None:
    chat = await open_compaction()
    await history(chat)
    original = chat.setup.models.get_model

    def broken(_provider: str, _model_id: str) -> Any:
        raise RuntimeError("models broke")

    chat.setup.models.get_model = broken
    outcome = await result(chat, await chat.root.compact(None, context))
    chat.setup.models.get_model = original
    assert outcome["status"] == "faulted"
    assert outcome["error"]["message"] == "models broke"
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


async def test_lists_concurrent_compactions_in_task_id_order() -> None:
    chat = await open_compaction()
    await history(chat)
    first, second = deferred(), deferred()
    chat.faux.summaries.extend([gated(deferred(), summary(), first), gated(deferred(), summary(), second)])
    a = await chat.root.compact(None, context)
    b = await chat.root.compact(None, context)
    await asyncio.gather(first.promise, second.promise)
    assert [status["taskId"] for status in (await live(chat))["compactions"]] == [a, b]
    await chat.root.abort(context)
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


async def _compaction_retrying(chat: Chat) -> bool:
    statuses = (await live(chat)).get("compactions") or []
    return bool(statuses and statuses[0].get("retry") is not None)


async def test_reports_start_and_end_and_the_retry_backoff_in_a_late_joiners_snapshot() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["retry"] = {
        "enabled": True,
        "max_retries": 2,
        "base_delay_ms": 60_000,
    }
    events: list[AgentEvent] = []
    stream = await watch_events(chat.harness, chat.root.id, context)
    stream.start(lambda batch, _context: events.extend(batch))
    chat.faux.summaries.append(failure("overloaded"))
    id = await chat.root.compact(None, context)
    await eventually(lambda: _compaction_retrying(chat))
    late = await watch_events(chat.harness, chat.root.id, context)
    [status] = late.snapshot["compactions"]
    assert status["taskId"] == id
    assert status["reason"] == "manual"
    assert status["blocking"] is False
    assert status["attempt"] == 1
    assert status["retry"]["error"] == "overloaded"
    assert isinstance(status["retry"]["at"], int)
    await late.stop()
    await chat.harness.abort_task(id, context)
    await eventually(
        lambda: any(event["type"] == "compaction_end" for event in events)
    )
    assert [
        event for event in events if event["type"].startswith("compaction_")
    ] == [
        {
            "type": "compaction_start",
            "taskId": id,
            "reason": "manual",
            "blocking": False,
        },
        {"type": "compaction_end", "taskId": id, "reason": "manual"},
    ]
    await stream.stop()
    await chat.harness.close(context)


async def test_shows_a_late_joiner_a_compaction_that_is_summarizing() -> None:
    chat = await open_compaction()
    await history(chat)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    stream = await watch_events(chat.harness, chat.root.id, context)
    assert stream.snapshot["compactions"] == [
        {"taskId": id, "reason": "manual", "blocking": False, "attempt": 1}
    ]
    await stream.stop()
    await chat.harness.abort_task(id, context)
    await chat.harness.close(context)


# ─── Recovery ─────────────────────────────────────────────────────────────


async def _first(storage: Any, now: Any = None) -> Chat:
    setup = chat_setup(
        {"models": [{"id": "faux-1", "contextWindow": 128_000, "maxTokens": 16_384}]}
    )
    if now is not None:
        setup.now = now
    add_section(setup.registry, "preamble", lambda *_args: "You are helpful.", {"tag": False})
    faux = script(setup)
    opened = await open_chat(storage, setup)
    setup.settings["compaction"] = dict(MANUAL)
    setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 1}
    setup.settings["stream"] = {"cache_retention": "none"}
    opened["harness"].resume()
    chat = Chat(opened["harness"], opened["root"], setup, faux)
    await history(chat)
    return chat


async def _reopen(storage: Any, setup: ChatSetup, faux: Script) -> Chat:
    opened = await open_chat(storage.reopen(), setup)
    opened["harness"].resume()
    return Chat(opened["harness"], opened["root"], setup, faux)


async def test_repeats_nothing_after_reopen_once_the_summary_is_placed() -> None:
    storage = ControlledStorage()
    chat = await _first(storage)
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    entries = await all_entries(chat.root)
    spent = await chat.harness.snapshot(UsageDoc, chat.root.id, context)
    await chat.harness.close(context)
    chat = await _reopen(storage, chat.setup, chat.faux)
    await chat.harness.wait_for_idle(context)
    assert len(chat.faux.summary_requests) == 1
    assert await all_entries(chat.root) == entries
    assert await chat.harness.snapshot(UsageDoc, chat.root.id, context) == spent
    inspection = await chat.harness.inspect(context)
    assert [inspection["tasks"], inspection["submissions"]] == [[], []]
    await chat.harness.close(context)


async def test_fails_an_overflow_run_with_its_text_when_its_compaction_fails_after_reopen() -> None:
    storage = ControlledStorage()
    chat = await _first(storage)
    chat.setup.settings["compaction"] = dict(ENABLED)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    chat.faux.agent.append(failure(OVERFLOW))
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    await reached.promise
    generation = (await live(chat))["run"]["taskId"]
    state = (await chat.harness.get_task(generation, context))["state"]
    assert state["status"] == "waiting"
    assert state["checkpoint"]["phase"] == "prepare"
    assert state["checkpoint"]["overflow"] == OVERFLOW
    await chat.harness.close(context)
    chat = await _reopen(storage, chat.setup, chat.faux)
    chat.faux.summaries.append(failure("bad request"))
    settled = await (await chat.harness.submission(input.id, context)).wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == OVERFLOW
    await chat.harness.close(context)


async def test_reruns_select_and_its_hook_after_a_crash_in_select() -> None:
    storage = ControlledStorage()
    chat = await _first(storage)
    calls = {"count": 0}
    reached = deferred()

    async def hook(_compaction: Any, _api: Any, hook_context: Any) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            reached.resolve()
            await aborted(hook_context.abort_signal)
        return None

    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": hook})
    id = await chat.root.compact(None, context)
    await reached.promise
    await chat.harness.close(context)
    chat = await _reopen(storage, chat.setup, chat.faux)
    chat.faux.summaries.append(summary())
    assert (await result(chat, id))["status"] == "completed"
    assert calls["count"] == 2
    await chat.harness.close(context)


async def test_resends_an_interrupted_summarization_once_and_counts_only_the_answered_attempt() -> None:
    storage = ControlledStorage()
    chat = await _first(storage)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    usage_before = (await chat.harness.snapshot(UsageDoc, chat.root.id, context))["models"]["faux/faux-1"]
    record = await chat.harness.get_task(id, context)
    assert record["state"]["checkpoint"]["phase"] == "summarize"
    await chat.harness.close(context)
    chat = await _reopen(storage, chat.setup, chat.faux)
    chat.faux.summaries.append(summary())
    assert (await result(chat, id))["status"] == "completed"
    assert len(chat.faux.summary_requests) == 2
    first_request, resent = chat.faux.summary_requests

    def without_timestamps(messages: Any) -> list[Any]:
        return [{**message, "timestamp": None} for message in messages]

    assert without_timestamps(resent["messages"]) == without_timestamps(first_request["messages"])
    spent = (await chat.harness.snapshot(UsageDoc, chat.root.id, context))["models"]["faux/faux-1"]
    assert spent["input"] > usage_before["input"]
    assert spent["output"] - usage_before["output"] == 2
    await chat.harness.close(context)


async def test_resumes_a_retry_backoff_after_reopen() -> None:
    storage = ControlledStorage()
    now = {"value": 1_000_000}
    chat = await _first(storage, now=lambda: now["value"])
    chat.setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 60_000}
    chat.faux.summaries.append(failure("overloaded"))
    id = await chat.root.compact(None, context)
    await eventually(lambda: _retry_listed(chat))
    record = await chat.harness.get_task(id, context)
    assert record["state"]["checkpoint"]["phase"] == "retry"
    assert record["state"]["checkpoint"]["attempt"] == 1
    # The backoff is a durable sleep until its deadline.
    until = record["state"]["checkpoint"]["until"]
    assert record["state"]["status"] == "sleeping"
    assert record["state"]["until"] == until == now["value"] + 60_000
    await chat.harness.close(context)

    # Earlier versions slept inside a running `retry` phase: reopened before its time, it sleeps again.
    stored = await storage.reopen().task(id, context)
    assert stored is not None
    legacy = dict(stored)
    legacy["state"] = {
        "status": "running",
        "checkpoint": {"phase": "retry", **{
            key: value
            for key, value in record["state"]["checkpoint"].items()
            if key not in ("phase", "until")
        }, "until": until},
    }
    await storage.reopen().commit([{"type": "task", "value": cast(Any, legacy)}], context)
    chat = await _reopen(storage, chat.setup, chat.faux)
    await eventually(
        lambda: _sleeping_record(chat, id)
    )
    assert len(chat.faux.summary_requests) == 1
    await chat.harness.close(context)

    now["value"] += 120_000
    chat = await _reopen(storage, chat.setup, chat.faux)
    chat.faux.summaries.append(summary())
    assert (await result(chat, id))["status"] == "completed"
    # The failed attempt, then the retry once the sleep was due.
    assert len(chat.faux.summary_requests) == 2
    await chat.harness.close(context)


def _sleeping_record(chat: Chat, id: Any) -> Any:
    async def check() -> bool:
        record = await chat.harness.get_task(id, context)
        return record is not None and record["state"]["status"] == "sleeping"

    return check()


async def _retry_listed(chat: Chat) -> bool:
    listed = (await live(chat)).get("compactions") or []
    return bool(listed) and listed[0].get("retry") is not None


async def test_keeps_a_generation_waiting_on_its_blocking_compaction_across_reopen() -> None:
    storage = ControlledStorage()
    chat = await _first(storage)
    setup = chat.setup
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    # The faux model window is 128k; this blocking threshold needs a small context window.
    chat.setup.settings["compaction"] = {**BLOCKING, "reserve_tokens": 128_000 - 700}
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    await reached.promise
    await chat.harness.close(context)
    chat = await _reopen(storage, setup, chat.faux)
    chat.faux.summaries.append(summary())
    chat.faux.agent.append(answer("a4"))
    assert (await (await chat.harness.submission(input.id, context)).wait(context))["status"] == "done"
    assert (await kinds(chat.root))[-3:] == ["pi.compaction", "pi.system", "pi.assistant"]
    await chat.harness.close(context)


async def test_keeps_a_queued_summary_across_reopen_and_places_it_at_the_next_boundary() -> None:
    storage = ControlledStorage()
    chat = await _first(storage)
    reached = deferred()
    chat.faux.agent.append(gated(deferred(), answer("never"), reached))
    input = await chat.root.submit({"type": "input", "content": "busy"}, context)
    await reached.promise
    chat.faux.summaries.append(summary())
    outcome = await result(chat, await chat.root.compact(None, context))
    await chat.harness.close(context)
    chat = await _reopen(storage, chat.setup, chat.faux)
    chat.faux.agent.append(answer("answered"))
    assert (await (await chat.harness.submission(input.id, context)).wait(context))["status"] == "done"
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    assert (await submission.wait(context))["status"] == "done"
    await chat.harness.close(context)


# ─── Compaction and the inbox ─────────────────────────────────────────────


async def _queued_summary(chat: Chat, content: str = "SUMMARY") -> Any:
    chat.faux.summaries.append(summary(content))
    outcome = await result(chat, await chat.root.compact(None, context))
    return await chat.harness.submission(outcome["result"]["submissionId"], context)


async def _busy(chat: Chat, reply: dict[str, Any] | None = None) -> dict[str, Any]:
    gate, reached = deferred(), deferred()
    chat.faux.agent.append(gated(gate, reply or answer("done"), reached))
    input = await chat.root.submit({"type": "input", "content": "busy"}, context)
    await reached.promise
    return {"input": input, "release": gate.resolve}


@pytest.mark.parametrize("order", ["summary first", "reset first"])
async def test_reset_and_summary_queued_together_place_by_order(order: str) -> None:
    chat = await open_compaction()
    await history(chat)
    run = await _busy(chat)
    if order == "summary first":
        submission = await _queued_summary(chat)
        await chat.root.reset(None, context)
    else:
        await chat.root.reset(None, context)
        submission = await _queued_summary(chat)
    run["release"]()
    await run["input"].wait(context)
    settled = await submission.wait(context)
    assert settled["status"] == ("done" if order == "summary first" else "unanswered")
    assert (await kinds(chat.root))[-1] == "pi.reset"
    assert (await chat.root.context(context))["head"]["kind"] == "pi.reset"
    await chat.harness.close(context)


async def test_places_a_summary_left_queued_by_a_failed_run_before_the_next_input() -> None:
    chat = await open_compaction()
    await history(chat)
    run = await _busy(chat, failure("bad request"))
    submission = await _queued_summary(chat)
    run["release"]()
    assert (await run["input"].wait(context))["status"] == "unanswered"
    assert (await submission.status(context))["status"] == "queued"
    await turn(chat, "again", "ok")
    assert (await submission.status(context))["status"] == "done"
    request = chat.faux.agent_requests[-1]["messages"]
    assert "SUMMARY" in first_user_text(request)
    assert "again" in [user_text(message) for message in request]
    await chat.harness.close(context)


async def test_keeps_the_full_retry_budget_after_an_overflow_compaction() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = dict(ENABLED)
    chat.faux.summaries.append(summary())
    chat.faux.agent.extend(
        [
            failure("prompt is too long"),
            failure("overloaded"),
            failure("overloaded"),
            answer("finally"),
        ]
    )
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    assert (await input.wait(context))["status"] == "done"
    await chat.harness.close(context)


async def test_loses_an_application_edit_placed_while_a_compaction_summarizes() -> None:
    chat = await open_compaction()
    await history(chat)
    [u1, *_rest] = await all_entries(chat.root)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary(), reached))
    id = await chat.root.compact(None, context)
    await reached.promise
    await chat.root.submit(
        {
            "type": "write",
            "entry": {
                "kind": "app.redact",
                "edits": [
                    {
                        "target": u1["id"],
                        "action": "replace",
                        "messages": [{"role": "user", "content": "REDACTED", "timestamp": 0}],
                    }
                ],
            },
        },
        context,
    )
    gate.resolve()
    await result(chat, id)
    # The summary was made from the unredacted entry, and the edit's target left the range.
    assert "[User]: u1 " in user_text(chat.faux.summary_requests[0]["messages"][1])
    assert "REDACTED" not in [
        user_text(message) for message in (await chat.root.context(context))["messages"]
    ]
    await chat.harness.close(context)


async def test_keeps_the_kept_entries_mounted_in_the_conversation_view() -> None:
    chat = await open_compaction()
    await history(chat)
    state = await chat.root.view_state(context)
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    await eventually(
        lambda: state.value["entries"]
        and state.value["entries"][0]["kind"] == "pi.compaction"
    )
    assert state.value["entries"] == (await chat.root.context(context))["entries"]
    state.dispose()
    await chat.harness.close(context)


# ─── Compaction edge cases ────────────────────────────────────────────────


async def test_keeps_the_one_compaction_limit_through_a_retry_backoff() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    asked = {"count": 0}

    def decline(*_args: Any) -> dict[str, Any]:
        asked["count"] += 1
        return {"decline": True}

    add_hooks(chat.setup.registry, CompactionTask, {"beforeCompact": decline})
    chat.faux.agent.append(failure("overloaded"))
    await turn(chat, text("u4", 200), "a4")
    # One blocking compaction, declined; the retried preparation, still above the threshold, started no other.
    assert asked["count"] == 1
    assert len(chat.faux.agent_requests) == 5
    await chat.harness.close(context)


async def test_does_not_start_a_background_compaction_when_a_manual_one_was_admitted() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    rendering, release = deferred(), deferred()
    hold = {"value": True}

    async def slow(*_args: Any) -> str:
        if hold["value"]:
            hold["value"] = False
            rendering.resolve()
            await release.promise
        return "slow"

    add_section(chat.setup.registry, "slow", slow)
    reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary(), reached))
    chat.faux.agent.append(answer("a4"))
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    await rendering.promise
    manual = await chat.root.compact(None, context)
    await reached.promise
    release.resolve()
    assert (await input.wait(context))["status"] == "done"
    assert [task["id"] for task in await compaction_tasks(chat)] == [manual]
    # A background compaction would have sent its own summarization request.
    assert len(chat.faux.summary_requests) == 1
    await chat.harness.abort_task(manual, context)
    await chat.harness.close(context)


async def test_starts_no_background_compaction_after_a_blocking_one_in_the_same_generation() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    # Blocking at 1500, background at 200: the kept part stays above the background threshold.
    chat.setup.settings["compaction"] = {
        **BACKGROUND,
        "background_tokens": 1300,
        "keep_recent_tokens": 400,
    }
    chat.faux.summaries.append(summary())
    await turn(chat, text("u4", 1000), "a4")
    assert len(chat.faux.summary_requests) == 1
    assert await compaction_tasks(chat) == []
    await chat.harness.close(context)


async def test_fails_the_run_when_its_blocking_compaction_faults_and_records_the_fault() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    original = chat.setup.models.complete_simple

    def broken(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("no credentials")

    chat.setup.models.complete_simple = broken
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == "Compaction failed: no credentials"
    chat.setup.models.complete_simple = original
    assert "pi.compaction" not in await kinds(chat.root)
    state = await live(chat)
    assert state.get("compactions") is None
    assert state["compactionFailure"]["reason"] == "threshold"
    assert state["compactionFailure"]["error"] == "no credentials"
    await chat.harness.close(context)


@pytest.mark.parametrize("how", ["abort", "overflow"])
async def test_fails_an_overflow_run_with_its_text_when_its_compaction_cannot_finish(
    how: str,
) -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = dict(ENABLED)
    reached = deferred()
    if how == "abort":
        chat.faux.summaries.append(gated(deferred(), summary(), reached))
    else:
        chat.faux.summaries.append(failure("prompt is too long for the summary"))
    chat.faux.agent.append(failure(OVERFLOW))
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    if how == "abort":
        await reached.promise
        [child] = await compaction_tasks(chat)
        await chat.harness.abort_task(child["id"], context)
    settled = await input.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == OVERFLOW
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


async def test_treats_a_length_stop_as_an_ordinary_answer_not_an_overflow() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = dict(ENABLED)
    chat.faux.agent.append(
        assistant_message("cut short", stop_reason="length", error_message=OVERFLOW)
    )
    input = await chat.root.submit({"type": "input", "content": "go"}, context)
    assert (await input.wait(context))["status"] == "done"
    assert chat.faux.summary_requests == []
    await chat.harness.close(context)


async def test_orders_the_events_of_a_summary_placed_at_once() -> None:
    chat = await open_compaction()
    await history(chat)
    events: list[AgentEvent] = []
    stream = await watch_events(chat.harness, chat.root.id, context)
    stream.start(lambda batch, _context: events.extend(batch))
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    await eventually(
        lambda: any(event["type"] == "compaction_end" for event in events)
    )
    end = next(
        index for index, event in enumerate(events) if event["type"] == "compaction_end"
    )
    assert [event["type"] for event in events[end - 2:end + 3]] == [
        "message_start",
        "message_end",
        "compaction_end",
        "submission",
        "usage_changed",
    ]
    await stream.stop()
    await chat.harness.close(context)


async def test_puts_compaction_start_last_in_the_batch_of_the_preparation_commit() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    batches: list[list[AgentEvent]] = []
    stream = await watch_events(chat.harness, chat.root.id, context)
    stream.start(lambda batch, _context: batches.append(list(batch)))
    # A new section makes the preparation commit append a system entry next to the compaction's status.
    add_section(chat.setup.registry, "extra", lambda *_args: "extra")
    chat.faux.summaries.append(gated(deferred(), summary()))
    await turn(chat, text("u4", 100), "a4")
    await eventually(
        lambda: any(
            any(event["type"] == "compaction_start" for event in batch)
            for batch in batches
        )
    )
    batch = next(
        batch
        for batch in batches
        if any(event["type"] == "compaction_start" for event in batch)
    )
    assert [event["type"] for event in batch] == [
        "message_start",
        "message_end",
        "compaction_start",
    ]
    await stream.stop()
    [task] = await compaction_tasks(chat)
    await chat.harness.abort_task(task["id"], context)
    await chat.harness.close(context)


async def test_keeps_a_background_summary_queued_through_a_retry_backoff_while_blocking_wins() -> None:
    chat = await open_compaction(context_window=2000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BACKGROUND)
    chat.setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 300}
    summary_gate, summary_reached = deferred(), deferred()
    chat.faux.summaries.append(gated(summary_gate, summary("BACKGROUND"), summary_reached))
    chat.faux.summaries.append(summary("BLOCKING"))
    failed = deferred()

    async def overloaded(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        failed.resolve()
        return failure("overloaded")

    chat.faux.agent.append(overloaded)
    chat.faux.agent.append(answer("a4"))
    input = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    await asyncio.gather(failed.promise, summary_reached.promise)
    # During the backoff: the background summary queues, and the thresholds drop so the retry blocks.
    await eventually(lambda: _generation_retrying(chat))
    [background] = await compaction_tasks(chat)
    summary_gate.resolve()
    queued = await result(chat, background["id"])
    submission = await chat.harness.submission(queued["result"]["submissionId"], context)
    assert (await submission.status(context))["status"] == "queued"
    chat.setup.settings["compaction"] = {
        **BACKGROUND,
        "reserve_tokens": 1500,
        "keep_recent_tokens": 50,
    }
    assert (await input.wait(context))["status"] == "done"
    assert "BLOCKING" in first_user_text(chat.faux.agent_requests[-1]["messages"])
    status = await submission.wait(context)
    assert status["status"] == "unanswered" and status["reason"] == "stale"
    await chat.harness.close(context)


async def _generation_retrying(chat: Chat) -> bool:
    return (await live(chat)).get("generation", {}).get("retry") is not None


async def test_summarizes_the_previous_summary_in_a_second_compaction() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.faux.summaries.append(summary("FIRST"))
    await result(chat, await chat.root.compact(None, context))
    await turn(chat, text("u4", 100), text("a4", 100))
    await turn(chat, text("u5", 100), text("a5", 100))
    chat.faux.summaries.append(summary("SECOND"))
    await result(chat, await chat.root.compact(None, context))
    prompt = user_text(chat.faux.summary_requests[1]["messages"][1])
    assert prompt.startswith(
        "<conversation>\n[User]: The conversation history before this point was compacted"
    )
    assert "FIRST" in prompt
    assert "SECOND" in first_user_text((await chat.root.context(context))["messages"])
    await chat.harness.close(context)


async def test_leaves_no_usage_submission_or_summary_when_the_classifying_commit_fails() -> None:
    storage = ControlledStorage()
    chat = await open_compaction(storage=storage)
    await history(chat)

    async def spent() -> Any:
        state = await chat.harness.snapshot(UsageDoc, chat.root.id, context)
        return state["models"]["faux/faux-1"]

    before = await spent()
    failure = RuntimeError("disk gone")

    async def break_commit(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        storage.fail_next_commit(failure)
        return summary()

    chat.faux.summaries.append(break_commit)
    id = await chat.root.compact(None, context)
    assert await chat.harness.closed == {"reason": "failed", "error": failure}
    await chat.harness.close(context)
    # Nothing of the failed commit landed: the task is still live, without usage, entry, or submission.
    storage.reopen()
    assert await storage.submission_by_request(chat.root.id, f"compaction:{id}", context) is None
    assert (await storage.task(id, context))["state"]["status"] != "terminal"
    reopened = await open_chat(storage, chat.setup)
    state = await reopened["harness"].snapshot(UsageDoc, reopened["root"].id, context)
    assert state["models"]["faux/faux-1"] == before
    assert "pi.compaction" not in await kinds(reopened["root"])
    await reopened["harness"].close(context)


# ─── Blocking and manual compaction together ──────────────────────────────


async def _blocking_run(chat: Chat) -> dict[str, Any]:
    """A run whose generation waits on a blocking compaction whose summary is held."""

    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    gate, reached = deferred(), deferred()
    chat.faux.summaries.append(gated(gate, summary("BLOCKING"), reached))
    input = await chat.root.submit({"type": "input", "content": text("u4", 200)}, context)
    await reached.promise
    [blocking] = await compaction_tasks(chat)
    return {"input": input, "gate": gate, "blocking": blocking["id"]}


async def test_places_a_manual_summary_selected_before_the_blocking_one_landed() -> None:
    chat = await open_compaction(context_window=1000)
    run = await _blocking_run(chat)
    # Selected from the same context as the blocking compaction, so it cuts at the same entry.
    chat.faux.summaries.append(summary("MANUAL"))
    manual = await result(chat, await chat.root.compact(None, context))
    submission = await chat.harness.submission(manual["result"]["submissionId"], context)
    assert (await submission.status(context))["status"] == "queued"
    chat.faux.agent.append(answer("a4"))
    run["gate"].resolve()
    assert (await run["input"].wait(context))["status"] == "done"
    # The request after the blocking compaction used its summary; the manual one landed at the final boundary.
    assert "BLOCKING" in first_user_text(chat.faux.agent_requests[-1]["messages"])
    assert (await submission.wait(context))["status"] == "done"
    markers = [entry for entry in await all_entries(chat.root) if entry["kind"] == "pi.compaction"]
    assert len(markers) == 2
    assert markers[1]["head"] == markers[0]["head"]
    messages = (await chat.root.context(context))["messages"]
    assert "MANUAL" in first_user_text(messages)
    assert not any("BLOCKING" in user_text(message) for message in messages)
    await chat.harness.close(context)


async def test_finds_nothing_for_a_manual_compaction_selected_after_the_blocking_summary() -> None:
    chat = await open_compaction(context_window=1000)
    run = await _blocking_run(chat)
    answer_gate, answer_reached = deferred(), deferred()
    chat.faux.agent.append(gated(answer_gate, answer("a4"), answer_reached))
    run["gate"].resolve()
    await answer_reached.promise
    assert await result(chat, await chat.root.compact(None, context)) == {
        "status": "completed",
        "result": {},
    }
    assert len(chat.faux.summary_requests) == 1
    answer_gate.resolve()
    assert (await run["input"].wait(context))["status"] == "done"
    await chat.harness.close(context)


async def test_aborts_the_run_and_both_compactions_on_esc_and_appends_nothing() -> None:
    chat = await open_compaction(context_window=1000)
    run = await _blocking_run(chat)
    manual_reached = deferred()
    chat.faux.summaries.append(gated(deferred(), summary("MANUAL"), manual_reached))
    manual = await chat.root.compact(None, context)
    await manual_reached.promise
    assert [status["blocking"] for status in (await live(chat))["compactions"]] == [True, False]
    await chat.root.abort(context)
    settled = await run["input"].wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "aborted"
    assert (await result(chat, run["blocking"]))["status"] == "aborted"
    assert (await result(chat, manual))["status"] == "aborted"
    state = await live(chat)
    assert state.get("compactions") is None
    assert state.get("run") is None
    assert "pi.compaction" not in await kinds(chat.root)
    assert (await chat.harness.inspect(context))["submissions"] == []
    await chat.harness.close(context)


async def test_places_a_summary_that_survived_esc_at_the_next_submission_before_its_input() -> None:
    chat = await open_compaction()
    await history(chat)
    reached = deferred()
    chat.faux.agent.append(gated(deferred(), answer("never"), reached))
    busy = await chat.root.submit({"type": "input", "content": "busy"}, context)
    await reached.promise
    chat.faux.summaries.append(summary())
    outcome = await result(chat, await chat.root.compact(None, context))
    submission = await chat.harness.submission(outcome["result"]["submissionId"], context)
    await chat.root.abort(context)
    settled = await busy.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "aborted"
    assert (await submission.status(context))["status"] == "queued"
    await turn(chat, "u5", "a5")
    assert (await submission.status(context))["status"] == "done"
    request = chat.faux.agent_requests[-1]["messages"]
    assert "SUMMARY" in first_user_text(request)
    assert "u5" in [user_text(message) for message in request]
    tail = [entry["kind"] for entry in (await all_entries(chat.root))[-4:]]
    assert tail == ["pi.compaction", "pi.user", "pi.system", "pi.assistant"]
    await chat.harness.close(context)


# ─── Compaction pinning, silent overflow, and late policy changes ─────────


async def test_keeps_the_pinned_model_through_a_retry_and_advances_the_live_attempt() -> None:
    setup = chat_setup(
        {
            "models": [
                {"id": "faux-1", "contextWindow": 100_000, "maxTokens": 900},
                {"id": "faux-2", "contextWindow": 100_000, "maxTokens": 900},
            ]
        }
    )
    chat = await open_compaction(setup=setup)
    await history(chat)
    seen: dict[str, Any] = {}

    async def switch(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        # Switched during the attempt: the retry still uses the pinned model.
        await chat.root.configure({"model": {"provider": "faux", "modelId": "faux-2"}}, context)
        return failure("overloaded")

    async def next_attempt(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        listed = (await live(chat)).get("compactions") or []
        seen["attempt"] = listed[0]["attempt"] if listed else None
        return summary()

    chat.faux.summaries.append(switch)
    chat.faux.summaries.append(next_attempt)
    assert (await result(chat, await chat.root.compact(None, context)))["status"] == "completed"
    assert [request["model"] for request in chat.faux.summary_requests] == ["faux-1", "faux-1"]
    assert seen["attempt"] == 2
    state = await chat.harness.snapshot(UsageDoc, chat.root.id, context)
    assert list(state["models"]) == ["faux/faux-1"]
    await chat.harness.close(context)


@pytest.mark.parametrize(
    "response",
    [
        answer("fine"),
        assistant_message((), stop_reason="length"),
    ],
)
async def test_treats_silent_overflow_as_an_ordinary_answer(response: dict[str, Any]) -> None:
    chat = await open_compaction(context_window=300)
    await history(chat)
    # Thresholds out of reach, so only overflow classification could compact.
    chat.setup.settings["compaction"] = {
        "enabled": True,
        "reserve_tokens": -100_000,
        "keep_recent_tokens": 150,
        "background_tokens": 0,
    }
    chat.faux.agent.append(response)
    input = await chat.root.submit({"type": "input", "content": "go"}, context)
    assert (await input.wait(context))["status"] == "done"
    [last] = (await all_entries(chat.root))[-1:]
    assert last["model"][0]["usage"]["input"] >= 300
    assert chat.faux.summary_requests == []
    await chat.harness.close(context)


async def test_sends_the_request_when_a_blocking_compaction_finds_nothing_under_a_late_policy() -> None:
    chat = await open_compaction(context_window=1000)
    await history(chat)
    chat.setup.settings["compaction"] = dict(BLOCKING)
    changed = {"value": False}

    async def policy(*_args: Any) -> str:
        # Rendering runs after preparation read the policy; the compaction reads this one.
        if not changed["value"]:
            changed["value"] = True
            chat.setup.settings["compaction"] = {**BLOCKING, "keep_recent_tokens": 100_000}
        return "p"

    add_section(chat.setup.registry, "policy", policy)
    await turn(chat, text("u4", 200), "a4")
    inspection = await chat.harness.inspect(context)
    assert [task for task in inspection["tasks"] if task["record"]["kind"] == "pi.compaction"] == []
    assert chat.faux.summary_requests == []
    assert "pi.compaction" not in await kinds(chat.root)
    await chat.harness.close(context)


# ─── Blocked compaction ───────────────────────────────────────────────────


async def test_survives_reopen_blocked_and_is_orphaned_on_abort_with_its_status_removed() -> None:
    storage = ControlledStorage()
    setup = chat_setup()
    opened = await open_chat(storage, setup)

    async def run(_task: Any, runtime: Any, task_context: Any) -> None:
        await runtime.commit(lambda: completed(), task_context)

    async def abort(_task: Any, runtime: Any, task_context: Any) -> None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, task_context
        )

    # A compaction stored by a newer version than this process registers, for example after a downgrade.
    newer = define_task(
        {
            "name": "pi.compaction",
            "version": 2,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )

    async def create(tx: Any) -> Any:
        id = await tx.create_task(
            newer, {"reason": "manual"}, {"ownership": {"kind": "conversation"}}
        )
        live = await tx.doc(LiveDoc, opened["root"].id)
        live["compactions"] = [{"taskId": id, "reason": "manual", "blocking": False, "attempt": 1}]
        return id

    id = await opened["root"].commit(create, context)
    await opened["harness"].close(context)
    reopened = await open_chat(storage.reopen(), setup)
    reopened["harness"].resume()
    chat = Chat(reopened["harness"], reopened["root"], setup, Script())
    inspection = await chat.harness.inspect(context)
    record = next(task for task in inspection["tasks"] if task["record"]["id"] == id)
    assert record["state"] == {"kind": "blocked", "reason": "task_too_old"}
    await chat.harness.abort_task(id, context)
    settled = await chat.harness.wait_for_task(id, context)
    assert settled["state"]["outcome"] == {"status": "orphaned", "reason": "task_too_old"}
    assert (await live(chat)).get("compactions") is None
    await chat.harness.close(context)


# ─── Context contributions ────────────────────────────────────────────────


async def test_applies_edits_carried_by_an_older_head_marker_in_the_range() -> None:
    chat = await open_compaction()

    async def write(tx: Any) -> dict[str, Any]:
        a = await tx.append_entry(chat.root.id, {"kind": "app.note", "model": [user("a")]})
        b = await tx.append_entry(chat.root.id, {"kind": "app.note", "model": [user("b")]})
        # An older marker that omits b, then a newer one whose range still contains the older marker.
        await tx.append_entry(
            chat.root.id,
            {
                "kind": "app.head",
                "head": a["id"],
                "edits": [{"target": b["id"], "action": "omit"}],
            },
        )
        c = await tx.append_entry(chat.root.id, {"kind": "app.note", "model": [user("c")]})
        await tx.append_entry(
            chat.root.id, {"kind": "app.head", "head": a["id"], "model": [user("H")]}
        )
        return {"a": a["id"], "b": b["id"], "c": c["id"]}

    ids = await chat.root.commit(write, context)
    view = await chat.root.context(context)
    assert [record["id"] for record in view["entries"]][1:] == [ids["a"], ids["b"], ids["c"]]
    assert [[user_text(message) for message in messages] for messages in view["contributions"]] == [
        ["H"],
        ["a"],
        [],
        ["c"],
    ]
    assert [user_text(message) for message in view["messages"]] == ["H", "a", "c"]
    # The summarizer sees the same contributions: the omitted entry stays out.
    chat.setup.settings["compaction"] = {**MANUAL, "keep_recent_tokens": 1}
    chat.faux.summaries.append(summary())
    await result(chat, await chat.root.compact(None, context))
    prompt = user_text(chat.faux.summary_requests[0]["messages"][1])
    assert "<conversation>\n[User]: H\n\n[User]: a\n</conversation>" in prompt
    await chat.harness.close(context)


# ─── Per-model compaction overrides (#10942) ──────────────────────────────


async def test_applies_the_current_models_override_to_the_blocking_threshold() -> None:
    for key in ("faux/faux-1", "faux/other"):
        chat = await open_compaction(context_window=1_000)
        await history(chat)
        # Harness-wide: blocking only above the full window; the override for faux-1 blocks above 700.
        chat.setup.settings["compaction"] = {
            **BLOCKING,
            "reserve_tokens": 0,
            "model_overrides": {key: {"reserve_tokens": 300}},
        }
        chat.faux.summaries.append(summary())
        await turn(chat, text("u4", 200), "a4")
        assert len(chat.faux.summary_requests) == (1 if key == "faux/faux-1" else 0)
        await chat.harness.close(context)


async def test_applies_the_request_models_override_on_a_context_overflow() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = {
        **MANUAL,
        "model_overrides": {"faux/faux-1": {"enabled": True}},
    }
    chat.faux.summaries.append(summary())
    chat.faux.agent.append(failure(OVERFLOW))
    chat.faux.agent.append(answer("fits"))
    submission = await chat.root.submit({"type": "input", "content": text("u4", 100)}, context)
    assert (await submission.wait(context))["status"] == "done"
    assert len(chat.faux.summary_requests) == 1
    await chat.harness.close(context)


async def test_applies_the_models_override_when_a_compaction_selects_and_sizes_its_summary() -> None:
    chat = await open_compaction()
    await history(chat)
    chat.setup.settings["compaction"] = {
        **MANUAL,
        "keep_recent_tokens": 100_000,
        "model_overrides": {"faux/faux-1": {"keep_recent_tokens": 150, "reserve_tokens": 500}},
    }
    chat.faux.summaries.append(summary())
    assert (await result(chat, await chat.root.compact(None, context)))["status"] == "completed"
    # 0.8 * 500 from the override, below the model's 900.
    assert chat.faux.summary_requests[0]["options"]["maxTokens"] == 400
    messages = (await chat.root.context(context))["messages"]
    assert [user_text(message) for message in messages[1:]] == [
        text("u3", 100),
        text("a3", 100),
    ]
    await chat.harness.close(context)

"""Built-in ``pi.compaction`` task: select an old prefix, summarize it, and place a summary.

Port of v1.3.0 ``src/harness/compaction.ts``. Compaction replaces an old prefix of the model context
with a summary entry whose ``head`` is the first kept entry. A compaction the generation owns blocks
it and appends directly; a conversation-owned one places its summary through a write submission.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, NotRequired, TypedDict, cast

from ..context import Context
from ..entries import CompactionEntry
from ..ids import ConversationId, EntryId, TaskId
from ..json import JsonObject
from ..providers import (
    calculate_context_tokens,
    estimate_message_tokens,
    is_retryable_assistant_error,
    retry_delay_ms,
)
from ..session.transaction import Transaction
from ..tasks import define_task
from ..types import EntryDraft, MessageRecord, TaskRecord
from .context import order_tool_results
from .inbox import QueueModes
from .live import (
    LiveDoc,
    add_compaction_status,
    clear_compaction_failure,
    compaction_status,
    fail_compaction_status,
    remove_compaction_status,
    start_generation,
)
from .provider import ensure_provider_session_id
from .submissions import admit_submission
from .types import CompactionReason, CompactionResult, ContextView, ModelRef
from .usage import record_usage
from .util import call_hook, request_stream_options, task_checkpoint

#: Longest tool result text a serialized summary source keeps.
TOOL_RESULT_MAX_CHARS = 2000

SUMMARY_PREFIX = (
    "The conversation history before this point was compacted into the following summary:\n\n<summary>\n"
)
SUMMARY_SUFFIX = "\n</summary>"

SUMMARIZATION_SYSTEM_PROMPT = (
    "You are a context summarization assistant. Your task is to read a conversation between a "
    "user and an AI assistant, then produce a structured summary following the exact format "
    "specified.\n\n"
    "Do NOT continue the conversation. Do NOT respond to any questions in the conversation. "
    "ONLY output the structured summary."
)

SUMMARIZATION_PROMPT = (
    "The messages above are a conversation to summarize. Create a structured context "
    "checkpoint summary that another LLM will use to continue the work. If the conversation "
    "starts with an earlier summary, preserve its information and fold the newer messages "
    "into it.\n\n"
    """Use this EXACT format:

## Goal
[What is the user trying to accomplish? Can be multiple items if the session covers different tasks.]

## Constraints & Preferences
- [Any constraints, preferences, or requirements mentioned by user]
- [Or "(none)" if none were mentioned]

## Progress
### Done
- [x] [Completed tasks/changes]

### In Progress
- [ ] [Current work]

### Blocked
- [Issues preventing progress, if any]

## Key Decisions
- **[Decision]**: [Brief rationale]

## Next Steps
1. [Ordered list of what should happen next]

## Critical Context
- [Any data, examples, or references needed to continue]
- [Or "(none)" if not applicable]

Keep each section concise. Preserve exact file paths, function names, and error messages."""
)


class CompactionInput(TypedDict):
    reason: CompactionReason
    instructions: NotRequired[str]


class SummaryRequest(TypedDict):
    """The pinned summarization request."""

    attempt: int
    model: ModelRef
    thinkingLevel: str
    streamOptions: JsonObject
    maxTokens: int
    tail: EntryId
    firstKept: EntryId


class SelectCheckpoint(TypedDict):
    phase: str


class SummarizeCheckpoint(SummaryRequest):
    phase: str


class RetryCheckpoint(SummaryRequest):
    phase: str
    until: int


CompactionCheckpoint = SelectCheckpoint | SummarizeCheckpoint | RetryCheckpoint
"""Checkpoint shapes the compaction phases read and write."""


@dataclass(frozen=True, slots=True)
class _Policy:
    """The compaction policy resolved for one model."""

    enabled: bool = True
    reserve_tokens: int = 16_384
    keep_recent_tokens: int = 20_000
    background_tokens: int = 32_768


def compaction_policy(settings: Any, model: ModelRef) -> _Policy:
    """The compaction policy for ``model``: its ``model_overrides`` entry over the harness-wide one."""

    policy = settings.compaction
    overrides = policy.get("model_overrides") or {}
    override = overrides.get(f"{model['provider']}/{model['modelId']}") or {}
    return _Policy(
        enabled=bool(override.get("enabled", policy.get("enabled", True))),
        reserve_tokens=int(override.get("reserve_tokens", policy.get("reserve_tokens", 16_384))),
        keep_recent_tokens=int(
            override.get("keep_recent_tokens", policy.get("keep_recent_tokens", 20_000))
        ),
        background_tokens=int(
            override.get("background_tokens", policy.get("background_tokens", 32_768))
        ),
    )


async def _select_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    conversation_id = runtime.conversation_id
    agent = await runtime.agent(context)
    settings = runtime.settings
    ref = agent.model
    model = None if ref is None else runtime.models.get_model(ref["provider"], ref["modelId"])
    if ref is None or model is None:
        await fail_no_model(runtime, ref, context)
        return
    policy = compaction_policy(settings, ref)
    view = await runtime.context(conversation_id, context)
    cut = select_cut(view, policy.keep_recent_tokens)
    if cut is None:
        await complete(runtime, context)
        return
    first_kept = view["entries"][cut]["id"]
    input = cast(CompactionInput, task["input"])
    reason = input["reason"]
    instructions = input.get("instructions")
    compaction: dict[str, Any] = {
        "reason": reason,
        "entries": view["entries"][:cut],
        "messages": summarized_messages(view, cut),
        "firstKept": first_kept,
    }
    if instructions is not None:
        compaction["instructions"] = instructions
    decision: Any = None

    async def before_compact(handler: Any) -> None:
        nonlocal decision
        result = await call_hook(handler, compaction, runtime, context)
        if decision is None:
            decision = result

    await runtime.hooks.each("beforeCompact", before_compact)
    if isinstance(decision, Mapping) and decision.get("decline") is True:
        await complete(runtime, context)
        return
    if isinstance(decision, Mapping) and decision.get("summary") is not None:
        await place(runtime, first_kept, str(decision["summary"]), context)
        return
    request: SummaryRequest = {
        "attempt": 1,
        "model": ref,
        "thinkingLevel": agent.thinking_level,
        "streamOptions": request_stream_options(settings.stream),
        "maxTokens": min(
            int(0.8 * policy.reserve_tokens),
            model.max_tokens if model.max_tokens > 0 else 2**31 - 1,
        ),
        "tail": max((entry["id"] for entry in view["entries"]), default=first_kept),
        "firstKept": first_kept,
    }

    async def start(tx: Transaction) -> dict[str, Any]:
        return {"status": "running", "checkpoint": {"phase": "summarize", **request}}

    await runtime.commit(start, context)


async def _summarize_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = task_checkpoint(task)
    request = {key: value for key, value in checkpoint.items() if key != "phase"}
    ref = cast(ModelRef, request["model"])
    thinking_level = cast(str, request["thinkingLevel"])
    stream_options = cast(Mapping[str, Any], request["streamOptions"])
    max_tokens = int(request["maxTokens"])
    tail = cast(EntryId, request["tail"])
    first_kept = cast(EntryId, request["firstKept"])
    attempt = int(request["attempt"])
    model = runtime.models.get_model(ref["provider"], ref["modelId"])
    if model is None:
        await fail_no_model(runtime, ref, context)
        return
    # The context at `tail` is immutable, so this is the range `select` chose.
    view = await runtime.context(runtime.conversation_id, context, {"at": tail})
    cut = next(
        (index for index, entry in enumerate(view["entries"]) if entry["id"] == first_kept),
        -1,
    )
    now = runtime.now()
    input = cast(CompactionInput, task["input"])
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": SUMMARIZATION_SYSTEM_PROMPT, "timestamp": now},
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": summary_prompt(summarized_messages(view, cut), input.get("instructions")),
                }
            ],
            "timestamp": now,
        },
    ]
    options: dict[str, Any] = {
        **{key: value for key, value in stream_options.items() if key != "deferred"},
        "cacheRetention": "none",
        "maxTokens": max_tokens,
        "signal": runtime.signal,
        "sessionId": await ensure_provider_session_id(runtime, context),
    }
    if thinking_level != "off":
        options["reasoning"] = thinking_level
    message = await runtime.models.complete_simple(model, {"messages": messages}, options)
    # An abort mark or close: the abort invocation or the reopened task handles the committed state.
    if runtime.signal is not None:
        runtime.signal.raise_if_aborted()
    summary = summary_text(message)
    policy = runtime.settings.retry
    retry = (
        message.get("stopReason") == "error"
        and is_retryable_assistant_error(message)
        and bool(policy.get("enabled", True))
        and attempt <= int(policy.get("max_retries", 3))
    )
    until = runtime.now() + retry_delay_ms(policy, attempt) if retry else 0

    async def settle(tx: Transaction) -> dict[str, Any]:
        await record_usage(
            tx,
            runtime.conversation_id,
            "models",
            f"{message.get('provider')}/{message.get('model')}",
            cast(Mapping[str, Any], message.get("usage") or {}),
        )
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        if summary is not None:
            return await _place_summary(tx, runtime, task, live, first_kept, summary)
        if retry:
            status = compaction_status(live, runtime.task_id)
            if status is not None:
                status["retry"] = {"at": until, "error": str(message.get("errorMessage") or "")}
            checkpoint = {"phase": "retry", **request, "until": until}
            return {"status": "sleeping", "checkpoint": checkpoint, "until": until}
        text = summary_failure(message)
        fail_compaction_status(live, runtime.task_id, text)
        return {
            "status": "terminal",
            "outcome": {"status": "failed", "error": {"message": text, "detail": {"reason": "model_error"}}},
        }

    await runtime.commit(settle, context)


async def _retry_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    """After the backoff's sleep; a run before ``until`` sleeps again (spec section 8.7)."""

    checkpoint = task_checkpoint(task)
    until = int(checkpoint["until"])
    request = {key: value for key, value in checkpoint.items() if key not in ("phase", "until")}
    if until > runtime.now():
        await runtime.sleep_until(until, checkpoint, context)
        return
    attempt = int(request["attempt"]) + 1

    async def retry(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        status = compaction_status(live, runtime.task_id)
        if status is not None:
            status["attempt"] = attempt
            if "retry" in status:
                del status["retry"]
        return {"status": "running", "checkpoint": {"phase": "summarize", **request, "attempt": attempt}}

    await runtime.commit(retry, context)


async def _abort(task: TaskRecord, runtime: Any, context: Context) -> None:
    async def stop(tx: Transaction) -> dict[str, Any]:
        remove_compaction_status(await tx.doc(LiveDoc, runtime.conversation_id), runtime.task_id)
        return {"status": "terminal", "outcome": {"status": "aborted"}}

    await runtime.commit(stop, context)


CompactionTask = define_task(
    {
        "name": "pi.compaction",
        "version": 1,
        "initial": lambda _input: {"phase": "select"},
        "phases": {
            "select": _select_phase,
            "summarize": _summarize_phase,
            "retry": _retry_phase,
        },
        "abort": _abort,
    }
)


async def create_compaction(
    tx: Transaction,
    conversation_id: ConversationId,
    input: CompactionInput,
    owner: TaskId | None = None,
) -> TaskId:
    """Create a compaction task with its live status in this commit.

    ``owner`` is the generation that waits for it (a blocking compaction); without one it is
    conversation-owned, and ``background`` unless the reason is manual.
    """

    ownership: Any = (
        {"kind": "conversation"} if owner is None else {"kind": "task", "taskId": owner}
    )
    background = owner is None and input["reason"] != "manual"
    task_id = await tx.create_task(
        CompactionTask,
        cast(Any, dict(input)),
        {
            "ownership": ownership,
            "conversationId": conversation_id,
            "background": background,
        },
    )
    add_compaction_status(
        await tx.doc(LiveDoc, conversation_id),
        {"taskId": task_id, "reason": input["reason"], "blocking": owner is not None, "attempt": 1},
    )
    return task_id


def select_cut(view: ContextView, keep_recent_tokens: int) -> int | None:
    """Index in ``view.entries`` of the first entry a summary keeps (spec section 8.7).

    Walks back from the tail until ``keep_recent_tokens`` are kept, then cuts at the first
    candidate at or after that entry: an entry whose contribution starts with a user or assistant
    message, never a tool result, and never a user entry that a result of the preceding
    assistant's calls still follows.
    """

    contributions = view["contributions"]
    start = 0 if view["head"] is None else 1
    candidates = [index for index in range(start, len(contributions)) if _is_candidate(contributions, index)]
    kept = 0
    cut: int | None = None
    for index in range(len(contributions) - 1, start - 1, -1):
        for message in contributions[index]:
            kept += estimate_message_tokens(message)
        if kept < keep_recent_tokens:
            continue
        following = next((candidate for candidate in candidates if candidate >= index), None)
        cut = following if following is not None else (candidates[-1] if candidates else None)
        break
    if cut is None:
        return None
    for index in range(start, cut):
        if contributions[index]:
            return cut
    return None


def _is_candidate(contributions: Sequence[Sequence[MessageRecord]], index: int) -> bool:
    first = contributions[index][0] if contributions[index] else None
    if first is None:
        return False
    if first["role"] == "assistant":
        return True
    if first["role"] != "user":
        return False
    # A result of the preceding assistant's calls that follows this entry belongs before it.
    calls: set[str] = set()
    for before in range(index - 1, -1, -1):
        assistant = next(
            (message for message in reversed(contributions[before]) if message["role"] == "assistant"),
            None,
        )
        if assistant is None:
            continue
        calls = {
            cast(str, content["id"])
            for content in cast(Sequence[Mapping[str, Any]], assistant["content"])
            if content.get("type") == "toolCall"
        }
        break
    if not calls:
        return True
    for after in range(index, len(contributions)):
        for position, message in enumerate(contributions[after]):
            if message["role"] == "assistant" and (after > index or position > 0):
                return True
            if message["role"] == "toolResult" and message.get("toolCallId") in calls:
                return False
    return True


def summarized_messages(view: ContextView, cut: int) -> list[MessageRecord]:
    """Model messages of the entries before ``cut``: the head marker first, context order."""

    return order_tool_results([message for messages in view["contributions"][:cut] for message in messages])


def estimate_context(view: ContextView, extra: Sequence[Mapping[str, Any]]) -> int:
    """Size of a request over ``view`` followed by ``extra`` (spec section 8.3).

    The usage of the newest assistant appended after the head marker, whose request included the
    marker, plus estimates of the messages after it; without one, estimates of every message.
    """

    measured: Mapping[str, Any] | None = None
    after = view["head"]["id"] if view["head"] is not None else -(2**31)
    index = len(view["entries"]) - 1
    while index >= 0 and measured is None:
        if view["entries"][index]["id"] > after:
            measured = next(
                (
                    message
                    for message in reversed(view["contributions"][index])
                    if message["role"] == "assistant" and _context_tokens(message) > 0
                ),
                None,
            )
        index -= 1
    from_index = (
        0
        if measured is None
        else next(
            (
                index
                for index in range(len(view["messages"]) - 1, -1, -1)
                if view["messages"][index] is measured
            ),
            -1,
        )
        + 1
    )
    tokens = 0 if measured is None else _context_tokens(measured)
    for message in view["messages"][from_index:]:
        tokens += estimate_message_tokens(message)
    for planned in extra:
        tokens += estimate_message_tokens(planned)
    return tokens


def _context_tokens(message: Mapping[str, Any]) -> int:
    return calculate_context_tokens(cast(Mapping[str, Any], message.get("usage") or {}))


def summary_text(message: Mapping[str, Any]) -> str | None:
    """The summary of a clean ``stop`` with text and no tool call; anything else is not a summary."""

    content = cast(Sequence[Mapping[str, Any]], message.get("content") or ())
    if message.get("stopReason") != "stop" or any(item.get("type") == "toolCall" for item in content):
        return None
    text = "\n".join(
        cast(str, item["text"]) for item in content if item.get("type") == "text"
    ).strip()
    return None if not text else text


def summary_failure(message: Mapping[str, Any]) -> str:
    stop_reason = message.get("stopReason")
    if stop_reason in ("error", "aborted"):
        return f"Summarization failed: {message.get('errorMessage') or stop_reason}"
    if stop_reason == "length":
        return "Summarization hit the token limit; the summary is incomplete"
    content = cast(Sequence[Mapping[str, Any]], message.get("content") or ())
    if any(item.get("type") == "toolCall" for item in content):
        return "Summarization attempted to call a tool"
    return "Summarization produced no text"


def summary_prompt(messages: Sequence[Mapping[str, Any]], instructions: str | None) -> str:
    """The summarizer's user message: the serialized conversation, the prompt, and any instructions."""

    focus = "" if instructions is None else f"\n\nAdditional focus: {instructions}"
    return (
        f"<conversation>\n{serialize_conversation(messages)}\n</conversation>\n\n"
        f"{SUMMARIZATION_PROMPT}{focus}"
    )


def serialize_conversation(messages: Sequence[Mapping[str, Any]]) -> str:
    """Messages as plain text, so the summarizer reads a transcript instead of continuing it.

    System messages are omitted. Thinking is omitted too: providers return summaries of the real
    reasoning, and replayed thinking is often not part of the live context, so including it can make
    the request larger than the context it summarizes. Visible text, tool calls, and tool results
    record what happened.
    """

    parts: list[str] = []
    for message in messages:
        role = message.get("role")
        if role == "user":
            text = _content_text(message.get("content"))
            if text:
                parts.append(f"[User]: {text}")
        elif role == "assistant":
            content = cast(Sequence[Mapping[str, Any]], message.get("content") or ())
            texts = [cast(str, item["text"]) for item in content if item.get("type") == "text"]
            calls = [_call_text(item) for item in content if item.get("type") == "toolCall"]
            if texts:
                parts.append("[Assistant]: " + "\n".join(texts))
            if calls:
                parts.append("[Assistant tool calls]: " + "; ".join(calls))
        elif role == "toolResult":
            text = _content_text(message.get("content"))
            if text:
                parts.append(f"[Tool result]: {_truncate(text, TOOL_RESULT_MAX_CHARS)}")
    return "\n\n".join(parts)


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return "\n".join(
        cast(str, block["text"])
        for block in cast(Sequence[Mapping[str, Any]], content or ())
        if block.get("type") == "text" and block.get("text") is not None
    )


def _call_text(item: Mapping[str, Any]) -> str:
    """One tool call as ``name(key=json, ...)``, its arguments in their committed order."""

    arguments = cast(Mapping[str, Any], item.get("arguments") or {})
    fields = ", ".join(
        f"{key}={json.dumps(value, separators=(',', ':'))}" for key, value in arguments.items()
    )
    return f"{item['name']}({fields})"


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}\n\n[... {len(text) - max_chars} more characters truncated]"


async def place(runtime: Any, first_kept: EntryId, summary: str, context: Context) -> None:
    """Place a summary supplied by a hook in its own commit."""

    async def commit(tx: Transaction, current: Any) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        return await _place_summary(tx, runtime, current, live, first_kept, summary)

    await runtime.commit(commit, context)


async def _place_summary(
    tx: Transaction,
    runtime: Any,
    task: TaskRecord,
    live: Any,
    first_kept: EntryId,
    summary: str,
) -> dict[str, Any]:
    """Place the summary entry and complete (spec section 8.7).

    A blocking compaction, owned by its generation, appends it: the generation holds the run and
    waits. A conversation-owned one admits it as a write submission, placed at once when idle,
    otherwise at the next boundary, or settled ``stale``.
    """

    remove_compaction_status(live, runtime.task_id)
    clear_compaction_failure(live)
    settings = runtime.settings
    text = f"{SUMMARY_PREFIX}{summary}{SUMMARY_SUFFIX}"
    entry = {
        "kind": CompactionEntry.kind,
        "head": first_kept,
        "model": [{"role": "user", "content": [{"type": "text", "text": text}], "timestamp": runtime.now()}],
        "data": {"reason": cast(CompactionInput, task["input"])["reason"]},
    }
    owner = task.get("owner")
    result: CompactionResult
    if owner is not None:
        appended = await tx.append_entry(
            CompactionEntry, runtime.conversation_id, cast(EntryDraft, entry)
        )
        result = {"entryId": appended["id"]}
    else:
        result = {
            "submissionId": await admit_submission(
                tx,
                runtime.conversation_id,
                {"type": "write", "requestId": f"compaction:{runtime.task_id}", "entry": entry},
                runtime.now(),
                QueueModes(settings.steering_mode, settings.follow_up_mode),
                _start_run(runtime),
            )
        }
    return {"status": "terminal", "outcome": {"status": "completed", "result": result}}


def _start_run(runtime: Any) -> Any:
    """The Harness's ``start_run`` for a boundary a compaction-owned admission applies."""

    async def start_run(
        tx: Transaction, conversation_id: ConversationId, live: Any, inputs: list[Any]
    ) -> None:
        await start_generation(tx, conversation_id, live, inputs, runtime.registry)

    return start_run


async def complete(runtime: Any, context: Context) -> None:
    """Remove the status and complete without a summary."""

    async def commit(tx: Transaction) -> dict[str, Any]:
        remove_compaction_status(await tx.doc(LiveDoc, runtime.conversation_id), runtime.task_id)
        return {"status": "terminal", "outcome": {"status": "completed", "result": {}}}

    await runtime.commit(commit, context)


async def fail_no_model(runtime: Any, ref: ModelRef | None, context: Context) -> None:
    message = (
        "No model is configured"
        if ref is None
        else f"Model {ref['provider']}/{ref['modelId']} is not available"
    )

    async def fail(tx: Transaction) -> dict[str, Any]:
        fail_compaction_status(await tx.doc(LiveDoc, runtime.conversation_id), runtime.task_id, message)
        return {
            "status": "terminal",
            "outcome": {"status": "failed", "error": {"message": message, "detail": {"reason": "no_model"}}},
        }

    await runtime.commit(fail, context)


__all__ = [
    "CompactionCheckpoint",
    "CompactionInput",
    "CompactionResult",
    "CompactionTask",
    "SUMMARIZATION_PROMPT",
    "SUMMARIZATION_SYSTEM_PROMPT",
    "SUMMARY_PREFIX",
    "SUMMARY_SUFFIX",
    "SummaryRequest",
    "TOOL_RESULT_MAX_CHARS",
    "complete",
    "create_compaction",
    "estimate_context",
    "fail_no_model",
    "place",
    "select_cut",
    "serialize_conversation",
    "summary_failure",
    "summary_prompt",
    "summary_text",
]

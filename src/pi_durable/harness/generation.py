"""Built-in ``pi.generation`` task: prepare, request, retry, poll, and classify a model response.

Port of v1.3.0 ``src/harness/generation.ts``. The run's inputs live in ``pi.live.run``. Text answers
stream through the model protocol; partials are throttled durable commits into ``pi.live`` and are
converted to aborted assistant entries when an attempt is interrupted. A tool-calling answer starts
a tool round: one ``pi.tool`` task per call, owned by the generation, which the ``tools`` phase
waits on and then ends with the round's controls. Compaction is a separate ticket; its branches are
marked where they will hook in.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, NotRequired, TypedDict, cast

from ..context import Context
from ..entries import AssistantEntry, ResetEntry, SystemEntry, UserEntry
from ..ids import ConversationId, EntryId, TaskId
from ..json import JsonObject, JsonValue, copy_json
from ..providers import (
    is_context_overflow,
    is_retryable_assistant_error,
    retry_delay_ms,
)
from ..session.drafts import DraftList, DraftObject
from ..session.transaction import Transaction
from ..tasks import define_task
from ..types import EntryDraft, MessageRecord, TaskRecord
from .agent import add_tools
from .inbox import QueueModes, apply_boundary, prepare_boundary
from .live import LiveDoc, end_run, start_run
from .prompt import current_tools, plan_system_entries, render_sections, replay_sections
from .provider import ensure_provider_session_id
from .tool import ToolTask, append_tool_result, entry_tool_calls, harness_error
from .types import ModelRef, PromptInput
from .usage import record_usage
from .util import assign_json, call_with_args

DEFAULT_POLL_AFTER_MS = 5_000

#: Host-facing settings use PEP 8 names; provider options and persisted checkpoints use the camelCase
#: pi-ai request shape.
_STREAM_OPTION_KEYS = {
    "timeout_ms": "timeoutMs",
    "max_retries": "maxRetries",
    "max_retry_delay_ms": "maxRetryDelayMs",
    "cache_retention": "cacheRetention",
}


def request_stream_options(stream: Mapping[str, Any]) -> dict[str, Any]:
    """Map Python-facing stream settings to the camelCase provider request shape."""

    return {_STREAM_OPTION_KEYS.get(key, key): value for key, value in stream.items()}


class GenerationInput(TypedDict):
    """The run's inputs live in ``pi.live.run``, so the task input is empty."""


class PrepareCheckpoint(TypedDict):
    phase: str
    attempt: int
    compacted: NotRequired[TaskId]
    overflow: NotRequired[str]
    threshold: NotRequired[bool]


class RequestCheckpoint(TypedDict):
    phase: str
    attempt: int
    compacted: NotRequired[TaskId]
    model: ModelRef
    thinkingLevel: str
    streamOptions: dict[str, Any]
    cutoff: EntryId


class RetryCheckpoint(TypedDict):
    phase: str
    attempt: int
    compacted: NotRequired[TaskId]
    until: int


class PollCheckpoint(TypedDict):
    phase: str
    attempt: int
    compacted: NotRequired[TaskId]
    model: ModelRef
    cutoff: EntryId
    handle: JsonObject
    pollAt: int


class ToolsCheckpoint(TypedDict):
    phase: str
    assistant: EntryId
    tools: list[TaskId]
    pending: list[str]


GenerationCheckpoint = (
    PrepareCheckpoint | RequestCheckpoint | RetryCheckpoint | PollCheckpoint | ToolsCheckpoint
)
"""Checkpoint shapes the generation phases read and write."""


class GenerationResult(TypedDict):
    entryId: EntryId


def _checkpoint(task: TaskRecord) -> dict[str, Any]:
    return cast(dict[str, Any], task["state"]["checkpoint"])


def _prepare(attempt: int, **extra: Any) -> dict[str, Any]:
    return {"phase": "prepare", "attempt": attempt, **extra}


def _compact_fields(compacted: Any) -> dict[str, Any]:
    return {} if compacted is None else {"compacted": compacted}


async def _prepare_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    conversation_id = runtime.conversation_id
    agent = await runtime.agent(context)
    settings = runtime.settings
    ref = agent.model
    model = None if ref is None else runtime.models.get_model(ref["provider"], ref["modelId"])
    if ref is None or model is None:
        await fail_no_model(runtime, ref, context)
        return
    checkpoint = _checkpoint(task)
    attempt = int(checkpoint["attempt"])
    compacted = checkpoint.get("compacted")
    overflow = checkpoint.get("overflow")
    threshold_blocked = checkpoint.get("threshold") is True
    if compacted is not None and overflow is not None:
        outcome = (await runtime.outcomes([compacted], context))[0]
        if outcome.get("status") != "completed" or (
            cast(Mapping[str, Any], outcome.get("result") or {}).get("entryId") is None
        ):
            await fail_model_error(runtime, str(overflow), context)
            return
    if compacted is not None and threshold_blocked:
        outcome = (await runtime.outcomes([compacted], context))[0]
        if outcome.get("status") != "completed":
            await fail_model_error(runtime, compaction_failure(outcome), context)
            return
    view = await runtime.context(conversation_id, context)
    shown = replay_sections(view["messages"])
    report = runtime.report
    env: Any = None
    try:
        env = await runtime.env(context)
    except BaseException as error:
        if context.abort_signal is not None and context.abort_signal.aborted:
            raise
        report(error)
    prompt_input = PromptInput(
        conversation_id=conversation_id,
        agent=agent,
        env=env,
        shown=shown,
        read=runtime,
    )
    desired = await render_sections(agent.sections, prompt_input, shown, report, context)
    entries = plan_system_entries(view, desired, agent.tools, runtime.now())
    # Threshold and overflow compaction land with the compaction ticket. Until then every request
    # is sent and an overflow fails like any other non-retryable model error.

    async def request_commit(tx: Transaction) -> dict[str, Any]:
        page = await tx.scan_entries({"conversationId": conversation_id}, 1)
        cutoff: EntryId | None = page["items"][0]["id"] if page["items"] else None
        for entry in entries:
            cutoff = (
                await tx.append_entry(
                    SystemEntry, conversation_id, cast(EntryDraft, entry)
                )
            )["id"]
        if cutoff is None:
            raise RuntimeError(f"Conversation {conversation_id} has no entries to send")
        checkpoint_value: dict[str, Any] = {
            "phase": "request",
            "attempt": attempt,
            **_compact_fields(compacted),
            "model": ref,
            "thinkingLevel": agent.thinking_level,
            "streamOptions": request_stream_options(settings.stream),
            "cutoff": cutoff,
        }
        return {"status": "running", "checkpoint": checkpoint_value}

    await runtime.commit(request_commit, context)


async def _request_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = _checkpoint(task)
    attempt = int(checkpoint["attempt"])
    compacted = checkpoint.get("compacted")
    ref = cast(ModelRef, checkpoint["model"])
    thinking_level = cast(str, checkpoint["thinkingLevel"])
    stream_options = cast(dict[str, Any], checkpoint["streamOptions"])
    cutoff = cast(EntryId, checkpoint["cutoff"])
    conversation_id = runtime.conversation_id

    async def start(tx: Transaction) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        await convert_partial(tx, live, conversation_id)
        live["generation"] = {"attempt": attempt}

    await runtime.commit(start, context)
    model = runtime.models.get_model(ref["provider"], ref["modelId"])
    if model is None:
        await fail_no_model(runtime, ref, context)
        return
    view = await runtime.context(conversation_id, context, {"at": cutoff})
    messages = list(view["messages"])

    async def before_request(handler: Callable[..., Any]) -> None:
        nonlocal messages
        replaced = await _call(handler, {"messages": messages}, runtime, context)
        if replaced is not None:
            value = replaced.get("messages") if isinstance(replaced, Mapping) else None
            if value is not None:
                messages = list(value)

    await runtime.hooks.each("beforeRequest", before_request)
    options: dict[str, Any] = {
        **stream_options,
        "signal": runtime.signal,
        "sessionId": await ensure_provider_session_id(runtime, context),
    }
    if thinking_level != "off":
        options["reasoning"] = thinking_level
    message = await stream_response(runtime, model, messages, options, attempt, context)
    request = {
        "attempt": attempt,
        "compacted": compacted,
        "model": ref,
        "cutoff": cutoff,
        "messages": list(view["messages"]),
    }
    await classify(runtime, request, message, context)


async def _retry_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = _checkpoint(task)
    attempt = int(checkpoint["attempt"])
    compacted = checkpoint.get("compacted")
    await runtime.sleep(int(checkpoint["until"]), context)
    conversation_id = runtime.conversation_id

    async def retry(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        live["generation"] = {"attempt": attempt + 1}
        return {
            "status": "running",
            "checkpoint": _prepare(attempt + 1, **_compact_fields(compacted)),
        }

    await runtime.commit(retry, context)


async def _poll_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = _checkpoint(task)
    ref = cast(ModelRef, checkpoint["model"])
    model = runtime.models.get_model(ref["provider"], ref["modelId"])
    if model is None:
        await fail_no_model(runtime, ref, context)
        return
    await runtime.sleep(int(checkpoint["pollAt"]), context)
    message = await runtime.models.fetch_deferred(
        model, cast(JsonObject, checkpoint["handle"]), {"signal": runtime.signal}
    )
    await classify(
        runtime,
        {
            "attempt": int(checkpoint["attempt"]),
            "compacted": checkpoint.get("compacted"),
            "model": ref,
            "cutoff": checkpoint["cutoff"],
            "pollAt": int(checkpoint["pollAt"]),
        },
        message,
        context,
    )


async def _tools_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = cast(ToolsCheckpoint, _checkpoint(task))
    assistant = checkpoint["assistant"]
    started = list(checkpoint["tools"])
    pending = list(checkpoint["pending"])
    if not pending:
        await finish_tool_round(runtime, assistant, started, context)
        return
    # Sequential round: start the next call and wait for it.
    call_id, rest = pending[0], pending[1:]

    async def start(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        task_id = await create_tool_task(tx, runtime, assistant, call_id)
        for slot in live.get("tools") or ():
            if slot.get("callId") == call_id and slot.get("taskId") is None:
                slot["taskId"] = task_id
                break
        checkpoint_value: dict[str, Any] = {
            "phase": "tools",
            "assistant": assistant,
            "tools": [*started, task_id],
            "pending": rest,
        }
        return {"status": "waiting", "checkpoint": checkpoint_value, "on": [task_id], "policy": "allSettled"}

    await runtime.commit(start, context)


async def _abort(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = _checkpoint(task)
    if checkpoint["phase"] == "poll":
        ref = cast(ModelRef, checkpoint["model"])
        model = runtime.models.get_model(ref["provider"], ref["modelId"])
        if model is not None:
            try:
                await runtime.models.cancel_deferred(
                    model, cast(JsonObject, checkpoint["handle"]), {"signal": runtime.signal}
                )
            except BaseException as error:
                runtime.report(error)
    conversation_id = runtime.conversation_id
    # Runs after the round's tool tasks are terminal; calls never started get ``aborted`` results.
    unstarted = (
        await read_calls(runtime, checkpoint["assistant"], checkpoint["pending"], context)
        if checkpoint["phase"] == "tools"
        else []
    )

    async def stop(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        await convert_partial(tx, live, conversation_id)
        for call in unstarted:
            message = f"Tool {call['name']} was aborted"
            await append_tool_result(
                tx,
                conversation_id,
                call,
                harness_error("aborted", message),
                {"timestamp": runtime.now()},
            )
        end_run(tx, live, runtime.task_id, {"status": "unanswered", "reason": "aborted"})
        return {"status": "terminal", "outcome": {"status": "aborted"}}

    await runtime.commit(stop, context)


GenerationTask = define_task(
    {
        "name": "pi.generation",
        "version": 1,
        "initial": lambda _input: {"phase": "prepare", "attempt": 1},
        "phases": {
            "prepare": _prepare_phase,
            "request": _request_phase,
            "retry": _retry_phase,
            "poll": _poll_phase,
            "tools": _tools_phase,
        },
        "abort": _abort,
    }
)


async def _call(handler: Callable[..., Any], *args: Any) -> Any:
    result = call_with_args(handler, *args)
    if hasattr(result, "__await__"):
        return await cast(Awaitable[Any], result)
    return result


def _delay(seconds: float) -> Awaitable[None]:
    return asyncio.sleep(seconds)


class _Flusher:
    """One throttled partial publisher: at most one commit in flight and one commit per interval."""

    def __init__(self, runtime: Any, interval_ms: int, attempt: int, context: Context) -> None:
        self._runtime = runtime
        self._interval = max(0, interval_ms) / 1000.0
        self._attempt = attempt
        self._context = context
        self._pending: dict[str, Any] | None = None
        self._stopped = False
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.ensure_future(self._run())
        self._task.add_done_callback(
            lambda done: done.exception() if not done.cancelled() else None
        )

    def push(self, partial: dict[str, Any]) -> None:
        self._pending = partial
        self._wake.set()

    async def stop(self) -> None:
        self._stopped = True
        self._stop.set()
        self._wake.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        while True:
            await self._wake.wait()
            self._wake.clear()
            if self._stopped:
                return
            if self._interval > 0:
                # A stop interrupts the throttle instead of waiting it out before classification.
                try:
                    await asyncio.wait_for(self._stop.wait(), self._interval)
                except TimeoutError:
                    pass
            if self._stopped:
                return
            partial = self._pending
            self._pending = None
            if partial is None:
                continue
            try:
                message = copy_json(partial)
                await self._commit(message)
            except BaseException as error:
                if not self._runtime.signal.aborted:
                    self._runtime.report(error)

    async def _commit(self, message: JsonValue) -> None:
        conversation_id = self._runtime.conversation_id
        attempt = self._attempt

        async def flush(tx: Transaction) -> None:
            live = await tx.doc(LiveDoc, conversation_id)
            if "generation" not in live:
                live["generation"] = {"attempt": attempt}
            generation = live["generation"]
            assign_json(generation, "message", message)

        await self._runtime.commit(flush, self._context)


async def stream_response(
    runtime: Any,
    model: Any,
    messages: Sequence[Mapping[str, Any]],
    options: dict[str, Any],
    attempt: int,
    context: Context,
) -> dict[str, Any]:
    """Stream one request and return the terminal message, publishing throttled partials."""

    interval = int(runtime.settings.progress.get("partial_interval_ms", 100))
    flusher = _Flusher(runtime, interval, attempt, context)
    flusher.start()
    try:
        stream = runtime.models.stream_simple(model, {"messages": list(messages)}, options)
        async for event in stream:
            kind = event.get("type")
            if kind in ("done", "error"):
                continue
            partial = event.get("partial")
            if not isinstance(partial, Mapping):
                continue
            content = partial.get("content")
            if not content:
                continue
            flusher.push(cast(dict[str, Any], dict(partial)))
        return await stream.result()
    finally:
        await flusher.stop()


async def classify(
    runtime: Any,
    request: Mapping[str, Any],
    message: Mapping[str, Any],
    context: Context,
) -> None:
    """Classify a terminal provider message in one commit that also clears the partial."""

    runtime.signal.raise_if_aborted()
    conversation_id = runtime.conversation_id
    attempt = int(request["attempt"])
    compacted = request.get("compacted")
    ref = cast(ModelRef, request["model"])
    cutoff = cast(EntryId, request["cutoff"])
    if message.get("stopReason") == "deferred" and message.get("deferred") is not None:
        handle = cast(dict[str, Any], copy_json(message["deferred"]))
        previous = request.get("pollAt")
        poll_at = runtime.now() + int(handle.get("pollAfterMs", DEFAULT_POLL_AFTER_MS))
        if previous is not None:
            poll_at = max(poll_at, int(previous) + 1)

        async def poll(tx: Transaction) -> dict[str, Any]:
            live = await tx.doc(LiveDoc, conversation_id)
            live["generation"] = {"attempt": attempt, "deferred": {"pollAt": poll_at}}
            return {
                "status": "running",
                "checkpoint": {
                    "phase": "poll",
                    "attempt": attempt,
                    **_compact_fields(compacted),
                    "model": ref,
                    "cutoff": cutoff,
                    "handle": handle,
                    "pollAt": poll_at,
                },
            }

        await runtime.commit(poll, context)
        return

    async def after_response(handler: Callable[..., Any]) -> None:
        await _call(handler, message, runtime, context)

    await runtime.hooks.each("afterResponse", after_response)
    calls = [
        content
        for content in cast(Sequence[Mapping[str, Any]], message.get("content") or ())
        if content.get("type") == "toolCall"
    ]
    if message.get("stopReason") == "toolUse" and calls:
        await start_tool_round(runtime, request, message, calls, context)
        return
    if message.get("stopReason") in ("stop", "length", "toolUse"):
        await answer(runtime, message, context)
        return

    settings = runtime.settings
    # Threshold and overflow compaction land with the compaction ticket. Until then an overflow is
    # classified like any other non-retryable error.
    overflow = message.get("stopReason") == "error" and is_context_overflow(message)
    policy = settings.retry
    retry = (
        message.get("stopReason") == "error"
        and not overflow
        and is_retryable_assistant_error(message)
        and bool(policy.get("enabled", True))
        and attempt <= int(policy.get("max_retries", 3))
    )
    until = runtime.now() + retry_delay_ms(policy, attempt) if retry else 0

    async def settle(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        await append_assistant(tx, conversation_id, message)
        if retry:
            error_text = str(message.get("errorMessage") or "")
            live["generation"] = {"attempt": attempt, "retry": {"at": until, "error": error_text}}
            return {
                "status": "running",
                "checkpoint": {
                    "phase": "retry",
                    "attempt": attempt,
                    **_compact_fields(compacted),
                    "until": until,
                },
            }
        text = str(
            message.get("errorMessage")
            or f"Model response ended with stop reason {message.get('stopReason')}"
        )
        end_run(
            tx,
            live,
            runtime.task_id,
            {"status": "unanswered", "reason": "model_error", "detail": text},
        )
        return {
            "status": "terminal",
            "outcome": {
                "status": "failed",
                "error": {"message": text, "detail": {"reason": "model_error"}},
            },
        }

    await runtime.commit(settle, context)


async def answer(runtime: Any, message: Mapping[str, Any], context: Context) -> None:
    """Commit a final answer, apply the final boundary, and hand the run to its successor."""

    continuation: Any = None

    async def on_yield(handler: Callable[..., Any]) -> None:
        nonlocal continuation
        if continuation is not None:
            return
        result = await _call(handler, message, runtime, context)
        if isinstance(result, Mapping):
            continuation = result.get("continue")

    await runtime.hooks.each("onYield", on_yield)
    conversation_id = runtime.conversation_id

    async def finish(tx: Transaction) -> dict[str, Any]:
        settings = runtime.settings
        boundary = await prepare_boundary(
            tx,
            conversation_id,
            QueueModes(settings.steering_mode, settings.follow_up_mode),
        )
        live = await tx.doc(LiveDoc, conversation_id)
        entry = await append_assistant(tx, conversation_id, message)
        result = {
            "status": "terminal",
            "outcome": {"status": "completed", "result": {"entryId": entry["id"]}},
        }
        placed = await apply_boundary(tx, boundary, "final", runtime.now())
        if continuation is not None and not placed.users and not placed.reset:
            user = {"role": "user", "content": continuation, "timestamp": runtime.now()}
            await tx.append_entry(
                UserEntry, conversation_id, cast(EntryDraft, {"model": [user]})
            )
            del live["generation"]
            hand_over(
                live,
                runtime.task_id,
                await create_generation(tx, conversation_id, task_for(runtime)),
            )
            return result
        end_run(
            tx,
            live,
            runtime.task_id,
            {"status": "done", "answer": entry["id"]},
        )
        if placed.users:
            await start_run(tx, conversation_id, live, placed.users, task_for(runtime))
        return result

    await runtime.commit(finish, context)


async def read_calls(
    runtime: Any,
    assistant: EntryId,
    call_ids: Sequence[str],
    context: Context,
) -> list[dict[str, Any]]:
    """The calls ``call_ids`` of the assistant entry, in the given order."""

    entry = await runtime.entry(AssistantEntry, assistant, context)
    calls = entry_tool_calls(entry)
    return [
        dict(call)
        for call_id in call_ids
        for call in calls
        if call.get("id") == call_id
    ]


def _is_sequential(tools: Sequence[Any], name: str) -> bool:
    tool = next((candidate for candidate in tools if candidate.name == name), None)
    return tool is not None and tool.execution_mode == "sequential"


async def create_tool_task(
    tx: Transaction, runtime: Any, assistant: EntryId, call_id: str
) -> TaskId:
    """A tool task for ``call_id``, owned by the generation."""

    return await tx.create_task(
        ToolTask,
        {"kind": "model", "assistant": assistant, "callId": call_id},
        {"ownership": {"kind": "task", "taskId": runtime.task_id}},
    )


async def start_tool_round(
    runtime: Any,
    request: Mapping[str, Any],
    message: Mapping[str, Any],
    calls: Sequence[Mapping[str, Any]],
    context: Context,
) -> None:
    """Append the tool-calling answer and start its tool round in one commit.

    A call to a tool the request did not offer gets its ``tool_unavailable`` result here; every other
    call gets a tool task owned by the generation, only the first one now when the round is
    sequential. The generation then waits for them in its ``tools`` phase, keeping the run.
    """

    conversation_id = runtime.conversation_id
    messages = request.get("messages")
    if messages is None:
        view = await runtime.context(conversation_id, context, {"at": request["cutoff"]})
        messages = view["messages"]
    offered = {tool["name"] for tool in current_tools(cast(Sequence[Mapping[str, Any]], messages))}
    # Read as the round starts; a tool is resolved as its tool task resolves it.
    agent = await runtime.agent(context)
    sequential = runtime.settings.tool_execution == "sequential" or any(
        call["name"] in offered and _is_sequential(agent.tools, call["name"]) for call in calls
    )

    async def start(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        entry = await append_assistant(tx, conversation_id, message)
        slots: list[dict[str, Any]] = []
        tool_tasks: list[TaskId] = []
        pending: list[str] = []
        for call in calls:
            if call["name"] not in offered:
                unavailable = harness_error("tool_unavailable", f"Tool {call['name']} is not available")
                result = await append_tool_result(
                    tx,
                    conversation_id,
                    call,
                    unavailable,
                    {"timestamp": runtime.now()},
                )
                slots.append(
                    {
                        "callId": call["id"],
                        "name": call["name"],
                        "status": "done",
                        "entry": result["id"],
                    }
                )
                continue
            if sequential and tool_tasks:
                pending.append(call["id"])
                slots.append({"callId": call["id"], "name": call["name"], "status": "pending"})
                continue
            task_id = await create_tool_task(tx, runtime, entry["id"], call["id"])
            tool_tasks.append(task_id)
            slots.append(
                {
                    "callId": call["id"],
                    "name": call["name"],
                    "taskId": task_id,
                    "status": "pending",
                }
            )
        if "generation" in live:
            del live["generation"]
        live["tools"] = slots
        checkpoint_value: dict[str, Any] = {
            "phase": "tools",
            "assistant": entry["id"],
            "tools": tool_tasks,
            "pending": pending,
        }
        return {
            "status": "waiting",
            "checkpoint": checkpoint_value,
            "on": tool_tasks,
            "policy": "allSettled",
        }

    await runtime.commit(start, context)


async def finish_tool_round(
    runtime: Any,
    assistant: EntryId,
    tools: Sequence[TaskId],
    context: Context,
) -> None:
    """The round's tools are terminal: apply their controls, then end the run or hand it on."""

    conversation_id = runtime.conversation_id
    controls: dict[TaskId, dict[str, Any] | None] = {}
    outcomes = await runtime.outcomes(tools, context)
    for task_id, outcome in zip(tools, outcomes, strict=False):
        control = None
        if outcome.get("status") == "completed":
            result = outcome.get("result")
            if isinstance(result, Mapping) and result.get("kind") != "nested":
                raw = result.get("control")
                control = cast(dict[str, Any], raw) if isinstance(raw, Mapping) else None
        controls[task_id] = control
    state = await runtime.snapshot(LiveDoc, conversation_id, context)
    slots = [dict(slot) for slot in (state or {}).get("tools") or ()]
    results = [slot["entry"] for slot in slots if slot.get("entry") is not None]

    async def after_tools(handler: Callable[..., Any]) -> None:
        await _call(handler, assistant, results, runtime, context)

    await runtime.hooks.each("afterTools", after_tools)
    # Every call of the round, including those answered without a task, must ask to terminate.
    terminate = bool(slots) and all(
        slot.get("taskId") is not None and (controls.get(slot["taskId"]) or {}).get("terminate") is True
        for slot in slots
    )
    added = [
        name
        for control in controls.values()
        for name in ((control or {}).get("addTools") or ())
    ]
    # The last handoff in call order wins.
    handoff: str | None = None
    for control in controls.values():
        if control is not None and control.get("handoff") is not None:
            handoff = str(control["handoff"])

    async def finish(tx: Transaction) -> dict[str, Any]:
        boundary = await prepare_boundary(
            tx,
            conversation_id,
            QueueModes(runtime.settings.steering_mode, runtime.settings.follow_up_mode),
        )
        if added:
            await add_tools(tx, conversation_id, added)
        live = await tx.doc(LiveDoc, conversation_id)
        now = runtime.now()
        if terminate or handoff is not None:
            if handoff is not None:
                message = {"role": "user", "content": handoff, "timestamp": now}
                entry = await tx.append_entry(
                    ResetEntry,
                    conversation_id,
                    cast(EntryDraft, {"head": "self", "model": [message]}),
                )
                boundary.head = entry["id"]
            placed = await apply_boundary(tx, boundary, "final", now)
            end_run(tx, live, runtime.task_id, {"status": "done", "answer": assistant})
            if placed.users:
                await start_run(tx, conversation_id, live, placed.users, task_for(runtime))
        else:
            placed = await apply_boundary(tx, boundary, "postTools", now)
            if placed.reset:
                # The queued reset cut the run's context before an answer.
                end_run(tx, live, runtime.task_id, {"status": "unanswered", "reason": "reset"})
                if placed.users:
                    await start_run(tx, conversation_id, live, placed.users, task_for(runtime))
            else:
                for key in ("tools", "nestedTools"):
                    if key in live:
                        del live[key]
                run = live.get("run")
                if run is not None and run["taskId"] == runtime.task_id:
                    run["inputs"].extend(placed.users)
                hand_over(
                    live,
                    runtime.task_id,
                    await create_generation(tx, conversation_id, task_for(runtime)),
                )
        return {
            "status": "terminal",
            "outcome": {"status": "completed", "result": {"entryId": assistant}},
        }

    await runtime.commit(finish, context)


def task_for(runtime: Any) -> Any:
    """The registry's generation task, the definition a successor run uses."""

    task = runtime.registry.task("pi.generation")
    if task is None:
        raise RuntimeError("Registry lacks the pi.generation task")
    return task


async def fail_model_error(runtime: Any, text: str, context: Context) -> None:
    conversation_id = runtime.conversation_id

    async def fail(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        end_run(
            tx,
            live,
            runtime.task_id,
            {"status": "unanswered", "reason": "model_error", "detail": text},
        )
        return {
            "status": "terminal",
            "outcome": {
                "status": "failed",
                "error": {"message": text, "detail": {"reason": "model_error"}},
            },
        }

    await runtime.commit(fail, context)


async def fail_no_model(runtime: Any, ref: ModelRef | None, context: Context) -> None:
    message = (
        "No model is configured"
        if ref is None
        else f"Model {ref['provider']}/{ref['modelId']} is not available"
    )
    conversation_id = runtime.conversation_id

    async def fail(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        end_run(tx, live, runtime.task_id, {"status": "unanswered", "reason": "no_model"})
        return {
            "status": "terminal",
            "outcome": {
                "status": "failed",
                "error": {"message": message, "detail": {"reason": "no_model"}},
            },
        }

    await runtime.commit(fail, context)


def compaction_failure(outcome: Mapping[str, Any]) -> str:
    status = outcome.get("status")
    if status in ("failed", "faulted"):
        return f"Compaction failed: {cast(Mapping[str, Any], outcome.get('error') or {}).get('message', '')}"
    if status == "orphaned":
        return f"Compaction failed: {outcome.get('reason', '')}"
    return "Compaction was aborted"


async def convert_partial(
    tx: Transaction, live: Any, conversation_id: ConversationId
) -> None:
    """Append a committed partial as an aborted assistant entry; the caller replaces or removes it."""

    generation = live.get("generation")
    if generation is None:
        return
    partial = generation.get("message")
    if partial is None:
        return
    message = cast(dict[str, Any], copy_json(_plain(partial)))
    message["stopReason"] = "aborted"
    await append_assistant(tx, conversation_id, message)


async def append_assistant(
    tx: Transaction, conversation_id: ConversationId, message: Mapping[str, Any]
) -> Any:
    """Append a provider result and add its usage to ``pi.usage`` in the same commit."""

    provider = str(message.get("provider") or "")
    model = str(message.get("model") or "")
    await record_usage(
        tx,
        conversation_id,
        "models",
        f"{provider}/{model}",
        cast(Mapping[str, Any], message.get("usage") or {}),
    )
    return await tx.append_entry(
        AssistantEntry,
        conversation_id,
        cast(EntryDraft, {"model": [cast(MessageRecord, copy_json(message))]}),
    )


async def create_generation(
    tx: Transaction, conversation_id: ConversationId, task: Any = GenerationTask
) -> TaskId:
    """A generation owned by its conversation."""

    return await tx.create_task(
        task, {}, {"ownership": {"kind": "conversation"}, "conversationId": conversation_id}
    )


def hand_over(live: Any, from_id: TaskId, to_id: TaskId) -> None:
    """Hand run control from ``from_id`` to ``to_id``; the run's inputs move with it."""

    run = live.get("run")
    if run is not None and run["taskId"] == from_id:
        run["taskId"] = to_id


# ── Partial writes ──────────────────────────────────────────────────────────


def _plain(value: Any) -> Any:
    """A detached JSON value for a draft member."""

    if isinstance(value, (DraftObject, DraftList)):
        return value.snapshot()
    return value


def _is_record(value: Any) -> bool:
    return isinstance(value, (dict, DraftObject))


def _is_array(value: Any) -> bool:
    return isinstance(value, (list, DraftList))


__all__ = [
    "DEFAULT_POLL_AFTER_MS",
    "GenerationCheckpoint",
    "GenerationInput",
    "GenerationResult",
    "GenerationTask",
    "append_assistant",
    "classify",
    "compaction_failure",
    "convert_partial",
    "create_generation",
    "fail_model_error",
    "fail_no_model",
    "hand_over",
    "stream_response",
]

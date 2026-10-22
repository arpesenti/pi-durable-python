"""Built-in ``pi.tool`` task: resolve, validate, hook, execute, and persist a tool call.

Port of v1.3.0 ``src/harness/tool.ts`` for model-issued calls. The phase handler resolves the called
tool among its phase agent's tools, repairs and validates the arguments, runs ``beforeTool``,
records intent, executes the tool, runs ``afterTool``, and settles the result — all in one handler so
no phase boundary separates resolution from settlement. ``execute`` is reached only by recovery and
applies the replay rule. A model-issued call settles by appending its ``pi.tool-result`` entry.

Nested calls (``executeTool()``), their ``NestedCallDoc``, and ``structuredOutput`` validation
arrive with the nested-tools ticket; the types and slots are already in place.
"""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypedDict, cast

from ..context import Context, await_with_context
from ..delta import OVERLAP_SCAN_LIMIT
from ..entries import AssistantEntry, ToolResultEntry
from ..errors import error_message, is_failure
from ..ids import ConversationId, EntryId, TaskId
from ..json import JsonValue, copy_json
from ..session.transaction import Transaction
from ..tasks import define_task
from ..types import EntryDraft, MessageRecord, TaskRecord
from .agent import DEFAULT_PROGRESS_POLICY
from .live import (
    TOOL_TASK_KIND,
    LiveDoc,
    clear_progress,
    finish_slot,
    remove_nested_slots,
    tool_slot,
)
from .output import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_LINES,
    PROGRESS_BYTES_PER_SECOND,
    OutputBuffer,
    OutputLimits,
    Progress,
    bound_output,
    overlap,
    utf8_byte_length,
)
from .types import ToolControl, ToolDiagnostic, ToolExecutionResult, ToolHookCall, ToolRegistration
from .usage import record_usage
from .util import assign_json, call_with_args
from .validation import ToolArgumentError, validate_tool_arguments


class ToolTaskInput(TypedDict):
    """A model-issued call, read from its assistant entry."""

    kind: Literal["model"]
    assistant: EntryId
    callId: str


class ExecuteCheckpoint(TypedDict):
    phase: Literal["execute"]
    arguments: dict[str, JsonValue]
    replay: Literal["safe", "unsafe"]


class CallCheckpoint(TypedDict):
    phase: Literal["call"]


ToolTaskCheckpoint = CallCheckpoint | ExecuteCheckpoint
"""Checkpoint shapes the tool phases read and write."""


class ToolTaskResult(TypedDict, total=False):
    """A model-issued call's result is its transcript entry; ``control`` only applies while completed."""

    kind: Literal["model"]
    entryId: EntryId
    control: ToolControl


class Ending(TypedDict, total=False):
    """How a tool task ends; the result entry is appended either way."""

    status: Literal["completed", "aborted", "failed"]
    message: str


COMPLETED: Ending = {"status": "completed"}

Content = list[JsonValue]


async def _call_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    call = await read_call(runtime, cast(ToolTaskInput, task["input"]), context)
    tool = await resolve_tool(runtime, cast(ToolTaskInput, task["input"]), call["name"], context)
    if tool is None:
        message = f"Tool {call['name']} is not available"
        await settle(
            runtime, call, COMPLETED, lambda _slot: harness_error("tool_unavailable", message), context
        )
        return
    prepared = prepare(tool, call.get("arguments") or {})
    if isinstance(prepared, str):
        error = invalid(prepared)
        await settle(runtime, call, COMPLETED, lambda _slot: error, context)
        return
    args = prepared
    checked = validate(tool, call, args)
    if isinstance(checked, str):
        error = invalid(checked)
        await settle(runtime, call, COMPLETED, lambda _slot: error, context)
        return
    args = checked
    block: str | None = None

    async def before_tool(handler: Callable[..., Any]) -> None:
        nonlocal args, block
        if block is not None:
            return
        try:
            decision = await _call(handler, {**call, "arguments": args}, runtime, context)
            if isinstance(decision, Mapping):
                if decision.get("block") is not None:
                    block = str(decision["block"])
                elif decision.get("arguments") is not None:
                    args = cast(dict[str, JsonValue], decision["arguments"])
        except BaseException as error:
            if runtime.signal is not None and runtime.signal.aborted:
                raise
            block = error_message(error)

    await runtime.hooks.each("beforeTool", before_tool)
    if block is not None:
        blocked = f"Tool call blocked: {block}"
        await settle(
            runtime, call, COMPLETED, lambda _slot: harness_error("blocked", blocked), context
        )
        return
    validated = validate(tool, call, args)
    if isinstance(validated, str):
        error = invalid(validated)
        await settle(runtime, call, COMPLETED, lambda _slot: error, context)
        return
    # Strict JSON: repair may set optional properties to None, which the slot and checkpoint cannot hold.
    final = cast(dict[str, JsonValue], copy_json(validated))

    async def intent(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        slot = tool_slot(live, runtime.task_id)
        if slot is not None:
            slot["status"] = "running"
        return {
            "status": "running",
            "checkpoint": {
                "phase": "execute",
                "arguments": final,
                "replay": tool.replay or "unsafe",
            },
        }

    await runtime.commit(intent, context)
    await run(runtime, cast(ToolTaskInput, task["input"]), call, tool, final, context)


async def _execute_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    checkpoint = cast(ExecuteCheckpoint, task["state"]["checkpoint"])
    args = checkpoint["arguments"]
    replay = checkpoint["replay"]
    call = await read_call(runtime, cast(ToolTaskInput, task["input"]), context)
    tool = await resolve_tool(runtime, cast(ToolTaskInput, task["input"]), call["name"], context)
    if replay == "safe" and tool is not None and tool.replay == "safe":
        # The rerun reports from scratch; clear what the interrupted attempt published.
        async def clear(tx: Transaction) -> None:
            live = await tx.doc(LiveDoc, runtime.conversation_id)
            slot = tool_slot(live, runtime.task_id)
            if slot is not None:
                clear_progress(slot)

        await runtime.commit(clear, context)
        await run(runtime, cast(ToolTaskInput, task["input"]), call, tool, args, context)
        return
    message = f"Tool {call['name']} was interrupted and may have partially run"
    # ``failed`` records cancellation intent, so the call's owned conversations, left unsupervised,
    # are aborted.
    ending: Ending = {"status": "failed", "message": message}
    await settle(
        runtime,
        call,
        ending,
        lambda slot: from_slot(slot, "interrupted", message),
        context,
    )


async def _abort(task: TaskRecord, runtime: Any, context: Context) -> None:
    call = await read_call(runtime, cast(ToolTaskInput, task["input"]), context)
    # Abandoned after a restart (``TaskOptions.abandonOnRestart``): say whether the call may have run.
    checkpoint = task["state"].get("checkpoint") or {}
    if task.get("abortReason") != "restart":
        code, message = "aborted", f"Tool {call['name']} was aborted"
    elif checkpoint.get("phase") == "execute":
        code = "interrupted"
        message = f"Tool {call['name']} was interrupted by a restart and may have partially run"
    else:
        code = "abandoned"
        message = f"Tool {call['name']} was not started: its caller ended with a restart"
    ending: Ending = {"status": "aborted"}
    await settle(
        runtime,
        call,
        ending,
        lambda slot: from_slot(slot, code, message),
        context,
    )


def _migrate(input: Any, checkpoint: Any, _version: int) -> dict[str, Any]:
    """Version 1 had only model-issued calls."""

    return {
        "input": {"kind": "model", **cast(Mapping[str, Any], input)},
        "checkpoint": checkpoint,
    }


ToolTask = define_task(
    {
        "name": TOOL_TASK_KIND,
        "version": 2,
        "initial": lambda _input: {"phase": "call"},
        "migrate": _migrate,
        "phases": {"call": _call_phase, "execute": _execute_phase},
        "abort": _abort,
    }
)


async def resolve_tool(
    runtime: Any,
    _input: ToolTaskInput,
    name: str,
    context: Context,
) -> ToolRegistration | None:
    """The called tool among the ones the model is offered for a model-issued call."""

    agent = await runtime.agent(context)
    return next((tool for tool in agent.tools if tool.name == name), None)


def entry_tool_calls(entry: Mapping[str, Any] | None) -> list[ToolHookCall]:
    """The tool calls of an entry's assistant message, in call order."""

    message = None if entry is None else (entry.get("model") or [None])[0]
    if not isinstance(message, Mapping) or message.get("role") != "assistant":
        return []
    return [
        cast(ToolHookCall, dict(content))
        for content in cast(Sequence[Mapping[str, Any]], message.get("content") or ())
        if content.get("type") == "toolCall"
    ]


async def read_call(runtime: Any, input: ToolTaskInput, context: Context) -> ToolHookCall:
    """The call, read from its assistant entry."""

    entry = await runtime.entry(AssistantEntry, input["assistant"], context)
    call = next(
        (candidate for candidate in entry_tool_calls(entry) if candidate.get("id") == input["callId"]),
        None,
    )
    if call is None:
        raise RuntimeError(f"Entry {input['assistant']} has no tool call {input['callId']}")
    return call


def prepare(tool: ToolRegistration, args: Any) -> dict[str, JsonValue] | str:
    """The call's arguments as repaired by the tool, or why the repair failed."""

    if tool.prepare_arguments is None:
        return cast(dict[str, JsonValue], args)
    try:
        repaired = tool.prepare_arguments(args)
        if hasattr(repaired, "__await__"):
            raise TypeError("prepare_arguments must be synchronous")
        return cast(dict[str, JsonValue], repaired)
    except Exception as error:
        return error_message(error)


def validate(tool: ToolRegistration, call: Mapping[str, Any], args: Any) -> dict[str, JsonValue] | str:
    """Arguments validated and coerced against the implementation's schema."""

    try:
        return cast(dict[str, JsonValue], validate_tool_arguments(tool, {**call, "arguments": args}))
    except ToolArgumentError as error:
        return error_message(error)
    except Exception as error:
        return error_message(error)


def invalid(message: str) -> ToolExecutionResult:
    return harness_error("invalid_arguments", message)


class Reported:
    """What a running tool reported through its api: output, the last details, and diagnostics."""

    __slots__ = ("output", "limits", "diagnostics", "details")

    def __init__(self, limits: OutputLimits) -> None:
        self.output = OutputBuffer(limits)
        self.limits = limits
        self.diagnostics: list[ToolDiagnostic] = []
        self.details: JsonValue = None


async def run(
    runtime: Any,
    input: ToolTaskInput,
    call: ToolHookCall,
    tool: ToolRegistration,
    args: dict[str, JsonValue],
    context: Context,
) -> None:
    """Execute with the resolved implementation, then settle its result."""

    reported = Reported(output_limits(tool))
    progress = publish_progress(runtime, reported, context)
    ended = False

    def assert_live() -> None:
        if ended:
            raise RuntimeError(f"Tool call {call['id']} has settled")

    api = ToolExecutionApiImpl(
        runtime=runtime,
        reported=reported,
        progress=progress,
        call=call,
        assert_live=assert_live,
        resumes=tool.replay == "safe",
    )
    result: ToolExecutionResult = {}
    ending: Ending = COMPLETED
    duration_ms: int | None = None
    try:
        # Built for this call, so a rerun after recovery gets the conversation's environment at that time.
        env = await runtime.env(context)
        api.env = env
        started_at = time.monotonic()
        try:
            raw = tool.execute(args, api, context) if tool.execute is not None else {}
            if hasattr(raw, "__await__"):
                raw = await cast(Awaitable[Any], raw)
            if raw is None:
                raw = {}
            if not isinstance(raw, Mapping):
                raise TypeError(f"Tool {call['name']} returned no result object")
            result = cast(ToolExecutionResult, dict(raw))
        finally:
            duration_ms = round((time.monotonic() - started_at) * 1000)
    except BaseException as error:
        if runtime.signal is not None and runtime.signal.aborted:
            ended = True
            for waiter in await progress.stop():
                _reject(waiter, error)
            raise
        if not is_failure(error):
            # A teardown signal without an abort mark keeps unwinding.
            raise
        result = {
            "isError": True,
            "diagnostics": [tool_diagnostic("tool_error", error_message(error))],
        }
        # A throw, from ``execute()`` or from building the environment, ends the task ``failed``, which
        # cancels what the call owned; it no longer supervises it.
        ending = {"status": "failed", "message": f"Tool {call['name']} threw"}
    ended = True
    reported.output.end()
    # Details still waiting for a progress commit settle with the terminal commit, the final flush.
    pending = await progress.stop()
    try:
        settled = await final_result(runtime, call, result, reported, context)
        await settle(runtime, call, ending, lambda _slot: settled, context, duration_ms)
    except BaseException as error:
        for waiter in pending:
            _reject(waiter, error)
        raise
    for waiter in pending:
        if not waiter.done():
            waiter.set_result(None)


def output_limits(tool: ToolRegistration) -> OutputLimits:
    """A tool's output limits, over the defaults 50 KiB, 2,000 lines, and head retention."""

    raw = dict(tool.output_limits or {})
    max_bytes = raw.get("max_bytes", raw.get("maxBytes"))
    max_lines = raw.get("max_lines", raw.get("maxLines"))
    retain = raw.get("retain")
    retain = "head" if retain is None else retain
    if retain not in ("head", "tail"):
        raise TypeError(f"Tool {tool.name} output_limits.retain must be 'head' or 'tail'")
    return OutputLimits(
        max_bytes=DEFAULT_MAX_BYTES if max_bytes is None else int(max_bytes),
        max_lines=DEFAULT_MAX_LINES if max_lines is None else int(max_lines),
        retain=cast(str, retain),
    )


class ToolExecutionApiImpl:
    """The api a running tool receives: bounded output, durable commits, and Harness reads."""

    def __init__(
        self,
        *,
        runtime: Any,
        reported: Reported,
        progress: Progress,
        call: ToolHookCall,
        assert_live: Callable[[], None],
        resumes: bool,
    ) -> None:
        self._runtime = runtime
        self._reported = reported
        self._progress = progress
        self._call = call
        self._assert_live = assert_live
        self._resumes = resumes
        self.task_id = runtime.task_id
        self.conversation_id = runtime.conversation_id
        self.call_id = call.get("id", "")
        self.registry = runtime.registry
        self.models = runtime.models
        self.env: Any = None
        limits = reported.limits
        self.output_window: Mapping[str, Any] | None = (
            {
                "max_bytes": limits.max_bytes,
                "max_lines": limits.max_lines,
                "min_interval_ms": int(
                    runtime.settings.progress.get(
                        "output_interval_ms", DEFAULT_PROGRESS_POLICY["output_interval_ms"]
                    )
                ),
                "bytes_per_second": PROGRESS_BYTES_PER_SECOND,
            }
            if limits.retain == "tail"
            else None
        )

    async def agent(self, context: Context) -> Any:
        return await self._runtime.agent(context)

    def output(self, chunk: str | bytes, skipped: Mapping[str, Any] | None = None) -> None:
        self._assert_live()
        if self._reported.output.push(chunk, skipped):
            self._progress.mark()

    def retained_output(self) -> Mapping[str, Any]:
        self._assert_live()
        retained = self._reported.output.snapshot()
        return {"text": retained.text, "truncated": retained.dropped_bytes > 0}

    def diagnostic(self, diagnostic: Mapping[str, Any]) -> None:
        self._assert_live()
        self._reported.diagnostics.append(cast(ToolDiagnostic, copy_json(dict(diagnostic))))
        self._progress.mark()

    def details(self, value: JsonValue, context: Context) -> Awaitable[None]:
        # The update and its waiter are registered before the caller awaits, as an async function's
        # synchronous prefix does upstream.
        self._assert_live()
        signal = context.abort_signal
        if signal is not None:
            signal.raise_if_aborted()
        self._reported.details = copy_json(value)
        committed = self._progress.mark_and_wait()
        committed.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)

        async def wait() -> None:
            # Cancelling the wait leaves the update in place; the commit's own outcome stays observed.
            await await_with_context(committed, context)

        return wait()

    async def commit(self, change: Callable[..., Any], context: Context) -> Any:
        box: dict[str, Any] = {}

        async def apply(tx: Transaction) -> None:
            result = change(tx)
            if hasattr(result, "__await__"):
                result = await cast(Awaitable[Any], result)
            box["value"] = result

        await self._runtime.commit(apply, context)
        return box.get("value")

    async def memo(self, name: str, *args: Any) -> JsonValue | None:
        return await self._runtime.memo(name, *args)

    async def create_task(
        self,
        task: Any,
        input: JsonValue,
        options: Mapping[str, Any],
        context: Context,
    ) -> TaskId:
        child = dict(options)
        ownership = cast(Mapping[str, Any], child["ownership"])
        if ownership.get("kind") == "task" and not self._resumes and "abandonOnRestart" not in child:
            child["abandonOnRestart"] = True
        box: dict[str, TaskId] = {}

        async def apply(tx: Transaction) -> None:
            box["id"] = await tx.create_task(task, input, cast(Any, child))

        await self._runtime.commit(apply, context)
        return box["id"]

    async def get_task(self, id: TaskId, context: Context) -> TaskRecord | None:
        return await self._runtime.get_task(id, context)

    async def wait_for_task(self, id: TaskId, context: Context) -> Any:
        return await self._runtime.wait_for_task(id, context)

    async def conversation(self, id: ConversationId, context: Context) -> Any:
        return await self._runtime.conversation(id, context)

    async def snapshot(self, token: Any, *args: Any) -> Any:
        return await self._runtime.snapshot(token, *args)

    async def snapshot_as_of(self, token: Any, *args: Any) -> Any:
        return await self._runtime.snapshot_as_of(token, *args)

    async def watch_doc(self, token: Any, *args: Any) -> Any:
        return await self._runtime.watch_doc(token, *args)

    async def execute_tool(
        self,
        name: str,
        args: Mapping[str, Any],
        context: Context,
        options: Mapping[str, Any] | None = None,
    ) -> Any:
        del name, args, context, options
        self._assert_live()
        raise NotImplementedError("Nested tool calls arrive with the nested-tools ticket")


_UNSET = object()


def publish_progress(runtime: Any, reported: Reported, context: Context) -> Progress:
    """Throttled commits of what the tool reported into its slot, each writing only what changed."""

    written: dict[str, Any] = {"text": "", "details": _UNSET, "diagnostics": 0}

    def write() -> Any:
        # Capture everything synchronously: the tool keeps reporting while the commit is in flight.
        snapshot = reported.output.snapshot()
        details = reported.details
        diagnostics = len(reported.diagnostics)
        added = reported.diagnostics[written["diagnostics"] : diagnostics]
        details_changed = written["details"] is not details
        # What the commit writes, as Chord diffs the string: an append, a trim plus an append of what
        # follows the shared part, or the whole window when its bounded overlap search finds nothing.
        bytes_ = 0
        previous = cast(str, written["text"])
        if snapshot.text != previous:
            shared = (
                len(previous)
                if snapshot.text.startswith(previous)
                else overlap(previous, snapshot.text, OVERLAP_SCAN_LIMIT)
            )
            bytes_ += utf8_byte_length(snapshot.text[shared:])
        if details_changed:
            bytes_ += utf8_byte_length(json.dumps(details))
        if added:
            bytes_ += utf8_byte_length(json.dumps(added))

        async def commit() -> int:
            async def flush(tx: Transaction) -> None:
                live = await tx.doc(LiveDoc, runtime.conversation_id)
                slot = tool_slot(live, runtime.task_id)
                if slot is None:
                    return
                # REMINDER: assign output as one string field, so the delta records an append or a trim.
                if (slot.get("output") or "") != snapshot.text:
                    slot["output"] = snapshot.text
                if snapshot.dropped_bytes > 0:
                    slot["droppedBytes"] = snapshot.dropped_bytes
                if snapshot.dropped_lines > 0:
                    slot["droppedLines"] = snapshot.dropped_lines
                if details_changed and details is not None:
                    assign_json(slot, "details", details)
                if added:
                    if "diagnostics" not in slot:
                        slot["diagnostics"] = list(added)
                    else:
                        for diagnostic in added:
                            slot["diagnostics"].append(diagnostic)

            await runtime.commit(flush, context)
            written["text"] = snapshot.text
            written["details"] = details
            written["diagnostics"] = diagnostics
            return bytes_

        return commit()

    def on_error(error: BaseException) -> None:
        # Rejections after an abort mark or close are expected; the committed state stays consistent.
        if runtime.signal is None or not runtime.signal.aborted:
            runtime.report(error)

    interval = runtime.settings.progress.get(
        "output_interval_ms", DEFAULT_PROGRESS_POLICY["output_interval_ms"]
    )
    return Progress(write, on_error, int(interval))


async def final_result(
    runtime: Any,
    call: ToolHookCall,
    result: ToolExecutionResult,
    reported: Reported,
    context: Context,
) -> ToolExecutionResult:
    """The settled result: retained output and details as fallbacks, ``afterTool`` applied, text bounded."""

    harness: list[ToolDiagnostic] = []
    retained = reported.output.snapshot() if "output" not in result else None
    if retained is None:
        output: Content = list(result.get("output") or [])
    else:
        output = [] if retained.text == "" else [{"type": "text", "text": retained.text}]
    final: ToolExecutionResult = {
        **result,
        "output": output,
        "details": result["details"] if "details" in result else reported.details,
        "diagnostics": [*reported.diagnostics, *(result.get("diagnostics") or [])],
    }

    async def after_tool(handler: Callable[..., Any]) -> None:
        nonlocal final
        replaced = await _call(handler, call, final, runtime, context)
        if isinstance(replaced, Mapping):
            final = cast(ToolExecutionResult, dict(replaced))

    await runtime.hooks.each("afterTool", after_tool)
    # The retained output's truncation applies only while afterTool kept that output.
    if retained is not None and retained.dropped_bytes > 0 and final.get("output") is output:
        harness.append(truncated(retained, reported.limits.retain))
    bounded = bound_content(list(final.get("output") or []), reported.limits)
    if bounded.dropped_bytes > 0:
        harness.append(truncated(bounded, reported.limits.retain))
    return {
        **final,
        "output": bounded.content,
        "diagnostics": [*(final.get("diagnostics") or []), *harness],
    }


async def settle(
    runtime: Any,
    call: ToolHookCall,
    ending: Ending,
    build: Callable[[Any], ToolExecutionResult],
    context: Context,
    duration_ms: int | None = None,
) -> None:
    """Commit the tool's terminal state and mark its slot done."""

    async def finish(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        slot = tool_slot(live, runtime.task_id)
        result = build(slot)
        entry = await append_tool_result(
            tx,
            runtime.conversation_id,
            call,
            result,
            {"timestamp": runtime.now(), "durationMs": duration_ms},
        )
        if slot is not None and "parentTaskId" not in slot:
            finish_slot(slot)
            slot["entry"] = entry["id"]
        control = result.get("control")
        if not isinstance(control, Mapping) or ending.get("status") != "completed":
            settled: ToolTaskResult = {"kind": "model", "entryId": entry["id"]}
        else:
            settled = {
                "kind": "model",
                "entryId": entry["id"],
                "control": cast(ToolControl, copy_json(dict(control))),
            }
        remove_nested_slots(live, runtime.task_id)
        if ending.get("status") == "aborted":
            return {"status": "terminal", "outcome": {"status": "aborted", "result": settled}}
        if ending.get("status") == "failed":
            error = {"message": ending.get("message", "")}
            return {
                "status": "terminal",
                "outcome": {"status": "failed", "error": error, "result": settled},
            }
        return {
            "status": "terminal",
            "outcome": {"status": "completed", "result": settled},
        }

    await runtime.commit(finish, context)


async def append_tool_result(
    tx: Transaction,
    conversation_id: ConversationId,
    call: Mapping[str, Any],
    result: ToolExecutionResult,
    meta: Mapping[str, Any],
) -> Any:
    """Append a ``pi.tool-result`` entry whose content ends with the rendered diagnostics."""

    timestamp = meta.get("timestamp")
    duration_ms = meta.get("durationMs")
    diagnostics = list(result.get("diagnostics") or [])
    content: Content = list(result.get("output") or [])
    if diagnostics:
        content.append({"type": "text", "text": render_diagnostics(diagnostics)})
    message: dict[str, Any] = {
        "role": "toolResult",
        "toolCallId": call["id"],
        "toolName": call["name"],
        "content": content,
        "isError": bool(result.get("isError", False)),
        "timestamp": timestamp,
    }
    if "details" in result:
        message["details"] = result["details"]
    if "usage" in result:
        message["usage"] = result["usage"]
    if duration_ms is not None:
        message["durationMs"] = duration_ms
    if "usage" in result:
        usage = cast(Mapping[str, Any], result["usage"])
        await record_usage(tx, conversation_id, "tools", str(call["name"]), usage)
    return await tx.append_entry(
        ToolResultEntry,
        conversation_id,
        cast(
            EntryDraft,
            {
                "kind": ToolResultEntry.kind,
                "model": [cast(MessageRecord, copy_json(message))],
                "data": {"diagnostics": copy_json(diagnostics)},
            },
        ),
    )


def render_diagnostics(diagnostics: Sequence[ToolDiagnostic]) -> str:
    lines = "\n".join(f"[{item.get('severity')}] {item.get('message')}" for item in diagnostics)
    return f"<harness>\n{lines}\n</harness>"


def from_slot(slot: Any, code: str, message: str) -> ToolExecutionResult:
    """An error result from the slot's durable partial output, details, and diagnostics."""

    slot_diagnostics = (slot.get("diagnostics") if slot is not None else None) or ()
    diagnostics = [
        cast(ToolDiagnostic, copy_json(_plain(item))) for item in slot_diagnostics
    ]
    dropped_bytes = int((slot.get("droppedBytes") if slot is not None else 0) or 0)
    if dropped_bytes > 0:
        diagnostics.append(
            truncated(
                {
                    "dropped_bytes": dropped_bytes,
                    "dropped_lines": int((slot.get("droppedLines") if slot is not None else 0) or 0),
                }
            )
        )
    diagnostics.append(tool_diagnostic(code, message))
    output = slot.get("output") if slot is not None else None
    result: ToolExecutionResult = {
        "output": [] if not output else [{"type": "text", "text": output}],
        "isError": True,
        "diagnostics": diagnostics,
    }
    if slot is not None and "details" in slot:
        result["details"] = copy_json(_plain(slot.get("details")))
    return result


def _plain(value: Any) -> Any:
    """A detached plain JSON value from a document draft member."""

    from ..session.drafts import DocumentDraft, DraftList, DraftObject

    if isinstance(value, (DocumentDraft, DraftObject, DraftList)):
        return value.snapshot()
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def harness_error(code: str, message: str) -> ToolExecutionResult:
    """An error result the Harness writes itself: no content and one ``error`` diagnostic with ``code``."""

    return {"output": [], "isError": True, "diagnostics": [tool_diagnostic(code, message)]}


def tool_diagnostic(code: str, message: str) -> ToolDiagnostic:
    return {"severity": "error", "code": code, "message": message}


def truncated(dropped: Any, retain: str | None = None) -> ToolDiagnostic:
    """The Harness's truncation diagnostic; ``retain`` is unknown when rebuilt from a slot after recovery."""

    if retain is None:
        kept = ""
    else:
        kept = " to its beginning" if retain == "head" else " to its end"
    if isinstance(dropped, Mapping):
        lines = int(dropped.get("dropped_lines", dropped.get("droppedLines", 0)) or 0)
        bytes_ = int(dropped.get("dropped_bytes", dropped.get("droppedBytes", 0)) or 0)
    else:
        lines = int(getattr(dropped, "dropped_lines", 0) or 0)
        bytes_ = int(getattr(dropped, "dropped_bytes", 0) or 0)
    return {
        "severity": "warn",
        "code": "truncated",
        "message": f"Output truncated{kept}: {lines} lines, {bytes_} bytes dropped",
    }


@dataclass(frozen=True, slots=True)
class _BoundedContent:
    content: Content
    dropped_bytes: int
    dropped_lines: int


def bound_content(content: Content, limits: OutputLimits) -> _BoundedContent:
    """Bound the text of result content, keeping other content.

    When the joined text exceeds the limits, the text items are replaced by one bounded item at the
    position of the first (head) or last (tail) text item.
    """

    texts = [item for item in content if isinstance(item, Mapping) and item.get("type") == "text"]
    joined = "".join(cast(str, item.get("text", "")) for item in texts)
    bounded = bound_output(joined, limits)
    if bounded.dropped_bytes == 0:
        return _BoundedContent(content=content, dropped_bytes=0, dropped_lines=0)
    keep = texts[0] if limits.retain == "head" else texts[-1]
    result: Content = []
    for item in content:
        if not isinstance(item, Mapping) or item.get("type") != "text":
            result.append(item)
        elif item is keep:
            result.append({**item, "text": bounded.text})
    return _BoundedContent(
        content=result, dropped_bytes=bounded.dropped_bytes, dropped_lines=bounded.dropped_lines
    )


async def _call(handler: Callable[..., Any], *args: Any) -> Any:
    result = call_with_args(handler, *args)
    if hasattr(result, "__await__"):
        return await cast(Awaitable[Any], result)
    return result


def _reject(waiter: Any, error: BaseException) -> None:
    if not waiter.done():
        waiter.set_exception(error)
        waiter.exception()


__all__ = [
    "ToolTask",
    "ToolTaskCheckpoint",
    "ToolTaskInput",
    "ToolTaskResult",
    "append_tool_result",
    "bound_content",
    "entry_tool_calls",
    "final_result",
    "from_slot",
    "harness_error",
    "output_limits",
    "prepare",
    "publish_progress",
    "read_call",
    "render_diagnostics",
    "resolve_tool",
    "run",
    "settle",
    "tool_diagnostic",
    "truncated",
    "validate",
]

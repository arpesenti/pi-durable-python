"""Built-in ``pi.tool`` task: resolve, validate, hook, execute, and persist a tool call.

Port of v1.3.0 ``src/harness/tool.ts``. The phase handler resolves the called tool among its phase
agent's tools, repairs and validates the arguments, runs ``beforeTool``, records intent, executes
the tool, runs ``afterTool``, and settles the result — all in one handler so no phase boundary
separates resolution from settlement. ``execute`` is reached only by recovery and applies the replay
rule. A model-issued call settles by appending its ``pi.tool-result`` entry; a nested call, which a
running tool made through ``executeTool()``, writes its result into its caller's task-scoped
``NestedCallDoc`` and ends with a small receipt.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypeAlias, TypedDict, cast

from ..context import Context, await_with_context
from ..delta import OVERLAP_SCAN_LIMIT
from ..documents import define_doc_family
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
from .scheduler import json_equal
from .types import (
    NestedToolExecutionResult,
    ToolControl,
    ToolDiagnostic,
    ToolExecutionResult,
    ToolHookCall,
    ToolRegistration,
)
from .usage import record_usage
from .util import assign_json, call_with_args
from .validation import (
    ToolArgumentError,
    structured_output_error,
    validate_tool_arguments,
)


class ModelToolTaskInput(TypedDict):
    """A model-issued call, read from its assistant entry."""

    kind: Literal["model"]
    assistant: EntryId
    callId: str


class NestedToolTaskInput(TypedDict):
    """A nested call, whose call the input carries because no entry holds it."""

    kind: Literal["nested"]
    parent: TaskId
    parentCallId: str
    key: str
    call: dict[str, Any]
    progress: NotRequired[Literal[False]]


ToolTaskInput: TypeAlias = ModelToolTaskInput | NestedToolTaskInput
"""A model-issued call or a nested one a running tool made through ``executeTool()``."""


class NestedCallState(TypedDict, total=False):
    """One nested call of a running tool: its task, and once it ended, its result."""

    taskId: TaskId
    """Absent only inside the commit that creates the call; a creation that fails leaves no member."""
    result: dict[str, JsonValue]
    """Exactly what ``executeTool()`` returns; absent while running, and for a faulted or orphaned call."""


NestedCallDoc = define_doc_family(
    {
        "kind": "pi.tool.nested-call",
        "version": 1,
        "scope": "task",
        "family": True,
        "initial": lambda _seed: {},
    }
)
"""The nested calls of a tool call, a task-scoped family of the calling task keyed by the call's key.

One document per call, so admitting a call rewrites no other call's, and a rerun finds the calls it
already made. Created with the nested task; retires with the caller.
"""

MADE_NESTED_CALLS = "pi.tool.madeNestedCalls"
"""Memo of a tool task that made a nested call, committed before its first admission of each attempt."""

MAX_SUMMARY_ERROR_CHARS = 500
"""How much error text a nested call's slot summary keeps."""


class ExecuteCheckpoint(TypedDict):
    phase: Literal["execute"]
    arguments: dict[str, JsonValue]
    replay: Literal["safe", "unsafe"]


class CallCheckpoint(TypedDict):
    phase: Literal["call"]


ToolTaskCheckpoint = CallCheckpoint | ExecuteCheckpoint
"""Checkpoint shapes the tool phases read and write."""


class ModelToolTaskResult(TypedDict):
    """A model-issued call's result is its transcript entry; ``control`` only applies while completed."""

    kind: Literal["model"]
    entryId: EntryId
    control: NotRequired[ToolControl]


class NestedToolTaskResult(TypedDict):
    """A nested call's result lives in its caller's ``NestedCallDoc``, so its receipt stays small."""

    kind: Literal["nested"]


ToolTaskResult: TypeAlias = ModelToolTaskResult | NestedToolTaskResult


class Ending(TypedDict, total=False):
    """How a tool task ends; the result entry is appended either way."""

    status: Literal["completed", "aborted", "failed"]
    message: str


COMPLETED: Ending = {"status": "completed"}

Content = list[JsonValue]


async def _call_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    input = cast(ToolTaskInput, task["input"])
    call = await read_call(runtime, input, context)
    tool = await resolve_tool(runtime, input, call["name"], context)
    if tool is None:
        message = f"Tool {call['name']} is not available"
        await settle(
            runtime, input, call, COMPLETED, lambda _slot: harness_error("tool_unavailable", message), context
        )
        return
    prepared = prepare(tool, call.get("arguments") or {})
    if isinstance(prepared, str):
        error = invalid(prepared)
        await settle(runtime, input, call, COMPLETED, lambda _slot: error, context)
        return
    args = prepared
    checked = validate(tool, call, args)
    if isinstance(checked, str):
        error = invalid(checked)
        await settle(runtime, input, call, COMPLETED, lambda _slot: error, context)
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
            runtime, input, call, COMPLETED, lambda _slot: harness_error("blocked", blocked), context
        )
        return
    validated = validate(tool, call, args)
    if isinstance(validated, str):
        error = invalid(validated)
        await settle(runtime, input, call, COMPLETED, lambda _slot: error, context)
        return
    # Strict JSON: repair may set optional properties to None, which the slot and checkpoint cannot hold.
    final = cast(dict[str, JsonValue], copy_json(validated))

    async def intent(tx: Transaction) -> dict[str, Any]:
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        slot = tool_slot(live, runtime.task_id)
        if slot is not None:
            slot["status"] = "running"
        # A nested slot shows the arguments the call runs with, which repair, hooks, and coercion may
        # have changed. Compared with the input, which the slot got at admission: reading the draft
        # would track every leaf.
        changed = input["kind"] == "nested" and not json_equal(
            cast(Any, input["call"]).get("arguments") or {}, final
        )
        if changed and slot is not None and "parentTaskId" in slot:
            slot["arguments"] = final
        return {
            "status": "running",
            "checkpoint": {
                "phase": "execute",
                "arguments": final,
                "replay": tool.replay or "unsafe",
            },
        }

    await runtime.commit(intent, context)
    await run(runtime, input, call, tool, final, context)


async def _execute_phase(task: TaskRecord, runtime: Any, context: Context) -> None:
    input = cast(ToolTaskInput, task["input"])
    checkpoint = cast(ExecuteCheckpoint, task["state"]["checkpoint"])
    args = checkpoint["arguments"]
    replay = checkpoint["replay"]
    call = await read_call(runtime, input, context)
    tool = await resolve_tool(runtime, input, call["name"], context)
    if replay == "safe" and tool is not None and tool.replay == "safe":
        # The rerun reports from scratch; clear what the interrupted attempt published.
        async def clear(tx: Transaction) -> None:
            live = await tx.doc(LiveDoc, runtime.conversation_id)
            slot = tool_slot(live, runtime.task_id)
            if slot is not None:
                clear_progress(slot)

        await runtime.commit(clear, context)
        await run(runtime, input, call, tool, args, context)
        return
    message = f"Tool {call['name']} was interrupted and may have partially run"
    # ``failed`` records cancellation intent, so the call's owned conversations, left unsupervised,
    # are aborted.
    ending: Ending = {"status": "failed", "message": message}
    await settle(
        runtime,
        input,
        call,
        ending,
        lambda slot: from_slot(slot, "interrupted", message),
        context,
    )


async def _abort(task: TaskRecord, runtime: Any, context: Context) -> None:
    input = cast(ToolTaskInput, task["input"])
    call = await read_call(runtime, input, context)
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
        input,
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
    input: ToolTaskInput,
    name: str,
    context: Context,
) -> ToolRegistration | None:
    """The called tool: among those the model is offered, else among those tools may call."""

    agent = await runtime.agent(context)
    candidates = agent.tools if input["kind"] == "model" else agent.callable
    return next((tool for tool in candidates if tool.name == name), None)


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
    """The call: from the input for a nested call, which hooks see with its parent, else from its entry."""

    if input["kind"] == "nested":
        return cast(
            ToolHookCall,
            {
                **input["call"],
                "parent": {"taskId": input["parent"], "callId": input["parentCallId"]},
            },
        )
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
    streams = input["kind"] == "model" or input.get("progress") is not False
    progress = publish_progress(runtime, reported, streams, context)
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
    # No admission starts after ``ended``; let those underway commit, so settlement aborts and lists
    # their calls.
    await asyncio.gather(*api.admissions, return_exceptions=True)
    reported.output.end()
    # Details still waiting for a progress commit settle with the terminal commit, the final flush.
    pending = await progress.stop()
    try:
        settled = await final_result(runtime, input, call, tool, result, reported, context)
        await settle(runtime, input, call, ending, lambda _slot: settled, context, duration_ms)
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
        self.admissions: list[asyncio.Future[Any]] = []
        """Nested call admissions this invocation started; cleanup waits for them."""
        self._made_nested: asyncio.Future[Any] | None = None
        """This attempt's commit of the ``MADE_NESTED_CALLS`` memo."""
        self._sequence = 0
        """Default keys: the order of this invocation's calls, so a rerun calling in order reattaches."""
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

    def execute_tool(
        self,
        name: str,
        args: Mapping[str, Any],
        context: Context,
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[NestedToolExecutionResult]:
        """Run another tool as a nested call of this one and return its result.

        The admission is registered synchronously, as upstream's async function's synchronous prefix
        is, so a caller that starts a call without awaiting it still lets it commit before settling.
        Cancelling the returned future's wait stops only the wait: the call runs on, and its key
        returns its result.
        """

        self._assert_live()
        values = options or {}
        if values.get("key") is not None:
            check_key(str(values["key"]))
            key = str(values["key"])
        else:
            self._sequence += 1
            key = str(self._sequence)
        admission = asyncio.ensure_future(
            self._admit(name, args, key, values.get("progress"), context)
        )
        admission.add_done_callback(_consume)
        self.admissions.append(admission)
        waiter = asyncio.ensure_future(self._await_nested(name, key, admission, context))
        waiter.add_done_callback(_consume)
        return waiter

    async def _await_nested(
        self,
        name: str,
        key: str,
        admission: asyncio.Future[Any],
        context: Context,
    ) -> NestedToolExecutionResult:
        task_id = await await_with_context(admission, context)
        settled = await self._runtime.wait_for_task(task_id, context)
        stored = await self._runtime.snapshot(NestedCallDoc, self.task_id, key, context)
        value = None if stored is None else stored.get("result")
        if value is None:
            outcome = cast(Mapping[str, Any], settled["state"])["outcome"]
            return fallback_result(task_id, name, cast(Mapping[str, Any], outcome))
        # A copy the tool may change; a nested call the scheduler faulted or orphaned stored none.
        return cast(NestedToolExecutionResult, copy_json(value))

    async def _admit(
        self,
        name: str,
        args: Mapping[str, Any],
        key: str,
        progress: Any,
        context: Context,
    ) -> TaskId:
        # The memo first, once per attempt; a failed one is tried again by the next call.
        if self._made_nested is None:
            self._made_nested = asyncio.ensure_future(self._mark_made_nested(context))
        await self._made_nested
        return await start_nested_call(
            self._runtime,
            self.call_id,
            name,
            args,
            {
                "key": key,
                "progress": progress,
                "abandonOnRestart": not self._resumes,
            },
            context,
        )

    async def _mark_made_nested(self, context: Context) -> Any:
        try:
            return await self._runtime.memo(MADE_NESTED_CALLS, True, context)
        except BaseException:
            self._made_nested = None
            raise


_UNSET = object()


def publish_progress(
    runtime: Any, reported: Reported, enabled: bool, context: Context
) -> Progress:
    """Throttled commits of what the tool reported into its slot, each writing only what changed."""

    written: dict[str, Any] = {"text": "", "details": _UNSET, "diagnostics": 0}

    def write() -> Any:
        if not enabled:
            return 0
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
    input: ToolTaskInput,
    call: ToolHookCall,
    tool: ToolRegistration,
    result: ToolExecutionResult,
    reported: Reported,
    context: Context,
) -> ToolExecutionResult:
    """The settled result: fallbacks, ``afterTool``, bounds, and ``structuredOutput`` checked.

    A nested call whose ``structuredOutput`` breaks the contract gets an error result without it; a
    model-issued call only loses it, and the break is reported to the host.
    """

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
    broken = structured_output_error(tool, final)
    if broken is not None:
        final = cast(
            ToolExecutionResult,
            {key: value for key, value in final.items() if key != "structuredOutput"},
        )
        if input["kind"] == "nested":
            final = {**final, "isError": True}
            harness.append(tool_diagnostic("invalid_structured_output", broken))
        else:
            # The model never sees structured output, so its call stands; the host learns of the break.
            runtime.report(ValueError(broken))
    # Programs get the tool's own value: without a schema, its output. The model reads only ``output``,
    # so model-issued calls skip both.
    if input["kind"] == "nested" and tool.structured_output_schema is None:
        final = {**final, "structuredOutput": output_value(bounded.content)}
    # A schema tool's error result without structured output has its text only in the output, which
    # programs never get: it becomes an error diagnostic, unless one says what went wrong already.
    if (
        input["kind"] == "nested"
        and tool.structured_output_schema is not None
        and final.get("isError") is True
        and "structuredOutput" not in final
    ):
        diagnostics = [*(final.get("diagnostics") or []), *harness]
        if not any(item.get("severity") == "error" for item in diagnostics):
            text = "\n".join(text_items(bounded.content))
            if text != "":
                harness.append(tool_diagnostic("tool_error", text))
    return {
        **final,
        "output": bounded.content,
        "diagnostics": [*(final.get("diagnostics") or []), *harness],
    }


async def settle(
    runtime: Any,
    input: ToolTaskInput,
    call: ToolHookCall,
    ending: Ending,
    build: Callable[[Any], ToolExecutionResult],
    context: Context,
    duration_ms: int | None = None,
) -> None:
    """Commit the tool's terminal state and mark its slot done.

    A model-issued call appends its result entry; a nested call writes its result to its caller's
    ``NestedCallDoc``, records its usage, and ends with a small receipt. Nested calls this task left
    running are aborted first, so they report into their slots before the slots below it leave
    ``pi.live``. ``build`` receives the slot so interruption and abort report the durable partial.
    """

    # Every live nested call the task owns, also those an earlier attempt made and this one never
    # called again. Mark them all before waiting for any: one may run until a sibling is cancelled.
    owned = await runtime.owned_tasks(context)
    nested_ids = [
        record["id"]
        for record in owned
        if record["kind"] == TOOL_TASK_KIND
        and cast(Any, record["input"]).get("kind") == "nested"
    ]
    # Read before the settling commit: no admission starts after the call ended, and those underway
    # were awaited.
    made_nested = await runtime.memo(MADE_NESTED_CALLS, context) is True
    if nested_ids:
        await asyncio.gather(*(runtime.abort_owned(id, context) for id in nested_ids))

    async def finish(tx: Transaction) -> dict[str, Any]:
        conversation_id = runtime.conversation_id
        live = await tx.doc(LiveDoc, conversation_id)
        slot = tool_slot(live, runtime.task_id)
        result = build(slot)
        settled: ToolTaskResult
        if input["kind"] == "nested":
            nested = nested_result(runtime.task_id, result, duration_ms)
            usage = nested.get("usage")
            if usage is not None:
                await record_usage(
                    tx, conversation_id, "tools", str(call["name"]), cast(Mapping[str, Any], usage)
                )
            # The caller is never terminal before its owned nested calls, so its document is open;
            # another task in it is a bug, not a race.
            state = await tx.doc(NestedCallDoc, input["parent"], input["key"], None)
            if state.get("taskId") != runtime.task_id:
                raise RuntimeError(f"Nested call {call['id']} is not in its caller's index")
            state["result"] = cast(Any, nested)
            if slot is not None and "parentTaskId" in slot:
                finish_slot(slot)
                slot["summary"] = summary_of(nested, cast(Content, result.get("output") or []))
            settled = {"kind": "nested"}
        else:
            entry = await append_tool_result(
                tx,
                conversation_id,
                call,
                result,
                {"timestamp": runtime.now(), "durationMs": duration_ms},
            )
            if slot is not None and "parentTaskId" not in slot:
                finish_slot(slot)
                slot["entry"] = entry["id"]
            control = result.get("control")
            if not isinstance(control, Mapping) or ending.get("status") != "completed":
                settled = {"kind": "model", "entryId": entry["id"]}
            else:
                settled = {
                    "kind": "model",
                    "entryId": entry["id"],
                    "control": cast(ToolControl, copy_json(dict(control))),
                }
        # A call that made no nested calls, in any attempt, has no slots below it; skipping the scan
        # keeps many leaves linear. Committed before the read: no admission starts after the call ended.
        if made_nested:
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


async def start_nested_call(
    runtime: Any,
    parent_call_id: str,
    name: str,
    args: Mapping[str, Any],
    options: Mapping[str, Any],
    context: Context,
) -> TaskId:
    """Create nested call ``key`` of ``parent_call_id`` in one commit.

    The commit holds the nested task, its slot, and its ``NestedCallDoc`` member. A key already made
    returns that call's task when it names the same tool and arguments.
    """

    key = str(options["key"])
    call: dict[str, Any] = {
        "type": "toolCall",
        "id": f"{parent_call_id}/{key}",
        "name": name,
        "arguments": copy_json(dict(args)),
    }
    box: dict[str, TaskId] = {}

    async def admit(tx: Transaction) -> None:
        # Opened, or created empty for a key not made yet.
        state = await tx.doc(NestedCallDoc, runtime.task_id, key, None)
        existing = state.get("taskId")
        if existing is not None:
            record = await tx.task(existing)
            input = None if record is None else record["input"]
            same = (
                isinstance(input, Mapping)
                and input.get("kind") == "nested"
                and cast(Any, input.get("call")) is not None
                and cast(Mapping[str, Any], input["call"])["name"] == name
                and json_equal(
                    cast(Any, cast(Mapping[str, Any], input["call"])).get("arguments"),
                    call["arguments"],
                )
            )
            if not same:
                raise RuntimeError(
                    f"Nested call {call['id']} was already made with another tool or other arguments"
                )
            box["id"] = existing
            return
        nested_input: dict[str, Any] = {
            "kind": "nested",
            "parent": runtime.task_id,
            "parentCallId": parent_call_id,
            "key": key,
            "call": call,
        }
        if options.get("progress") is False:
            nested_input["progress"] = False
        created = await tx.create_task(
            ToolTask,
            cast(Any, nested_input),
            {
                "ownership": {"kind": "task", "taskId": runtime.task_id},
                "abandonOnRestart": bool(options.get("abandonOnRestart")),
            },
        )
        state["taskId"] = created
        live = await tx.doc(LiveDoc, runtime.conversation_id)
        slot = {
            "callId": call["id"],
            "parentCallId": parent_call_id,
            "parentTaskId": runtime.task_id,
            "name": name,
            "taskId": created,
            "arguments": call["arguments"],
            "status": "pending",
        }
        existing_slots = live.get("nestedTools")
        if existing_slots is None:
            live["nestedTools"] = [slot]
        else:
            existing_slots.append(slot)
        box["id"] = created

    await runtime.commit(admit, context)
    return box["id"]


def nested_result(
    task_id: TaskId, result: ToolExecutionResult, duration_ms: int | None
) -> dict[str, Any]:
    """A nested call's result, as stored and returned: strict JSON, without ``output`` or ``control``."""

    nested: dict[str, Any] = {"taskId": task_id}
    if "structuredOutput" in result:
        nested["structuredOutput"] = result["structuredOutput"]
    nested["isError"] = bool(result.get("isError", False))
    if "details" in result:
        nested["details"] = result["details"]
    nested["diagnostics"] = list(result.get("diagnostics") or [])
    if "usage" in result:
        nested["usage"] = result["usage"]
    if duration_ms is not None:
        nested["durationMs"] = duration_ms
    return cast(dict[str, Any], copy_json(nested))


def text_items(content: Sequence[Any]) -> list[str]:
    """The text of every text item in result content, in order."""

    return [
        cast(str, item.get("text", ""))
        for item in content
        if isinstance(item, Mapping) and item.get("type") == "text"
    ]


def output_value(output: Content) -> JsonValue:
    """What programs get of a schema-less tool's output: a text, an image, ``""``, or the list."""

    if len(output) == 0:
        return ""
    if len(output) > 1:
        return output
    only = output[0]
    return only.get("text", "") if isinstance(only, Mapping) and only.get("type") == "text" else only


def fallback_result(
    task_id: TaskId, name: str, outcome: Mapping[str, Any]
) -> NestedToolExecutionResult:
    """What the caller gets for a nested call without a stored result: a faulted or orphaned one."""

    status = str(outcome.get("status"))
    if status == "faulted":
        error = cast(Mapping[str, Any], outcome.get("error") or {})
        message = f"Tool {name} failed: {error_message(error)}"
    elif status == "orphaned":
        message = f"Tool {name} could not resume: {outcome.get('reason')}"
    else:
        message = f"Tool {name} ended without a result"
    error_result = harness_error(status, message)
    return cast(
        NestedToolExecutionResult,
        {
            "taskId": task_id,
            "isError": True,
            "diagnostics": list(error_result.get("diagnostics") or []),
        },
    )


def summary_of(result: Mapping[str, Any], output: Content) -> dict[str, Any]:
    """How a nested call ended, for its slot: error text, from its diagnostics or else its output."""

    error = error_text_of(result, output)[:MAX_SUMMARY_ERROR_CHARS] if result.get("isError") else ""
    summary: dict[str, Any] = {"isError": bool(result.get("isError"))}
    if result.get("durationMs") is not None:
        summary["durationMs"] = result["durationMs"]
    if result.get("usage") is not None:
        summary["usage"] = result["usage"]
    if error != "":
        summary["error"] = error
    return summary


def error_text_of(result: Mapping[str, Any], output: Content) -> str:
    """The error messages of a nested result, else the text items of its output, joined with newlines."""

    errors = [
        cast(Mapping[str, Any], item)
        for item in cast(Sequence[Any], result.get("diagnostics") or ())
        if isinstance(item, Mapping) and item.get("severity") == "error"
    ]
    if errors:
        return "\n".join(str(item.get("message")) for item in errors)
    return "\n".join(text_items(output))


def check_key(key: str) -> None:
    """Explicit keys are path segments of the call ID, so IDs never collide."""

    if key == "" or "/" in key or key == "__proto__" or re.fullmatch(r"[1-9][0-9]*", key):
        raise ValueError(
            f'Nested call key {json.dumps(key)} must be non-empty, without "/", not "__proto__", '
            "and not a positive integer"
        )


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
    joined = "".join(text_items(content))
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


def _consume(future: asyncio.Future[Any]) -> None:
    """Observe a rejection a caller may ignore, so it raises only when awaited."""

    if not future.cancelled():
        future.exception()


__all__ = [
    "MADE_NESTED_CALLS",
    "ModelToolTaskInput",
    "ModelToolTaskResult",
    "NestedCallDoc",
    "NestedCallState",
    "NestedToolTaskInput",
    "NestedToolTaskResult",
    "ToolTask",
    "ToolTaskCheckpoint",
    "ToolTaskInput",
    "ToolTaskResult",
    "append_tool_result",
    "bound_content",
    "check_key",
    "entry_tool_calls",
    "fallback_result",
    "final_result",
    "from_slot",
    "harness_error",
    "nested_result",
    "output_limits",
    "output_value",
    "prepare",
    "publish_progress",
    "read_call",
    "render_diagnostics",
    "resolve_tool",
    "run",
    "settle",
    "start_nested_call",
    "summary_of",
    "text_items",
    "tool_diagnostic",
    "truncated",
    "validate",
]

"""Experimental agent events: committed conversation changes as coding-agent events (spec §9.4).

``watch_events()`` translates one conversation's committed publications into batches shaped like
the coding agent's session events. It owns no tracker or persistence: every event follows the
commit that makes it true. The stream buffers at most 100 undelivered batches and replaces them
with one ``snapshot`` beyond that, so it converges without promising every transition.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, Literal, NotRequired, Protocol, TypeAlias, TypedDict, cast

from ..context import Context
from ..delta import Op
from ..ids import ConversationId, SubmissionId, TaskId
from ..session.observation import CommittedWatch
from ..types import (
    CommitPublication,
    EntryRecord,
    Storage,
    SubmissionRecord,
)
from .agent import AgentDoc
from .inbox import InboxState
from .live import CompactionFailure, CompactionStatus, LiveState, NestedToolSlot, ToolSlot
from .scheduler import json_equal
from .tool import NestedCallDoc, NestedCallState
from .types import (
    AgentState,
    NestedToolExecutionResult,
)
from .usage import UsageDoc, UsageState
from .util import cancellation_error, scan_all
from .view import ConversationView, ConversationViews

if TYPE_CHECKING:
    from .harness import Harness


class QueuedItem(TypedDict):
    """One queued submission as the agent events show it."""

    id: SubmissionId
    mode: Literal["steer", "followUp", "write"]


class SnapshotEvent(TypedDict):
    """Everything a consumer should hold, captured atomically at attachment."""

    type: Literal["snapshot"]
    entries: list[EntryRecord]
    run: NotRequired[dict[str, list[SubmissionId]]]
    generation: NotRequired[dict[str, Any]]
    tools: list[ToolSlot]
    nestedTools: list[NestedToolSlot]
    compactions: list[CompactionStatus]
    compactionFailure: NotRequired[CompactionFailure]
    inbox: list[QueuedItem]
    agent: AgentState
    usage: UsageState


class ToolEventCall(TypedDict):
    """Which call a tool event is about."""

    toolCallId: str
    toolName: str
    taskId: NotRequired[TaskId]
    parentToolCallId: NotRequired[str]
    parentTaskId: NotRequired[TaskId]


MessageChange: TypeAlias = dict[str, Any]
"""One change to the in-flight assistant message, relative to that message."""

AgentEvent: TypeAlias = dict[str, Any]
"""One translated event; every variant carries ``type`` in the coding agent's vocabulary."""


class AgentEventStream(Protocol):
    """Serialized stream of one conversation's event batches, one per commit."""

    @property
    def snapshot(self) -> SnapshotEvent: ...

    def start(self, listener: Callable[[list[AgentEvent], Context], Awaitable[None] | None]) -> None: ...

    def stop(self) -> Any: ...

    @property
    def closed(self) -> Any: ...


class _EventStream:
    """One attached stream: its snapshot, its watch, and the delivery adapter."""

    __slots__ = ("_snapshot", "_watch")

    def __init__(self, snapshot: SnapshotEvent, watch: CommittedWatch) -> None:
        self._snapshot = snapshot
        self._watch = watch

    @property
    def snapshot(self) -> SnapshotEvent:
        return self._snapshot

    def start(
        self, listener: Callable[[list[AgentEvent], Context], Awaitable[None] | None]
    ) -> None:
        def deliver(events: list[AgentEvent], _ops: list[Op], context: Context) -> Any:
            return listener(events, context)

        self._watch.start(deliver)

    def stop(self) -> Any:
        return self._watch.stop()

    @property
    def closed(self) -> Any:
        return self._watch.closed


class _EventsObserver:
    """The view observer that turns each publication into one event batch."""

    __slots__ = ("_conversation_id", "_current", "_held", "_watch")

    def __init__(
        self,
        conversation_id: ConversationId,
        current: list[ConversationView],
        held: set[TaskId],
        watch: CommittedWatch,
    ) -> None:
        self._conversation_id = conversation_id
        self._current = current
        self._held = held
        self._watch = watch

    def publication(
        self,
        before: ConversationView,
        after: ConversationView,
        ops: list[Op],
        publication: CommitPublication,
        context: Context,
    ) -> None:
        self._current[0] = after
        events = translate(
            self._conversation_id, before, after, ops, publication, self._held
        )
        if events:
            self._watch.advance_frame(events, [], context)

    def close_session(self, failure: BaseException | None) -> None:
        self._watch.close_session(failure)


async def watch_events(
    harness: Harness,
    conversation_id: ConversationId,
    context: Context,
) -> AgentEventStream:
    """Attach to one conversation's agent events (spec §9.4)."""

    views: ConversationViews = harness._views
    holder: dict[str, Any] = {}

    async def create(
        initial: ConversationView,
        release: Callable[[], None],
        storage: Storage,
        attached_views: ConversationViews,
    ) -> _EventsObserver:
        # Generations whose held outcome already ended their turn, read on the line with the snapshot.
        completing = await scan_all(
            lambda cursor: storage.scan_tasks(
                {
                    "conversationId": conversation_id,
                    "kind": "pi.generation",
                    "status": "completing",
                },
                100,
                cursor,
                context,
            )
        )
        held = {record["id"] for record in completing}
        current = [initial]
        snapshot = _snapshot_of(initial)
        watch = CommittedWatch(
            [],
            release,
            attached_views.report,
            replace=lambda: [_snapshot_of(current[0])],
        )
        holder["snapshot"] = snapshot
        holder["watch"] = watch
        return _EventsObserver(conversation_id, current, held, watch)

    await views.attach(conversation_id, create, context)
    watch = holder["watch"]
    signal = context.abort_signal
    if signal is not None and signal.aborted:
        watch.cancel()
        raise cancellation_error(signal)
    if signal is not None:
        watch.observe_cancellation(signal)
    return _EventStream(holder["snapshot"], watch)


def _parts(view: ConversationView) -> dict[str, Any]:
    docs = view["docs"]
    return {
        "live": docs.get("pi.live") or {},
        "inbox": docs.get("pi.inbox"),
        "agent": docs.get("pi.agent"),
        "usage": docs.get("pi.usage"),
    }


def _snapshot_of(view: ConversationView) -> SnapshotEvent:
    parts = _parts(view)
    live: LiveState = parts["live"]
    snapshot = SnapshotEvent(
        type="snapshot",
        entries=list(view["entries"]),
        tools=list(live.get("tools") or ()),
        nestedTools=list(live.get("nestedTools") or ()),
        compactions=list(live.get("compactions") or ()),
        inbox=_queued(parts["inbox"]),
        agent=cast(
            AgentState,
            parts["agent"] if parts["agent"] is not None else AgentDoc.definition.initial(),
        ),
        usage=cast(
            UsageState,
            parts["usage"] if parts["usage"] is not None else UsageDoc.definition.initial(),
        ),
    )
    run = live.get("run")
    if run is not None:
        snapshot["run"] = {"inputs": list(run["inputs"])}
        # A consumer holding the snapshot must not be able to mutate the committed document.
    generation = live.get("generation")
    if generation is not None:
        snapshot["generation"] = generation
    failure = live.get("compactionFailure")
    if failure is not None:
        snapshot["compactionFailure"] = failure
    return snapshot


def _queued(inbox: InboxState | None) -> list[QueuedItem]:
    items = inbox["items"] if inbox is not None else []
    return [{"id": item["id"], "mode": item["mode"]} for item in items]


def _result_of(entries: list[EntryRecord], call_id: str) -> EntryRecord | None:
    for entry in entries:
        model = entry.get("model")
        message = model[0] if model else None
        if (
            message is not None
            and message.get("role") == "toolResult"
            and message.get("toolCallId") == call_id
        ):
            return entry
    return None


def translate(
    conversation_id: ConversationId,
    before: ConversationView,
    after: ConversationView,
    view_ops: list[Op],
    publication: CommitPublication,
    held: set[TaskId],
) -> list[AgentEvent]:
    """Every event one publication causes, in the order of spec §9.4."""

    entries: list[EntryRecord] = []
    tasks: dict[TaskId, Any] = {}
    submissions: list[SubmissionRecord] = []
    # Nested results this commit stored, by nested task ID; read now, since the caller's
    # documents may retire next.
    nested_results: dict[TaskId, NestedToolExecutionResult] = {}
    for change in publication["changes"]:
        if (
            change["type"] == "document"
            and change["record"]["kind"] == NestedCallDoc.definition.kind
            and change["value"] is not None
            and change["conversationId"] == conversation_id
        ):
            nested_state = cast(NestedCallState, change["value"])
            task_id = nested_state.get("taskId")
            result = cast(NestedToolExecutionResult | None, nested_state.get("result"))
            if task_id is not None and result is not None:
                nested_results[task_id] = result
        if change["type"] == "entry" and change["value"]["conversationId"] == conversation_id:
            entries.append(change["value"])
        if change["type"] == "task" and change["value"]["conversationId"] == conversation_id:
            tasks[change["value"]["id"]] = change["value"]
        if change["type"] == "submission" and change["value"]["conversationId"] == conversation_id:
            submissions.append(change["value"])
    if not view_ops and not entries and not tasks and not submissions:
        return []
    # Entries are appended in ID order; submission records are published in the order the commit touched them.
    submissions.sort(key=lambda record: record["id"])
    was = _parts(before)
    now = _parts(after)
    events: list[AgentEvent] = []

    # Progress: tool starts, the in-flight message, tool updates, retry and deferred state.
    slots_before: dict[str, Any] = {
        str(slot["callId"]): slot for slot in (was["live"].get("tools") or ())
    }
    slots: list[Any] = list(now["live"].get("tools") or ())
    nested_before: dict[TaskId, Any] = {
        cast(TaskId, slot["taskId"]): slot
        for slot in (was["live"].get("nestedTools") or ())
    }
    nested: list[Any] = list(now["live"].get("nestedTools") or ())
    for candidate in [*slots, *nested]:
        previous_slot = (
            nested_before.get(candidate["taskId"])
            if "parentCallId" in candidate
            else slots_before.get(candidate["callId"])
        )
        if candidate.get("status") != "running" or (previous_slot or {}).get("status") == "running":
            continue
        # A nested slot carries the arguments the call runs with; a model-issued call's are in
        # its intent checkpoint.
        task_id = candidate.get("taskId")
        task = tasks.get(task_id) if task_id is not None else None
        if "parentCallId" in candidate:
            args = candidate.get("arguments") or {}
        else:
            record = cast(dict[str, Any], task or {})
            checkpoint = cast(dict[str, Any], record.get("state") or {}).get("checkpoint") or {}
            args = checkpoint.get("arguments") or {}
        events.append({"type": "tool_execution_start", **_call_of(candidate), "args": args})

    partial_before = (was["live"].get("generation") or {}).get("message")
    partial = (now["live"].get("generation") or {}).get("message")
    if partial is not None and partial_before is None:
        events.append({"type": "message_start", "message": partial})
    elif partial is not None and not json_equal(partial, partial_before):
        events.append(
            {
                "type": "message_update",
                "usage": partial.get("usage") or {},
                "changes": _message_changes(view_ops, partial),
            }
        )
    for index, current_slot in enumerate(slots):
        previous_slot = slots_before.get(current_slot["callId"])
        if (
            current_slot.get("status") != "running"
            or (previous_slot or {}).get("status") != "running"
        ):
            continue
        update = _tool_update(view_ops, ["tools", index], current_slot, previous_slot or {})
        if update is not None:
            events.append(
                {"type": "tool_execution_update", **_call_of(current_slot), **update}
            )
    for index, current_nested in enumerate(nested):
        previous_nested = nested_before.get(current_nested["taskId"])
        if (
            current_nested.get("status") != "running"
            or (previous_nested or {}).get("status") != "running"
        ):
            continue
        update = _tool_update(
            view_ops, ["nestedTools", index], current_nested, previous_nested or {}
        )
        if update is not None:
            events.append(
                {"type": "tool_execution_update", **_call_of(current_nested), **update}
            )

    generation = now["live"].get("generation")
    generation_before = was["live"].get("generation")
    if (
        generation is not None
        and generation.get("retry") is not None
        and (generation_before or {}).get("retry") is None
    ):
        retry = generation["retry"]
        events.append(
            {
                "type": "auto_retry_start",
                "attempt": generation.get("attempt"),
                "at": retry["at"],
                "errorMessage": retry["error"],
            }
        )
    if (
        generation_before is not None
        and generation_before.get("retry") is not None
        and (generation or {}).get("retry") is None
    ):
        events.append({"type": "auto_retry_end", "attempt": generation_before.get("attempt")})
    deferred = (generation or {}).get("deferred")
    deferred_before = (generation_before or {}).get("deferred") or {}
    if deferred is not None and deferred.get("pollAt") != deferred_before.get("pollAt"):
        events.append({"type": "deferred_poll", "pollAt": deferred["pollAt"]})

    # Tools that end in this commit: a slot that becomes done, one created done, or an unfinished one
    # that vanishes because its run ended. A done slot that vanishes ended earlier.
    tool_ends: list[AgentEvent] = []

    def end_tool(slot: Any, entry_id: int | None) -> None:
        entry = next((entry for entry in entries if entry["id"] == entry_id), None)
        tool_ends.append(
            {
                "type": "tool_execution_end",
                **_call_of(slot),
                **({} if entry is None else {"entry": entry}),
            }
        )

    for previous_slot in slots_before.values():
        if previous_slot.get("status") == "done":
            continue
        matching = next(
            (
                candidate
                for candidate in slots
                if candidate["callId"] == previous_slot["callId"]
            ),
            None,
        )
        if matching is not None and matching.get("status") == "done":
            end_tool(matching, matching.get("entry"))
        elif matching is None:
            result_entry = _result_of(entries, previous_slot["callId"])
            end_tool(previous_slot, None if result_entry is None else result_entry["id"])
    for current_slot in slots:
        if (
            current_slot.get("status") == "done"
            and current_slot["callId"] not in slots_before
        ):
            end_tool(current_slot, current_slot.get("entry"))

    # Nested calls that end in this commit, the same way, children before the calls that made them:
    # the lists hold parents first, so walk them backwards.
    def end_nested(slot: Any) -> None:
        result = nested_results.get(cast(TaskId, slot["taskId"]))
        events.append(
            {
                "type": "tool_execution_end",
                **_call_of(slot),
                **({} if result is None else {"result": result}),
            }
        )

    nested_now: dict[TaskId, Any] = {
        cast(TaskId, slot["taskId"]): slot for slot in nested
    }
    for previous_nested in reversed(list(nested_before.values())):
        if previous_nested.get("status") == "done":
            continue
        matching_nested = nested_now.get(previous_nested["taskId"])
        if matching_nested is not None and matching_nested.get("status") == "done":
            end_nested(matching_nested)
        elif matching_nested is None:
            end_nested(previous_nested)
    for current_nested in reversed(nested):
        if (
            current_nested.get("status") == "done"
            and current_nested["taskId"] not in nested_before
        ):
            end_nested(current_nested)

    # Entries in append order; a tool's end directly precedes its result's message, as in the coding agent.
    assistant_appended = False
    for entry in entries:
        for end in tool_ends:
            if end.get("entry") is entry:
                events.append(end)
        model = entry.get("model")
        message = model[0] if model else None
        if message is None:
            events.append({"type": "entry_appended", "entry": entry})
            continue
        # A streamed answer already started with its first partial.
        streamed = (
            message.get("role") == "assistant"
            and partial_before is not None
            and not assistant_appended
        )
        if message.get("role") == "assistant":
            assistant_appended = True
        if not streamed:
            events.append({"type": "message_start", "message": message})
        events.append({"type": "message_end", "entry": entry})
    # Ends without a result entry: a faulted or orphaned tool, or one whose run ended.
    for end in tool_ends:
        if "entry" not in end:
            events.append(end)

    # Compaction ends, task failures, then turn and run ends.
    compactions_before: list[Any] = list(was["live"].get("compactions") or ())
    compactions: list[Any] = list(now["live"].get("compactions") or ())
    failure: Any = now["live"].get("compactionFailure")
    for status in compactions_before:
        if not any(current["taskId"] == status["taskId"] for current in compactions):
            error = (
                failure["error"]
                if failure is not None and failure["taskId"] == status["taskId"]
                else None
            )
            events.append(
                {
                    "type": "compaction_end",
                    "taskId": status["taskId"],
                    "reason": status["reason"],
                    **({} if error is None else {"error": error}),
                }
            )
    # A generation's turn ends when its outcome is committed: at a `completing` hold or at terminal,
    # whichever comes first, so a successor created at the hold starts after it.
    turn_ended = False
    for task_record in tasks.values():
        task_state = task_record["state"]
        status = task_state["status"]
        if (
            task_record["kind"] == "pi.generation"
            and status == "completing"
            and task_record["id"] not in held
        ):
            held.add(task_record["id"])
            turn_ended = True
        if status != "terminal":
            continue
        if task_record["kind"] == "pi.generation":
            if task_record["id"] in held:
                held.discard(task_record["id"])
            else:
                turn_ended = True
        outcome = task_state["outcome"]
        if outcome["status"] in ("faulted", "orphaned"):
            message = (
                outcome["error"]["message"]
                if outcome["status"] == "faulted"
                else outcome["reason"]
            )
            events.append(
                {
                    "type": "task_failed",
                    "taskId": task_record["id"],
                    "kind": task_record["kind"],
                    "message": message,
                }
            )
    if turn_ended:
        events.append({"type": "turn_end"})
    run = now["live"].get("run")
    run_before = was["live"].get("run")
    run_changed = (run or {}).get("inputs", [None])[0] != (run_before or {}).get("inputs", [None])[0]
    if run_before is not None and run_changed:
        events.append({"type": "run_end", "inputs": list(run_before["inputs"])})

    # Submissions, document state, then what began.
    for submission_record in submissions:
        events.append({"type": "submission", "record": submission_record})
    if now["inbox"] is not was["inbox"]:
        events.append({"type": "inbox_update", "items": _queued(now["inbox"])})
    # A retired document reads as its initial value, as in a snapshot.
    if now["agent"] is not was["agent"]:
        events.append(
            {
                "type": "agent_changed",
                "agent": now["agent"] if now["agent"] is not None else AgentDoc.definition.initial(),
            }
        )
    if now["usage"] is not was["usage"]:
        events.append(
            {
                "type": "usage_changed",
                "usage": now["usage"] if now["usage"] is not None else UsageDoc.definition.initial(),
            }
        )
    for status in compactions:
        if not any(current["taskId"] == status["taskId"] for current in compactions_before):
            events.append(
                {
                    "type": "compaction_start",
                    "taskId": status["taskId"],
                    "reason": status["reason"],
                    "blocking": status.get("blocking", False),
                }
            )
    if run is not None and run_changed:
        events.append({"type": "run_start", "inputs": list(run["inputs"])})
    if (
        run is not None
        and run.get("taskId") != (run_before or {}).get("taskId")
        and (tasks.get(run["taskId"]) or {}).get("kind") == "pi.generation"
    ):
        events.append({"type": "turn_start"})
    return events


PARTIAL_PATH = ["docs", "pi.live", "generation", "message"]


def _message_changes(view_ops: list[Op], message: dict[str, Any]) -> list[MessageChange]:
    """Translate the view operations on the in-flight message into message changes."""

    changes: list[MessageChange] = []
    # A block sent whole already holds every later change to it in this batch.
    whole: set[int] = set()
    for op in view_ops:
        if op[0] == "r":
            continue
        path = op[1]
        if not _starts_with(path, PARTIAL_PATH):
            # The whole message or generation was replaced.
            if _starts_with(PARTIAL_PATH, path):
                return [{"type": "message", "message": message}]
            continue
        rest = path[len(PARTIAL_PATH):]
        if not rest:
            return [{"type": "message", "message": message}]
        if rest[0] == "usage":
            continue
        if rest[0] != "content":
            return [{"type": "message", "message": message}]
        if len(rest) == 1:
            if op[0] != "p" or op[3] != 0:
                return [{"type": "message", "message": message}]
            for offset, block in enumerate(op[4]):
                kind = (
                    "text_start"
                    if block.get("type") == "text"
                    else "thinking_start"
                    if block.get("type") == "thinking"
                    else "toolcall_start"
                )
                changes.append(
                    {"type": kind, "contentIndex": op[2] + offset, "block": block}
                )
            continue
        content_index = rest[1]
        field = rest[2] if len(rest) > 2 else None
        if content_index in whole:
            continue
        if op[0] == "a" and len(rest) == 3 and field in ("text", "thinking"):
            changes.append(
                {
                    "type": "text_delta" if field == "text" else "thinking_delta",
                    "contentIndex": content_index,
                    "delta": op[2],
                }
            )
        elif op[0] == "a" and field == "arguments":
            changes.append(
                {
                    "type": "toolcall_delta",
                    "contentIndex": content_index,
                    "path": rest[3:],
                    "delta": op[2],
                }
            )
        else:
            whole.add(content_index)
            changes.append(
                {
                    "type": "block",
                    "contentIndex": content_index,
                    "block": message["content"][content_index],
                }
            )
    return changes


def _call_of(slot: Any) -> ToolEventCall:
    """The identifying fields of a slot's tool events."""

    call = ToolEventCall(toolCallId=slot["callId"], toolName=slot["name"])
    task_id = slot.get("taskId")
    if task_id is not None:
        call["taskId"] = task_id
    if "parentCallId" in slot:
        call["parentToolCallId"] = slot["parentCallId"]
        call["parentTaskId"] = slot["parentTaskId"]
    return call


_MISSING = object()


def _tool_update(
    view_ops: list[Op],
    at: list[Any],
    slot: Any,
    previous: Any,
) -> dict[str, Any] | None:
    """Output, details, and diagnostics changes of a running slot at ``at`` in ``pi.live``."""

    output_path = ["docs", "pi.live", *at, "output"]
    trim_start = 0
    append = ""
    replaced = False
    for op in view_ops:
        if op[0] == "r" or not _starts_with(op[1], output_path):
            continue
        if op[0] == "t":
            trim_start += op[2]
        elif op[0] == "a":
            append += op[2]
        else:
            replaced = True
    output: dict[str, Any] | None = None
    current_output = slot.get("output")
    if replaced or (
        not json_equal(current_output, previous.get("output"))
        and trim_start == 0
        and append == ""
    ):
        output = {"set": current_output or ""}
    elif trim_start > 0 or append != "":
        output = {}
        if trim_start > 0:
            output["trimStart"] = trim_start
        if append != "":
            output["append"] = append
    update: dict[str, Any] = {}
    if output is not None:
        update["output"] = output
    # A safe replay clears a running slot's progress: removed details send `null`, removed diagnostics `[]`.
    current = slot.get("details", _MISSING)
    prior = previous.get("details", _MISSING)
    if current is _MISSING or prior is _MISSING:
        if current is not prior:
            update["details"] = None if current is _MISSING else current
    elif not json_equal(current, prior):
        update["details"] = current
    current_diagnostics = slot.get("diagnostics", _MISSING)
    prior_diagnostics = previous.get("diagnostics", _MISSING)
    if current_diagnostics is _MISSING or prior_diagnostics is _MISSING:
        if current_diagnostics is not prior_diagnostics:
            update["diagnostics"] = (
                [] if current_diagnostics is _MISSING else current_diagnostics
            )
    elif not json_equal(current_diagnostics, prior_diagnostics):
        update["diagnostics"] = current_diagnostics
    return update or None


def _starts_with(path: Any, prefix: list[Any]) -> bool:
    return isinstance(path, list) and len(prefix) <= len(path) and path[: len(prefix)] == prefix


__all__ = [
    "AgentEvent",
    "AgentEventStream",
    "MessageChange",
    "SnapshotEvent",
    "ToolEventCall",
    "watch_events",
]

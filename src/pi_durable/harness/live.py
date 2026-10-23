"""Built-in ``pi.live``: run control and presentation of current work (spec section 8.2).

``run`` is present exactly while a run owns the conversation. Generation publishes its attempt,
partial, retry, and deferred state here; a tool call publishes throttled running output into its
slot, model-issued or nested. A compaction task lists its status here while it selects, summarizes,
or waits out a retry backoff, and the newest failure stays for a client to show.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, NotRequired, TypedDict, cast

from ..documents import define_doc
from ..errors import error_message
from ..ids import ConversationId, EntryId, SubmissionId, TaskId
from ..json import JsonValue
from ..session.drafts import DocumentDraft
from ..session.transaction import Transaction
from ..types import SubmissionSettlement, TaskRecord
from .types import CompactionReason, ToolDiagnostic


class RunState(TypedDict):
    """The task that settles the run's inputs and those inputs; present exactly while busy."""

    taskId: TaskId
    inputs: list[SubmissionId]


class SlotProgress(TypedDict, total=False):
    """What a running tool call publishes, shared by model-issued and nested calls."""

    callId: str
    name: str
    status: str
    output: str
    droppedBytes: int
    droppedLines: int
    details: JsonValue
    diagnostics: list[ToolDiagnostic]


class ToolSlot(SlotProgress):
    """Presentation of one tool call of the current round.

    ``taskId`` is absent for a call not started yet (a sequential round) and for a call its request
    did not offer, which starts ``done`` with the ``entry`` generation wrote. ``entry`` is absent
    when the tool task faulted or was orphaned.
    """

    taskId: NotRequired[TaskId]
    entry: NotRequired[EntryId]


class NestedToolSummary(TypedDict, total=False):
    """How a nested call ended: an error flag, its execution time, spend, and bounded error text."""

    isError: bool
    durationMs: int
    usage: dict[str, JsonValue]
    error: str


class NestedToolSlot(SlotProgress):
    """Presentation of one nested call: a call a running tool made through ``executeTool()``."""

    parentCallId: str
    parentTaskId: TaskId
    taskId: TaskId
    arguments: dict[str, JsonValue]
    summary: NotRequired[NestedToolSummary]


class CompactionStatus(TypedDict, total=False):
    """Presentation of one live compaction task (spec section 8.7)."""

    taskId: TaskId
    reason: CompactionReason
    blocking: bool
    attempt: int
    retry: dict[str, JsonValue]


class CompactionFailure(TypedDict):
    """The newest compaction that failed since a compaction last produced a summary."""

    taskId: TaskId
    reason: CompactionReason
    error: str


class LiveState(TypedDict, total=False):
    """Built-in live conversation state."""

    run: RunState
    generation: dict[str, JsonValue]
    tools: list[ToolSlot]
    nestedTools: list[NestedToolSlot]
    compactions: list[CompactionStatus]
    compactionFailure: CompactionFailure


def _checkpoint_when(value: dict[str, Any], _ops: Any, _info: Any) -> bool:
    """A complete base whenever nothing runs: no generation and no running tool slots."""

    if value.get("generation") is not None:
        return False
    for key in ("tools", "nestedTools"):
        slots = value.get(key) or []
        if any(slot.get("status") == "running" for slot in slots):
            return False
    return True


LiveDoc = define_doc(
    {
        "kind": "pi.live",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {},
        "checkpoint_when": _checkpoint_when,
    }
)

GENERATION_TASK_NAME = "pi.generation"
TOOL_TASK_KIND = "pi.tool"
COMPACTION_TASK_KIND = "pi.compaction"
RUN_TASK_KINDS = frozenset({GENERATION_TASK_NAME})


async def start_generation(
    tx: Transaction,
    conversation_id: ConversationId,
    live: Any,
    inputs: list[SubmissionId],
    registry: Any,
) -> None:
    """Create the registry's generation task and give it run control of ``inputs``."""

    task = registry.task(GENERATION_TASK_NAME)
    if task is None:
        raise RuntimeError("Registry lacks the pi.generation task")
    await start_run(tx, conversation_id, live, inputs, task)


async def start_run(
    tx: Transaction,
    conversation_id: ConversationId,
    live: DocumentDraft,
    inputs: list[SubmissionId],
    task: Any,
) -> None:
    """Create the generation task and give it run control of ``inputs``."""

    task_id = await tx.create_task(
        task,
        {},
        {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
    )
    live["run"] = {"taskId": task_id, "inputs": list(inputs)}


def end_run(
    tx: Transaction,
    live: DocumentDraft,
    task_id: TaskId,
    settlement: SubmissionSettlement,
) -> None:
    """End the run owned by ``task_id``: settle its inputs and remove run control."""

    run = live.get("run")
    if run is not None and run["taskId"] == task_id:
        for id in run["inputs"]:
            tx.settle_submission(cast(SubmissionId, id), settlement)
        del live["run"]
    for key in ("generation", "tools", "nestedTools"):
        if key in live:
            del live[key]


def add_compaction_status(live: Any, status: Mapping[str, Any]) -> None:
    """Add the status of a compaction task created in this commit; statuses stay in task ID order."""

    statuses = live.get("compactions")
    if statuses is None:
        live["compactions"] = [dict(status)]
    else:
        statuses.append(dict(status))


def compaction_status(live: Any, task_id: TaskId) -> Any:
    """The status draft of compaction task ``task_id``, if listed."""

    statuses = live.get("compactions")
    if statuses is None:
        return None
    return next((status for status in statuses if status.get("taskId") == task_id), None)


def remove_compaction_status(live: Any, task_id: TaskId) -> None:
    """Remove the status of compaction task ``task_id``, and the list once empty."""

    statuses = live.get("compactions")
    if statuses is None:
        return
    for index in range(len(statuses)):
        if statuses[index].get("taskId") == task_id:
            del statuses[index]
            break
    if len(statuses) == 0:
        del live["compactions"]


def clear_compaction_failure(live: Any) -> None:
    """Remove the recorded compaction failure, once a compaction produces a summary."""

    if "compactionFailure" in live:
        del live["compactionFailure"]


def fail_compaction_status(live: Any, task_id: TaskId, error: str) -> None:
    """Remove the status of compaction task ``task_id`` and record that it failed with ``error``."""

    status = compaction_status(live, task_id)
    if status is not None:
        live["compactionFailure"] = {
            "taskId": status["taskId"],
            "reason": status["reason"],
            "error": error,
        }
    remove_compaction_status(live, task_id)


def tool_slot(live: Any, task_id: TaskId) -> Any:
    """The slot of tool task ``task_id``, model-issued or nested, if ``pi.live`` still lists it.

    Nested slots are found by binary search: a tool may make many nested calls, each of which looks
    up its slot in several commits, and their task IDs ascend.
    """

    slots = live.get("tools") if hasattr(live, "get") else None
    for slot in slots or ():
        if slot.get("taskId") == task_id:
            return slot
    nested = live.get("nestedTools") if hasattr(live, "get") else None
    if nested is None:
        return None
    low = 0
    high = len(nested) - 1
    while low <= high:
        middle = (low + high) >> 1
        candidate = nested[middle]
        candidate_id = candidate.get("taskId")
        if candidate_id == task_id:
            return candidate
        if candidate_id is not None and candidate_id < task_id:
            low = middle + 1
        else:
            high = middle - 1
    return None


def finish_slot(slot: Any) -> None:
    """Mark a slot done and clear its running output; the caller sets ``entry`` or ``summary``."""

    slot["status"] = "done"
    clear_progress(slot)


def remove_nested_slots(live: Any, task_id: TaskId) -> None:
    """Remove the nested slots below tool task ``task_id``, transitively; parents precede children."""

    nested = live.get("nestedTools") if hasattr(live, "get") else None
    if nested is None:
        return
    removed = {task_id}
    for slot in nested:
        if slot.get("parentTaskId") in removed:
            removed.add(slot.get("taskId"))
    for index in range(len(nested) - 1, -1, -1):
        if nested[index].get("parentTaskId") in removed:
            del nested[index]
    if len(nested) == 0:
        del live["nestedTools"]


def clear_progress(slot: Any) -> None:
    """Remove what a tool published while running; its result entry or a rerun replaces it."""

    for key in ("output", "droppedBytes", "droppedLines", "details", "diagnostics"):
        if key in slot:
            del slot[key]


async def settle_scheduler_outcome(tx: Transaction, record: TaskRecord, outcome: dict[str, Any]) -> None:
    """Harness cleanup for a terminal outcome the scheduler writes itself (faulted or orphaned)."""

    if record["kind"] == TOOL_TASK_KIND:
        live = await tx.doc(LiveDoc, record["conversationId"])
        slot = tool_slot(live, record["id"])
        if slot is not None:
            finish_slot(slot)
        remove_nested_slots(live, record["id"])
        return
    if record["kind"] == COMPACTION_TASK_KIND:
        failure_text = (
            str(cast(Mapping[str, Any], outcome.get("error") or {}).get("message", ""))
            if outcome.get("status") == "faulted"
            else str(outcome.get("reason", ""))
        )
        fail_compaction_status(
            await tx.doc(LiveDoc, record["conversationId"]), record["id"], failure_text
        )
        return
    if record["kind"] not in RUN_TASK_KINDS:
        return
    live = await tx.doc(LiveDoc, record["conversationId"])
    run = live.get("run")
    if run is None or run["taskId"] != record["id"]:
        return
    from .generation import convert_partial

    await convert_partial(tx, live, record["conversationId"])
    if outcome.get("status") == "faulted":
        error = outcome.get("error") or {}
        end_run(
            tx,
            live,
            record["id"],
            {
                "status": "unanswered",
                "reason": "faulted",
                "detail": error.get("message", error_message(error)),
            },
        )
        return
    end_run(
        tx,
        live,
        record["id"],
        {"status": "unanswered", "reason": outcome.get("reason", "aborted")},
    )


__all__ = [
    "COMPACTION_TASK_KIND",
    "GENERATION_TASK_NAME",
    "CompactionFailure",
    "CompactionStatus",
    "LiveDoc",
    "LiveState",
    "NestedToolSlot",
    "NestedToolSummary",
    "RUN_TASK_KINDS",
    "RunState",
    "TOOL_TASK_KIND",
    "ToolSlot",
    "add_compaction_status",
    "clear_compaction_failure",
    "clear_progress",
    "compaction_status",
    "end_run",
    "fail_compaction_status",
    "finish_slot",
    "remove_compaction_status",
    "remove_nested_slots",
    "settle_scheduler_outcome",
    "start_generation",
    "start_run",
    "tool_slot",
]

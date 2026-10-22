"""Built-in ``pi.live``: run control and presentation of current work (spec section 8.2).

``run`` is present exactly while a run owns the conversation. Generation publishes its attempt,
partial, retry, and deferred state here; a tool call publishes throttled running output into its
slot, model-issued or nested. Compaction status arrives with its ticket.
"""

from __future__ import annotations

from typing import Any, NotRequired, TypedDict, cast

from ..documents import define_doc
from ..errors import error_message
from ..ids import ConversationId, EntryId, SubmissionId, TaskId
from ..json import JsonValue
from ..session.drafts import DocumentDraft
from ..session.transaction import Transaction
from ..types import SubmissionSettlement, TaskRecord
from .types import ToolDiagnostic


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


class LiveState(TypedDict, total=False):
    """Built-in live conversation state."""

    run: RunState
    generation: dict[str, JsonValue]
    tools: list[ToolSlot]
    nestedTools: list[NestedToolSlot]
    compactions: list[dict[str, JsonValue]]
    compactionFailure: dict[str, JsonValue]


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
RUN_TASK_KINDS = frozenset({GENERATION_TASK_NAME})


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
    "GENERATION_TASK_NAME",
    "LiveDoc",
    "LiveState",
    "NestedToolSlot",
    "NestedToolSummary",
    "RUN_TASK_KINDS",
    "RunState",
    "TOOL_TASK_KIND",
    "ToolSlot",
    "clear_progress",
    "end_run",
    "finish_slot",
    "remove_nested_slots",
    "settle_scheduler_outcome",
    "start_run",
    "tool_slot",
]

"""Built-in ``pi.live``: run control and presentation of current work (spec section 8.2).

``run`` is present exactly while a run owns the conversation. Generation publishes its attempt,
partial, retry, and deferred state here; a tool call publishes throttled running output into its
slot, model-issued or nested. A compaction task lists its status here while it selects, summarizes,
or waits out a retry backoff, and the newest failure stays for a client to show.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, NotRequired, TypedDict, cast

from ..documents import define_doc
from ..errors import error_message
from ..ids import ConversationId, EntryId, SubmissionId, TaskId
from ..json import JsonValue
from ..session.drafts import DocumentDraft
from ..session.transaction import Transaction
from ..types import CommitOptions, SubmissionSettlement, TaskRecord, UnansweredSubmissionSettlement
from .types import CompactionReason, ToolDiagnostic

RunFailureReason = Literal[
    "model_error",
    "no_model",
    "aborted",
    "reset",
    "faulted",
    "orphaned",
    "interrupted",
    "restart_limit",
    "nothing_to_answer",
    "turn_limit",
]
"""Why a run ended without an answer (spec section 8.2).

``model_error``: the response failed after any retries, or the compaction a request needed failed.
``no_model``: no model is configured, or the configured one is not available. ``aborted``: the run
was aborted; ``detail`` says when its abort handler was cut off ``restart_limit`` times and did not
finish. ``reset``: a queued reset started a new context at the post-tools boundary. ``faulted``: the
run's task threw where it cannot recover, or storage rejected its commit. ``orphaned``: its task was
aborted while no registered definition could take it. ``interrupted``: the host stopped while the
model was answering, more often than ``retry`` retries. ``restart_limit``: the generation was cut off
``restart_limit`` times in a row. ``nothing_to_answer``: the context did not end in a user message or
tool results, so no request was sent. ``turn_limit``: a tool round reached the conversation's
``max_turns``; a continue resumes on its results.
"""


class RunFailure(TypedDict):
    """The newest run that ended without an answer.

    ``taskId``, ``inputs`` and ``reason`` are required, as upstream declares them
    (``src/harness/live.ts:121-140``); ``detail`` and ``first`` carry meaning only for some reasons.
    """

    taskId: TaskId
    """The task that ended the run: the run's task at its end."""
    inputs: list[SubmissionId]
    """The run's inputs, settled ``unanswered``."""
    reason: RunFailureReason
    detail: NotRequired[str]
    """``model_error``: the error text; ``no_model``: which model is missing; ``faulted``: the error
    message; ``orphaned``: why the task could not run; ``interrupted`` and ``restart_limit``: what was
    cut off, and how often; ``turn_limit``: the tool rounds the run took."""
    first: NotRequired[EntryId]
    """The failed run's ``run.first``, so a continue's ``afterRun`` hooks see its history too."""


class RunState(TypedDict, total=False):
    """The task that settles the run's inputs and those inputs; present exactly while busy."""

    taskId: TaskId
    inputs: list[SubmissionId]
    turns: int
    """Tool rounds this run has taken; the count a turn limit bounds, and that a stored run starts at 0."""
    first: NotRequired[EntryId]
    """The run's first entry: the first one the boundary that started it placed, a continue's the
    failed run's it continues; otherwise its first tool-calling answer. A run started before it was
    recorded may lack it; ``afterRun`` hooks then get the answer as the first entry."""
    afterRun: NotRequired[dict[str, Any]]
    """Present once the run answered and its generation waits for the tasks ``afterRun`` hooks asked
    for, listed once created: queued items stay queued until they are terminal, and the run then ends
    without a failure."""


class RunEndDone(TypedDict):
    """The run answered."""

    status: Literal["done"]
    answer: EntryId


class RunEndUnanswered(TypedDict, total=False):
    """The run ended without an answer, settling its inputs ``unanswered`` and recording why."""

    status: Literal["unanswered"]
    reason: RunFailureReason
    detail: str
    settlement: NotRequired[SubmissionSettlement]
    """The inputs' settlement; default ``unanswered`` with ``reason`` and ``detail``."""


RunEnd = RunEndDone | RunEndUnanswered
"""How a run ends."""


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

    ``taskId`` is absent for a call not started yet (a sequential round). ``unoffered`` marks a call
    to a tool its request did not offer: its task settles ``tool_unavailable`` without ever
    resolving the name. ``entry`` is absent when the tool task faulted or was orphaned.
    """

    taskId: NotRequired[TaskId]
    unoffered: NotRequired[Literal[True]]
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
    trigger: CompactionReason
    blocking: bool
    attempt: int
    retry: dict[str, JsonValue]


class CompactionFailure(TypedDict):
    """The newest compaction that failed since a compaction last produced a summary."""

    taskId: TaskId
    trigger: CompactionReason
    error: str


class LiveState(TypedDict, total=False):
    """Built-in live conversation state."""

    run: RunState
    generation: dict[str, JsonValue]
    tools: list[ToolSlot]
    nestedTools: list[NestedToolSlot]
    compactions: list[CompactionStatus]
    compactionFailure: CompactionFailure
    runFailure: RunFailure
    """Why the newest run ended without an answer, so a client can show it after the run ended. Set in
    the commit that ends the run; removed when a later commit starts a run. A run started in the same
    commit, by ``afterFailedRun: "continue"`` or after a reset, keeps it."""


def _checkpoint_when(value: dict[str, Any], _ops: Any, _info: Any) -> bool:
    """A complete base whenever nothing runs: no generation and no running tool slots."""

    if value.get("generation") is not None:
        return False
    for key in ("tools", "nestedTools"):
        slots = value.get(key) or []
        if any(slot.get("status") == "running" for slot in slots):
            return False
    return True


def _rename_reason(value: Mapping[str, Any]) -> dict[str, Any]:
    """A copy of a version 1 compaction status or failure with ``reason`` renamed ``trigger``.

    The keys keep their stored order: a migrated value is compared with one written fresh, and the
    port's JSON keeps the order its members were set in.
    """

    return {"trigger" if key == "reason" else key: field for key, field in value.items()}


def _migrate(value: Mapping[str, Any], _version: int) -> dict[str, Any]:
    """Carry a stored version 1 or 2 ``pi.live`` forward to version 3.

    Version 1 named a compaction's trigger ``reason``, in ``compactions`` and ``compactionFailure``;
    version 2 already names it ``trigger``, so renaming leaves it alone. Versions before ``run.turns``
    was stored may lack the count: a run in flight across the upgrade counts its turns from 0. An
    absent member stays absent — a migration never materialises one.
    """

    migrated = dict(value)
    run = value.get("run")
    if run is not None:
        turns = run.get("turns")
        counted = turns if isinstance(turns, (int, float)) and not isinstance(turns, bool) else 0
        migrated["run"] = {**run, "turns": counted}
    compactions = value.get("compactions")
    if compactions is not None:
        migrated["compactions"] = [_rename_reason(status) for status in compactions]
    failure = value.get("compactionFailure")
    if failure is not None:
        migrated["compactionFailure"] = _rename_reason(failure)
    return migrated


LiveDoc = define_doc(
    {
        "kind": "pi.live",
        "version": 3,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {},
        "migrate": _migrate,
        "checkpoint_when": _checkpoint_when,
    }
)

GENERATION_TASK_NAME = "pi.generation"
TOOL_TASK_KIND = "pi.tool"
COMPACTION_TASK_KIND = "pi.compaction"
#:
#: How the progress throttles commit a partial answer and running tool output into ``pi.live``: deferred,
#: so Storage gets them together, at the next commit that reads or writes ``pi.live`` or after
#: ``settings.progress.persist_within_ms``. Every commit that consumes or clears streamed fields reads
#: ``pi.live``, so it stores them with its own changes (`live.ts:230-235`).
PROGRESS_COMMIT: CommitOptions = {"persistence": {"kind": "deferred"}}

RUN_TASK_KINDS = frozenset({GENERATION_TASK_NAME})

LIMITS_RESTARTS = frozenset({*RUN_TASK_KINDS, TOOL_TASK_KIND, COMPACTION_TASK_KIND})
"""Built-in task kinds that end themselves once cut off ``settings.restart_limit`` times
(``TaskRecord.restarts``); the scheduler faults a task of any other kind instead (spec §5.4).
"""


def cut_off_too_often(task: Mapping[str, Any], runtime: Any) -> bool:
    """Whether ``task`` was cut off ``settings.restart_limit`` times in a row, so its phase ends it
    instead of running again.

    The settings are read only for a task that was cut off at all, so the normal path resolves the
    host's settings nothing more often than before the limit applied to every kind.
    """

    restarts = task.get("restarts")
    return bool(restarts is not None and restarts >= runtime.settings.restart_limit)


def salvageable(task: Mapping[str, Any], runtime: Any) -> bool:
    """Whether a task at the limit may still do the cleanup it does when it ends, such as converting a
    partial: only at the limit. Past it, that cleanup may be what cut the task off, so it ends without
    it.
    """

    return bool(int(task.get("restarts") or 0) <= runtime.settings.restart_limit)


def cut_off_count(task: Mapping[str, Any]) -> str:
    """How often a task was cut off, for a message: ``once``, or ``3 times in a row``."""

    restarts = int(task.get("restarts") or 0)
    return "once" if restarts == 1 else f"{restarts} times in a row"


async def start_generation(
    tx: Transaction,
    conversation_id: ConversationId,
    live: Any,
    inputs: list[SubmissionId],
    registry: Any,
    first: EntryId | None = None,
) -> None:
    """Create the registry's generation task and give it run control of ``inputs``."""

    task = registry.task(GENERATION_TASK_NAME)
    if task is None:
        raise RuntimeError("Registry lacks the pi.generation task")
    await start_run(tx, conversation_id, live, inputs, task, first)


async def start_run(
    tx: Transaction,
    conversation_id: ConversationId,
    live: DocumentDraft,
    inputs: list[SubmissionId],
    task: Any,
    first: EntryId | None = None,
    *,
    keep_failure: bool = False,
) -> None:
    """Create the generation task and give it run control of ``inputs``.

    ``first`` is the run's first entry, ``LiveRun.first``: the first entry the boundary that starts it
    placed, or a continue's the failed run's. The run failure is removed unless ``keep_failure``: a run
    started in the commit that ended the failed one -- the queue's next run under
    ``afterFailedRun: "continue"``, or the run a reset starts -- keeps its failure, so it is shown
    until a later run starts.
    """

    task_id = await tx.create_task(
        task,
        {},
        {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
    )
    run: RunState = {"taskId": task_id, "inputs": list(inputs), "turns": 0}
    if first is not None:
        run["first"] = first
    live["run"] = run
    if not keep_failure and "runFailure" in live:
        del live["runFailure"]


def _unanswered_settlement(reason: RunFailureReason, detail: str | None) -> UnansweredSubmissionSettlement:
    """The default settlement of a run's inputs: ``unanswered`` with the run's reason and detail."""

    settlement: UnansweredSubmissionSettlement = {"status": "unanswered", "reason": reason}
    if detail is not None:
        settlement["detail"] = detail
    return settlement


def end_run(
    tx: Transaction,
    live: DocumentDraft,
    task_id: TaskId,
    end: RunEnd,
) -> None:
    """End the run owned by ``task_id``: settle its inputs, remove run control, and record why.

    Without an answer the run's reason stays on the live document as ``runFailure``, and the inputs
    settle ``unanswered`` with that reason unless ``end`` carries its own ``settlement``. A run that
    answered already, and runs only its ``afterRun`` tasks, records no failure, however it ends. Always
    removes ``generation``, ``tools`` and ``nestedTools``, whose presentation belongs to the ending run.
    """

    run = live.get("run")
    if run is not None and run["taskId"] == task_id:
        inputs = list(run["inputs"])
        first = run.get("first")
        if end["status"] == "done":
            for id in inputs:
                tx.settle_submission(cast(SubmissionId, id), {"status": "done", "answer": end["answer"]})
        elif run.get("afterRun") is None:
            reason = end["reason"]
            detail = end.get("detail")
            settlement: SubmissionSettlement = end.get("settlement") or _unanswered_settlement(
                reason, detail
            )
            for submitted in inputs:
                tx.settle_submission(submitted, settlement)
            failure: RunFailure = {"taskId": task_id, "inputs": inputs, "reason": reason}
            if detail is not None:
                failure["detail"] = detail
            if first is not None:
                failure["first"] = first
            live["runFailure"] = failure
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
            "trigger": status["trigger"],
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
    """Harness cleanup for a terminal outcome the scheduler writes itself (faulted or orphaned).

    Only an abort handler is faulted for the limit here -- the built-in phases end themselves -- so the
    task keeps the meaning it had: an aborted run still ends ``aborted`` with the limit as its detail,
    and an aborted compaction is no failure. Past the limit the abort handler may have died doing the
    cleanup, so it is not attempted again (spec §5.4).
    """

    if record["kind"] == TOOL_TASK_KIND:
        live = await tx.doc(LiveDoc, record["conversationId"])
        slot = tool_slot(live, record["id"])
        if slot is not None:
            finish_slot(slot)
        remove_nested_slots(live, record["id"])
        return
    error = outcome.get("error") or {}
    detail = error.get("detail") if outcome.get("status") == "faulted" else None
    cut_off = isinstance(detail, Mapping) and detail.get("reason") == "restart_limit"
    if record["kind"] == COMPACTION_TASK_KIND:
        live = await tx.doc(LiveDoc, record["conversationId"])
        if cut_off:
            remove_compaction_status(live, record["id"])
            return
        fail_compaction_status(
            live,
            record["id"],
            str(error.get("message", ""))
            if outcome.get("status") == "faulted"
            else str(outcome.get("reason", "")),
        )
        return
    if record["kind"] not in RUN_TASK_KINDS:
        return
    live = await tx.doc(LiveDoc, record["conversationId"])
    run = live.get("run")
    if run is None or run["taskId"] != record["id"]:
        return
    from .generation import convert_partial

    # Cut off, the abort handler may have died converting the partial: the run ends without it.
    if not cut_off:
        await convert_partial(tx, live, record["conversationId"])
    if outcome.get("status") == "faulted":
        end_run(
            tx,
            live,
            record["id"],
            {
                "status": "unanswered",
                "reason": "aborted" if cut_off else "faulted",
                "detail": error.get("message", error_message(error)),
            },
        )
        return
    end_run(
        tx,
        live,
        record["id"],
        {
            "status": "unanswered",
            "reason": "orphaned",
            "detail": str(outcome.get("reason", "")),
            "settlement": {"status": "unanswered", "reason": outcome.get("reason", "aborted")},
        },
    )


__all__ = [
    "COMPACTION_TASK_KIND",
    "GENERATION_TASK_NAME",
    "LIMITS_RESTARTS",
    "CompactionFailure",
    "CompactionStatus",
    "LiveDoc",
    "LiveState",
    "NestedToolSlot",
    "NestedToolSummary",
    "RUN_TASK_KINDS",
    "RunEnd",
    "RunEndDone",
    "RunEndUnanswered",
    "RunFailure",
    "RunFailureReason",
    "RunState",
    "TOOL_TASK_KIND",
    "ToolSlot",
    "add_compaction_status",
    "clear_compaction_failure",
    "clear_progress",
    "compaction_status",
    "cut_off_count",
    "cut_off_too_often",
    "end_run",
    "fail_compaction_status",
    "finish_slot",
    "remove_compaction_status",
    "remove_nested_slots",
    "settle_scheduler_outcome",
    "PROGRESS_COMMIT",
    "start_generation",
    "start_run",
    "tool_slot",
]

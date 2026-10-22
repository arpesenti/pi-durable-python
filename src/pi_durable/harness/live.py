"""Built-in ``pi.live``: run control and presentation of current work (spec section 8.2).

Only run control is needed by the submission core: ``run`` is present exactly while a run owns
the conversation. Generation and tool presentation arrive with the built-in tasks.
"""

from __future__ import annotations

from typing import Any, TypedDict, cast

from ..documents import define_doc
from ..errors import error_message
from ..ids import ConversationId, SubmissionId, TaskId
from ..json import JsonValue
from ..session.drafts import DocumentDraft
from ..session.transaction import Transaction
from ..types import SubmissionSettlement, TaskRecord


class RunState(TypedDict):
    """The task that settles the run's inputs and those inputs; present exactly while busy."""

    taskId: TaskId
    inputs: list[SubmissionId]


class LiveState(TypedDict, total=False):
    """Built-in live conversation state."""

    run: RunState
    generation: dict[str, JsonValue]
    tools: list[dict[str, JsonValue]]
    nestedTools: list[dict[str, JsonValue]]
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


async def settle_scheduler_outcome(tx: Transaction, record: TaskRecord, outcome: dict[str, Any]) -> None:
    """Harness cleanup for a generation the scheduler ended itself (faulted or orphaned)."""

    if record["kind"] not in RUN_TASK_KINDS:
        return
    live = await tx.doc(LiveDoc, record["conversationId"])
    run = live.get("run")
    if run is None or run["taskId"] != record["id"]:
        return
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
    "RUN_TASK_KINDS",
    "RunState",
    "end_run",
    "settle_scheduler_outcome",
    "start_run",
]

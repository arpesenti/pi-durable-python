"""Core record shapes and JSON-level constructors.

Records are plain JSON objects, exactly as they are persisted. This keeps the storage
boundary, JSONL journals, and SQLite rows free of mapping layers and matches the
reference package's "records are JSON" contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from .ids import ROOT_CONVERSATION_ID
from .json import copy_json, omit_none

ConversationRecord = dict[str, Any]
EntryRecord = dict[str, Any]
TaskRecord = dict[str, Any]
SubmissionRecord = dict[str, Any]
DocumentRecord = dict[str, Any]
StorageWrite = dict[str, Any]
CommitChange = dict[str, Any]
CommitPublication = dict[str, Any]
Page = dict[str, Any]
Cursor = dict[str, Any]

TaskStatus = Literal["pending", "running", "waiting", "sleeping", "completing", "terminal"]
SubmissionStatus = Literal["queued", "placed", "done", "unanswered"]
ScanOrder = Literal["ascending", "descending"]

TASK_STATUSES: tuple[TaskStatus, ...] = (
    "pending",
    "running",
    "waiting",
    "sleeping",
    "completing",
    "terminal",
)
SUBMISSION_STATUSES: tuple[SubmissionStatus, ...] = ("queued", "placed", "done", "unanswered")
TERMINAL_TASK_STATUSES = ("terminal",)
LIVE_TASK_STATUSES = ("pending", "running", "waiting", "sleeping", "completing")


def conversation_record(
    conversation_id: int,
    parent: Mapping[str, Any] | None = None,
    owner: Mapping[str, Any] | None = None,
) -> ConversationRecord:
    return omit_none(
        {
            "id": conversation_id,
            "parent": copy_json(parent) if parent is not None else None,
            "owner": copy_json(owner) if owner is not None else None,
        }
    )


def entry_record(
    entry_id: int,
    conversation_id: int,
    kind: str,
    *,
    model: Any = None,
    data: Any = None,
    head: int | None = None,
    edits: Any = None,
    by_task_id: int | None = None,
) -> EntryRecord:
    record: EntryRecord = {"id": entry_id, "conversationId": conversation_id, "kind": kind}
    if model is not None:
        record["model"] = copy_json(model)
    if data is not None:
        record["data"] = copy_json(data)
    if head is not None:
        record["head"] = head
    if edits is not None:
        record["edits"] = copy_json(edits)
    if by_task_id is not None:
        record["byTaskId"] = by_task_id
    return record


def task_status(record: Mapping[str, Any]) -> TaskStatus:
    return record["state"]["status"]


def task_checkpoint(record: Mapping[str, Any]) -> Any:
    return record["state"].get("checkpoint")


def is_terminal(record: Mapping[str, Any]) -> bool:
    return record["state"]["status"] == "terminal"


def terminal_outcome(record: Mapping[str, Any]) -> dict[str, Any] | None:
    state = record["state"]
    if state["status"] in ("completing", "terminal"):
        return state["outcome"]
    return None


def settled_submission(record: Mapping[str, Any]) -> bool:
    return record["status"] in ("done", "unanswered")


def make_page(items: list[Any], next_cursor: Cursor | None = None) -> Page:
    page: Page = {"items": copy_json(items)}
    if next_cursor is not None:
        page["next"] = next_cursor
    return page


def next_cursor(last_id: int, order: ScanOrder) -> Cursor:
    return {"after": last_id, "order": order}


@dataclass
class TaskOptions:
    """Creation options for a durable task.

    ``ownership`` is ``{"kind": "conversation"}`` or
    ``{"kind": "task", "taskId": <id>}``. A conversation-owned task may be background.
    """

    ownership: dict[str, Any]
    conversation_id: int | None = None
    background: bool | None = None
    abandon_on_restart: bool = False


@dataclass
class TransactionScope:
    """Defaults a commit binds to: ``create_task`` conversation and entry attribution."""

    conversation_id: int | None = None
    task_id: int | None = None


def ownerless() -> dict[str, Any]:
    return {"kind": "ownerless"}


def task_owned(task_id: int) -> dict[str, Any]:
    return {"kind": "task", "taskId": task_id}


def conversation_owned() -> dict[str, Any]:
    return {"kind": "conversation"}


def child_of(task_id: int) -> dict[str, Any]:
    return {"kind": "task", "taskId": task_id}


__all__ = [
    "LIVE_TASK_STATUSES",
    "ROOT_CONVERSATION_ID",
    "SUBMISSION_STATUSES",
    "TASK_STATUSES",
    "TERMINAL_TASK_STATUSES",
    "CommitChange",
    "CommitPublication",
    "ConversationRecord",
    "Cursor",
    "DocumentRecord",
    "EntryRecord",
    "Page",
    "ScanOrder",
    "StorageWrite",
    "SubmissionRecord",
    "SubmissionStatus",
    "TaskOptions",
    "TaskRecord",
    "TaskStatus",
    "TransactionScope",
    "child_of",
    "conversation_owned",
    "conversation_record",
    "entry_record",
    "is_terminal",
    "make_page",
    "next_cursor",
    "ownerless",
    "settled_submission",
    "task_checkpoint",
    "task_owned",
    "task_status",
    "terminal_outcome",
]

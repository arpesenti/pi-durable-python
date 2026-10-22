"""Built-in ``pi.inbox`` and the submission boundary (spec section 6).

Queued submissions wait in one conversation-scoped document, in ID order. A boundary reads the
document on the Session line, places the selected writes and user items, and removes them
positionally; run control itself lives in ``pi.live``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypedDict, cast

from ..documents import define_doc
from ..entries import UserEntry
from ..ids import ConversationId, EntryId, SubmissionId
from ..json import JsonObject, JsonValue, copy_json
from ..session.drafts import DocumentDraft, DraftList, DraftObject
from ..session.transaction import Transaction
from ..types import EntryDraft, EntryRecord
from .types import InputEntry, QueueMode, UserInput


class InboxItem(TypedDict, total=False):
    """One queued submission: an input with its content, or a passive entry write."""

    id: SubmissionId
    mode: Literal["steer", "followUp", "write"]
    content: UserInput
    entry: JsonObject


class InboxState(TypedDict):
    """The inbox document value: queued submissions in submission-ID order."""

    items: list[InboxItem]


InboxDoc = define_doc(
    {
        "kind": "pi.inbox",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"items": []},
        "checkpoint_when": lambda value, _ops, _info: len(value.get("items") or []) == 0,
    }
)


@dataclass(slots=True)
class QueueModes:
    """The queue modes a boundary reads on the Session line."""

    steering_mode: QueueMode
    follow_up_mode: QueueMode


@dataclass(slots=True)
class Boundary:
    """What a boundary reads before the commit's first table write."""

    conversation_id: ConversationId
    inbox: DocumentDraft
    steering_mode: QueueMode
    follow_up_mode: QueueMode
    head: EntryId | None


@dataclass(frozen=True, slots=True)
class BoundaryResult:
    """Selected user submission IDs and whether a reset was among the placed writes."""

    users: list[SubmissionId]
    reset: bool


def _plain(value: Any) -> Any:
    """Return a plain JSON value from a document draft member or a record."""

    if isinstance(value, DraftObject):
        return {key: _plain(value[key]) for key in value.keys()}
    if isinstance(value, DraftList):
        return [_plain(item) for item in value]
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def _entry_draft(item: Any) -> EntryDraft:
    return cast(EntryDraft, _plain(item["entry"]))


async def prepare_boundary(
    tx: Transaction, conversation_id: ConversationId, modes: QueueModes
) -> Boundary:
    """Read the active-range start and the inbox before the commit's first table write."""

    marker = await tx.latest_head_marker(conversation_id)
    inbox = await tx.doc(InboxDoc, conversation_id)
    return Boundary(
        conversation_id=conversation_id,
        inbox=inbox,
        steering_mode=modes.steering_mode,
        follow_up_mode=modes.follow_up_mode,
        head=None if marker is None else marker.get("head"),
    )


async def apply_boundary(
    tx: Transaction, boundary: Boundary, at: Literal["postTools", "final"], now: int
) -> BoundaryResult:
    """Place the queued items a boundary selects, writes first and then user items, each in ID order.

    A selected reset turns a ``postTools`` boundary into ``final``. A head write whose target is
    older than the active range, including one started earlier in this commit, is stale. Selected
    and stale items are removed positionally.
    """

    conversation_id = boundary.conversation_id
    items = boundary.inbox["items"]
    reset = has_queued_reset(items)
    final = at == "final" or reset

    def pick(mode: Literal["steer", "followUp"], queue_mode: QueueMode) -> list[int]:
        indexes = [index for index, item in enumerate(items) if item["mode"] == mode]
        return indexes if queue_mode == "all" else indexes[:1]

    writes = [index for index, item in enumerate(items) if item["mode"] == "write"]
    users = [*pick("steer", boundary.steering_mode)]
    if final:
        users.extend(pick("followUp", boundary.follow_up_mode))
    users.sort()

    placed: list[SubmissionId] = []
    for index in writes:
        item = items[index]
        draft = _entry_draft(item)
        if is_stale(boundary, draft):
            tx.settle_submission(cast(SubmissionId, item["id"]), {"status": "unanswered", "reason": "stale"})
            continue
        entry = await tx.append_entry(conversation_id, draft)
        head = draft.get("head")
        if head is not None:
            boundary.head = entry["id"] if head == "self" else head
        tx.place_submission(cast(SubmissionId, item["id"]), entry["id"])

    for index in users:
        item = items[index]
        entry = await append_input(
            tx,
            conversation_id,
            cast(UserInput, _plain(item.get("content"))),
            _plain(item["entry"]) if "entry" in item else None,
            now,
        )
        tx.place_submission(cast(SubmissionId, item["id"]), entry["id"])
        placed.append(cast(SubmissionId, item["id"]))

    for index in sorted([*writes, *users], reverse=True):
        del items[index]
    return BoundaryResult(users=placed, reset=reset)


async def append_input(
    tx: Transaction,
    conversation_id: ConversationId,
    content: UserInput,
    entry: InputEntry | None,
    now: int,
) -> EntryRecord:
    """Append the entry an input is placed as: ``entry``'s kind and data, or ``pi.user``."""

    message = {"role": "user", "content": content, "timestamp": now}
    if entry is None:
        draft = cast(EntryDraft, {"model": [message]})
        return await tx.append_entry(UserEntry, conversation_id, draft)
    draft = cast(EntryDraft, {**pick_input_entry(entry), "model": [message]})
    return await tx.append_entry(conversation_id, draft)


def pick_input_entry(entry: InputEntry) -> InputEntry:
    """Only ``kind`` and ``data`` of an input's entry, as strict JSON."""

    picked: dict[str, JsonValue] = {"kind": entry.get("kind", "")}
    if "data" in entry:
        picked["data"] = copy_json(entry["data"])
    return cast(InputEntry, picked)


def is_display_only(entry: EntryDraft) -> bool:
    """Whether a write changes nothing the model sees: no ``model``, ``head``, or ``edits``."""

    return entry.get("model") is None and entry.get("head") is None and entry.get("edits") is None


def has_queued_reset(items: Any) -> bool:
    """Whether a reset (``head: "self"`` write) is queued."""

    return any(item["mode"] == "write" and item["entry"].get("head") == "self" for item in items)


def is_stale(boundary: Boundary, entry: EntryDraft) -> bool:
    """Whether a head write targets an entry before the active range."""

    head = entry.get("head")
    return (
        isinstance(head, int)
        and not isinstance(head, bool)
        and boundary.head is not None
        and head < boundary.head
    )


async def remove_inbox_item(tx: Transaction, conversation_id: ConversationId, id: SubmissionId) -> None:
    """Remove a withdrawn submission's item; the caller settles the submission."""

    items = (await tx.doc(InboxDoc, conversation_id))["items"]
    for index in range(len(items)):
        if items[index]["id"] == id:
            del items[index]
            return


async def withdraw_queued_inputs(tx: Transaction, conversation_id: ConversationId) -> None:
    """Withdraw every queued input of one conversation; queued writes stay for later placement."""

    items = (await tx.doc(InboxDoc, conversation_id))["items"]
    for index in range(len(items) - 1, -1, -1):
        item = items[index]
        if item["mode"] == "write":
            continue
        tx.settle_submission(cast(SubmissionId, item["id"]), {"status": "unanswered", "reason": "aborted"})
        del items[index]


__all__ = [
    "Boundary",
    "BoundaryResult",
    "InboxDoc",
    "InboxItem",
    "InboxState",
    "QueueModes",
    "append_input",
    "apply_boundary",
    "has_queued_reset",
    "is_display_only",
    "is_stale",
    "pick_input_entry",
    "prepare_boundary",
    "remove_inbox_item",
    "withdraw_queued_inputs",
]

"""Submissions, inbox queues, and turn-boundary placement."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ..context import user_message
from ..json import copy_json
from .docs import InboxDoc

INPUT = "input"
WRITE = "write"
STEER = "steer"
FOLLOW_UP = "followUp"
WRITE_MODE = "write"


class Submission:
    """Awaitable host object for one durably admitted submission."""

    def __init__(self, harness: Any, submission_id: int) -> None:
        self._harness = harness
        self.id = submission_id

    async def status(self, context: Any = None) -> dict[str, Any]:
        record = await self._harness.submission_record(self.id)
        if record is None:
            raise ValueError(f"Submission {self.id} does not exist")
        return record

    async def wait(self, context: Any = None) -> dict[str, Any]:
        record = await self.status()
        if record["status"] in ("done", "unanswered"):
            return record
        # Waiting asks for progress, so it never waits on a paused Harness.
        self._harness.resume()
        while True:
            record = await self.status()
            if record["status"] in ("done", "unanswered"):
                return record
            await self._harness.wait_for_submission_change(self.id)

    async def abort(self, context: Any = None) -> str:
        return await self._harness.abort_submission(self.id)


def input_item(submission: Mapping[str, Any], mode: str) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": submission["id"],
        "mode": mode,
        "type": INPUT,
        "content": copy_json(submission.get("content")),
    }
    if isinstance(submission.get("inputEntry"), Mapping):
        item["entry"] = copy_json(submission["inputEntry"])
    return item


def write_item(submission: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": submission["id"],
        "mode": WRITE_MODE,
        "type": WRITE,
        "entry": copy_json(submission["entry"]),
    }


async def read_inbox(tx: Any, conversation_id: int) -> dict[str, Any]:
    return await tx.doc(InboxDoc, conversation_id)


def select_boundary_items(
    items: Sequence[Mapping[str, Any]], *, final: bool, steering_mode: str, follow_up_mode: str
) -> list[Mapping[str, Any]]:
    writes = [item for item in items if item.get("mode") == WRITE_MODE]
    steers = [item for item in items if item.get("mode") == STEER]
    if steering_mode != "all":
        steers = steers[:1]
    follow_ups = []
    if final:
        follow_ups = [item for item in items if item.get("mode") == FOLLOW_UP]
        if follow_up_mode != "all":
            follow_ups = follow_ups[:1]
    return [*writes, *steers, *follow_ups]


async def apply_boundary(
    tx: Any,
    *,
    conversation_id: int,
    final: bool,
    settings: Any,
    now: int,
    active_start: int | None = None,
) -> dict[str, Any]:
    """Place the items this boundary selects. Run inside the committing transaction.

    ``active_start`` is the newest head marker's ``head`` at the start of the active range.
    A write whose head targets an older entry is stale: it settles ``unanswered`` and writes
    no entry, so it cannot resurrect history a reset or summary cut.
    """
    inbox = await tx.doc(InboxDoc, conversation_id)
    items = list(inbox.get("items") or [])
    selected = select_boundary_items(
        items,
        final=final,
        steering_mode=getattr(settings, "steering_mode", "one-at-a-time"),
        follow_up_mode=getattr(settings, "follow_up_mode", "one-at-a-time"),
    )
    if not selected:
        return {"inputs": [], "writes": [], "items": items}
    selected_ids = {item["id"] for item in selected}
    placed_inputs: list[int] = []
    placed_writes: list[int] = []
    for item in selected:
        if item["type"] == WRITE:
            draft = copy_json(item["entry"])
            head = draft.get("head")
            if (
                head is not None
                and head != "self"
                and active_start is not None
                and int(head) < active_start
            ):
                tx.settle_submission(item["id"], {"status": "unanswered", "reason": "stale"})
                continue
            entry = await tx.append_entry(conversation_id, draft)
            if head is not None:
                active_start = entry["id"] if head == "self" else int(head)
            tx.place_submission(item["id"], entry["id"])
            placed_writes.append(item["id"])
            continue
        custom = item.get("entry")
        kind = custom["kind"] if isinstance(custom, Mapping) and custom.get("kind") else "pi.user"
        data = custom.get("data") if isinstance(custom, Mapping) else None
        entry = await tx.append_entry(
            conversation_id,
            {
                "kind": kind,
                **({"data": copy_json(data)} if data is not None else {}),
                "model": [user_message(copy_json(item.get("content")), timestamp=now)],
            },
        )
        tx.place_submission(item["id"], entry["id"])
        placed_inputs.append(item["id"])
    inbox["items"] = [item for item in items if item["id"] not in selected_ids]
    return {"inputs": placed_inputs, "writes": placed_writes, "items": inbox["items"]}


def queued_submission(
    *,
    conversation_id: int,
    draft: Mapping[str, Any],
    request_id: str | None,
) -> dict[str, Any]:
    if draft.get("type") == WRITE:
        record: dict[str, Any] = {
            "type": WRITE,
            "conversationId": conversation_id,
            "status": "queued",
        }
    else:
        record = {
            "type": INPUT,
            "conversationId": conversation_id,
            "status": "queued",
            "content": copy_json(draft.get("content")),
        }
        if draft.get("entry") is not None:
            # The custom input entry lives in the inbox item until placement.
            record["inputEntry"] = copy_json(draft["entry"])
    if request_id is not None:
        record["requestId"] = request_id
    return record


async def withdraw_queued_inputs(tx: Any, conversation_id: int, reason: str = "aborted") -> int:
    inbox = await tx.doc(InboxDoc, conversation_id)
    items = list(inbox.get("items") or [])
    kept: list[dict[str, Any]] = []
    withdrawn = 0
    for item in items:
        if item.get("mode") == WRITE_MODE:
            kept.append(item)
            continue
        tx.settle_submission(item["id"], {"status": "unanswered", "reason": reason})
        withdrawn += 1
    inbox["items"] = kept
    return withdrawn


async def withdraw_queued_items(
    tx: Any, conversation_id: int, *, include_writes: bool, reason: str = "aborted"
) -> int:
    inbox = await tx.doc(InboxDoc, conversation_id)
    items = list(inbox.get("items") or [])
    kept: list[dict[str, Any]] = []
    withdrawn = 0
    for item in items:
        if not include_writes and item.get("mode") == WRITE_MODE:
            kept.append(item)
            continue
        tx.settle_submission(item["id"], {"status": "unanswered", "reason": reason})
        withdrawn += 1
    inbox["items"] = kept
    return withdrawn


__all__ = [
    "FOLLOW_UP",
    "INPUT",
    "STEER",
    "WRITE",
    "WRITE_MODE",
    "Submission",
    "apply_boundary",
    "input_item",
    "queued_submission",
    "read_inbox",
    "select_boundary_items",
    "withdraw_queued_inputs",
    "withdraw_queued_items",
    "write_item",
]

"""Submission admission, waits, and withdrawal (spec section 6).

``Conversation.submit()`` durably admits an input or passive write in one commit; the returned
handle tracks its settlement. Admission decides between immediate placement and the inbox; a
boundary later places queued items, and run control lives in ``pi.live``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal, cast

from ..context import Context
from ..errors import ConversationBusy
from ..ids import ConversationId, SubmissionId
from ..json import JsonValue, copy_json
from ..session.drafts import DocumentDraft
from ..session.transaction import Transaction
from ..types import EntryDraft, SubmissionCreate, SubmissionRecord
from .inbox import (
    InboxDoc,
    QueueModes,
    append_input,
    apply_boundary,
    has_queued_reset,
    is_display_only,
    is_stale,
    pick_input_entry,
    prepare_boundary,
    remove_inbox_item,
)
from .live import LiveDoc
from .types import InputEntry, SettledSubmissionRecord, Submission, SubmissionDraft
from .util import Waiters, closed_error, resolved

AbortResult = Literal["aborted", "already_placed", "settled"]

StartRun = Callable[[Transaction, ConversationId, DocumentDraft, list[SubmissionId]], Awaitable[None]]


def _type(draft: Mapping[str, Any]) -> Any:
    return draft.get("type")


def _request_id(draft: Mapping[str, Any]) -> str | None:
    value = draft.get("request_id", draft.get("requestId"))
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("A submission request ID must be a string")
    return value


def _when_busy(draft: Mapping[str, Any]) -> str | None:
    return draft.get("when_busy", draft.get("whenBusy"))


def is_settled(record: SubmissionRecord) -> bool:
    """Whether a submission reached ``done`` or ``unanswered``."""

    return record["status"] in ("done", "unanswered")


def check_input_entry(entry: InputEntry) -> None:
    """Reject an input entry whose kind is not a non-empty string outside the ``pi.`` prefix."""

    kind = entry.get("kind")
    if not isinstance(kind, str) or not kind or kind.startswith("pi."):
        raise TypeError(
            "An input entry's kind must be a non-empty string not starting with \"pi.\", "
            f"got {kind!r}"
        )


def _clean_entry(entry: Mapping[str, Any]) -> dict[str, JsonValue]:
    """Store the fields the caller supplied as strict JSON, preserving explicit JSON nulls."""

    return {key: copy_json(value) for key, value in entry.items()}


class Submissions:
    """Admission, waits, and withdrawal of the durable submissions of one Harness."""

    def __init__(
        self,
        session: Any,
        now: Callable[[], int],
        queue_modes: Callable[[], QueueModes],
        resume: Callable[[], None],
        start_run: StartRun,
    ) -> None:
        self._session = session
        self._now = now
        self._queue_modes = queue_modes
        self._resume = resume
        self._start_run = start_run
        self._waiters: Waiters[SubmissionId, SettledSubmissionRecord] = Waiters()
        self._closed = False
        session.observe_commits(self._observe)
        session.subscribe_close(self._close)

    async def submit(
        self, conversation_id: ConversationId, draft: SubmissionDraft, context: Context
    ) -> Submission:
        self._resume()
        value = cast(Mapping[str, Any], draft)

        async def admit(tx: Transaction) -> SubmissionId:
            return await admit_submission(
                tx,
                conversation_id,
                value,
                self._now(),
                self._queue_modes(),
                self._start_run,
            )

        id = await self._session.commit_with(admit, context)
        return SubmissionHandle(cast(SubmissionId, id), self)

    async def _record(self, id: SubmissionId, context: Context) -> SubmissionRecord | None:
        return await self._session.read_on_line(
            lambda: self._session.storage.submission(id, context)
        )

    async def get(self, id: SubmissionId, context: Context) -> Submission | None:
        record = await self._record(id, context)
        return None if record is None else SubmissionHandle(record["id"], self)

    async def status(self, id: SubmissionId, context: Context) -> SubmissionRecord:
        record = await self._record(id, context)
        if record is None:
            raise ValueError(f"Submission {id} does not exist")
        return record

    async def wait(self, id: SubmissionId, context: Context) -> SettledSubmissionRecord:
        self._resume()

        async def find() -> dict[str, Any]:
            record = await self._session.storage.submission(id, context)
            if record is None:
                raise ValueError(f"Submission {id} does not exist")
            if is_settled(record):
                return {"promise": resolved(cast(SettledSubmissionRecord, record))}
            if self._closed:
                raise closed_error(self._session)
            return {"promise": self._waiters.add(id, context)}

        found = await self._session.read_on_line(find)
        return cast(SettledSubmissionRecord, await found["promise"])

    async def abort(
        self, id: SubmissionId, context: Context, conversation_id: ConversationId | None = None
    ) -> AbortResult | Literal["not_found"]:
        async def withdraw(tx: Transaction) -> AbortResult | Literal["not_found"]:
            record = await tx.submission(id)
            if record is None or (
                conversation_id is not None and record["conversationId"] != conversation_id
            ):
                return "not_found"
            if record["status"] == "queued":
                tx.settle_submission(id, {"status": "unanswered", "reason": "aborted"})
                await remove_inbox_item(tx, record["conversationId"], id)
                return "aborted"
            return "already_placed" if record["status"] == "placed" else "settled"

        return await self._session.commit_with(withdraw, context)

    def _observe(self, publication: Any, _context: Context) -> None:
        for change in publication["changes"]:
            if change["type"] != "submission":
                continue
            value = change["value"]
            if is_settled(value):
                self._waiters.resolve(value["id"], cast(SettledSubmissionRecord, value))

    def _close(self) -> None:
        self._closed = True
        self._waiters.reject_all(closed_error(self._session))


class SubmissionHandle:
    """One admitted submission, tracked by the Harness that returned it."""

    __slots__ = ("id", "_submissions")

    def __init__(self, id: SubmissionId, submissions: Submissions) -> None:
        self.id = id
        self._submissions = submissions

    async def status(self, context: Context) -> SubmissionRecord:
        return await self._submissions.status(self.id, context)

    async def wait(self, context: Context) -> SettledSubmissionRecord:
        return await self._submissions.wait(self.id, context)

    async def abort(self, context: Context) -> AbortResult:
        result = await self._submissions.abort(self.id, context)
        if result == "not_found":
            raise ValueError(f"Submission {self.id} does not exist")
        return result


async def admit_submission(
    tx: Transaction,
    conversation_id: ConversationId,
    draft: Mapping[str, Any],
    now: int,
    queue_modes: QueueModes,
    start_run: StartRun,
) -> SubmissionId:
    """Admit one submission inside a commit (spec section 6)."""

    kind = _type(draft)
    if kind not in ("input", "write"):
        raise TypeError(f"Submission draft type must be 'input' or 'write', got {kind!r}")
    entry = draft.get("entry")
    if kind == "input" and entry is not None:
        if not isinstance(entry, Mapping):
            raise TypeError("An input entry must be an object")
        check_input_entry(cast(InputEntry, entry))
    request_id = _request_id(draft)
    if request_id is not None:
        existing = await tx.submission_by_request(conversation_id, request_id)
        if existing is not None:
            if existing["type"] != kind:
                raise ValueError(
                    f"Request {request_id} already identifies a submission of type {existing['type']}"
                )
            return existing["id"]
    request = {} if request_id is None else {"requestId": request_id}

    display_only = False
    if kind == "write":
        if not isinstance(entry, Mapping):
            raise TypeError("A write submission requires an entry draft")
        inbox = await tx.doc(InboxDoc, conversation_id)
        display_only = is_display_only(cast(Any, entry))
        if display_only and not has_queued_reset(inbox["items"]):
            return await _place_write(tx, conversation_id, request, entry)

    live = await tx.doc(LiveDoc, conversation_id)
    busy = live.get("run") is not None
    when_busy = _when_busy(draft)
    if busy and kind == "input" and when_busy == "reject":
        raise ConversationBusy(conversation_id)

    boundary = None if busy else await prepare_boundary(tx, conversation_id, queue_modes)
    if boundary is None or len(boundary.inbox["items"]) > 0:
        inbox = boundary.inbox if boundary is not None else await tx.doc(InboxDoc, conversation_id)
        record = await tx.create_submission(
            cast(
                SubmissionCreate,
                {
                    "conversationId": conversation_id,
                    **request,
                    "type": kind,
                    "status": "queued",
                },
            )
        )
        id = record["id"]
        if kind == "write":
            inbox["items"].append(
                {"id": id, "mode": "write", "entry": _clean_entry(cast(Mapping[str, Any], entry))}
            )
        else:
            mode = "steer" if when_busy == "steer" else "followUp"
            item: dict[str, Any] = {
                "id": id,
                "mode": mode,
                "content": copy_json(draft.get("content")),
            }
            if entry is not None:
                item["entry"] = pick_input_entry(cast(InputEntry, entry))
            inbox["items"].append(item)
        if boundary is None or display_only:
            return id
        placed = await apply_boundary(tx, boundary, "final", now)
        if placed.users:
            await start_run(tx, conversation_id, live, placed.users)
        return id

    entry = cast(Mapping[str, Any], entry) if entry is not None else None
    if kind == "write":
        assert entry is not None
        if is_stale(boundary, cast(Any, entry)):
            record = await tx.create_submission(
                cast(
                    SubmissionCreate,
                    {
                        "conversationId": conversation_id,
                        **request,
                        "type": "write",
                        "status": "unanswered",
                        "reason": "stale",
                    },
                )
            )
            return record["id"]
        return await _place_write(tx, conversation_id, request, entry)

    placed_entry = await append_input(
        tx,
        conversation_id,
        copy_json(draft.get("content")),
        None if entry is None else cast(InputEntry, entry),
        now,
    )
    record = await tx.create_submission(
        cast(
            SubmissionCreate,
            {
                "conversationId": conversation_id,
                **request,
                "type": "input",
                "status": "placed",
                "entry": placed_entry["id"],
            },
        )
    )
    await start_run(tx, conversation_id, live, [record["id"]])
    return record["id"]


async def _place_write(
    tx: Transaction,
    conversation_id: ConversationId,
    request: Mapping[str, str],
    entry: Mapping[str, Any],
) -> SubmissionId:
    """Append a write that a boundary or display-only admission places at once."""

    appended = await tx.append_entry(conversation_id, cast(EntryDraft, _clean_entry(entry)))
    record = await tx.create_submission(
        cast(
            SubmissionCreate,
            {
                "conversationId": conversation_id,
                **request,
                "type": "write",
                "status": "done",
                "entry": appended["id"],
            },
        )
    )
    return record["id"]


__all__ = [
    "AbortResult",
    "SubmissionHandle",
    "Submissions",
    "admit_submission",
    "check_input_entry",
    "is_settled",
]

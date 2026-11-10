"""Context derivation: the active transcript and its model-facing context.

Port of v1.3.0 ``src/harness/context.ts``. A read captures two bounds on the Session line — the
newest head marker at or below the cutoff and the cutoff entry — and then derives the view from
the immutable entries at or below the cutoff off that line.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, NotRequired, Protocol, TypedDict, cast

from ..context import Context
from ..entries import SystemEntry
from ..ids import ConversationId, EntryId
from ..json import freeze_json
from ..types import (
    ContextEdit,
    Cursor,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    MessageRecord,
    Storage,
)
from .types import ContextView

#: Largest page one range scan requests, matching the other built-in scans.
_SCAN_PAGE_SIZE = 256

#: Stop reasons whose assistant message never reaches a later provider request.
EXCLUDED_STOP_REASONS = frozenset({"aborted", "error", "deferred"})

#: Text of the result synthesized for a call the visible history cut off.
MISSING_RESULT_TEXT = "Tool result unavailable: history ends before this call completed."


class ContextBounds(TypedDict):
    """Head marker and newest visible entry that fix one committed context range."""

    head: EntryWithHead | None
    tail: EntryId


class EntryRange(TypedDict):
    """The visible range within captured bounds, without the conversation it belongs to."""

    minEntryId: NotRequired[EntryId]
    maxEntryId: EntryId


class _Undecided:
    """Marker for a range whose entries so far leave the baseline open."""

    def __repr__(self) -> str:
        return "UNDECIDED"


UNDECIDED = _Undecided()
"""No entry so far supplies a baseline, and a later entry still may."""

Baseline = MessageRecord | None | _Undecided
"""The baseline's system message, ``None`` when the range cannot have one, or ``UNDECIDED``."""


class ContextRange(TypedDict):
    """A derived context range, reusable as the previous range of a later read."""

    bounds: ContextBounds
    entries: list[EntryRecord]
    view: ContextView
    edited: set[EntryId]
    settled: list[MessageRecord]
    open: list[MessageRecord]
    baseline: Baseline
    #: Whether the range reached the conversation's newest entry when it was read, so the entries
    #: of later commits extend it (:func:`advance_range`); a range cut off at an earlier entry is not.
    current: bool


class ContextSession(Protocol):
    """The Session seam a context read needs: run a job on the mutation line, answer what its own
    commits already made known of a conversation's newest entry and head marker, and say where that
    newest entry stood at the last publication."""

    async def read_on_line(self, job: Callable[[Storage], Awaitable[Any]]) -> Any: ...

    async def latest_entry_id(
        self, conversation_id: ConversationId, context: Context
    ) -> EntryId | None: ...

    async def latest_head_marker(
        self, conversation_id: ConversationId, at_or_before: EntryId | None, context: Context
    ) -> EntryWithHead | None: ...

    def known_latest_entry_id(self, conversation_id: ConversationId) -> EntryId | None: ...


async def capture_context_bounds(
    session: ContextSession,
    storage: Storage,
    conversation_id: ConversationId,
    context: Context,
    at: EntryId | None = None,
    visible: bool = False,
) -> ContextBounds | None:
    """Capture the bounds of the current context, or of the context cut off at the visible entry ``at``.

    The newest entry and the head marker come from ``session``, which answers either from what its own
    commits made known (no Storage scan) or through ``storage``. ``visible`` says ``at`` is known to
    be visible, as an entry of a range of the conversation is: the read of storage that would prove
    it is then skipped.
    """

    tail: EntryId
    if at is None:
        latest = await session.latest_entry_id(conversation_id, context)
        if latest is None:
            return None
        tail = latest
    else:
        if not visible and await storage.entry(conversation_id, at, context) is None:
            raise ValueError(f"Entry {at} is not visible from conversation {conversation_id}")
        tail = at
    return {
        "head": await session.latest_head_marker(conversation_id, tail, context),
        "tail": tail,
    }


async def read_context(
    session: ContextSession,
    storage: Storage,
    conversation_id: ConversationId,
    context: Context,
    at: EntryId | None = None,
) -> ContextView:
    """Read one conversation's committed context: bounds on the Session line, entries off it."""

    bounds = await session.read_on_line(
        lambda line: capture_context_bounds(session, line, conversation_id, context, at)
    )
    return await derive_context(storage, conversation_id, bounds, context)


async def derive_context(
    storage: Storage,
    conversation_id: ConversationId,
    bounds: ContextBounds | None,
    context: Context,
) -> ContextView:
    """Derive the active transcript and model context of one conversation within captured bounds.

    The newest visible head marker ``H`` fixes the range: from ``H.head`` (or transcript start)
    through the tail. Per target, the newest edit in the range wins. Context entries are ``H``
    followed by the range's non-head entries.
    """

    if bounds is None:
        return empty_view()
    return derive_view(
        bounds,
        await scan_range(storage, conversation_id, range_query(bounds), context),
    )


def empty_view() -> ContextView:
    """Return the view of a conversation without entries."""

    return {"head": None, "entries": [], "contributions": [], "messages": []}


def derive_view(bounds: ContextBounds, entries: Sequence[EntryRecord]) -> ContextView:
    """Derive the context view from the oldest-first entries scanned within ``bounds``."""

    return derive_range(bounds, entries, False)["view"]


def derive_range(bounds: ContextBounds, entries: Sequence[EntryRecord], current: bool) -> ContextRange:
    """Derive the context view and the range a later read can extend."""

    edits: dict[EntryId, ContextEdit] = {}
    # Edits of every entry in the range count, including older head markers `select_active` drops.
    for entry in entries:
        for edit in entry.get("edits") or []:
            edits[edit["target"]] = edit
    active = select_active(bounds["head"], entries)
    contributions = [contribute(entry, edits.get(entry["id"])) for entry in active]
    settled, open_messages = settle([], [message for messages in contributions for message in messages])
    baseline = find_baseline(bounds["head"], active)
    messages = lead_with_system(
        [*settled, *order_tool_results(open_messages)],
        baseline,
    )
    view: ContextView = {
        "head": bounds["head"],
        "entries": list(active),
        "contributions": contributions,
        "messages": messages,
    }
    return {
        "bounds": bounds,
        "entries": list(entries),
        "view": view,
        "edited": set(edits),
        "settled": settled,
        "open": open_messages,
        "baseline": baseline,
        "current": current,
    }


async def read_context_from(
    session: Any,
    storage: Storage,
    conversation_id: ConversationId,
    context: Context,
    at: EntryId | None,
    previous: ContextRange | None,
) -> tuple[ContextView, ContextRange | None]:
    """Read the context, reusing ``previous``: with the same head, only newer entries are scanned.

    A range outlives the read, so its entries are frozen like ``MemoryStorage`` records, and the
    returned view has its own arrays. A caller that keeps the range for :func:`advance_range`
    checks ``current`` again, synchronously where it installs it: a commit can be published between
    this function's return and the caller's continuation.
    """

    visible = at is not None and previous is not None and range_entry(previous, at) is not None
    bounds = await session.read_on_line(
        lambda line: capture_context_bounds(session, line, conversation_id, context, at, visible)
    )
    if bounds is None:
        return empty_view(), None
    freeze_json(bounds["head"])
    current = at is None
    if previous is None or _head_id(previous["bounds"]) != _head_id(bounds):
        scanned = await scan_range(storage, conversation_id, range_query(bounds), context)
        range_value = derive_range(bounds, cast(list[EntryRecord], freeze_json(scanned)), current)
    elif bounds["tail"] == previous["bounds"]["tail"]:
        range_value = previous
    elif bounds["tail"] < previous["bounds"]["tail"]:
        range_value = derive_range(
            bounds,
            [entry for entry in previous["entries"] if entry["id"] <= bounds["tail"]],
            False,
        )
    else:
        min_entry_id = cast(EntryId, previous["bounds"]["tail"] + 1)
        added = await scan_range(
            storage,
            conversation_id,
            {"minEntryId": min_entry_id, "maxEntryId": bounds["tail"]},
            context,
        )
        range_value = extend_range(
            previous, bounds, cast(list[EntryRecord], freeze_json(added)), current
        )
    range_value = demote_missed(range_value, session.known_latest_entry_id(conversation_id))
    view = range_value["view"]
    return (
        {
            "head": view["head"],
            "entries": list(view["entries"]),
            "contributions": list(view["contributions"]),
            "messages": list(view["messages"]),
        },
        range_value,
    )


def extend_range(
    previous: ContextRange,
    bounds: ContextBounds,
    added: Sequence[EntryRecord],
    current: bool,
) -> ContextRange:
    """Extend ``previous`` by entries after its tail under the same head marker."""

    entries = [*previous["entries"], *added]
    if any("edits" in entry or "head" in entry or entry["id"] in previous["edited"] for entry in added):
        return derive_range(bounds, entries, current)
    contributions = [contribute(entry, None) for entry in added]
    settled, open_messages = settle(
        previous["settled"],
        [*previous["open"], *[message for messages in contributions for message in messages]],
    )
    baseline = previous["baseline"]
    if baseline is UNDECIDED:
        baseline = find_baseline(bounds["head"], added)
    view: ContextView = {
        "head": bounds["head"],
        "entries": [*previous["view"]["entries"], *added],
        "contributions": [*previous["view"]["contributions"], *contributions],
        "messages": lead_with_system([*settled, *order_tool_results(open_messages)], baseline),
    }
    return {
        "bounds": bounds,
        "entries": entries,
        "view": view,
        "edited": previous["edited"],
        "settled": settled,
        "open": open_messages,
        "baseline": baseline,
        "current": current,
    }


def demote_missed(range_value: ContextRange, known_latest: EntryId | None) -> ContextRange:
    """``range_value`` as not ``current`` when an entry was committed while its scan ran off the line.

    The entries the scan returned are those at or below its bounds' tail; a conversation whose newest
    entry the Session already knows to be another one had an entry the range did not see, so extending
    it from the next publication would skip that entry. A caller keeping a range checks this again,
    synchronously where it installs the range: a commit can be published between the read's return
    and the caller's continuation.
    """

    if range_value["current"] and known_latest != range_value["bounds"]["tail"]:
        return {**range_value, "current": False}
    return range_value


def range_entry(range_value: ContextRange, id: EntryId) -> EntryRecord | None:
    """Entry ``id`` of ``range_value``, by binary search: a range holds its entries in ID order."""

    entries = range_value["entries"]
    low, high = 0, len(entries) - 1
    while low <= high:
        middle = (low + high) // 2
        entry = entries[middle]
        if entry["id"] == id:
            return entry
        if entry["id"] < id:
            low = middle + 1
        else:
            high = middle - 1
    return None


def advance_range(
    range_value: ContextRange,
    appended: Sequence[EntryRecord],
) -> ContextRange | None:
    """``range_value`` with the entries one commit appended to its conversation, without reading Storage.

    The conversation's own new entries are its newest, and a new head marker's head never lies
    before the range's start. A range that is not ``current`` stays as it is; the next read extends
    it from Storage. ``None`` drops the range.
    """

    if not range_value["current"] or not appended:
        return range_value
    added = cast(list[EntryRecord], freeze_json(list(appended)))
    head: EntryWithHead | None = range_value["bounds"]["head"]
    for entry in added:
        if entry.get("head") is not None:
            head = cast(EntryWithHead, entry)
    bounds: ContextBounds = {"head": head, "tail": added[-1]["id"]}
    previous_head = range_value["bounds"]["head"]
    previous_id = None if previous_head is None else previous_head["id"]
    if (None if head is None else head["id"]) == previous_id:
        return extend_range(range_value, bounds, added, True)
    assert head is not None
    start = head["head"]
    # Heads only move forward: a compaction keeps a later entry, a reset starts at itself, older is stale.
    if previous_head is not None and start < previous_head["head"]:
        return None
    entries = [entry for entry in [*range_value["entries"], *added] if entry["id"] >= start]
    return derive_range(bounds, entries, True)


def _head_id(bounds: ContextBounds) -> EntryId | None:
    head = bounds["head"]
    return None if head is None else head["id"]


def select_active(
    head: EntryWithHead | None,
    entries: Sequence[EntryRecord],
) -> list[EntryRecord]:
    """Return the head marker followed by the range's non-head entries, or the whole range."""

    if head is None:
        return list(entries)
    return [cast(EntryRecord, head), *[entry for entry in entries if "head" not in entry]]


def contribute(entry: EntryRecord, edit: ContextEdit | None) -> list[MessageRecord]:
    """Return one active entry's model messages after its edit and excluded stop reasons.

    Tool result ordering has not applied yet.
    """

    if edit is not None and edit["action"] == "omit":
        return []
    if edit is not None and edit["action"] == "replace":
        contributed = edit["messages"]
    else:
        contributed = entry.get("model") or []
    return [
        message
        for message in contributed
        if message["role"] != "assistant" or message.get("stopReason") not in EXCLUDED_STOP_REASONS
    ]


def settle(
    settled: Sequence[MessageRecord],
    open_messages: Sequence[MessageRecord],
) -> tuple[list[MessageRecord], list[MessageRecord]]:
    """Move the ordered messages before the last assistant message of ``open`` into ``settled``.

    ``order_tool_results()`` of a sequence equals the concatenation over its parts when each later
    part starts with an assistant message.
    """

    last = _find_last_index(open_messages, lambda message: message["role"] == "assistant")
    if last <= 0:
        return list(settled), list(open_messages)
    return (
        [*settled, *order_tool_results(open_messages[:last])],
        list(open_messages[last:]),
    )


def find_baseline(
    head: EntryWithHead | None,
    entries: Sequence[EntryRecord],
) -> Baseline:
    """Return the baseline system message among ``entries``, oldest first.

    The baseline is the system message of the first ``pi.system`` entry after the head marker, or
    of the transcript without one, when generation wrote it with every section and tool in effect.
    ``None`` when an earlier entry carries a system message or the first ``pi.system`` entry has
    none, and ``UNDECIDED`` when no entry so far settles it.
    """

    # An edit that targets the baseline replaces or omits its messages, so the entry's own system
    # message is never contributed and cannot lead: `lead_with_system()` compares by identity.
    for entry in entries:
        if head is not None and entry["id"] <= head["id"]:
            continue
        if SystemEntry.is_(entry):
            return next(
                (message for message in entry.get("model") or [] if message["role"] == "system"),
                None,
            )
        if head is None and carries_system_message(entry):
            return None
    return UNDECIDED


def carries_system_message(entry: EntryRecord) -> bool:
    """Return whether an entry's own messages or its replacement edits include a system message."""

    def is_system(message: MessageRecord) -> bool:
        return message["role"] == "system"

    return any(is_system(message) for message in entry.get("model") or []) or any(
        edit["action"] == "replace" and any(is_system(message) for message in edit["messages"])
        for edit in entry.get("edits") or []
    )


def lead_with_system(
    messages: Sequence[MessageRecord],
    baseline: Baseline,
) -> list[MessageRecord]:
    """Move the first system message to the front when only user messages precede it.

    It also leads when it is the baseline's system message. Providers treat only a leading system
    message as the initial prompt and tool set, so without one a later tool change rewrites the
    request's tool list and invalidates the whole prompt cache. Only the first system message
    moves, so replaying system messages in order gives the same sections and tools. Any other
    system message before an assistant message stays where it was committed: moving it would
    change what it means to the model.
    """

    index = next(
        (position for position, message in enumerate(messages) if message["role"] == "system"),
        -1,
    )
    if index <= 0:
        return list(messages)
    system = messages[index]
    if system is not baseline and any(message["role"] != "user" for message in messages[:index]):
        return list(messages)
    return [system, *messages[:index], *messages[index + 1 :]]


def order_tool_results(messages: Sequence[MessageRecord]) -> list[MessageRecord]:
    """Place each assistant's tool results directly after it in call order.

    Results are taken from the messages before the next assistant; a missing result is synthesized
    and unmatched results are dropped.
    """

    ordered: list[MessageRecord] = []
    for index, message in enumerate(messages):
        if message["role"] == "toolResult":
            continue
        ordered.append(message)
        if message["role"] != "assistant":
            continue
        calls = [
            content
            for content in cast(list[dict[str, Any]], message["content"])
            if content.get("type") == "toolCall"
        ]
        if not calls:
            continue
        results: dict[str, int] = {}
        for next_index in range(index + 1, len(messages)):
            candidate = messages[next_index]
            if candidate["role"] == "assistant":
                break
            tool_call_id = candidate.get("toolCallId")
            if (
                candidate["role"] == "toolResult"
                and isinstance(tool_call_id, str)
                and tool_call_id not in results
            ):
                results[tool_call_id] = next_index
        for call in calls:
            result_index = results.get(cast(str, call["id"]))
            ordered.append(
                missing_result(call, cast("int | None", message.get("timestamp")))
                if result_index is None
                else messages[result_index]
            )
    return ordered


def missing_result(call: dict[str, Any], timestamp: int | None) -> MessageRecord:
    """Return the frozen error result for a call whose result the visible history lacks."""

    return cast(
        MessageRecord,
        freeze_json(
            {
                "role": "toolResult",
                "toolCallId": call["id"],
                "toolName": call["name"],
                "content": [{"type": "text", "text": MISSING_RESULT_TEXT}],
                "isError": True,
                "details": {"reason": "missing_result"},
                "timestamp": timestamp,
            }
        ),
    )


def range_query(bounds: ContextBounds) -> EntryRange:
    """Return the visible range of ``bounds``: from the head marker's head, or transcript start."""

    head = bounds["head"]
    if head is None:
        return {"maxEntryId": bounds["tail"]}
    return {"minEntryId": head["head"], "maxEntryId": bounds["tail"]}


async def scan_range(
    storage: Storage,
    conversation_id: ConversationId,
    entry_range: EntryRange,
    context: Context,
) -> list[EntryRecord]:
    """Return the visible entries within ``entry_range``, oldest first."""

    entries: list[EntryRecord] = []
    cursor: Cursor | None = None
    while True:
        page = await storage.scan_entries(
            cast(EntryQuery, {**entry_range, "conversationId": conversation_id}),
            _SCAN_PAGE_SIZE,
            cursor,
            context,
        )
        entries.extend(page["items"])
        if "next" not in page:
            break
        cursor = page["next"]
    entries.reverse()
    return entries


def _find_last_index(values: Sequence[Any], predicate: Callable[[Any], bool]) -> int:
    for index in range(len(values) - 1, -1, -1):
        if predicate(values[index]):
            return index
    return -1


__all__ = [
    "EXCLUDED_STOP_REASONS",
    "MISSING_RESULT_TEXT",
    "Baseline",
    "ContextBounds",
    "ContextRange",
    "ContextSession",
    "EntryRange",
    "UNDECIDED",
    "advance_range",
    "capture_context_bounds",
    "carries_system_message",
    "contribute",
    "derive_context",
    "demote_missed",
    "derive_range",
    "derive_view",
    "empty_view",
    "extend_range",
    "find_baseline",
    "lead_with_system",
    "missing_result",
    "order_tool_results",
    "read_context",
    "read_context_from",
    "range_entry",
    "range_query",
    "scan_range",
    "select_active",
    "settle",
]

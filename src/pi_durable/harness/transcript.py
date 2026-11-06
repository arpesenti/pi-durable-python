"""A client's transcript: what one consumer has of a conversation, merged from windowed views (spec §9.3).

A view carries a window, and entries that leave a window leave the view but not the client that
already held them. :class:`ConversationTranscript` is the layer over any view — a watch of the
mount, or a replica of it on another host — that keeps every entry it has seen as ``items``, opens a
``gap`` where a window does not continue what it holds, and marks the top ``earlier`` while the
transcript's start is unknown. Nothing here counts entries or infers a range from an entry ID: only
exclusive bounds become the inclusive bounds of an ``EntryQuery``, and :attr:`resume` is the window
that continues a dropped watch. A merge costs O(items), never re-sorts, never re-fetches, and one
that adds nothing returns the previous items.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, Literal, TypedDict, cast

from ..context import Context
from ..ids import EntryId
from ..json import JsonObject
from ..types import ConversationRecord, Cursor, EntryRecord, Page
from .util import rejected, require_integer, resolved
from .view import ConversationView, ConversationWindow

NEGATIVE_INFINITY: float = float("-inf")
"""The sentinel below every entry ID: an end of the known range that is not an entry."""

DEFAULT_PAGE_SIZE = 50
"""Entries ``load_earlier()`` and ``fill_gap()`` load per call, when the caller gives none."""


class TranscriptEntryItem(TypedDict):
    """One held entry."""

    kind: Literal["entry"]
    entry: EntryRecord


class TranscriptGapItem(TypedDict):
    """Entries strictly between ``after`` and ``upTo`` are missing: render "load more"."""

    kind: Literal["gap"]
    after: EntryId
    upTo: EntryId


class TranscriptEarlierItem(TypedDict):
    """The transcript continues above: render "load earlier". Always the first item."""

    kind: Literal["earlier"]


TranscriptItem = TranscriptEntryItem | TranscriptGapItem | TranscriptEarlierItem
"""One row of a client's transcript."""


class TranscriptView(TypedDict):
    """The conversation view as a renderer sees it: ``items``, the whole transcript, not the window."""

    conversation: ConversationRecord
    docs: dict[str, JsonObject]
    items: list[TranscriptItem]


EntryLoader = Callable[[Mapping[str, Any], int, Context], Awaitable[Page[EntryRecord, Cursor]]]
"""Pages a conversation's entries, as ``Conversation.entries()`` does without a cursor."""


class ConversationTranscriptOptions(TypedDict, total=False):
    """Options of :class:`ConversationTranscript`."""

    page_size: int


EARLIER: TranscriptEarlierItem = {"kind": "earlier"}
"""The single ``earlier`` item: it carries no state, so it is shared by every transcript."""


class ConversationTranscript:
    """Every entry one client has of a conversation, and the gaps between them (spec §9.3).

    Call :meth:`merge` with every value of the view. A window whose ``before`` lies past the newest
    entry held does not continue the transcript, so what lies between becomes a gap the caller can
    fill; :meth:`load_earlier` pages backwards to the transcript's start. Entries never change once
    committed, so the transcript only ever grows.
    """

    def __init__(
        self, load: EntryLoader, options: ConversationTranscriptOptions | None = None
    ) -> None:
        page_size = (options or {}).get("page_size", DEFAULT_PAGE_SIZE)
        self._page_size = require_integer(page_size, "Page size")
        if self._page_size < 1:
            raise ValueError(f"Page size must be an integer >= 1, got {page_size!r}")
        self._load = load
        self._entries: list[TranscriptEntryItem] = []
        self._ids: set[EntryId] = set()
        self._gaps: list[TranscriptGapItem] = []
        self._started = False
        self._low: float | int = NEGATIVE_INFINITY
        self._tail: float | int = NEGATIVE_INFINITY
        self._items: list[TranscriptItem] = []
        self._last: ConversationView | None = None
        self._view: TranscriptView | None = None
        self._earlier: asyncio.Future[TranscriptView] | None = None
        self._filling: dict[str, asyncio.Future[TranscriptView]] = {}

    @property
    def newest(self) -> EntryId | None:
        """The ID the transcript reaches: its newest entry, or a later empty window's ``before``."""

        return None if self._tail == NEGATIVE_INFINITY else cast(EntryId, int(self._tail))

    @property
    def resume(self) -> ConversationWindow | None:
        """The window that resumes this transcript: ``{from}`` right after ``newest``, ``{}`` for an
        empty transcript, and absent before the first merge.
        """

        if not self._started:
            return None
        newest = self.newest
        return {} if newest is None else {"from": cast(EntryId, newest + 1)}

    def merge(self, view: ConversationView) -> TranscriptView:
        """Take one value of the view: its window's entries join the transcript."""

        self._last = view
        entries = list(view["entries"])
        before = view.get("before")
        low = NEGATIVE_INFINITY if before is None else before
        high = entries[-1]["id"] if entries else (NEGATIVE_INFINITY if before is None else before)
        self._cover(low, high, entries)
        return self._result()

    def load_earlier(self, context: Context) -> Awaitable[TranscriptView]:
        """Load the page before the oldest entry held; the current view when nothing is earlier."""

        loop = asyncio.get_running_loop()
        if self._last is None:
            return rejected(RuntimeError("ConversationTranscript: merge a view first"))
        if self._low == NEGATIVE_INFINITY:
            return resolved(self._result())
        if self._earlier is not None:
            return self._earlier
        low = self._low
        loading = loop.create_task(self._load_earlier(low, context))
        self._earlier = loading

        def settled(*_result: Any) -> None:
            if self._earlier is loading:
                self._earlier = None

        loading.add_done_callback(settled)
        return loading

    def fill_gap(
        self, gap: TranscriptGapItem, context: Context
    ) -> Awaitable[TranscriptView]:
        """Load the next page of ``gap``, shrinking it from its start, or removing it once nothing
        is missing.

        A gap that has shrunk since loads the gap it became; one that is no longer there gives the
        current view.
        """

        loop = asyncio.get_running_loop()
        current = next(
            (
                each
                for each in self._gaps
                if each["after"] == gap["after"] and each["upTo"] == gap["upTo"]
            ),
            None,
        ) or next(
            (
                each
                for each in self._gaps
                if each["after"] < gap["upTo"] and gap["after"] < each["upTo"]
            ),
            None,
        )
        if current is None:
            if self._last is None:
                return rejected(RuntimeError("ConversationTranscript: merge a view first"))
            return resolved(self._result())
        # Keyed by the gap the call resolved to, so a stale descriptor and the current one share it.
        key = f"{current['after']}:{current['upTo']}"
        running = self._filling.get(key)
        if running is not None:
            return running
        filling = loop.create_task(self._fill(current, context))
        self._filling[key] = filling

        def settled(*_result: Any) -> None:
            if self._filling.get(key) is filling:
                del self._filling[key]

        filling.add_done_callback(settled)
        return filling

    async def _load_earlier(self, low: float | int, context: Context) -> TranscriptView:
        page = await self._call({"maxEntryId": low, "order": "descending"}, context)
        items = list(reversed(page["items"]))
        oldest = items[0] if items else None
        # No next page, or an empty one: the transcript starts here.
        if page.get("next") is None or oldest is None:
            self._cover(NEGATIVE_INFINITY, low, items)
        else:
            self._cover(oldest["id"] - 1, low, items)
        return self._result()

    async def _fill(self, current: TranscriptGapItem, context: Context) -> TranscriptView:
        page = await self._call(
            {
                "minEntryId": current["after"] + 1,
                "maxEntryId": current["upTo"] - 1,
                "order": "ascending",
            },
            context,
        )
        items = list(page["items"])
        newest = items[-1] if items else None
        # No next page, or an empty one: nothing else is missing.
        if page.get("next") is None or newest is None:
            self._cover(current["after"], current["upTo"] - 1, items)
        else:
            self._cover(current["after"], newest["id"], items)
        return self._result()

    async def _call(
        self, query: Mapping[str, Any], context: Context
    ) -> Page[EntryRecord, Cursor]:
        loaded = self._load(query, self._page_size, context)
        if inspect.isawaitable(loaded):
            return await loaded
        return loaded

    def _cover(
        self,
        low: float | int,
        high: float | int,
        added: Sequence[EntryRecord],
    ) -> None:
        """Record that every entry with an ID in ``(low, high]`` is known, ``added`` being those
        entries in ID order.
        """

        fresh = [entry for entry in added if entry["id"] not in self._ids]
        for entry in fresh:
            self._ids.add(entry["id"])
        held = self._entries
        # New entries past every held one: the items grow at their end.
        appended = (
            not fresh or not held or fresh[0]["id"] > held[-1]["entry"]["id"]
        )
        items: list[TranscriptEntryItem] = [
            {"kind": "entry", "entry": entry} for entry in fresh
        ]
        if items:
            self._entries = held + items if appended else _merge_by_id(held, items)
        rebuild = False
        if not self._started or (
            not held and not self._gaps and low >= self._tail
        ):
            # The first range, or one after an empty transcript: it is all the client knows.
            rebuild = not self._started or low != self._low
            self._started = True
            self._low = low
            self._tail = max(self._tail, high)
        elif low > self._tail:
            # Past the newest known ID: what lies between is missing.
            up_to = added[0]["id"] if len(added) > 0 else low + 1
            self._gaps = [*self._gaps, _gap_of(self._tail, up_to)]
            self._tail = high
            rebuild = True
        elif high != NEGATIVE_INFINITY and high < self._low:
            # Below the oldest known ID: what lies between is missing.
            following = next(
                (item["entry"]["id"] for item in held if item["entry"]["id"] > high), None
            )
            up_to = following if following is not None else cast(EntryId, self._low + 1)
            self._gaps = [_gap_of(high, up_to), *self._gaps]
            self._low = low
            rebuild = True
        else:
            gaps = self._subtract(low, high)
            if gaps is not self._gaps:
                self._gaps = gaps
                rebuild = True
            if low < self._low:
                self._low = low
                rebuild = True
            if high > self._tail:
                self._tail = high
        if rebuild or not appended:
            self._items = self._build()
        elif items:
            self._items = [*self._items, *items]

    def _subtract(self, low: float | int, high: float | int) -> list[TranscriptGapItem]:
        """The gaps without ``(low, high]``; the same list when none changes, as when the range is empty."""

        if high <= low:
            return self._gaps
        changed = False
        gaps: list[TranscriptGapItem] = []
        for gap in self._gaps:
            # Missing: IDs in (after, upTo); known now: IDs in (low, high].
            if high <= gap["after"] or low + 1 >= gap["upTo"]:
                gaps.append(gap)
                continue
            changed = True
            if low > gap["after"]:
                first_known = next(
                    (
                        item["entry"]["id"]
                        for item in self._entries
                        if item["entry"]["id"] > low
                    ),
                    None,
                )
                floor = cast(EntryId, low + 1) if first_known is None else first_known
                gaps.append(_gap_of(gap["after"], min(gap["upTo"], floor)))
            if high + 1 < gap["upTo"]:
                gaps.append(_gap_of(high, gap["upTo"]))
        return gaps if changed else self._gaps

    def _build(self) -> list[TranscriptItem]:
        """Items in ID order: ``earlier`` until the start is known, then entries with each gap
        before the entry after it.
        """

        items: list[TranscriptItem] = [] if self._low == NEGATIVE_INFINITY else [EARLIER]
        gaps = self._gaps
        next_gap = 0
        for item in self._entries:
            while (
                next_gap < len(gaps) and gaps[next_gap]["after"] < item["entry"]["id"]
            ):
                items.append(gaps[next_gap])
                next_gap += 1
            items.append(item)
        while next_gap < len(gaps):
            items.append(gaps[next_gap])
            next_gap += 1
        return items

    def _result(self) -> TranscriptView:
        """The view with the current items; the previous one when nothing changed."""

        last = self._last
        assert last is not None  # every caller merged a view first
        previous = self._view
        if (
            previous is not None
            and previous["items"] is self._items
            and previous["docs"] is last["docs"]
            and previous["conversation"] is last["conversation"]
        ):
            return previous
        self._view = {
            "conversation": last["conversation"],
            "docs": last["docs"],
            "items": self._items,
        }
        return self._view


def _gap_of(after: float | int, up_to: float | int) -> TranscriptGapItem:
    """One gap item; a gap is replaced rather than mutated, so callers may hold it."""

    return {
        "kind": "gap",
        "after": cast(EntryId, int(after)),
        "upTo": cast(EntryId, int(up_to)),
    }


def _merge_by_id(
    left: Sequence[TranscriptEntryItem], right: Sequence[TranscriptEntryItem]
) -> list[TranscriptEntryItem]:
    """Two ID-ordered item lists merged in one pass."""

    merged: list[TranscriptEntryItem] = []
    first = second = 0
    while first < len(left) and second < len(right):
        if left[first]["entry"]["id"] < right[second]["entry"]["id"]:
            merged.append(left[first])
            first += 1
        else:
            merged.append(right[second])
            second += 1
    merged.extend(left[first:])
    merged.extend(right[second:])
    return merged


__all__ = [
    "ConversationTranscript",
    "ConversationTranscriptOptions",
    "EntryLoader",
    "TranscriptEntryItem",
    "TranscriptEarlierItem",
    "TranscriptGapItem",
    "TranscriptItem",
    "TranscriptView",
]

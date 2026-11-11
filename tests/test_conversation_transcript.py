"""Translated v1.5.0 ``conversation-transcript`` suite.

Covers ``ConversationTranscript``: the client's whole transcript merged from windowed views, the
``earlier`` marker, gaps opened by a window that does not continue the transcript, page-by-page
fills, shared concurrent loads, retries of a failed load, and ``resume`` — the window that continues
a dropped watch.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, cast

import pytest
from chat_support import all_entries, chat_setup, open_chat
from session_support import context, flush

from pi_durable import ConversationTranscript, MemoryStorage
from pi_durable.context import Context
from pi_durable.harness.transcript import (
    TranscriptGapItem,
    TranscriptView,
)
from pi_durable.harness.view import ConversationView
from pi_durable.types import ConversationRecord, Cursor, EntryRecord, Page

CONVERSATION = cast(ConversationRecord, {"id": 1})


def transcript_entries(count: int) -> list[EntryRecord]:
    """Entries with IDs that have holes: every third ID is missing."""

    entries: list[EntryRecord] = []
    id = 1
    while len(entries) < count:
        if id % 3 != 0:
            entries.append(
                cast(EntryRecord, {"id": id, "conversationId": 1, "kind": "note"})
            )
        id += 1
    return entries


class Loader:
    """A loader over ``entries`` with ``Conversation.entries()`` semantics, counting its calls."""

    def __init__(self, entries: list[EntryRecord], always_cursor: bool = False) -> None:
        self.entries = entries
        self.calls = 0
        self.always_cursor = always_cursor
        self.gate: asyncio.Future[None] | None = None

    async def __call__(
        self, query: Mapping[str, Any], limit: int, _context: Context
    ) -> Page[EntryRecord, Cursor]:
        if self.gate is not None:
            await self.gate
        self.calls += 1
        return self._page(query, limit)

    def _page(self, query: Mapping[str, Any], limit: int) -> Page[EntryRecord, Cursor]:
        matching = [
            entry
            for entry in self.entries
            if (query.get("minEntryId") is None or entry["id"] >= query["minEntryId"])
            and (query.get("maxEntryId") is None or entry["id"] <= query["maxEntryId"])
        ]
        ordered = matching if query.get("order") == "ascending" else list(reversed(matching))
        items = ordered[:limit]
        page: Page[EntryRecord, Cursor] = {"items": items}
        if len(ordered) > limit or self.always_cursor:
            page["next"] = {"more": True}
        return page


class Throwing(Loader):
    """A loader whose adapter fails before it returns a promise, then recovers."""

    def __init__(self, entries: list[EntryRecord], offline: bool = True) -> None:
        super().__init__(entries)
        self.offline = offline

    async def __call__(
        self, query: Mapping[str, Any], limit: int, _context: Context
    ) -> Page[EntryRecord, Cursor]:
        self.calls += 1
        # An adapter that checks its connection before it returns a promise.
        if self.offline:
            raise RuntimeError("temporarily offline")
        return self._page(query, limit)


class Rejecting(Loader):
    """A loader whose request fails, then recovers."""

    def __init__(self, entries: list[EntryRecord]) -> None:
        super().__init__(entries)
        self.fail = True

    async def __call__(
        self, query: Mapping[str, Any], limit: int, _context: Context
    ) -> Page[EntryRecord, Cursor]:
        self.calls += 1
        if self.fail:
            raise RuntimeError("timeout")
        return self._page(query, limit)


def window(
    held: list[EntryRecord],
    before: Any = None,
    docs: dict[str, Any] | None = None,
) -> ConversationView:
    """A view value, with ``before`` only when an entry precedes the window."""

    value: dict[str, Any] = {
        "conversation": CONVERSATION,
        "entries": held,
        "docs": {} if docs is None else docs,
    }
    if before is not None:
        value["before"] = before
    return cast(ConversationView, value)


def window_of(
    all: list[EntryRecord],
    first: int,
    last: int,
    docs: dict[str, Any] | None = None,
) -> ConversationView:
    """The view of a window: ``entries`` with the entry before them in ``all``."""

    return window(all[first : last + 1], all[first - 1]["id"] if first > 0 else None, docs)


def shape(view: TranscriptView) -> list[Any]:
    """Items as a compact list: entry IDs, ``gap after..upTo``, and ``earlier``."""

    return [
        item["entry"]["id"]
        if item["kind"] == "entry"
        else f"gap {item['after']}..{item['upTo']}"
        if item["kind"] == "gap"
        else "earlier"
        for item in view["items"]
    ]


def ids_of(entries: list[EntryRecord]) -> list[int]:
    return [entry["id"] for entry in entries]


def gap_of(view: TranscriptView) -> TranscriptGapItem:
    return next(item for item in view["items"] if item["kind"] == "gap")


async def test_shows_a_first_window_under_an_earlier_marker_or_alone_when_it_starts_the_transcript() -> None:
    all = transcript_entries(20)
    later = ConversationTranscript(Loader(all))
    assert shape(later.merge(window_of(all, 15, 19))) == ["earlier", *ids_of(all[15:])]
    assert later.newest == all[19]["id"]
    whole = ConversationTranscript(Loader(all))
    assert shape(whole.merge(window_of(all, 0, 19))) == ids_of(all)


async def test_appends_keeps_entries_the_window_dropped_and_returns_the_same_items_when_nothing_is_added(
) -> None:
    all = transcript_entries(30)
    helper = ConversationTranscript(Loader(all))
    docs_a: dict[str, Any] = {"a": {"n": 1}}
    first = helper.merge(window_of(all, 10, 19, docs_a))
    grown = helper.merge(window_of(all, 10, 21, docs_a))
    assert shape(grown) == ["earlier", *ids_of(all[10:22])]
    # The window dropped its oldest; the transcript keeps them.
    spliced = helper.merge(window_of(all, 18, 21, docs_a))
    assert spliced["items"] is grown["items"]
    assert spliced is grown
    # A docs-only change: a new view, the same items.
    docs: dict[str, Any] = {"a": {"n": 2}}
    streamed = helper.merge({**window_of(all, 18, 21), "docs": docs})
    assert streamed is not grown
    assert streamed["items"] is grown["items"]
    assert streamed["docs"] is docs
    assert first["items"] is not grown["items"]


async def test_opens_a_gap_when_a_window_does_not_continue_the_transcript_and_fills_it_page_by_page() -> None:
    all = transcript_entries(200)
    load = Loader(all)
    helper = ConversationTranscript(load, {"page_size": 50})
    helper.merge(window_of(all, 150, 159))
    # A stall: the next window starts well past the newest entry held.
    stalled = helper.merge(window_of(all, 190, 199))
    gap = gap_of(stalled)
    assert gap == {"kind": "gap", "after": all[159]["id"], "upTo": all[190]["id"]}
    assert shape(stalled) == [
        "earlier",
        *ids_of(all[150:160]),
        f"gap {all[159]['id']}..{all[190]['id']}",
        *ids_of(all[190:]),
    ]
    # 30 entries are missing; one page loads them all and closes the gap.
    filled = await helper.fill_gap(gap, context)
    assert shape(filled) == ["earlier", *ids_of(all[150:])]
    assert load.calls == 1


async def test_shrinks_a_gap_from_its_start_while_more_than_a_page_is_missing() -> None:
    all = transcript_entries(200)
    helper = ConversationTranscript(Loader(all), {"page_size": 10})
    helper.merge(window_of(all, 100, 109))
    view = helper.merge(window_of(all, 190, 199))
    view = await helper.fill_gap(gap_of(view), context)
    assert gap_of(view) == {
        "kind": "gap",
        "after": all[119]["id"],
        "upTo": all[190]["id"],
    }
    while any(item["kind"] == "gap" for item in view["items"]):
        view = await helper.fill_gap(gap_of(view), context)
    assert shape(view) == ["earlier", *ids_of(all[100:])]


async def test_loads_earlier_pages_until_the_start_then_drops_the_marker() -> None:
    all = transcript_entries(45)
    load = Loader(all)
    helper = ConversationTranscript(load, {"page_size": 20})
    helper.merge(window_of(all, 40, 44))
    view = await helper.load_earlier(context)
    assert shape(view) == ["earlier", *ids_of(all[20:])]
    # A page without a next cursor reaches the start.
    view = await helper.load_earlier(context)
    assert shape(view) == ids_of(all)
    calls = load.calls
    assert await helper.load_earlier(context) is view
    assert load.calls == calls


async def test_shares_one_load_between_concurrent_calls_of_load_earlier_and_of_fill_gap_for_one_gap() -> None:
    all = transcript_entries(100)
    gate: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    load = Loader(all)
    load.gate = gate
    helper = ConversationTranscript(load)
    helper.merge(window_of(all, 40, 49))
    stalled = helper.merge(window_of(all, 90, 99))
    earlier = [helper.load_earlier(context), helper.load_earlier(context)]
    fills = [helper.fill_gap(gap_of(stalled), context), helper.fill_gap(gap_of(stalled), context)]
    assert earlier[0] is earlier[1]
    assert fills[0] is fills[1]
    gate.set_result(None)
    await asyncio.gather(*earlier, *fills)
    assert load.calls == 2
    assert shape(await helper.load_earlier(context)) == ids_of(all)


async def test_resumes_after_newest_an_empty_window_right_after_it_adds_nothing_one_past_it_opens_a_gap(
) -> None:
    all = transcript_entries(30)
    helper = ConversationTranscript(Loader(all))
    view = helper.merge(window_of(all, 10, 19))
    # A window that is empty but starts right after `newest` continues it; one that starts past it
    # leaves the entries up to its `before` missing.
    assert helper.merge(window([], helper.newest))["items"] is view["items"]
    past = helper.merge(window([], all[25]["id"]))
    assert shape(past)[-1] == f"gap {all[19]['id']}..{all[25]['id'] + 1}"
    assert helper.newest == all[25]["id"]
    # The next window continues it.
    assert shape(helper.merge(window_of(all, 26, 27)))[-3:] == [
        f"gap {all[19]['id']}..{all[25]['id'] + 1}",
        all[26]["id"],
        all[27]["id"],
    ]


async def test_fills_a_gap_from_a_window_that_covers_it() -> None:
    all = transcript_entries(100)
    helper = ConversationTranscript(Loader(all))
    helper.merge(window_of(all, 40, 49))
    helper.merge(window_of(all, 90, 99))
    # A wider window, such as a new watch with a larger `max`, covers the second half of the gap.
    view = helper.merge(window_of(all, 70, 99))
    assert shape(view) == [
        "earlier",
        *ids_of(all[40:50]),
        f"gap {all[49]['id']}..{all[70]['id']}",
        *ids_of(all[70:]),
    ]


async def test_rejects_load_earlier_before_the_first_merge_and_returns_the_view_for_a_gap_that_is_gone(
) -> None:
    all = transcript_entries(10)
    helper = ConversationTranscript(Loader(all))
    with pytest.raises(RuntimeError, match="merge a view first"):
        await helper.load_earlier(context)
    view = helper.merge(window_of(all, 0, 9))
    stale = cast(TranscriptGapItem, {"kind": "gap", "after": 1, "upTo": 2})
    assert await helper.fill_gap(stale, context) is view


async def test_retries_a_load_whose_loader_threw_synchronously_for_load_earlier_and_fill_gap() -> None:
    all = transcript_entries(100)
    load = Throwing(all)
    helper = ConversationTranscript(load)
    helper.merge(window_of(all, 40, 49))
    stalled = helper.merge(window_of(all, 90, 99))
    with pytest.raises(RuntimeError, match="temporarily offline"):
        await helper.load_earlier(context)
    with pytest.raises(RuntimeError, match="temporarily offline"):
        await helper.fill_gap(gap_of(stalled), context)
    load.offline = False
    assert shape(await helper.load_earlier(context)) == [
        *ids_of(all[:50]),
        f"gap {all[49]['id']}..{all[90]['id']}",
        *ids_of(all[90:]),
    ]
    assert shape(await helper.fill_gap(gap_of(stalled), context)) == ids_of(all)
    assert load.calls == 4


async def test_retries_a_load_that_rejected() -> None:
    all = transcript_entries(30)
    load = Rejecting(all)
    helper = ConversationTranscript(load)
    helper.merge(window_of(all, 20, 29))
    with pytest.raises(RuntimeError, match="timeout"):
        await helper.load_earlier(context)
    load.fail = False
    assert shape(await helper.load_earlier(context)) == ids_of(all)


async def test_keeps_one_gap_for_an_empty_window_whose_before_lies_inside_it() -> None:
    all = transcript_entries(100)
    helper = ConversationTranscript(Loader(all))
    helper.merge(window_of(all, 40, 49))
    before = helper.merge(window_of(all, 90, 99))
    view = helper.merge(window([], all[65]["id"], before["docs"]))
    assert view["items"] is before["items"]
    assert [item for item in view["items"] if item["kind"] == "gap"] == [gap_of(before)]


async def test_shares_one_load_between_a_stale_gap_descriptor_and_the_gap_it_shrank_into() -> None:
    all = transcript_entries(100)
    gate: asyncio.Future[None] = asyncio.get_running_loop().create_future()
    load = Loader(all)
    load.gate = gate
    helper = ConversationTranscript(load, {"page_size": 10})
    helper.merge(window_of(all, 10, 19))
    stale = gap_of(helper.merge(window_of(all, 90, 99)))
    gate.set_result(None)
    current = gap_of(await helper.fill_gap(stale, context))
    assert current != stale
    calls = load.calls
    a, b = helper.fill_gap(stale, context), helper.fill_gap(current, context)
    assert a is b
    await a
    assert load.calls == calls + 1


async def test_treats_an_empty_page_with_a_next_cursor_as_the_end_of_the_range() -> None:
    all = transcript_entries(60)
    # A remote loader that always returns a cursor.
    load = Loader(all, always_cursor=True)
    helper = ConversationTranscript(load, {"page_size": 100})
    helper.merge(window_of(all, 10, 19))
    view = helper.merge(window_of(all, 40, 49))
    view = await helper.fill_gap(gap_of(view), context)
    # The first page held every missing entry; the next one is empty and closes the gap.
    view = await helper.fill_gap(gap_of(view), context)
    assert not any(item["kind"] == "gap" for item in view["items"])
    await helper.load_earlier(context)
    view = await helper.load_earlier(context)
    assert shape(view) == ids_of(all[:50])


async def test_resumes_with_resume_undefined_before_a_merge_for_an_empty_transcript_and_after_the_newest(
) -> None:
    all = transcript_entries(30)
    helper = ConversationTranscript(Loader(all))
    assert helper.resume is None
    helper.merge(window([], None))
    assert helper.resume == {}
    helper.merge(window_of(all, 0, 9))
    assert helper.resume == {"from": all[9]["id"] + 1}


async def test_follows_the_walkthrough_live_appends_a_stall_fill_gap_load_earlier_and_a_resume_with_from(
) -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]

    async def note() -> EntryRecord:
        return cast(
            EntryRecord,
            await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context),
        )

    for _index in range(120):
        await note()
    helper = ConversationTranscript(
        lambda query, limit, call_context: root.entries(
            cast(Any, dict(query)), limit, None, call_context
        )
    )
    window = {"min": 10, "max": 20}
    watch = await root.watch(context, window)
    view = helper.merge(watch.value)

    async def listener(call: Any, _context: Any) -> None:
        nonlocal view
        if call["kind"] == "value":
            view = helper.merge(call["value"])

    watch.start(listener)
    for _index in range(15):
        await note()
    await flush()
    assert not any(item["kind"] == "gap" for item in view["items"])
    assert len([item for item in view["items"] if item["kind"] == "entry"]) == 25
    # A drop: the client misses 40 entries, more than `max`, and resumes after the newest it has.
    await watch.stop()
    for _index in range(40):
        await note()
    resumed = await root.watch(context, {**window, **cast(dict[str, Any], helper.resume)})
    view = helper.merge(resumed.value)
    assert len([item for item in view["items"] if item["kind"] == "gap"]) == 1
    while any(item["kind"] == "gap" for item in view["items"]):
        view = await helper.fill_gap(gap_of(view), context)
    while view["items"] and view["items"][0]["kind"] == "earlier":
        view = await helper.load_earlier(context)
    entries = [item["entry"]["id"] for item in view["items"] if item["kind"] == "entry"]
    assert entries == ids_of(await all_entries(root))
    # A short drop resumes without a gap.
    for _index in range(3):
        await note()
    again = await root.watch(context, {**window, **cast(dict[str, Any], helper.resume)})
    assert len(again.value["entries"]) == 3
    view = helper.merge(again.value)
    assert all(item["kind"] == "entry" for item in view["items"])
    await resumed.stop()
    await again.stop()
    await harness.close(context)

"""Translated v1.5.0 ``harness-view`` suite.

Covers the conversation view mount: hydration of the newest window and the built-in documents, one
frame per touching commit, structural sharing, head markers kept in place, forks, retirement,
overflow, one window per attached consumer, close, cancellation, listener isolation, and unrelated
commits. Upstream's one ``observeConversation`` case is #44-dependent by name: the synchronous
observer seam does not exist until #44 lands, so its watch-and-state halves are translated here and
the observer half is recorded in the audit report.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest
from chat_support import all_entries, assistant_message, chat_setup, open_chat
from session_support import context, flush
from task_support import deferred, eventually, with_signal

from pi_durable import (
    AgentDoc,
    InboxDoc,
    LiveDoc,
    MemoryStorage,
    ProviderDoc,
    UsageDoc,
    define_doc,
)
from pi_durable.context import AbortController
from pi_durable.delta import apply_immutable
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage
from pi_durable.types import EntryRecord

MOUNTED = ["pi.agent", "pi.inbox", "pi.live", "pi.provider", "pi.usage"]


async def record(conversation: Any, window: Any = None) -> dict[str, Any]:
    """Start a watch of ``conversation`` that records its acquisition revision and every frame."""

    watch = await conversation.watch(context, window)
    initial = watch.value
    frames: list[dict[str, Any]] = []

    async def listener(call: Any, _context: Any) -> None:
        frames.append({"value": call["value"], "ops": call["ops"]})

    watch.start(listener)
    return {"watch": watch, "initial": initial, "frames": frames, "stop": watch.stop}


async def fresh(conversation: Any, window: Any = None) -> Any:
    """A freshly built view of ``conversation``, for comparing with an advanced one."""

    watch = await conversation.watch(context, window)
    value = watch.value
    await watch.stop()
    return value


async def committed(harness: Any, conversation: Any, record_value: Any, size: int) -> Any:
    """The view as committed state defines it, read without any mount: the newest ``size`` entries,
    the entry before them, and the built-in documents.
    """

    docs: dict[str, Any] = {}
    for token in (AgentDoc, LiveDoc, InboxDoc, ProviderDoc, UsageDoc):
        mounted = await harness.snapshot(token, conversation.id, context)
        if mounted is not None:
            docs[token.definition.kind] = mounted
    newest = list((await conversation.entries({}, size + 1, None, context))["items"])
    before = newest.pop()["id"] if len(newest) > size else None
    view: dict[str, Any] = {
        "conversation": record_value,
        "entries": list(reversed(newest)),
        "docs": docs,
    }
    if before is not None:
        view["before"] = before
    return view


def replay(initial: Any, frames: list[dict[str, Any]]) -> Any:
    """Replay every frame's operations from ``initial``, checking each delivered revision."""

    value = initial
    for frame in frames:
        value = apply_immutable(value, frame["ops"])
        assert value == frame["value"]
    return value


def answer(text: str) -> dict[str, Any]:
    return assistant_message([{"type": "text", "text": text}])


async def drained() -> None:
    await flush()


def touches(harness: Any, conversation: Any) -> dict[str, int]:
    counter = {"count": 0}

    def listener(publication: Any, _context: Any) -> None:
        touched = False
        for change in publication["changes"]:
            if change["type"] == "entry" and change["value"]["conversationId"] == conversation.id:
                touched = True
            if (
                change["type"] == "document"
                and change["conversationId"] == conversation.id
                and change["record"]["kind"] in MOUNTED
                and change["ops"]
            ):
                touched = True
        if touched:
            counter["count"] += 1

    harness.subscribe_commits(listener)
    return counter


def kinds(value: Any) -> list[str]:
    return [entry["kind"] for entry in value["entries"]]


def note_change(conversation: Any, kind: str, index: int) -> Callable[[Any], Any]:
    """The commit change that appends one numbered entry of ``kind`` to ``conversation``."""

    async def change(tx: Any) -> EntryRecord:
        return cast(EntryRecord, await tx.append_entry(conversation.id, {"kind": kind, "data": index}))

    return change


async def notes(conversation: Any, count: int, kind: str = "note") -> list[EntryRecord]:
    """Append one entry of ``kind`` per commit; returns the entries."""

    written: list[EntryRecord] = []
    for index in range(count):
        entry = await conversation.commit(note_change(conversation, kind, index), context)
        written.append(cast(EntryRecord, entry))
    return written


def ids(entries: Any) -> list[int]:
    return [entry["id"] for entry in entries]


async def test_hydrates_the_newest_entries_and_the_builtin_documents() -> None:
    setup = chat_setup()
    setup.faux.set_responses([answer("hello")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    view = await fresh(root)
    assert view["conversation"] == {"id": root.id}
    assert view["entries"] == await all_entries(root)
    assert "before" not in view
    assert sorted(view["docs"]) == MOUNTED
    assert view["docs"]["pi.live"] == {}
    assert view["docs"]["pi.inbox"] == {"items": []}
    await harness.close(context)


async def test_publishes_one_frame_per_touching_commit_whose_operations_rebuild_every_revision() -> None:
    setup = chat_setup()
    release = deferred()

    async def held(_request: Any, _options: Any) -> Any:
        await release.promise
        return answer("a longer answer")

    setup.faux.set_responses([held])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    recorded = await record(root)
    initial, frames = recorded["initial"], recorded["frames"]
    touching = touches(harness, root)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await eventually(
        lambda: any(
            frame["value"]["docs"].get("pi.live", {}).get("generation") is not None
            for frame in frames
        )
    )
    release.resolve()
    await submission.wait(context)
    await harness.wait_for_idle(context)
    await drained()
    assert len(frames) == touching["count"]
    assert replay(initial, frames) == await committed(
        harness, root, initial["conversation"], len(frames[-1]["value"]["entries"])
    )
    assert any(
        op[0] == "p" and op[1] == ["entries"] and op[4][0]["kind"] == "pi.user"
        for op in frames[0]["ops"]
    )
    # Document operations keep their exact shape under the mount path.
    assert ["s", ["docs", "pi.live", "generation"], {"attempt": 1}] in [
        op for frame in frames for op in frame["ops"]
    ]
    await recorded["stop"]()
    await harness.close(context)


async def test_shares_unchanged_parts_between_revisions_and_skips_commits_that_touch_nothing_mounted(
) -> None:
    setup = chat_setup()
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    other = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    recorded = await record(root)
    initial, frames = recorded["initial"], recorded["frames"]
    await other.commit(lambda tx: tx.append_entry(other.id, {"kind": "note"}), context)
    await root.configure({"thinkingLevel": "high"}, context)
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    await drained()
    assert len(frames) == 2
    assert frames[0]["ops"] == [["s", ["docs", "pi.agent", "thinkingLevel"], "high"]]
    assert frames[0]["value"]["entries"] is initial["entries"]
    assert frames[0]["value"]["docs"]["pi.live"] is initial["docs"]["pi.live"]
    assert frames[1]["value"]["docs"] is frames[0]["value"]["docs"]
    await recorded["stop"]()
    await harness.close(context)


async def test_keeps_head_markers_in_place_a_summary_and_a_reset_are_appended_entries() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]

    async def note(kind: str) -> Any:
        return await root.commit(lambda tx: tx.append_entry(root.id, {"kind": kind}), context)

    await note("a")
    b = await note("b")
    await note("c")
    recorded = await record(root)
    initial, frames = recorded["initial"], recorded["frames"]
    summary = await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "summary", "head": b["id"]}), context
    )
    await note("d")
    await root.reset(None, context)
    await drained()
    assert [kinds(frame["value"]) for frame in frames] == [
        ["a", "b", "c", "summary"],
        ["a", "b", "c", "summary", "d"],
        ["a", "b", "c", "summary", "d", "pi.reset"],
    ]
    assert frames[0]["ops"] == [["p", ["entries"], 3, 0, [summary]]]
    # The summary's ``head`` tells a renderer where the entries it summarizes end.
    assert frames[0]["value"]["entries"][3]["head"] == b["id"]
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"], 6)
    await recorded["stop"]()
    await harness.close(context)


async def test_shows_a_forks_inherited_entries_in_id_order_its_head_markers_in_place() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    a = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "a"}), context)
    b = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "b"}), context)
    fork = await root.fork(b["id"], {"ownership": {"kind": "ownerless"}}, context)
    recorded = await record(fork)
    initial, frames = recorded["initial"], recorded["frames"]
    await fork.commit(
        lambda tx: tx.append_entry(fork.id, {"kind": "summary", "head": b["id"]}), context
    )
    await drained()
    assert ids(initial["entries"]) == [a["id"], b["id"]]
    assert kinds(frames[0]["value"]) == ["a", "b", "summary"]
    assert frames[0]["value"] == await committed(harness, fork, initial["conversation"], 3)
    await recorded["stop"]()
    await harness.close(context)


async def test_shows_a_forks_inherited_entries_and_follows_only_the_forks_own_commits() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    first = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "first"}), context)
    fork = await root.fork(first["id"], {"ownership": {"kind": "ownerless"}}, context)
    recorded = await record(fork)
    initial, frames = recorded["initial"], recorded["frames"]
    assert kinds(initial) == ["first"]
    assert initial["conversation"]["parent"] == {"conversationId": root.id, "at": first["id"]}
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "parent"}), context)
    await fork.commit(lambda tx: tx.append_entry(fork.id, {"kind": "child"}), context)
    await drained()
    assert [kinds(frame["value"]) for frame in frames] == [["first", "child"]]
    await recorded["stop"]()
    await harness.close(context)


async def test_unmounts_a_retired_document_and_mounts_its_recreation_whole() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    recorded = await record(root)
    frames = recorded["frames"]
    await root.commit(lambda tx: tx.retire_doc(LiveDoc, root.id), context)

    async def recreate(tx: Any) -> None:
        live = await tx.doc(LiveDoc, root.id)
        live["tools"] = []

    await root.commit(recreate, context)
    await drained()
    assert [frame["ops"] for frame in frames] == [
        [["d", ["docs", "pi.live"]]],
        [["s", ["docs", "pi.live"], {"tools": []}]],
    ]
    assert frames[0]["value"]["docs"].get("pi.live") is None
    await recorded["stop"]()
    await harness.close(context)


async def test_folds_undelivered_commits_into_one_call_whose_ops_reach_the_newest_view() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    watch = await root.watch(context)
    acquisition = watch.value
    for _index in range(101):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    frames: list[dict[str, Any]] = []

    async def listener(call: Any, _context: Any) -> None:
        frames.append({"value": call["value"], "ops": call["ops"]})

    watch.start(listener)
    await drained()
    assert len(frames) == 1
    assert frames[0]["ops"][0][0] != "r"
    assert apply_immutable(acquisition, frames[0]["ops"]) == frames[0]["value"]
    # The default window took 100 appends, then dropped its oldest 51 at the 101st, down to 50.
    assert frames[0]["value"] == await committed(harness, root, acquisition["conversation"], 50)
    await watch.stop()
    await harness.close(context)


async def test_keeps_watches_of_one_conversation_independent_and_remounts_after_the_last_one_detaches(
) -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    kept = await record(root)
    recorded = await record(root)
    frames = recorded["frames"]
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "one"}), context)
    await drained()
    assert kinds(kept["frames"][-1]["value"]) == ["one"]
    await recorded["stop"]()
    assert len(frames) == 1
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "two"}), context)
    await drained()
    last = kept["frames"][-1]["value"]
    assert kinds(last) == ["one", "two"]
    await kept["stop"]()
    # No observer is left, so the mount was dropped: a new observer builds a new revision.
    rebuilt = await fresh(root)
    assert rebuilt == last
    assert rebuilt is not last
    await harness.close(context)


async def test_ends_watches_at_close_and_rejects_later_acquisition() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    watch = await root.watch(context)
    await harness.close(context)
    assert await watch.closed == {"kind": "ended"}
    assert watch.value["entries"] == []
    with pytest.raises(Exception, match="closed"):
        await root.watch(context)


async def test_rejects_an_acquisition_cancelled_or_closed_while_it_waits_for_the_session_line() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]

    def hold() -> tuple[Any, Any]:
        release = deferred()

        async def blocked(_tx: Any) -> None:
            await release.promise

        return release, asyncio.ensure_future(root.commit(blocked, context))

    release, blocking = hold()
    await asyncio.sleep(0)
    controller = AbortController()
    cancelled = root.watch(with_signal(controller.signal))
    controller.abort(RuntimeError("cancelled"))
    release.resolve()
    await blocking
    with pytest.raises(RuntimeError, match="cancelled"):
        await cancelled

    release, blocking = hold()
    await asyncio.sleep(0)
    closed_while_queued = root.watch(context)
    await asyncio.sleep(0)
    closing = asyncio.ensure_future(harness.close(context))
    release.resolve()
    await blocking
    with pytest.raises(RuntimeError, match="Harness is closed"):
        await closed_while_queued
    await closing


async def test_shares_one_mount_between_concurrent_observers_and_isolates_a_failing_listener() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    failing = await root.watch(context)
    recorded = await record(root)
    frames = recorded["frames"]
    shared = await root.watch(context)
    # Each consumer has its own window; the documents are the mount's.
    assert failing.value == shared.value
    assert failing.value["docs"] is shared.value["docs"]
    await shared.stop()

    async def listener(_call: Any, _context: Any) -> None:
        raise RuntimeError("listener failed")

    failing.start(listener)
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "one"}), context)
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "two"}), context)
    await drained()
    assert (await failing.closed)["kind"] == "listener_error"
    assert len(frames) == 2
    await recorded["stop"]()
    await harness.close(context)


async def test_publishes_one_frame_for_a_commit_that_appends_several_entries_and_edits_a_document() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    other = define_doc(
        {
            "kind": "app.other",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "initial",
            "initial": lambda: {"n": 0},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    recorded = await record(root)
    initial, frames = recorded["initial"], recorded["frames"]

    async def change(tx: Any) -> None:
        await tx.append_entry(root.id, {"kind": "a"})
        live = await tx.doc(LiveDoc, root.id)
        live["tools"] = []
        await tx.append_entry(root.id, {"kind": "b"})

    await root.commit(change, context)

    # A document that is not mounted publishes nothing.
    async def unrelated(tx: Any) -> None:
        doc = await tx.doc(other, root.id)
        doc["n"] = 1

    await root.commit(unrelated, context)
    await drained()
    assert len(frames) == 1
    assert kinds(frames[0]["value"]) == ["a", "b"]
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"], 2)
    await recorded["stop"]()
    await harness.close(context)


async def test_starts_with_the_newest_min_entries_and_the_entry_before_them_or_the_whole_short_transcript(
) -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    written = await notes(root, 10)
    view = await fresh(root, {"min": 4})
    assert ids(view["entries"]) == ids(written[6:])
    assert view["before"] == written[5]["id"]
    # The default window: the newest 50, here all ten, starting the transcript.
    all_view = await fresh(root)
    assert ids(all_view["entries"]) == ids(written)
    assert "before" not in all_view
    await harness.close(context)


async def test_takes_appends_up_to_max_then_drops_its_oldest_in_one_splice_down_to_min() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    written = await notes(root, 4)
    recorded = await record(root, {"min": 3, "max": 5})
    initial, frames = recorded["initial"], recorded["frames"]
    assert ids(initial["entries"]) == ids(written[1:])
    appended = await notes(root, 3)
    await drained()
    assert [len(frame["value"]["entries"]) for frame in frames] == [4, 5, 3]
    e5, e6, e7 = appended
    assert frames[0]["ops"] == [["p", ["entries"], 3, 0, [e5]]]
    # One splice drops the oldest and moves ``before`` to the last entry that left.
    assert frames[2]["ops"] == [
        ["p", ["entries"], 5, 0, [e7]],
        ["p", ["entries"], 0, 3, []],
        ["s", ["before"], written[3]["id"]],
    ]
    assert ids(frames[2]["value"]["entries"]) == ids([e5, e6, e7])
    assert frames[2]["value"]["before"] == written[3]["id"]
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"], 3)
    # Entries that stay keep their identity: a splice moves them, it never copies them.
    assert frames[2]["value"]["entries"][0] is frames[1]["value"]["entries"][3]
    await recorded["stop"]()
    await harness.close(context)


async def test_keeps_the_newest_min_of_a_commit_that_appends_more_than_max_at_once() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    await notes(root, 2)
    recorded = await record(root, {"min": 2, "max": 3})
    initial, frames = recorded["initial"], recorded["frames"]

    async def burst(tx: Any) -> list[EntryRecord]:
        entries: list[EntryRecord] = []
        for _index in range(5):
            entries.append(
                cast(EntryRecord, await tx.append_entry(root.id, {"kind": "burst"}))
            )
        return entries

    written = cast(list[EntryRecord], await root.commit(burst, context))
    await drained()
    assert len(frames) == 1
    assert ids(frames[0]["value"]["entries"]) == ids(written[3:])
    assert frames[0]["value"]["before"] == written[2]["id"]
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"], 2)
    await recorded["stop"]()
    await harness.close(context)


async def test_starts_at_from_when_the_entries_since_fit_in_max_otherwise_at_the_newest_min() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    written = await notes(root, 30)
    window = {"min": 5, "max": 10}
    # Ten entries since `from`: all of them, with the entry before `from`.
    within = await fresh(root, {**window, "from": written[20]["id"]})
    assert ids(within["entries"]) == ids(written[20:])
    assert within["before"] == written[19]["id"]
    # Fewer than `min` since `from`: only those.
    short = await fresh(root, {**window, "from": written[28]["id"]})
    assert ids(short["entries"]) == ids(written[28:])
    assert short["before"] == written[27]["id"]
    # Resuming after the newest entry: an empty window right after it.
    resumed = await fresh(root, {**window, "from": written[29]["id"] + 1})
    assert resumed["entries"] == []
    assert resumed["before"] == written[29]["id"]
    # Eleven since `from`: more than `max`, so the newest `min`; `before` shows the client a gap.
    beyond = await fresh(root, {**window, "from": written[19]["id"]})
    assert ids(beyond["entries"]) == ids(written[25:])
    assert beyond["before"] == written[24]["id"]
    # From the transcript's start.
    whole = await fresh(root, {"min": 40, "from": written[0]["id"]})
    assert ids(whole["entries"]) == ids(written)
    assert "before" not in whole
    await harness.close(context)


async def test_advances_a_resumed_window_by_appends_from_its_start() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    written = await notes(root, 5)
    recorded = await record(root, {"from": written[4]["id"] + 1})
    frames = recorded["frames"]
    assert frames == []
    later = await notes(root, 2)
    await drained()
    assert ids(frames[-1]["value"]["entries"]) == ids(later)
    assert frames[-1]["value"]["before"] == written[4]["id"]
    assert replay(recorded["initial"], frames) == frames[-1]["value"]
    await recorded["stop"]()
    await harness.close(context)


async def test_uses_from_only_for_the_first_value_a_from_past_the_newest_starts_empty_and_takes_appends(
) -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    written = await notes(root, 3)
    from_id = written[2]["id"] + 1000
    recorded = await record(root, {"from": from_id})
    frames = recorded["frames"]
    assert recorded["initial"]["entries"] == []
    assert recorded["initial"]["before"] == written[2]["id"]
    # An entry below `from` is still appended: the window continues from its attachment.
    next_entry = (await notes(root, 1))[0]
    assert next_entry["id"] < from_id
    await drained()
    assert frames[-1]["value"]["entries"] == [next_entry]
    assert frames[-1]["value"]["before"] == written[2]["id"]
    await recorded["stop"]()
    await harness.close(context)


async def test_rejects_window_bounds_that_are_not_integers_or_put_max_below_min() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    with pytest.raises(ValueError, match="Window min"):
        await root.watch(context, {"min": 0})
    with pytest.raises(ValueError, match="Window min"):
        await root.watch(context, {"min": 1.5})
    with pytest.raises(ValueError, match="Window min"):
        await root.watch(context, {"min": True})
    with pytest.raises(ValueError, match="Window max"):
        await root.watch(context, {"min": 10, "max": 9})
    with pytest.raises(ValueError, match="Window max"):
        await root.watch(context, {"max": 1.5})
    with pytest.raises(ValueError, match="Window from"):
        await root.watch(context, {"from": float("nan")})
    # Upstream also refuses `harness.observeConversation(id, observer, context, { max: 0 })`:
    # #44 owns that seam, and `resolve_window` runs on the same path both take.
    # `max` may equal `min`.
    watch = await root.watch(context, {"min": 3, "max": 3})
    await watch.stop()
    await harness.close(context)


async def test_gives_each_consumer_its_own_window_over_one_mount() -> None:
    """Translated from upstream "gives each observer its own window over one mount".

    Upstream attaches the narrow consumer with ``harness.observeConversation(id, observer, context,
    window)``; that observer seam is #44's to add, so this translates the half that exists today:
    two watches of one mount, each with its own window and the same document values.
    """

    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    written = await notes(root, 6)
    narrow = await record(root, {"min": 2, "max": 3})
    wide = await record(root, {"min": 5})
    assert ids(narrow["initial"]["entries"]) == ids(written[4:])
    assert ids(wide["initial"]["entries"]) == ids(written[1:])
    next_entry = (await notes(root, 1))[0]
    await drained()
    assert len(narrow["frames"]) == 1
    assert len(wide["frames"]) == 1
    assert ids(narrow["frames"][0]["value"]["entries"]) == ids([*written[4:], next_entry])
    assert ids(wide["frames"][0]["value"]["entries"]) == ids([*written[1:], next_entry])
    # Two windows, one mount: the entries differ, the documents are the same object.
    assert narrow["frames"][0]["value"]["docs"] is wide["frames"][0]["value"]["docs"]
    await narrow["stop"]()
    await wide["stop"]()
    await harness.close(context)


async def test_bounds_an_overflow_replacement_by_max() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    watch = await root.watch(context, {"min": 5, "max": 6})
    # Past 10,000 pending operations the next call is one root replacement: here an append per
    # commit, and a splice and a `before` every other commit.
    for _index in range(6000):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    frames: list[dict[str, Any]] = []

    async def listener(call: Any, _context: Any) -> None:
        frames.append({"value": call["value"], "ops": call["ops"]})

    watch.start(listener)
    await drained()
    assert len(frames) == 1
    assert frames[0]["ops"] == [["r", frames[0]["value"]]]
    assert len(frames[0]["value"]["entries"]) <= 6
    await watch.stop()
    await harness.close(context)


async def test_lets_a_long_lived_watch_drop_the_entries_that_left_its_window(
    tmp_path: Path,
) -> None:
    # On SQLite, which keeps no entry objects, so only the watch can hold an old one.
    #
    # Upstream also holds a ``WeakRef`` to the first entry the watch ever saw and asserts it is
    # collected once the window moves past it. CPython takes no weak reference to a ``list`` or a
    # ``dict``, and a committed entry record is a frozen ``dict``, so the port asserts what that
    # probe protected: the long-lived watch holds exactly its current window and nothing of what
    # left it.
    storage = await open_posix_sqlite_storage(str(tmp_path / "session.sqlite3"))
    opened = await open_chat(storage, chat_setup())
    harness, root = opened["harness"], opened["root"]
    watch = await root.watch(context, {"min": 5, "max": 10})
    seen: list[list[EntryRecord]] = []

    async def listener(call: Any, _context: Any) -> None:
        if call["kind"] == "value":
            seen.append(call["value"]["entries"])

    watch.start(listener)
    await notes(root, 1)
    await eventually(lambda: bool(seen))
    first = seen[0][0]
    written = await notes(root, 200)
    await drained()
    latest = [first, *written]
    assert len(seen[-1]) <= 10
    # The value the long-lived watch carries now is the newest suffix of the transcript.
    assert ids(seen[-1]) == ids(latest[-len(seen[-1]) :])
    # The entry the window started on is gone from every value the watch now carries.
    assert first not in watch.value["entries"]
    await watch.stop()
    await harness.close(context)

"""Translated v1.3.0 ``harness-view`` suite.

Covers the conversation view mount: hydration of the active transcript and built-in documents,
one exact frame per touching commit, structural sharing, head cuts and forks, retirement,
overflow, independent observers, close, cancellation, listener isolation, and unrelated commits.
"""

from __future__ import annotations

import asyncio
from typing import Any

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

MOUNTED = ["pi.agent", "pi.inbox", "pi.live", "pi.provider", "pi.usage"]


async def record(conversation: Any) -> dict[str, Any]:
    """Start a watch that records its acquisition revision and every delivered frame."""

    watch = await conversation.watch(context)
    initial = watch.value
    frames: list[dict[str, Any]] = []

    async def listener(value: Any, ops: Any, _context: Any) -> None:
        frames.append({"value": value, "ops": ops})

    watch.start(listener)
    return {"watch": watch, "initial": initial, "frames": frames}


async def fresh(conversation: Any) -> Any:
    """A freshly built view of ``conversation``, for comparing with an advanced one."""

    state = await conversation.view_state(context)
    value = state.value
    state.dispose()
    return value


async def committed(harness: Any, conversation: Any, record_value: Any) -> Any:
    """The view as committed state defines it, read without any mount."""

    docs: dict[str, Any] = {}
    for token in (AgentDoc, LiveDoc, InboxDoc, ProviderDoc, UsageDoc):
        value = await harness.snapshot(token, conversation.id, context)
        if value is not None:
            docs[token.definition.kind] = value
    return {
        "conversation": record_value,
        "entries": (await conversation.context(context))["entries"],
        "docs": docs,
    }


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


async def test_hydrates_the_active_entries_and_the_builtin_documents() -> None:
    setup = chat_setup()
    setup.faux.set_responses([answer("hello")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    view = await fresh(root)
    assert view["conversation"] == {"id": root.id}
    assert view["entries"] == await all_entries(root)
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
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"])
    assert any(
        op[0] == "p" and op[1] == ["entries"] and op[4][0]["kind"] == "pi.user"
        for op in frames[0]["ops"]
    )
    assert ["s", ["docs", "pi.live", "generation"], {"attempt": 1}] in [
        op for frame in frames for op in frame["ops"]
    ]
    await recorded["watch"].stop()
    await harness.close(context)


async def test_shares_unchanged_parts_and_skips_commits_that_touch_nothing_mounted() -> None:
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
    await recorded["watch"].stop()
    await harness.close(context)


async def test_cuts_the_entries_at_a_head_marker_keeping_the_entries_from_its_head() -> None:
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
        ["summary", "b", "c"],
        ["summary", "b", "c", "d"],
        ["pi.reset"],
    ]
    assert frames[0]["ops"] == [["p", ["entries"], 0, 1, [summary]]]
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"])
    await recorded["watch"].stop()
    await harness.close(context)


async def test_keeps_only_mounted_entries_for_a_raw_head_write_before_the_active_range() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    old = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "old"}), context)
    await root.reset(None, context)
    recorded = await record(root)
    frames = recorded["frames"]
    await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "summary", "head": old["id"]}), context
    )
    await drained()
    # Model context now starts at `old` again, but the mount never held it; a rebuilt mount shows it.
    assert kinds(frames[0]["value"]) == ["summary"]
    await recorded["watch"].stop()
    assert kinds(await fresh(root)) == ["summary", "old"]
    await harness.close(context)


async def test_cuts_a_forks_view_into_its_inherited_entries() -> None:
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
    assert [entry["id"] for entry in initial["entries"]] == [a["id"], b["id"]]
    assert kinds(frames[0]["value"]) == ["summary", "b"]
    assert frames[0]["value"] == await committed(harness, fork, initial["conversation"])
    await recorded["watch"].stop()
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
    await recorded["watch"].stop()
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
    await recorded["watch"].stop()
    await harness.close(context)


async def test_replaces_undelivered_frames_with_the_newest_view_after_100_pending_frames() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    watch = await root.watch(context)
    for _index in range(101):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    frames: list[dict[str, Any]] = []

    async def listener(value: Any, ops: Any, _context: Any) -> None:
        frames.append({"value": value, "ops": ops})

    watch.start(listener)
    await drained()
    assert len(frames) == 1
    assert frames[0]["ops"] == [["r", frames[0]["value"]]]
    assert len(frames[0]["value"]["entries"]) == 101
    await watch.stop()
    await harness.close(context)


async def test_keeps_states_and_watches_independent_and_remounts_after_the_last_detaches() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    state = await root.view_state(context)
    recorded = await record(root)
    frames = recorded["frames"]
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "one"}), context)
    await drained()
    assert kinds(state.value) == ["one"]
    await recorded["watch"].stop()
    assert len(frames) == 1
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "two"}), context)
    await drained()
    assert kinds(state.value) == ["one", "two"]
    last = state.value
    state.dispose()
    # No observer is left, so the mount was dropped: a new observer builds a new revision.
    rebuilt = await fresh(root)
    assert rebuilt == last
    assert rebuilt is not last
    await harness.close(context)


async def test_ends_states_and_watches_at_close_and_rejects_later_acquisition() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    watch = await root.watch(context)
    state = await root.view_state(context)
    await harness.close(context)
    assert await watch.closed == {"reason": "session_closed"}
    assert state.value["entries"] == []
    with pytest.raises(Exception, match="closed"):
        await root.watch(context)
    with pytest.raises(Exception, match="closed"):
        await root.view_state(context)


async def test_rejects_acquisition_cancelled_or_closed_while_it_waits_for_the_session_line() -> None:
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
    shared = await root.view_state(context)
    assert failing.value is shared.value
    shared.dispose()

    async def listener(_value: Any, _ops: Any, _context: Any) -> None:
        raise RuntimeError("listener failed")

    failing.start(listener)
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "one"}), context)
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "two"}), context)
    await drained()
    assert (await failing.closed)["reason"] == "listener_error"
    assert len(frames) == 2
    await recorded["watch"].stop()
    await harness.close(context)


async def test_publishes_one_frame_for_a_commit_that_appends_entries_and_edits_a_document() -> None:
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

    async def unrelated(tx: Any) -> None:
        doc = await tx.doc(other, root.id)
        doc["n"] = 1

    await root.commit(unrelated, context)
    await drained()
    assert len(frames) == 1
    assert kinds(frames[0]["value"]) == ["a", "b"]
    assert replay(initial, frames) == await committed(harness, root, initial["conversation"])
    await recorded["watch"].stop()
    await harness.close(context)

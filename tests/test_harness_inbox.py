"""Translated upstream ``harness-inbox`` cases for queueing, boundaries, and failed runs."""

from __future__ import annotations

import json
from typing import Any

import pytest
from session_support import ControlledStorage, context, document_changes
from submission_support import (
    all_entries,
    fail,
    inbox,
    live,
    open_submissions,
    release,
    reset,
    transcript,
    wait_busy,
)

from pi_durable import InboxDoc, LiveDoc, MemoryStorage, ResetEntry


@pytest.fixture(autouse=True)
def _reset_probe() -> Any:
    reset()
    yield
    reset()


def queued_note(text: str) -> dict[str, Any]:
    return {"kind": "note", "model": [{"role": "user", "content": text, "timestamp": 0}]}


async def status(submission: Any) -> dict[str, Any]:
    return await submission.status(context)


async def test_queues_busy_submissions_and_places_writes_before_user_items_at_the_final_boundary() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    f1 = await root.submit({"type": "input", "content": "f1"}, context)
    write = await root.submit({"type": "write", "entry": queued_note("w")}, context)
    f2 = await root.submit({"type": "input", "content": "f2", "when_busy": "followUp"}, context)
    for submission in (f1, write, f2):
        assert (await status(submission))["status"] == "queued"
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (f1.id, "followUp"),
        (write.id, "write"),
        (f2.id, "followUp"),
    ]
    assert transcript(await all_entries(root)) == ["pi.user:a"]

    fail(task_id)
    assert (await input.wait(context))["status"] == "unanswered"
    await harness.wait_for_idle(context)
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (f1.id, "followUp"),
        (write.id, "write"),
        (f2.id, "followUp"),
    ]

    g = await root.submit({"type": "input", "content": "g"}, context)
    successor = await wait_busy(harness, root)
    assert transcript(await all_entries(root)) == ["pi.user:a", "note:w", "pi.user:f1"]
    assert (await status(write))["status"] == "done"
    assert (await status(f1))["status"] == "placed"
    assert (await status(f2))["status"] == "queued"
    assert (await status(g))["status"] == "queued"
    assert (await live(harness, root))["run"]["inputs"] == [f1.id]
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (f2.id, "followUp"),
        (g.id, "followUp"),
    ]
    release(successor)
    await f1.wait(context)
    await harness.close(context)


async def test_places_every_follow_up_in_one_successor_run_with_follow_up_mode_all() -> None:
    opened = await open_submissions(settings={"follow_up_mode": "all"})
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    f1 = await root.submit({"type": "input", "content": "f1"}, context)
    f2 = await root.submit({"type": "input", "content": "f2"}, context)
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)

    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert (await live(harness, root))["run"]["inputs"] == [f1.id, f2.id, g.id]
    assert transcript(await all_entries(root)) == [
        "pi.user:a",
        "pi.user:f1",
        "pi.user:f2",
        "pi.user:g",
    ]
    assert await inbox(harness, root) == []
    await harness.close(context)


async def test_places_a_queued_reset_before_user_items_so_follow_ups_run_in_the_new_context() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    await root.reset(None, context)
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)

    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    entries = await all_entries(root)
    assert transcript(entries) == ["pi.user:a", "pi.reset", "pi.user:f"]
    reset_entry = next(entry for entry in entries if ResetEntry.is_(entry))
    assert reset_entry["head"] == reset_entry["id"]
    assert (await status(follow_up))["status"] == "placed"
    assert (await status(g))["status"] == "queued"
    await harness.close(context)


async def test_makes_head_writes_stale_at_once_and_behind_waiting_items() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    old = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    await root.reset(None, context)
    before = len(await all_entries(root))
    stale_idle = await root.submit(
        {"type": "write", "entry": {"kind": "summary", "head": old["id"]}}, context
    )
    assert await stale_idle.wait(context) == {
        "id": stale_idle.id,
        "conversationId": root.id,
        "type": "write",
        "status": "unanswered",
        "reason": "stale",
    }
    assert len(await all_entries(root)) == before

    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    stale_queued = await root.submit(
        {"type": "write", "entry": {"kind": "summary", "head": old["id"]}}, context
    )
    assert (await status(stale_queued))["status"] == "queued"
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)
    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert (await stale_queued.wait(context))["status"] == "unanswered"
    assert (await stale_queued.status(context))["reason"] == "stale"
    assert (await status(g))["status"] == "placed"
    assert not any(entry["kind"] == "summary" for entry in await all_entries(root))
    await harness.close(context)


async def test_appends_a_data_only_write_mid_run_at_once_and_leaves_context_unchanged() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    await root.submit({"type": "input", "content": "a"}, context)
    await wait_busy(harness, root)
    chat = {"kind": "chat.message", "data": {"author": "bob", "text": "lunch?"}}
    write = await root.submit(
        {"type": "write", "entry": chat, "request_id": "bob:1"}, context
    )
    settled = await write.status(context)
    assert settled["status"] == "done"
    assert await inbox(harness, root) == []
    assert transcript(await all_entries(root)) == ["pi.user:a", "chat.message"]
    again = await root.submit(
        {"type": "write", "entry": chat, "request_id": "bob:1"}, context
    )
    assert again.id == write.id
    assert len(await all_entries(root)) == 2
    await harness.close(context)


async def test_queues_a_data_only_write_behind_a_queued_reset() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    await root.reset(None, context)
    note = await root.submit(
        {"type": "write", "entry": {"kind": "chat.message", "data": {"text": "x"}}},
        context,
    )
    assert (await status(note))["status"] == "queued"
    assert [(item["mode"], item["entry"]["kind"]) for item in await inbox(harness, root)] == [
        ("write", "pi.reset"),
        ("write", "chat.message"),
    ]
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)
    await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert transcript(await all_entries(root)) == ["pi.user:a", "pi.reset", "chat.message", "pi.user:g"]
    await harness.close(context)


async def test_withdraws_a_queued_submission_and_keeps_writes_across_conversation_abort() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    await wait_busy(harness, root)
    kept = await root.submit({"type": "write", "entry": queued_note("kept")}, context)
    withdrawn = await root.submit({"type": "input", "content": "f"}, context)
    assert await withdrawn.abort(context) == "aborted"
    assert (await withdrawn.wait(context))["status"] == "unanswered"
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (kept.id, "write")
    ]

    await root.abort(context)
    assert (await input.wait(context))["status"] == "unanswered"
    assert (await kept.status(context))["status"] == "queued"
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (kept.id, "write")
    ]
    await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert transcript(await all_entries(root)) == ["pi.user:a", "note:kept", "pi.user:g"]
    await harness.close(context)


async def test_leaves_the_inbox_alone_when_the_runs_task_is_aborted() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    f = await root.submit({"type": "input", "content": "f"}, context)
    assert await harness.abort_task(task_id, context) == "marked"
    assert (await input.wait(context))["status"] == "unanswered"
    assert (await input.status(context))["reason"] == "aborted"
    await harness.wait_for_idle(context)
    assert (await status(f))["status"] == "queued"
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (f.id, "followUp")
    ]
    await harness.close(context)


async def test_commits_inbox_changes_as_positional_operations_without_retained_values() -> None:
    opened = await open_submissions(ControlledStorage())
    harness, root = opened["harness"], opened["root"]
    ops: list[list[Any]] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] == "pi.inbox" and change["ops"]:
                ops.append(list(change["ops"]))

    harness.subscribe_commits(observe)
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    w1 = await root.submit({"type": "write", "entry": queued_note("w")}, context)
    f1 = await root.submit({"type": "input", "content": "f1"}, context)
    w2 = await root.submit({"type": "write", "entry": queued_note("w")}, context)
    f2 = await root.submit({"type": "input", "content": "f2"}, context)
    w3 = await root.submit({"type": "write", "entry": queued_note("w")}, context)
    assert ops == [
        [["p", ["items"], 0, 0, [{"id": w1.id, "mode": "write", "entry": queued_note("w")}]]],
        [["p", ["items"], 1, 0, [{"id": f1.id, "mode": "followUp", "content": "f1"}]]],
        [["p", ["items"], 2, 0, [{"id": w2.id, "mode": "write", "entry": queued_note("w")}]]],
        [["p", ["items"], 3, 0, [{"id": f2.id, "mode": "followUp", "content": "f2"}]]],
        [["p", ["items"], 4, 0, [{"id": w3.id, "mode": "write", "entry": queued_note("w")}]]],
    ]
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)
    ops.clear()
    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert ops
    removals = [op for bundle in ops for op in bundle if op[3] > 0]
    assert removals
    assert all(op[0] == "p" and op[4] == [] for op in removals)
    assert "f2" not in json.dumps(ops)
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (f2.id, "followUp"),
        (g.id, "followUp"),
    ]
    await harness.close(context)


async def test_writes_the_inbox_as_a_complete_base_exactly_when_it_becomes_empty() -> None:
    written: list[tuple[str, bool]] = []
    inbox_id: dict[str, int] = {}

    class RecordingStorage(MemoryStorage):
        async def commit(self, writes: Any, call_context: Any) -> Any:
            for write in writes:
                if (
                    write["type"] == "document.change"
                    and inbox_id.get("id") == write["id"]
                ):
                    content = write["content"]
                    written.append(
                        (
                            content["kind"],
                            content["kind"] == "base"
                            and len(content["value"]["items"]) == 0,
                        )
                    )
            return await super().commit(writes, call_context)

    opened = await open_submissions(RecordingStorage())
    harness, root = opened["harness"], opened["root"]
    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] == "pi.inbox":
                inbox_id["id"] = change["record"]["id"]

    harness.subscribe_commits(observe)
    input = await root.submit({"type": "input", "content": "a"}, context)
    await wait_busy(harness, root)
    await root.submit({"type": "input", "content": "f1"}, context)
    await root.submit({"type": "input", "content": "f2"}, context)
    await root.abort(context)
    await input.wait(context)
    assert written[-1] == ("base", True)
    await harness.close(context)


async def test_keeps_queued_submissions_across_reopen_and_settles_them_afterwards(tmp_path: Any) -> None:
    from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage

    path = str(tmp_path / "session.sqlite")
    opened = await open_submissions(await open_posix_sqlite_storage(path))
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    await wait_busy(harness, root)
    f = await root.submit({"type": "input", "content": "f"}, context)
    await harness.close(context)

    opened = await open_submissions(await open_posix_sqlite_storage(path))
    harness, root = opened["harness"], opened["root"]
    recovered = await harness.submission(f.id, context)
    assert recovered is not None
    harness.resume()
    first = await wait_busy(harness, root)
    release(first)
    await harness.wait_for_task(first, context)
    second = await wait_busy(harness, root)
    release(second)
    assert (await recovered.wait(context))["status"] == "done"
    assert transcript(await all_entries(root))[-2:] == ["pi.user:f", "pi.assistant:answer"]
    original = await harness.submission(input.id, context)
    assert original is not None
    assert (await original.status(context))["status"] == "done"
    await harness.close(context)


async def test_makes_a_head_write_stale_behind_a_reset_placed_in_the_same_boundary() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    target = (await all_entries(root))[0]
    await root.reset(None, context)
    summary = await root.submit(
        {"type": "write", "entry": {"kind": "summary", "head": target["id"]}}, context
    )
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)
    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert (await summary.status(context))["status"] == "unanswered"
    assert (await summary.status(context))["reason"] == "stale"
    assert (await g.status(context))["status"] == "placed"
    assert transcript(await all_entries(root)) == ["pi.user:a", "pi.reset", "pi.user:g"]
    await harness.close(context)


async def test_a_fork_starts_with_an_empty_inbox() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    await root.submit({"type": "input", "content": "a"}, context)
    await wait_busy(harness, root)
    await root.submit({"type": "input", "content": "f"}, context)
    at = (await all_entries(root))[0]["id"]
    child = await root.fork(at, {"ownership": {"kind": "ownerless"}}, context)
    state = await harness.snapshot(InboxDoc, child.id, context)
    assert state == {"items": []}
    assert await harness.snapshot(LiveDoc, child.id, context) == {}
    await harness.close(context)


async def test_a_display_only_write_queued_behind_a_reset_runs_no_boundary() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    await root.reset(None, context)
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)

    before = len(await all_entries(root))
    note = await root.submit(
        {"type": "write", "entry": {"kind": "chat.message", "data": {"text": "x"}}},
        context,
    )
    assert (await note.status(context))["status"] == "queued"
    assert await live(harness, root) == {}
    assert len(await all_entries(root)) == before
    items = await inbox(harness, root)
    assert [item["mode"] for item in items] == ["followUp", "write", "write"]
    assert items[0]["id"] == follow_up.id
    assert items[-1]["id"] == note.id
    assert items[-1]["entry"] == {"kind": "chat.message", "data": {"text": "x"}}

    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert transcript(await all_entries(root)) == [
        "pi.user:a",
        "pi.reset",
        "chat.message",
        "pi.user:f",
    ]
    assert (await g.status(context))["status"] == "queued"
    await harness.close(context)


async def test_steering_mode_all_places_every_steer_in_id_order_before_follow_ups() -> None:
    opened = await open_submissions(settings={"steering_mode": "all"})
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    first = await root.submit({"type": "input", "content": "s1", "when_busy": "steer"}, context)
    follow_up = await root.submit({"type": "input", "content": "f1"}, context)
    second = await root.submit({"type": "input", "content": "s2", "when_busy": "steer"}, context)
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)

    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    assert (await live(harness, root))["run"]["inputs"] == [
        first.id,
        follow_up.id,
        second.id,
    ]
    assert transcript(await all_entries(root)) == [
        "pi.user:a",
        "pi.user:s1",
        "pi.user:f1",
        "pi.user:s2",
    ]
    assert (await g.status(context))["status"] == "queued"
    await harness.close(context)


async def test_a_display_only_write_after_a_failed_run_neither_restarts_work_nor_places_the_inbox() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)

    note = await root.submit(
        {"type": "write", "entry": {"kind": "chat.message", "data": {"text": "x"}}},
        context,
    )
    assert (await note.status(context))["status"] == "done"
    assert await live(harness, root) == {}
    assert [(item["id"], item["mode"]) for item in await inbox(harness, root)] == [
        (follow_up.id, "followUp")
    ]
    assert transcript(await all_entries(root)) == ["pi.user:a", "chat.message"]
    await harness.close(context)

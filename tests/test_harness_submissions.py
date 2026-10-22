"""Translated upstream ``harness-submissions`` cases for the submission core."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from session_support import ControlledStorage, context, flush
from submission_support import (
    all_entries,
    open_submissions,
    release,
    reset,
    wait_busy,
)
from task_support import deferred, with_signal

from pi_durable import ConversationBusy, LiveDoc
from pi_durable.context import AbortController
from pi_durable.ids import SubmissionId


@pytest.fixture(autouse=True)
def _reset_probe() -> Any:
    reset()
    yield
    reset()


async def test_appends_an_idle_write_and_settles_it_done_without_a_turn() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit(
        {"type": "write", "entry": {"kind": "note", "data": {"text": "x"}}}, context
    )
    settled = await submission.wait(context)
    assert settled == {
        "id": submission.id,
        "conversationId": root.id,
        "type": "write",
        "status": "done",
        "entry": settled["entry"],
    }
    entries = await all_entries(root)
    assert entries == [
        {
            "id": settled["entry"],
            "conversationId": root.id,
            "kind": "note",
            "data": {"text": "x"},
        }
    ]
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    tasks = await root.commit(
        lambda tx: tx.scan_tasks({"conversationId": root.id}, 10), context
    )
    assert tasks["items"] == []
    await harness.close(context)


async def test_places_idle_input_and_rejects_busy_input_with_when_busy_reject_without_writing() -> None:
    storage = ControlledStorage()
    opened = await open_submissions(storage)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    task_id = await wait_busy(harness, root)
    record = await submission.status(context)
    assert record["status"] == "placed"
    entry = await root.commit(lambda tx: tx.entry(record["entry"]), context)
    assert entry is not None
    assert entry["model"][0]["role"] == "user"
    assert entry["model"][0]["content"] == "hi"
    assert isinstance(entry["model"][0]["timestamp"], int)
    state = await harness.snapshot(LiveDoc, root.id, context)
    assert state is not None
    assert state["run"]["inputs"] == [submission.id]
    assert (await harness.get_task(task_id, context))["kind"] == "pi.generation"

    commits = len(storage.commits)
    with pytest.raises(ConversationBusy) as rejected:
        await root.submit({"type": "input", "content": "again", "when_busy": "reject"}, context)
    assert rejected.value.conversation_id == root.id
    assert len(storage.commits) == commits
    await harness.close(context)


async def test_deduplicates_request_ids_per_conversation_before_any_write() -> None:
    storage = ControlledStorage()
    opened = await open_submissions(storage)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "hi", "request_id": "r1"}, context)
    await wait_busy(harness, root)
    commits = len(storage.commits)
    again = await root.submit(
        {"type": "input", "content": "different", "request_id": "r1"}, context
    )
    assert again.id == first.id
    assert len(storage.commits) == commits
    with pytest.raises(ValueError, match="Request r1 already identifies a submission of type input"):
        await root.submit(
            {"type": "write", "entry": {"kind": "note"}, "request_id": "r1"}, context
        )
    assert (await first.status(context))["status"] == "placed"
    assert (await first.status(context))["requestId"] == "r1"

    other = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    write = await other.submit(
        {"type": "write", "entry": {"kind": "note"}, "request_id": "r1"}, context
    )
    assert write.id != first.id
    assert (
        await other.submit(
            {"type": "write", "entry": {"kind": "note"}, "request_id": "r1"}, context
        )
    ).id == write.id
    await harness.close(context)


async def test_reports_abort_results_and_looks_submissions_up_by_conversation() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    task_id = await wait_busy(harness, root)
    assert await submission.abort(context) == "already_placed"
    assert await harness.abort_submission(submission.id, context, root.id) == "already_placed"
    other = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    assert await harness.abort_submission(submission.id, context, other.id) == "not_found"
    assert await harness.abort_submission(SubmissionId(999_999), context) == "not_found"
    assert await harness.submission(SubmissionId(999_999), context) is None

    release(task_id)
    await submission.wait(context)
    assert await submission.abort(context) == "settled"
    assert await harness.abort_submission(submission.id, context) == "settled"
    await harness.close(context)


async def test_cancels_only_a_wait_and_rejects_pending_waits_on_close() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await wait_busy(harness, root)
    controller = AbortController()
    cancelled = asyncio.ensure_future(submission.wait(with_signal(controller.signal)))
    pending = asyncio.ensure_future(submission.wait(context))
    await flush()
    controller.abort(RuntimeError("stop waiting"))
    with pytest.raises(RuntimeError, match="stop waiting"):
        await cancelled
    assert (await submission.status(context))["status"] == "placed"
    await harness.close(context)
    with pytest.raises(RuntimeError, match="Harness is closed"):
        await pending


async def test_rejects_a_wait_whose_submission_read_spans_the_start_of_close() -> None:
    entered = deferred()
    hold = deferred()
    holding = {"value": False}

    class HeldReads(ControlledStorage):
        async def submission(self, id: Any, read_context: Any) -> Any:
            if holding["value"]:
                entered.resolve()
                await hold.promise
            return await super().submission(id, read_context)

    opened = await open_submissions(HeldReads())
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await wait_busy(harness, root)
    holding["value"] = True
    waiting = asyncio.ensure_future(submission.wait(context))
    await entered.promise
    closing = harness.close(context)
    hold.resolve()
    with pytest.raises(RuntimeError, match="Harness is closed"):
        await waiting
    await closing


async def test_settles_submissions_by_their_current_record_in_the_transaction() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    entry = await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "note"}), context
    )
    def create(record: Any) -> Any:
        return root.commit(lambda tx: tx.create_submission(record), context)

    queued = await create(
        {"conversationId": root.id, "type": "input", "status": "queued"}
    )
    write = await create(
        {"conversationId": root.id, "type": "write", "status": "queued"}
    )
    answer = {"status": "done", "answer": entry["id"]}

    async def settle(tx: Any, id: Any, settlement: Any) -> None:
        tx.settle_submission(id, settlement)

    with pytest.raises(ValueError, match="is not a placed input"):
        await root.commit(lambda tx: settle(tx, queued["id"], answer), context)
    with pytest.raises(ValueError, match="is not a placed input"):
        await root.commit(lambda tx: settle(tx, write["id"], answer), context)
    with pytest.raises(ValueError, match="does not exist"):
        await root.commit(
            lambda tx: settle(
                tx, SubmissionId(999_999), {"status": "unanswered", "reason": "x"}
            ),
            context,
        )

    placed = await root.commit(
        lambda tx: _settle_twice(tx, root.id, entry["id"], answer), context
    )
    assert await (
        await harness.submission(placed, context)
    ).status(context) == {
        "id": placed,
        "conversationId": root.id,
        "type": "input",
        "status": "done",
        "entry": entry["id"],
        "answer": entry["id"],
    }
    await harness.close(context)


async def _settle_twice(tx: Any, conversation_id: Any, entry: Any, answer: Any) -> Any:
    record = await tx.create_submission(
        {"conversationId": conversation_id, "type": "input", "status": "placed", "entry": entry}
    )
    tx.settle_submission(record["id"], answer)
    tx.settle_submission(record["id"], {"status": "unanswered", "reason": "late"})
    return record["id"]

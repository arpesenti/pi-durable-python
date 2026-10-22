"""Translated upstream ``harness-input-entries`` cases: inputs placed as application entries."""

from __future__ import annotations

from typing import Any

import pytest
from session_support import context
from submission_support import (
    all_entries,
    fail,
    inbox,
    open_submissions,
    release,
    reset,
    transcript,
    wait_busy,
)

from pi_durable import ConversationBusy, LiveDoc


@pytest.fixture(autouse=True)
def _reset_probe() -> Any:
    reset()
    yield
    reset()


def mention(text: str) -> dict[str, Any]:
    return {"kind": "chat.mention", "data": {"author": "ann", "text": text}}


async def test_places_an_idle_input_as_its_entry_with_the_content_as_its_user_message() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit(
        {"type": "input", "content": "ann: hi", "entry": mention("hi")}, context
    )
    task_id = await wait_busy(harness, root)
    entries = await all_entries(root)
    assert transcript(entries) == ["chat.mention:ann: hi"]
    placed = entries[0]
    assert placed["data"] == {"author": "ann", "text": "hi"}
    assert placed["model"][0]["role"] == "user"
    assert placed["model"][0]["content"] == "ann: hi"
    record = await submission.status(context)
    assert record["status"] == "placed"
    assert record["entry"] == placed["id"]
    view = await root.context(context, {"at": placed["id"]})
    assert [message for message in view["messages"] if message["role"] != "system"] == placed["model"]
    release(task_id)
    settled = await submission.wait(context)
    assert settled["status"] == "done"
    assert settled["entry"] == placed["id"]
    await harness.close(context)


async def test_rejects_an_input_entry_whose_kind_is_invalid_or_reserved_writing_nothing() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    before = len(await all_entries(root))
    for kind in (123, "", "pi.user", "pi.compaction"):
        with pytest.raises(TypeError, match="An input entry's kind must be"):
            await root.submit(
                {"type": "input", "content": "x", "entry": {"kind": kind}}, context
            )
    assert len(await all_entries(root)) == before
    assert await harness.snapshot(LiveDoc, root.id, context) == {}
    await root.reset(None, context)
    assert (await all_entries(root))[-1]["kind"] == "pi.reset"
    await harness.close(context)


async def test_takes_only_kind_and_data_from_an_untyped_entry() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    extra = {
        "kind": "chat.mention",
        "model": [{"role": "user", "content": "forged", "timestamp": 0}],
        "head": "self",
        "edits": [{"target": 1, "action": "omit"}],
    }
    idle = await root.submit({"type": "input", "content": "idle", "entry": extra}, context)
    task_id = await wait_busy(harness, root)
    release(task_id)
    await idle.wait(context)
    placed = (await all_entries(root))[0]
    assert placed["kind"] == "chat.mention"
    assert "head" not in placed
    assert "edits" not in placed
    assert placed["model"][0]["role"] == "user"
    assert placed["model"][0]["content"] == "idle"

    first = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    queued = await root.submit({"type": "input", "content": "later", "entry": extra}, context)
    assert (await inbox(harness, root))[-1] == {
        "id": queued.id,
        "mode": "followUp",
        "content": "later",
        "entry": {"kind": "chat.mention"},
    }
    fail(task_id)
    await first.wait(context)
    await harness.wait_for_idle(context)
    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    mentions = [entry for entry in await all_entries(root) if entry["kind"] == "chat.mention"]
    assert [(entry["model"][0]["content"], "head" in entry, "edits" in entry) for entry in mentions] == [
        ("idle", False, False),
        ("later", False, False),
    ]
    assert (await queued.status(context))["status"] == "placed"
    assert (await g.status(context))["status"] == "queued"
    await harness.close(context)


async def test_queues_busy_inputs_with_their_entries_and_places_them_at_boundaries() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    task_id = await wait_busy(harness, root)
    steer = await root.submit(
        {
            "type": "input",
            "content": "s",
            "entry": mention("s"),
            "when_busy": "steer",
            "request_id": "r-s",
        },
        context,
    )
    follow_up = await root.submit(
        {"type": "input", "content": "f", "entry": {"kind": "p2.input"}, "when_busy": "followUp"},
        context,
    )
    assert await inbox(harness, root) == [
        {
            "id": steer.id,
            "mode": "steer",
            "content": "s",
            "entry": {"kind": "chat.mention", "data": {"author": "ann", "text": "s"}},
        },
        {"id": follow_up.id, "mode": "followUp", "content": "f", "entry": {"kind": "p2.input"}},
    ]
    again = await root.submit(
        {
            "type": "input",
            "content": "other",
            "entry": mention("other"),
            "when_busy": "steer",
            "request_id": "r-s",
        },
        context,
    )
    assert again.id == steer.id
    with pytest.raises(ConversationBusy):
        await root.submit(
            {"type": "input", "content": "x", "entry": mention("x"), "when_busy": "reject"},
            context,
        )
    assert len(await inbox(harness, root)) == 2

    fail(task_id)
    await input.wait(context)
    await harness.wait_for_idle(context)
    g = await root.submit({"type": "input", "content": "g"}, context)
    await wait_busy(harness, root)
    entries = await all_entries(root)
    assert transcript(entries) == [
        "pi.user:a",
        "chat.mention:s",
        "p2.input:f",
    ]
    assert (await steer.status(context))["entry"] == entries[1]["id"]
    assert (await follow_up.status(context))["entry"] == entries[2]["id"]
    assert (await g.status(context))["status"] == "queued"
    await harness.close(context)


async def test_withdraws_queued_inputs_with_entries_one_by_one_and_by_conversation_abort() -> None:
    opened = await open_submissions()
    harness, root = opened["harness"], opened["root"]
    input = await root.submit({"type": "input", "content": "a"}, context)
    await wait_busy(harness, root)
    withdrawn = await root.submit({"type": "input", "content": "w", "entry": mention("w")}, context)
    aborted = await root.submit({"type": "input", "content": "x", "entry": mention("x")}, context)
    assert await withdrawn.abort(context) == "aborted"
    assert [item["id"] for item in await inbox(harness, root)] == [aborted.id]
    await root.abort(context)
    for submission in (withdrawn, aborted):
        assert (await submission.wait(context))["status"] == "unanswered"
        assert (await submission.status(context))["reason"] == "aborted"
    assert (await input.wait(context))["status"] == "unanswered"
    assert await inbox(harness, root) == []
    assert not any(entry["kind"] == "chat.mention" for entry in await all_entries(root))
    await harness.close(context)

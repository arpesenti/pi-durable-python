"""Translated upstream ``harness-live-deltas`` generation cases.

The tool-slot cases need the tool task and land with the tool-execution ticket. The generation
cases here cover throttled text partials as appends and the complete-base rule of ``pi.live``.
"""

from __future__ import annotations

from typing import Any

from chat_support import all_entries, assistant_message, chat_setup, open_chat, slow_message
from session_support import context, document_changes

from pi_durable import LiveDoc, MemoryStorage


def live_commit_ops(harness: Any) -> list[list[Any]]:
    commits: list[list[Any]] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] == "pi.live" and change["ops"]:
                commits.append(list(change["ops"]))

    harness.subscribe_commits(observe)
    return commits


async def test_streams_partial_text_as_appends() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [slow_message(assistant_message("word " * 250), chunks=20, interval=0.05)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    commits = live_commit_ops(harness)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    partials = [
        ops
        for ops in commits
        if any(op[0] == "s" and op[1] == ["generation", "message"] for op in ops)
        or any(
            op[0] == "a" and op[1][:3] == ["generation", "message", "content"]
            for op in ops
        )
    ]
    assert len(partials) > 2
    assert partials[0] == [["s", ["generation", "message"], partials[0][0][2]]]
    for ops in partials[1:]:
        assert ops == [
            ["a", ["generation", "message", "content", 0, "text"], ops[0][2]]
        ], ops
    assert [entry["kind"] for entry in await all_entries(root)][-1] == "pi.assistant"
    await harness.close(context)


async def test_stores_a_complete_base_exactly_in_the_commits_where_nothing_runs() -> None:
    written: dict[int, str] = {}
    live_id: dict[str, Any] = {}

    class RecordingStorage(MemoryStorage):
        async def commit(self, writes: Any, call_context: Any) -> Any:
            kind: str | None = None
            for write in writes:
                if (
                    write["type"] == "document.change"
                    and write["id"] == live_id.get("id")
                ):
                    kind = write["content"]["kind"]
            seq = await super().commit(writes, call_context)
            if kind is not None:
                written[seq] = kind
            return seq

    setup = chat_setup()
    setup.faux.set_responses(
        [slow_message(assistant_message("streamed answer"), chunks=8, interval=0.03)]
    )
    opened = await open_chat(RecordingStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values: dict[int, dict[str, Any]] = {}

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] != "pi.live":
                continue
            live_id["id"] = change["record"]["id"]
            if change["value"] is not None:
                values[publication["seq"]] = change["value"]

    harness.subscribe_commits(observe)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)

    def nothing_runs(value: dict[str, Any]) -> bool:
        return "generation" not in value and not any(
            slot.get("status") == "running" for slot in value.get("tools") or ()
        )

    kinds = []
    for seq, kind in written.items():
        assert (kind == "base") is nothing_runs(values[seq]), (kind, values[seq])
        kinds.append(kind)
    # The generation-only path writes a base when the run ends (and at the submission commit before
    # this subscription); the tool-handover bases arrive with the tool-execution ticket.
    assert kinds.count("base") >= 1
    assert "delta" in kinds
    await harness.close(context)


def test_keeps_a_complete_base_exactly_while_nothing_runs() -> None:
    definition = LiveDoc.definition
    check = definition.checkpoint_when
    assert check is not None

    def base(value: dict[str, Any]) -> bool:
        return check(value, [], {"deltasSinceBase": 1000})

    def slot(status: str) -> dict[str, Any]:
        return {"callId": "c", "name": "n", "status": status}

    run = {"taskId": 1, "inputs": []}
    assert base({}) is True
    assert base({"run": run, "generation": {"attempt": 1}}) is False
    assert base({"run": run, "tools": [slot("pending"), slot("done")]}) is True
    assert base({"run": run, "tools": [slot("done"), slot("running")]}) is False
    assert base({"run": run}) is True

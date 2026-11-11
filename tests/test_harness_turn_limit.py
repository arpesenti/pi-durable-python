"""Translated upstream ``harness-turn-limit`` suite: ``AgentState.maxTurns`` bounds one run.

The round that reaches the limit runs its tools, then the run ends ``unanswered`` with
``runFailure`` reason ``turn_limit`` and the count as its detail, like a failure: ``afterFailedRun``
decides the queued items, and a continue resumes the run on the round's results with a fresh budget
(spec sections 8.2, 8.5).
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from chat_support import (
    assistant_message,
    chat_setup,
    faux_text,
    faux_tool_call,
    live_of,
    only_failure,
    open_chat,
    text_of,
)
from session_support import context
from task_support import deferred, eventually
from tool_support import DONE, add_hooks, add_tool, tool

from pi_durable import GenerationTask, LiveDoc, MemoryStorage, ResetEntry, ToolResultEntry
from pi_durable.harness.types import AfterFailedRun


def round_(call_id: str) -> dict[str, Any]:
    """A tool-calling answer with one ``noop`` call."""

    return assistant_message([faux_tool_call("noop", {}, id=call_id)], stop_reason="toolUse")


async def limited(max_turns: int | None, configure: Any = None) -> dict[str, Any]:
    """A chat with a ``noop`` tool and the given turn limit."""

    setup = chat_setup()
    add_tool(setup.registry, tool("noop", lambda *_args: {"output": []}))
    if configure is not None:
        configure(setup)
    opened = await open_chat(MemoryStorage(), setup)
    if max_turns is not None:
        await opened["root"].configure({"max_turns": max_turns}, context)
    return {**opened, "setup": setup}


async def last_kind(root: Any) -> str | None:
    page = await root.entries({}, 1000, None, context)
    entries = list(reversed(page["items"]))
    return None if not entries else entries[-1]["kind"]


async def live(harness: Any, root: Any) -> dict[str, Any]:
    return live_of(await harness.snapshot(LiveDoc, root.id, context))


async def test_ends_a_run_after_the_tool_round_that_reaches_the_limit() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :73."""

    opened = await limited(1)
    harness, root, setup = opened["harness"], opened["root"], opened["setup"]
    setup.faux.set_responses([round_("c1"), DONE])
    submission = await root.submit({"type": "input", "content": "go"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "turn_limit"
    assert settled["detail"] == "1 turn"
    assert setup.faux.call_count == 1
    # The transcript ends with the round's results; the generation completed, so nothing is reported.
    assert await last_kind(root) == ToolResultEntry.kind
    state = await live(harness, root)
    only_failure(state, state["runFailure"]["taskId"], [submission.id], "turn_limit", "1 turn")
    generations = await harness.commit(
        lambda tx: tx.scan_tasks({"conversationId": root.id}, 20), context
    )
    generation = next(
        task
        for task in generations["items"]
        if task["kind"] == GenerationTask.definition.name
    )
    assert generation["state"]["status"] == "terminal"
    assert generation["state"]["outcome"]["status"] == "completed"
    assert setup.reports == []
    await harness.close(context)


async def test_counts_tool_rounds_only() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :97."""

    opened = await limited(2)
    harness, root, setup = opened["harness"], opened["root"], opened["setup"]
    setup.faux.set_responses([round_("c1"), DONE, round_("c2"), round_("c3"), DONE])
    first = await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    assert first["status"] == "done"
    # A new run starts with a fresh budget.
    second = await (await root.submit({"type": "input", "content": "two"}, context)).wait(context)
    assert second["status"] == "unanswered"
    assert second["reason"] == "turn_limit"
    assert second["detail"] == "2 turns"
    await harness.close(context)


async def test_runs_on_yield_at_the_limit_and_a_continuation_starts_the_count_over() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :108."""

    yields = [0]

    def configure(setup: Any) -> None:
        def on_yield(_answer: Any, _api: Any, _ctx: Any) -> Any:
            yields[0] += 1
            return {"continue": "keep going"} if yields[0] == 1 else None

        add_hooks(setup.registry, GenerationTask, {"onYield": on_yield})

    opened = await limited(2, configure)
    harness, root, setup = opened["harness"], opened["root"], opened["setup"]
    # Round (1 turn), answer: the continuation resets the count; then two rounds reach the limit.
    setup.faux.set_responses([round_("c1"), DONE, round_("c2"), round_("c3"), DONE])
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "turn_limit"
    assert yields[0] == 1
    assert setup.faux.call_count == 4
    await harness.close(context)


async def test_settles_a_steer_that_joined_the_run_at_an_earlier_round_with_turn_limit_too() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :125."""

    gate = deferred()
    started = deferred()
    setup = chat_setup()
    calls = [0]

    async def noop(*_args: Any) -> Any:
        if calls[0] == 0:
            calls[0] += 1
            started.resolve()
            await gate.promise
        return {"output": []}

    add_tool(setup.registry, tool("noop", noop))
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"max_turns": 2}, context)
    setup.faux.set_responses([round_("c1"), round_("c2"), DONE])
    first = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    steer = await root.submit(
        {"type": "input", "content": "also this", "when_busy": "steer"}, context
    )
    gate.resolve()
    for submission in (first, steer):
        settled = await submission.wait(context)
        assert settled["status"] == "unanswered"
        assert settled["reason"] == "turn_limit"
        assert settled["detail"] == "2 turns"
    assert (await live(harness, root))["runFailure"]["inputs"] == [first.id, steer.id]
    assert setup.faux.call_count == 2
    await harness.close(context)


@pytest.mark.parametrize("policy", ["keep", "continue", "withdraw"])
async def test_leaves_items_queued_during_the_last_round_to_after_failed_run(
    policy: AfterFailedRun,
) -> None:
    """Upstream ``harness-turn-limit.test.ts`` :160."""

    gate = deferred()
    started = deferred()
    setup = chat_setup()
    setup.settings["after_failed_run"] = policy

    async def noop(*_args: Any) -> Any:
        started.resolve()
        await gate.promise
        return {"output": []}

    add_tool(setup.registry, tool("noop", noop))
    setup.faux.set_responses([round_("c1"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"max_turns": 1}, context)
    first = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    steer = await root.submit(
        {"type": "input", "content": "also", "when_busy": "followUp"}, context
    )
    gate.resolve()
    assert (await first.wait(context))["status"] == "unanswered"
    await harness.wait_for_idle(context)
    queued = await steer.status(context)
    if policy == "keep":
        assert queued["status"] == "queued"
    if policy == "continue":
        assert queued["status"] == "done"
    if policy == "withdraw":
        assert queued["status"] == "unanswered"
        assert queued["reason"] == "run_failed"
    await harness.close(context)


async def test_applies_a_changed_limit_at_the_end_of_the_round_and_null_removes_it() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :192."""

    setup = chat_setup()
    root: dict[str, Any] = {}
    rounds = [0]

    async def noop(*_args: Any) -> Any:
        rounds[0] += 1
        # Lowered during the second round: that round ends the run.
        if rounds[0] == 2:
            await root["root"].configure({"max_turns": 1}, context)
        return {"output": []}

    add_tool(setup.registry, tool("noop", noop))
    setup.faux.set_responses([round_("c1"), round_("c2"), round_("c3"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    root["root"] = opened["root"]
    harness = opened["harness"]
    await opened["root"].configure({"max_turns": 5}, context)
    settled = await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(
        context
    )
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "turn_limit"
    assert settled["detail"] == "2 turns"
    await opened["root"].configure({"max_turns": None}, context)
    assert (await opened["root"].agent(context)).max_turns is None
    setup.faux.set_responses([round_("c4"), round_("c5"), DONE])
    again = await (
        await opened["root"].submit({"type": "input", "content": "again"}, context)
    ).wait(context)
    assert again["status"] == "done"
    await harness.close(context)


async def test_rejects_a_limit_that_is_not_an_integer_1_or_more() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :221."""

    opened = await limited(None)
    harness, root = opened["harness"], opened["root"]
    for value in (0, -1, 1.5, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="maxTurns must be an integer 1 or more"):
            await root.configure({"max_turns": value}, context)
    await root.configure({"max_turns": 3}, context)
    assert (await root.agent(context)).max_turns == 3
    await harness.close(context)


async def test_lets_terminate_settle_done_at_the_limit_and_writes_a_handoffs_reset() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :232."""

    from pi_durable import ToolTask

    def configure(setup: Any) -> None:
        def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
            if call["id"] == "t":
                return {**result, "control": {"terminate": True}}
            return {**result, "control": {"handoff": "fresh"}}

        add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})

    opened = await limited(1, configure)
    harness, root, setup = opened["harness"], opened["root"], opened["setup"]
    setup.faux.set_responses([round_("t"), round_("h"), DONE])
    one = await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    assert one["status"] == "done"
    handed_off = await (await root.submit({"type": "input", "content": "two"}, context)).wait(
        context
    )
    assert handed_off["status"] == "unanswered"
    assert handed_off["reason"] == "turn_limit"
    page = await root.entries({}, 1000, None, context)
    entries = list(reversed(page["items"]))
    resets = [entry for entry in entries if ResetEntry.is_(entry)]
    texts = [cast(Any, entry["model"][0])["content"] for entry in resets]
    assert texts == ["fresh"]
    await harness.close(context)


async def test_is_resumed_by_a_continue_with_a_fresh_budget_on_the_rounds_results() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :251."""

    opened = await limited(1)
    harness, root, setup = opened["harness"], opened["root"], opened["setup"]
    continued: dict[str, Any] = {}

    def last_role(request: Any, _options: Any) -> dict[str, Any]:
        continued["role"] = request["messages"][-1]["role"]
        return assistant_message([faux_text("finished")])

    setup.faux.set_responses([round_("c1"), last_role])
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    resumed = await root.submit({"type": "continue"}, context)
    assert (await live(harness, root))["run"]["turns"] == 0
    settled = await resumed.wait(context)
    assert settled["status"] == "done" and settled["type"] != "write"
    entry = await root.commit(lambda tx: tx.entry(cast(Any, settled)["answer"]), context)
    assert text_of(cast(Any, entry["model"][0])) == "finished"
    assert continued["role"] == "toolResult"
    await harness.close(context)


async def test_is_inherited_by_forks_and_children_which_may_set_their_own() -> None:
    """Upstream ``harness-turn-limit.test.ts`` :273."""

    setup = chat_setup()
    child: dict[str, Any] = {}

    async def spawn(_args: Any, api: Any, call_context: Any) -> Any:
        async def make(tx: Any) -> Any:
            created = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": api.task_id}}
            )
            return created["id"]

        child["id"] = await api.commit(make, call_context)
        return {"output": []}

    add_tool(setup.registry, tool("spawn", spawn))
    setup.faux.set_responses(
        [
            assistant_message([faux_tool_call("spawn", {}, id="c1")], stop_reason="toolUse"),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"max_turns": 4}, context)
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    spawned = await harness.conversation(child["id"], context)
    assert (await spawned.agent(context)).max_turns == 4
    page = await root.entries({}, 1000, None, context)
    at = list(reversed(page["items"]))[-1]["id"]
    fork = await root.fork(at, {"ownership": {"kind": "ownerless"}}, context)
    assert (await fork.agent(context)).max_turns == 4
    own = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}, "agent": {"max_turns": 2}}, context
    )
    assert (await own.agent(context)).max_turns == 2
    await harness.close(context)


async def test_keeps_the_stored_count_across_a_restart(tmp_path: Any) -> None:
    """Upstream ``harness-turn-limit.test.ts`` :307."""

    from session_support import ControlledStorage

    setup = chat_setup()
    setup.settings["progress"] = {"output_interval_ms": 0, "persist_within_ms": 0}
    started = deferred()
    runs = [0]

    async def noop(_args: Any, _api: Any, call_context: Any) -> Any:
        runs[0] += 1
        if runs[0] == 2:
            started.resolve()
            from task_support import aborted

            await aborted(call_context.abort_signal)
        return {"output": []}

    add_tool(setup.registry, tool("noop", noop, replay="safe"))
    setup.faux.set_responses([round_("c1"), round_("c2"), DONE])
    storage = ControlledStorage()
    first = await open_chat(storage, setup)
    await first["root"].configure({"max_turns": 2}, context)
    first["harness"].resume()
    id = (await first["root"].submit({"type": "input", "content": "go"}, context)).id
    await started.promise
    assert (await live(first["harness"], first["root"]))["run"]["turns"] == 2
    storage.crash()
    await first["harness"].close(context)

    # The replay-safe call runs again after reopen; the stored count still ends the run at this round.
    runs[0] = 0
    second = await open_chat(storage.reopen(), setup)
    second["harness"].resume()
    assert (await live(second["harness"], second["root"]))["run"]["turns"] == 2
    submission = await second["harness"].submission(id, context)
    settled = await submission.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "turn_limit"
    assert settled["detail"] == "2 turns"
    await eventually(lambda: second["harness"] is not None)
    await second["harness"].close(context)

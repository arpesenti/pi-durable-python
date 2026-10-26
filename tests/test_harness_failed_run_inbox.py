"""Translated v1.4.0 ``harness-failed-run-inbox`` suite: what a failed run leaves queued (spec §6).

``settings.after_failed_run`` decides what a run that fails with ``model_error`` or ``no_model``
does with the inputs queued behind it, in the commit that settles its own inputs: ``keep`` (the
default) leaves them queued, ``continue`` applies the final boundary so the next run starts with
them, and ``withdraw`` settles them ``unanswered`` with reason ``run_failed``, keeping queued
writes.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from chat_support import assistant_message, chat_setup, open_chat, text_of
from session_support import context
from task_support import deferred

from pi_durable import MemoryStorage
from pi_durable.entries import AssistantEntry
from pi_durable.harness.types import AfterFailedRun


def _failing_once() -> dict[str, Any]:
    """A response that fails for good once ``gate`` opens; ``reached`` settles when it is requested."""

    gate = deferred()
    reached = deferred()

    async def step(_request: Mapping[str, Any], _options: Mapping[str, Any]) -> dict[str, Any]:
        reached.resolve()
        await gate.promise
        return assistant_message((), stop_reason="error", error_message="Invalid request")

    return {"step": step, "gate": gate, "reached": reached}


async def _fail_with_queued_input(policy: AfterFailedRun | None) -> dict[str, Any]:
    """A run that fails with an input queued behind it, under ``policy``."""

    setup = chat_setup()
    if policy is not None:
        setup.settings["after_failed_run"] = policy
    failing = _failing_once()
    setup.faux.set_responses(
        [
            failing["step"],
            assistant_message("second answer"),
            assistant_message("third answer"),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    root = opened["root"]
    first = await root.submit({"type": "input", "content": "Analyze this report"}, context)
    await failing["reached"].promise
    second = await root.submit(
        {"type": "input", "content": "Email me the summary", "when_busy": "followUp"}, context
    )
    failing["gate"].resolve()
    settled = await first.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "model_error"
    return {**opened, "second": second}


async def test_stay_queued_by_default() -> None:
    opened = await _fail_with_queued_input(None)
    harness = opened["harness"]
    second = opened["second"]
    await harness.wait_for_idle(context)
    assert (await second.status(context))["status"] == "queued"
    # The next input runs the queue, as before.
    third = await opened["root"].submit({"type": "input", "content": "Anything else?"}, context)
    assert (await second.wait(context))["status"] == "done"
    assert (await third.wait(context))["status"] == "done"
    await harness.close(context)


async def test_start_the_next_run_with_continue() -> None:
    opened = await _fail_with_queued_input("continue")
    settled = await opened["second"].wait(context)
    assert settled["status"] == "done"
    assert settled["type"] == "input"
    answer = await opened["root"].commit(
        lambda tx: tx.entry(AssistantEntry, settled["answer"]), context
    )
    assert text_of((answer or {}).get("model", ())[0]) == "second answer"
    await opened["harness"].close(context)


async def test_place_queued_writes_and_keep_them_with_withdraw() -> None:
    for policy in ("continue", "withdraw"):
        setup = chat_setup()
        setup.settings["after_failed_run"] = policy
        failing = _failing_once()
        setup.faux.set_responses([failing["step"], assistant_message("second answer")])
        opened = await open_chat(MemoryStorage(), setup)
        root = opened["root"]
        first = await root.submit({"type": "input", "content": "first"}, context)
        await failing["reached"].promise
        note = {"role": "user", "content": "a note the model sees", "timestamp": 1}
        write = await root.submit(
            {"type": "write", "entry": {"kind": "test.note", "model": [note]}}, context
        )
        assert (await write.status(context))["status"] == "queued"
        failing["gate"].resolve()
        await first.wait(context)
        await opened["harness"].wait_for_idle(context)
        # `continue` places the write at the failed run's boundary; `withdraw` withdraws inputs only.
        assert (await write.status(context))["status"] == (
            "done" if policy == "continue" else "queued"
        )
        await opened["harness"].close(context)


async def test_settle_unanswered_with_run_failed_with_withdraw() -> None:
    opened = await _fail_with_queued_input("withdraw")
    settled = await opened["second"].wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "run_failed"
    await opened["harness"].close(context)

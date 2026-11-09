"""Translated upstream ``harness-run-failure`` suite: why the newest run ended without an answer.

``pi.live.runFailure`` is the reason a renderer shows: the run's task at its end, the inputs it was
answering, the reason, the detail behind it, and the run's first entry, which a continue takes over.
It is written in the commit that settles the run's inputs ``unanswered`` and removed by the next
commit that starts a run -- a run started in the same commit, the queue's next run under
``afterFailedRun: "continue"`` or the one a reset starts, keeps it (spec §8.2).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, get_args

from chat_support import (
    assistant_message,
    chat_setup,
    failing_once,
    faux_text,
    faux_tool_call,
    live_of,
    only_failure,
    open_chat,
    unanswered_reached,
)
from session_support import context
from task_support import deferred
from tool_support import add_tool, tool

import pi_durable
from pi_durable import LiveDoc, MemoryStorage, RunFailure, RunFailureReason
from pi_durable.harness import live as live_module
from pi_durable.ids import id_from_number


async def live(harness: Any, conversation: Any) -> dict[str, Any]:
    return live_of(await harness.snapshot(LiveDoc, conversation.id, context))


async def test_records_a_model_error_with_its_text_and_a_later_run_clears_it() -> None:
    """Upstream ``harness-run-failure.test.ts`` :34."""

    setup = chat_setup()
    setup.faux.set_responses(
        [
            assistant_message([], stop_reason="error", error_message="Invalid request"),
            assistant_message([faux_text("fine")]),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "model_error"
    assert settled["detail"] == "Invalid request"
    state = await live(harness, root)
    only_failure(
        state, state["runFailure"]["taskId"], [submission.id], "model_error", "Invalid request"
    )
    next_input = await root.submit({"type": "input", "content": "again"}, context)
    # The commit that starts the next run removes it.
    assert (await live(harness, root)).get("runFailure") is None
    assert (await next_input.wait(context))["status"] == "done"
    assert await live(harness, root) == {}
    await harness.close(context)


async def test_records_a_missing_model_with_which_one() -> None:
    """Upstream ``harness-run-failure.test.ts`` :63."""

    setup = chat_setup()
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"model": {"provider": "faux", "modelId": "gone"}}, context)
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "no_model"
    # The settlement is unchanged: no detail.
    assert "detail" not in settled
    state = await live(harness, root)
    only_failure(
        state,
        state["runFailure"]["taskId"],
        [submission.id],
        "no_model",
        "Model faux/gone is not available",
    )
    await harness.close(context)


async def test_records_an_abort() -> None:
    """Upstream ``harness-run-failure.test.ts`` :81."""

    setup = chat_setup()
    busy = unanswered_reached()
    setup.faux.set_responses([busy["step"]])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await busy["reached"].promise
    task_id = (await live(harness, root))["run"]["taskId"]
    await root.abort(context)
    settled = await submission.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "aborted"
    only_failure(await live(harness, root), task_id, [submission.id], "aborted")
    await harness.close(context)


async def test_records_a_reset_that_cut_the_run_before_an_answer() -> None:
    """Upstream ``harness-run-failure.test.ts`` :101."""

    setup = chat_setup()
    gate = deferred()
    started = deferred()

    async def hold(_args: Any, _api: Any, _call_context: Any) -> Any:
        started.resolve()
        await gate.promise
        return {}

    add_tool(setup.registry, tool("hold", hold))
    second = unanswered_reached()
    setup.faux.set_responses(
        [
            assistant_message([faux_tool_call("hold", {}, id="c1")], stop_reason="toolUse"),
            second["step"],
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    await root.reset(None, context)
    follow_up = await root.submit(
        {"type": "input", "content": "next", "when_busy": "followUp"}, context
    )
    gate.resolve()
    settled = await first.wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "reset"
    await second["reached"].promise
    state = await live(harness, root)
    # The follow-up's run started in the commit that ended the first; the failure stays until a later
    # run starts.
    assert state["run"]["inputs"] == [follow_up.id]
    failure = state["runFailure"]
    assert failure["inputs"] == [first.id]
    assert failure["reason"] == "reset"
    await root.abort(context)
    failure = (await live(harness, root))["runFailure"]
    assert failure["inputs"] == [follow_up.id]
    assert failure["reason"] == "aborted"
    await harness.close(context)


async def test_keeps_the_failure_while_a_run_continues_the_queue_in_the_same_commit() -> None:
    """Upstream ``harness-run-failure.test.ts`` :145."""

    for policy in ("continue", "keep"):
        setup = chat_setup()
        setup.settings["after_failed_run"] = policy
        failing = failing_once()
        busy = unanswered_reached()
        setup.faux.set_responses(
            [failing["step"], busy["step"], assistant_message([faux_text("third")])]
        )
        opened = await open_chat(MemoryStorage(), setup)
        harness, root = opened["harness"], opened["root"]
        first = await root.submit({"type": "input", "content": "first"}, context)
        await failing["reached"].promise
        second = await root.submit(
            {"type": "input", "content": "second", "when_busy": "followUp"}, context
        )
        failing["gate"].resolve()
        await first.wait(context)
        if policy == "continue":
            await busy["reached"].promise
            state = await live(harness, root)
            assert state["run"]["inputs"] == [second.id]
            failure = state["runFailure"]
            assert failure["inputs"] == [first.id]
            assert failure["reason"] == "model_error"
        else:
            await harness.wait_for_idle(context)
            failure = (await live(harness, root))["runFailure"]
            assert failure["inputs"] == [first.id]
        await harness.close(context)


def test_the_surface_run_failure_has_upstream_requiredness_and_the_whole_reason_set() -> None:
    """Port-authored, not translated: ``RunFailure`` and ``RunFailureReason`` as the surface has them.

    Upstream declares both in ``src/index.ts:51-52``, so a host reads why the newest run ended without
    an answer through the package rather than through the module that defines it. The vendored
    specification (``docs/upstream/spec-1.5.0.md:4644-4668``) requires ``taskId``, ``inputs`` and
    ``reason`` and makes only ``detail`` and ``first`` optional, and lists the ten reasons a renderer
    matches on. Both halves are asserted here: the reason set is compared as a whole so a reason cannot
    be dropped, renamed or quietly added, and the required/optional split is enforced where Python can
    actually see it — by mypy over the two ``TYPE_CHECKING`` negatives below, not by
    ``__required_keys__``, which reads ``NotRequired`` as an ordinary string annotation under this
    module's ``from __future__ import annotations`` and would report all five keys as required.
    """

    assert RunFailure is live_module.RunFailure
    assert RunFailureReason is live_module.RunFailureReason
    assert {"RunFailure", "RunFailureReason"} <= set(pi_durable.__all__)
    assert set(RunFailure.__annotations__) == {"taskId", "inputs", "reason", "detail", "first"}
    reasons = get_args(RunFailureReason)
    assert set(reasons) == {
        "model_error",
        "no_model",
        "aborted",
        "reset",
        "faulted",
        "orphaned",
        "interrupted",
        "restart_limit",
        "nothing_to_answer",
        "turn_limit",
    }
    assert len(reasons) == 10, "the reason set is upstream's, not a superset of it"

    # The whole documented shape compiles, including the two optional fields.
    recorded: RunFailure = {
        "taskId": id_from_number(7),
        "inputs": [id_from_number(3)],
        "reason": "turn_limit",
        "detail": "3 turns",
        "first": id_from_number(11),
    }
    assert recorded["reason"] == "turn_limit"


if TYPE_CHECKING:
    # The three required fields are required: a failure with no task is not a ``RunFailure``.
    _no_task: RunFailure = {"inputs": [], "reason": "aborted"}  # type: ignore[typeddict-item]
    _no_reason: RunFailure = {"taskId": id_from_number(1), "inputs": []}  # type: ignore[typeddict-item]

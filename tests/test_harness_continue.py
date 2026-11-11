"""Translated upstream ``harness-continue`` cases: a run with no new user message.

Upstream ``test/harness-continue.test.ts`` holds 13 cases: 12 are translated here and 1 is excluded.

Excluded, telemetry-only (ADR 0002 keeps telemetry out of the port):
"starts a root trace linked to its caller, as an input does".

One translation is narrower than upstream in its crash premise, named so rather than silently:
"answers a run interrupted by a host that died while the model answered" simulates the crash's count
by writing ``restarts`` on the reopened record, because its host death is a clean ``close()`` -- and
a close unwinds the in-flight task back to ``pending`` uncounted, "a close is not a crash", the way
upstream's close does -- where upstream's ``kill()`` leaves the task ``running`` for open to charge.
The charge a crash leaves behind is what ``test_harness_restart_failure.py`` proves on the ``kill()``
seam (open counts a task found ``running``, ``scheduler.py:635``). The rule under test - the
generation's own, that an in-flight attempt the host stopped is a failed attempt under
``settings.retry`` and ends the run ``interrupted`` - is asserted as upstream asserts it.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from chat_support import (
    all_entries,
    assistant_message,
    captured_partial,
    chat_setup,
    failing_once,
    faux_text,
    faux_tool_call,
    hang_after_partial,
    open_chat,
    text_of,
    unanswered,
    usage,
)
from session_support import ControlledStorage, context
from task_support import aborted, deferred, eventually
from tool_support import add_tool, tool

from pi_durable import ConversationBusy, InboxDoc, LiveDoc, MemoryStorage, UserEntry, configure
from pi_durable.harness.types import SettledSubmissionRecord
from pi_durable.testing import commit_writes

failure = assistant_message([], stop_reason="error", error_message="Invalid request")


def answering(text: str, seen: list[list[str]]) -> Any:
    """A faux step that records the roles of the context it was asked with, then answers ``text``."""

    def step(request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        seen.append([str(message["role"]) for message in request["messages"]])
        return assistant_message([faux_text(text)])

    return step


async def answer_text(root: Any, settled: SettledSubmissionRecord) -> str | None:
    """The text of the answer entry a settled input or continue submission carries."""

    assert settled["status"] == "done" and settled["type"] != "write"
    entry = await root.commit(lambda tx: tx.entry(settled["answer"]), context)
    assert entry is not None
    return text_of(cast(Any, entry["model"][0]))


async def user_entry_count(root: Any) -> int:
    return len([entry for entry in await all_entries(root) if UserEntry.is_(entry)])


async def test_answers_a_run_that_failed_without_a_new_user_message() -> None:
    setup = chat_setup()
    seen: list[list[str]] = []
    setup.faux.set_responses([failure, answering("recovered", seen)])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "Analyze this"}, context)
    settled = await first.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"

    resumed = await root.submit({"type": "continue"}, context)
    record = await resumed.status(context)
    # A continue is placed without an entry of its own: a status alone no longer says one exists.
    assert record["status"] == "placed" and "entry" not in record
    settled = await resumed.wait(context)
    assert settled["type"] == "continue"
    assert await answer_text(root, settled) == "recovered"
    assert seen[-1][-1] == "user"
    # The failed input stays unanswered; no user entry was added.
    assert (await first.status(context))["status"] == "unanswered"
    assert await user_entry_count(root) == 1
    # The failed run's reason was on the live document until this run answered.
    state = await harness.snapshot(LiveDoc, root.id, context)
    assert (state or {}).get("runFailure") is None
    await harness.close(context)


async def test_ends_without_a_request_when_there_is_nothing_to_answer() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("answered")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    empty = await (await root.submit({"type": "continue"}, context)).wait(context)
    assert empty["status"] == "unanswered" and empty["reason"] == "nothing_to_answer"
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    calls = setup.faux.call_count
    # A context ending in an answer has nothing to continue.
    after = await (await root.submit({"type": "continue"}, context)).wait(context)
    assert after["status"] == "unanswered" and after["reason"] == "nothing_to_answer"
    assert setup.faux.call_count == calls
    state = await harness.snapshot(LiveDoc, root.id, context)
    assert (state or {})["runFailure"]["reason"] == "nothing_to_answer"
    await harness.close(context)


async def test_does_not_continue_a_text_answer_cut_off_at_its_output_limit() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message([faux_text("cut")], stop_reason="length")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "write a lot"}, context)).wait(context)
    settled = await (await root.submit({"type": "continue"}, context)).wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "nothing_to_answer"
    assert setup.faux.call_count == 1
    await harness.close(context)


async def test_places_a_reset_a_failed_run_kept_and_finds_nothing_to_answer() -> None:
    """Upstream ``harness-continue.test.ts``: places a reset a failed run kept, and finds nothing to
    answer in the context it starts."""

    setup = chat_setup()
    failing = failing_once()
    setup.faux.set_responses([failing["step"]])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "first"}, context)
    await failing["reached"].promise
    await root.reset(None, context)
    failing["gate"].resolve()
    settled = await first.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert len((inbox or {})["items"]) == 1

    resumed = await (await root.submit({"type": "continue"}, context)).wait(context)
    assert resumed["status"] == "unanswered" and resumed["reason"] == "nothing_to_answer"
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert (inbox or {}).get("items", []) == []
    messages = [m for m in (await root.context(context))["messages"] if m["role"] != "system"]
    assert messages == []
    assert setup.faux.call_count == 1
    await harness.close(context)


async def test_starts_inputs_queued_behind_a_continue_with_nothing_to_answer() -> None:
    """Upstream ``harness-continue.test.ts``: starts the inputs queued behind a continue with nothing
    to answer under ``afterFailedRun: "continue"``."""

    setup = chat_setup()
    setup.settings["after_failed_run"] = "continue"
    setup.faux.set_responses([assistant_message("answered"), assistant_message("second answered")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    resumed = await root.submit({"type": "continue"}, context)
    queued = await root.submit({"type": "input", "content": "two", "whenBusy": "followUp"}, context)
    assert (await queued.status(context))["status"] == "queued"
    record = await resumed.wait(context)
    assert record["status"] == "unanswered" and record["reason"] == "nothing_to_answer"
    settled = await queued.wait(context)
    assert await answer_text(root, settled) == "second answered"
    await harness.close(context)


async def generation_id(harness: Any, conversation_id: Any) -> Any:
    """The id of the generation task the conversation is running."""

    page = await harness.commit(
        lambda tx: tx.scan_tasks({"kind": "pi.generation"}, 100),
        context,
    )
    owned = [record for record in page["items"] if record["conversationId"] == conversation_id]
    assert len(owned) == 1, f"expected one generation task, got {owned}"
    return owned[0]["id"]


async def test_answers_a_run_interrupted_by_a_host_that_died_while_the_model_answered() -> None:
    # A close is not a crash: it unwinds the in-flight task back to ``pending`` with no count. The
    # charge a crash would have left -- open counts a task found ``running`` (``scheduler.py:635``)
    # -- is written here on the record instead, and the generation's own rule, that an in-flight
    # attempt is a failed attempt under the retry policy, is asserted as upstream asserts it.
    storage = ControlledStorage()
    setup = chat_setup()
    setup.settings["progress"] = {"partial_interval_ms": 0}
    setup.faux.set_responses([hang_after_partial("z" * 400, usage(input=3))])
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await captured_partial(harness, root.id)
    task_id = await generation_id(harness, root.id)
    # The host dies with the request in flight, leaving its committed partial.
    await harness.close(context)

    left = dict(cast(Any, await storage.reopen().task(task_id, context)))
    left["restarts"] = 1
    await commit_writes(storage.reopen(), [{"type": "task", "value": cast(Any, left)}], context)

    second = chat_setup()
    second.settings["retry"] = {"enabled": False}
    seen: list[list[str]] = []
    second.faux.set_responses([answering("after the crash", seen)])
    opened = await open_chat(storage.reopen(), second)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    interrupted = await cast(Any, await harness.submission(submission.id, context)).wait(context)
    assert interrupted["status"] == "unanswered" and interrupted["reason"] == "interrupted"
    settled = await (await root.submit({"type": "continue"}, context)).wait(context)
    assert await answer_text(root, settled) == "after the crash"
    # The aborted partial is not model context: the request ends in the user message.
    assert seen[-1][-1] == "user"
    await harness.close(context)


async def test_answers_the_tool_results_of_a_round_aborted_midway() -> None:
    setup = chat_setup()
    started = deferred()

    def wait_execute(_args: Any, _api: Any, call_context: Any) -> Any:
        started.resolve()
        return aborted(call_context.abort_signal)

    add_tool(setup.registry, tool("wait", wait_execute))
    seen: list[list[str]] = []
    setup.faux.set_responses(
        [
            assistant_message([faux_tool_call("wait", {}, id="c1")], stop_reason="toolUse"),
            answering("after the abort", seen),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    await root.abort(context)
    settled = await first.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "aborted"

    resumed = await root.submit({"type": "continue"}, context)
    settled = await resumed.wait(context)
    assert await answer_text(root, settled) == "after the abort"
    assert seen[-1][-1] == "toolResult"
    await harness.close(context)


async def test_skips_system_messages_when_deciding_there_is_something_to_answer() -> None:
    setup = chat_setup()
    seen: list[list[str]] = []
    setup.faux.set_responses(
        [assistant_message("first answer"), failure, answering("resumed", seen)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"instructions": "Be thorough."}, context)
    await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    # Changed instructions append a system delta after the next user message, before its request.
    await root.configure({"instructions": "Be brief."}, context)
    second = await root.submit({"type": "input", "content": "two"}, context)
    settled = await second.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    view = await root.context(context)
    assert view["messages"][-1]["role"] == "system"

    resumed = await root.submit({"type": "continue"}, context)
    settled = await resumed.wait(context)
    assert await answer_text(root, settled) == "resumed"
    await harness.close(context)


async def test_is_rejected_while_busy_after_request_id_deduplication() -> None:
    setup = chat_setup()
    setup.faux.set_responses([failure, unanswered()])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    resumed = await root.submit({"type": "continue", "request_id": "resume-1"}, context)
    await eventually(lambda: setup.faux.call_count == 2)
    with pytest.raises(ConversationBusy):
        await root.submit({"type": "continue"}, context)
    # The same request ID returns the existing submission instead of rejecting.
    again = await root.submit({"type": "continue", "request_id": "resume-1"}, context)
    assert again.id == resumed.id
    await harness.close(context)


async def test_continues_an_answer_with_tool_calls_cut_off_at_its_output_limit() -> None:
    setup = chat_setup()
    seen: list[list[str]] = []
    setup.faux.set_responses(
        [
            assistant_message(
                [faux_text("calling"), faux_tool_call("echo", {}, id="c1")],
                stop_reason="length",
            ),
            answering("resumed", seen),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    # The run answered: a cut-off answer is an answer, and its calls never ran.
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"

    resumed = await root.submit({"type": "continue"}, context)
    settled = await resumed.wait(context)
    assert await answer_text(root, settled) == "resumed"
    assert seen[-1][-2:] == ["assistant", "toolResult"]
    await harness.close(context)


async def test_is_submitted_by_a_tool_through_the_handle_of_a_conversation_it_owns() -> None:
    setup = chat_setup()
    busy = deferred()
    release = deferred()
    outcome: dict[str, Any] = {}

    def delegate(_args: Any, api: Any, call_context: Any) -> Any:
        async def work() -> dict[str, Any]:
            async def create(tx: Any) -> Any:
                created = await tx.create_conversation(
                    {"ownership": {"kind": "task", "taskId": api.task_id}}
                )
                await configure(tx, created["id"], {"model": {"provider": "faux", "modelId": "faux-1"}})
                return created["id"]

            child = await api.commit(create, call_context)
            handle = await api.conversation(child, call_context)
            first = await handle.submit({"type": "input", "content": "work"}, call_context)
            settled = await first.wait(call_context)
            outcome["first"] = settled["status"]
            resumed = await handle.submit({"type": "continue"}, call_context)
            await busy.promise
            try:
                outcome["busy"] = await handle.submit({"type": "continue"}, call_context)
            except BaseException as error:
                outcome["busy"] = error
            release.resolve()
            settled = await resumed.wait(call_context)
            outcome["resumed"] = settled["status"]
            return {"output": [{"type": "text", "text": settled["status"]}]}

        return work()

    add_tool(setup.registry, tool("delegate", delegate))
    setup.faux.set_responses(
        [
            assistant_message([faux_tool_call("delegate", {}, id="c1")], stop_reason="toolUse"),
            failure,
            _gated(busy, release, assistant_message("child recovered")),
            assistant_message("done"),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert outcome["first"] == "unanswered"
    assert outcome["resumed"] == "done"
    assert isinstance(outcome["busy"], ConversationBusy)
    await harness.close(context)


def _gated(busy: Any, release: Any, message: dict[str, Any]) -> Any:
    """A faux step that tells the tool the continue's run started, and waits until it is released."""

    async def step(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        busy.resolve()
        await release.promise
        return message

    return step


async def test_places_inputs_a_failed_run_left_queued_first_and_they_join_its_run() -> None:
    setup = chat_setup()
    gate = deferred()
    reached = deferred()

    async def failing(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        reached.resolve()
        await gate.promise
        return failure

    setup.faux.set_responses([failing, assistant_message("both handled")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.submit({"type": "input", "content": "first"}, context)
    await reached.promise
    queued = await root.submit(
        {"type": "input", "content": "second", "when_busy": "followUp"}, context
    )
    gate.resolve()
    await harness.wait_for_idle(context)
    assert (await queued.status(context))["status"] == "queued"

    resumed = await root.submit({"type": "continue"}, context)
    live = await harness.snapshot(LiveDoc, root.id, context)
    assert live is not None
    assert live["run"]["inputs"] == [queued.id, resumed.id]
    by_continue = await resumed.wait(context)
    by_queued = await queued.wait(context)
    assert await answer_text(root, by_continue) == "both handled"
    assert by_queued["status"] == "done" and by_queued["answer"] == by_continue["answer"]
    await harness.close(context)

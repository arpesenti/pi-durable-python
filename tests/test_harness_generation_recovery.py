"""Translated upstream ``harness-generation-recovery`` cases: reopen resumes committed progress."""

from __future__ import annotations

import asyncio
from typing import Any, cast

from chat_support import (
    AssistantStream,
    ChatSetup,
    Step,
    all_entries,
    assistant_message,
    chat_setup,
    faux_text,
    open_chat,
    text_of,
    usage,
)
from harness_support import add_section
from session_support import ControlledStorage, context, flush
from task_support import aborted, eventually
from tool_support import DONE, add_tool, calls, result_text, results, tool

from pi_durable import GenerationTask, LiveDoc, UsageDoc
from pi_durable.context import AbortError


def hang_after_partial(text: str, spend: dict[str, Any] | None = None) -> Step:
    """A response that commits one partial, then blocks until its attempt is interrupted."""

    def build(_request: dict[str, Any], options: dict[str, Any]) -> AssistantStream:
        async def events() -> Any:
            partial = assistant_message([faux_text(text)], usage=spend)
            partial["stopReason"] = "pending"
            yield {"type": "start", "partial": partial}
            yield {"type": "text_delta", "partial": partial}
            signal = options.get("signal")
            if signal is not None:
                await signal.wait()
            raise AbortError(None)

        never: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        return AssistantStream(events(), never)

    return Step(build)


async def open_again(storage: ControlledStorage, setup: ChatSetup) -> dict[str, Any]:
    return await open_chat(storage.reopen(), setup)


async def live_state(harness: Any, conversation_id: int) -> dict[str, Any]:
    value = await harness.snapshot(LiveDoc, conversation_id, context)
    return {} if value is None else dict(value)


async def generation_record(harness: Any, conversation_id: int) -> dict[str, Any] | None:
    page = await harness.commit(
        lambda tx: tx.scan_tasks({"kind": GenerationTask.definition.name}, 100),
        context,
    )
    owned = [
        record
        for record in page["items"]
        if record["conversationId"] == conversation_id
    ]
    if not owned:
        return None
    return sorted(owned, key=lambda record: record["id"])[-1]


async def capture_partial(harness: Any, conversation_id: int) -> dict[str, Any]:
    seen: dict[str, Any] = {}

    async def check() -> bool:
        live = await live_state(harness, conversation_id)
        generation = live.get("generation") or {}
        if "message" not in generation:
            return False
        seen["live"] = live
        return True

    await eventually(check)
    return seen["live"]


async def usage_input(harness: Any, conversation_id: int) -> int:
    state = await harness.snapshot(UsageDoc, conversation_id, context)
    models = (state or {}).get("models") or {}
    return sum(int(entry["input"]) for entry in models.values())


async def test_resumes_an_interrupted_request_from_its_committed_partial() -> None:
    storage = ControlledStorage()
    setup = chat_setup()
    setup.settings["progress"] = {"partial_interval_ms": 0}
    add_section(setup.registry, "preamble", lambda *_args: "p", {"tag": False})
    setup.faux.set_responses([hang_after_partial("half an ans", usage(input=3))])
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    live = await capture_partial(harness, root.id)
    assert [entry["kind"] for entry in await all_entries(root)] == ["pi.user", "pi.system"]
    assert live["run"]["inputs"] == [submission.id]
    await harness.close(context)

    setup.faux.set_responses([assistant_message("the answer", usage=usage(input=5))])
    recovered = await open_again(storage, setup)
    harness, root = recovered["harness"], recovered["root"]
    before = await generation_record(harness, root.id)
    assert before is not None
    assert before["state"]["status"] == "pending"
    assert before["state"]["checkpoint"]["phase"] == "request"
    assert before["state"]["checkpoint"]["attempt"] == 1

    harness.resume()
    settled = await cast(Any, await harness.submission(submission.id, context)).wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "pi.assistant",
    ]
    assert [entry["kind"] for entry in entries].count("pi.system") == 1
    assert entries[2]["model"][0]["stopReason"] == "aborted"
    assert text_of(entries[2]["model"][0]) == "half an ans"
    assert text_of(entries[3]["model"][0]) == "the answer"
    assert settled["answer"] == entries[3]["id"]
    assert await live_state(harness, root.id) == {}
    assert await usage_input(harness, root.id) == 8
    after = await generation_record(harness, root.id)
    assert after is not None
    assert after["state"] == {
        "status": "terminal",
        "outcome": {"status": "completed", "result": {"entryId": entries[3]["id"]}},
    }
    await harness.close(context)


async def test_resumes_an_interrupted_tool_round_without_repeating_the_answer() -> None:
    storage = ControlledStorage()
    setup = chat_setup()
    started = asyncio.Event()
    runs = {"count": 0}

    async def work(_args: Any, _api: Any, call_context: Any) -> Any:
        runs["count"] += 1
        if runs["count"] == 1:
            started.set()
            await aborted(call_context.abort_signal)
        return {"output": [{"type": "text", "text": "tool output"}]}

    add_tool(setup.registry, tool("work", work, replay="safe"))
    setup.faux.set_responses([calls(("work", {}, "c1")), DONE])
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await started.wait()
    await harness.close(context)

    setup.faux.set_responses([DONE])
    recovered = await open_again(storage, setup)
    harness, root = recovered["harness"], recovered["root"]
    harness.resume()
    settled = await cast(Any, await harness.submission(submission.id, context)).wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    kinds = [entry["kind"] for entry in entries]
    assert kinds.count("pi.assistant") == 2
    assert kinds.count("pi.tool-result") == 1
    assistant_entries = [entry for entry in entries if entry["kind"] == "pi.assistant"]
    assert assistant_entries[0]["model"][0]["stopReason"] == "toolUse"
    assert text_of(assistant_entries[1]["model"][0]) == "done"
    assert [result_text(message) for message in results(entries)] == ["tool output"]
    assert runs["count"] == 2
    assert await live_state(harness, root.id) == {}
    await harness.close(context)


async def test_resumes_an_interrupted_retry_backoff_with_its_attempt() -> None:
    storage = ControlledStorage()
    clock = {"now": 1_000}
    setup = chat_setup()
    setup.now = lambda: clock["now"]
    setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 60_000}
    setup.faux.set_responses(
        [
            assistant_message((), stop_reason="error", error_message="503 Service Unavailable"),
            assistant_message("after the wait"),
        ]
    )
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)

    async def retrying() -> bool:
        record = await generation_record(harness, root.id)
        return record is not None and record["state"]["checkpoint"]["phase"] == "retry"

    await eventually(retrying)
    record = await generation_record(harness, root.id)
    assert record is not None
    until = record["state"]["checkpoint"]["until"]
    assert until == clock["now"] + 60_000
    await harness.close(context)

    clock["now"] = until
    setup.faux.set_responses([assistant_message("after the wait")])
    recovered = await open_again(storage, setup)
    harness, root = recovered["harness"], recovered["root"]
    record = await generation_record(harness, root.id)
    assert record is not None
    assert record["state"]["checkpoint"]["phase"] == "retry"
    assert record["state"]["checkpoint"]["attempt"] == 1
    harness.resume()
    settled = await cast(Any, await harness.submission(submission.id, context)).wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.assistant",
        "pi.assistant",
    ]
    assert entries[1]["model"][0]["stopReason"] == "error"
    assert text_of(entries[2]["model"][0]) == "after the wait"
    assert setup.faux.call_count == 1
    await harness.close(context)


async def test_resumes_an_interrupted_deferred_poll() -> None:
    storage = ControlledStorage()
    clock = {"now": 1_000}
    setup = chat_setup({"deferred": {"pendingFetches": 1, "pollAfterMs": 60_000}})
    setup.now = lambda: clock["now"]
    setup.settings["stream"] = {"deferred": True}
    setup.faux.set_responses([assistant_message("deferred answer")])
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)

    async def polling() -> bool:
        record = await generation_record(harness, root.id)
        return record is not None and record["state"]["checkpoint"]["phase"] == "poll"

    await eventually(polling)
    polled = await generation_record(harness, root.id)
    assert polled is not None
    handle = polled["state"]["checkpoint"]["handle"]
    poll_at = polled["state"]["checkpoint"]["pollAt"]
    await harness.close(context)

    clock["now"] = poll_at
    setup.faux.deferred["pendingFetches"] = 0
    setup.faux.set_responses([assistant_message("deferred answer")])
    recovered = await open_again(storage, setup)
    harness, root = recovered["harness"], recovered["root"]
    record = await generation_record(harness, root.id)
    assert record is not None
    assert record["state"]["checkpoint"]["phase"] == "poll"
    assert record["state"]["checkpoint"]["handle"] == handle
    harness.resume()
    settled = await cast(Any, await harness.submission(submission.id, context)).wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    assert text_of(entries[-1]["model"][0]) == "deferred answer"
    assert await live_state(harness, root.id) == {}
    await harness.close(context)


async def test_does_not_repeat_a_generation_that_finished_before_the_restart() -> None:
    storage = ControlledStorage()
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("once", usage=usage(input=4))])
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    assert len(entries) == 2
    await harness.close(context)

    calls = setup.faux.call_count
    recovered = await open_again(storage, setup)
    harness, root = recovered["harness"], recovered["root"]
    harness.resume()
    for _ in range(3):
        await flush()
    assert setup.faux.call_count == calls
    assert [entry["id"] for entry in await all_entries(root)] == [entry["id"] for entry in entries]
    assert await usage_input(harness, root.id) == 4
    task = await generation_record(harness, root.id)
    assert task is not None and task["state"]["status"] == "terminal"
    inspection = await harness.inspect(context)
    assert inspection["tasks"] == []
    await harness.close(context)

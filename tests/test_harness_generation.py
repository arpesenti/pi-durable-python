"""Translated upstream ``harness-generation`` cases: the generation run loop and streaming."""

from __future__ import annotations

import asyncio
import re
from typing import Any, cast

import pytest
from chat_support import (
    AssistantStream,
    FauxModels,
    Step,
    all_entries,
    assistant_message,
    chat_setup,
    faux_tool_call,
    live_of,
    open_chat,
    slow_message,
    text_of,
    unanswered,
    usage,
)
from harness_support import add_section
from session_support import ControlledStorage, context, document_changes
from task_support import eventually

from pi_durable import (
    AssistantEntry,
    GenerationTask,
    Harness,
    LiveDoc,
    MemoryStorage,
    Models,
    ProviderDoc,
    UsageDoc,
    UserEntry,
    define_extension,
    hook,
)
from pi_durable.context import AbortSignal
from pi_durable.harness.agent import resolve_settings
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import HarnessSettings, RegistrySnapshot

ERROR_503 = assistant_message((), stop_reason="error", error_message="503 Service Unavailable")


async def live(harness: Any, conversation: Any) -> dict[str, Any]:
    return live_of(await harness.snapshot(LiveDoc, conversation.id, context))


async def run_task(harness: Any, conversation: Any) -> Any:
    found: dict[str, Any] = {}

    async def check() -> bool:
        value = await live(harness, conversation)
        if "run" in value:
            found["task"] = value["run"]["taskId"]
            return True
        return False

    await eventually(check)
    return found["task"]


def live_publications(harness: Any) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] == "pi.live" and change["value"] is not None:
                values.append(change["value"])

    harness.subscribe_commits(observe)
    return values


async def test_answers_an_input_and_settles_its_submission() -> None:
    setup = chat_setup()
    add_section(setup.registry, "preamble", lambda *_args: "You are helpful.", {"tag": False})
    setup.faux.set_responses([assistant_message("Hello there")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "done" and settled["type"] == "input"

    answer = await root.commit(lambda tx: tx.entry(AssistantEntry, settled["answer"]), context)
    assert text_of(answer["model"][0]) == "Hello there"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.system", "pi.assistant"]
    assert entries[0]["id"] == settled["entry"]
    system_message = entries[1]["model"][0]
    assert system_message["sections"] == {"preamble": "You are helpful."}
    assert system_message["content"] == ""
    assert isinstance(system_message["timestamp"], int)
    assert await live(harness, root) == {}
    task = (await root.commit(lambda tx: tx.scan_tasks({"conversationId": root.id}, 10), context))["items"][0]
    assert task["kind"] == "pi.generation"
    assert [entry.get("byTaskId") for entry in entries] == [None, task["id"], task["id"]]
    assert task["state"] == {
        "status": "terminal",
        "outcome": {"status": "completed", "result": {"entryId": settled["answer"]}},
    }
    await harness.close(context)


async def test_stores_partials_as_deltas_and_a_complete_base_once_nothing_is_in_flight() -> None:
    setup = chat_setup()
    setup.faux.set_responses([slow_message(assistant_message("w" * 200), chunks=8, interval=0.03)])
    storage = ControlledStorage()
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    record = await storage.find_document(
        {"kind": "pi.live", "scope": {"kind": "conversation", "conversationId": root.id}},
        "current",
        context,
    )
    harness.resume()
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert record is not None
    contents = [
        write["content"]["kind"]
        for writes in storage.commits
        for write in writes
        if write["type"] == "document.change" and write["id"] == record["id"]
    ]
    assert "delta" in contents
    assert contents[-1] == "base"
    await harness.close(context)


async def test_still_ends_a_run_whose_input_something_else_already_settled() -> None:
    setup = chat_setup()
    setup.faux.set_responses([unanswered()])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await eventually(lambda: _requested(setup))
    async def withdraw(tx: Any) -> None:
        tx.settle_submission(submission.id, {"status": "unanswered", "reason": "withdrawn"})

    await root.commit(withdraw, context)
    task_id = await run_task(harness, root)
    assert await harness.abort_task(task_id, context) == "marked"
    assert (await harness.wait_for_task(task_id, context))["state"]["outcome"] == {"status": "aborted"}
    assert (await submission.status(context))["reason"] == "withdrawn"
    assert await live(harness, root) == {}
    await harness.close(context)


async def _requested(setup: Any) -> bool:
    return setup.faux.call_count > 0


async def test_fails_with_no_model_when_no_model_is_configured_or_the_model_is_unknown() -> None:
    setup = chat_setup()
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    plain = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    harness.resume()
    unset = await (await plain.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert unset["status"] == "unanswered" and unset["reason"] == "no_model"

    await root.configure({"model": {"provider": "faux", "modelId": "missing"}}, context)
    unknown = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert unknown["status"] == "unanswered" and unknown["reason"] == "no_model"
    assert "entry" in unknown
    assert [entry["kind"] for entry in await all_entries(root)] == ["pi.user"]
    tasks = await root.commit(lambda tx: tx.scan_tasks({"conversationId": root.id}, 10), context)
    assert tasks["items"][0]["state"] == {
        "status": "terminal",
        "outcome": {
            "status": "failed",
            "error": {
                "message": "Model faux/missing is not available",
                "detail": {"reason": "no_model"},
            },
        },
    }
    assert await live(harness, root) == {}
    assert await live(harness, plain) == {}
    await harness.close(context)


async def test_retries_a_retryable_error_after_a_durable_backoff_and_then_answers() -> None:
    setup = chat_setup()
    add_section(setup.registry, "preamble", lambda *_args: "p", {"tag": False})
    setup.faux.set_responses([ERROR_503, assistant_message("recovered")])
    setup.settings["retry"] = {"enabled": True, "max_retries": 3, "base_delay_ms": 1}
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values = live_publications(harness)
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "pi.assistant",
    ]
    assert entries[2]["model"][0]["stopReason"] == "error"
    assert any(
        value.get("generation", {}).get("retry", {}).get("error") == "503 Service Unavailable"
        for value in values
    )
    assert any(value.get("generation", {}).get("attempt") == 2 for value in values)
    assert await live(harness, root) == {}
    await harness.close(context)


async def test_fails_with_model_error_once_retries_are_exhausted() -> None:
    setup = chat_setup()
    setup.faux.set_responses([ERROR_503, ERROR_503, assistant_message("never")])
    setup.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == "503 Service Unavailable"
    assert [entry["kind"] for entry in await all_entries(root)] == [
        "pi.user",
        "pi.assistant",
        "pi.assistant",
    ]
    assert setup.faux.get_pending_response_count() == 1
    await harness.close(context)


async def test_fails_a_retryable_error_without_retrying_when_the_policy_is_disabled() -> None:
    setup = chat_setup()
    setup.faux.set_responses([ERROR_503, assistant_message("never")])
    setup.settings["retry"] = {"enabled": False}
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert setup.faux.call_count == 1
    await harness.close(context)


async def test_reports_section_wrapper_failures_while_preparing() -> None:
    setup = chat_setup()
    add_section(setup.registry, "cwd", lambda *_args: "/repo")
    from pi_durable import define_extension, wrap_section

    def broken(_section: Any) -> Any:
        raise RuntimeError("wrapper failed")

    setup.registry.install(define_extension({"name": "broken", "wraps": [wrap_section("cwd", broken)]}))
    setup.faux.set_responses([assistant_message("ok")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    assert (await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context))[
        "status"
    ] == "done"
    assert any(str(error) == "wrapper failed" for error in setup.reports)
    assert [entry["kind"] for entry in await all_entries(root)] == ["pi.user", "pi.assistant"]
    await harness.close(context)


async def test_fails_a_non_retryable_error_without_retrying() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [
            assistant_message((), stop_reason="error", error_message="Invalid request"),
            assistant_message("never"),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    assert settled["detail"] == "Invalid request"
    tasks = await root.commit(lambda tx: tx.scan_tasks({"conversationId": root.id}, 10), context)
    assert tasks["items"][0]["state"]["outcome"] == {
        "status": "failed",
        "error": {"message": "Invalid request", "detail": {"reason": "model_error"}},
    }
    await harness.close(context)


async def test_polls_a_deferred_response_until_it_is_ready() -> None:
    setup = chat_setup({"deferred": {"pendingFetches": 1, "pollAfterMs": 1}})
    setup.faux.set_responses([assistant_message("deferred answer")])
    setup.settings["stream"] = {"deferred": True}
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values = live_publications(harness)
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert settled["status"] == "done"
    assert setup.faux.deferred_fetch_count == 2
    poll_times = [
        value["generation"]["deferred"]["pollAt"]
        for value in values
        if "deferred" in value.get("generation", {})
    ]
    assert len(poll_times) == 2
    assert poll_times[1] > poll_times[0]
    answer = await root.commit(lambda tx: tx.entry(AssistantEntry, settled["answer"]), context)
    assert text_of(answer["model"][0]) == "deferred answer"
    await harness.close(context)


async def test_converts_the_committed_partial_when_aborted_during_streaming() -> None:
    setup = chat_setup()
    setup.faux.set_responses([slow_message(assistant_message("x" * 400), chunks=40, interval=0.03)])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    task_id = await run_task(harness, root)
    partial = await _wait_partial(harness, root)
    assert await harness.abort_task(task_id, context) == "marked"
    assert (await submission.wait(context))["status"] == "unanswered"
    assert (await submission.wait(context))["reason"] == "aborted"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant"]
    converted = entries[1]["model"][0]
    assert converted["stopReason"] == "aborted"
    converted_text = text_of(converted)
    partial_text = text_of(partial)
    assert converted_text is not None and partial_text is not None
    assert converted_text.startswith(partial_text)
    assert await live(harness, root) == {}
    assert (await harness.wait_for_task(task_id, context))["state"]["outcome"] == {"status": "aborted"}
    await harness.close(context)


async def _wait_partial(harness: Any, root: Any) -> dict[str, Any]:
    found: dict[str, Any] = {}

    async def check() -> bool:
        value = await live(harness, root)
        message = value.get("generation", {}).get("message")
        if message is not None:
            found["message"] = message
            return True
        return False

    await eventually(check)
    return found["message"]


async def test_cancels_a_deferred_response_when_aborted_during_polling() -> None:
    setup = chat_setup({"deferred": {"pendingFetches": 100, "pollAfterMs": 60_000}})
    setup.faux.set_responses([assistant_message("never")])
    setup.settings["stream"] = {"deferred": True}
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    task_id = await run_task(harness, root)
    await eventually(lambda: _deferred_pending(harness, root))
    await harness.abort_task(task_id, context)
    assert (await submission.wait(context))["status"] == "unanswered"
    assert (await submission.wait(context))["reason"] == "aborted"
    assert len(setup.faux.cancelled_deferred) == 1
    assert await live(harness, root) == {}
    await harness.close(context)


async def _deferred_pending(harness: Any, root: Any) -> bool:
    value = await live(harness, root)
    return "deferred" in value.get("generation", {})


async def test_reports_a_failed_deferred_cancellation_and_still_ends_the_run_aborted() -> None:
    setup = chat_setup({"deferred": {"pendingFetches": 100, "pollAfterMs": 60_000}})
    setup.faux.set_responses([assistant_message("never")])
    setup.settings["stream"] = {"deferred": True}

    async def cancel_failed(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("cancel failed")

    setup.faux.cancel_deferred = cancel_failed  # type: ignore[method-assign]
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    task_id = await run_task(harness, root)
    await eventually(lambda: _deferred_pending(harness, root))
    await harness.abort_task(task_id, context)
    assert (await submission.wait(context))["status"] == "unanswered"
    assert (await submission.wait(context))["reason"] == "aborted"
    assert any(str(error) == "cancel failed" for error in setup.reports)
    assert await live(harness, root) == {}
    await harness.close(context)


async def test_forwards_stream_options_and_the_thinking_level() -> None:
    setup = chat_setup()
    seen: list[dict[str, Any]] = []

    def capture(_request: Any, options: Any) -> dict[str, Any]:
        seen.append(dict(options))
        return assistant_message("a" if len(seen) == 1 else "b")

    setup.faux.set_responses([capture, capture])
    setup.settings["stream"] = {"timeout_ms": 1_234, "headers": {"x-test": "1"}}
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"thinkingLevel": "high"}, context)
    harness.resume()
    await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    await root.configure({"thinkingLevel": None}, context)
    setup.settings["stream"] = {"timeout_ms": 99}
    await (await root.submit({"type": "input", "content": "two"}, context)).wait(context)
    session_id = (await harness.snapshot(ProviderDoc, root.id, context))["sessionId"]
    assert seen[0]["timeoutMs"] == 1_234
    assert seen[0]["headers"] == {"x-test": "1"}
    assert seen[0]["reasoning"] == "high"
    assert seen[0]["sessionId"] == session_id
    assert isinstance(seen[0]["signal"], AbortSignal)
    assert "reasoning" not in seen[1]
    assert seen[1]["timeoutMs"] == 99
    assert "headers" not in seen[1]
    assert seen[1]["sessionId"] == session_id
    await harness.close(context)


async def test_keeps_provider_session_ids_request_local_across_concurrent_conversations() -> None:
    setup = chat_setup()
    seen: dict[str, list[str]] = {}

    def capture(request: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
        message = next(
            (
                candidate
                for candidate in reversed(request["messages"])
                if candidate["role"] == "user"
            ),
            None,
        )
        content = message["content"] if message is not None else ""
        text = content if isinstance(content, str) else ""
        seen.setdefault(text, []).append(options.get("sessionId", ""))
        return assistant_message(f"answer:{text}")

    setup.faux.set_responses([capture, capture, capture, capture])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    child = await harness.create_conversation(
        {
            "ownership": {"kind": "ownerless"},
            "agent": {"model": {"provider": "faux", "modelId": "faux-1"}},
        },
        context,
    )
    harness.resume()
    await asyncio.gather(
        *[
            (await conversation.submit({"type": "input", "content": f"first-{index}"}, context)).wait(
                context
            )
            for index, conversation in enumerate([root, child])
        ]
    )
    await asyncio.gather(
        *[
            (await conversation.submit({"type": "input", "content": f"second-{index}"}, context)).wait(
                context
            )
            for index, conversation in enumerate([root, child])
        ]
    )
    root_id = (await harness.snapshot(ProviderDoc, root.id, context))["sessionId"]
    child_id = (await harness.snapshot(ProviderDoc, child.id, context))["sessionId"]
    assert root_id != child_id
    assert seen["first-0"] == [root_id]
    assert seen["second-0"] == [root_id]
    assert seen["first-1"] == [child_id]
    assert seen["second-1"] == [child_id]
    await harness.close(context)


async def test_creates_and_persists_provider_state_before_a_legacy_conversation_request() -> None:
    setup = chat_setup()
    sent: dict[str, Any] = {}

    def capture(_request: Any, options: Any) -> dict[str, Any]:
        sent["session"] = options.get("sessionId")
        return assistant_message("ok")

    setup.faux.set_responses([capture])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.commit(lambda tx: tx.retire_doc(ProviderDoc, root.id), context)
    assert await harness.snapshot(ProviderDoc, root.id, context) is None
    harness.resume()
    await (await root.submit({"type": "input", "content": "legacy"}, context)).wait(context)
    stored = await harness.snapshot(ProviderDoc, root.id, context)
    assert re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        stored["sessionId"],
    )
    assert sent["session"] == stored["sessionId"]
    await harness.close(context)


async def test_reads_settings_through_getters_at_every_decision() -> None:
    setup = chat_setup()
    timeouts = {"value": 111}
    seen: list[int | None] = []

    def capture(_request: Any, options: Any) -> dict[str, Any]:
        seen.append(options.get("timeoutMs"))
        timeouts["value"] = 222
        return ERROR_503

    def capture_again(_request: Any, options: Any) -> dict[str, Any]:
        seen.append(options.get("timeoutMs"))
        return assistant_message("ok")

    setup.faux.set_responses([capture, capture_again])

    class Settings:
        @property
        def stream(self) -> dict[str, Any]:
            return {"timeout_ms": timeouts["value"]}

        retry = {"base_delay_ms": 1}

    harness = await Harness.open(
        MemoryStorage(),
        {"models": setup.models, "registry": setup.registry, "settings": Settings()},
        context,
    )
    root = await harness.root(
        context, {"agent": {"model": {"provider": "faux", "modelId": "faux-1"}}}
    )
    harness.resume()
    assert (await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context))[
        "status"
    ] == "done"
    assert seen == [111, 222]
    await harness.close(context)


async def test_resolves_settings_over_the_builtin_defaults() -> None:
    declared: HarnessSettings = {"stream": {"timeout_ms": 5}, "retry": {"max_retries": 1}}
    assert resolve_settings(declared).stream == {"timeout_ms": 5}
    assert resolve_settings(declared).retry["max_retries"] == 1
    resolved = resolve_settings(None)
    assert resolved.stream == {}
    assert resolved.retry == {
        "enabled": True,
        "max_retries": 3,
        "base_delay_ms": 2_000,
        "max_agent_delay_ms": 60_000,
    }
    assert resolved.compaction == {
        "enabled": True,
        "reserve_tokens": 16_384,
        "keep_recent_tokens": 20_000,
        "background_tokens": 32_768,
    }
    assert resolved.progress == {"partial_interval_ms": 100, "output_interval_ms": 100}
    assert resolved.tool_execution == "parallel"
    assert resolved.steering_mode == "one-at-a-time"
    assert resolved.follow_up_mode == "one-at-a-time"
    assert resolved.context_retention_ms == 600_000
    assert resolved.document_retention_ms is not None
    assert resolve_settings({"retry": {"enabled": False}, "compaction": {"background_tokens": 0}}).retry == {
        "enabled": False,
        "max_retries": 3,
        "base_delay_ms": 2_000,
        "max_agent_delay_ms": 60_000,
    }
    assert resolve_settings({"compaction": {"background_tokens": 0}}).compaction["background_tokens"] == 0
    assert resolve_settings({"progress": {"output_interval_ms": 500}}).progress == {
        "partial_interval_ms": 100,
        "output_interval_ms": 500,
    }


async def test_commits_partials_no_more_often_than_the_partial_interval() -> None:
    async def published(settings: dict[str, Any]) -> bool:
        setup = chat_setup()
        setup.settings.update(settings)

        def short(_request: dict[str, Any], _options: dict[str, Any]) -> AssistantStream:
            async def events() -> Any:
                yield {
                    "type": "start",
                    "partial": {**assistant_message("partial"), "stopReason": "pending"},
                }
                await asyncio.sleep(0.3)

            async def result() -> dict[str, Any]:
                return assistant_message("final")

            return AssistantStream(events(), result())

        setup.faux.set_responses([Step(short)])
        opened = await open_chat(MemoryStorage(), setup)
        harness, root = opened["harness"], opened["root"]
        values = live_publications(harness)
        harness.resume()
        assert (await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context))[
            "status"
        ] == "done"
        await harness.close(context)
        return any(
            text_of(value.get("generation", {}).get("message")) == "partial" for value in values
        )

    assert await published({}) is True
    assert await published({"progress": {"partial_interval_ms": 5_000}}) is False


async def test_renders_sections_that_read_conversation_documents_through_input_read() -> None:
    from pi_durable import define_doc

    Agent = define_doc(
        {
            "kind": "test.agent",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"cwd": "/", "kind": "main"},
        }
    )
    setup = chat_setup()
    add_section(
        setup.registry,
        "cwd",
        lambda input, ctx: _agent_cwd(input.read, Agent, input.conversation_id, ctx),
    )
    add_section(
        setup.registry,
        "agents",
        lambda input, ctx: _agent_agents(input.read, Agent, input.conversation_id, ctx),
    )
    setup.faux.set_responses([assistant_message("a"), assistant_message("b")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]

    async def set_cwd(tx: Any) -> None:
        agent = await tx.doc(Agent, root.id)
        agent["cwd"] = "/repo"

    await root.commit(set_cwd, context)

    async def init(tx: Any, id: Any) -> None:
        agent = await tx.doc(Agent, id)
        agent["kind"] = "sub"
        agent["cwd"] = "/sub"

    sub = await harness.create_conversation(
        {
            "ownership": {"kind": "ownerless"},
            "agent": {"model": {"provider": "faux", "modelId": "faux-1"}},
            "init": init,
        },
        context,
    )
    harness.resume()
    await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    await (await sub.submit({"type": "input", "content": "two"}, context)).wait(context)

    async def system_of(conversation: Any) -> dict[str, Any]:
        entries = await all_entries(conversation)
        return next(entry for entry in entries if entry["kind"] == "pi.system")["model"][0]

    assert (await system_of(root))["sections"] == {
        "cwd": "<cwd>\n/repo\n</cwd>",
        "agents": "<agents>\nRead AGENTS.md\n</agents>",
    }
    sub_system = await system_of(sub)
    assert sub_system["role"] == "system"
    assert sub_system["content"] == ""
    assert sub_system["sections"] == {"cwd": "<cwd>\n/sub\n</cwd>"}
    assert isinstance(sub_system["timestamp"], int)
    await harness.close(context)


async def _agent_cwd(read: Any, token: Any, conversation_id: Any, ctx: Any) -> str | None:
    value = await read.snapshot(token, conversation_id, ctx)
    return None if value is None else value["cwd"]


async def _agent_agents(read: Any, token: Any, conversation_id: Any, ctx: Any) -> str | None:
    value = await read.snapshot(token, conversation_id, ctx)
    return None if value is not None and value["kind"] == "sub" else "Read AGENTS.md"


async def test_commits_no_partial_for_a_response_that_turns_deferred_after_an_empty_start() -> None:
    setup = chat_setup()
    handle = {
        "provider": "faux",
        "modelId": "faux-1",
        "api": "faux",
        "id": "handle-1",
        "pollAfterMs": 60_000,
    }

    def deferred_stream(_request: dict[str, Any], _options: dict[str, Any]) -> AssistantStream:
        async def events() -> Any:
            yield {"type": "start", "partial": assistant_message((), stop_reason="pending")}
            await asyncio.sleep(0.3)

        async def result() -> dict[str, Any]:
            return assistant_message((), stop_reason="deferred", deferred=handle)

        return AssistantStream(events(), result())

    setup.faux.set_responses([Step(deferred_stream)])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values = live_publications(harness)
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await eventually(lambda: _deferred_pending(harness, root))
    assert not any(
        "message" in value.get("generation", {}) for value in values
    )
    await harness.abort_task((await live(harness, root))["run"]["taskId"], context)
    assert (await submission.wait(context))["status"] == "unanswered"
    assert (await submission.wait(context))["reason"] == "aborted"
    assert [entry["kind"] for entry in await all_entries(root)] == ["pi.user"]
    await harness.close(context)


async def test_faults_a_run_task_settling_its_inputs_and_converting_the_committed_partial() -> None:
    setup = chat_setup()

    def invalid(_request: dict[str, Any], _options: dict[str, Any]) -> AssistantStream:
        async def events() -> Any:
            yield {
                "type": "start",
                "partial": {**assistant_message("partial"), "stopReason": "pending"},
            }
            await asyncio.sleep(0.3)

        async def result() -> dict[str, Any]:
            return {**assistant_message("final"), "invalid": lambda: None}

        return AssistantStream(events(), result())

    setup.faux.set_responses([Step(invalid)])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values = live_publications(harness)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "faulted"
    assert "non-JSON" in settled["detail"]
    assert any(
        text_of(value.get("generation", {}).get("message")) == "partial" for value in values
    )
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant"]
    assert entries[1]["model"][0]["role"] == "assistant"
    assert entries[1]["model"][0]["stopReason"] == "aborted"
    assert text_of(entries[1]["model"][0]) == "partial"
    assert await live(harness, root) == {}
    tasks = await root.commit(lambda tx: tx.scan_tasks({"conversationId": root.id}, 10), context)
    assert tasks["items"][0]["state"]["status"] == "terminal"
    assert tasks["items"][0]["state"]["outcome"]["status"] == "faulted"
    await harness.close(context)


async def test_orphans_a_blocked_run_task_with_full_run_cleanup() -> None:
    setup = chat_setup()
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]

    async def blocked(tx: Any) -> dict[str, Any]:
        entry = await tx.append_entry(
            UserEntry, root.id, {"model": [{"role": "user", "content": "hi", "timestamp": 1}]}
        )
        submission = await tx.create_submission(
            {
                "conversationId": root.id,
                "type": "input",
                "status": "placed",
                "entry": entry["id"],
            }
        )
        definition = GenerationTask.definition
        newer = type(definition)(
            name=definition.name,
            version=2,
            initial=definition.initial,
            phases=definition.phases,
            abort=definition.abort,
            migrate=definition.migrate,
            hooks=definition.hooks,
        )
        task_id = await tx.create_task(
            _task_like(newer),
            {},
            {"ownership": {"kind": "conversation"}, "conversationId": root.id},
        )
        live = await tx.doc(LiveDoc, root.id)
        live["run"] = {"taskId": task_id, "inputs": [submission["id"]]}
        return {"taskId": task_id, "submissionId": submission["id"]}

    ids = await harness.commit(blocked, context)
    harness.resume()
    with pytest.raises(Exception, match="is busy"):
        await root.submit({"type": "input", "content": "busy", "when_busy": "reject"}, context)
    assert await harness.abort_task(ids["taskId"], context) == "marked"
    assert (await harness.wait_for_task(ids["taskId"], context))["state"]["outcome"] == {
        "status": "orphaned",
        "reason": "task_too_old",
    }
    submission = await harness.submission(ids["submissionId"], context)
    assert (await submission.status(context))["status"] == "unanswered"
    assert (await submission.status(context))["reason"] == "task_too_old"
    assert await live(harness, root) == {}
    await harness.close(context)


class _task_like:
    def __init__(self, definition: Any) -> None:
        self.definition = definition


async def test_rejects_a_registry_without_the_builtin_tasks() -> None:
    empty = create_registry().snapshot()

    class Snapshot:
        def installed(self) -> Any:
            return []

        def extension(self, _name: str) -> None:
            return None

        def tools(self) -> list[Any]:
            return []

        def sections(self) -> list[Any]:
            return []

        def tasks(self) -> list[Any]:
            return [task for task in empty.tasks() if task.definition.name != "pi.generation"]

        def task(self, name: str) -> Any:
            return None if name == "pi.generation" else empty.task(name)

    snapshot: RegistrySnapshot = Snapshot()
    with pytest.raises(Exception, match="Registry lacks built-in tasks pi.generation"):
        await Harness.open(
            MemoryStorage(),
            {"models": chat_setup().models, "registry": _reader(snapshot)},
            context,
        )


class _reader:
    def __init__(self, snapshot: RegistrySnapshot) -> None:
        self._snapshot = snapshot

    def snapshot(self) -> RegistrySnapshot:
        return self._snapshot

    def subscribe(self, _listener: Any) -> Any:
        return lambda: None

async def test_runs_before_request_and_after_response_hooks_for_every_attempt() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("first"), assistant_message("second")])
    seen: list[list[str]] = []
    responses: list[str] = []

    def before_request(request: Any, _api: Any, _context: Any) -> Any:
        seen.append([message["role"] for message in request["messages"]])
        return {"messages": [{"role": "user", "content": "rewritten", "timestamp": 1}]}

    def after_response(message: Any, _api: Any, _context: Any) -> None:
        responses.append(message["stopReason"])

    setup.registry.install(
        define_extension(
            {
                "name": "hooks",
                "hooks": [
                    hook(
                        GenerationTask,
                        {"beforeRequest": before_request, "afterResponse": after_response},
                    )
                ],
            }
        )
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "one"}, context)).wait(context)
    await (await root.submit({"type": "input", "content": "two"}, context)).wait(context)
    assert seen == [["user"], ["user", "assistant", "user"]]
    assert [request[0]["messages"] for request in setup.faux.requests] == [
        [{"role": "user", "content": "rewritten", "timestamp": 1}],
        [{"role": "user", "content": "rewritten", "timestamp": 1}],
    ]
    assert responses == ["stop", "stop"]
    await harness.close(context)


async def test_the_first_on_yield_continuation_hands_the_run_to_a_successor() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("first"), assistant_message("second")])
    calls: list[str] = []

    def on_yield(_message: Any, _api: Any, _context: Any) -> Any:
        calls.append("yield")
        return {"continue": "again"} if len(calls) == 1 else None

    setup.registry.install(
        define_extension(
            {
                "name": "yield",
                "hooks": [hook(GenerationTask, {"onYield": on_yield})],
            }
        )
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    settled = await submission.wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.assistant",
        "pi.user",
        "pi.assistant",
    ]
    assert text_of(entries[1]["model"][0]) == "first"
    assert entries[2]["model"][0]["content"] == "again"
    assert text_of(entries[3]["model"][0]) == "second"
    assert settled["answer"] == entries[3]["id"]
    assert await live(harness, root) == {}
    await harness.close(context)


async def test_reports_an_env_throw_and_renders_sections_without_an_environment() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("ok")])
    add_section(
        setup.registry,
        "cwd",
        lambda input, _context: "no env" if input.env is None else "has env",
    )

    def build_env(_target: Any, _context: Any) -> Any:
        raise RuntimeError("env failed")

    opened = await open_chat(MemoryStorage(), setup, env=build_env)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    assert (await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context))[
        "status"
    ] == "done"
    assert any(str(error) == "env failed" for error in setup.reports)
    system = next(entry for entry in await all_entries(root) if entry["kind"] == "pi.system")
    assert system["model"][0]["sections"] == {"cwd": "<cwd>\nno env\n</cwd>"}
    await harness.close(context)

async def test_the_model_protocol_carries_tool_calls_and_usage() -> None:
    models = FauxModels()
    tool_call = faux_tool_call("read", {"path": "/x"}, id="c1")
    models.set_responses(
        [
            assistant_message([tool_call], stop_reason="toolUse", usage=usage(input=7, output=3)),
            assistant_message([tool_call], stop_reason="toolUse", usage=usage(input=7, output=3)),
        ]
    )
    assert isinstance(models, Models)
    model = models.get_model("faux", "faux-1")
    assert model is not None
    completed = await models.complete(model, {"messages": []})
    assert completed["content"][0] == tool_call
    assert completed["usage"]["input"] == 7
    stream = models.stream(model, {"messages": []})
    async for _event in stream:
        pass
    streamed = await stream.result()
    assert streamed["content"][0]["name"] == "read"
    assert streamed["usage"]["output"] == 3

async def test_a_continuation_clears_the_partial_before_its_successor() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [
            slow_message(assistant_message("first"), chunks=4, interval=0.03),
            assistant_message("second"),
        ]
    )
    calls: list[str] = []

    def on_yield(_message: Any, _api: Any, _context: Any) -> Any:
        calls.append("yield")
        return {"continue": "again"} if len(calls) == 1 else None

    setup.registry.install(
        define_extension(
            {"name": "yield", "hooks": [hook(GenerationTask, {"onYield": on_yield})]}
        )
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == [
        "pi.user",
        "pi.assistant",
        "pi.user",
        "pi.assistant",
    ]
    assert [text_of(entry["model"][0]) for entry in entries if entry["kind"] == "pi.assistant"] == [
        "first",
        "second",
    ]
    assert all(
        entry["model"][0]["stopReason"] == "stop"
        for entry in entries
        if entry["kind"] == "pi.assistant"
    )
    await harness.close(context)

async def test_keeps_a_tool_calling_answer_durable_until_the_tool_task_lands() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [
            assistant_message(
                [faux_tool_call("read", {}, id="c1")],
                stop_reason="toolUse",
                usage=usage(input=2, output=1),
            )
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    assert settled["status"] == "unanswered"
    assert settled["reason"] == "faulted"
    assert "Tool execution arrives" in settled["detail"]
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant"]
    message = cast(dict[str, Any], entries[1]["model"][0])
    assert message["content"][0]["type"] == "toolCall"
    assert (await harness.snapshot(UsageDoc, root.id, context))["models"]["faux/faux-1"] == usage(
        input=2, output=1
    )
    assert await live(harness, root) == {}
    await harness.close(context)

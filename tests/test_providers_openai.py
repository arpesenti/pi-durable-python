"""OpenAI Chat Completions adapter coverage (optional ``openai`` extra).

The adapter translates OpenAI streaming chunks into the port's ``Models`` protocol. The cases here
use an injected fake client, so they need no network and no credentials: they pin the request the
adapter builds, the events and terminal message it streams, and what the Harness persists.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from email.utils import formatdate
from typing import Any

import pytest
from chat_support import all_entries
from harness_support import add_section
from provider_support import (
    HANG,
    FakeOpenAIClient,
    ProviderError,
    open_provider_harness,
    openai_chunk,
    openai_text,
    openai_tool_call,
    openai_usage,
    running_task,
)
from session_support import context
from task_support import eventually

from pi_durable import AssistantEntry, ProviderDoc, UsageDoc, define_extension, define_tool
from pi_durable.harness.registry import create_registry
from pi_durable.providers import Model, is_context_overflow, is_retryable_assistant_error
from pi_durable.providers.openai import OPENAI_API, OpenAIModels


def model(**overrides: Any) -> Model:
    values: dict[str, Any] = {
        "provider": "openai",
        "model_id": "gpt-test",
        "context_window": 128_000,
        "max_tokens": 4_096,
    }
    values.update(overrides)
    return Model(**values)


async def collect(stream: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events = [event async for event in stream]
    return events, await stream.result()


async def answer_message(root: Any, settled: Mapping[str, Any]) -> dict[str, Any]:
    return (await root.commit(lambda tx: tx.entry(AssistantEntry, settled["answer"]), context))["model"][0]


async def test_answers_through_the_adapter_and_records_usage() -> None:
    client = FakeOpenAIClient(
        openai_text(
            "Hello there",
            usage=openai_usage(12, 5, cached_tokens=4),
        )
    )
    models = OpenAIModels([model()], client=client)
    opened = await open_provider_harness(models, "openai", "gpt-test")
    harness, root = opened["harness"], opened["root"]
    try:
        settled = await (
            await root.submit({"type": "input", "content": "hi"}, context)
        ).wait(context)
        assert settled["status"] == "done"

        answer = await answer_message(root, settled)
        assert answer["content"] == [{"type": "text", "text": "Hello there"}]
        assert answer["stopReason"] == "stop"
        assert answer["provider"] == "openai"
        assert answer["model"] == "gpt-test"
        assert answer["api"] == OPENAI_API
        assert answer["responseId"] == "chatcmpl-1"

        usage = await harness.snapshot(UsageDoc, root.id, context)
        assert usage["models"]["openai/gpt-test"] == {
            "input": 8,
            "output": 5,
            "cacheRead": 4,
            "cacheWrite": 0,
            "reasoning": 0,
            "totalTokens": 17,
            "cost": {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0},
        }
        request = client.requests[0]
        assert request["model"] == "gpt-test"
        assert request["stream"] is True
        assert request["stream_options"] == {"include_usage": True}
        assert request["max_completion_tokens"] == 4_096
        assert request["messages"][-1]["role"] == "user"
        assert request["messages"][-1]["content"] == "hi"
    finally:
        await harness.close(context)


async def test_streams_text_and_thinking_deltas_and_forwards_request_options() -> None:
    client = FakeOpenAIClient(
        [
            openai_chunk(delta={"role": "assistant", "content": ""}),
            openai_chunk(delta={"reasoning_content": "think "}),
            openai_chunk(delta={"reasoning_content": "hard"}),
            openai_chunk(delta={"content": "answer"}),
            openai_chunk(delta={}, finish_reason="stop", usage=openai_usage(3, 2, reasoning_tokens=2)),
        ]
    )
    models = OpenAIModels([model()], client=client)
    events, message = await collect(
        models.stream_simple(
            model(),
            {"messages": [{"role": "user", "content": "hi", "timestamp": 1}]},
            {
                "reasoning": "high",
                "sessionId": "session-1",
                "timeoutMs": 2_000,
                "headers": {"x-test": "1"},
                "metadata": {"user_id": "u-1"},
            },
        )
    )
    kinds = [event["type"] for event in events]
    assert kinds[0] == "start"
    assert "thinking_delta" in kinds
    assert "text_delta" in kinds
    assert kinds[-1] == "done"
    assert message["content"] == [
        {"type": "thinking", "thinking": "think hard", "thinkingSignature": "reasoning_content"},
        {"type": "text", "text": "answer"},
    ]
    request = client.requests[0]
    assert request["reasoning_effort"] == "high"
    assert request["prompt_cache_key"] == "session-1"
    assert request["timeout"] == 2.0
    assert request["extra_headers"] == {"x-test": "1"}
    assert request["metadata"] == {"user_id": "u-1"}


async def test_translates_the_system_prompt_tools_and_tool_results() -> None:
    client = FakeOpenAIClient(openai_text("done"))
    registry = create_registry()
    add_section(registry, "preamble", lambda *_args: "You are a probe.", {"tag": False})
    registry.install(
        define_extension(
            {
                "name": "tools",
                "tools": [
                    define_tool(
                        {
                            "name": "probe",
                            "description": "Probe things",
                            "parameters": {
                                "type": "object",
                                "properties": {"text": {"type": "string"}},
                            },
                            "execute": lambda *_args: {"output": [{"type": "text", "text": "ok"}]},
                        }
                    )
                ],
            }
        )
    )
    models = OpenAIModels([model(input=("text",))], client=client)
    opened = await open_provider_harness(models, "openai", "gpt-test", registry=registry)
    harness, root = opened["harness"], opened["root"]
    try:
        await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
        request = client.requests[0]
        assert request["messages"][0]["role"] == "developer"
        assert "You are a probe." in request["messages"][0]["content"]
        assert request["tools"] == [
            {
                "type": "function",
                "function": {
                    "name": "probe",
                    "description": "Probe things",
                    "parameters": {
                        "type": "object",
                        "properties": {"text": {"type": "string"}},
                    },
                },
            }
        ]
    finally:
        await harness.close(context)


async def test_assembles_streamed_tool_call_arguments_into_a_valid_call() -> None:
    registry = create_registry()
    received: list[dict[str, Any]] = []

    async def probe(arguments: Mapping[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        del api, call_context
        received.append(dict(arguments))
        return {"output": [{"type": "text", "text": "probed"}]}

    registry.install(
        define_extension(
            {
                "name": "tools",
                "tools": [
                    define_tool(
                        {
                            "name": "probe",
                            "description": "Probe things",
                            "parameters": {
                                "type": "object",
                                "properties": {"text": {"type": "string"}},
                            },
                            "execute": probe,
                        }
                    )
                ],
            }
        )
    )
    client = FakeOpenAIClient(
        openai_tool_call(arguments='{"text": "hello"}', split=[9, 8]),
        openai_text("done", id="chatcmpl-2"),
    )
    models = OpenAIModels([model()], client=client)
    opened = await open_provider_harness(models, "openai", "gpt-test", registry=registry)
    harness, root = opened["harness"], opened["root"]
    try:
        settled = await (
            await root.submit({"type": "input", "content": "hi"}, context)
        ).wait(context)
        assert settled["status"] == "done"
        assert received == [{"text": "hello"}]
        entries = await all_entries(root)
        assistant = next(entry for entry in entries if entry["kind"] == "pi.assistant")
        assert assistant["model"][0]["content"] == [
            {
                "type": "toolCall",
                "id": "call_1",
                "name": "probe",
                "arguments": {"text": "hello"},
            }
        ]
        result = next(entry for entry in entries if entry["kind"] == "pi.tool-result")
        assert result["model"][0]["toolCallId"] == "call_1"
        assert (await harness.snapshot(UsageDoc, root.id, context))["tools"] == {}
    finally:
        await harness.close(context)


async def test_reports_malformed_tool_arguments_to_the_model() -> None:
    client = FakeOpenAIClient(openai_tool_call(arguments='{"text": "unterminated}', split=[10, 13]))
    models = OpenAIModels([model()], client=client)
    events, message = await collect(
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})
    )
    assert [event["type"] for event in events].count("toolcall_delta") == 2
    call = message["content"][0]
    assert call["type"] == "toolCall"
    assert call["arguments"] == {}
    assert "Received arguments" in call["argumentsError"]
    assert '"unterminated}' in call["argumentsError"]


async def test_classifies_provider_failures_with_the_shared_classifiers() -> None:
    client = FakeOpenAIClient(ProviderError("Error code: 429 - rate limit exceeded", status_code=429))
    models = OpenAIModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})
    )
    assert message["stopReason"] == "error"
    assert "429" in message["errorMessage"]
    assert is_retryable_assistant_error(message) is True

    client = FakeOpenAIClient(ProviderError("insufficient_quota: out of budget", status_code=429))
    models = OpenAIModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})
    )
    assert is_retryable_assistant_error(message) is False

    client = FakeOpenAIClient(
        [
            openai_chunk(delta={"content": ""}, finish_reason="content_filter"),
        ]
    )
    models = OpenAIModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})
    )
    assert message["stopReason"] == "error"
    assert message["rawStopReason"] == "content_filter"
    assert message["errorMessage"] == "Provider finish_reason: content_filter"


async def test_feeds_provider_errors_into_the_durable_retry_loop() -> None:
    client = FakeOpenAIClient(
        ProviderError("Error code: 503 - service unavailable", status_code=503),
        openai_text("recovered"),
    )
    models = OpenAIModels([model()], client=client)
    opened = await open_provider_harness(
        models,
        "openai",
        "gpt-test",
        settings={"retry": {"enabled": True, "max_retries": 3, "base_delay_ms": 1}},
    )
    harness, root = opened["harness"], opened["root"]
    try:
        settled = await (
            await root.submit({"type": "input", "content": "hi"}, context)
        ).wait(context)
        assert settled["status"] == "done"
        entries = await all_entries(root)
        assistant = [entry["model"][0] for entry in entries if entry["kind"] == "pi.assistant"]
        assert [message["stopReason"] for message in assistant] == ["error", "stop"]
        assert "503" in str(assistant[0]["errorMessage"])
        assert len(client.requests) == 2
    finally:
        await harness.close(context)


async def test_classifies_a_context_overflow_error() -> None:
    client = FakeOpenAIClient(
        [
            openai_chunk(
                delta={},
                finish_reason="stop",
                usage=openai_usage(200_000, 0),
            ),
        ]
    )
    models = OpenAIModels([model(context_window=128_000)], client=client)
    _events, message = await collect(
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})
    )
    assert message["usage"]["input"] == 200_000
    assert is_context_overflow(message, 128_000) is True


async def test_retries_a_provider_request_inside_one_attempt_when_asked() -> None:
    client = FakeOpenAIClient(
        ProviderError("Error code: 500 - server error", status_code=500, headers={"retry-after-ms": "0"}),
        openai_text("recovered"),
    )
    models = OpenAIModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(
            model(), {"messages": [{"role": "user", "content": "hi"}]}, {"maxRetries": 1}
        )
    )
    assert message["stopReason"] == "stop"
    assert len(client.requests) == 2


async def test_forwards_the_conversation_session_id_as_the_prompt_cache_key() -> None:
    client = FakeOpenAIClient(openai_text("one"), openai_text("two", id="chatcmpl-2"))
    models = OpenAIModels([model()], client=client)
    opened = await open_provider_harness(models, "openai", "gpt-test")
    harness, root = opened["harness"], opened["root"]
    try:
        for turn in range(2):
            settled = await (
                await root.submit({"type": "input", "content": f"turn {turn}"}, context)
            ).wait(context)
            assert settled["status"] == "done"
        session_id = (await harness.snapshot(ProviderDoc, root.id, context))["sessionId"]
        assert [request["prompt_cache_key"] for request in client.requests] == [
            session_id,
            session_id,
        ]
    finally:
        await harness.close(context)


async def test_aborts_a_hanging_request_through_the_invocation_signal() -> None:
    client = FakeOpenAIClient(HANG)
    models = OpenAIModels([model()], client=client)
    opened = await open_provider_harness(models, "openai", "gpt-test")
    harness, root = opened["harness"], opened["root"]
    try:
        submission = await root.submit({"type": "input", "content": "hi"}, context)
        task_id = await running_task(root)
        await eventually(lambda: bool(client.requests))
        assert await harness.abort_task(task_id, context) == "marked"
        settled = await submission.wait(context)
        assert settled["status"] == "unanswered"
        assert settled["reason"] == "aborted"
    finally:
        await harness.close(context)


async def test_parses_an_http_date_retry_after_header() -> None:
    retry_at = formatdate(time.time() + 30, usegmt=True)
    client = FakeOpenAIClient(
        ProviderError(
            "Error code: 429 - rate limit exceeded",
            status_code=429,
            headers={"retry-after": retry_at},
        ),
        openai_text("never"),
    )
    models = OpenAIModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(
            model(),
            {"messages": [{"role": "user", "content": "hi"}]},
            {"maxRetries": 2, "maxRetryDelayMs": 1_000},
        )
    )
    assert message["stopReason"] == "error"
    assert "Server requested" in str(message["errorMessage"])
    assert len(client.requests) == 1


async def test_keeps_calls_apart_when_a_chunk_omits_the_tool_call_index() -> None:
    client = FakeOpenAIClient(
        [
            openai_chunk(
                delta={
                    "tool_calls": [
                        {"index": 0, "id": "call_a", "function": {"name": "probe", "arguments": "{}"}},
                        {"index": 2, "id": "call_b", "function": {"name": "probe", "arguments": "{}"}},
                    ]
                }
            ),
            openai_chunk(
                delta={
                    "tool_calls": [
                        {"id": "call_c", "function": {"name": "probe", "arguments": '{"x": 1}'}}
                    ]
                }
            ),
            openai_chunk(delta={}, finish_reason="tool_calls"),
        ]
    )
    models = OpenAIModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})
    )
    calls = [block for block in message["content"] if block["type"] == "toolCall"]
    assert [call["id"] for call in calls] == ["call_a", "call_b", "call_c"]
    assert calls[2]["arguments"] == {"x": 1}


async def test_requires_an_api_key_only_when_no_client_is_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    models = OpenAIModels([model()])
    with pytest.raises(ValueError, match="No API key for provider: openai"):
        models.stream_simple(model(), {"messages": [{"role": "user", "content": "hi"}]})


async def test_returns_no_model_for_unknown_pairs() -> None:
    models = OpenAIModels([model()], client=FakeOpenAIClient(openai_text("x")))
    assert models.get_model("openai", "gpt-test") is not None
    assert models.get_model("openai", "missing") is None
    assert models.get_model("anthropic", "gpt-test") is None


async def test_completes_without_streaming_partials() -> None:
    client = FakeOpenAIClient(openai_text("summary"))
    models = OpenAIModels([model()], client=client)
    message = await models.complete_simple(
        model(), {"messages": [{"role": "user", "content": "hi"}]}
    )
    assert message["content"] == [{"type": "text", "text": "summary"}]

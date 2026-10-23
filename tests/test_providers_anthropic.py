"""Anthropic Messages adapter coverage (optional ``anthropic`` extra).

Like the OpenAI suite, every case injects a fake ``AsyncAnthropic``-shaped client: the cases pin the
request the adapter builds, the events it streams, the terminal message, and what the Harness
persists. No credentials or network access are involved.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from chat_support import all_entries
from harness_support import add_section
from provider_support import (
    HANG,
    FakeAnthropicClient,
    ProviderError,
    anthropic_event,
    anthropic_finish,
    anthropic_start,
    anthropic_text,
    anthropic_text_block,
    anthropic_thinking_block,
    anthropic_tool_block,
    anthropic_usage,
    open_provider_harness,
    running_task,
)
from session_support import context
from task_support import eventually

from pi_durable import AssistantEntry, UsageDoc, define_extension, define_tool
from pi_durable.harness.registry import create_registry
from pi_durable.providers import Model, is_context_overflow, is_retryable_assistant_error
from pi_durable.providers.anthropic import ANTHROPIC_API, AnthropicModels


def model(**overrides: Any) -> Model:
    values: dict[str, Any] = {
        "provider": "anthropic",
        "model_id": "claude-test",
        "context_window": 200_000,
        "max_tokens": 8_192,
        "cost": {"input": 3.0, "output": 15.0, "cacheRead": 0.3, "cacheWrite": 3.75},
    }
    values.update(overrides)
    return Model(**values)


async def collect(stream: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events = [event async for event in stream]
    return events, await stream.result()


async def answer_message(root: Any, settled: Mapping[str, Any]) -> dict[str, Any]:
    return (await root.commit(lambda tx: tx.entry(AssistantEntry, settled["answer"]), context))["model"][0]


def simple_context() -> dict[str, Any]:
    return {"messages": [{"role": "user", "content": "hi", "timestamp": 1}]}


async def test_answers_through_the_adapter_and_records_usage() -> None:
    client = FakeAnthropicClient(
        anthropic_text(
            "Hello there",
            usage=anthropic_usage(10, 5, cache_read=6, cache_write=4),
        )
    )
    models = AnthropicModels([model()], client=client)
    opened = await open_provider_harness(models, "anthropic", "claude-test")
    harness, root = opened["harness"], opened["root"]
    try:
        settled = await (
            await root.submit({"type": "input", "content": "hi"}, context)
        ).wait(context)
        assert settled["status"] == "done"

        answer = await answer_message(root, settled)
        assert answer["content"] == [{"type": "text", "text": "Hello there"}]
        assert answer["stopReason"] == "stop"
        assert answer["provider"] == "anthropic"
        assert answer["model"] == "claude-test"
        assert answer["api"] == ANTHROPIC_API
        assert answer["responseId"] == "msg_1"

        usage = await harness.snapshot(UsageDoc, root.id, context)
        # input 10, output 5, cache read 6, cache write 4, priced at the model's rates.
        record = usage["models"]["anthropic/claude-test"]
        counters = ("input", "output", "cacheRead", "cacheWrite", "totalTokens")
        assert {key: record[key] for key in counters} == {
            "input": 10,
            "output": 5,
            "cacheRead": 6,
            "cacheWrite": 4,
            "totalTokens": 25,
        }
        assert record["cost"]["input"] == pytest.approx(30.0 / 1_000_000)
        assert record["cost"]["output"] == pytest.approx(75.0 / 1_000_000)
        assert record["cost"]["cacheRead"] == pytest.approx(1.8 / 1_000_000)
        assert record["cost"]["cacheWrite"] == pytest.approx(15.0 / 1_000_000)
        assert record["cost"]["total"] == pytest.approx(121.8 / 1_000_000)
        request = client.requests[0]
        assert request["model"] == "claude-test"
        assert request["stream"] is True
        assert request["max_tokens"] == 8_192
        assert "system" not in request
        assert request["messages"] == [{"role": "user", "content": "hi"}]
    finally:
        await harness.close(context)


async def test_translates_the_system_prompt_tools_thinking_and_messages() -> None:
    client = FakeAnthropicClient(anthropic_text("done"))
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
                            "execute": lambda *_args: {"output": []},
                        }
                    )
                ],
            }
        )
    )
    models = AnthropicModels([model()], client=client)
    opened = await open_provider_harness(models, "anthropic", "claude-test", registry=registry)
    harness, root = opened["harness"], opened["root"]
    try:
        await root.configure({"thinkingLevel": "low"}, context)
        await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
        request = client.requests[0]
        assert "You are a probe." in request["system"][0]["text"]
        assert request["system"][0]["cache_control"] == {"type": "ephemeral"}
        assert request["tools"] == [
            {
                "name": "probe",
                "description": "Probe things",
                "input_schema": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                },
                "cache_control": {"type": "ephemeral"},
            }
        ]
        assert request["thinking"] == {"type": "enabled", "budget_tokens": 2_048}
        assert request["max_tokens"] == 8_192
        assert "temperature" not in request
    finally:
        await harness.close(context)


async def test_streams_thinking_with_its_signature_and_text() -> None:
    client = FakeAnthropicClient(
        [
            anthropic_start(),
            *anthropic_thinking_block(0, "consider this", "sig-1"),
            *anthropic_text_block(1, "answer"),
            *anthropic_finish("end_turn", usage=anthropic_usage(10, 5, thinking_tokens=2)),
        ]
    )
    models = AnthropicModels([model()], client=client)
    events, message = await collect(models.stream_simple(model(), simple_context()))
    kinds = [event["type"] for event in events]
    assert kinds[0] == "start"
    assert "thinking_delta" in kinds
    assert "text_delta" in kinds
    assert kinds[-1] == "done"
    assert message["content"] == [
        {"type": "thinking", "thinking": "consider this", "thinkingSignature": "sig-1"},
        {"type": "text", "text": "answer"},
    ]
    assert message["usage"]["reasoning"] == 2


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
    client = FakeAnthropicClient(
        [
            anthropic_start(),
            *anthropic_tool_block(0, arguments='{"text": "hello"}', split=[9, 8]),
            *anthropic_finish("tool_use", usage=anthropic_usage(10, 4)),
        ],
        anthropic_text("done", id="msg_2"),
    )
    models = AnthropicModels([model()], client=client)
    opened = await open_provider_harness(models, "anthropic", "claude-test", registry=registry)
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
                "id": "toolu_1",
                "name": "probe",
                "arguments": {"text": "hello"},
            }
        ]
        second = client.requests[1]["messages"]
        assert second[-2] == {
            "role": "assistant",
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_1",
                    "name": "probe",
                    "input": {"text": "hello"},
                }
            ],
        }
        assert second[-1] == {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": "toolu_1",
                    "content": [{"type": "text", "text": "probed"}],
                    "is_error": False,
                }
            ],
        }
    finally:
        await harness.close(context)


async def test_normalizes_foreign_tool_ids_and_drops_foreign_thinking() -> None:
    client = FakeAnthropicClient(anthropic_text("done"))
    models = AnthropicModels([model()], client=client)
    messages = [
        {"role": "system", "content": "", "sections": {"preamble": "p"}, "timestamp": 1},
        {"role": "user", "content": "go", "timestamp": 2},
        {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "foreign", "thinkingSignature": "sig"},
                {
                    "type": "toolCall",
                    "id": "call|with|pipes",
                    "name": "probe",
                    "arguments": {"text": "x"},
                },
            ],
            "api": "openai-completions",
            "provider": "openai",
            "model": "gpt-test",
            "stopReason": "toolUse",
            "usage": {},
            "timestamp": 3,
        },
        {
            "role": "toolResult",
            "toolCallId": "call|with|pipes",
            "toolName": "probe",
            "content": [{"type": "text", "text": "ok"}],
            "isError": False,
            "timestamp": 4,
        },
    ]
    _events, message = await collect(models.stream_simple(model(), {"messages": messages}))
    assert message["stopReason"] == "stop"
    request_messages = client.requests[0]["messages"]
    assert request_messages[1] == {
        "role": "assistant",
        "content": [
            {
                "type": "tool_use",
                "id": "call_with_pipes",
                "name": "probe",
                "input": {"text": "x"},
            }
        ],
    }
    assert request_messages[2]["content"][0]["tool_use_id"] == "call_with_pipes"


async def test_replays_signed_thinking_from_the_same_model() -> None:
    client = FakeAnthropicClient(anthropic_text("done"))
    models = AnthropicModels([model()], client=client)
    messages = [
        {"role": "user", "content": "go", "timestamp": 1},
        {
            "role": "assistant",
            "content": [
                {"type": "thinking", "thinking": "mine", "thinkingSignature": "sig"},
                {"type": "text", "text": "a"},
            ],
            "api": ANTHROPIC_API,
            "provider": "anthropic",
            "model": "claude-test",
            "stopReason": "stop",
            "usage": {},
            "timestamp": 2,
        },
    ]
    await collect(models.stream_simple(model(), {"messages": messages}))
    assert client.requests[0]["messages"][1]["content"][0] == {
        "type": "thinking",
        "thinking": "mine",
        "signature": "sig",
    }


async def test_maps_stop_reasons_and_failures() -> None:
    client = FakeAnthropicClient(
        [anthropic_start(), *anthropic_text_block(0, "truncated"), *anthropic_finish("max_tokens")]
    )
    models = AnthropicModels([model()], client=client)
    _events, message = await collect(models.stream_simple(model(), simple_context()))
    assert message["stopReason"] == "length"
    assert message["rawStopReason"] == "max_tokens"

    client = FakeAnthropicClient(
        [
            anthropic_start(),
            *anthropic_text_block(0, "no"),
            anthropic_event(
                "message_delta",
                delta={
                    "stop_reason": "refusal",
                    "stop_details": {"explanation": "cannot help with that"},
                },
                usage=anthropic_usage(10, 1),
            ),
            anthropic_event("message_stop"),
        ]
    )
    models = AnthropicModels([model()], client=client)
    _events, message = await collect(models.stream_simple(model(), simple_context()))
    assert message["stopReason"] == "error"
    assert message["errorMessage"] == "cannot help with that"


async def test_classifies_provider_failures_with_the_shared_classifiers() -> None:
    client = FakeAnthropicClient(ProviderError("Error code: 529 - overloaded_error", status_code=529))
    models = AnthropicModels([model()], client=client)
    _events, message = await collect(models.stream_simple(model(), simple_context()))
    assert message["stopReason"] == "error"
    assert is_retryable_assistant_error(message) is True

    client = FakeAnthropicClient(
        ProviderError("Your credit balance is too low: available balance", status_code=400)
    )
    models = AnthropicModels([model()], client=client)
    _events, message = await collect(models.stream_simple(model(), simple_context()))
    assert is_retryable_assistant_error(message) is False

    client = FakeAnthropicClient(
        ProviderError("prompt is too long: 210000 tokens > 200000 maximum", status_code=400)
    )
    models = AnthropicModels([model()], client=client)
    _events, message = await collect(models.stream_simple(model(), simple_context()))
    assert is_context_overflow(message) is True


async def test_streams_an_error_event_from_the_provider() -> None:
    client = FakeAnthropicClient(
        [
            anthropic_start(),
            anthropic_event(
                "error",
                error={"type": "overloaded_error", "message": "Overloaded"},
            ),
        ]
    )
    models = AnthropicModels([model()], client=client)
    events, message = await collect(models.stream_simple(model(), simple_context()))
    assert events[-1]["type"] == "error"
    assert message["stopReason"] == "error"
    assert message["errorMessage"] == "Overloaded"
    assert is_retryable_assistant_error(message) is True


async def test_honors_cache_retention_and_metadata() -> None:
    with_system = {
        "messages": [
            {"role": "system", "content": "", "sections": {"preamble": "p"}, "timestamp": 1},
            {"role": "user", "content": "hi", "timestamp": 2},
        ]
    }
    client = FakeAnthropicClient(anthropic_text("x"))
    models = AnthropicModels([model()], client=client)
    await collect(
        models.stream_simple(
            model(), with_system, {"cacheRetention": "long", "metadata": {"user_id": "u-1"}}
        )
    )
    request = client.requests[0]
    assert request["system"][0]["cache_control"] == {"type": "ephemeral", "ttl": "1h"}
    assert request["metadata"] == {"user_id": "u-1"}

    client = FakeAnthropicClient(anthropic_text("x"))
    models = AnthropicModels([model()], client=client)
    await collect(models.stream_simple(model(), with_system, {"cacheRetention": "none"}))
    assert "cache_control" not in client.requests[0]["system"][0]


async def test_retries_a_provider_request_inside_one_attempt_when_asked() -> None:
    client = FakeAnthropicClient(
        ProviderError("Error code: 500 - internal error", status_code=500, headers={"retry-after-ms": "0"}),
        anthropic_text("recovered"),
    )
    models = AnthropicModels([model()], client=client)
    _events, message = await collect(
        models.stream_simple(model(), simple_context(), {"maxRetries": 1})
    )
    assert message["stopReason"] == "stop"
    assert len(client.requests) == 2


async def test_aborts_a_hanging_request_through_the_invocation_signal() -> None:
    client = FakeAnthropicClient(HANG)
    models = AnthropicModels([model()], client=client)
    opened = await open_provider_harness(models, "anthropic", "claude-test")
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


async def test_requires_an_api_key_only_when_no_client_is_injected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    models = AnthropicModels([model()])
    with pytest.raises(ValueError, match="No API key for provider: anthropic"):
        models.stream_simple(model(), simple_context())


async def test_completes_and_reports_unknown_models() -> None:
    client = FakeAnthropicClient(anthropic_text("summary"))
    models = AnthropicModels([model()], client=client)
    assert models.get_model("anthropic", "claude-test") is not None
    assert models.get_model("anthropic", "missing") is None
    message = await models.complete_simple(model(), simple_context())
    assert message["content"] == [{"type": "text", "text": "summary"}]

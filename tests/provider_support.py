"""Shared fakes and builders for the OpenAI and Anthropic adapter suites.

The adapter tests never touch the network. Each adapter takes an injected client object with the
shape of the official SDK client it wraps, and these fakes record the request and replay one
scripted provider stream, expressed in the provider's own chunk or event format.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterable, Mapping, Sequence
from types import SimpleNamespace
from typing import Any

from session_support import context

from pi_durable import Harness, MemoryStorage
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import Registry

# ── Attribute-shaped provider objects ──────────────────────────────────────


def to_object(value: Any) -> Any:
    """Turn nested dicts into attribute objects, the way the SDK models expose them."""

    if isinstance(value, Mapping):
        return SimpleNamespace(**{key: to_object(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return [to_object(item) for item in value]
    return value


# ── Scripted provider streams ──────────────────────────────────────────────


class FakeAsyncStream:
    """An async iterable over scripted provider chunks."""

    def __init__(self, chunks: Iterable[Any]) -> None:
        self._chunks = list(chunks)

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        for chunk in self._chunks:
            yield chunk


class HangingStream:
    """A provider stream that never produces a chunk or a result."""

    def __aiter__(self) -> AsyncIterator[Any]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[Any]:
        await asyncio.Event().wait()
        yield  # pragma: no cover


#: A scripted response that keeps the request open until its invocation is cancelled.
HANG = HangingStream()


class ProviderError(Exception):
    """A stand-in for an SDK status error carrying the fields the adapters read."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.status = status_code
        self.response = SimpleNamespace(headers=dict(headers or {}))


class FakeOpenAIClient:
    """An ``AsyncOpenAI``-shaped client that replays scripted chat-completion chunks."""

    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        create = _make_create(self)
        completions = SimpleNamespace(create=create)
        self.chat = SimpleNamespace(completions=completions)

    def next_response(self) -> Any:
        response = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if callable(response) and not isinstance(response, (list, tuple)):
            response = response(self.requests[-1])
        return response


class FakeAnthropicClient:
    """An ``AsyncAnthropic``-shaped client that replays scripted message events."""

    def __init__(self, *responses: Any) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        messages = SimpleNamespace(create=_make_create(self))
        self.messages = messages
        self.beta = SimpleNamespace(messages=messages)

    def next_response(self) -> Any:
        response = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if callable(response) and not isinstance(response, (list, tuple)):
            response = response(self.requests[-1])
        return response


def _make_create(client: Any) -> Callable[..., Any]:
    async def create(**params: Any) -> Any:
        client.requests.append(params)
        response = client.next_response()
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, HangingStream):
            return response
        return FakeAsyncStream(to_object(chunk) for chunk in response)

    return create


# ── OpenAI chunk builders ──────────────────────────────────────────────────


def openai_chunk(
    *,
    delta: Mapping[str, Any] | None = None,
    finish_reason: str | None = None,
    usage: Mapping[str, Any] | None = None,
    id: str = "chatcmpl-1",
    model: str = "gpt-test",
) -> dict[str, Any]:
    choices = []
    if delta is not None or finish_reason is not None:
        choices.append({"index": 0, "delta": dict(delta or {}), "finish_reason": finish_reason})
    return {
        "id": id,
        "object": "chat.completion.chunk",
        "model": model,
        "choices": choices,
        "usage": dict(usage) if usage is not None else None,
    }


def openai_usage(
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    *,
    cached_tokens: int | None = None,
    cache_write_tokens: int | None = None,
    reasoning_tokens: int | None = None,
) -> dict[str, Any]:
    details: dict[str, Any] = {}
    if cached_tokens is not None:
        details["cached_tokens"] = cached_tokens
    if cache_write_tokens is not None:
        details["cache_write_tokens"] = cache_write_tokens
    completion_details = (
        {"reasoning_tokens": reasoning_tokens} if reasoning_tokens is not None else {}
    )
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "prompt_tokens_details": details,
        "completion_tokens_details": completion_details,
        "total_tokens": prompt_tokens + completion_tokens,
    }


def openai_text(
    text: str,
    *,
    stop: str = "stop",
    usage: Mapping[str, Any] | None = None,
    id: str = "chatcmpl-1",
    model: str = "gpt-test",
) -> list[dict[str, Any]]:
    """A text answer streamed in two deltas with a terminal usage chunk."""

    return [
        openai_chunk(delta={"role": "assistant", "content": ""}, id=id, model=model),
        openai_chunk(delta={"content": text[: len(text) // 2]}, id=id, model=model),
        openai_chunk(delta={"content": text[len(text) // 2 :]}, id=id, model=model),
        openai_chunk(
            delta={},
            finish_reason=stop,
            usage=usage if usage is not None else openai_usage(10, 4),
            id=id,
            model=model,
        ),
    ]


def openai_tool_call(
    *,
    id: str = "call_1",
    name: str = "probe",
    arguments: str = "{}",
    split: Sequence[int] | None = None,
    usage: Mapping[str, Any] | None = None,
    model: str = "gpt-test",
) -> list[dict[str, Any]]:
    """A tool-call stream whose argument text arrives in the listed slices."""

    pieces = split or [len(arguments)]
    chunks: list[dict[str, Any]] = [
        openai_chunk(
            delta={
                "role": "assistant",
                "tool_calls": [
                    {"index": 0, "id": id, "type": "function", "function": {"name": name, "arguments": ""}}
                ],
            },
            model=model,
        )
    ]
    offset = 0
    for size in pieces:
        chunks.append(
            openai_chunk(
                delta={
                    "tool_calls": [
                        {"index": 0, "function": {"arguments": arguments[offset : offset + size]}}
                    ]
                },
                model=model,
            )
        )
        offset += size
    chunks.append(
        openai_chunk(
            delta={},
            finish_reason="tool_calls",
            usage=usage if usage is not None else openai_usage(10, 4),
            model=model,
        )
    )
    return chunks


# ── Anthropic event builders ───────────────────────────────────────────────


def anthropic_event(kind: str, **fields: Any) -> dict[str, Any]:
    return {"type": kind, **fields}


def anthropic_usage(
    input_tokens: int = 0,
    output_tokens: int = 0,
    *,
    cache_read: int = 0,
    cache_write: int = 0,
    cache_write_1h: int = 0,
    thinking_tokens: int | None = None,
) -> dict[str, Any]:
    usage: dict[str, Any] = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_input_tokens": cache_read,
        "cache_creation_input_tokens": cache_write,
    }
    if cache_write_1h:
        usage["cache_creation"] = {"ephemeral_1h_input_tokens": cache_write_1h}
    if thinking_tokens is not None:
        usage["output_tokens_details"] = {"thinking_tokens": thinking_tokens}
    return usage


def anthropic_start(
    *,
    id: str = "msg_1",
    model: str = "claude-test",
    usage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return anthropic_event(
        "message_start",
        message={"id": id, "model": model, "usage": usage or anthropic_usage(10, 0)},
    )


def anthropic_text_block(index: int, text: str) -> list[dict[str, Any]]:
    return [
        anthropic_event(
            "content_block_start", index=index, content_block={"type": "text", "text": ""}
        ),
        anthropic_event(
            "content_block_delta",
            index=index,
            delta={"type": "text_delta", "text": text},
        ),
        anthropic_event("content_block_stop", index=index),
    ]


def anthropic_thinking_block(index: int, thinking: str, signature: str) -> list[dict[str, Any]]:
    return [
        anthropic_event(
            "content_block_start",
            index=index,
            content_block={"type": "thinking", "thinking": "", "signature": ""},
        ),
        anthropic_event(
            "content_block_delta",
            index=index,
            delta={"type": "thinking_delta", "thinking": thinking},
        ),
        anthropic_event(
            "content_block_delta",
            index=index,
            delta={"type": "signature_delta", "signature": signature},
        ),
        anthropic_event("content_block_stop", index=index),
    ]


def anthropic_tool_block(
    index: int,
    *,
    id: str = "toolu_1",
    name: str = "probe",
    arguments: str = "{}",
    split: Sequence[int] | None = None,
) -> list[dict[str, Any]]:
    pieces = split or [len(arguments)]
    events = [
        anthropic_event(
            "content_block_start",
            index=index,
            content_block={"type": "tool_use", "id": id, "name": name, "input": {}},
        )
    ]
    offset = 0
    for size in pieces:
        events.append(
            anthropic_event(
                "content_block_delta",
                index=index,
                delta={"type": "input_json_delta", "partial_json": arguments[offset : offset + size]},
            )
        )
        offset += size
    events.append(anthropic_event("content_block_stop", index=index))
    return events


def anthropic_finish(
    stop_reason: str = "end_turn",
    *,
    usage: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return [
        anthropic_event(
            "message_delta",
            delta={"stop_reason": stop_reason},
            usage=usage or anthropic_usage(10, 5),
        ),
        anthropic_event("message_stop"),
    ]


def anthropic_text(
    text: str,
    *,
    id: str = "msg_1",
    model: str = "claude-test",
    stop_reason: str = "end_turn",
    usage: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return [
        anthropic_start(id=id, model=model, usage=anthropic_usage(10, 0)),
        *anthropic_text_block(0, text),
        *anthropic_finish(stop_reason, usage=usage or anthropic_usage(10, 5)),
    ]


# ── Harness helpers ────────────────────────────────────────────────────────


async def running_task(conversation: Any) -> Any:
    """The conversation's newest task, which is the running generation once a request starts."""

    tasks = await conversation.commit(
        lambda tx: tx.scan_tasks({"conversationId": conversation.id}, 10), context
    )
    return tasks["items"][0]["id"]


async def open_provider_harness(
    models: Any,
    provider: str,
    model_id: str,
    *,
    registry: Registry | None = None,
    settings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Open a Harness whose root agent resolves ``provider/model_id`` through ``models``."""

    harness = await Harness.open(
        MemoryStorage(),
        {
            "models": models,
            "registry": registry or create_registry(),
            "settings": dict(settings or {}),
            "now": lambda: int(time.time() * 1000),
        },
        context,
    )
    root = await harness.root(
        context, {"agent": {"model": {"provider": provider, "modelId": model_id}}}
    )
    harness.resume()
    return {"harness": harness, "root": root}


__all__ = [
    "FakeAnthropicClient",
    "HANG",
    "FakeAsyncStream",
    "FakeOpenAIClient",
    "ProviderError",
    "anthropic_event",
    "anthropic_finish",
    "anthropic_start",
    "anthropic_text",
    "anthropic_text_block",
    "anthropic_thinking_block",
    "anthropic_tool_block",
    "anthropic_usage",
    "openai_chunk",
    "openai_text",
    "openai_tool_call",
    "openai_usage",
    "open_provider_harness",
    "running_task",
    "to_object",
]

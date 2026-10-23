"""OpenAI Chat Completions model adapter (optional ``openai`` extra).

The adapter implements the port's :class:`~pi_durable.providers.Models` protocol over the official
``AsyncOpenAI`` client. It builds the Chat Completions request from a pi-ai shaped transcript,
translates streamed chunks into the port's assistant-message events (text, thinking, and tool-call
deltas), and returns the terminal message generation and compaction classify.

The model catalog belongs to the caller: pass a sequence of :class:`~pi_durable.providers.Model`
values and the adapter resolves ``(provider, model_id)`` through it, so context windows, max-token
ceilings, input capabilities, and cost rates travel with each model.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping, Sequence
from typing import Any, cast

from . import Model
from .base import ProviderModels
from .json_parse import parse_streaming_json, parse_tool_call_arguments
from .request import optional_int, request_options
from .retry import retry_provider_request
from .stream import ProviderStream, content_index, now_ms, provider_field
from .transcript import (
    collapse_system_messages,
    has_tool_history,
    system_message_text,
    transform_messages,
)
from .usage import calculate_cost, zero_usage

OPENAI_API = "openai-completions"
DEFAULT_BASE_URL = "https://api.openai.com/v1"
PROMPT_CACHE_KEY_MAX_LENGTH = 64

_REASONING_FIELDS = ("reasoning_content", "reasoning", "reasoning_text")
_REASONING_EFFORT = {
    "minimal": "minimal",
    "low": "low",
    "medium": "medium",
    "high": "high",
    "xhigh": "high",
    "max": "high",
}
_STOP_REASONS = {
    "stop": "stop",
    "end": "stop",
    "length": "length",
    "function_call": "toolUse",
    "tool_calls": "toolUse",
}


class OpenAIModels(ProviderModels):
    """The ``Models`` protocol over OpenAI Chat Completions.

    ``client`` injects an ``AsyncOpenAI``-shaped client (tests use a fake); otherwise a client is
    built lazily from ``api_key``, ``base_url``, and ``headers``. Without an injected client an API
    key is required in the same call, ``OPENAI_API_KEY``, or ``options["apiKey"]``.
    """

    api = OPENAI_API

    def __init__(
        self,
        models: Sequence[Model] = (),
        *,
        client: Any | None = None,
        api_key: str | None = None,
        base_url: str | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(models)
        self._client = client
        self._api_key = api_key
        self._base_url = base_url
        self._headers = dict(headers or {})

    # ── Models protocol ───────────────────────────────────────────────────

    def stream_simple(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> ProviderStream:
        return self._stream(model, context, options or {}, simple=True)

    def stream(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> ProviderStream:
        return self._stream(model, context, options or {}, simple=False)

    # ── Request assembly ──────────────────────────────────────────────────

    def _stream(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any],
        *,
        simple: bool,
    ) -> ProviderStream:
        client = self._resolve_client(model, options)
        output = _output_message(model)
        return _OpenAIStream(client, model, context, options, output, simple=simple)

    def _resolve_client(self, model: Model, options: Mapping[str, Any]) -> Any:
        if self._client is not None:
            return self._client
        api_key = (
            self._api_key
            or cast(str | None, options.get("apiKey"))
            or os.environ.get("OPENAI_API_KEY")
        )
        if not api_key:
            raise ValueError(f"No API key for provider: {model.provider}")
        try:
            from openai import AsyncOpenAI
        except ImportError as error:  # pragma: no cover - covered by the extras test
            raise ImportError(
                "The OpenAI adapter requires the 'openai' extra: "
                "pip install pi-durable-python[openai]"
            ) from error
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=self._base_url,
            default_headers=dict(self._headers) or None,
            max_retries=0,
        )
        return self._client


def _output_message(model: Model) -> dict[str, Any]:
    return {
        "role": "assistant",
        "content": [],
        "api": OPENAI_API,
        "provider": model.provider,
        "model": model.model_id,
        "usage": zero_usage(),
        "stopReason": "pending",
        "timestamp": now_ms(),
    }


class _OpenAIStream(ProviderStream):
    """One Chat Completions request translated into assistant-message events."""

    def __init__(
        self,
        client: Any,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any],
        output: dict[str, Any],
        *,
        simple: bool,
    ) -> None:
        self._client = client
        self._model = model
        self._context = context
        self._options = options
        self._simple = simple
        self._text_block: dict[str, Any] | None = None
        self._thinking_block: dict[str, Any] | None = None
        self._tool_blocks: dict[int, dict[str, Any]] = {}
        self._tool_text: dict[int, str] = {}
        self._tool_block_index: dict[int, int] = {}
        self._has_finish_reason = False
        super().__init__(output, signal=cast(Any, options.get("signal")))

    async def _produce(self) -> None:
        params = build_params(
            self._model,
            self._context,
            self._options,
            simple=self._simple,
            base_url=self._base_url(),
        )
        sdk_options = request_options(self._options)
        create = self._client.chat.completions.create

        async def call() -> Any:
            return await create(**params, **sdk_options)

        stream = await retry_provider_request(
            call,
            max_retries=int(self._options.get("maxRetries") or 0),
            max_retry_delay_ms=optional_int(self._options.get("maxRetryDelayMs")),
            signal=cast(Any, self._options.get("signal")),
        )
        self.emit("start")
        async for chunk in stream:
            self._consume(chunk)
        self._finish_blocks()
        if self._signal is not None and self._signal.aborted:
            raise asyncio.CancelledError
        if not self._has_finish_reason:
            raise RuntimeError("Stream ended without finish_reason")
        if self.output["stopReason"] == "error":
            raise RuntimeError(cast(str, self.output.get("errorMessage") or "Provider returned an error"))
        self.emit("done", reason=self.output["stopReason"], message=self.output)

    def _base_url(self) -> str:
        configured = getattr(self._client, "base_url", None)
        return DEFAULT_BASE_URL if configured is None else str(configured)

    # ── Chunk translation ────────────────────────────────────────────────

    def _consume(self, chunk: Any) -> None:
        response_id = provider_field(chunk, "id")
        if response_id and not self.output.get("responseId"):
            self.output["responseId"] = response_id
        response_model = provider_field(chunk, "model")
        if (
            isinstance(response_model, str)
            and response_model
            and response_model != self._model.model_id
            and not self.output.get("responseModel")
        ):
            self.output["responseModel"] = response_model
        usage = provider_field(chunk, "usage")
        if usage is not None:
            self.output["usage"] = parse_usage(usage, self._model)

        choices = provider_field(chunk, "choices") or ()
        choice = choices[0] if choices else None
        if choice is None:
            return
        finish_reason = provider_field(choice, "finish_reason")
        if finish_reason is not None:
            self.output["rawStopReason"] = finish_reason
            stop_reason, error_message = map_stop_reason(finish_reason)
            self.output["stopReason"] = stop_reason
            if error_message is not None:
                self.output["errorMessage"] = error_message
            self._has_finish_reason = True
        delta = provider_field(choice, "delta")
        if delta is None:
            return

        for name in _REASONING_FIELDS:
            reasoning = provider_field(delta, name)
            if isinstance(reasoning, str) and reasoning:
                self._append_thinking(reasoning, name)
                break
        content = provider_field(delta, "content")
        if isinstance(content, str) and content:
            self._append_text(content)
        for call in provider_field(delta, "tool_calls") or ():
            self._append_tool_call(call)

    def _append_text(self, text: str) -> None:
        block = self._text_block
        if block is None:
            block = {"type": "text", "text": ""}
            self.output["content"].append(block)
            self._text_block = block
            self.emit("text_start", content_index=len(self.output["content"]) - 1)
        block["text"] += text
        self.emit("text_delta", content_index=len(self.output["content"]) - 1, delta=text)

    def _append_thinking(self, text: str, signature: str) -> None:
        block = self._thinking_block
        if block is None:
            block = {"type": "thinking", "thinking": "", "thinkingSignature": signature}
            self.output["content"].append(block)
            self._thinking_block = block
            self.emit("thinking_start", content_index=len(self.output["content"]) - 1)
        block["thinking"] += text
        self.emit("thinking_delta", content_index=len(self.output["content"]) - 1, delta=text)

    def _append_tool_call(self, call: Any) -> None:
        provider_index = provider_field(call, "index")
        if not isinstance(provider_index, int):
            call_id = provider_field(call, "id")
            provider_index = next(
                (
                    index
                    for index, block in self._tool_blocks.items()
                    if call_id and block["id"] == call_id
                ),
                max(self._tool_blocks, default=-1) + 1,
            )
        block = self._tool_blocks.get(provider_index)
        if block is None:
            block = {"type": "toolCall", "id": "", "name": "", "arguments": {}}
            self._tool_blocks[provider_index] = block
            self._tool_text[provider_index] = ""
            self.output["content"].append(block)
            self._tool_block_index[id(block)] = provider_index
            self.emit("toolcall_start", content_index=len(self.output["content"]) - 1)
        call_id = provider_field(call, "id")
        if call_id and not block["id"]:
            block["id"] = call_id
        function = provider_field(call, "function")
        name = provider_field(function, "name")
        if name and not block["name"]:
            block["name"] = name
        arguments = provider_field(function, "arguments")
        if isinstance(arguments, str) and arguments:
            self._tool_text[provider_index] += arguments
            block["arguments"] = parse_streaming_json(self._tool_text[provider_index])
            self.emit(
                "toolcall_delta",
                content_index=content_index(self.output["content"], block),
                delta=arguments,
            )

    def _finish_blocks(self) -> None:
        for block_position, block in enumerate(self.output["content"]):
            kind = block.get("type")
            if kind == "text":
                self.emit("text_end", content_index=block_position, content=block["text"])
            elif kind == "thinking":
                self.emit("thinking_end", content_index=block_position, content=block["thinking"])
            elif kind == "toolCall":
                provider_index = self._tool_block_index.get(id(block))
                text = self._tool_text.get(provider_index, "") if provider_index is not None else ""
                arguments, error = parse_tool_call_arguments(text)
                block["arguments"] = arguments
                if error is not None:
                    block["argumentsError"] = error
                self.emit("toolcall_end", content_index=block_position, toolCall=block)


def build_params(
    model: Model,
    context: Mapping[str, Any],
    options: Mapping[str, Any],
    *,
    simple: bool,
    base_url: str,
) -> dict[str, Any]:
    """The Chat Completions request body for one transcript."""

    head, rest = collapse_system_messages(cast(Sequence[Mapping[str, Any]], context.get("messages") or ()))
    transformed = transform_messages(rest, model, api=OPENAI_API)
    messages = convert_messages(transformed, model)
    if head is not None:
        text = system_message_text(head)
        if text:
            messages.insert(0, {"role": _instruction_role(model), "content": text})

    params: dict[str, Any] = {
        "model": model.model_id,
        "messages": messages,
        "stream": True,
        "stream_options": {"include_usage": True},
        "store": False,
    }
    declared = (head.get("toolsAdded") or ()) if head is not None else ()
    tools = convert_tools(cast(Sequence[Mapping[str, Any]], declared))
    if tools:
        params["tools"] = tools
    elif has_tool_history(rest):
        params["tools"] = []

    max_tokens = optional_int(options.get("maxTokens")) or model.max_tokens
    if max_tokens and max_tokens > 0:
        params["max_completion_tokens"] = max_tokens
    reasoning = options.get("reasoning")
    if not simple:
        reasoning = options.get("reasoningEffort", reasoning)
    if model.reasoning and isinstance(reasoning, str) and reasoning != "off":
        effort = _REASONING_EFFORT.get(reasoning)
        if effort is not None:
            params["reasoning_effort"] = effort
    if "temperature" in options and options["temperature"] is not None:
        params["temperature"] = options["temperature"]
    if "toolChoice" in options and options["toolChoice"] is not None:
        params["tool_choice"] = options["toolChoice"]

    retention = str(options.get("cacheRetention") or "short")
    session_id = options.get("sessionId")
    if (
        retention != "none"
        and isinstance(session_id, str)
        and session_id
        and ("api.openai.com" in base_url or retention == "long")
    ):
        params["prompt_cache_key"] = session_id[:PROMPT_CACHE_KEY_MAX_LENGTH]
    if retention == "long":
        params["prompt_cache_retention"] = "24h"
    metadata = options.get("metadata")
    if isinstance(metadata, Mapping) and metadata:
        params["metadata"] = dict(metadata)
    return params


def _instruction_role(model: Model) -> str:
    return "developer" if model.reasoning else "system"


def convert_messages(
    messages: Sequence[Mapping[str, Any]], model: Model
) -> list[dict[str, Any]]:
    """Convert transformed pi-ai messages to Chat Completions messages."""

    params: list[dict[str, Any]] = []
    index = 0
    while index < len(messages):
        message = messages[index]
        role = message.get("role")
        if role == "user":
            converted = _user_message(message)
            if converted is not None:
                params.append(converted)
        elif role == "assistant":
            converted = _assistant_message(message)
            if converted is not None:
                params.append(converted)
        elif role == "toolResult":
            images: list[dict[str, Any]] = []
            while index < len(messages) and messages[index].get("role") == "toolResult":
                result = messages[index]
                params.append(_tool_message(result))
                if "image" in model.input:
                    images.extend(_image_parts(result.get("content")))
                index += 1
            if images:
                params.append(
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Attached image(s) from tool result:"},
                            *images,
                        ],
                    }
                )
            continue
        index += 1
    return params


def _user_message(message: Mapping[str, Any]) -> dict[str, Any] | None:
    content = message.get("content")
    if isinstance(content, str):
        return {"role": "user", "content": content}
    parts: list[dict[str, Any]] = []
    for block in cast(Sequence[Mapping[str, Any]], content or ()):
        if block.get("type") == "text":
            text = cast(str, block.get("text") or "")
            if text:
                parts.append({"type": "text", "text": text})
        elif block.get("type") == "image":
            parts.append(_image_part(block))
    return {"role": "user", "content": parts} if parts else None


def _assistant_message(message: Mapping[str, Any]) -> dict[str, Any] | None:
    content = cast(Sequence[Mapping[str, Any]], message.get("content") or ())
    text = "".join(
        cast(str, block.get("text") or "") for block in content if block.get("type") == "text"
    )
    calls = [block for block in content if block.get("type") == "toolCall"]
    converted: dict[str, Any] = {"role": "assistant", "content": text or None}
    if calls:
        converted["tool_calls"] = [
            {
                "id": cast(str, call.get("id") or ""),
                "type": "function",
                "function": {
                    "name": cast(str, call.get("name") or ""),
                    "arguments": _json_arguments(call.get("arguments")),
                },
            }
            for call in calls
        ]
    if not text and not calls:
        return None
    return converted


def _tool_message(message: Mapping[str, Any]) -> dict[str, Any]:
    content = message.get("content")
    text = "\n".join(
        cast(str, block.get("text") or "")
        for block in cast(Sequence[Mapping[str, Any]], content or ())
        if block.get("type") == "text"
    )
    has_images = any(
        block.get("type") == "image"
        for block in cast(Sequence[Mapping[str, Any]], content or ())
    )
    if not text:
        text = "(see attached image)" if has_images else "(no tool output)"
    return {
        "role": "tool",
        "content": text,
        "tool_call_id": cast(str, message.get("toolCallId") or ""),
    }


def _image_parts(content: Any) -> list[dict[str, Any]]:
    return [
        _image_part(block)
        for block in cast(Sequence[Mapping[str, Any]], content or ())
        if block.get("type") == "image"
    ]


def _image_part(block: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "image_url",
        "image_url": {
            "url": f"data:{block.get('mimeType')};base64,{block.get('data')}",
        },
    }


def _json_arguments(arguments: Any) -> str:
    return json.dumps(arguments if arguments is not None else {}, ensure_ascii=False, separators=(",", ":"))


def convert_tools(tools: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Convert stored tool declarations to Chat Completions function tools."""

    return [
        {
            "type": "function",
            "function": {
                "name": cast(str, tool.get("name") or ""),
                "description": cast(str, tool.get("description") or ""),
                "parameters": tool.get("parameters") or {"type": "object", "properties": {}},
            },
        }
        for tool in tools
    ]


def parse_usage(raw: Any, model: Model) -> dict[str, Any]:
    """Map an OpenAI usage object onto the pi-ai usage shape and price it."""

    prompt_tokens = int(provider_field(raw, "prompt_tokens") or 0)
    details = provider_field(raw, "prompt_tokens_details") or {}
    cached = provider_field(details, "cached_tokens")
    if cached is None:
        cached = provider_field(raw, "prompt_cache_hit_tokens")
    if cached is None:
        cached = provider_field(raw, "cached_tokens")
    cache_write = int(provider_field(details, "cache_write_tokens") or 0)
    cache_read = int(cached or 0)
    input_tokens = max(0, prompt_tokens - cache_read - cache_write)
    output_tokens = int(provider_field(raw, "completion_tokens") or 0)
    completion_details = provider_field(raw, "completion_tokens_details") or {}
    reasoning = int(provider_field(completion_details, "reasoning_tokens") or 0)
    usage = zero_usage()
    usage["input"] = input_tokens
    usage["output"] = output_tokens
    usage["cacheRead"] = cache_read
    usage["cacheWrite"] = cache_write
    usage["reasoning"] = reasoning
    usage["totalTokens"] = input_tokens + output_tokens + cache_read + cache_write
    calculate_cost(model, usage)
    return usage


def map_stop_reason(reason: str) -> tuple[str, str | None]:
    """Map an OpenAI ``finish_reason`` to a stop reason, with error text when it is a failure."""

    if reason in _STOP_REASONS:
        return _STOP_REASONS[reason], None
    return "error", f"Provider finish_reason: {reason}"


__all__ = ["OPENAI_API", "OpenAIModels"]

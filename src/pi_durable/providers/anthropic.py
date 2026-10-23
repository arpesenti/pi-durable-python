"""Anthropic Messages model adapter (optional ``anthropic`` extra).

The adapter implements the port's :class:`~pi_durable.providers.Models` protocol over the official
``AsyncAnthropic`` client. It builds a Messages request from a pi-ai shaped transcript, translates
streamed events into the port's assistant-message events (text, signed thinking, and tool-call
deltas), and returns the terminal message generation and compaction classify.

The model catalog belongs to the caller: pass a sequence of :class:`~pi_durable.providers.Model`
values and the adapter resolves ``(provider, model_id)`` through it, so context windows, max-token
ceilings, input capabilities, and cost rates travel with each model. Anthropic does not take a
session ID; the conversation's ``pi.provider`` identity is used by providers that have a cache key.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import Mapping, Sequence
from typing import Any, cast

from . import Model
from .base import ProviderModels
from .json_parse import parse_streaming_json, parse_tool_call_arguments
from .request import optional_int, request_options
from .retry import retry_provider_request
from .stream import ProviderStream, content_index, now_ms, provider_field, provider_mapping
from .transcript import collapse_system_messages, system_message_text, transform_messages
from .usage import calculate_cost, zero_usage

ANTHROPIC_API = "anthropic-messages"
DEFAULT_MAX_TOKENS = 8_192

_THINKING_BUDGETS = {
    "minimal": 1_024,
    "low": 2_048,
    "medium": 8_192,
    "high": 16_384,
    "xhigh": 16_384,
    "max": 16_384,
}
_MIN_ANSWER_TOKENS = 1_024
_TOOL_CALL_ID = re.compile(r"[^a-zA-Z0-9_-]")

_STOP_REASONS = {
    "end_turn": "stop",
    "max_tokens": "length",
    "tool_use": "toolUse",
    "pause_turn": "stop",
    "stop_sequence": "stop",
}


class AnthropicModels(ProviderModels):
    """The ``Models`` protocol over Anthropic Messages.

    ``client`` injects an ``AsyncAnthropic``-shaped client (tests use a fake); otherwise a client is
    built lazily from ``api_key``, ``base_url``, and ``headers``. Without an injected client an API
    key is required in the same call, ``ANTHROPIC_API_KEY``, or ``options["apiKey"]``.
    """

    api = ANTHROPIC_API

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
        return _AnthropicStream(client, model, context, options, output, simple=simple)

    def _resolve_client(self, model: Model, options: Mapping[str, Any]) -> Any:
        if self._client is not None:
            return self._client
        api_key = (
            self._api_key
            or cast(str | None, options.get("apiKey"))
            or os.environ.get("ANTHROPIC_API_KEY")
        )
        if not api_key:
            raise ValueError(f"No API key for provider: {model.provider}")
        try:
            from anthropic import AsyncAnthropic
        except ImportError as error:  # pragma: no cover - covered by the extras test
            raise ImportError(
                "The Anthropic adapter requires the 'anthropic' extra: "
                "pip install pi-durable-python[anthropic]"
            ) from error
        self._client = AsyncAnthropic(
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
        "api": ANTHROPIC_API,
        "provider": model.provider,
        "model": model.model_id,
        "usage": zero_usage(),
        "stopReason": "pending",
        "timestamp": now_ms(),
    }


class _AnthropicStream(ProviderStream):
    """One Messages request translated into assistant-message events."""

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
        self._blocks: dict[int, dict[str, Any]] = {}
        self._tool_text: dict[int, str] = {}
        super().__init__(output, signal=cast(Any, options.get("signal")))

    async def _produce(self) -> None:
        params = build_params(self._model, self._context, self._options, simple=self._simple)
        sdk_options = request_options(self._options)
        create = self._client.messages.create

        async def call() -> Any:
            return await create(**params, **sdk_options)

        stream = await retry_provider_request(
            call,
            max_retries=int(self._options.get("maxRetries") or 0),
            max_retry_delay_ms=optional_int(self._options.get("maxRetryDelayMs")),
            signal=cast(Any, self._options.get("signal")),
        )
        self.emit("start")
        async for event in stream:
            self._consume(event)
        if self._signal is not None and self._signal.aborted:
            raise asyncio.CancelledError
        if self.output["stopReason"] == "pending":
            raise RuntimeError("Anthropic stream ended without a stop reason")
        if self.output["stopReason"] == "error":
            raise RuntimeError(cast(str, self.output.get("errorMessage") or "Provider returned an error"))
        self.emit("done", reason=self.output["stopReason"], message=self.output)

    # ── Event translation ────────────────────────────────────────────────

    def _consume(self, event: Any) -> None:
        kind = provider_field(event, "type")
        if kind == "message_start":
            message = provider_field(event, "message") or {}
            self.output["responseId"] = provider_field(message, "id")
            response_model = provider_field(message, "model")
            if isinstance(response_model, str) and response_model and response_model != self._model.model_id:
                self.output["responseModel"] = response_model
            usage = provider_field(message, "usage")
            if usage is not None:
                self.output["usage"] = parse_usage(usage, self._model)
        elif kind == "content_block_start":
            self._start_block(provider_field(event, "index"), provider_field(event, "content_block"))
        elif kind == "content_block_delta":
            self._apply_delta(provider_field(event, "index"), provider_field(event, "delta"))
        elif kind == "content_block_stop":
            self._stop_block(provider_field(event, "index"))
        elif kind == "message_delta":
            delta = provider_field(event, "delta") or {}
            stop_reason = provider_field(delta, "stop_reason")
            if stop_reason is not None:
                self.output["rawStopReason"] = stop_reason
                mapped, error_message = map_stop_reason(
                    stop_reason, provider_field(delta, "stop_details")
                )
                self.output["stopReason"] = mapped
                if error_message is not None:
                    self.output["errorMessage"] = error_message
            usage = provider_field(event, "usage")
            if usage is not None:
                self._merge_usage(usage)
        elif kind == "error":
            error = provider_field(event, "error") or {}
            raise RuntimeError(str(provider_field(error, "message") or "Anthropic stream error"))

    def _start_block(self, index: int, content_block: Any) -> None:
        kind = provider_field(content_block, "type")
        block: dict[str, Any] | None = None
        if kind == "text":
            block = {"type": "text", "text": cast(str, provider_field(content_block, "text") or "")}
            event_kind = "text_start"
        elif kind == "thinking":
            block = {
                "type": "thinking",
                "thinking": cast(str, provider_field(content_block, "thinking") or ""),
                "thinkingSignature": cast(str, provider_field(content_block, "signature") or ""),
            }
            event_kind = "thinking_start"
        elif kind == "redacted_thinking":
            block = {
                "type": "thinking",
                "thinking": "[Reasoning redacted]",
                "thinkingSignature": cast(str, provider_field(content_block, "data") or ""),
                "redacted": True,
            }
            event_kind = "thinking_start"
        elif kind == "tool_use":
            block = {
                "type": "toolCall",
                "id": cast(str, provider_field(content_block, "id") or ""),
                "name": cast(str, provider_field(content_block, "name") or ""),
                "arguments": provider_mapping(provider_field(content_block, "input")),
            }
            self._tool_text[index] = ""
            event_kind = "toolcall_start"
        else:
            return
        self._blocks[index] = block
        self.output["content"].append(block)
        self.emit(event_kind, content_index=len(self.output["content"]) - 1)

    def _apply_delta(self, index: int, delta: Any) -> None:
        block = self._blocks.get(index)
        if block is None:
            return
        kind = provider_field(delta, "type")
        block_index = content_index(self.output["content"], block)
        if kind == "text_delta" and block["type"] == "text":
            text = cast(str, provider_field(delta, "text") or "")
            block["text"] += text
            self.emit("text_delta", content_index=block_index, delta=text)
        elif kind == "thinking_delta" and block["type"] == "thinking":
            thinking = cast(str, provider_field(delta, "thinking") or "")
            block["thinking"] += thinking
            self.emit("thinking_delta", content_index=block_index, delta=thinking)
        elif kind == "signature_delta" and block["type"] == "thinking":
            block["thinkingSignature"] += cast(str, provider_field(delta, "signature") or "")
        elif kind == "input_json_delta" and block["type"] == "toolCall":
            text = cast(str, provider_field(delta, "partial_json") or "")
            self._tool_text[index] += text
            block["arguments"] = parse_streaming_json(self._tool_text[index])
            self.emit("toolcall_delta", content_index=block_index, delta=text)

    def _stop_block(self, index: int) -> None:
        block = self._blocks.pop(index, None)
        if block is None:
            return
        block_index = content_index(self.output["content"], block)
        kind = block["type"]
        if kind == "text":
            self.emit("text_end", content_index=block_index, content=block["text"])
        elif kind == "thinking":
            self.emit("thinking_end", content_index=block_index, content=block["thinking"])
        elif kind == "toolCall":
            text = self._tool_text.pop(index, "")
            if text.strip():
                arguments, error = parse_tool_call_arguments(text)
                block["arguments"] = arguments
                if error is not None:
                    block["argumentsError"] = error
            self.emit("toolcall_end", content_index=block_index, toolCall=block)

    def _merge_usage(self, raw: Any) -> None:
        usage = parse_usage(raw, self._model, base=cast(dict[str, Any], self.output["usage"]))
        self.output["usage"] = usage


def build_params(
    model: Model,
    context: Mapping[str, Any],
    options: Mapping[str, Any],
    *,
    simple: bool,
) -> dict[str, Any]:
    """The Messages request body for one transcript."""

    head, rest = collapse_system_messages(cast(Sequence[Mapping[str, Any]], context.get("messages") or ()))
    transformed = transform_messages(
        rest, model, api=ANTHROPIC_API, normalize_tool_call_id=normalize_tool_call_id
    )
    retention = str(options.get("cacheRetention") or "short")
    cache_control = None if retention == "none" else _cache_control(retention)

    messages = convert_messages(transformed)
    params: dict[str, Any] = {
        "model": model.model_id,
        "messages": messages,
        "stream": True,
    }
    text = system_message_text(head) if head is not None else ""
    if text:
        block: dict[str, Any] = {"type": "text", "text": text}
        if cache_control is not None:
            block["cache_control"] = cache_control
        params["system"] = [block]

    max_tokens = optional_int(options.get("maxTokens")) or model.max_tokens
    if max_tokens <= 0:
        max_tokens = DEFAULT_MAX_TOKENS
    reasoning = options.get("reasoning")
    if not simple:
        reasoning = options.get("effort", reasoning)
    if model.reasoning and isinstance(reasoning, str) and reasoning != "off":
        budget = _THINKING_BUDGETS.get(reasoning)
        if budget is not None:
            max_tokens, budget = adjust_max_tokens_for_thinking(
                optional_int(options.get("maxTokens")), model.max_tokens or max_tokens, budget
            )
            params["thinking"] = {"type": "enabled", "budget_tokens": budget}
    elif "thinkingEnabled" in options and options["thinkingEnabled"] and model.reasoning:
        budget = optional_int(options.get("thinkingBudgetTokens")) or 1_024
        max_tokens, budget = adjust_max_tokens_for_thinking(
            optional_int(options.get("maxTokens")), model.max_tokens or max_tokens, budget
        )
        params["thinking"] = {"type": "enabled", "budget_tokens": budget}
    params["max_tokens"] = max_tokens

    declared = (head.get("toolsAdded") or ()) if head is not None else ()
    tools = convert_tools(cast(Sequence[Mapping[str, Any]], declared), cache_control)
    if tools:
        params["tools"] = tools

    if "temperature" in options and options["temperature"] is not None and "thinking" not in params:
        params["temperature"] = options["temperature"]
    if "toolChoice" in options and options["toolChoice"] is not None:
        params["tool_choice"] = options["toolChoice"]
    metadata = options.get("metadata")
    if isinstance(metadata, Mapping):
        user_id = metadata.get("user_id")
        if isinstance(user_id, str):
            params["metadata"] = {"user_id": user_id}
    return params


def convert_messages(messages: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Convert transformed pi-ai messages to Messages API messages."""

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
            blocks: list[dict[str, Any]] = []
            while index < len(messages) and messages[index].get("role") == "toolResult":
                blocks.append(_tool_result_block(messages[index]))
                index += 1
            params.append({"role": "user", "content": blocks})
            continue
        index += 1
    return params


def _user_message(message: Mapping[str, Any]) -> dict[str, Any] | None:
    content = message.get("content")
    if isinstance(content, str):
        return {"role": "user", "content": content} if content.strip() else None
    blocks: list[dict[str, Any]] = []
    for block in cast(Sequence[Mapping[str, Any]], content or ()):
        if block.get("type") == "text":
            text = cast(str, block.get("text") or "")
            if text.strip():
                blocks.append({"type": "text", "text": text})
        elif block.get("type") == "image":
            blocks.append(_image_block(block))
    return {"role": "user", "content": blocks} if blocks else None


def _assistant_message(message: Mapping[str, Any]) -> dict[str, Any] | None:
    blocks: list[dict[str, Any]] = []
    for block in cast(Sequence[Mapping[str, Any]], message.get("content") or ()):
        kind = block.get("type")
        if kind == "text":
            text = cast(str, block.get("text") or "")
            if text.strip():
                blocks.append({"type": "text", "text": text})
        elif kind == "thinking":
            if block.get("redacted"):
                blocks.append(
                    {"type": "redacted_thinking", "data": block.get("thinkingSignature")}
                )
                continue
            signature = block.get("thinkingSignature")
            thinking = cast(str, block.get("thinking") or "")
            if not thinking.strip():
                continue
            if not isinstance(signature, str) or not signature.strip():
                # An unsigned block cannot be replayed as thinking; keep its text as visible context.
                blocks.append({"type": "text", "text": thinking})
            else:
                blocks.append({"type": "thinking", "thinking": thinking, "signature": signature})
        elif kind == "toolCall":
            blocks.append(
                {
                    "type": "tool_use",
                    "id": cast(str, block.get("id") or ""),
                    "name": cast(str, block.get("name") or ""),
                    "input": dict(cast(Mapping[str, Any], block.get("arguments") or {})),
                }
            )
    return {"role": "assistant", "content": blocks} if blocks else None


def _tool_result_block(message: Mapping[str, Any]) -> dict[str, Any]:
    raw = cast(Sequence[Mapping[str, Any]], message.get("content") or ())
    blocks: list[dict[str, Any]] = []
    for block in raw:
        if block.get("type") == "text":
            text = cast(str, block.get("text") or "")
            if text:
                blocks.append({"type": "text", "text": text})
        elif block.get("type") == "image":
            blocks.append(_image_block(block))
    if not blocks:
        blocks = [{"type": "text", "text": "(no tool output)"}]
    return {
        "type": "tool_result",
        "tool_use_id": cast(str, message.get("toolCallId") or ""),
        "content": blocks,
        "is_error": bool(message.get("isError")),
    }


def _image_block(block: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": block.get("mimeType"),
            "data": block.get("data"),
        },
    }


def convert_tools(
    tools: Sequence[Mapping[str, Any]], cache_control: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Convert stored tool declarations to Messages tools, caching the last one."""

    converted: list[dict[str, Any]] = []
    for tool in tools:
        definition: dict[str, Any] = {
            "name": cast(str, tool.get("name") or ""),
            "description": cast(str, tool.get("description") or ""),
            "input_schema": tool.get("parameters") or {"type": "object", "properties": {}},
        }
        converted.append(definition)
    if converted and cache_control is not None:
        converted[-1]["cache_control"] = cache_control
    return converted


def normalize_tool_call_id(value: str) -> str:
    """Anthropic tool IDs use ``[a-zA-Z0-9_-]`` and at most 64 characters."""

    return _TOOL_CALL_ID.sub("_", value)[:64]


def adjust_max_tokens_for_thinking(
    base_max_tokens: int | None, model_max_tokens: int, thinking_budget: int
) -> tuple[int, int]:
    """Fit a thinking budget under the response ceiling, leaving room for the answer."""

    max_tokens = (
        model_max_tokens
        if base_max_tokens is None
        else min(base_max_tokens + thinking_budget, model_max_tokens)
    )
    if max_tokens <= thinking_budget:
        thinking_budget = min(thinking_budget, max(0, max_tokens - _MIN_ANSWER_TOKENS))
    return max_tokens, thinking_budget


def parse_usage(raw: Any, model: Model, *, base: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Map an Anthropic usage object onto the pi-ai usage shape and price it."""

    usage = zero_usage() if base is None else {**base, "cost": dict(base.get("cost") or {})}
    input_tokens = optional_int(provider_field(raw, "input_tokens"))
    output_tokens = optional_int(provider_field(raw, "output_tokens"))
    cache_read = optional_int(provider_field(raw, "cache_read_input_tokens"))
    cache_write = optional_int(provider_field(raw, "cache_creation_input_tokens"))
    cache_creation = provider_field(raw, "cache_creation") or {}
    cache_write_1h = optional_int(provider_field(cache_creation, "ephemeral_1h_input_tokens"))
    details = provider_field(raw, "output_tokens_details") or {}
    reasoning = optional_int(provider_field(details, "thinking_tokens"))
    if input_tokens is not None:
        usage["input"] = input_tokens
        usage["output"] = output_tokens or 0
        usage["cacheRead"] = cache_read or 0
        usage["cacheWrite"] = cache_write or 0
        if cache_write_1h is not None:
            usage["cacheWrite1h"] = cache_write_1h
        if reasoning is not None:
            usage["reasoning"] = reasoning
        usage["totalTokens"] = (
            int(usage["input"])
            + int(usage["output"])
            + int(usage["cacheRead"])
            + int(usage["cacheWrite"])
        )
    else:
        if output_tokens is not None:
            usage["output"] = output_tokens
        if cache_read is not None:
            usage["cacheRead"] = cache_read
        if cache_write is not None:
            usage["cacheWrite"] = cache_write
        if cache_write_1h is not None:
            usage["cacheWrite1h"] = cache_write_1h
        if reasoning is not None:
            usage["reasoning"] = reasoning
        usage["totalTokens"] = (
            int(usage.get("input") or 0)
            + int(usage.get("output") or 0)
            + int(usage.get("cacheRead") or 0)
            + int(usage.get("cacheWrite") or 0)
        )
    calculate_cost(model, usage)
    return usage


def map_stop_reason(reason: str, stop_details: Any = None) -> tuple[str, str | None]:
    """Map an Anthropic ``stop_reason`` to a stop reason, with error text when it is a failure."""

    if reason in _STOP_REASONS:
        return _STOP_REASONS[reason], None
    if reason == "refusal":
        explanation = provider_field(stop_details, "explanation")
        refused = (
            explanation
            if isinstance(explanation, str) and explanation
            else "The model refused to complete the request"
        )
        return "error", refused
    if reason == "sensitive":
        return "error", "Provider stopped with: sensitive"
    return "error", f"Unhandled stop reason: {reason}"


def _cache_control(retention: str) -> dict[str, Any]:
    control: dict[str, Any] = {"type": "ephemeral"}
    if retention == "long":
        control["ttl"] = "1h"
    return control


__all__ = ["ANTHROPIC_API", "AnthropicModels"]

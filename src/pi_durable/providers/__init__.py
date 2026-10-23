"""Provider-facing model protocol and the shared pi-ai helpers generation reads.

The port replaces pi-ai with a small ``Models`` protocol. The OpenAI and Anthropic adapters in the
optional extras implement it; the generation task only needs model lookup, a streaming request, a
non-streaming request, and the deferred-response calls. The classifiers mirror the pi-ai retry and
overflow utilities generation uses to decide an attempt's outcome.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Awaitable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

DEFAULT_MAX_AGENT_RETRY_DELAY_MS = 60_000


@dataclass(frozen=True, slots=True)
class Model:
    """One provider model's identity, limits, and input capabilities."""

    provider: str
    model_id: str
    context_window: int = 0
    max_tokens: int = 0
    reasoning: bool = True
    input: tuple[str, ...] = ("text", "image")
    """What the model takes inline; ``"image"`` enables the ``read`` image placeholder rule."""

    input_limits: Mapping[str, Any] | None = None
    """Per-input limits, such as ``{"images": {"resize": {"max_width": 2000}}}``."""


class AssistantMessageStream(Protocol):
    """A pi-ai style stream: partial events while it runs, then the terminal message."""

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]: ...

    def result(self) -> Awaitable[dict[str, Any]]: ...


@runtime_checkable
class Models(Protocol):
    """The model access generation and compaction use."""

    def get_model(self, provider: str, model_id: str) -> Model | None: ...

    def stream_simple(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> AssistantMessageStream: ...

    def complete_simple(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]: ...

    def stream(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> AssistantMessageStream: ...

    def complete(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]: ...

    def fetch_deferred(
        self,
        model: Model,
        handle: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]: ...

    def cancel_deferred(
        self,
        model: Model,
        handle: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[None]: ...


# ── Usage and token estimates ───────────────────────────────────────────────


# ── Retry classification ────────────────────────────────────────────────────

_NON_RETRYABLE = re.compile(
    "|".join(
        [
            "GoUsageLimitError",
            "FreeUsageLimitError",
            "Monthly usage limit reached",
            "available balance",
            "insufficient_quota",
            "out of budget",
            "quota exceeded",
            "billing",
            "subscription_sharing_usage_limit_exceeded",
        ]
    ),
    re.IGNORECASE,
)

_RETRYABLE = re.compile(
    "|".join(
        [
            "overloaded",
            "server_busy",
            "servers are currently busy",
            "currently experiencing high demand",
            "model is at capacity",
            "rate.?limit",
            "too many requests",
            "429",
            "500",
            "502",
            "503",
            "504",
            "520",
            "524",
            "service.?(?:is.?)?(?:temporarily.?)?unavailable",
            "server.?error",
            "internal.?error",
            "provider.?returned.?(?:an.?)?error",
            "exceeded request buffer limit while retrying upstream",
            "unable to forward upstream",
            "network.?error",
            "connection.?error",
            "connection.?refused",
            "connection.?lost",
            "other side closed",
            "fetch failed",
            "getaddrinfo",
            "ENOTFOUND",
            "EAI_AGAIN",
            "upstream.?connect",
            "reset before headers",
            "socket hang up",
            "socket connection was closed",
            "timed? out",
            "timeout",
            "terminated",
            "websocket.?closed",
            "websocket.?error",
            "ended without",
            "stream ended before message_stop",
            "stream ended before a terminal response event",
            "http2 request did not get a response",
            "pending stream has been canceled",
            "retry delay",
            "you can retry your request",
            "try your request again",
            "please retry your request",
            "ResourceExhausted",
            "subscription_sharing_usage_unavailable",
            "subscription_sharing_user_unavailable",
        ]
    ),
    re.IGNORECASE,
)


def is_retryable_assistant_error(message: Mapping[str, Any]) -> bool:
    """Whether a failed assistant message looks like a transient provider or transport error."""

    if message.get("stopReason") != "error":
        return False
    text = message.get("errorMessage")
    if not isinstance(text, str) or not text:
        return False
    if _NON_RETRYABLE.search(text):
        return False
    return _RETRYABLE.search(text) is not None


def retry_delay_ms(policy: Mapping[str, Any], attempt: int) -> int:
    """Exponential backoff ``baseDelayMs * 2 ** (attempt - 1)``, capped at 60 seconds."""

    base = int(policy.get("base_delay_ms", 2_000))
    delay = base * (2 ** max(0, attempt - 1))
    cap = int(policy.get("max_agent_delay_ms", DEFAULT_MAX_AGENT_RETRY_DELAY_MS))
    return min(delay, cap)


# ── Overflow classification ─────────────────────────────────────────────────

_OVERFLOW = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"prompt (?:is )?too long",
        r"prompt exceeds max length",
        r"request_too_large",
        r"input is too long for requested model",
        r"exceeds the context window",
        r"exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))?",
        r"input token count.*exceeds the maximum",
        r"maximum prompt length is \d+",
        r"reduce the length of the messages",
        r"maximum context length is \d+ tokens",
        r"exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?",
        r"input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)",
        r"exceeds the limit of \d+",
        r"exceeds the available context size",
        r"greater than the context length",
        r"context window exceeds limit",
        r"exceeded model token limit",
        r"too large for model with \d+ maximum context length",
        r"prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?",
        r"model_context_window_exceeded",
        r"prompt too long; exceeded (?:max )?context length",
        r"range of input length should be",
        r"context[_ ]length[_ ]exceeded",
        r"too many tokens",
        r"token limit exceeded",
    )
]
_NON_OVERFLOW = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"^(Throttling error|Service unavailable):",
        r"rate limit",
        r"too many requests",
    )
]
_CEREBRAS_BODYLESS = re.compile(r"^4(?:00|13)\s*(?:status code)?\s*\(no body\)", re.IGNORECASE)


def is_context_overflow(message: Mapping[str, Any], context_window: int | None = None) -> bool:
    """Whether an assistant message reports or silently shows a context overflow."""

    text = message.get("errorMessage")
    if message.get("stopReason") == "error" and isinstance(text, str) and text:
        if not any(pattern.search(text) for pattern in _NON_OVERFLOW):
            if any(pattern.search(text) for pattern in _OVERFLOW):
                return True
            if message.get("provider") == "cerebras" and _CEREBRAS_BODYLESS.search(text):
                return True
    usage_value = message.get("usage") or {}
    input_tokens = int(usage_value.get("input", 0)) + int(usage_value.get("cacheRead", 0))
    if context_window and message.get("stopReason") == "stop" and input_tokens > context_window:
        return True
    if (
        context_window
        and message.get("stopReason") == "length"
        and int(usage_value.get("output", 0)) == 0
        and input_tokens >= context_window * 0.99
    ):
        return True
    return False


__all__ = [
    "DEFAULT_MAX_AGENT_RETRY_DELAY_MS",
    "AssistantMessageStream",
    "Model",
    "Models",
    "is_context_overflow",
    "is_retryable_assistant_error",
    "retry_delay_ms",
]

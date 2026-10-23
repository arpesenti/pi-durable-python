"""Shared Harness helpers for the translated generation suites (upstream ``chat-support.ts``).

The Python port has no pi-ai dependency, so ``FauxModels`` plays the part of the faux provider: it
returns queued assistant messages, streams partial events for a request, and serves deferred polls.
"""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

from session_support import context

from pi_durable import Harness, MemoryStorage
from pi_durable.context import AbortError
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import Registry
from pi_durable.json import copy_json
from pi_durable.providers import Model
from pi_durable.types import EntryRecord, Storage


def zero_usage() -> dict[str, Any]:
    return {
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 0,
        "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0},
    }


def usage(
    *,
    input: int = 0,
    output: int = 0,
    cache_read: int = 0,
    cache_write: int = 0,
    total_tokens: int | None = None,
    cost: Mapping[str, float] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """A complete pi-ai ``Usage`` object."""

    zero = {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0}
    value: dict[str, Any] = {
        "input": input,
        "output": output,
        "cacheRead": cache_read,
        "cacheWrite": cache_write,
        "totalTokens": input + output + cache_read + cache_write if total_tokens is None else total_tokens,
        "cost": {**zero, **dict(cost or {})},
    }
    value.update(extra)
    return value


def faux_text(text: str) -> dict[str, Any]:
    return {"type": "text", "text": text}


def faux_tool_call(name: str, arguments: Mapping[str, Any], *, id: str) -> dict[str, Any]:
    return {"type": "toolCall", "id": id, "name": name, "arguments": dict(arguments)}


def assistant_message(
    content: Any = (),
    *,
    stop_reason: str = "stop",
    error_message: str | None = None,
    provider: str = "faux",
    model: str = "faux-1",
    usage: Mapping[str, Any] | None = None,
    deferred: Mapping[str, Any] | None = None,
    timestamp: int = 1,
) -> dict[str, Any]:
    """A pi-ai ``AssistantMessage`` with the fields generation reads."""

    if isinstance(content, str):
        content = [faux_text(content)]
    message: dict[str, Any] = {
        "role": "assistant",
        "content": list(content),
        "api": "faux",
        "provider": provider,
        "model": model,
        "usage": dict(usage) if usage is not None else zero_usage(),
        "stopReason": stop_reason,
        "timestamp": timestamp,
    }
    if error_message is not None:
        message["errorMessage"] = error_message
    if deferred is not None:
        message["deferred"] = dict(deferred)
    return message


def text_of(message: Mapping[str, Any] | None) -> str | None:
    """Text of the first text content of a message."""

    if message is None or message.get("role") == "system":
        return None
    content = message.get("content")
    if isinstance(content, str):
        return content
    for item in content or ():
        if item.get("type") == "text":
            return cast(str, item["text"])
    return None


class AssistantStream:
    """A stream of partial events plus the terminal message, as pi-ai exposes."""

    def __init__(
        self,
        events: AsyncIterator[dict[str, Any]],
        result: Awaitable[dict[str, Any]],
    ) -> None:
        self._events = events
        self._result = result

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self._events

    def result(self) -> Awaitable[dict[str, Any]]:
        return self._result


@dataclass(slots=True)
class Step:
    """One queued faux response."""

    build: Callable[
        [dict[str, Any], dict[str, Any]],
        AssistantStream | Awaitable[dict[str, Any]] | dict[str, Any],
    ]
    deferred_remaining: int = 0


def static(message: dict[str, Any]) -> Step:
    """A step that streams no partial and ends with ``message``."""

    return Step(lambda _request, _options: message)


def message_stream(message: dict[str, Any]) -> AssistantStream:
    """A stream whose only event is the empty ``start`` partial pi-ai emits."""

    start = copy.deepcopy(message)
    if isinstance(start.get("content"), list):
        start["content"] = []
    else:
        start["content"] = ""
    start["stopReason"] = "pending"

    async def events() -> AsyncIterator[dict[str, Any]]:
        yield {"type": "start", "partial": start}

    return AssistantStream(events(), _future(message))


def slow_message(message: dict[str, Any], *, chunks: int = 4, interval: float = 0.6) -> Step:
    """A step that commits growing text partials before returning ``message``."""

    def build(_request: dict[str, Any], options: dict[str, Any]) -> AssistantStream:
        value = text_of(message) or ""

        async def events() -> AsyncIterator[dict[str, Any]]:
            partial = copy.deepcopy(message)
            partial["content"] = [{"type": "text", "text": ""}]
            partial["stopReason"] = "pending"
            yield {"type": "start", "partial": partial}
            step = max(1, len(value) // max(1, chunks))
            for end in range(step, len(value) + step, step):
                await asyncio.sleep(interval)
                signal = options.get("signal")
                if signal is not None and signal.aborted:
                    raise AbortError(signal.reason)
                partial = copy.deepcopy(partial)
                partial["content"] = [{"type": "text", "text": value[:end]}]
                yield {"type": "text_delta", "partial": partial}

        return AssistantStream(events(), _future(message))

    return Step(build)


def _future(message: dict[str, Any]) -> asyncio.Future[dict[str, Any]]:
    future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
    future.set_result(message)
    return future


def _never() -> asyncio.Future[dict[str, Any]]:
    return asyncio.get_running_loop().create_future()


def unanswered() -> Step:
    """A step that never answers; the request ends only through cancellation."""

    def build(_request: dict[str, Any], options: dict[str, Any]) -> AssistantStream:
        async def events() -> AsyncIterator[dict[str, Any]]:
            signal = options.get("signal")
            if signal is not None:
                await signal.wait()
            raise AbortError(None)
            yield  # pragma: no cover

        return AssistantStream(events(), _never())

    return Step(build)


class FauxModels:
    """The ``Models`` protocol over a queue of scripted responses."""

    def __init__(
        self,
        *,
        models: Sequence[Mapping[str, Any]] = (),
        deferred: Mapping[str, Any] | None = None,
    ) -> None:
        self.models = [
            Model(
                provider=str(item.get("provider", "faux")),
                model_id=str(item.get("id", "faux-1")),
                context_window=int(item.get("contextWindow", 200_000)),
                max_tokens=int(item.get("maxTokens", 8_192)),
                reasoning=bool(item.get("reasoning", True)),
            )
            for item in models
        ] or [Model(provider="faux", model_id="faux-1", context_window=200_000, max_tokens=8_192)]
        self.responses: list[Step] = []
        self.requests: list[tuple[dict[str, Any], dict[str, Any]]] = []
        self.models_used: list[str] = []
        self.current_model: Model | None = None
        self.call_count = 0
        self.deferred_fetch_count = 0
        self.cancelled_deferred: list[dict[str, Any]] = []
        self.deferred = dict(deferred or {})

    def set_responses(self, responses: Sequence[Any]) -> None:
        steps: list[Step] = []
        for response in responses:
            if isinstance(response, Step):
                steps.append(response)
            elif callable(response):
                steps.append(Step(_call_step(response)))
            elif isinstance(response, Mapping):
                steps.append(static(dict(response)))
            else:
                raise TypeError(f"Unsupported faux response {response!r}")
        pending = int(self.deferred.get("pendingFetches", 0))
        if pending > 0 and steps:
            steps[0].deferred_remaining = pending
        self.responses = steps
        self.requests = []
        self.models_used = []
        self.call_count = 0

    def get_pending_response_count(self) -> int:
        return len(self.responses)

    def get_model(self, provider: str, model_id: str) -> Model | None:
        return next(
            (model for model in self.models if model.provider == provider and model.model_id == model_id),
            None,
        )

    def stream_simple(
        self,
        model: Model,
        request: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> AssistantStream:
        values = dict(options or {})
        request_value = cast(dict[str, Any], copy_json(dict(request)))
        self.requests.append((request_value, values))
        self.models_used.append(model.model_id)
        self.current_model = model
        self.call_count += 1
        if not self.responses:
            raise RuntimeError("No faux response is queued")
        step = self.responses[0]
        if step.deferred_remaining > 0:
            step.deferred_remaining -= 1
            message = assistant_message(
                (),
                stop_reason="deferred",
                deferred={
                    "provider": "faux",
                    "modelId": "faux-1",
                    "api": "faux",
                    "id": "handle",
                    "pollAfterMs": self.deferred.get("pollAfterMs", 1),
                },
            )
            return message_stream(message)
        self.responses.pop(0)
        result = step.build(request_value, values)
        if isinstance(result, AssistantStream):
            return result
        if isinstance(result, Mapping):
            return message_stream(dict(result))
        return _awaiting_stream(result)

    def stream(
        self,
        model: Model,
        request: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> AssistantStream:
        return self.stream_simple(model, request, options)

    def complete(
        self,
        model: Model,
        request: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]:
        return self.complete_simple(model, request, options)

    def complete_simple(
        self,
        model: Model,
        request: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]:
        async def complete() -> dict[str, Any]:
            stream = self.stream_simple(model, request, options)
            async for _event in stream:
                pass
            return await stream.result()

        return complete()

    async def fetch_deferred(
        self,
        model: Model,
        handle: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        del model, options
        self.deferred_fetch_count += 1
        if not self.responses:
            raise RuntimeError("No faux response is queued")
        step = self.responses[0]
        pending = int(self.deferred.get("pendingFetches", 0))
        if self.deferred_fetch_count <= pending:
            poll_after = int(handle.get("pollAfterMs", 1))
            return assistant_message(
                (),
                stop_reason="deferred",
                deferred={**dict(handle), "pollAfterMs": poll_after},
            )
        self.responses.pop(0)
        result = step.build({}, {})
        if isinstance(result, Mapping):
            return dict(result)
        if isinstance(result, AssistantStream):
            return await result.result()
        return await result

    async def cancel_deferred(
        self,
        model: Model,
        handle: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        del model, options
        self.cancelled_deferred.append(dict(handle))


def _call_step(response: Callable[..., Any]) -> Callable[..., Any]:
    def build(request: dict[str, Any], options: dict[str, Any]) -> Any:
        return _as_stream(response, request, options)

    return build


def _awaiting_stream(result: Awaitable[dict[str, Any]]) -> AssistantStream:
    async def events() -> AsyncIterator[dict[str, Any]]:
        return
        yield  # pragma: no cover

    return AssistantStream(events(), result)


def _as_stream(
    response: Callable[..., Any],
    request: dict[str, Any],
    options: dict[str, Any],
) -> Any:
    return response(request, options)


@dataclass(slots=True)
class ChatSetup:
    """Models and registry that survive a close/reopen, like a host process's own objects."""

    models: FauxModels
    registry: Registry
    reports: list[BaseException] = field(default_factory=list)
    settings: dict[str, Any] = field(default_factory=dict)
    now: Callable[[], int] = lambda: 0

    @property
    def faux(self) -> FauxModels:
        return self.models


def chat_setup(options: Mapping[str, Any] | None = None) -> ChatSetup:
    values = dict(options or {})
    models = FauxModels(
        models=cast(Sequence[Mapping[str, Any]], values.get("models") or ()),
        deferred=cast(Mapping[str, Any] | None, values.get("deferred")),
    )
    return ChatSetup(
        models=models,
        registry=create_registry(),
        settings={},
        now=lambda: int(time.time() * 1000),
    )


async def open_chat(
    storage: Storage,
    setup: ChatSetup,
    *,
    env: Any = None,
) -> dict[str, Any]:
    """Open a Harness over ``storage`` and return its root configured with the faux model."""

    options: dict[str, Any] = {
        "models": setup.models,
        "registry": setup.registry,
        "settings": setup.settings,
        "now": setup.now,
        "on_report": setup.reports.append,
    }
    if env is not None:
        options["env"] = env
    harness = await Harness.open(storage, options, context)
    root = await harness.root(
        context, {"agent": {"model": {"provider": "faux", "modelId": "faux-1"}}}
    )
    return {"harness": harness, "root": root}


async def all_entries(conversation: Any, call_context: Any = context) -> list[EntryRecord]:
    """Raw entries of a conversation, oldest first."""

    page = await conversation.entries({}, 1_000, None, call_context)
    return list(reversed(page["items"]))


def live_of(value: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(value or {})


__all__ = [
    "AssistantStream",
    "ChatSetup",
    "EntryRecord",
    "FauxModels",
    "MemoryStorage",
    "Step",
    "all_entries",
    "assistant_message",
    "chat_setup",
    "faux_text",
    "faux_tool_call",
    "live_of",
    "message_stream",
    "open_chat",
    "slow_message",
    "static",
    "text_of",
    "unanswered",
    "usage",
    "zero_usage",
]

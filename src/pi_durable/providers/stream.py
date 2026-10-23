"""Async plumbing shared by the model adapters.

Each adapter turns one SDK request into a pi-ai style stream: partial events while the provider
responds, then a terminal assistant message. :class:`ProviderStream` owns the queue, the terminal
future, and the abort wiring so both adapters behave the same when a request fails or is cancelled.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
from typing import Any

from ..context import AbortSignal

ABORT_MESSAGE = "Request was aborted"


def provider_field(value: Any, key: str, default: Any = None) -> Any:
    """Read one field from an SDK object or a mapping."""

    if isinstance(value, Mapping):
        return value.get(key, default)
    return getattr(value, key, default)


def provider_mapping(value: Any) -> dict[str, Any]:
    """Read a nested mapping from an SDK object, a mapping, or a value with ``model_dump()``."""

    if isinstance(value, Mapping):
        return dict(value)
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        dumped = dump()
        return dict(dumped) if isinstance(dumped, Mapping) else {}
    return dict(vars(value)) if hasattr(value, "__dict__") else {}


def content_index(content: Sequence[Any], block: Any) -> int:
    """The index of ``block`` in ``content``, matched by identity rather than by value."""

    for index, candidate in enumerate(content):
        if candidate is block:
            return index
    raise ValueError("block is not part of the message content")


def error_status(error: BaseException) -> int | None:
    """The HTTP status an SDK error carries, if any."""

    status = provider_field(error, "status_code")
    if status is None:
        status = provider_field(error, "status")
    return status if isinstance(status, int) else None


def error_headers(error: BaseException) -> Any:
    """The response headers an SDK error carries, if any."""

    response = provider_field(error, "response")
    headers = provider_field(response, "headers") if response is not None else None
    return headers if headers is not None else provider_field(error, "headers")


def format_provider_error(error: BaseException) -> str:
    """The text a failed adapter attempt records, with the HTTP status when the SDK exposes it."""

    message = str(error) or type(error).__name__
    status = error_status(error)
    if status is not None and str(status) not in message:
        return f"{status}: {message}"
    return message


def now_ms() -> int:
    """A Unix timestamp in milliseconds, as pi-ai's ``Date.now()``."""

    return int(time.time() * 1000)


async def complete_stream(stream: Any) -> dict[str, Any]:
    """Consume a stream and return its terminal message."""

    async for _event in stream:
        pass
    return await stream.result()


class ProviderStream:
    """A pi-ai style stream over one asynchronous producer.

    Subclasses implement :meth:`_produce`, pushing partial events with :meth:`push` and returning
    once the terminal message is complete. Every ``BaseException`` becomes an ``error`` event and a
    terminal message with ``stopReason`` ``error`` or, when the invocation was signalled, ``aborted``.
    """

    def __init__(self, output: dict[str, Any], *, signal: AbortSignal | None = None) -> None:
        self.output = output
        self._signal = signal
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        loop = asyncio.get_running_loop()
        self._result: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._task: asyncio.Task[None] = asyncio.ensure_future(self._run())

    # ── Stream surface ─────────────────────────────────────────────────────

    def push(self, event: dict[str, Any]) -> None:
        self._queue.put_nowait(event)

    def emit(self, kind: str, *, content_index: int | None = None, **fields: Any) -> None:
        """Push one assistant-message event carrying the live partial and any extra fields."""

        event: dict[str, Any] = {"type": kind, "partial": self.output, **fields}
        if content_index is not None:
            event["contentIndex"] = content_index
        self.push(event)

    def result(self) -> Awaitable[dict[str, Any]]:
        return self._result

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[dict[str, Any]]:
        try:
            while True:
                event = await self._queue.get()
                if event is None:
                    return
                yield event
        finally:
            # A consumer that stops early cannot leave the provider request running.
            if not self._task.done():
                self._task.cancel()

    # ── Producer plumbing ─────────────────────────────────────────────────

    async def _produce(self) -> None:
        raise NotImplementedError

    async def _run(self) -> None:
        remove: Any = None
        try:
            if self._signal is not None:
                if self._signal.aborted:
                    raise asyncio.CancelledError
                remove = self._signal.add_listener(self._cancel)
            await self._produce()
        except asyncio.CancelledError:
            self._fail("aborted", ABORT_MESSAGE)
        except Exception as error:
            aborted = self._signal is not None and self._signal.aborted
            self._fail("aborted" if aborted else "error", format_provider_error(error))
        finally:
            if remove is not None:
                remove()
            if not self._result.done():
                self._result.set_result(self.output)
            self._queue.put_nowait(None)

    def _cancel(self) -> None:
        if not self._task.done():
            self._task.cancel()

    def _fail(self, stop_reason: str, message: str) -> None:
        self.output["stopReason"] = stop_reason
        self.output["errorMessage"] = message
        self.emit("error", reason=stop_reason, error=self.output)


__all__ = [
    "ABORT_MESSAGE",
    "ProviderStream",
    "complete_stream",
    "content_index",
    "error_headers",
    "error_status",
    "format_provider_error",
    "now_ms",
    "provider_field",
    "provider_mapping",
]

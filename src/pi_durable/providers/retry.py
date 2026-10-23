"""Provider-level retries inside one request attempt, shared by the model adapters.

Port of pi-ai 1.3.0 ``utils/provider-retry.ts``. Both SDKs are called with their own retries
disabled (``max_retries=0``) and wrapped here instead, so a server-requested delay is interruptible
and a delay above ``maxRetryDelayMs`` fails at once.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from email.utils import parsedate_to_datetime
from typing import Any, TypeVar

from ..context import AbortSignal
from .stream import ABORT_MESSAGE, error_headers, error_status

T = TypeVar("T")

DEFAULT_MAX_RETRY_DELAY_MS = 60_000
RETRYABLE_STATUSES = frozenset({408, 409, 429})


def is_retryable_provider_error(error: BaseException) -> bool:
    """The SDK retry policy: an explicit header first, then status, then a transport failure."""

    should_retry = _header(error_headers(error), "x-should-retry")
    if should_retry == "true":
        return True
    if should_retry == "false":
        return False
    status = error_status(error)
    if status is None:
        return True
    return status in RETRYABLE_STATUSES or status >= 500


def provider_retry_delay_ms(
    error: BaseException, retry_index: int, max_retry_delay_ms: int | None
) -> int:
    """How long to wait before the next provider attempt."""

    headers = error_headers(error)
    retry_after_ms = _header(headers, "retry-after-ms")
    if retry_after_ms is not None:
        delay = float(retry_after_ms)
        _validate_delay(delay, max_retry_delay_ms, str(error))
        return max(0, int(delay))
    retry_after = _header(headers, "retry-after")
    if retry_after is not None:
        delay = _retry_after_ms(retry_after)
        _validate_delay(delay, max_retry_delay_ms, str(error))
        return max(0, int(delay))
    exponential = min(0.5 * 2**retry_index, 8) * 1000
    return int(exponential * (1 - random.random() * 0.25))


def _retry_after_ms(value: str) -> float:
    """A ``Retry-After`` header, either delta-seconds or an HTTP date."""

    try:
        return float(value) * 1000
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return 0.0
    if retry_at is None:
        return 0.0
    return (retry_at.timestamp() - time.time()) * 1000


async def retry_provider_request(
    request: Callable[[], Awaitable[T]],
    *,
    max_retries: int = 0,
    max_retry_delay_ms: int | None = None,
    signal: AbortSignal | None = None,
) -> T:
    """Run ``request``, retrying retryable provider failures while attempts remain."""

    remaining = max(0, max_retries)
    for attempt in range(remaining + 1):
        if signal is not None and signal.aborted:
            raise asyncio.CancelledError(ABORT_MESSAGE)
        try:
            return await request()
        except Exception as error:
            if signal is not None and signal.aborted:
                raise
            if attempt >= remaining or not is_retryable_provider_error(error):
                raise
            delay = provider_retry_delay_ms(error, attempt, max_retry_delay_ms)
            await asyncio.sleep(delay / 1000.0)
    raise RuntimeError("unreachable")  # pragma: no cover


def _validate_delay(delay_ms: float, max_retry_delay_ms: int | None, message: str) -> None:
    limit = DEFAULT_MAX_RETRY_DELAY_MS if max_retry_delay_ms is None else max_retry_delay_ms
    if limit > 0 and delay_ms > limit:
        raise RuntimeError(
            f"Server requested {delay_ms / 1000:.0f}s retry delay (max: {limit / 1000:.0f}s). {message}"
        )


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    get = getattr(headers, "get", None)
    if get is None:
        return None
    value: Any = get(name)
    if value is None:
        value = get(name.title())
    return None if value is None else str(value)


__all__ = [
    "DEFAULT_MAX_RETRY_DELAY_MS",
    "RETRYABLE_STATUSES",
    "is_retryable_provider_error",
    "provider_retry_delay_ms",
    "retry_provider_request",
]

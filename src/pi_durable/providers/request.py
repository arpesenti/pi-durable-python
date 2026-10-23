"""Per-request SDK options shared by the model adapters.

Both official SDKs accept ``timeout`` in seconds and ``extra_headers`` per request. Provider-level
retries are disabled at the client and run through :mod:`pi_durable.providers.retry` instead, so no
per-request retry field is sent.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def request_options(options: Mapping[str, Any]) -> dict[str, Any]:
    """Map the port's request options onto the SDK's per-request keyword arguments."""

    values: dict[str, Any] = {}
    timeout_ms = optional_int(options.get("timeoutMs"))
    if timeout_ms is not None:
        values["timeout"] = timeout_ms / 1000.0
    headers = options.get("headers")
    if isinstance(headers, Mapping) and headers:
        values["extra_headers"] = dict(headers)
    return values


def optional_int(value: Any) -> int | None:
    """Coerce a present value to ``int``, preserving ``None``."""

    if value is None:
        return None
    return int(value)


__all__ = ["optional_int", "request_options"]

"""Provider usage accounting shared by the model adapters.

Port of the pi-ai 1.3.0 usage shape and ``calculateCost()``. Every adapter reports tokens in the
same four buckets and prices them at the model's catalog rates so the durable usage ledger sums
providers without knowing which one produced a response.
"""

from __future__ import annotations

from typing import Any

from . import Model


def zero_cost() -> dict[str, float]:
    """A complete pi-ai ``Usage.cost`` value with every rate at zero."""

    return {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0, "total": 0.0}


def zero_usage() -> dict[str, Any]:
    """A complete pi-ai ``Usage`` value with every counter at zero."""

    return {
        "input": 0,
        "output": 0,
        "cacheRead": 0,
        "cacheWrite": 0,
        "totalTokens": 0,
        "cost": zero_cost(),
    }


def calculate_cost(model: Model, usage: dict[str, Any]) -> dict[str, float]:
    """Fill ``usage["cost"]`` from the model's USD-per-million-token rates."""

    rates = model.cost or {}
    input_rate = float(rates.get("input", 0.0))
    output_rate = float(rates.get("output", 0.0))
    cache_read_rate = float(rates.get("cacheRead", 0.0))
    cache_write_rate = float(rates.get("cacheWrite", 0.0))
    long_write = int(usage.get("cacheWrite1h") or 0)
    short_write = max(0, int(usage.get("cacheWrite") or 0) - long_write)
    cost = {
        "input": input_rate / 1_000_000 * int(usage.get("input") or 0),
        "output": output_rate / 1_000_000 * int(usage.get("output") or 0),
        "cacheRead": cache_read_rate / 1_000_000 * int(usage.get("cacheRead") or 0),
        "cacheWrite": (
            cache_write_rate * short_write + input_rate * 2 * long_write
        )
        / 1_000_000,
    }
    cost["total"] = cost["input"] + cost["output"] + cost["cacheRead"] + cost["cacheWrite"]
    usage["cost"] = cost
    return cost


__all__ = ["calculate_cost", "zero_cost", "zero_usage"]

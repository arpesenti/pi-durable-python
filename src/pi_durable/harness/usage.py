"""Built-in ``pi.usage``: the ledger of one conversation's own spend.

Port of v1.3.0 ``src/harness/usage.ts``. Every built-in writer of an assistant or tool-result entry
adds that message's usage in the same commit; compaction summarization attempts add theirs without
an entry. ``Harness.usage()`` sums every conversation's document into the Session total.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any, TypedDict, cast

from ..documents import define_doc
from ..ids import ConversationId
from ..json import JsonValue, copy_json
from ..session.transaction import Transaction


class UsageState(TypedDict):
    """Spend keyed by ``provider/modelId`` and by tool name."""

    models: dict[str, dict[str, JsonValue]]
    tools: dict[str, dict[str, JsonValue]]


UsageDoc = define_doc(
    {
        "kind": "pi.usage",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"models": {}, "tools": {}},
        "checkpoint_when": lambda _state, _ops, _info: True,
    }
)


async def record_usage(
    tx: Transaction,
    conversation_id: ConversationId,
    bucket: str,
    key: str,
    usage: Mapping[str, Any],
) -> None:
    """Add ``usage`` to one bucket of the conversation's ``pi.usage``."""

    totals = (await tx.doc(UsageDoc, conversation_id))[bucket]
    current = totals.get(key) if hasattr(totals, "get") else None
    if current is None:
        totals[key] = _strict_usage(usage)
    else:
        add_usage(cast(MutableMapping[str, Any], current), usage)


def add_usage(total: MutableMapping[str, Any], usage: Mapping[str, Any]) -> None:
    """Add every counter of ``usage`` to ``total``; optional counters add once either side reports them."""

    for key in ("input", "output", "cacheRead", "cacheWrite", "totalTokens"):
        total[key] = int(total.get(key, 0)) + int(usage.get(key, 0))
    for key in ("cacheWrite1h", "reasoning"):
        if usage.get(key) is not None:
            total[key] = int(total.get(key, 0)) + int(usage[key])
    if "cost" not in total:
        total["cost"] = {}
    total_cost = cast(MutableMapping[str, Any], total["cost"])
    usage_cost = cast(Mapping[str, Any], usage.get("cost") or {})
    for key in ("input", "output", "cacheRead", "cacheWrite", "total"):
        total_cost[key] = total_cost.get(key, 0) + usage_cost.get(key, 0)


def add_usage_state(sum_value: UsageState, state: Mapping[str, Any]) -> None:
    """Add every bucket of ``state`` into ``sum``."""

    for bucket in ("models", "tools"):
        destination = sum_value[bucket]
        for key, value in cast(Mapping[str, Any], state.get(bucket) or {}).items():
            if key in destination:
                add_usage(cast(MutableMapping[str, Any], destination[key]), value)
            else:
                destination[key] = cast(dict[str, JsonValue], copy_json(value))


def _strict_usage(usage: Mapping[str, Any]) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], copy_json(dict(usage)))


__all__ = ["UsageDoc", "UsageState", "add_usage", "add_usage_state", "record_usage"]

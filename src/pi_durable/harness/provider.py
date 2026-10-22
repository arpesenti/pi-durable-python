"""Built-in ``pi.provider``: the stable provider-facing identity of one conversation.

Port of v1.3.0 ``src/harness/provider.ts``. Generation and compaction pass the persisted UUID to
the provider as ``sessionId``; a fork starts with a fresh identity instead of copying its parent's.
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any, TypedDict

from ..context import Context
from ..documents import define_doc
from ..session.transaction import Transaction


class ProviderState(TypedDict):
    """Stable provider-facing identity of one conversation."""

    sessionId: str


def uuidv7() -> str:
    """A UUIDv7 (time-ordered) string, as pi-ai's ``uuidv7()``."""

    milliseconds = int(time.time() * 1000) & ((1 << 48) - 1)
    value = bytearray(os.urandom(16))
    value[0:6] = milliseconds.to_bytes(6, "big")
    value[6] = (value[6] & 0x0F) | 0x70
    value[8] = (value[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(value)))


ProviderDoc = define_doc(
    {
        "kind": "pi.provider",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"sessionId": uuidv7()},
        "checkpoint_when": lambda _state, _ops, _info: True,
    }
)


async def ensure_provider_session_id(runtime: Any, context: Context) -> str:
    """Return the persisted identity without writing in the normal path.

    A legacy conversation without ``pi.provider`` gets one migration commit whose ``tx.doc()`` runs
    ``initial()`` before the provider request starts.
    """

    existing = await runtime.snapshot(ProviderDoc, runtime.conversation_id, context)
    if existing is not None:
        return _session_id(existing)

    created: dict[str, str] = {}

    async def create(tx: Transaction) -> None:
        created["session"] = _session_id(await tx.doc(ProviderDoc, runtime.conversation_id))

    await runtime.commit(create, context)
    if "session" not in created:
        raise RuntimeError(f"Conversation {runtime.conversation_id} has no provider session ID")
    return created["session"]


def _session_id(state: Any) -> str:
    value = state["sessionId"]
    if not isinstance(value, str) or not value:
        raise TypeError("pi.provider is missing its sessionId")
    return value


__all__ = ["ProviderDoc", "ProviderState", "ensure_provider_session_id", "uuidv7"]

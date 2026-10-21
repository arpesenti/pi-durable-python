"""Definition-free document copies selected by a conversation fork."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .documents import address_id
from .errors import StorageRequestError
from .storage.base import Storage


async def prepare_fork_document_copies(
    storage: Storage,
    parent_conversation_id: int,
    at: int,
    child_conversation_id: int,
) -> list[dict[str, Any]]:
    """Select every persisted conversation document copied by one fork.

    Documents whose policy is ``asOf`` are taken from the owning ancestor at the fork
    entry's commit; ``current`` documents are taken from the parent's current state.
    """
    stored = await storage.entry(parent_conversation_id, at)
    if stored is None:
        raise StorageRequestError(f"Entry {at} is not visible from conversation {parent_conversation_id}")
    entry = stored["entry"]
    copies: list[dict[str, Any]] = []
    copied_addresses: set[str] = set()
    await _collect(
        storage,
        {"kind": "conversation", "conversationId": entry["conversationId"]},
        stored["commitSeq"],
        "asOf",
        child_conversation_id,
        copies,
        copied_addresses,
    )
    await _collect(
        storage,
        {"kind": "conversation", "conversationId": parent_conversation_id},
        "current",
        "current",
        child_conversation_id,
        copies,
        copied_addresses,
    )
    return copies


async def _collect(
    storage: Storage,
    scope: Mapping[str, Any],
    at: int | str,
    policy: str,
    child_conversation_id: int,
    copies: list[dict[str, Any]],
    copied_addresses: set[str],
) -> None:
    cursor = None
    while True:
        page = await storage.scan_documents({"scope": scope, "at": at}, 256, cursor)
        for source in page["items"]:
            if source["scope"]["kind"] != "conversation" or source.get("fork") != policy:
                continue
            document_id = await storage.mint_id()
            identity: dict[str, Any] = {
                "id": document_id,
                "kind": source["kind"],
                "scope": {"kind": "conversation", "conversationId": child_conversation_id},
                "history": source["history"],
                "fork": source["fork"],
            }
            if "key" in source:
                identity["key"] = source["key"]
            address = address_id(identity)
            if address in copied_addresses:
                member = identity["kind"] if "key" not in identity else f"{identity['kind']}/{identity['key']}"
                raise ValueError(f"Fork selects multiple source documents for {member}")
            copied_addresses.add(address)
            copies.append({"record": identity, "source": {"id": source["id"], "at": at}})
        cursor = page.get("next")
        if cursor is None:
            break


__all__ = ["prepare_fork_document_copies"]

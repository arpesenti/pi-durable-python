"""Fork document copy selection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from ..context import Context
from ..documents import address_id
from ..ids import ConversationId, DocumentId, EntryId
from ..types import Cursor, DocumentCopySource, DocumentCreate, Storage

_SCAN_PAGE_SIZE = 256


@dataclass(frozen=True, slots=True)
class ForkDocumentCopy:
    """One definition-free document copy to create with a forked conversation."""

    record: DocumentCreate
    source: DocumentCopySource


async def prepare_fork_document_copies(
    storage: Storage,
    parent_conversation_id: ConversationId,
    at: EntryId,
    child_conversation_id: ConversationId,
    context: Context,
) -> list[ForkDocumentCopy]:
    """Select every persisted conversation document copied by one fork."""

    entry = await storage.entry(parent_conversation_id, at, context)
    if entry is None:
        raise ValueError(f"Entry {at} is not visible from conversation {parent_conversation_id}")
    copies: list[ForkDocumentCopy] = []
    copied_addresses: set[str] = set()
    await _collect_copies(
        storage,
        {"kind": "conversation", "conversationId": entry["entry"]["conversationId"]},
        entry["commitSeq"],
        "asOf",
        child_conversation_id,
        copies,
        copied_addresses,
        context,
    )
    await _collect_copies(
        storage,
        {"kind": "conversation", "conversationId": parent_conversation_id},
        "current",
        "current",
        child_conversation_id,
        copies,
        copied_addresses,
        context,
    )
    return copies


async def _collect_copies(
    storage: Storage,
    scope: dict[str, Any],
    at: int | str,
    policy: str,
    child_conversation_id: ConversationId,
    copies: list[ForkDocumentCopy],
    copied_addresses: set[str],
    context: Context,
) -> None:
    cursor: Cursor | None = None
    while True:
        page = await storage.scan_documents(
            {"scope": cast(Any, scope), "at": cast(Any, at)},
            _SCAN_PAGE_SIZE,
            cursor,
            context,
        )
        for source in page["items"]:
            if source["scope"]["kind"] != "conversation" or source.get("fork") != policy:
                continue
            id = cast(DocumentId, await storage.mint_id())
            record: dict[str, Any] = {
                "id": id,
                "kind": source["kind"],
                "scope": {"kind": "conversation", "conversationId": child_conversation_id},
                "history": source.get("history"),
                "fork": source.get("fork"),
            }
            if "key" in source:
                record["key"] = source["key"]
            created = cast(DocumentCreate, record)
            copy_address = address_id(cast(Any, created))
            if copy_address in copied_addresses:
                member = created["kind"] if "key" not in created else f"{created['kind']}/{created['key']}"
                raise ValueError(f"Fork selects multiple source documents for {member}")
            copied_addresses.add(copy_address)
            copies.append(
                ForkDocumentCopy(
                    record=created,
                    source={"id": source["id"], "at": cast(Any, at)},
                )
            )
        if "next" not in page:
            break
        cursor = page["next"]


__all__ = ["ForkDocumentCopy", "prepare_fork_document_copies"]

"""Translated v1.3.0 ``memory-storage`` suite.

Every storage conformance case runs against a fresh ``MemoryStorage`` through the public
``create_storage_conformance`` seam; the prepared-commit case is the suite's own local case.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from storage_support import expect

from pi_durable import MemoryStorage
from pi_durable.context import BACKGROUND_CONTEXT as context
from pi_durable.ids import ROOT_CONVERSATION_ID, EntryId
from pi_durable.testing import (
    StorageConformanceCase,
    StorageConformanceOptions,
    create_expect_assertions,
    create_storage_conformance,
)


async def _with_memory_storage(use: Callable[[Any], Awaitable[None]]) -> None:
    await use(MemoryStorage())


CASES: list[StorageConformanceCase] = create_storage_conformance(
    StorageConformanceOptions(
        assertions=create_expect_assertions(expect), with_storage=_with_memory_storage
    )
)


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_storage_conformance(case: StorageConformanceCase) -> None:
    await case.run()


async def test_does_not_expose_retained_state_through_a_prepared_commit() -> None:
    storage = MemoryStorage()
    await storage.commit(
        [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
    )
    entry_id = EntryId(2)
    prepared = storage.prepare_commit(
        [
            {
                "type": "entry",
                "value": {
                    "id": entry_id,
                    "conversationId": ROOT_CONVERSATION_ID,
                    "kind": "test",
                    "data": {"nested": [1]},
                },
            }
        ]
    )
    exposed = prepared.writes[0]
    assert exposed["type"] == "entry"
    with pytest.raises(TypeError):
        exposed["value"]["data"]["nested"].append(2)

    assert prepared.apply() == 2
    assert prepared.apply() == 2
    stored = await storage.entry(entry_id, context)
    assert stored is not None
    expect(stored["entry"]["data"]).to_equal({"nested": [1]})


async def test_applies_operation_payloads_without_aliasing_them_into_the_document() -> None:
    """A payload set or spliced by one operation is addressable by later operations."""

    storage = MemoryStorage()
    await storage.commit(
        [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
    )
    document_id = await storage.mint_id()
    payload = {"nested": {"count": 1}}
    appended = [{"value": 1}]
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": {
                    "id": document_id,
                    "kind": "aliasing",
                    "scope": {"kind": "session"},
                },
                "content": {"kind": "base", "version": 1, "value": {"items": []}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": document_id,
                "content": {
                    "kind": "delta",
                    "version": 1,
                    "ops": [
                        ["s", ["payload"], payload],
                        ["s", ["payload", "nested", "count"], 2],
                        ["p", ["items"], 0, 0, appended],
                        ["s", ["items", 0, "value"], 3],
                    ],
                },
            }
        ],
        context,
    )

    payload["nested"]["count"] = 99
    appended[0]["value"] = 99
    document = await storage.document(document_id, "current", context)
    assert document is not None
    assert document["value"] == {
        "payload": {"nested": {"count": 2}},
        "items": [{"value": 3}],
    }

"""Translated v1.5.0 ``memory-storage`` suite.

Every storage conformance case runs against a fresh ``MemoryStorage`` through the public
``create_storage_conformance`` seam; the prepared-commit case, the aliasing case, and the case
that delivers the receipts of the batches applied before one whose application throws are the
suite's own local cases.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import pytest
from storage_support import expect

from pi_durable import MemoryStorage, PreparedMemoryCommit
from pi_durable.context import BACKGROUND_CONTEXT as context
from pi_durable.ids import ROOT_CONVERSATION_ID, ConversationId, EntryId, Seq
from pi_durable.testing import (
    StorageConformanceCase,
    StorageConformanceOptions,
    commit_writes,
    create_expect_assertions,
    create_storage_conformance,
)
from pi_durable.types import StorageWrite


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


def _entry(id: int, conversation_id: ConversationId, kind: str = "message") -> Any:
    return {"id": id, "conversationId": conversation_id, "kind": kind}


class _FailingApply(MemoryStorage):
    """A memory storage whose prepared commit for one sequence throws when it is applied."""

    def __init__(self, failure: BaseException) -> None:
        super().__init__()
        self.fail_at: Seq | None = None
        self._failure = failure

    def prepare_commit(
        self, writes: Sequence[StorageWrite], seq: Seq | None = None
    ) -> PreparedMemoryCommit:
        prepared = super().prepare_commit(writes, seq)
        if prepared.seq != self.fail_at:
            return prepared

        def apply() -> Seq:
            raise self._failure

        return PreparedMemoryCommit(prepared.seq, prepared.writes, apply)


async def test_delivers_the_receipts_of_the_batches_applied_before_one_whose_apply_throws() -> None:
    """What was applied before the failure is stored, so its receipts stand and the call rejects."""

    failure = RuntimeError("apply failed")
    storage = _FailingApply(failure)
    root_seq = await commit_writes(
        storage, [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
    )
    first_id = EntryId(await storage.mint_id())
    second_id = EntryId(await storage.mint_id())
    storage.fail_at = Seq(root_seq + 2)
    receipts: list[tuple[int, Seq]] = []

    def receive(index: int, seq: Seq) -> None:
        receipts.append((index, seq))

    batches: list[list[StorageWrite]] = [
        [{"type": "entry", "value": _entry(first_id, ROOT_CONVERSATION_ID)}],
        [{"type": "entry", "value": _entry(second_id, ROOT_CONVERSATION_ID)}],
    ]

    with pytest.raises(RuntimeError) as raised:
        await storage.commit_batches(batches, receive, context)

    assert raised.value is failure
    assert receipts == [(0, root_seq + 1)]
    first = await storage.entry(first_id, context)
    assert first is not None
    assert first["commitSeq"] == root_seq + 1
    assert await storage.entry(second_id, context) is None

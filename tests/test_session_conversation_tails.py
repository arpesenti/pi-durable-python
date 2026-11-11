"""Translated upstream ``test/session-conversation-tails.test.ts`` (v1.5.0) case for case.

Upstream ``src/session/conversation-tails.ts`` keeps, per recently used conversation, the newest
entry ID and the newest head marker, bounded by least-recent use, kept current from the Session's
own commits. All five ``it()`` cases of the upstream suite are translated here, same setup, same
injections, same expected values, in the upstream order. Where a case differs from upstream:
``freezes the markers it keeps`` asserts the frozenness of the marker the tails keep, because the
port's ``freeze_json`` keeps a detached frozen copy where upstream's ``freeze`` freezes the
handed-in record in place.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from pi_durable.ids import ConversationId, EntryId, id_from_number
from pi_durable.session.conversation_tails import ConversationTails
from pi_durable.types import ConversationWrite, EntryWrite


def conversation(id: int) -> ConversationId:
    return id_from_number(id)


def entry_id(id: int) -> EntryId:
    return id_from_number(id)


def entry(id: int, conversation_id: int, head: int | None = None) -> EntryWrite:
    value: dict[str, Any] = {"id": id, "conversationId": conversation_id, "kind": "note"}
    if head is not None:
        value["head"] = entry_id(head)
    return cast(EntryWrite, {"type": "entry", "value": value})


async def never() -> None:
    raise RuntimeError("read")


async def test_reads_once_then_follows_the_commits() -> None:
    """Upstream ``test/session-conversation-tails.test.ts:22``."""

    tails = ConversationTails()
    reads = 0

    async def read() -> EntryId:
        nonlocal reads
        reads += 1
        return entry_id(5)

    assert await tails.latest(conversation(1), read) == 5
    assert await tails.latest(conversation(1), read) == 5
    assert reads == 1
    tails.apply([entry(9, 1)])
    assert await tails.latest(conversation(1), never) == 9


async def test_knows_a_new_root_conversation_is_empty_and_a_fork_only_once_read_or_written() -> None:
    """Upstream ``test/session-conversation-tails.test.ts:36``."""

    tails = ConversationTails()
    tails.apply(
        [
            cast(ConversationWrite, {"type": "conversation", "value": {"id": conversation(2)}}),
            cast(
                ConversationWrite,
                {
                    "type": "conversation",
                    "value": {
                        "id": conversation(3),
                        "parent": {"conversationId": conversation(2), "at": entry_id(4)},
                    },
                },
            ),
        ]
    )
    assert await tails.latest(conversation(2), never) is None
    assert await tails.marker(conversation(2), None, never) is None

    async def read_entry_id_4() -> EntryId:
        return entry_id(4)

    assert await tails.latest(conversation(3), read_entry_id_4) == 4


async def test_answers_a_cutoff_from_the_newest_marker_only_at_or_after_it() -> None:
    """Upstream ``test/session-conversation-tails.test.ts:50``."""

    tails = ConversationTails()
    tails.apply(
        [
            cast(ConversationWrite, {"type": "conversation", "value": {"id": conversation(1)}}),
            entry(10, 1),
            entry(11, 1, head=10),
        ]
    )
    marker = await tails.marker(conversation(1), None, never)
    assert marker is not None and marker["id"] == 11
    at_or_after = await tails.marker(conversation(1), entry_id(12), never)
    assert at_or_after is not None and at_or_after["id"] == 11

    # An older cutoff may have an older marker: read, and not kept.
    reads = 0

    async def older() -> None:
        nonlocal reads
        reads += 1
        return None

    assert await tails.marker(conversation(1), entry_id(10), older) is None
    assert await tails.marker(conversation(1), entry_id(10), older) is None
    assert reads == 2
    kept = await tails.marker(conversation(1), None, never)
    assert kept is not None and kept["id"] == 11


def test_keeps_a_bounded_number_of_conversations_fewer_with_markers() -> None:
    """Upstream ``test/session-conversation-tails.test.ts:68``."""

    tails = ConversationTails()
    for id in range(1, 5001):
        tails.apply([entry(2 * id, id), entry(2 * id + 1, id, head=2 * id)])
    assert tails.size == 4096
    assert tails.markers == 256


async def test_freezes_the_markers_it_keeps() -> None:
    """Upstream ``test/session-conversation-tails.test.ts:75``.

    Upstream's ``freeze`` freezes the handed-in record in place; the port's ``freeze_json`` keeps
    a detached frozen copy, so the assertion is on the marker the tails keep, not the write.
    """

    tails = ConversationTails()
    write = entry(2, 1, head=1)
    tails.apply([write])
    kept = await tails.marker(conversation(1), None, never)
    assert kept is not None and kept is not write["value"]
    with pytest.raises(TypeError, match="immutable"):
        kept["head"] = entry_id(2)

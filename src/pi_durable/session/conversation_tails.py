"""The newest entry and newest head marker of each recently used conversation.

Port of v1.5.0 ``src/session/conversation-tails.ts``. Kept current from the Session's own commits:
every entry in that Session's Storage is written by one of its commits, so a value read from Storage
stays valid until the next entry of that conversation, and a read of the newest entry of a
conversation whose entry this Session just committed costs no Storage read at all.

Two facts make this sound. Read only on the Session line, after touching the conversation's appends,
so no commit that appends to it is unpublished. And the newest entry visible from a conversation is
its own newest, or for a fork without entries of its own its parent's entry at the fork point: entry
IDs grow, so a conversation's own new entry is always the newest.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar, cast

from ..ids import ConversationId, EntryId
from ..json import freeze_json
from ..types import EntryWithHead, StorageWrite

_T = TypeVar("_T")

#: Conversations whose newest entry ID is kept: IDs only, so many.
LATEST_CAPACITY = 4096

#: Conversations whose newest head marker is kept: whole entries (a compaction summary is one), so fewer.
MARKER_CAPACITY = 256


class ConversationTails:
    """Newest entry ID and newest head marker per conversation, bounded by least-recent use.

    Kept markers are frozen: entry records are immutable.
    """

    def __init__(self) -> None:
        #: Newest entry ID per conversation, ``None`` for none.
        self._latest: dict[ConversationId, EntryId | None] = {}
        #: Newest head marker per conversation, ``None`` for none.
        self._markers: dict[ConversationId, EntryWithHead | None] = {}

    async def latest(
        self,
        conversation_id: ConversationId,
        read: Callable[[], Awaitable[EntryId | None]],
    ) -> EntryId | None:
        """The newest entry ID visible from ``conversation_id``, read with ``read`` when not known."""

        known = _use(self._latest, conversation_id)
        if known is not _MISSING:
            return known
        latest = await read()
        _put(self._latest, conversation_id, latest, LATEST_CAPACITY)
        return latest

    async def marker(
        self,
        conversation_id: ConversationId,
        at_or_before: EntryId | None,
        read: Callable[[], Awaitable[EntryWithHead | None]],
    ) -> EntryWithHead | None:
        """The newest visible head marker at or before ``at_or_before`` (any when ``None``).

        The newest marker answers every cutoff at or after it, and no marker answers every cutoff.
        """

        found = _use(self._markers, conversation_id)
        if found is None:
            return None
        if isinstance(found, dict) and (at_or_before is None or at_or_before >= found["id"]):
            return cast(EntryWithHead, found)
        latest_known = self._latest.get(conversation_id, _MISSING)
        marker = await read()
        # Only a read that no newer entry can change: no cutoff, or one at or after the newest entry.
        if at_or_before is None or (
            latest_known is not _MISSING and (latest_known or 0) <= at_or_before  # type: ignore[operator]
        ):
            kept = None if marker is None else cast(EntryWithHead, freeze_json(marker))
            _put(self._markers, conversation_id, kept, MARKER_CAPACITY)
        return marker

    def peek(self, conversation_id: ConversationId) -> EntryId | None:
        """The newest entry ID of ``conversation_id`` if known, without reading.

        ``None`` means unknown: the two cases are not told apart, because nothing that asks treats
        "no entries" differently from "not known yet".
        """

        return self._latest.get(conversation_id)

    def apply(self, writes: Sequence[StorageWrite]) -> None:
        """At a commit's publication: apply a durable batch."""

        for write in writes:
            if write["type"] == "conversation":
                # A new root conversation sees no entries; a fork sees its parent's up to the fork point.
                if write["value"].get("parent") is not None:
                    continue
                _put(self._latest, write["value"]["id"], None, LATEST_CAPACITY)
                _put(self._markers, write["value"]["id"], None, MARKER_CAPACITY)
            elif write["type"] == "entry":
                entry = write["value"]
                _put(self._latest, entry["conversationId"], entry["id"], LATEST_CAPACITY)
                if entry.get("head") is not None:
                    _put(  # type: ignore[misc]
                        self._markers,
                        entry["conversationId"],
                        freeze_json(entry),
                        MARKER_CAPACITY,
                    )

class _Missing:
    """Marker for a conversation whose newest entry is not known, told apart from "no entries"."""


_MISSING = _Missing()


def _use(map_: dict[ConversationId, _T], key: ConversationId) -> Any:
    """``map_``'s value for ``key``, made the most recently used; ``_MISSING`` when it holds none."""

    if key not in map_:
        return _MISSING
    value = map_.pop(key)
    map_[key] = value
    return value


def _put(map_: dict[ConversationId, _T], key: ConversationId, value: _T, capacity: int) -> None:
    """Set ``map_``'s value for ``key`` as the most recently used, dropping the oldest past ``capacity``."""

    map_.pop(key, None)
    map_[key] = value
    while len(map_) > capacity:
        del map_[next(iter(map_))]


__all__ = [
    "LATEST_CAPACITY",
    "MARKER_CAPACITY",
    "ConversationTails",
]

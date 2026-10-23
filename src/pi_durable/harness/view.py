"""Conversation view mount: the active transcript and the built-in documents (spec §9.3).

The Harness keeps at most one mount per conversation. Its first observer builds the revision on
the Session line from committed state, and the mount advances from the Session's commit
publications, so every delivered frame is already durable. Document operations keep their exact
shape under the mount path; entries keep the raw active-transcript shape of ``ContextView``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, TypedDict, cast

from ..context import Context, without_abort_signal
from ..delta import Op, apply_immutable
from ..ids import ConversationId, DocumentId
from ..json import JsonObject
from ..session.observation import CommittedState, CommittedWatch
from ..session.session import SessionImpl, contained
from ..types import (
    CommitPublication,
    ConversationRecord,
    EntryRecord,
    Storage,
)
from .agent import AgentDoc
from .context import capture_context_bounds, derive_context
from .inbox import InboxDoc
from .live import LiveDoc
from .provider import ProviderDoc
from .usage import UsageDoc
from .util import cancellation_error, closed_error


class ConversationView(TypedDict):
    """Structural mount of one conversation's active transcript and built-in documents.

    ``conversation`` is the immutable record; ``entries`` is the head marker followed by the
    non-head entries from its head; ``docs`` holds ``pi.agent``, ``pi.live``, ``pi.inbox``,
    ``pi.provider``, and ``pi.usage`` keyed by kind, with absent documents absent.
    """

    conversation: ConversationRecord
    entries: list[EntryRecord]
    docs: dict[str, JsonObject]


MOUNTED: tuple[Any, ...] = (
    AgentDoc,
    LiveDoc,
    InboxDoc,
    ProviderDoc,
    UsageDoc,
)
"""The built-in conversation documents every view mounts."""

MOUNTED_KINDS = frozenset(token.definition.kind for token in MOUNTED)


@dataclass(slots=True)
class Mount:
    """One conversation's current revision, mounted incarnations, and observers."""

    value: ConversationView
    docs: dict[str, tuple[DocumentId, int]]
    observers: set[Any] = field(default_factory=set)


class ConversationViews:
    """The Harness's conversation view mounts, one per conversation while observed."""

    def __init__(self, session: SessionImpl, storage: Storage) -> None:
        self._session = session
        self._storage = storage
        self._mounts: dict[ConversationId, Mount] = {}
        self._closed = False
        session.observe_commits(self._on_publication)
        session.subscribe_close(self._on_close)

    def report(self, error: BaseException) -> None:
        self._session.report(error)

    async def state(self, id: ConversationId, context: Context) -> CommittedState:
        """A disposable read-only state of the view."""

        return await self.attach(id, _state_observer, context)

    async def watch(self, id: ConversationId, context: Context) -> CommittedWatch:
        """A serialized exact-frame watch of the view; cancelling ``context`` stops it."""

        observer = await self.attach(id, _watch_observer, context)
        signal = context.abort_signal
        if signal is not None and signal.aborted:
            observer.cancel()
            raise cancellation_error(signal)
        if signal is not None:
            observer.observe_cancellation(signal)
        return observer

    async def attach(
        self,
        id: ConversationId,
        create: Callable[
            [ConversationView, Callable[[], None], Storage, ConversationViews],
            Any,
        ],
        context: Context,
    ) -> Any:
        """Register an observer created from the current revision, atomically on the Session line."""

        async def job() -> Any:
            self._session.assert_usable()
            mount = self._mounts.get(id)
            if mount is None:
                mount = await self._build(id, context)
            detached = False

            def detach() -> None:
                nonlocal detached
                if detached:
                    return
                detached = True
                mount.observers.discard(observer)
                if not mount.observers and self._mounts.get(id) is mount:
                    del self._mounts[id]

            observer = create(mount.value, detach, self._storage, self)
            if asyncio.iscoroutine(observer):
                observer = await observer
            # Close or cancellation may begin while the mount hydrates; register nothing then.
            if self._closed:
                raise closed_error(self._session)
            self._session.assert_usable()
            signal = context.abort_signal
            if signal is not None and signal.aborted:
                raise cancellation_error(signal)
            self._mounts[id] = mount
            mount.observers.add(observer)
            return observer

        return await self._session.read_on_line(job)

    async def _build(self, id: ConversationId, context: Context) -> Mount:
        conversation = await self._storage.conversation(id, context)
        if conversation is None:
            raise RuntimeError(f"Conversation {id} does not exist")
        bounds = await capture_context_bounds(self._storage, id, context)
        derived = await derive_context(self._storage, id, bounds, context)
        entries = list(derived["entries"])
        docs: dict[str, JsonObject] = {}
        incarnations: dict[str, tuple[DocumentId, int]] = {}
        for token in MOUNTED:
            loaded = await self._session._document_on_line(token, id, context)
            if loaded is None:
                continue
            kind = token.definition.kind
            docs[kind] = loaded.value
            incarnations[kind] = (loaded.record["id"], loaded.value_version)
        value: ConversationView = {
            "conversation": conversation,
            "entries": entries,
            "docs": docs,
        }
        return Mount(value=value, docs=incarnations)

    def _on_publication(self, publication: CommitPublication, context: Context) -> None:
        for id, mount in list(self._mounts.items()):
            advance_mount(id, mount, publication, context, self.report)

    def _on_close(self) -> None:
        self._closed = True
        failure = self._session.failure
        for mount in list(self._mounts.values()):
            for observer in list(mount.observers):
                contained(partial(observer.close_session, failure), self.report)
        self._mounts.clear()


def _state_observer(
    value: ConversationView,
    release: Callable[[], None],
    _storage: Storage,
    views: ConversationViews,
) -> CommittedState:
    return CommittedState(value, release, views.report)


def _watch_observer(
    value: ConversationView,
    release: Callable[[], None],
    _storage: Storage,
    views: ConversationViews,
) -> CommittedWatch:
    return CommittedWatch(value, release, views.report)


def advance_mount(
    id: ConversationId,
    mount: Mount,
    publication: CommitPublication,
    context: Context,
    report: Callable[[BaseException], None],
) -> None:
    """Derive the mount's operations from one publication, apply them, and publish the revision."""

    doc_ops: list[Op] = []
    entry_ops: list[Op] = []
    entries: list[EntryRecord] = mount.value["entries"]
    docs = mount.value["docs"]
    # Entry writes are published in ID order.
    for change in publication["changes"]:
        if change["type"] == "entry" and change["value"]["conversationId"] == id:
            entry = change["value"]
            target = entry.get("head")
            if target is None:
                entry_ops.append(["p", ["entries"], len(entries), 0, [entry]])
                entries = [*entries, entry]
                continue
            # A head marker keeps the non-head entries from its head, always a suffix, and goes in front.
            kept = next(
                (
                    index
                    for index, candidate in enumerate(entries)
                    if "head" not in candidate and candidate["id"] >= target
                ),
                len(entries),
            )
            entry_ops.append(["p", ["entries"], 0, kept, [entry]])
            entries = [entry, *entries[kept:]]
            continue
        if change["type"] != "document" or change["conversationId"] != id:
            continue
        kind = change["record"]["kind"]
        if kind not in MOUNTED_KINDS or "key" in change["record"]:
            continue
        path = ["docs", kind]
        mounted = mount.docs.get(kind)
        if change["value"] is None:
            if mounted is None or mounted[0] != change["record"]["id"]:
                continue
            del mount.docs[kind]
            docs = {current: value for current, value in docs.items() if current != kind}
            doc_ops.append(["d", path])
        elif (
            mounted is not None
            and mounted[0] == change["record"]["id"]
            and mounted[1] == change["version"]
        ):
            current = docs[kind]
            docs = {
                **docs,
                kind: cast(JsonObject, apply_immutable(current, change["ops"])),
            }
            doc_ops.extend(_prefixed(op, path) for op in change["ops"])
        else:
            if change["value"] is None or change["version"] is None:
                continue
            mount.docs[kind] = (change["record"]["id"], change["version"])
            docs = {**docs, kind: change["value"]}
            doc_ops.append(["s", path, change["value"]])

    ops = [*doc_ops, *entry_ops]
    before = mount.value
    after = before
    frame_context = without_abort_signal(context)
    if ops:
        value: ConversationView = {
            "conversation": before["conversation"],
            "entries": entries,
            "docs": docs,
        }
        mount.value = value
        after = value
        for observer in list(mount.observers):
            advance = getattr(observer, "advance_frame", None) or getattr(
                observer, "advance", None
            )
            if advance is not None:
                contained(partial(advance, value, ops, frame_context), report)
    # Every publication reaches event adapters, including one that changes no mounted state.
    for observer in list(mount.observers):
        publication_listener = getattr(observer, "publication", None)
        if publication_listener is not None:
            contained(
                partial(
                    publication_listener,
                    before,
                    after,
                    ops,
                    publication,
                    frame_context,
                ),
                report,
            )


def _prefixed(op: Op, prefix: list[Any]) -> Op:
    """Move ``op`` under ``prefix``; a root replacement becomes a set of the prefix."""

    verb = op[0]
    if verb == "r":
        return ["s", prefix, op[1]]
    path = [*prefix, *op[1]]
    if verb in ("s", "a", "t"):
        return [verb, path, op[2]]
    if verb == "d":
        return ["d", path]
    if verb == "m":
        return ["m", path, op[2]]
    if verb == "p":
        return ["p", path, op[2], op[3], op[4]]
    raise TypeError(f"unknown op verb: {verb}")


__all__ = [
    "ConversationView",
    "ConversationViews",
    "MOUNTED",
    "MOUNTED_KINDS",
    "advance_mount",
]

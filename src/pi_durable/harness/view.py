"""Conversation view mount: a bounded entry window and the built-in documents (spec §9.3).

The Harness keeps at most one mount per conversation. Its first consumer builds the mounted
documents on the Session line from committed state, and the mount advances from the Session's
commit publications, so every delivered change is already durable. Each attached consumer holds its
own window over the transcript — the fork-aware history in entry ID order, head markers kept where
they were written — so two consumers of one conversation share the document values and never the
entries. A watch of the mount delivers those publications coalesced, like every other watch.
Document operations keep their exact shape under the mount path; entries keep the raw
``EntryRecord`` shape, which is not the model's transcript: ``Conversation.context()`` derives that.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, NotRequired, TypedDict, cast

from ..context import Context, without_abort_signal
from ..delta import Op, apply_immutable
from ..ids import ConversationId, DocumentId, EntryId
from ..json import JsonObject
from ..session.observation import CommittedWatch
from ..session.session import SessionImpl, contained
from ..types import (
    CommitPublication,
    ConversationRecord,
    Cursor,
    EntryQuery,
    EntryRecord,
    Storage,
    to_document_commit_change,
    watch_value_call,
)
from .agent import AgentDoc
from .inbox import InboxDoc
from .live import LiveDoc
from .provider import ProviderDoc
from .usage import UsageDoc
from .util import cancellation_error, closed_error, require_integer


class ConversationView(TypedDict):
    """Structural mount of one conversation's entry window and built-in documents.

    ``conversation`` is the immutable record; ``entries`` is one consumer's window over the
    transcript, in entry ID order with head markers in place — not the active transcript, which
    ``Conversation.context()`` derives; ``before`` names the entry just before the window and is a
    key only when the window does not start the transcript; ``docs`` holds ``pi.agent``,
    ``pi.live``, ``pi.inbox``, ``pi.provider``, and ``pi.usage`` keyed by kind, with absent
    documents absent, and is shared by every consumer of the mount.
    """

    conversation: ConversationRecord
    entries: list[EntryRecord]
    before: NotRequired[EntryId]
    docs: dict[str, JsonObject]


# ``from`` is a Python keyword, so the window is declared with the functional syntax that keeps the
# public key the specification names (spec §9.3).
ConversationWindow = TypedDict(
    "ConversationWindow",
    {"from": NotRequired[EntryId], "min": NotRequired[int], "max": NotRequired[int]},
    total=False,
)
"""Which entries one consumer's window holds: its start, its floor, and its ceiling.

``from`` selects only the first value, inclusive; ``min`` defaults to fifty and ``max`` to twice it.
A bound that is not an integer, a ``min`` below one, a ``max`` below ``min``, or a ``from`` that is
not an entry ID is refused.
"""

DEFAULT_WINDOW_MIN = 50
"""``ConversationWindow.min`` when the consumer gives none."""

SCAN_PAGE_SIZE = 256
"""How far a window read scans at a time, so an attachment never reads the whole transcript."""


MOUNTED: tuple[Any, ...] = (
    AgentDoc,
    LiveDoc,
    InboxDoc,
    ProviderDoc,
    UsageDoc,
)
"""The built-in conversation documents every view mounts."""

MOUNTED_KINDS = frozenset(token.definition.kind for token in MOUNTED)


@dataclass(frozen=True, slots=True)
class WindowBounds:
    """One consumer's window with its defaults applied."""

    from_: EntryId | None
    min: int
    max: int


def resolve_window(window: ConversationWindow | None) -> WindowBounds:
    """``window`` with its defaults; a bound that is not an integer, a ``min`` below one, a ``max``
    below ``min``, or a ``from`` that is not an entry ID raises :class:`ValueError`.
    """

    given = window or {}
    minimum = (
        DEFAULT_WINDOW_MIN
        if given.get("min") is None
        else require_integer(given["min"], "Window min")
    )
    if minimum < 1:
        raise ValueError(f"Window min must be an integer >= 1, got {given['min']!r}")
    maximum = (
        2 * minimum if given.get("max") is None else require_integer(given["max"], "Window max")
    )
    if maximum < minimum:
        raise ValueError(f"Window max must be an integer >= min ({minimum}), got {given['max']!r}")
    start = None if given.get("from") is None else require_integer(given["from"], "Window from")
    return WindowBounds(None if start is None else cast(EntryId, start), minimum, maximum)


@dataclass(slots=True, eq=False)
class Windowed:
    """One attached consumer of a mount: its own bounds and the revision it was last given."""

    min: int
    max: int
    value: ConversationView
    observer: Any


@dataclass(slots=True)
class Mount:
    """One conversation's mount: the document values every consumer shares, and their windows."""

    conversation: ConversationRecord
    docs: dict[str, JsonObject]
    incarnations: dict[str, tuple[DocumentId, int]]
    observers: set[Windowed] = field(default_factory=set)


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

    async def watch(
        self, id: ConversationId, context: Context, window: ConversationWindow | None = None
    ) -> CommittedWatch:
        """A serialized coalesced watch of the view, with a window of its own; aborting ``context``
        ends it ``aborted``.
        """

        observer = await self.attach(id, window, _watch_observer, context)
        signal = context.abort_signal
        if signal is not None and signal.aborted:
            observer.abort_acquisition()
            raise cancellation_error(signal)
        if signal is not None:
            observer.observe_cancellation(signal)
        return observer

    async def attach(
        self,
        id: ConversationId,
        window: ConversationWindow | None,
        create: Callable[
            [ConversationView, Callable[[], None], Storage, ConversationViews],
            Any,
        ],
        context: Context,
    ) -> Any:
        """Register one consumer, created from its own window's first revision, atomically on the
        Session line.
        """

        bounds = resolve_window(window)

        async def job() -> Any:
            self._session.assert_usable()
            mount = self._mounts.get(id)
            if mount is None:
                mount = await self._build(id, context)
            entries, before = await load_window(self._storage, id, bounds, context)
            value = mounted_view(mount.conversation, entries, before, mount.docs)
            holder: dict[str, Windowed] = {}
            detached = False

            def detach() -> None:
                nonlocal detached
                if detached:
                    return
                detached = True
                windowed = holder.get("windowed")
                if windowed is not None:
                    mount.observers.discard(windowed)
                if not mount.observers and self._mounts.get(id) is mount:
                    del self._mounts[id]

            observer = create(value, detach, self._storage, self)
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
            windowed = Windowed(min=bounds.min, max=bounds.max, value=value, observer=observer)
            holder["windowed"] = windowed
            mount.observers.add(windowed)
            return observer

        return await self._session.read_on_line(job)

    async def _build(self, id: ConversationId, context: Context) -> Mount:
        conversation = await self._storage.conversation(id, context)
        if conversation is None:
            raise RuntimeError(f"Conversation {id} does not exist")
        docs: dict[str, JsonObject] = {}
        incarnations: dict[str, tuple[DocumentId, int]] = {}
        for token in MOUNTED:
            loaded = await self._session._document_on_line(token, id, context)
            if loaded is None:
                continue
            kind = token.definition.kind
            docs[kind] = loaded.value
            incarnations[kind] = (loaded.record["id"], loaded.value_version)
        return Mount(conversation=conversation, docs=docs, incarnations=incarnations)

    def _on_publication(self, publication: CommitPublication, context: Context) -> None:
        for id, mount in list(self._mounts.items()):
            advance_mount(id, mount, publication, context, self.report)

    def _on_close(self) -> None:
        self._closed = True
        failure = self._session.failure
        for mount in list(self._mounts.values()):
            for windowed in list(mount.observers):
                contained(partial(windowed.observer.close_session, failure), self.report)
        self._mounts.clear()


def mounted_view(
    conversation: ConversationRecord,
    entries: list[EntryRecord],
    before: EntryId | None,
    docs: dict[str, JsonObject],
) -> ConversationView:
    """One view revision; ``before`` is a key only when the window does not start the transcript."""

    value: ConversationView = {"conversation": conversation, "entries": entries, "docs": docs}
    if before is not None:
        value["before"] = before
    return value


async def load_window(
    storage: Storage,
    conversation_id: ConversationId,
    bounds: WindowBounds,
    context: Context,
) -> tuple[list[EntryRecord], EntryId | None]:
    """A new consumer's window, read with bounded scans (spec §9.3).

    The entries from ``from`` when at most ``max`` lie at or after it, and the newest entry below
    ``from`` beside them; otherwise the newest ``min`` entries and the one before them, so ``before``
    shows a client the entries it missed. A ``from`` past the newest entry gives an empty window
    whose ``before`` is that newest entry.
    """

    if bounds.from_ is not None:
        since = await _scan_up_to(
            storage,
            {
                "conversationId": conversation_id,
                "minEntryId": bounds.from_,
                "order": "ascending",
            },
            bounds.max + 1,
            context,
        )
        if len(since) <= bounds.max:
            if bounds.from_ <= 1:
                return since, None
            previous = await storage.scan_entries(
                {"conversationId": conversation_id, "maxEntryId": cast(EntryId, bounds.from_ - 1)},
                1,
                None,
                context,
            )
            return since, (previous["items"][0]["id"] if previous["items"] else None)
    newest = await _scan_up_to(
        storage, {"conversationId": conversation_id}, bounds.min + 1, context
    )
    before = newest.pop()["id"] if len(newest) > bounds.min else None
    return list(reversed(newest)), before


async def _scan_up_to(
    storage: Storage, query: EntryQuery, limit: int, context: Context
) -> list[EntryRecord]:
    """Up to ``limit`` entries of ``query``, following cursors past short pages."""

    entries: list[EntryRecord] = []
    cursor: Cursor | None = None
    while len(entries) < limit:
        page = await storage.scan_entries(
            query, min(SCAN_PAGE_SIZE, limit - len(entries)), cursor, context
        )
        entries.extend(page["items"])
        cursor = page.get("next")
        if cursor is None:
            break
    return entries


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
    """Derive the mount's operations from one publication and hand each consumer the next revision
    of its own window.

    The document operations are derived once, applied once, and shared; each window then takes the
    appended entries and, past its ``max``, drops its oldest down to its ``min`` in one splice that
    also moves ``before``. One that throws is reported and the others still get theirs.
    """

    doc_ops: list[Op] = []
    appended: list[EntryRecord] = []
    docs = mount.docs
    # Entry writes are published in ID order, and a window is the history in that order: every
    # appended entry joins it, head markers included.
    for change in publication["changes"]:
        if change["type"] == "entry" and change["value"]["conversationId"] == id:
            appended.append(change["value"])
            continue
        if change["type"] != "document":
            continue
        settled = to_document_commit_change(change)
        if settled["type"] != "document" or settled["conversationId"] != id:
            continue
        kind = settled["record"]["kind"]
        if kind not in MOUNTED_KINDS or "key" in settled["record"]:
            continue
        path = ["docs", kind]
        mounted = mount.incarnations.get(kind)
        if settled["kind"] == "retire":
            if mounted is None or mounted[0] != settled["record"]["id"]:
                continue
            del mount.incarnations[kind]
            docs = {current: value for current, value in docs.items() if current != kind}
            doc_ops.append(["d", path])
        elif (
            mounted is not None
            and mounted[0] == settled["record"]["id"]
            and mounted[1] == settled["version"]
        ):
            current = docs[kind]
            docs = {
                **docs,
                kind: cast(JsonObject, apply_immutable(current, settled["ops"])),
            }
            doc_ops.extend(_prefixed(op, path) for op in settled["ops"])
        else:
            mount.incarnations[kind] = (settled["record"]["id"], settled["version"])
            docs = {**docs, kind: settled["value"]}
            doc_ops.append(["s", path, settled["value"]])

    frame_context = without_abort_signal(context)
    if not doc_ops and not appended:
        # A publication that touches nothing mounted creates no revision.
        for windowed in list(mount.observers):
            _publish(windowed, windowed.value, windowed.value, [], publication, frame_context, report)
        return
    if doc_ops:
        mount.docs = docs
    for windowed in list(mount.observers):
        previous = windowed.value
        entries = previous["entries"]
        before = previous.get("before")
        ops = list(doc_ops)
        if appended:
            ops.append(["p", ["entries"], len(entries), 0, list(appended)])
            entries = [*entries, *appended]
            if len(entries) > windowed.max:
                drop = len(entries) - windowed.min
                before = entries[drop - 1]["id"]
                entries = entries[drop:]
                ops.append(["p", ["entries"], 0, drop, []])
                ops.append(["s", ["before"], before])
        windowed.value = mounted_view(mount.conversation, entries, before, mount.docs)
        call = watch_value_call(windowed.value, ops)
        advance = getattr(windowed.observer, "advance", None)
        if advance is not None:
            contained(partial(advance, call, frame_context), report)
        # Every publication reaches event adapters, including one that changes no mounted state.
        _publish(windowed, previous, windowed.value, ops, publication, frame_context, report)


def _publish(
    windowed: Windowed,
    before: ConversationView,
    after: ConversationView,
    ops: list[Op],
    publication: CommitPublication,
    frame_context: Context,
    report: Callable[[BaseException], None],
) -> None:
    """Give one consumer the whole publication, on top of its own window's change."""

    publication_listener = getattr(windowed.observer, "publication", None)
    if publication_listener is None:
        return
    contained(
        partial(publication_listener, before, after, ops, publication, frame_context),
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
    "DEFAULT_WINDOW_MIN",
    "MOUNTED",
    "MOUNTED_KINDS",
    "ConversationView",
    "ConversationViews",
    "ConversationWindow",
    "advance_mount",
    "load_window",
    "resolve_window",
]

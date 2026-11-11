"""Deferred commits (``CommitPersistence``): what a deferred commit holds in memory, what stores it,
and what Storage and each kind of listener then sees.

Every case here is the upstream case of the same name from
``test/session-deferred-commits.test.ts`` (v1.5.0): same setup, same injections, same expected
values, in the upstream order. The authority for the behaviour is the shipped upstream source --
``src/session/session.ts`` (``#deferral``, ``#hold``, ``#storeHolds``, ``#release``,
``#armHoldTimer``), ``src/session/transaction.ts`` (``holdable``, ``adoptHeld``, ``takeHeld``,
``storeHeld``) and ``src/timers.ts`` -- beside ``docs/upstream/spec-1.5.0.md`` §4.1.

The two audiences are the ticket. A held commit reaches the Session's own followers -- its watches,
its document observers, and the internal commit listener -- when memory adopts it, and the host's
``observe_commits()`` observer only when Storage made it durable, the deadline's store included. The
deadline's store is itself admitted as a part of the writer's line, and it publishes to that host
observer alone: ``session.ts:1253`` says it outright, "Memory already has them, so only
``observeCommits()`` observers see it".

One upstream case is told with the host's GC instead of CPython's: ``retains a held revision only
until it is stored`` runs V8's ``gc()`` through ``setFlagsFromString("--expose-gc")``, which has no
Python counterpart. The same property -- the pinned revision outliving the hold and nothing longer --
is proved here by the collector's own object set, the way the watch suite already proves it
(``tests/test_session_watches.py``).
"""

from __future__ import annotations

import asyncio
import gc
from collections.abc import Callable
from typing import Any, cast

import pytest
from session_support import (
    ControlledStorage,
    context,
    create_conversation,
    flush,
    observe_commits_as_host,
)

from pi_durable import define_doc
from pi_durable.delta import MAX_PENDING_OPS, apply_immutable
from pi_durable.errors import ReadOnlySession
from pi_durable.json import JsonObject
from pi_durable.session.session import DocumentCacheSizes, SessionImpl
from pi_durable.types import (
    CommitOptions,
    CommitPublication,
    ConversationId,
    DocumentAddress,
    DocumentId,
    ObservedCommit,
    Storage,
    StorageWrite,
)


def _checkpoint(value: JsonObject, _ops: Any, _info: Any) -> bool:
    """``TextDoc``'s predicate: ``base`` selects a base, ``explode`` throws out of settlement."""

    if value.get("explode") is True:
        raise RuntimeError("checkpoint failed")
    return value.get("base") is True


TextDoc = define_doc(
    {
        "kind": "test.deferred.text",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"text": ""},
        "checkpoint_when": _checkpoint,
    }
)

#: ``TextDoc`` at version 2, which a reload migrates to.
TextDocV2 = define_doc(
    {
        "kind": "test.deferred.text",
        "version": 2,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"text": "", "migrated": True},
        "migrate": lambda value, _version: {**value, "migrated": True},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)

#: A document a deferred commit may not hold: other history and fork semantics.
RewindableDoc = define_doc(
    {
        "kind": "test.deferred.rewindable",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {"text": ""},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)

CurrentForkDoc = define_doc(
    {
        "kind": "test.deferred.current-fork",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "current",
        "initial": lambda: {"text": ""},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)

#: A document of the Session rather than a conversation: never holdable.
SessionTextDoc = define_doc(
    {
        "kind": "test.deferred.session",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"text": ""},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


class ArmedTimer:
    """One foreground timer the Session armed through the host's ``set_timer``.

    Upstream's manual ``SetTimer`` records the arming and hands back the disarm, and declines every
    background timer so commits evict. A case fires the recorded callbacks itself, so neither a wall
    clock nor a loop timer decides when a deadline's store runs, and ``armings`` counts how often the
    Session armed one at all -- the guard against a same-deadline re-arm firing twice.
    """

    def __init__(self, callback: Callable[[], None], delay_ms: float) -> None:
        self.callback = callback
        self.delay_ms = delay_ms
        self.disarmed = False
        self.fired = False

    def disarm(self) -> None:
        self.disarmed = True


class DeferredSession:
    """A Session over controlled storage with a manual clock and manual foreground timers.

    Background timers are declined, as upstream's support declines them, so documents evict at each
    commit. Both audiences are recorded: what ``observe_commits()`` was handed, and what the Session's
    own followers were.
    """

    def __init__(self, **options: Any) -> None:
        self.clock = {"now": 1_000}
        self.armings: list[ArmedTimer] = []
        self.storage = ControlledStorage()
        self.conversation_id: ConversationId = cast(ConversationId, 0)
        self.record_id: DocumentId = cast(DocumentId, 0)
        self.session = SessionImpl(
            cast(Storage, self.storage),
            lambda: self.clock["now"],
            lambda: options.get("retention_ms", 600_000),
            self._set_timer,
            False,
            lambda: options.get("persist_within_ms"),
        )
        # What the host's ``observe_commits()`` observer was handed: only what Storage made durable.
        self.stored: list[CommitPublication] = []
        observe_commits_as_host(self.session, lambda publication, _context: self.stored.append(publication))
        # What the Session's own followers were handed: every commit, held ones included.
        self.observed: list[ObservedCommit] = []
        self.session.observe_stored_commits(lambda publication, _context: self.observed.append(publication))

    def _set_timer(
        self, callback: Callable[[], None], delay_ms: float, *, background: bool = False
    ) -> Callable[[], None] | None:
        if background:
            # Declined, as upstream's support declines it: documents evict at each commit.
            return None
        timer = ArmedTimer(callback, delay_ms)
        self.armings.append(timer)
        return timer.disarm

    @property
    def armed(self) -> list[ArmedTimer]:
        """Timers armed and neither disarmed nor fired."""

        return [timer for timer in self.armings if not timer.disarmed and not timer.fired]

    async def open(self) -> DeferredSession:
        self.conversation_id = await create_conversation(self.session)

        async def touch(tx: Any) -> None:
            await tx.doc(TextDoc, self.conversation_id)

        await self.session.commit(touch, context)
        record = await self.storage.find_document(self.address, "current", context)
        assert record is not None
        self.record_id = record["id"]
        return self

    @property
    def address(self) -> DocumentAddress:
        return {
            "kind": TextDoc.definition.kind,
            "scope": {"kind": "conversation", "conversationId": self.conversation_id},
        }

    async def fire(self) -> None:
        """Fire every armed timer and let the line run what they queued."""

        for timer in self.armings:
            if timer.disarmed or timer.fired:
                continue
            timer.fired = True
            timer.callback()
        # The store a fired timer queued is a job on the line: this waits for the line to be reached.
        await self.session.read_on_line(_nothing)

    def mark(self) -> Callable[[], list[list[StorageWrite]]]:
        """Batches admitted to Storage from here on."""

        from_index = len(self.storage.commits)
        return lambda: self.storage.commits[from_index:]

    async def stored_text(self, id: DocumentId | None = None) -> JsonObject | None:
        """``TextDoc``'s value as Storage holds it."""

        stored = await self.storage.document(self.record_id if id is None else id, "current", context)
        return None if stored is None else stored["value"]

    @property
    def sizes(self) -> DocumentCacheSizes:
        return self.session.document_cache_sizes


async def _nothing(_storage: Any) -> None:
    return None


async def open_deferred(**options: Any) -> DeferredSession:
    return await DeferredSession(**options).open()


DEFERRED: CommitOptions = {"persistence": {"kind": "deferred"}}

IMMEDIATE: CommitOptions = {"persistence": {"kind": "immediate"}}


def within(within_ms: float) -> CommitOptions:
    return {"persistence": {"kind": "deferred", "within_ms": within_ms}}


async def append(
    opened: DeferredSession,
    text: str,
    options: CommitOptions | None = None,
    conversation_id: ConversationId | None = None,
) -> Any:
    """One commit appending ``text`` to the document; deferred unless the case says otherwise."""

    target = opened.conversation_id if conversation_id is None else conversation_id

    async def change(tx: Any) -> None:
        document = await tx.doc(TextDoc, target)
        document["text"] = f"{document['text']}{text}"

    return await opened.session.commit(change, context, DEFERRED if options is None else options)


def changes_of(batches: list[list[StorageWrite]]) -> list[dict[str, Any]]:
    """The document-change writes of ``batches``, in line order."""

    return [
        cast(dict[str, Any], write)
        for batch in batches
        for write in batch
        if write["type"] == "document.change"
    ]


# ── holding a deferred commit ───────────────────────────────────────────────


async def test_adopts_a_commit_only_changing_a_latest_initial_fork_conversation_document_unstored() -> None:
    """Upstream: "adopts a commit that only changes an existing latest, initial-fork conversation document
    in memory, unstored"."""

    opened = await open_deferred()
    admitted = len(opened.storage.commits)
    publications = len(opened.stored)
    await append(opened, "a")
    await append(opened, "b")
    assert len(opened.storage.commits) == admitted
    assert len(opened.stored) == publications
    assert await opened.session.snapshot(TextDoc, opened.conversation_id, context) == {"text": "ab"}
    assert await opened.stored_text() == {"text": ""}
    assert opened.sizes["held"] == 1
    assert [publication["kind"] for publication in opened.observed[-2:]] == ["memory", "memory"]


async def test_stores_a_deferred_commit_at_once_when_it_writes_anything_else() -> None:
    """Upstream: "stores a deferred commit at once when it writes anything else"."""

    opened = await open_deferred()
    session = opened.session

    async def stores(change: Callable[[Any], Any]) -> bool:
        admitted = len(opened.storage.commits)
        await session.commit(change, context, DEFERRED)
        return len(opened.storage.commits) > admitted

    # An entry next to the held document's change.
    async def with_entry(tx: Any) -> None:
        (await tx.doc(TextDoc, opened.conversation_id))["text"] += "x"
        await tx.append_entry(opened.conversation_id, {"kind": "test.note"})

    assert await stores(with_entry) is True
    # Creation, then other document semantics.
    async def create_rewindable(tx: Any) -> None:
        await tx.doc(RewindableDoc, opened.conversation_id)

    assert await stores(create_rewindable) is True

    async def change_rewindable(tx: Any) -> None:
        (await tx.doc(RewindableDoc, opened.conversation_id))["text"] = "r"

    assert await stores(change_rewindable) is True

    async def create_current_fork(tx: Any) -> None:
        await tx.doc(CurrentForkDoc, opened.conversation_id)

    assert await stores(create_current_fork) is True

    async def change_current_fork(tx: Any) -> None:
        (await tx.doc(CurrentForkDoc, opened.conversation_id))["text"] = "c"

    assert await stores(change_current_fork) is True

    async def create_session_text(tx: Any) -> None:
        await tx.doc(SessionTextDoc)

    assert await stores(create_session_text) is True

    async def change_session_text(tx: Any) -> None:
        (await tx.doc(SessionTextDoc))["text"] += "s"

    assert await stores(change_session_text) is True
    # A held document next to an immediate one: the whole commit is stored.
    async def both_documents(tx: Any) -> None:
        (await tx.doc(TextDoc, opened.conversation_id))["text"] += "y"
        (await tx.doc(SessionTextDoc))["text"] += "s"

    assert await stores(both_documents) is True
    # Retirement.
    async def retire_current_fork(tx: Any) -> None:
        await tx.retire_doc(CurrentForkDoc, opened.conversation_id)

    assert await stores(retire_current_fork) is True
    assert opened.sizes["held"] == 0
    # The control: the same change alone is held.
    async def append_alone(tx: Any) -> None:
        (await tx.doc(TextDoc, opened.conversation_id))["text"] += "z"

    assert await stores(append_alone) is False


async def test_stores_a_deferred_commit_that_migrates_its_document_at_once_as_a_base() -> None:
    """Upstream: "stores a deferred commit that migrates its document at once, as a base"."""

    opened = await open_deferred()
    batches = opened.mark()

    async def migrate(tx: Any) -> None:
        (await tx.doc(TextDocV2, opened.conversation_id))["text"] = "v2"

    await opened.session.commit(migrate, context, DEFERRED)
    assert [write["content"]["kind"] for write in changes_of(batches())] == ["base"]
    assert opened.sizes["held"] == 0


async def test_rejects_a_deferred_commit_on_a_read_only_session_like_any_commit_with_writes() -> None:
    """Upstream: "rejects a deferred commit on a read-only Session like any commit with writes"."""

    opened = await open_deferred()
    viewer = SessionImpl(cast(Storage, opened.storage), None, lambda: 600_000, None, True)

    async def change(tx: Any) -> None:
        document = await tx.doc(TextDoc, opened.conversation_id)
        document["text"] = f"{document['text']}a"

    with pytest.raises(ReadOnlySession):
        await viewer.commit(change, context, DEFERRED)


async def test_rejects_a_within_ms_that_is_not_a_number_of_milliseconds_zero_or_more() -> None:
    """Upstream: "rejects a withinMs that is not a number of milliseconds, 0 or more"."""

    opened = await open_deferred()
    with pytest.raises(TypeError):
        await append(opened, "a", within(-1))
    with pytest.raises(TypeError):
        await append(opened, "a", within(float("nan")))
    # The mark that asks for nothing special commits, and resolves with nothing.
    assert await append(opened, "a", IMMEDIATE) is None


# ── storing held changes ────────────────────────────────────────────────────


async def test_stores_them_first_folded_with_its_own_in_a_commit_that_writes_the_document() -> None:
    """Upstream: "stores them first, folded with its own, in a commit that writes the document"."""

    opened = await open_deferred()
    await append(opened, "a")
    await append(opened, "b")
    batches = opened.mark()
    await append(opened, "c", IMMEDIATE)
    assert len(batches()) == 1
    assert [write["content"] for write in changes_of(batches())] == [
        {"version": 1, "kind": "delta", "ops": [["a", ["text"], "abc"]]}
    ]
    assert await opened.stored_text() == {"text": "abc"}
    assert opened.sizes["held"] == 0


async def test_stores_them_in_a_commit_that_only_reads_the_document_with_its_other_writes() -> None:
    """Upstream: "stores them in a commit that only reads the document, with its other writes"."""

    opened = await open_deferred()
    await append(opened, "held")
    batches = opened.mark()

    async def derive(tx: Any) -> None:
        text = (await tx.doc(TextDoc, opened.conversation_id))["text"]
        await tx.append_entry(opened.conversation_id, {"kind": "test.derived", "data": text})

    await opened.session.commit(derive, context)
    assert len(batches()) == 1
    assert sorted(write["type"] for write in batches()[0]) == ["document.change", "entry"]
    assert await opened.stored_text() == {"text": "held"}


async def test_leaves_them_held_through_commits_that_neither_touch_the_document_nor_write() -> None:
    """Upstream: "leaves them held through commits that neither read nor write the document, or write
    nothing"."""

    opened = await open_deferred()
    await append(opened, "held")
    batches = opened.mark()

    async def unrelated(tx: Any) -> None:
        await tx.append_entry(opened.conversation_id, {"kind": "test.unrelated"})

    await opened.session.commit(unrelated, context)

    async def read_only(tx: Any) -> None:
        await tx.doc(TextDoc, opened.conversation_id)

    await opened.session.commit(read_only, context)
    assert changes_of(batches()) == []
    assert opened.sizes["held"] == 1


async def test_keeps_them_when_a_commit_that_reads_the_document_fails_before_storage() -> None:
    """Upstream: "keeps them when a commit that reads the document fails before Storage"."""

    opened = await open_deferred()
    await append(opened, "held")

    async def callback_fails(tx: Any) -> None:
        (await tx.doc(TextDoc, opened.conversation_id))["text"] += "!"
        await tx.append_entry(opened.conversation_id, {"kind": "test.note"})
        raise RuntimeError("callback failed")

    with pytest.raises(RuntimeError, match="callback failed"):
        await opened.session.commit(callback_fails, context)

    # Its checkpoint predicate throws: settlement fails after every other step.
    async def checkpoint_fails(tx: Any) -> None:
        (await tx.doc(TextDoc, opened.conversation_id))["explode"] = True

    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await opened.session.commit(checkpoint_fails, context)
    assert opened.sizes["held"] == 1
    await opened.fire()
    assert await opened.stored_text() == {"text": "held"}


async def test_stores_every_held_document_in_one_transaction_when_the_earliest_deadline_comes() -> None:
    """Upstream: "stores every held document in one transaction when the earliest deadline comes"."""

    opened = await open_deferred()
    other = await create_conversation(opened.session)

    async def touch(tx: Any) -> None:
        await tx.doc(TextDoc, other)

    await opened.session.commit(touch, context)
    await append(opened, "one")
    await append(opened, "two", conversation_id=other)
    assert [timer.delay_ms for timer in opened.armed] == [1_000]
    publications = len(opened.stored)
    batches = opened.mark()
    await opened.fire()
    assert len(batches()) == 1
    assert len(changes_of(batches())) == 2
    assert len(opened.stored) == publications + 1
    assert opened.sizes["held"] == 0
    assert opened.armed == []
    record = await opened.storage.find_document(
        {
            "kind": TextDoc.definition.kind,
            "scope": {"kind": "conversation", "conversationId": other},
        },
        "current",
        context,
    )
    assert record is not None
    stored = await opened.storage.document(record["id"], "current", context)
    assert stored is not None and stored["value"] == {"text": "two"}


async def test_arms_the_earliest_deadline_each_held_commit_asked_for_or_the_default() -> None:
    """Upstream: "arms the earliest deadline of the held commits, each with its withinMs or the default"."""

    opened = await open_deferred(persist_within_ms=2_000)
    await append(opened, "a")
    assert [timer.delay_ms for timer in opened.armed] == [2_000]
    opened.clock["now"] += 500
    # Earlier: the timer moves up.
    await append(opened, "b", within(100))
    assert [timer.delay_ms for timer in opened.armed] == [100]
    # Later: the deadline stays, and the same deadline re-arms nothing -- the guard that a re-arm of
    # an already-armed timer is a no-op (`session.ts:1309`).
    arming = len(opened.armings)
    await append(opened, "c", within(60_000))
    assert [timer.delay_ms for timer in opened.armed] == [100]
    assert len(opened.armings) == arming


async def test_arms_no_timer_for_an_infinite_window_and_moves_it_on_when_a_commit_stores() -> None:
    """Upstream: "arms no timer for an infinite window, and moves the timer on when a commit stores the
    held document"."""

    opened = await open_deferred()
    await append(opened, "a", within(float("inf")))
    assert opened.armed == []
    assert opened.sizes["held"] == 1
    await append(opened, "b")
    assert [timer.delay_ms for timer in opened.armed] == [1_000]
    await append(opened, "c", IMMEDIATE)
    assert opened.armed == []
    assert opened.sizes["held"] == 0


async def test_stores_them_at_close_and_closes_storage_when_that_store_fails() -> None:
    """Upstream: "stores them at close, after the commits admitted before it, and closes Storage when that
    store fails"."""

    opened = await open_deferred()
    await append(opened, "held")
    await opened.session.close(context)
    reopened = await opened.storage.reopen().document(opened.record_id, "current", context)
    assert reopened is not None and reopened["value"] == {"text": "held"}

    failing = await open_deferred()
    await append(failing, "lost")
    failure = RuntimeError("disk gone")
    failing.storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk gone"):
        await failing.session.close(context)
    assert await failing.session.closed == {"kind": "failed", "error": failure}
    # Closed: the backend refuses calls until reopened.
    with pytest.raises(Exception, match="closed"):
        await failing.storage.document(failing.record_id, "current", context)
    stored = await failing.storage.reopen().document(failing.record_id, "current", context)
    assert stored is not None and stored["value"] == {"text": ""}


async def test_stores_nothing_once_the_session_failed() -> None:
    """Upstream: "stores nothing once the Session failed"."""

    opened = await open_deferred()
    await append(opened, "lost")
    batches = opened.mark()
    opened.session.fail(RuntimeError("crash"))
    await opened.session.closed
    assert batches() == []
    stored = await opened.storage.reopen().document(opened.record_id, "current", context)
    assert stored is not None and stored["value"] == {"text": ""}


async def test_stores_them_before_unload_documents_drops_the_trackers() -> None:
    """Upstream: "stores them before unloadDocuments() drops the trackers"."""

    opened = await open_deferred()
    await append(opened, "held")
    await opened.session.unload_documents()
    assert await opened.stored_text() == {"text": "held"}
    assert opened.sizes["cached"] == 0 and opened.sizes["held"] == 0
    assert await opened.session.snapshot(TextDoc, opened.conversation_id, context) == {"text": "held"}


async def test_stores_them_before_a_token_of_another_definition_version_reloads_the_document() -> None:
    """Upstream: "stores them before a token of another definition version reloads the document"."""

    opened = await open_deferred()
    await append(opened, "held")
    batches = opened.mark()
    assert await opened.session.snapshot(TextDocV2, opened.conversation_id, context) == {
        "text": "held",
        "migrated": True,
    }
    assert [write["content"]["kind"] for write in changes_of(batches())] == ["delta"]
    assert await opened.stored_text() == {"text": "held"}

    # The same inside a commit, which then migrates with a base.
    async def read(tx: Any) -> None:
        await tx.doc(TextDoc, opened.conversation_id)

    await opened.session.commit(read, context)
    await append(opened, "+")

    async def migrate(tx: Any) -> None:
        document = await tx.doc(TextDocV2, opened.conversation_id)
        document["text"] = f"{document['text']}v2"

    await opened.session.commit(migrate, context)
    assert await opened.stored_text() == {"text": "held+v2", "migrated": True}


async def test_stores_them_before_the_documents_retirement_in_the_retiring_commit() -> None:
    """Upstream: "stores them before the document's retirement, in the retiring commit"."""

    opened = await open_deferred()
    await append(opened, "held")
    batches = opened.mark()

    async def retire(tx: Any) -> None:
        await tx.retire_doc(TextDoc, opened.conversation_id)

    await opened.session.commit(retire, context)
    assert [write["type"] for write in batches()[0]] == ["document.change", "document.retire"]
    assert changes_of(batches())[0]["content"] == {
        "version": 1,
        "kind": "delta",
        "ops": [["a", ["text"], "held"]],
    }
    assert opened.sizes["held"] == 0
    assert await opened.session.snapshot(TextDoc, opened.conversation_id, context) is None


# ── bases and overflow ──────────────────────────────────────────────────────


async def test_stores_a_base_when_a_held_commit_selected_one_and_one_delta_otherwise() -> None:
    """Upstream: "stores a base when a held commit selected one, and counts the store as one delta
    otherwise"."""

    opened = await open_deferred()
    batches = opened.mark()

    async def select_base(tx: Any) -> None:
        document = await tx.doc(TextDoc, opened.conversation_id)
        document["text"] = "x"
        document["base"] = True

    await opened.session.commit(select_base, context, DEFERRED)
    await append(opened, "y")
    await opened.fire()
    assert [write["content"] for write in changes_of(batches())] == [
        {"version": 1, "kind": "base", "value": {"text": "xy", "base": True}}
    ]


async def test_stores_the_whole_value_once_more_than_max_pending_ops_operations_are_held() -> None:
    """Upstream: "stores the whole value once more than MAX_PENDING_OPS operations are held"."""

    opened = await open_deferred()
    await overflow(opened)
    batches = opened.mark()
    await opened.fire()
    value = await opened.session.snapshot(TextDoc, opened.conversation_id, context)
    assert [write["content"] for write in changes_of(batches())] == [
        {"version": 1, "kind": "base", "value": value}
    ]
    # A durable listener replaces its copy with the stored value.
    change = cast(dict[str, Any], opened.stored[-1]["changes"][0])
    assert change["type"] == "document"
    assert change["ops"] == [["r", value]]


async def test_stores_a_base_of_the_commits_own_value_when_it_stores_an_overflowed_hold() -> None:
    """Upstream: "stores a base of the commit's own value when it stores an overflowed hold"."""

    opened = await open_deferred()
    await overflow(opened)
    await append(opened, "a")
    batches = opened.mark()
    await append(opened, "b", IMMEDIATE)
    value = await opened.session.snapshot(TextDoc, opened.conversation_id, context)
    assert value is not None and value["text"] == "ab"
    stored = [write["content"] for write in changes_of(batches())]
    assert stored == [{"version": 1, "kind": "base", "value": value}]


async def overflow(opened: DeferredSession) -> None:
    """Hold more than ``MAX_PENDING_OPS`` operations.

    Deferred commits of one set per key, each below the tracker's own limit, past which a store is one
    root replacement (``session-deferred-commits.test.ts:499-517``).
    """

    async def reset(tx: Any) -> None:
        (await tx.doc(TextDoc, opened.conversation_id))["keys"] = {}

    await opened.session.commit(reset, context)
    for from_index in range(0, MAX_PENDING_OPS + 1, 4_000):

        async def fill(tx: Any, from_index: int = from_index) -> None:
            keys = (await tx.doc(TextDoc, opened.conversation_id))["keys"]
            for index in range(from_index, from_index + 4_000):
                keys[f"k{index}"] = index

        await opened.session.commit(fill, context, DEFERRED)


# ── publications ────────────────────────────────────────────────────────────


async def test_give_observe_commits_exactly_what_was_stored_and_nothing_for_a_held_commit() -> None:
    """Upstream: "give observeCommits() exactly what was stored, and nothing for a held commit"."""

    opened = await open_deferred()
    from_index = len(opened.storage.commits)
    publications = len(opened.stored)
    await append(opened, "a")
    await append(opened, "b")
    await append(opened, "c", IMMEDIATE)
    await append(opened, "d")
    await opened.fire()
    await append(opened, "e")

    async def with_entry(tx: Any) -> None:
        await tx.doc(TextDoc, opened.conversation_id)
        await tx.append_entry(opened.conversation_id, {"kind": "test.note"})

    await opened.session.commit(with_entry, context)
    await append(opened, "f")
    await opened.session.close(context)
    batches = opened.storage.commits[from_index:]
    published = opened.stored[publications:]
    assert len(published) == len(batches)
    # A replica built from the publications alone always equals what Storage holds.
    from_storage: Any = {"text": ""}
    from_publications: Any = {"text": ""}
    for index, batch in enumerate(batches):
        write = next(
            candidate
            for candidate in changes_of([batch])
            if candidate["id"] == opened.record_id
        )
        change = next(
            candidate
            for candidate in published[index]["changes"]
            if candidate["type"] == "document"
            and candidate["kind"] == "change"
            and candidate["record"]["id"] == opened.record_id
        )
        content = cast(dict[str, Any], write["content"])
        from_storage = (
            content["value"] if content["kind"] == "base" else apply_immutable(from_storage, content["ops"])
        )
        from_publications = apply_immutable(from_publications, change["ops"])
        assert from_publications == from_storage
        assert change["value"] == from_storage
    assert from_storage == {"text": "abcdef"}
    assert [sorted(change["type"] for change in publication["changes"]) for publication in published] == [
        ["document"],
        ["document"],
        ["document", "entry"],
        ["document"],
    ]


async def test_show_every_held_commit_to_watches_and_the_sessions_own_observers_once() -> None:
    """Upstream: "show every held commit to watches and the Session's own observers, once"."""

    opened = await open_deferred()
    watch = await opened.session.watch_doc(TextDoc, opened.conversation_id, context)
    assert watch is not None
    values: list[str] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        values.append(call["value"]["text"])

    watch.start(listener)
    from_index = len(opened.observed)
    for text in ["a", "b", "c"]:
        await append(opened, text)
        await flush()
    await append(opened, "d", IMMEDIATE)
    await flush()
    await opened.fire()
    assert values == ["a", "ab", "abc", "abcd"]
    seen = opened.observed[from_index:]
    assert [publication["kind"] for publication in seen] == ["memory", "memory", "memory", "stored"]
    # The storing commit shows its own operations; the held ones were shown already. The timer showed
    # nothing: the deadline's store reaches no follower of committed memory at all.
    assert [
        [
            change["ops"]
            for change in publication["changes"]
            if change["type"] == "document" and change["kind"] == "change"
        ]
        for publication in seen
    ] == [
        [[["a", ["text"], "a"]]],
        [[["a", ["text"], "b"]]],
        [[["a", ["text"], "c"]]],
        [[["a", ["text"], "d"]]],
    ]
    await watch.stop()


# ── pinning ─────────────────────────────────────────────────────────────────


async def test_keeps_a_held_document_cached_with_retention_0_until_it_is_stored_then_lets_it_go() -> None:
    """Upstream: "keeps a held document cached with retention 0 until it is stored, then lets it go"."""

    opened = await open_deferred(retention_ms=0)
    await opened.session.read_on_line(_nothing)
    assert (opened.sizes["cached"], opened.sizes["held"], opened.sizes["attached"]) == (0, 0, 0)
    await append(opened, "held")
    await opened.session.read_on_line(_nothing)
    assert (opened.sizes["cached"], opened.sizes["held"], opened.sizes["attached"]) == (1, 1, 1)
    await opened.fire()

    async def nothing(_tx: Any) -> None:
        return None

    await opened.session.commit(nothing, context)
    assert (opened.sizes["cached"], opened.sizes["held"], opened.sizes["attached"]) == (0, 0, 0)
    assert await opened.session.snapshot(TextDoc, opened.conversation_id, context) == {"text": "held"}


async def test_retains_a_held_revision_only_until_it_is_stored() -> None:
    """Upstream: "retains a held revision only until it is stored".

    Upstream forces V8's GC through ``--expose-gc``; CPython tracks the same JSON containers, so the
    collector's own object set proves the same property -- the pinned revision survives the hold, and
    nothing keeps it once the hold is gone.
    """

    opened = await open_deferred(retention_ms=0)

    async def stream(tx: Any) -> None:
        document = await tx.doc(TextDoc, opened.conversation_id)
        document["text"] = "x" * 1_000
        # The value carries one nested member: a dict of atomics alone is something CPython's
        # collector untracks, and then no observation of it means anything.
        document["keys"] = {"kept": 1}

    await opened.session.commit(stream, context, DEFERRED)
    # The revision is the value the held commit adopted in memory -- the one the hold pins. The
    # recorded publications would keep it too, so they give it up before the collector is asked.
    memory = [publication for publication in opened.observed if publication["kind"] == "memory"]
    assert memory, "the deferred commit showed its own change to the Session's own followers"
    revision = memory[-1]["changes"][0]
    assert revision["type"] == "document" and revision["kind"] == "change"
    identity = id(revision["value"])
    opened.stored.clear()
    opened.observed.clear()
    del memory, revision
    await collect()
    assert survives(identity)
    await opened.fire()

    async def nothing(_tx: Any) -> None:
        return None

    await opened.session.commit(nothing, context)
    # The recorded publications would keep it.
    opened.stored.clear()
    opened.observed.clear()
    await collect()
    assert not survives(identity)


async def collect() -> None:
    """Give the collector the same chance upstream's ``gc()`` loop gives V8."""

    for _round in range(5):
        await asyncio.sleep(0)
        gc.collect()


def survives(identity: int) -> bool:
    """Whether the object whose ``id`` one recorded is still reachable from the collector."""

    return any(id(candidate) == identity for candidate in gc.get_objects())


# ── crash ───────────────────────────────────────────────────────────────────


async def test_leaves_storage_at_the_last_stored_state_and_nothing_held_after_reopen() -> None:
    """Upstream: "leaves Storage at the last stored state, which memory had, and nothing held after
    reopen"."""

    opened = await open_deferred()
    states: list[str] = []

    def follow(publication: ObservedCommit, _context: Any) -> None:
        for change in publication["changes"]:
            # Publications carry untyped document values; this one is ``TextDoc``'s.
            if (
                change["type"] == "document"
                and change["kind"] == "change"
                and change["record"]["id"] == opened.record_id
            ):
                states.append(cast(dict[str, Any], change["value"])["text"])

    opened.session.observe_stored_commits(follow)
    await append(opened, "a")
    await opened.fire()
    await append(opened, "b")
    await append(opened, "c")
    opened.session.fail(RuntimeError("crash"))
    await opened.session.closed
    reopened = SessionImpl(cast(Storage, opened.storage.reopen()), None, lambda: 600_000)
    value = await reopened.snapshot(TextDoc, opened.conversation_id, context)
    assert value == {"text": "a"}
    assert value is not None and value["text"] in states
    assert reopened.document_cache_sizes["held"] == 0
    await reopened.close(context)

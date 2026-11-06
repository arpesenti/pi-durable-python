"""Translated v1.5.0 ``session-observers`` suite.

Covers ``Session.observe_doc()`` — the synchronous observer of one document address that the
conversation-view and task-graph observers are built on — and ``Session.observe_commits()``, the
synchronous observer of stored commits. An observer mirrors state rather than rendering it: it is
handed the very call the matching watch is handed, unfolded and on the mutation line; its detach is
idempotent; and a Session close ends it ``failed`` with a session-closed error, because its host
follows the Session into the next one (decision "Slice 8 S1", spec §9.1).

Two upstream cases are deliberately not translated here, each named with where its guarantee lives
instead:

- ``:105`` "advances when memory adopts a held commit, before anything is stored" — a deferred commit
  is held in memory before Storage sees it, which is **#47**'s work: the persistence mark on the
  commit, the persist-within deadline, and the foreground timer that arms it do not exist yet. The
  commit observer is fed from the durability receipt, so it is ready for that case; #47 carries these
  three sub-clauses.
- ``:309`` "keeps no value of a document retired for good, while still attached" — its claim is a
  garbage-collection claim, and CPython cannot take a weak reference to the plain ``dict`` a document
  value is. The Session's own cache sizes are the port's measure of the same guarantee: see
  ``tests/test_session_document_retention.py::test_keeps_no_value_of_a_document_retired_for_good_while_an_observer_is_attached``.
  Where an object can be weakly held — the host's own observer — the collection case is translated
  directly, as ``test_keeps_no_detached_observer_alive``.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from typing import Any, cast

import pytest
from session_support import (
    ControlledStorage,
    context,
    create_conversation,
    document_changes,
    flush,
)

from pi_durable import MemoryStorage, define_doc
from pi_durable.context import AbortController, AbortError, with_abort_signal
from pi_durable.harness.types import LISTENER, ReportInfo
from pi_durable.session.session import SessionImpl
from pi_durable.types import CommitPublication, Storage

Counter = define_doc(
    {
        "kind": "observe.session-counter",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"count": 0},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)

ConversationCounter = define_doc(
    {
        "kind": "observe.conversation-counter",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"count": 0},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


Copied = define_doc(
    {
        "kind": "observe.copied",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "current",
        "initial": lambda: {"count": 0},
        "checkpoint_when": lambda *_args: False,
    }
)


class ReportingSession(SessionImpl):
    """A Session that keeps its reports, which a plain Session drops."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.reports: list[tuple[BaseException, ReportInfo]] = []

    def report(self, error: BaseException, info: ReportInfo | None = None) -> None:
        self.reports.append((error, info if info is not None else LISTENER))


def open_session() -> tuple[ControlledStorage, ReportingSession]:
    storage = ControlledStorage()
    return storage, ReportingSession(cast(Storage, storage), lambda: 1_000, lambda: 600_000)


class Recorder:
    """An observer recording each call, whether it ran while Storage held the commit, and its ends."""

    def __init__(self, storage: ControlledStorage) -> None:
        self.storage = storage
        self.calls: list[dict[str, Any]] = []
        self.ends: list[Any] = []

    @property
    def advances(self) -> list[Any]:
        # ``calls`` holds the end too, once there is one: the advances are what was mirrored.
        return [call["advance"] for call in self.calls if "advance" in call]

    def advance(self, call: Any, _context: Any = None) -> None:
        # Durable's failures are final, so every call a state observer gets is a `value` call.
        assert call["kind"] == "value"
        self.calls.append({"advance": call, "stored": len(self.storage.commits)})

    def end(self, end: Any) -> None:
        self.ends.append(end)
        self.calls.append({"end": end})


class CommitRecorder:
    """A commit observer recording each publication's sequence, and its ends."""

    def __init__(self) -> None:
        self.seqs: list[int] = []
        self.publications: list[CommitPublication] = []
        self.ends: list[Any] = []

    def advance(self, publication: CommitPublication, _context: Any = None) -> None:
        self.publications.append(publication)
        self.seqs.append(publication["seq"])

    def end(self, end: Any) -> None:
        self.ends.append(end)


async def collect() -> None:
    """The CPython equivalent of the upstream ``--expose-gc`` collection: what nothing holds is gone."""

    for _round in range(5):
        await asyncio.sleep(0)
        gc.collect()


async def add(session: SessionImpl, by: int = 1) -> Any:
    async def change(tx: Any) -> None:
        (await tx.doc(Counter)).count += by

    return await session.commit(change, context)


async def create(session: SessionImpl, conversation_id: Any, value: int) -> None:
    async def change(tx: Any) -> None:
        (await tx.doc(ConversationCounter, conversation_id)).count = value

    await session.commit(change, context)


async def retire(session: SessionImpl, conversation_id: Any) -> None:
    async def change(tx: Any) -> None:
        await tx.retire_doc(ConversationCounter, conversation_id)

    await session.commit(change, context)


# ── observe_doc ─────────────────────────────────────────────────────────────


async def test_returns_none_for_an_absent_address_and_attaches_nothing() -> None:
    """Upstream: "returns undefined for an absent address and attaches nothing"."""

    storage, session = open_session()
    recorder = Recorder(storage)
    assert await session.observe_doc(Counter, recorder, context) is None
    await add(session)
    assert recorder.calls == []
    assert session.document_cache_sizes["attached"] == 0
    await session.close(context)


async def test_starts_at_the_committed_value_and_advances_at_each_stored_publication_with_its_ops() -> None:
    """Upstream: "starts at the committed value and advances at the publication of each stored
    commit with its ops"."""

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    observation = await session.observe_doc(Counter, recorder, context)
    assert observation is not None and observation.value == {"count": 1}
    await add(session, 2)
    # Published once Storage stored it, and with the commit's own operations.
    assert recorder.calls == [
        {
            "advance": {
                "kind": "value",
                "value": {"count": 3},
                "ops": [["s", ["count"], 3]],
                "retired": False,
            },
            "stored": 2,
        }
    ]
    observation.detach()
    await session.close(context)


async def test_hands_the_very_committed_value_the_publication_and_the_snapshot_carry() -> None:
    """Proven of the deleted state getter by "shares exact committed value references with Chord"
    and "exposes trusted shared values without freezing", and proven of the observer here.

    What an observer is handed is what Storage stored: one object, shared with the commit's
    publication and with the snapshot, rather than a copy or a frozen stand-in made for the observer
    (spec §9.1).
    """

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    commits = CommitRecorder()
    session.observe_commits(commits)
    observation = await session.observe_doc(Counter, recorder, context)
    assert observation is not None
    await add(session, 2)

    handed = recorder.calls[-1]["advance"]["value"]
    published = document_changes(commits.publications[-1])[0]
    assert handed is published["value"]
    assert handed is await session.snapshot(Counter, context)
    # A plain dict, as the Session holds it: nothing was deep-frozen on the way out.
    assert type(handed) is dict
    observation.detach()
    await session.close(context)


async def test_hydrates_a_definition_free_fork_copy_for_an_observer() -> None:
    """Proven of the deleted state getter by "cold loads a definition-free fork copy".

    A conversation forked at an entry starts its documents from what the parent held there. An
    observer attached to that copy — which the Session never loaded, and which carries a value its
    own definition never produced — is handed the copied value, read from Storage (spec §9.1, §9.2).
    """

    storage, session = open_session()
    parent_id = await create_conversation(session)

    async def seed(tx: Any) -> Any:
        entry = await tx.append_entry(parent_id, {"kind": "point"})
        (await tx.doc(Copied, parent_id)).count = 7
        return entry["id"]

    first = await session.commit(seed, context)

    async def fork(tx: Any) -> Any:
        child = await tx.fork_conversation(
            parent_id, first, {"ownership": {"kind": "ownerless"}}
        )
        return child["id"]

    child_id = await session.commit(fork, context)
    reads = storage.document_read_count
    recorder = Recorder(storage)
    observation = await session.observe_doc(Copied, child_id, recorder, context)
    assert observation is not None and observation.value == {"count": 7}
    # The copy came from Storage, not from a tracker the parent still held.
    assert storage.document_read_count > reads
    observation.detach()
    await session.close(context)


async def test_takes_nothing_from_a_commit_that_settles_after_the_close_sealed_it() -> None:
    """Proven of the deleted state getter by "keeps its last value and ignores a commit that settles
    during close".

    Close seals admission first: the value the observation was handed stays its value, and a commit
    already underway when the seal fell reaches the observer with nothing at all — not even a call to
    say it changed (spec §9.1).
    """

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    observation = await session.observe_doc(Counter, recorder, context)
    assert observation is not None and observation.value == {"count": 1}
    gate = storage.hold_commits()
    committing = asyncio.ensure_future(add(session, 5))
    await gate.entered.wait()
    closing = asyncio.ensure_future(session.close(context))
    await flush()
    gate.release()
    await committing
    await closing
    # Nothing of the commit reached it: no call at all, only the close's own end.
    assert recorder.advances == []
    assert observation.value == {"count": 1}
    assert len(recorder.ends) == 1
    assert recorder.ends[0]["kind"] == "failed"
    assert str(recorder.ends[0]["error"]) == "session closed"


async def test_continues_from_the_value_it_holds_once_the_cache_is_emptied() -> None:
    """Proven of the deleted state getter by "continues from exact committed values after cache
    unload", and proven of the observer here.

    An observer mirrors the committed state, not the cache: what the Session evicts and loads again
    changes nothing about the call it is handed next, which still unfolds from what it holds (spec
    §9.1, §9.2).
    """

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    observation = await session.observe_doc(Counter, recorder, context)
    assert observation is not None and observation.value == {"count": 1}
    reads = storage.document_read_count
    await session.unload_documents()
    await add(session, 4)
    assert recorder.calls[-1]["advance"]["value"] == {"count": 5}
    assert recorder.calls[-1]["advance"]["ops"] == [["s", ["count"], 5]]
    # The line reloaded what it had evicted; the observer noticed neither.
    assert storage.document_read_count > reads
    observation.detach()
    await session.close(context)


async def test_follows_a_conversation_documents_address_through_a_retirement_into_its_recreation() -> None:
    """Upstream: "follows a conversation document's address: a retirement with the last value, then
    the new incarnation"."""

    storage, session = open_session()
    conversation_id = await create_conversation(session)
    await create(session, conversation_id, 2)
    recorder = Recorder(storage)
    observation = await session.observe_doc(ConversationCounter, conversation_id, recorder, context)
    assert observation is not None
    await retire(session, conversation_id)
    # The address is absent now: a new attachment finds nothing, the existing one goes on.
    assert await session.observe_doc(
        ConversationCounter, conversation_id, Recorder(storage), context
    ) is None
    await create(session, conversation_id, 9)
    assert recorder.advances == [
        {"kind": "value", "value": observation.value, "ops": [], "retired": True},
        {"kind": "value", "value": {"count": 9}, "ops": [["r", {"count": 9}]], "retired": False},
    ]
    # The retirement keeps the last value the observer was given: the same object, not a copy.
    assert recorder.advances[0]["value"] is observation.value
    observation.detach()
    await session.close(context)


async def test_gets_nothing_after_detach_which_is_idempotent_and_unpins_the_document() -> None:
    """Upstream: "gets nothing after detach, which is idempotent and unpins the document"."""

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    observation = await session.observe_doc(Counter, recorder, context)
    assert observation is not None
    assert session.document_cache_sizes["attached"] == 1
    observation.detach()
    observation.detach()
    assert session.document_cache_sizes["attached"] == 0
    await add(session)
    await session.close(context)
    assert recorder.calls == []


async def test_ends_once_when_the_session_closes_failed_while_a_watch_ends_ended() -> None:
    """Upstream: "ends once when the Session closes, ``failed`` (S1: an adapter follows the next
    Session), a watch ``ended``"."""

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    await session.observe_doc(Counter, recorder, context)
    watch = await session.watch_doc(Counter, context)
    assert watch is not None
    await session.close(context)
    assert len(recorder.ends) == 1
    assert recorder.ends[0]["kind"] == "failed"
    assert str(recorder.ends[0]["error"]) == "session closed"
    assert await watch.closed == {"kind": "ended"}


async def test_ends_each_close_s_observers_with_an_error_of_that_close() -> None:
    """Upstream: "ends each close's observers with an error of that close, not one shared by every
    Session"."""

    errors: list[BaseException] = []
    for _round in range(2):
        _storage, session = open_session()
        await add(session)

        class Ender:
            def advance(self, _call: Any, _context: Any = None) -> None:
                return None

            def end(self, end: Any) -> None:
                if end["kind"] == "failed":
                    errors.append(cast(BaseException, end["error"]))

        await session.observe_doc(Counter, Ender(), context)
        await session.close(context)
    assert [str(error) for error in errors] == ["session closed", "session closed"]
    assert errors[0] is not errors[1]


async def test_ends_with_the_error_that_failed_the_session() -> None:
    """Upstream: "ends with the error that failed the Session"."""

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    await session.observe_doc(Counter, recorder, context)
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk gone"):
        await add(session)
    await session.closed
    assert recorder.ends == [{"kind": "failed", "error": failure}]


async def test_reports_a_throwing_observer_detaches_it_and_ends_it_with_the_error() -> None:
    """Upstream: "reports a throwing observer, detaches it and ends it with the error; the commit and
    other observers go on"."""

    storage, session = open_session()
    await add(session)
    bug = RuntimeError("observer bug")
    ends: list[Any] = []

    class Thrower:
        def advance(self, _call: Any, _context: Any = None) -> None:
            raise bug

        def end(self, end: Any) -> None:
            ends.append(end)

    await session.observe_doc(Counter, Thrower(), context)
    healthy = Recorder(storage)
    await session.observe_doc(Counter, healthy, context)
    await add(session)
    await add(session)
    assert session.reports == [(bug, {"kind": "listener_error"})]
    assert ends == [{"kind": "listener_error", "error": bug}]
    assert len(healthy.calls) == 2
    assert await session.snapshot(Counter, context) == {"count": 3}
    await session.close(context)
    # Detached at its throw: the close does not end it again.
    assert len(ends) == 1
    assert session.failure is None


async def test_attaches_atomically_on_the_line_a_commit_queued_before_it_is_in_its_value() -> None:
    """Upstream: "attaches atomically on the line: a commit queued before the attachment is in its
    value"."""

    storage, session = open_session()
    await add(session)
    recorder = Recorder(storage)
    committing = asyncio.ensure_future(add(session, 10))
    await asyncio.sleep(0)
    observation = await session.observe_doc(Counter, recorder, context)
    await committing
    assert observation is not None and observation.value == {"count": 11}
    # The commit queued before the attachment is in the value, not in a call after it.
    assert recorder.calls == []
    observation.detach()
    await session.close(context)


async def test_rejects_an_attachment_whose_context_is_cancelled_and_attaches_nothing() -> None:
    """Upstream: "rejects an attachment whose context is cancelled, and attaches nothing"."""

    storage, session = open_session()
    await add(session)
    controller = AbortController()
    controller.abort(RuntimeError("gone"))
    cancelled = with_abort_signal(controller.signal, context)
    with pytest.raises(AbortError) as aborted:
        await session.observe_doc(Counter, Recorder(storage), cancelled)
    assert isinstance(aborted.value.reason, RuntimeError)
    assert session.document_cache_sizes["attached"] == 0
    await session.close(context)


async def test_keeps_no_detached_observer_alive() -> None:
    """Upstream: "keeps no detached observer alive"."""

    session = SessionImpl(MemoryStorage(), lambda: 1_000, lambda: 600_000)
    await add(session)

    class Silent:
        def advance(self, _call: Any, _context: Any = None) -> None:
            return None

        def end(self, _end: Any) -> None:
            return None

    async def attach(detach: bool) -> tuple[Any, weakref.ReferenceType[Any]]:
        observer = Silent()
        observation = await session.observe_doc(Counter, observer, context)
        assert observation is not None
        if detach:
            observation.detach()
        return observation, weakref.ref(observer)

    # Control: an attached observer stays, so the collection below is real.
    attached, attached_observer = await attach(False)
    await collect()
    assert attached_observer() is not None
    attached.detach()

    detached, detached_observer = await attach(True)
    await collect()
    assert detached_observer() is None
    await flush()
    await session.close(context)


async def test_rejects_never_throws_on_a_closed_session_or_a_missing_scope_id() -> None:
    """Upstream: "rejects, never throws, on a closed Session or a missing scope ID"."""

    storage, session = open_session()
    with pytest.raises(TypeError):
        await session.observe_doc(ConversationCounter, None, Recorder(storage), context)
    await session.close(context)
    with pytest.raises(RuntimeError, match="Session is closed"):
        await session.observe_doc(Counter, Recorder(storage), context)


# Upstream's "keeps no value of a document retired for good, while still attached" is not translated
# here. Its claim — that a retired incarnation leaves nothing held behind — is a garbage-collection
# claim, and CPython cannot weakref the plain dict a document value is. The Session's own cache sizes
# are the port's measure of the same guarantee: see tests/test_session_document_retention.py,
# test_keeps_no_value_of_a_document_retired_for_good_while_an_observer_is_attached.


async def test_reports_an_observer_whose_call_returns_a_rejected_awaitable() -> None:
    """Upstream: "reports an observer whose call returns a rejected promise, then detaches it and
    ends it ``listener_error``"."""

    _storage, session = open_session()
    await add(session)
    bug = RuntimeError("async observer bug")
    ends: list[Any] = []
    calls = 0

    class LateThrower:
        def advance(self, _call: Any, _context: Any = None) -> Any:
            nonlocal calls
            calls += 1

            async def reject() -> None:
                raise bug

            return reject()

        def end(self, end: Any) -> None:
            ends.append(end)

    await session.observe_doc(Counter, LateThrower(), context)
    await add(session)
    await flush()
    await add(session)
    assert calls == 1
    assert session.reports == [(bug, {"kind": "listener_error"})]
    assert ends == [{"kind": "listener_error", "error": bug}]
    await session.close(context)
    assert len(ends) == 1


# ── observe_commits ─────────────────────────────────────────────────────────


async def test_gets_every_stored_commit_then_failed_once_the_session_closed() -> None:
    """Upstream: "gets every stored commit, then ``failed`` once the Session closed (S1), after its
    last publication"."""

    _storage, session = open_session()
    recorder = CommitRecorder()
    session.observe_commits(recorder)
    await add(session)
    await add(session)
    await session.close(context)
    await flush()
    assert recorder.seqs == [1, 2]
    assert len(recorder.ends) == 1
    assert recorder.ends[0]["kind"] == "failed"
    assert str(recorder.ends[0]["error"]) == "session closed"


async def test_commit_observer_ends_failed_with_the_error_that_failed_the_session() -> None:
    """Upstream: "ends ``failed`` with the error that failed the Session"."""

    storage, session = open_session()
    recorder = CommitRecorder()
    session.observe_commits(recorder)
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk gone"):
        await add(session)
    await session.closed
    assert recorder.seqs == []
    assert recorder.ends == [{"kind": "failed", "error": failure}]


async def test_commit_observer_gets_nothing_after_detach_which_is_idempotent() -> None:
    """Upstream: "gets nothing after detach, which is idempotent, not even its end"."""

    _storage, session = open_session()
    recorder = CommitRecorder()
    observation = session.observe_commits(recorder)
    await add(session)
    await flush()
    observation.detach()
    observation.detach()
    await add(session)
    await session.close(context)
    await flush()
    assert recorder.seqs == [1]
    assert recorder.ends == []


async def test_reports_a_throwing_commit_observer_and_the_others_go_on() -> None:
    """Upstream: "reports a throwing observer, detaches it and ends it ``listener_error``; the others
    go on"."""

    _storage, session = open_session()
    bug = RuntimeError("commit observer bug")
    ends: list[Any] = []
    thrown = 0

    class Thrower:
        def advance(self, _publication: Any, _context: Any = None) -> None:
            nonlocal thrown
            thrown += 1
            raise bug

        def end(self, end: Any) -> None:
            ends.append(end)

    session.observe_commits(Thrower())
    healthy = CommitRecorder()
    session.observe_commits(healthy)
    await add(session)
    await add(session)
    await flush()
    assert thrown == 1
    assert session.reports == [(bug, {"kind": "listener_error"})]
    assert ends == [{"kind": "listener_error", "error": bug}]
    assert healthy.seqs == [1, 2]
    await session.close(context)
    await flush()
    assert len(ends) == 1


# ── observe_doc detached during a commit ────────────────────────────────────


async def retiring() -> tuple[ControlledStorage, ReportingSession, Any, Any]:
    """A Session with a conversation counter at 1, and the commit retiring and recreating it."""

    storage, session = open_session()
    conversation_id = await create_conversation(session)
    await create(session, conversation_id, 1)

    async def recreate() -> None:
        async def change(tx: Any) -> None:
            await tx.retire_doc(ConversationCounter, conversation_id)
            (await tx.doc(ConversationCounter, conversation_id)).count = 2

        await session.commit(change, context)

    return storage, session, conversation_id, recreate


async def test_gets_nothing_more_once_it_throws_on_the_retirement_and_pins_nothing() -> None:
    """Upstream: "gets nothing more of the commit once it throws on the retirement, and pins nothing
    for the recreation"."""

    storage, session, conversation_id, recreate = await retiring()
    bug = RuntimeError("observer bug")
    changes: list[Any] = []
    ends: list[Any] = []

    class Thrower:
        def advance(self, call: Any, _context: Any = None) -> None:
            changes.append(call)
            raise bug

        def end(self, end: Any) -> None:
            ends.append(end)

    observation = await session.observe_doc(ConversationCounter, conversation_id, Thrower(), context)
    assert observation is not None
    await recreate()
    assert changes == [{"kind": "value", "value": observation.value, "ops": [], "retired": True}]
    assert ends == [{"kind": "listener_error", "error": bug}]
    assert session.reports == [(bug, {"kind": "listener_error"})]
    assert session.document_cache_sizes["attached"] == 0
    observation.detach()
    assert session.document_cache_sizes["attached"] == 0
    await session.close(context)


async def test_gets_nothing_more_once_it_detaches_in_the_retirement_call() -> None:
    """Upstream: "gets nothing more of the commit once it detaches in the retirement call, and pins
    nothing"."""

    _storage, session, conversation_id, recreate = await retiring()
    changes: list[Any] = []
    ends: list[Any] = []
    box: dict[str, Any] = {}

    class Detacher:
        def advance(self, call: Any, _context: Any = None) -> None:
            changes.append(call)
            box["observation"].detach()

        def end(self, end: Any) -> None:
            ends.append(end)

    detacher = Detacher()
    observation = await session.observe_doc(ConversationCounter, conversation_id, detacher, context)
    assert observation is not None
    box["observation"] = observation
    await recreate()
    assert changes == [{"kind": "value", "value": observation.value, "ops": [], "retired": True}]
    assert session.document_cache_sizes["attached"] == 0
    await session.close(context)
    assert ends == []


async def test_gets_nothing_of_the_commit_when_another_observer_detached_it_first() -> None:
    """Upstream: "gets nothing of the commit when another observer's call detached it first, and pins
    nothing"."""

    storage, session, conversation_id, recreate = await retiring()
    second: dict[str, Any] = {}

    class DetachesTheOther:
        def advance(self, _call: Any, _context: Any = None) -> None:
            other = second.get("observation")
            if other is not None:
                other.detach()

        def end(self, _end: Any) -> None:
            return None

    first = await session.observe_doc(ConversationCounter, conversation_id, DetachesTheOther(), context)
    assert first is not None
    detached = Recorder(storage)
    second["observation"] = await session.observe_doc(
        ConversationCounter, conversation_id, detached, context
    )
    await recreate()
    assert detached.calls == []
    # Only the first observer pins the new incarnation; its detach leaves nothing pinned.
    first.detach()
    assert session.document_cache_sizes["attached"] == 0
    await session.close(context)

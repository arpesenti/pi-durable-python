"""Translated upstream ``test/session-adoption.test.ts`` (v1.5.0) case for case.

What a Session does once Storage has stored one of its commits: take the durability receipt inside
the commit, adopt, publish, and fail the Session when the adopting or the storing goes wrong --
besides what each audience then sees. Every ``it()`` case of the upstream suite is translated here,
same setup, same injections, same expected values, in the upstream order.

The storage seam is the upstream ``FaultyStorage`` over the port's controlled storage: it records
``adopt`` and its own resolution in ``order``, sets ``adopting`` while the receipt runs, and makes
its next commit misbehave as ``fault`` says. The Session's own audience is followed with
``observe_stored_commits`` -- the port's ``followCommits``; the host's, with ``observe_commits``.
"""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from session_support import (
    CommitObserverRecorder,
    ControlledStorage,
    context,
    create_conversation,
    document_changes,
    flush,
)

from pi_durable import define_doc
from pi_durable.context import Context
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.json import JsonObject
from pi_durable.session.session import SessionImpl
from pi_durable.session.transaction import Transaction
from pi_durable.storage.jsonl import JsonlStorage
from pi_durable.types import (
    CommitOptions,
    CommitPublication,
    CommitReceipt,
    CommitResult,
    ObservedCommit,
    Seq,
    StorageWrite,
)


def _no_checkpoint(_value: JsonObject, _ops: Any, _info: Any) -> bool:
    return False


CounterDoc = define_doc(
    {
        "kind": "adoption.counter",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"count": 0},
        "checkpoint_when": _no_checkpoint,
    }
)


#: A latest, initial-fork conversation document, the kind a deferred commit may hold.
NoteDoc = define_doc(
    {
        "kind": "adoption.note",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {},
        "checkpoint_when": _no_checkpoint,
    }
)


class FaultyStorage(ControlledStorage):
    """Controlled storage that records ``adopt`` and its own resolution in ``order``; its next
    commit misbehaves as ``fault`` says (the upstream ``FaultyStorage``)."""

    def __init__(self) -> None:
        super().__init__()
        self.fault: str | None = None
        self.order: list[str] = []
        #: Set while the receipt runs, the storage's synchronous step.
        self.adopting = False
        #: The error a ``reject_after_adopt`` fault throws, kept for identity assertions.
        self.reclaim_error: RuntimeError | None = None

    async def commit_batches(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        commit_context: Context,
    ) -> CommitResult:
        fault = self.fault
        self.fault = None
        if fault == "skip_adopt":
            return await super().commit_batches(batches, lambda _index, _seq: None, commit_context)

        def adopting(index: int, seq: Seq) -> None:
            self.order.append("adopt")
            self.adopting = True
            try:
                receive(index, seq)
            finally:
                self.adopting = False
            self.order.append("adopted")

        result = await super().commit_batches(batches, adopting, commit_context)
        self.order.append("resolve")
        if fault == "reject_after_adopt":
            if self.reclaim_error is None:
                self.reclaim_error = RuntimeError("reclaim failed")
            raise self.reclaim_error
        return result


def _open() -> tuple[FaultyStorage, SessionImpl]:
    storage = FaultyStorage()
    return storage, SessionImpl(storage)


async def _bump(session: SessionImpl) -> None:
    await session.commit(lambda tx: _bump_change(tx), context)


async def _bump_change(tx: Transaction) -> None:
    (await tx.doc(CounterDoc))["count"] += 1


async def _create_note(tx: Transaction, conversation_id: Any) -> None:
    await tx.doc(NoteDoc, conversation_id)


async def _note_change(tx: Transaction, conversation_id: Any, field: str, value: int) -> None:
    (await tx.doc(NoteDoc, conversation_id))[field] = value


#: The upstream ``{ persistence: { kind: "deferred", withinMs: Infinity } }``.
DEFERRED: CommitOptions = {"persistence": {"kind": "deferred", "within_ms": float("inf")}}


def _spied(monkeypatch: pytest.MonkeyPatch, method: str) -> list[Any]:
    """The calls of one ``Transaction`` method, spied for the test (upstream ``vi.spyOn``)."""

    calls: list[Any] = []
    original = getattr(Transaction, method)

    def spy(self: Transaction, *args: Any) -> Any:
        calls.append(args)
        return original(self, *args)

    monkeypatch.setattr(Transaction, method, spy)
    return calls


async def test_takes_the_durability_receipt_inside_the_commit_and_adopts_once_storage_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream ``test/session-adoption.test.ts:84``."""

    storage, session = _open()
    adopts = _spied(monkeypatch, "adopt")
    await _bump(session)
    assert storage.order == ["adopt", "adopted", "resolve"]
    assert len(adopts) == 1
    assert await session.snapshot(CounterDoc, context) == {"count": 1}
    await session.close(context)


async def test_fails_the_session_without_discarding_when_storage_rejects_after_adoption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream ``test/session-adoption.test.ts:94``."""

    storage, session = _open()
    await _bump(session)
    discards = _spied(monkeypatch, "discard")
    storage.fault = "reject_after_adopt"
    with pytest.raises(RuntimeError, match="reclaim failed"):
        await _bump(session)
    assert not discards
    assert await session.closed == {"kind": "failed", "error": storage.reclaim_error}


async def test_discards_the_staged_changes_when_storage_rejects_before_adoption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream ``test/session-adoption.test.ts:104``."""

    storage, session = _open()
    await _bump(session)
    discards = _spied(monkeypatch, "discard")
    failure = RuntimeError("disk full")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk full"):
        await _bump(session)
    assert len(discards) == 1
    assert await session.closed == {"kind": "failed", "error": failure}


async def test_fails_the_session_with_its_own_adoption_error_which_never_reaches_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upstream ``test/session-adoption.test.ts:115``."""

    storage, session = _open()
    await _bump(session)
    failure = RuntimeError("adoption failed")

    def adoption_error(_self: Transaction, _seq: Seq) -> Any:
        raise failure

    monkeypatch.setattr(Transaction, "adopt", adoption_error)
    with pytest.raises(RuntimeError, match="adoption failed"):
        await _bump(session)
    # Storage saw a normal adoption and resolved.
    assert storage.order[-3:] == ["adopt", "adopted", "resolve"]
    assert await session.closed == {"kind": "failed", "error": failure}


async def test_fails_the_session_when_storage_resolves_without_adopting() -> None:
    """Upstream ``test/session-adoption.test.ts:128``."""

    storage, session = _open()
    storage.fault = "skip_adopt"
    with pytest.raises(RuntimeError, match="without adopting"):
        await _bump(session)
    assert (await session.closed)["kind"] == "failed"


async def test_runs_its_own_observers_once_storage_resolved_and_host_listeners_after() -> None:
    """Upstream ``test/session-adoption.test.ts:137``."""

    storage, session = _open()
    session.observe_stored_commits(
        lambda _publication, _context: storage.order.append(f"observed adopting={storage.adopting}")
    )
    session.observe_commits(
        CommitObserverRecorder(
            lambda _publication, _context: storage.order.append(f"host adopting={storage.adopting}")
        )
    )
    await _bump(session)
    assert storage.order == [
        "adopt",
        "adopted",
        "resolve",
        "observed adopting=False",
        "host adopting=False",
    ]
    await session.close(context)


async def test_calls_the_host_listeners_registered_when_its_dispatch_runs() -> None:
    """Upstream ``test/session-adoption.test.ts:153``: "calls the host listeners registered when
    its dispatch runs, and none for a commit published without one" (both halves, in order)."""

    _, session = _open()
    calls: list[str] = []
    removed = session.observe_commits(
        CommitObserverRecorder(lambda _publication, _context: calls.append("removed"))
    )
    session.observe_commits(
        CommitObserverRecorder(lambda _publication, _context: calls.append("kept"))
    )

    changed = [False]

    def own(_publication: ObservedCommit, _context: Context) -> None:
        # During the publication, after the dispatch was taken.
        if changed[0]:
            return
        changed[0] = True
        removed.detach()
        session.observe_commits(
            CommitObserverRecorder(lambda _publication, _context: calls.append("added"))
        )

    session.observe_stored_commits(own)
    await _bump(session)
    assert calls == ["kept", "added"]

    # A Session with no host listener at the publication takes no dispatch, so one added during it
    # misses it.
    _, bare = _open()
    late: list[Seq] = []
    added = [False]

    def own_bare(_publication: ObservedCommit, _context: Context) -> None:
        if added[0]:
            return
        added[0] = True
        bare.observe_commits(
            CommitObserverRecorder(lambda publication, _context: late.append(publication["seq"]))
        )

    bare.observe_stored_commits(own_bare)
    await _bump(bare)
    await flush()
    assert late == []
    await _bump(bare)
    await flush()
    assert len(late) == 1
    await session.close(context)
    await bare.close(context)


async def test_fails_the_session_after_storage_returned_for_a_throw_of_its_own_observers() -> None:
    """Upstream ``test/session-adoption.test.ts:205``."""

    storage, session = _open()
    bug = RuntimeError("observer bug")

    def throwing(_publication: ObservedCommit, _context: Context) -> None:
        storage.order.append("observed")
        raise bug

    session.observe_stored_commits(throwing)
    session.observe_stored_commits(
        lambda _publication, _context: storage.order.append("observed after")
    )
    session.observe_commits(
        CommitObserverRecorder(
            lambda _publication, _context: storage.order.append(
                f"host failed={session.failure is not None}"
            )
        )
    )
    session.subscribe_close(lambda: storage.order.append("close"))
    # The commit stands and resolves; the Session fails only after the hosts got it.
    await _bump(session)
    await flush()
    assert storage.order == [
        "adopt",
        "adopted",
        "resolve",
        "observed",
        "observed after",
        "host failed=False",
        "close",
    ]
    assert await session.closed == {"kind": "failed", "error": bug}
    reopened = SessionImpl(storage.reopen())
    assert await reopened.snapshot(CounterDoc, context) == {"count": 1}
    await reopened.close(context)


async def test_publishes_a_commit_that_storage_rejects_after_adopting_it_then_fails_the_session(
) -> None:
    """Upstream ``test/session-adoption.test.ts:241``."""

    storage, session = _open()
    await _bump(session)
    stored: list[Seq] = []
    session.observe_stored_commits(
        lambda _publication, _context: storage.order.append("observed")
    )

    def host_record(publication: CommitPublication, _context: Context) -> None:
        storage.order.append("host")
        stored.append(publication["seq"])

    session.observe_commits(CommitObserverRecorder(host_record))
    storage.order.clear()
    storage.fault = "reject_after_adopt"
    with pytest.raises(RuntimeError, match="reclaim failed"):
        await _bump(session)
    assert storage.order == ["adopt", "adopted", "resolve", "observed", "host"]
    assert len(stored) == 1
    assert (await session.closed)["kind"] == "failed"
    reopened = SessionImpl(storage.reopen())
    assert await reopened.snapshot(CounterDoc, context) == {"count": 2}
    await reopened.close(context)


async def test_keeps_what_host_listeners_get_exactly_what_was_stored_held_changes_included() -> None:
    """Upstream ``test/session-adoption.test.ts:266``."""

    storage, session = _open()
    conversation_id = await create_conversation(session)
    await session.commit(lambda tx: _create_note(tx, conversation_id), context)
    observed: list[ObservedCommit] = []
    stored: list[CommitPublication] = []
    def own_store(publication: ObservedCommit, _context: Context) -> None:
        storage.order.append("observed")
        observed.append(publication)

    def host_store(publication: CommitPublication, _context: Context) -> None:
        storage.order.append("host")
        stored.append(publication)

    session.observe_stored_commits(own_store)
    session.observe_commits(CommitObserverRecorder(host_store))
    storage.order.clear()
    await session.commit(lambda tx: _note_change(tx, conversation_id, "a", 1), context, DEFERRED)
    # Held in memory: observed at once, nothing stored.
    assert storage.order == ["observed"]
    await session.commit(lambda tx: _note_change(tx, conversation_id, "b", 2), context)
    assert storage.order == ["observed", "adopt", "adopted", "resolve", "observed", "host"]
    assert observed[1]["kind"] == "stored"
    assert [change["ops"] for change in document_changes(observed[1])] == [[["s", ["b"], 2]]]
    assert len(stored) == 1
    assert [change["ops"] for change in document_changes(stored[0])] == [
        [["s", ["a"], 1], ["s", ["b"], 2]]
    ]
    assert stored[0]["seq"] == observed[1]["seq"]
    await session.close(context)


async def test_publishes_a_store_of_held_changes_to_host_listeners_once_storage_resolved() -> None:
    """Upstream ``test/session-adoption.test.ts:311``."""

    storage, session = _open()
    conversation_id = await create_conversation(session)
    await session.commit(lambda tx: _create_note(tx, conversation_id), context)
    session.observe_commits(
        CommitObserverRecorder(
            lambda _publication, _context: storage.order.append(f"host adopting={storage.adopting}")
        )
    )
    await session.commit(lambda tx: _note_change(tx, conversation_id, "a", 1), context, DEFERRED)
    storage.order.clear()
    await session.unload_documents()
    assert storage.order == ["adopt", "adopted", "resolve", "host adopting=False"]
    await session.close(context)


class _RenameLog(PosixExecutionEnv):
    """A POSIX environment that records each sidecar rename, the last step of a reclaim."""

    def __init__(self, cwd: str) -> None:
        super().__init__(cwd=cwd)
        self.order: list[str] = []

    async def rename_file(
        self, source_path: str, destination_path: str, context: Context
    ) -> Any:
        self.order.append(os.path.basename(destination_path))
        return await super().rename_file(source_path, destination_path, context)


async def test_publishes_a_jsonl_commit_once_the_storage_reclaimed_its_sidecar_and_resolved(
    tmp_path: Path,
) -> None:
    """Upstream ``test/session-adoption.test.ts:336``.

    A base at the second count makes JSONL rewrite the document's sidecar after adopting the commit.
    """

    Rebased = define_doc(
        {
            "kind": "adoption.rebased",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": lambda value, _ops, _info: value["count"] == 2,
        }
    )
    env = _RenameLog(cwd=str(tmp_path))
    storage = await JsonlStorage.open(str(tmp_path), env, context)
    session = SessionImpl(storage)

    async def count() -> None:
        await session.commit(lambda tx: _rebased_bump(tx, Rebased), context)

    try:
        await count()
        session.observe_commits(
            CommitObserverRecorder(lambda _publication, _context: env.order.append("host"))
        )
        await count()
        env.order.append("resolved")
        assert len(env.order) == 3
        assert re.fullmatch(r"doc-\d+\.jsonl", env.order[0]) is not None
        assert env.order[1] == "host"
        assert env.order[2] == "resolved"
    finally:
        await session.close(context)


async def _rebased_bump(tx: Transaction, token: Any) -> None:
    (await tx.doc(token))["count"] += 1


async def test_publishes_a_store_of_held_changes_that_storage_rejects_after_adopting_it() -> None:
    """Upstream ``test/session-adoption.test.ts:382``."""
    storage, session = _open()
    conversation_id = await create_conversation(session)
    await session.commit(lambda tx: _create_note(tx, conversation_id), context)
    stored: list[list[list[Any]]] = []
    session.observe_commits(
        CommitObserverRecorder(
            lambda publication, _context: stored.append(
                [change["ops"] for change in document_changes(publication)]
            )
        )
    )
    await session.commit(lambda tx: _note_change(tx, conversation_id, "a", 1), context, DEFERRED)
    storage.order.clear()
    storage.fault = "reject_after_adopt"
    with pytest.raises(RuntimeError, match="reclaim failed"):
        await session.unload_documents()
    assert storage.order == ["adopt", "adopted", "resolve"]
    assert stored == [[[["s", ["a"], 1]]]]
    assert await session.closed == {"kind": "failed", "error": storage.reclaim_error}

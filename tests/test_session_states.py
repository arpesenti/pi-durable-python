"""Translated v1.3.0 ``session-states`` suite."""

from __future__ import annotations

from typing import Any

from session_support import (
    context,
    create_conversation,
    document_changes,
    flush,
    open_test_session,
)

from pi_durable import define_doc, define_doc_family
from pi_durable.session.session import SessionImpl

StateDoc = define_doc(
    {
        "kind": "state.state",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"value": 0, "retained": {"label": "stable"}},
    }
)
FamilyDoc = define_doc_family(
    {
        "kind": "state.family",
        "version": 1,
        "scope": "session",
        "initial": lambda seed: {"value": len(seed)},
    }
)


async def create_state() -> tuple[Any, SessionImpl, list[Any]]:
    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(StateDoc)

    await session.commit(create, context)
    await flush()
    return storage, session, publications


async def test_never_creates_an_absent_document() -> None:
    storage, session, _publications = open_test_session()
    commits = len(storage.commits)
    assert await session.document_state(StateDoc, context) is None
    assert await session.document_state(FamilyDoc, "missing", context) is None
    assert len(storage.commits) == commits
    assert storage.mint_count == 0


async def test_returns_an_immediately_hydrated_state_with_contiguous_deliveries() -> None:
    _storage, session, _publications = await create_state()
    baseline = await session.snapshot(StateDoc, context)
    state = await session.document_state(StateDoc, context)
    assert state is not None
    deliveries: list[tuple[Any, int]] = []
    state.subscribe(lambda value, _context, delivery: deliveries.append((value, delivery.sequence)))
    assert state.value is baseline

    async def first(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 1

    async def second(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 2

    await session.commit(first, context)
    await session.commit(second, context)
    await flush()

    assert [sequence for _value, sequence in deliveries] == [0, 1, 2]
    assert [value["value"] for value, _sequence in deliveries] == [0, 1, 2]
    assert state.value is await session.snapshot(StateDoc, context)
    state.dispose()


async def test_creates_independent_disposable_states_for_one_incarnation() -> None:
    _storage, session, _publications = await create_state()
    first = await session.document_state(StateDoc, context)
    second = await session.document_state(StateDoc, context)
    assert first is not None and second is not None
    assert second is not first

    async def change(tx: Any, value: int) -> None:
        (await tx.doc(StateDoc)).value = value

    await session.commit(lambda tx: change(tx, 1), context)
    await flush()
    assert first.value is not None and first.value["value"] == 1
    assert second.value is not None and second.value["value"] == 1

    first.dispose()
    await session.commit(lambda tx: change(tx, 2), context)
    await flush()
    assert first.value is not None and first.value["value"] == 1
    assert second.value is not None and second.value["value"] == 2
    second.dispose()


async def test_shares_exact_committed_value_references_with_chord() -> None:
    _storage, session, publications = await create_state()
    state = await session.document_state(StateDoc, context)
    assert state is not None

    async def change(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 4

    await session.commit(change, context)
    await flush()
    published = document_changes(publications[-1])[0]
    assert state.value is published["value"]
    snapshot = await session.snapshot(StateDoc, context)
    assert snapshot is not None
    assert state.value is not None
    assert state.value["retained"] is snapshot["retained"]
    state.dispose()


async def test_captures_a_late_baseline_without_redelivering_a_covered_commit() -> None:
    _storage, session, _publications = await create_state()

    async def change(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 1

    await session.commit(change, context)
    state = await session.document_state(StateDoc, context)
    assert state is not None
    deliveries: list[int] = []
    state.subscribe(lambda _value, _context, delivery: deliveries.append(delivery.sequence))
    await flush()
    assert state.value is not None and state.value["value"] == 1
    assert deliveries == [0]
    state.dispose()


async def test_publishes_null_retirement_and_never_follows_a_replacement() -> None:
    _storage, session, _publications = await create_state()
    old_state = await session.document_state(StateDoc, context)
    assert old_state is not None

    async def replace(tx: Any) -> None:
        await tx.retire_doc(StateDoc)
        (await tx.doc(StateDoc)).value = 10

    await session.commit(replace, context)
    await flush()
    assert old_state.value is None

    replacement = await session.document_state(StateDoc, context)
    assert replacement is not None
    assert replacement.value is not None
    assert replacement.value["value"] == 10

    async def update(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 11

    await session.commit(update, context)
    await flush()
    assert old_state.value is None
    assert replacement.value is not None
    assert replacement.value["value"] == 11
    old_state.dispose()
    replacement.dispose()


async def test_cold_loads_a_definition_free_fork_copy() -> None:
    Copied = define_doc(
        {
            "kind": "state.copied",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"value": 0},
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> Any:
        created = await tx.append_entry(parent_id, {"kind": "point"})
        (await tx.doc(Copied, parent_id)).value = 7
        return created

    entry = await session.commit(create_parent, context)

    async def fork(tx: Any) -> Any:
        child = await tx.fork_conversation(
            parent_id, entry["id"], {"ownership": {"kind": "ownerless"}}
        )
        return child["id"]

    child_id = await session.commit(fork, context)
    reads = storage.document_read_count
    state = await session.document_state(Copied, child_id, context)
    assert state is not None
    assert state.value == {"value": 7}
    assert storage.document_read_count > reads
    state.dispose()


async def test_hydrates_a_migrated_tracker_without_writing_and_skips_an_equal_base() -> None:
    Old = define_doc(
        {
            "kind": "state.migration",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"value": 3},
        }
    )
    Current = define_doc(
        {
            "kind": "state.migration",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"value": 0, "migrated": False},
            "migrate": lambda value, _from_version: {"value": value["value"], "migrated": True},
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)
    state = await session.document_state(Current, context)
    assert state is not None
    assert state.value == {"value": 3, "migrated": True}
    assert len(storage.commits) == commits
    baseline = state.value

    async def touch(tx: Any) -> None:
        await tx.doc(Current)

    await session.commit(touch, context)
    await flush()
    assert len(storage.commits) == commits + 1
    assert state.value is baseline

    async def update(tx: Any) -> None:
        (await tx.doc(Current)).value = 4

    await session.commit(update, context)
    await flush()
    assert state.value == {"value": 4, "migrated": True}
    state.dispose()


async def test_continues_from_exact_committed_values_after_cache_unload() -> None:
    storage, session, _publications = await create_state()
    state = await session.document_state(StateDoc, context)
    assert state is not None
    baseline = state.value
    reads = storage.document_read_count
    await session.unload_documents()
    reloaded = await session.snapshot(StateDoc, context)
    assert reloaded == baseline
    assert reloaded is not baseline
    assert storage.document_read_count > reads

    async def update(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 6

    await session.commit(update, context)
    await flush()
    assert state.value is not None and state.value["value"] == 6
    state.dispose()


async def test_exposes_trusted_shared_values_without_freezing() -> None:
    _storage, session, _publications = await create_state()
    snapshot = await session.snapshot(StateDoc, context)
    state = await session.document_state(StateDoc, context)
    assert state is not None
    assert state.value is snapshot
    assert type(state.value) is dict
    assert type(state.value["retained"]) is dict
    state.dispose()

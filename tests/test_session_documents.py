"""Translated v1.3.0 ``session-documents`` definitions, drafts, and retirement cases."""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from session_support import (
    context,
    create_conversation,
    document_changes,
    open_test_session,
)

from pi_durable import define_doc, define_doc_family
from pi_durable.documents import DocToken
from pi_durable.ids import ConversationId, TaskId, id_from_number
from pi_durable.session.session import SessionImpl


async def flush() -> None:
    await asyncio.sleep(0)


live_init_count = 0


def live_initial() -> dict[str, Any]:
    global live_init_count
    live_init_count += 1
    return {"items": [], "nested": {"count": 0}, "other": {"label": "x"}}


LiveDoc = define_doc(
    {
        "kind": "test.live",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": live_initial,
    }
)
RewindableLiveDoc = define_doc(
    {
        "kind": "test.live",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {"items": [], "nested": {"count": 0}, "other": {"label": "x"}},
    }
)
LiveDocV2 = define_doc(
    {
        "kind": "test.live",
        "version": 2,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"items": [], "nested": {"count": 0}, "other": {"label": "x"}},
    }
)
CounterDoc = define_doc(
    {"kind": "test.counter", "version": 1, "scope": "session", "initial": lambda: {"count": 0}}
)
TaskScopedDoc = define_doc(
    {"kind": "test.task-scoped", "version": 1, "scope": "task", "initial": lambda: {"count": 0}}
)

seeds: list[str] = []


def member_initial(seed: str) -> dict[str, Any]:
    seeds.append(seed)
    return {"seed": seed, "hits": 0}


MemberDoc = define_doc_family(
    {
        "kind": "test.member",
        "version": 1,
        "family": True,
        "scope": "session",
        "initial": member_initial,
    }
)

async def setup_live() -> tuple[Any, SessionImpl, list[Any], ConversationId]:
    storage, session, publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def create(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live["items"].push("a", "b")

    await session.commit(create, context)
    return storage, session, publications, conversation_id


async def test_creates_an_initial_base_on_first_access_and_adopts_it_after_storage_success() -> None:
    storage, session, publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def change(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live.message = "hello"

    await session.commit(change, context)
    writes = storage.commits[-1]
    assert len(writes) == 1
    written = writes[0]
    assert written["type"] == "document.create"
    assert written["record"]["kind"] == "test.live"
    assert written["record"]["scope"] == {
        "kind": "conversation",
        "conversationId": conversation_id,
    }
    assert written["record"]["history"] == "latest"
    assert written["record"]["fork"] == "initial"
    assert written["content"] == {
        "kind": "base",
        "version": 1,
        "value": {
            "message": "hello",
            "items": [],
            "nested": {"count": 0},
            "other": {"label": "x"},
        },
    }
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot == {
        "message": "hello",
        "items": [],
        "nested": {"count": 0},
        "other": {"label": "x"},
    }
    await flush()
    publication = publications[-1]
    published = document_changes(publication)[0]
    assert published["record"]["createdAt"] == publication["seq"]
    assert published["value"] is snapshot
    assert published["conversationId"] == conversation_id
    admitted = cast(Any, storage.admitted_commits[-1][0])
    assert admitted["content"]["value"] is published["value"]


async def test_returns_shared_snapshots_and_keeps_prior_revisions_stable() -> None:
    _storage, session, _publications, conversation_id = await setup_live()
    first = await session.snapshot(LiveDoc, conversation_id, context)
    assert first is not None
    assert await session.snapshot(LiveDoc, conversation_id, context) is first

    async def change(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id)).nested.count = 1

    await session.commit(change, context)
    second = await session.snapshot(LiveDoc, conversation_id, context)
    assert second is not None
    assert second is not first
    first_value = cast(Any, first)
    second_value = cast(Any, second)
    assert first_value["nested"]["count"] == 0
    assert second_value["nested"]["count"] == 1
    # Unchanged subtrees are structurally shared between immutable revisions.
    assert second_value["items"] is first_value["items"]
    assert second_value["other"] is first_value["other"]


async def test_adopts_by_pointer_swap_and_shares_operation_payloads_with_the_revision() -> None:
    storage, session, publications, conversation_id = await setup_live()

    async def change(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live.other = {"label": "y"}
        live["items"].push("c")

    await session.commit(change, context)
    await flush()
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None
    published = document_changes(publications[-1])[0]
    assert published["value"] is snapshot
    admitted = next(
        write for write in storage.admitted_commits[-1] if write["type"] == "document.change"
    )
    assert admitted["content"]["kind"] == "delta"
    assert published["ops"] is admitted["content"]["ops"]
    set_op = next(op for op in published["ops"] if op[0] == "s")
    assert set_op == ["s", ["other"], {"label": "y"}]
    assert set_op[2] is snapshot["other"]
    push_op = next(op for op in published["ops"] if op[0] == "p")
    assert push_op == ["p", ["items"], 2, 0, ["c"]]
    assert push_op[4][0] is snapshot["items"][-1]


async def test_emits_assignment_nested_mutation_and_delete_operations() -> None:
    _storage, session, publications, conversation_id = await setup_live()

    async def change(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live.nested.count = 3
        live.extra = {"nested": [1, 2]}
        del live.other

    await session.commit(change, context)
    await flush()
    assert document_changes(publications[-1])[0]["ops"] == [
        ["s", ["extra"], {"nested": [1, 2]}],
        ["d", ["other"]],
        ["s", ["nested", "count"], 3],
    ]
    assert await session.snapshot(LiveDoc, conversation_id, context) == {
        "items": ["a", "b"],
        "nested": {"count": 3},
        "extra": {"nested": [1, 2]},
    }


async def test_copies_assigned_values_per_placement() -> None:
    _storage, session, _publications, conversation_id = await setup_live()
    shared = {"label": "shared"}

    async def change(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live.other = shared
        live["copy"] = shared
        shared["label"] = "mutated"

    await session.commit(change, context)
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None
    assert snapshot["other"] == {"label": "shared"}
    assert snapshot["copy"] == {"label": "shared"}
    assert snapshot["copy"] is not snapshot["other"]
    assert snapshot["other"] is not shared


async def test_suppresses_writes_and_publications_for_empty_batches() -> None:
    storage, session, publications, conversation_id = await setup_live()
    await flush()
    commits = len(storage.commits)
    published = len(publications)

    async def change(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live.nested.count = 0
        live["items"].push("z")
        live["items"].pop()

    await session.commit(change, context)
    await flush()
    assert len(storage.commits) == commits
    assert len(publications) == published


async def test_writes_and_publishes_replayable_nonempty_structural_no_ops() -> None:
    storage, session, publications, conversation_id = await setup_live()
    before = await session.snapshot(LiveDoc, conversation_id, context)
    assert before is not None

    async def change(tx: Any) -> None:
        items = (await tx.doc(LiveDoc, conversation_id))["items"]
        first = items.shift()
        items.unshift(first)

    await session.commit(change, context)
    await flush()
    writes = storage.commits[-1]
    assert writes[0]["type"] == "document.change"
    assert writes[0]["content"]["kind"] == "delta"
    after = await session.snapshot(LiveDoc, conversation_id, context)
    assert after == before
    assert after is not before
    assert len(document_changes(publications[-1])[0]["ops"]) > 0


async def test_revokes_escaped_drafts_when_the_callback_settles() -> None:
    _storage, session, _publications, conversation_id = await setup_live()
    escaped: Any = None
    items: Any = None

    async def change(tx: Any) -> None:
        nonlocal escaped, items
        escaped = await tx.doc(LiveDoc, conversation_id)
        items = escaped["items"]
        escaped.message = "inside"

    await session.commit(change, context)
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        _ = escaped.message
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        escaped.message = "outside"
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        len(items)
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        items.push("outside")
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None and snapshot["message"] == "inside"

    returned = await session.commit(lambda tx: tx.doc(LiveDoc, conversation_id), context)
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        _ = returned.message


async def test_aborts_every_change_when_the_callback_fails() -> None:
    storage, session, _publications, conversation_id = await setup_live()
    before = await session.snapshot(LiveDoc, conversation_id, context)
    commits = len(storage.commits)

    async def change(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        counter = await tx.doc(CounterDoc)
        live.message = "lost"
        counter.count = 5
        raise RuntimeError("callback failed")

    with pytest.raises(RuntimeError, match="callback failed"):
        await session.commit(change, context)
    assert len(storage.commits) == commits
    assert await session.snapshot(LiveDoc, conversation_id, context) is before
    assert await session.snapshot(CounterDoc, context) is None

    async def update(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id)).message = "kept"

    await session.commit(update, context)
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None and snapshot["message"] == "kept"


async def test_memoizes_concurrent_duplicate_acquisition_and_initializes_once() -> None:
    storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    initialized = live_init_count
    mints = storage.mint_count

    async def change(tx: Any) -> None:
        first, second = await asyncio.gather(
            tx.doc(LiveDoc, conversation_id), tx.doc(LiveDoc, conversation_id)
        )
        assert first is second
        assert await tx.doc(LiveDoc, conversation_id) is first
        first.message = "once"

    await session.commit(change, context)
    assert live_init_count == initialized + 1
    assert storage.mint_count == mints + 1
    creates = [
        write for write in storage.commits[-1] if write["type"] == "document.create"
    ]
    assert len(creates) == 1


async def test_uses_the_first_family_seed_and_ignores_seeds_for_existing_members() -> None:
    _storage, session, _publications = open_test_session()
    seeds.clear()

    async def create(tx: Any) -> None:
        first = await tx.doc(MemberDoc, "k", "first")
        second = await tx.doc(MemberDoc, "k", "second")
        assert second is first
        first.hits += 1

    await session.commit(create, context)

    async def update(tx: Any) -> None:
        (await tx.doc(MemberDoc, "k", "third")).hits += 1
        (await tx.doc(MemberDoc, "other", "fourth")).hits += 1

    await session.commit(update, context)
    assert seeds == ["first", "fourth"]
    assert await session.snapshot(MemberDoc, "k", context) == {"seed": "first", "hits": 2}
    assert await session.snapshot(MemberDoc, "other", context) == {"seed": "fourth", "hits": 1}


async def test_scopes_nested_edits_to_the_transaction_and_rejects_after_settlement() -> None:
    _storage, session, _publications, conversation_id = await setup_live()
    captured: Any = None

    async def change(tx: Any) -> None:
        nonlocal captured
        captured = tx
        live = await tx.doc(LiveDoc, conversation_id)
        live.nested.count = 1

    await session.commit(change, context)
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        await captured.doc(LiveDoc, conversation_id)
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        await captured.conversation(conversation_id)


async def test_rejects_tokens_whose_semantics_or_version_disagree_with_the_stored_incarnation() -> None:
    _storage, session, _publications, conversation_id = await setup_live()
    with pytest.raises(TypeError, match="does not match the supplied definition semantics"):
        await session.snapshot(RewindableLiveDoc, conversation_id, context)
    with pytest.raises(TypeError, match="does not match the supplied definition semantics"):
        await session.commit(
            lambda tx: tx.doc(RewindableLiveDoc, conversation_id), context
        )
    with pytest.raises(ValueError, match="requires migration from version 1"):
        await session.commit(lambda tx: tx.doc(LiveDocV2, conversation_id), context)


async def test_rejects_non_json_initializer_values_and_draft_placements_before_admission() -> None:
    storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    DateDoc = define_doc(
        {
            "kind": "test.date",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"at": object()},
        }
    )
    with pytest.raises(TypeError, match="strict JSON"):
        await session.commit(lambda tx: tx.doc(DateDoc), context)

    async def invalid_placement(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live["items"].push(float("nan"))

    commits = len(storage.commits)
    with pytest.raises(TypeError, match="strict JSON"):
        await session.commit(invalid_placement, context)
    assert len(storage.commits) == commits
    assert await session.snapshot(LiveDoc, conversation_id, context) is None


async def test_rolls_back_prepared_documents_when_batch_assembly_fails() -> None:
    storage, session, _publications, conversation_id = await setup_live()

    async def create_counter(tx: Any) -> None:
        (await tx.doc(CounterDoc)).count = 1

    await session.commit(create_counter, context)
    live = await session.snapshot(LiveDoc, conversation_id, context)
    counter = await session.snapshot(CounterDoc, context)
    commits = len(storage.commits)

    async def change(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id)).message = "lost"
        (await tx.doc(CounterDoc)).count = 2
        # Replacing a missing task fails during assembly, after every change was prepared.
        tx.set_task(
            {
                "id": id_from_number(999),
                "conversationId": conversation_id,
                "kind": "missing",
                "version": 1,
                "input": None,
                "background": False,
                "abortRequested": False,
                "state": {"status": "pending", "checkpoint": {"phase": "start"}},
            }
        )

    with pytest.raises(ValueError, match="Task 999 does not exist"):
        await session.commit(change, context)
    assert len(storage.commits) == commits
    assert await session.snapshot(LiveDoc, conversation_id, context) is live
    assert await session.snapshot(CounterDoc, context) is counter

    async def next_change(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id)).message = "next"
        (await tx.doc(CounterDoc)).count = 3

    await session.commit(next_change, context)
    counter_after = await session.snapshot(CounterDoc, context)
    assert counter_after is not None and counter_after["count"] == 3


async def test_retires_documents_and_creates_a_new_incarnation_at_the_same_address() -> None:
    storage, session, publications, conversation_id = await setup_live()
    await flush()
    old_id = document_changes(publications[-1])[0]["record"]["id"]
    replacement: Any = None

    async def change(tx: Any) -> None:
        nonlocal replacement
        live = await tx.doc(LiveDoc, conversation_id)
        live.message = "final"
        await tx.retire_doc(LiveDoc, conversation_id)
        replacement = await tx.doc(LiveDoc, conversation_id)
        assert replacement is not live
        replacement.message = "new"

    await session.commit(change, context)
    writes = storage.commits[-1]
    assert len(writes) == 3
    assert any(
        write["type"] == "document.change" and write["id"] == old_id for write in writes
    )
    assert {"type": "document.retire", "id": old_id} in writes
    assert any(
        write["type"] == "document.create"
        and write["record"]["kind"] == "test.live"
        for write in writes
    )
    await flush()
    publication = publications[-1]
    retired, created = document_changes(publication)
    assert retired["record"]["id"] == old_id
    assert retired["value"] is None
    assert retired["ops"] == []
    assert retired["record"]["retiredAt"] == publication["seq"]
    assert created["record"]["id"] != old_id
    assert created["record"]["createdAt"] == publication["seq"]
    assert created["value"]["message"] == "new"
    assert created["ops"] == []
    assert await session.snapshot(LiveDoc, conversation_id, context) is created["value"]

    await session.commit(
        lambda tx: tx.retire_doc(LiveDoc, conversation_id), context
    )
    assert await session.snapshot(LiveDoc, conversation_id, context) is None
    await session.unload_documents()
    assert await session.snapshot(LiveDoc, conversation_id, context) is None
    commits = len(storage.commits)
    await session.commit(
        lambda tx: tx.retire_doc(LiveDoc, conversation_id), context
    )
    assert len(storage.commits) == commits


async def test_retires_without_acquisition_and_recreates_existing_and_absent_addresses() -> None:
    storage, session, publications, conversation_id = await setup_live()
    await flush()
    old_id = document_changes(publications[-1])[0]["record"]["id"]
    await session.unload_documents()

    async def recreate_existing(tx: Any) -> None:
        retired = asyncio.ensure_future(tx.retire_doc(LiveDoc, conversation_id))
        replacement = asyncio.ensure_future(tx.doc(LiveDoc, conversation_id))
        live, _ = await asyncio.gather(replacement, retired)
        live.message = "replacement"

    await session.commit(recreate_existing, context)
    writes = storage.commits[-1]
    assert len(writes) == 2
    assert {"type": "document.retire", "id": old_id} in writes
    assert any(
        write["type"] == "document.create"
        and write["record"]["kind"] == "test.live"
        for write in writes
    )

    async def recreate_absent(tx: Any) -> None:
        retired = asyncio.ensure_future(tx.retire_doc(MemberDoc, "absent"))
        replacement = asyncio.ensure_future(tx.doc(MemberDoc, "absent", "seed"))
        member, _ = await asyncio.gather(replacement, retired)
        member.hits = 1

    await session.commit(recreate_absent, context)
    writes = storage.commits[-1]
    assert len([write for write in writes if write["type"] == "document.retire"]) == 0
    assert len([write for write in writes if write["type"] == "document.create"]) == 1
    assert await session.snapshot(MemberDoc, "absent", context) == {"seed": "seed", "hits": 1}


async def test_retires_the_existing_incarnation_when_retirement_races_a_pending_acquisition() -> None:
    storage, session, publications, conversation_id = await setup_live()
    await flush()
    old_id = document_changes(publications[-1])[0]["record"]["id"]

    async def change(tx: Any) -> None:
        acquired = asyncio.ensure_future(tx.doc(LiveDoc, conversation_id))
        retired = asyncio.ensure_future(tx.retire_doc(LiveDoc, conversation_id))
        live, _ = await asyncio.gather(acquired, retired)
        live.message = "final"

    await session.commit(change, context)
    writes = storage.commits[-1]
    assert len(writes) == 2
    assert any(
        write["type"] == "document.change"
        and write["id"] == old_id
        and write["content"]["kind"] == "delta"
        for write in writes
    )
    assert {"type": "document.retire", "id": old_id} in writes
    assert await session.snapshot(LiveDoc, conversation_id, context) is None

    async def recreate(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id)).message = "second"

    await session.commit(recreate, context)
    await flush()
    second_id = document_changes(publications[-1])[0]["record"]["id"]

    async def race(tx: Any) -> None:
        acquired = asyncio.ensure_future(tx.doc(LiveDoc, conversation_id))
        retired = asyncio.ensure_future(tx.retire_doc(LiveDoc, conversation_id))
        recreated = asyncio.ensure_future(tx.doc(LiveDoc, conversation_id))
        old, fresh = await asyncio.gather(acquired, recreated)
        await retired
        assert fresh is not old
        fresh.message = "third"

    await session.commit(race, context)
    writes = storage.commits[-1]
    assert len(writes) == 2
    assert {"type": "document.retire", "id": second_id} in writes
    assert any(
        write["type"] == "document.create"
        and write["record"]["kind"] == "test.live"
        for write in writes
    )
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None and snapshot["message"] == "third"


async def test_reloads_an_unloaded_document_from_storage() -> None:
    _storage, session, _publications, conversation_id = await setup_live()
    loaded = await session.snapshot(LiveDoc, conversation_id, context)
    await session.unload_documents()
    reloaded = await session.snapshot(LiveDoc, conversation_id, context)
    assert reloaded is not loaded
    assert reloaded == loaded

    async def change(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id))["items"].push("c")

    await session.commit(change, context)
    await session.unload_documents()
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None and snapshot["items"] == ["a", "b", "c"]


async def test_validates_document_owners() -> None:
    _storage, session, _publications = open_test_session()
    with pytest.raises(ValueError, match="Task 4242 does not exist"):
        await session.commit(lambda tx: tx.doc(TaskScopedDoc, TaskId(4242)), context)
    with pytest.raises(ValueError, match="Conversation 4242 does not exist"):
        await session.commit(lambda tx: tx.doc(LiveDoc, ConversationId(4242)), context)


def test_document_token_type_is_the_runtime_token() -> None:
    assert isinstance(LiveDoc, DocToken)


async def test_rejects_a_callback_that_succeeds_with_a_pending_acquisition_and_drains_it() -> None:
    storage, session, _publications, conversation_id = await setup_live()
    await session.unload_documents()
    gate = storage.hold_find_document()
    commits = len(storage.commits)
    pending: asyncio.Future[Any] | None = None

    async def change(tx: Any) -> None:
        nonlocal pending
        pending = asyncio.ensure_future(tx.doc(LiveDoc, conversation_id))
        await asyncio.sleep(0)

    commit = asyncio.ensure_future(session.commit(change, context))
    await gate.entered.wait()
    settled: list[bool] = []
    commit.add_done_callback(lambda _future: settled.append(True))
    await flush()
    assert settled == []
    gate.release()
    with pytest.raises(RuntimeError, match="pending Tx operations"):
        await commit
    assert pending is not None
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        await pending
    assert len(storage.commits) == commits

    async def after(tx: Any) -> None:
        (await tx.doc(LiveDoc, conversation_id)).message = "after"

    await session.commit(after, context)
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None and snapshot["message"] == "after"


async def test_does_not_initialize_or_mint_for_an_absent_acquisition_after_settlement() -> None:
    storage, session, _publications = open_test_session()
    initialized = 0

    def initial() -> dict[str, int]:
        nonlocal initialized
        initialized += 1
        return {"count": 0}

    LateDoc = define_doc(
        {"kind": "test.late", "version": 1, "scope": "session", "initial": initial}
    )
    gate = storage.hold_find_document()
    mints = storage.mint_count
    pending: asyncio.Future[Any] | None = None

    async def change(tx: Any) -> None:
        nonlocal pending
        pending = asyncio.ensure_future(tx.doc(LateDoc))
        await asyncio.sleep(0)

    commit = asyncio.ensure_future(session.commit(change, context))
    await gate.entered.wait()
    gate.release()
    with pytest.raises(RuntimeError, match="pending Tx operations"):
        await commit
    assert pending is not None
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        await pending
    assert initialized == 0
    assert storage.mint_count == mints


async def test_rejects_with_the_callback_error_when_it_fails_with_a_pending_acquisition() -> None:
    storage, session, _publications, conversation_id = await setup_live()
    await session.unload_documents()
    gate = storage.hold_find_document()
    pending: asyncio.Future[Any] | None = None

    async def change(tx: Any) -> None:
        nonlocal pending
        pending = asyncio.ensure_future(tx.doc(LiveDoc, conversation_id))
        await asyncio.sleep(0)
        raise RuntimeError("callback failed")

    commit = asyncio.ensure_future(session.commit(change, context))
    await gate.entered.wait()
    gate.release()
    with pytest.raises(RuntimeError, match="callback failed"):
        await commit
    assert pending is not None
    with pytest.raises(RuntimeError, match="Transaction has settled"):
        await pending


async def test_replays_nested_mutations_inside_assigned_array_elements() -> None:
    _storage, session, publications, conversation_id = await setup_live()

    async def change(tx: Any) -> None:
        items = (await tx.doc(LiveDoc, conversation_id))["items"]
        items[0] = {"n": 1}
        items[0]["n"] = 2

    await session.commit(change, context)
    expected = {
        "items": [{"n": 2}, "b"],
        "nested": {"count": 0},
        "other": {"label": "x"},
    }
    assert document_changes(publications[-1])[0]["ops"] == [["s", ["items", 0], {"n": 2}]]
    assert await session.snapshot(LiveDoc, conversation_id, context) == expected
    await session.unload_documents()
    assert await session.snapshot(LiveDoc, conversation_id, context) == expected


async def test_emits_and_replays_array_deletion_operations() -> None:
    _storage, session, publications, conversation_id = await setup_live()

    async def change(tx: Any) -> None:
        del (await tx.doc(LiveDoc, conversation_id))["items"][0]

    await session.commit(change, context)
    assert document_changes(publications[-1])[0]["ops"] == [["p", ["items"], 0, 1, []]]
    snapshot = await session.snapshot(LiveDoc, conversation_id, context)
    assert snapshot is not None and snapshot["items"] == ["b"]
    await session.unload_documents()
    reloaded = await session.snapshot(LiveDoc, conversation_id, context)
    assert reloaded is not None and reloaded["items"] == ["b"]

"""Translated v1.3.0 ``session-checkpoints-migrations`` suite.

Covers base-versus-delta selection, the stored delta count, migration in memory and on write,
version compatibility, lazy access, and historical rewindable reads across forks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, cast

import pytest
from session_support import (
    context,
    create_conversation,
    document_changes,
    flush,
    open_test_session,
)

from pi_durable import define_doc, define_doc_family
from pi_durable.errors import StorageRequestError
from pi_durable.ids import ConversationId, EntryId
from pi_durable.session.session import SessionImpl
from pi_durable.types import StorageWrite


def _document_writes(writes: Sequence[StorageWrite]) -> list[dict[str, Any]]:
    return [
        dict(write)
        for write in writes
        if write["type"] in ("document.create", "document.change", "document.retire")
    ]


def _as_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value)


def _write_kind(write: Any) -> str:
    record = _as_dict(write)
    if record["type"] == "document.change":
        return cast(str, _as_dict(record["content"])["kind"])
    return cast(str, record["type"])


def _changes(storage: Any, *, offset: int = 0) -> list[dict[str, Any]]:
    return [
        dict(write)
        for writes in storage.commits[offset:]
        for write in writes
        if write["type"] == "document.change"
    ]


# ── Checkpoints ─────────────────────────────────────────────────────────────


async def test_calls_checkpoint_when_with_its_definition_as_the_receiver() -> None:
    def checkpoint_when(self: Any, value: Any, _ops: Any, _info: Any) -> bool:
        return bool(value["count"] == self.initial()["count"] + 2)

    ReceiverDoc = define_doc(
        {
            "kind": "checkpoint.receiver",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, _publications = open_test_session()
    for count in range(3):

        async def change(tx: Any, count: int = count) -> None:
            (await tx.doc(ReceiverDoc))["count"] = count

        await session.commit(change, context)
    assert [change["content"]["kind"] for change in _changes(storage)] == ["delta", "base"]


async def test_selects_bases_only_for_nonempty_ordinary_batches_and_passes_the_prepared_value() -> (
    None
):
    calls: list[tuple[Any, Any]] = []
    false_calls: list[tuple[Any, Any]] = []

    def base_checkpoint(value: Any, ops: Any, _info: Any) -> bool:
        calls.append((value, ops))
        return True

    def delta_checkpoint(value: Any, ops: Any, _info: Any) -> bool:
        false_calls.append((value, ops))
        return False

    BaseDoc = define_doc(
        {
            "kind": "checkpoint.base",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"items": []},
            "checkpoint_when": base_checkpoint,
        }
    )
    DeltaDoc = define_doc(
        {
            "kind": "checkpoint.delta",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": delta_checkpoint,
        }
    )
    DefaultDoc = define_doc(
        {
            "kind": "checkpoint.default",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
        }
    )
    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(BaseDoc)
        await tx.doc(DeltaDoc)
        await tx.doc(DefaultDoc)

    await session.commit(create, context)
    assert calls == []
    assert false_calls == []
    created = _document_writes(storage.commits[-1])
    assert len(created) == 3
    assert all(
        write["type"] == "document.create" and write["content"]["kind"] == "base"
        for write in created
    )

    async def change(tx: Any) -> None:
        (await tx.doc(BaseDoc))["items"].push("x")
        (await tx.doc(DeltaDoc))["count"] += 1
        (await tx.doc(DefaultDoc))["count"] += 1

    await session.commit(change, context)
    await flush()
    writes = [
        _as_dict(write)
        for write in storage.admitted_commits[-1]
        if write["type"] == "document.change"
    ]
    assert [_write_kind(write) for write in writes] == ["base", "delta", "delta"]
    assert len(calls) == 1
    assert len(false_calls) == 1
    snapshot = await session.snapshot(BaseDoc, context)
    delta_snapshot = await session.snapshot(DeltaDoc, context)
    published, published_delta, _default = document_changes(publications[-1])
    assert calls[0][0] is snapshot
    assert calls[0][1] is published["ops"]
    assert false_calls[0][0] is delta_snapshot
    assert false_calls[0][1] is published_delta["ops"]
    assert _as_dict(writes[1]["content"])["ops"] is false_calls[0][1]
    assert _as_dict(writes[0]["content"])["value"] is snapshot


async def test_passes_the_stored_delta_count_including_after_unload_and_version_bases() -> None:
    seen: list[int] = []

    def checkpoint_v1(_value: Any, _ops: Any, info: Any) -> bool:
        seen.append(info["deltasSinceBase"])
        return bool(info["deltasSinceBase"] >= 2)

    def checkpoint_v2(_value: Any, _ops: Any, info: Any) -> bool:
        seen.append(info["deltasSinceBase"])
        return False

    V1 = define_doc(
        {
            "kind": "checkpoint.deltas-since-base",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": checkpoint_v1,
        }
    )
    V2 = define_doc(
        {
            "kind": "checkpoint.deltas-since-base",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "migrate": lambda value, _from_version: {"count": value["count"]},
            "checkpoint_when": checkpoint_v2,
        }
    )
    storage, session, _publications = open_test_session()

    async def increment(token: Any) -> None:
        async def change(tx: Any) -> None:
            (await tx.doc(token))["count"] += 1

        await session.commit(change, context)

    async def create(tx: Any) -> None:
        await tx.doc(V1)

    await session.commit(create, context)
    await increment(V1)
    await increment(V1)
    await increment(V1)
    await session.unload_documents()
    await increment(V1)
    assert seen == [0, 1, 2, 0]
    kinds = [_write_kind(writes[0]) for writes in storage.commits[-4:]]
    assert kinds == ["delta", "delta", "base", "delta"]

    # A required version base resets the count without calling the predicate.
    await increment(V2)
    await increment(V2)
    assert seen == [0, 1, 2, 0, 0]
    version_base = _as_dict(storage.commits[-2][0])["content"]
    assert version_base["kind"] == "base"
    assert version_base["version"] == 2


async def test_skips_the_predicate_for_empty_batches_but_calls_it_for_structural_no_ops() -> None:
    calls: list[int] = []

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        calls.append(1)
        return False

    Doc = define_doc(
        {
            "kind": "checkpoint.no-op",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"items": ["a", "b"]},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Doc)

    await session.commit(create, context)
    commits = len(storage.commits)

    async def empty(tx: Any) -> None:
        value = await tx.doc(Doc)
        value["items"].push("x")
        value["items"].pop()

    await session.commit(empty, context)
    assert len(calls) == 0
    assert len(storage.commits) == commits

    async def structural(tx: Any) -> None:
        value = await tx.doc(Doc)
        first = value["items"].shift()
        value["items"].unshift(first)

    await session.commit(structural, context)
    assert len(calls) == 1
    assert storage.commits[-1][0]["type"] == "document.change"
    assert storage.commits[-1][0]["content"]["kind"] == "delta"


async def test_rolls_back_every_prepared_document_when_a_checkpoint_predicate_throws() -> None:
    state = {"throw": True, "first_calls": 0}

    def first_checkpoint(_value: Any, _ops: Any, _info: Any) -> bool:
        state["first_calls"] += 1
        return False

    def second_checkpoint(_value: Any, _ops: Any, _info: Any) -> bool:
        if state["throw"]:
            raise RuntimeError("checkpoint failed")
        return False

    First = define_doc(
        {
            "kind": "checkpoint.rollback.first",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": first_checkpoint,
        }
    )
    Second = define_doc(
        {
            "kind": "checkpoint.rollback.second",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": second_checkpoint,
        }
    )
    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(First)
        await tx.doc(Second)

    await session.commit(create, context)
    await flush()
    first = await session.snapshot(First, context)
    second = await session.snapshot(Second, context)
    commits = len(storage.commits)
    published = len(publications)

    async def failing(tx: Any) -> None:
        (await tx.doc(First))["count"] = 1
        (await tx.doc(Second))["count"] = 2

    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await session.commit(failing, context)
    await flush()
    assert state["first_calls"] == 1
    assert len(storage.commits) == commits
    assert len(publications) == published
    assert await session.snapshot(First, context) is first
    assert await session.snapshot(Second, context) is second

    state["throw"] = False

    async def succeeding(tx: Any) -> None:
        (await tx.doc(First))["count"] = 3
        (await tx.doc(Second))["count"] = 4

    await session.commit(succeeding, context)
    assert await session.snapshot(First, context) == {"count": 3}
    assert await session.snapshot(Second, context) == {"count": 4}


async def test_persists_repeated_false_decisions_as_deltas_and_replays_the_complete_tail() -> None:
    calls: list[int] = []

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        calls.append(1)
        return False

    Doc = define_doc(
        {
            "kind": "checkpoint.tail",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"values": []},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Doc)

    await session.commit(create, context)
    for value in range(1, 9):

        async def append(tx: Any, value: int = value) -> None:
            (await tx.doc(Doc))["values"].push(value)

        await session.commit(append, context)
    changes = _changes(storage)
    assert len(changes) == 8
    assert all(change["content"]["kind"] == "delta" for change in changes)
    assert len(calls) == 8
    await session.unload_documents()
    assert await session.snapshot(Doc, context) == {"values": [1, 2, 3, 4, 5, 6, 7, 8]}


async def test_keeps_a_prepared_root_replacement_as_a_delta_when_the_predicate_is_false() -> None:
    initial = {f"field{index}": 0 for index in range(4_100)}
    Doc = define_doc(
        {
            "kind": "checkpoint.root-replacement",
            "version": 1,
            "scope": "session",
            "initial": lambda: initial,
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Doc)

    await session.commit(create, context)

    async def replace(tx: Any) -> None:
        value = await tx.doc(Doc)
        for index in range(4_100):
            value[f"field{index}"] = 1

    await session.commit(replace, context)
    write = storage.commits[-1][0]
    assert write["type"] == "document.change"
    assert write["content"]["kind"] == "delta"
    assert len(write["content"]["ops"]) == 1
    assert write["content"]["ops"][0][0] == "r"
    await session.unload_documents()
    snapshot = await session.snapshot(Doc, context)
    assert snapshot is not None
    assert snapshot["field4099"] == 1


async def test_uses_ordinary_checkpoint_selection_before_retirement() -> None:
    calls: list[int] = []

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        calls.append(1)
        return True

    Doc = define_doc(
        {
            "kind": "checkpoint.retire",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Doc)

    await session.commit(create, context)

    async def change_and_retire(tx: Any) -> None:
        (await tx.doc(Doc))["count"] = 1
        await tx.retire_doc(Doc)

    await session.commit(change_and_retire, context)
    assert len(calls) == 1
    writes = _document_writes(storage.commits[-1])
    assert len(writes) == 2
    assert writes[0]["type"] == "document.change"
    assert writes[0]["content"]["kind"] == "base"
    assert writes[1]["type"] == "document.retire"


# ── Migrations ──────────────────────────────────────────────────────────────


async def test_migrates_read_only_once_per_cold_load_copies_the_result_and_writes_nothing() -> (
    None
):
    Old = define_doc(
        {
            "kind": "migration.read-only",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 2},
        }
    )
    calls: list[int] = []
    retained: dict[str, Any] = {}

    def migrate(value: Any, from_version: int) -> Any:
        assert from_version == 1
        calls.append(1)
        retained.update({"count": value["count"], "labels": ["migrated"]})
        return retained

    Current = define_doc(
        {
            "kind": "migration.read-only",
            "version": 3,
            "scope": "session",
            "initial": lambda: {"count": 0, "labels": []},
            "migrate": migrate,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)

    first = await session.snapshot(Current, context)
    assert await session.snapshot(Current, context) is first
    assert len(calls) == 1
    assert len(storage.commits) == commits
    retained["count"] = 99
    retained["labels"].append("mutated")
    assert first == {"count": 2, "labels": ["migrated"]}

    await session.unload_documents()
    second = await session.snapshot(Current, context)
    assert second is not first
    assert second == first
    assert len(calls) == 2
    assert len(storage.commits) == commits


async def test_writes_the_required_base_on_the_first_transaction_then_writes_deltas() -> None:
    Old = define_doc(
        {
            "kind": "migration.transition",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 4},
        }
    )
    checkpoints: list[int] = []

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        checkpoints.append(1)
        return False

    Current = define_doc(
        {
            "kind": "migration.transition",
            "version": 3,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "migrate": lambda value, from_version: {"count": value["count"] + from_version - 1},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()
    # An observer of the older shape.
    watch = await session.watch_doc(Old, context)
    assert watch is not None
    frames: list[dict[str, Any]] = []
    watch.start(lambda value, ops, _context: frames.append({"value": value, "ops": ops}))
    assert await session.snapshot(Current, context) == {"count": 4}
    snapshot = await session.snapshot(Current, context)
    # An observer of the new shape: the migration changes nothing it sees.
    current = await session.watch_doc(Current, context)
    assert current is not None
    current_frames: list[Any] = []
    current.start(lambda value, _ops, _context: current_frames.append(value))

    async def touch(tx: Any) -> None:
        await tx.doc(Current)

    await session.commit(touch, context)
    await flush()
    write = storage.commits[-1][0]
    assert write["type"] == "document.change"
    assert write["content"]["kind"] == "base"
    assert write["content"]["version"] == 3
    assert write["content"]["value"] == {"count": 4}
    assert checkpoints == []
    # The migration-only base is published, so the older-shape watch receives the new value as a
    # root replacement.
    published = document_changes(publications[-1])
    assert len(published) == 1
    assert published[0]["version"] == 3
    assert published[0]["value"] == {"count": 4}
    assert published[0]["ops"] == []
    assert frames == [{"value": {"count": 4}, "ops": [["r", {"count": 4}]]}]
    assert current_frames == []
    await watch.stop()
    await current.stop()
    assert await session.snapshot(Current, context) is snapshot

    async def edit(tx: Any) -> None:
        (await tx.doc(Current))["count"] = 7

    await session.commit(edit, context)
    assert storage.commits[-1][0]["type"] == "document.change"
    assert storage.commits[-1][0]["content"]["kind"] == "delta"
    assert storage.commits[-1][0]["content"]["version"] == 3
    assert len(checkpoints) == 1
    await session.unload_documents()
    assert await session.snapshot(Current, context) == {"count": 7}


async def test_rolls_migration_and_edits_back_then_coalesces_later_edits_into_one_base() -> None:
    Old = define_doc(
        {
            "kind": "migration.rollback",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 1},
        }
    )
    migrations: list[int] = []

    def migrate(value: Any, _from_version: int) -> Any:
        migrations.append(1)
        return {"count": value["count"], "migrated": True}

    Current = define_doc(
        {
            "kind": "migration.rollback",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0, "migrated": False},
            "migrate": migrate,
        }
    )
    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)

    async def failing(tx: Any) -> None:
        (await tx.doc(Current))["count"] = 8
        raise RuntimeError("rollback")

    with pytest.raises(RuntimeError, match="rollback"):
        await session.commit(failing, context)
    assert len(storage.commits) == commits
    assert await session.snapshot(Current, context) == {"count": 1, "migrated": True}
    assert len(migrations) == 1

    async def succeed(tx: Any) -> None:
        value = await tx.doc(Current)
        value["count"] = 9
        value["migrated"] = False

    await session.commit(succeed, context)
    writes = _document_writes(storage.commits[-1])
    assert len(writes) == 1
    assert writes[0]["type"] == "document.change"
    assert writes[0]["content"]["kind"] == "base"
    assert writes[0]["content"]["version"] == 2
    assert writes[0]["content"]["value"] == {"count": 9, "migrated": False}
    await flush()
    published = document_changes(publications[-1])[0]
    admitted = storage.admitted_commits[-1][0]
    assert admitted["type"] == "document.change"
    assert admitted["content"]["kind"] == "base"
    assert published["value"] is await session.snapshot(Current, context)
    assert published["value"] is admitted["content"]["value"]
    assert len(published["ops"]) > 0
    assert len(migrations) == 1


async def test_strict_checks_migration_results_and_remains_usable_after_rejection() -> None:
    Old = define_doc(
        {
            "kind": "migration.invalid",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 1},
        }
    )
    Invalid = define_doc(
        {
            "kind": "migration.invalid",
            "version": 2,
            "scope": "session",
            "initial": lambda: {},
            "migrate": lambda _value, _from_version: {"invalid": object()},
        }
    )
    Other = define_doc(
        {
            "kind": "migration.invalid.other",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"ok": True},
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)
    with pytest.raises(TypeError, match="strict JSON"):
        await session.snapshot(Invalid, context)

    async def touch(tx: Any) -> None:
        await tx.doc(Invalid)

    with pytest.raises(TypeError, match="strict JSON"):
        await session.commit(touch, context)
    assert len(storage.commits) == commits

    async def touch_other(tx: Any) -> None:
        await tx.doc(Other)

    await session.commit(touch_other, context)
    assert await session.snapshot(Other, context) == {"ok": True}


async def test_rejects_newer_versions_and_older_versions_without_migration() -> None:
    V2 = define_doc(
        {
            "kind": "migration.compatibility",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 2},
        }
    )
    V1 = define_doc(
        {
            "kind": "migration.compatibility",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 1},
        }
    )
    V3WithoutMigration = define_doc(
        {
            "kind": "migration.compatibility",
            "version": 3,
            "scope": "session",
            "initial": lambda: {"count": 3},
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(V2)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)

    with pytest.raises(ValueError, match="newer version 2 than 1"):
        await session.snapshot(V1, context)

    async def touch_v1(tx: Any) -> None:
        await tx.doc(V1)

    with pytest.raises(ValueError, match="newer version 2 than 1"):
        await session.commit(touch_v1, context)
    with pytest.raises(ValueError, match="requires migration from version 2"):
        await session.snapshot(V3WithoutMigration, context)

    async def touch_v3(tx: Any) -> None:
        await tx.doc(V3WithoutMigration)

    with pytest.raises(ValueError, match="requires migration from version 2"):
        await session.commit(touch_v3, context)
    assert len(storage.commits) == commits


async def test_persists_a_required_migration_base_before_retirement_without_the_predicate() -> (
    None
):
    Old = define_doc(
        {
            "kind": "migration.retire",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 1},
        }
    )

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        raise RuntimeError("must not run")

    Current = define_doc(
        {
            "kind": "migration.retire",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "migrate": lambda value, _from_version: {"count": value["count"]},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()

    async def migrate_and_retire(tx: Any) -> None:
        await tx.doc(Current)
        await tx.retire_doc(Current)

    await session.commit(migrate_and_retire, context)
    writes = _document_writes(storage.commits[-1])
    assert len(writes) == 2
    assert writes[0]["type"] == "document.change"
    assert writes[0]["content"]["kind"] == "base"
    assert writes[0]["content"]["version"] == 2
    assert writes[0]["content"]["value"] == {"count": 1}
    assert writes[1]["type"] == "document.retire"


async def test_leaves_unaccessed_older_documents_and_unavailable_definitions_untouched() -> None:
    FirstV1 = define_doc(
        {
            "kind": "migration.lazy.first",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 1},
        }
    )
    SecondV1 = define_doc(
        {
            "kind": "migration.lazy.second",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 2},
        }
    )
    second_migrations: list[int] = []
    FirstV2 = define_doc(
        {
            "kind": "migration.lazy.first",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "migrate": lambda value, _from_version: {"count": value["count"]},
        }
    )
    def migrate_second(value: Any, _from_version: int) -> Any:
        second_migrations.append(1)
        return {"count": value["count"]}

    define_doc(
        {
            "kind": "migration.lazy.second",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "migrate": migrate_second,
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(FirstV1)
        await tx.doc(SecondV1)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)
    assert await session.snapshot(FirstV2, context) == {"count": 1}
    assert len(storage.commits) == commits
    assert second_migrations == []
    record = await storage.find_document(
        {"kind": "migration.lazy.second", "scope": {"kind": "session"}}, "current", context
    )
    assert record is not None
    stored = await storage.document(record["id"], "current", context)
    assert stored is not None
    assert stored["version"] == 1


# ── Historical snapshots ────────────────────────────────────────────────────


HistoryV1 = define_doc(
    {
        "kind": "history.migration",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {"count": 0},
    }
)
HistoryFamily = define_doc_family(
    {
        "kind": "history.family",
        "version": 1,
        "family": True,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda seed: {"seed": seed, "count": 0},
    }
)
history_migrations: list[int] = []


def _history_migrate(value: Any, from_version: int) -> Any:
    history_migrations.append(from_version)
    return {"count": value["count"], "version": 3}


HistoryV3 = define_doc(
    {
        "kind": "history.migration",
        "version": 3,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {"count": 0, "version": 3},
        "migrate": _history_migrate,
    }
)


async def _history_session() -> tuple[Any, SessionImpl, ConversationId, EntryId, EntryId, EntryId]:
    history_migrations.clear()
    storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def first(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "first"}))["id"]
        (await tx.doc(HistoryV1, conversation_id))["count"] = 1
        (await tx.doc(HistoryFamily, conversation_id, "member", "seed"))["count"] = 1
        return entry

    first_entry = await session.commit(first, context)

    async def second(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "second"}))["id"]
        (await tx.doc(HistoryV1, conversation_id))["count"] = 2
        return entry

    second_entry = await session.commit(second, context)
    await session.unload_documents()
    commits = len(storage.commits)
    assert await session.snapshot(HistoryV3, conversation_id, context) == {
        "count": 2,
        "version": 3,
    }
    assert history_migrations == [1]
    assert len(storage.commits) == commits

    async def third(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "third"}))["id"]
        await tx.doc(HistoryV3, conversation_id)
        return entry

    third_entry = await session.commit(third, context)
    assert any(
        write["type"] == "document.change"
        and write["content"]["kind"] == "base"
        and write["content"]["version"] == 3
        for write in storage.commits[-1]
    )
    return storage, session, conversation_id, first_entry, second_entry, third_entry


async def test_migrates_current_and_historical_values_and_follows_fork_ancestry() -> None:
    (
        _storage,
        session,
        conversation_id,
        first_entry,
        second_entry,
        third_entry,
    ) = await _history_session()
    V3 = HistoryV3
    Family = HistoryFamily

    assert await session.snapshot_as_of(V3, conversation_id, first_entry, context) == {
        "count": 1,
        "version": 3,
    }
    assert await session.snapshot_as_of(V3, conversation_id, second_entry, context) == {
        "count": 2,
        "version": 3,
    }
    assert await session.snapshot_as_of(V3, conversation_id, third_entry, context) == {
        "count": 2,
        "version": 3,
    }
    assert history_migrations == [1, 1, 1]
    assert await session.snapshot_as_of(Family, conversation_id, "member", first_entry, context) == {
        "seed": "seed",
        "count": 1,
    }

    async def fork(tx: Any) -> ConversationId:
        child = await tx.fork_conversation(
            conversation_id, second_entry, {"ownership": {"kind": "ownerless"}}
        )
        return child["id"]

    child_id = await session.commit(fork, context)
    assert await session.snapshot_as_of(V3, child_id, first_entry, context) == {
        "count": 1,
        "version": 3,
    }
    assert await session.snapshot_as_of(V3, child_id, second_entry, context) == {
        "count": 2,
        "version": 3,
    }
    assert await session.snapshot_as_of(Family, child_id, "member", first_entry, context) == {
        "seed": "seed",
        "count": 1,
    }
    with pytest.raises(ValueError, match=f"Entry {third_entry} is not visible"):
        await session.snapshot_as_of(V3, child_id, third_entry, context)


async def test_selects_the_incarnation_alive_at_the_entry_commit() -> None:
    Doc = define_doc(
        {
            "kind": "history.incarnation",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "initial"},
        }
    )
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def before(tx: Any) -> EntryId:
        return (await tx.append_entry(conversation_id, {"kind": "before"}))["id"]

    before_creation = await session.commit(before, context)

    async def create(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "create"}))["id"]
        (await tx.doc(Doc, conversation_id))["value"] = "old"
        return entry

    created_at = await session.commit(create, context)

    async def retire(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "retire"}))["id"]
        await tx.retire_doc(Doc, conversation_id)
        return entry

    retired_at = await session.commit(retire, context)

    async def recreate(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "recreate"}))["id"]
        (await tx.doc(Doc, conversation_id))["value"] = "new"
        return entry

    recreated_at = await session.commit(recreate, context)

    assert await session.snapshot_as_of(Doc, conversation_id, before_creation, context) is None
    assert await session.snapshot_as_of(Doc, conversation_id, created_at, context) == {
        "value": "old"
    }
    assert await session.snapshot_as_of(Doc, conversation_id, retired_at, context) is None
    assert await session.snapshot_as_of(Doc, conversation_id, recreated_at, context) == {
        "value": "new"
    }
    await session.close(context)
    with pytest.raises(RuntimeError, match="closed"):
        await session.snapshot_as_of(Doc, conversation_id, recreated_at, context)


async def test_rejects_historical_reads_for_latest_only_documents() -> None:
    Latest = define_doc(
        {
            "kind": "history.latest-only",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"count": 0},
        }
    )
    # The same logical address declared rewindable: the token cannot gain history the record
    # never kept.
    Rewindable = define_doc(
        {
            "kind": "history.latest-only",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"count": 0},
        }
    )
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def create(tx: Any) -> EntryId:
        entry = (await tx.append_entry(conversation_id, {"kind": "point"}))["id"]
        await tx.doc(Latest, conversation_id)
        return entry

    entry = await session.commit(create, context)
    with pytest.raises(TypeError, match="rewindable"):
        await session.snapshot_as_of(Latest, conversation_id, entry, context)
    with pytest.raises(StorageRequestError, match="does not retain historical content"):
        await session.snapshot_as_of(Rewindable, conversation_id, entry, context)
    assert await session.snapshot(Latest, conversation_id, context) == {"count": 0}


# ── Tracker cache across definition versions ────────────────────────────────

V1Doc = define_doc(
    {
        "kind": "cache.versioned",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"name": "first"},
    }
)
V2Doc = define_doc(
    {
        "kind": "cache.versioned",
        "version": 2,
        "scope": "session",
        "initial": lambda: {"names": []},
        "migrate": lambda value, _from_version: {"names": [value["name"]]},
    }
)


async def test_migrates_a_document_cached_by_an_older_token_without_unloading() -> None:
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(V1Doc)

    await session.commit(create, context)
    assert await session.snapshot(V1Doc, context) == {"name": "first"}

    # Reloaded extension code accesses the still-cached document with a newer token.
    assert await session.snapshot(V2Doc, context) == {"names": ["first"]}

    async def edit(tx: Any) -> None:
        (await tx.doc(V2Doc))["names"].push("second")

    await session.commit(edit, context)
    write = _document_writes(storage.commits[-1])[0]
    assert write["type"] == "document.change"
    assert write["content"]["version"] == 2
    assert write["content"]["kind"] == "base"
    assert write["content"]["value"] == {"names": ["first", "second"]}
    with pytest.raises(ValueError, match="newer version 2"):
        await session.snapshot(V1Doc, context)


async def test_sends_observers_of_an_older_shape_a_root_replacement() -> None:
    _storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(V1Doc)

    await session.commit(create, context)
    state = await session.document_state(V1Doc, context)
    assert state is not None
    watch = await session.watch_doc(V1Doc, context)
    assert watch is not None
    frames: list[Any] = []
    delivered = asyncio.Event()

    def listener(_value: Any, ops: Any, _context: Any) -> None:
        frames.append(ops)
        delivered.set()

    watch.start(listener)

    async def edit(tx: Any) -> None:
        (await tx.doc(V2Doc))["names"].push("second")

    await session.commit(edit, context)
    await asyncio.wait_for(delivered.wait(), timeout=1)
    await flush()
    assert state.value == {"names": ["first", "second"]}
    assert watch.value == {"names": ["first", "second"]}
    assert frames == [[["r", {"names": ["first", "second"]}]]]
    state.dispose()
    await watch.stop()


async def test_serves_an_older_token_from_storage_after_an_in_memory_migration() -> None:
    _storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(V1Doc)

    await session.commit(create, context)
    await session.unload_documents()
    assert await session.snapshot(V2Doc, context) == {"names": ["first"]}
    assert await session.snapshot(V1Doc, context) == {"name": "first"}

    async def edit(tx: Any) -> None:
        (await tx.doc(V1Doc))["name"] = "renamed"

    await session.commit(edit, context)
    assert await session.snapshot(V2Doc, context) == {"names": ["renamed"]}

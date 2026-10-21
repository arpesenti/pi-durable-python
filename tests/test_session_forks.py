"""Translated v1.3.0 ``session-forks`` suite.

Covers the three fork copy policies, ancestry and the entry-owning ancestor, stored versions rather
than migrated caches, retirement and recreation incarnations, invisible fork points, duplicate
selections, same-transaction source writes, rollback, storage rejection, retire-and-recreate, scope
filtering, and family copies across storage pages.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

import pytest
from session_support import (
    context,
    create_conversation,
    document_changes,
    document_copy_changes,
    flush,
    open_test_session,
)

from pi_durable import define_doc, define_doc_family, define_task
from pi_durable.errors import SessionFailed
from pi_durable.ids import ConversationId, EntryId, TaskId
from pi_durable.types import StorageWrite


def document_creates(writes: Sequence[StorageWrite]) -> list[dict[str, Any]]:
    return [cast(dict[str, Any], write) for write in writes if write["type"] == "document.create"]


def document_copies(writes: Sequence[StorageWrite]) -> list[dict[str, Any]]:
    return [cast(dict[str, Any], write) for write in writes if write["type"] == "document.copy"]


async def test_copies_as_of_and_current_singleton_and_family_bases_while_leaving_initial_absent() -> None:
    AsOf = define_doc(
        {
            "kind": "fork.policies.as-of",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "as-initial"},
        }
    )
    Current = define_doc(
        {
            "kind": "fork.policies.current",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"value": "current-initial"},
        }
    )
    Initial = define_doc(
        {
            "kind": "fork.policies.initial",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "initial",
            "initial": lambda: {"value": "fresh"},
        }
    )
    AsOfFamily = define_doc_family(
        {
            "kind": "fork.policies.as-of-family",
            "version": 1,
            "family": True,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda seed: {"value": seed},
        }
    )
    CurrentFamily = define_doc_family(
        {
            "kind": "fork.policies.current-family",
            "version": 1,
            "family": True,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda seed: {"value": seed},
        }
    )
    storage, session, publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "fork-point"}))["id"]
        (await tx.doc(AsOf, parent_id)).value = "as-at-fork"
        (await tx.doc(Current, parent_id)).value = "current-at-fork"
        (await tx.doc(Initial, parent_id)).value = "parent-only"
        (await tx.doc(AsOfFamily, parent_id, "a", "unused")).value = "family-as-a"
        (await tx.doc(AsOfFamily, parent_id, "b", "unused")).value = "family-as-b"
        (await tx.doc(CurrentFamily, parent_id, "a", "unused")).value = "family-current-a"
        return at

    fork_at = await session.commit(create_parent, context)

    async def change_parent(tx: Any) -> None:
        (await tx.doc(AsOf, parent_id)).value = "as-after-fork"
        (await tx.doc(Current, parent_id)).value = "current-when-copied"
        (await tx.doc(AsOfFamily, parent_id, "a", "unused")).value = "family-as-after"
        (await tx.doc(CurrentFamily, parent_id, "a", "unused")).value = "family-current-when-copied"

    await session.commit(change_parent, context)
    await flush()
    document_reads = storage.document_read_count

    child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    await flush()
    assert storage.document_read_count == document_reads

    assert await session.snapshot(AsOf, child["id"], context) == {"value": "as-at-fork"}
    assert await session.snapshot(Current, child["id"], context) == {"value": "current-when-copied"}
    assert await session.snapshot(Initial, child["id"], context) is None
    assert await session.snapshot(AsOfFamily, child["id"], "a", context) == {"value": "family-as-a"}
    assert await session.snapshot(AsOfFamily, child["id"], "b", context) == {"value": "family-as-b"}
    assert await session.snapshot(CurrentFamily, child["id"], "a", context) == {
        "value": "family-current-when-copied"
    }

    admitted = storage.admitted_commits[-1]
    copies = document_copies(admitted)
    assert len(copies) == 5
    assert all(copy["record"]["scope"]["kind"] == "conversation" for copy in copies)
    assert all(
        copy["record"]["scope"]["conversationId"] == child["id"] for copy in copies
    )
    publication = publications[-1]
    copied = document_copy_changes(publication)
    assert len(copied) == 5
    for change in copied:
        assert change["record"]["createdAt"] == publication["seq"]
        assert change["conversationId"] == child["id"]
        write = next(candidate for candidate in copies if candidate["record"]["id"] == change["record"]["id"])
        assert change["source"] == write["source"]

    parent_record = await storage.find_document(
        {"kind": AsOf.definition.kind, "scope": {"kind": "conversation", "conversationId": parent_id}},
        "current",
        context,
    )
    child_record = await storage.find_document(
        {"kind": AsOf.definition.kind, "scope": {"kind": "conversation", "conversationId": child["id"]}},
        "current",
        context,
    )
    assert child_record is not None and parent_record is not None
    assert child_record["id"] != parent_record["id"]

    async def change_child(tx: Any) -> None:
        (await tx.doc(AsOf, child["id"])).value = "child-independent"

    await session.commit(change_child, context)
    assert await session.snapshot(AsOf, parent_id, context) == {"value": "as-after-fork"}
    assert await session.snapshot(AsOf, child["id"], context) == {"value": "child-independent"}

    async def create_initial(tx: Any) -> None:
        (await tx.doc(Initial, child["id"])).value = "child-created"

    await session.commit(create_initial, context)
    assert await session.snapshot(Initial, child["id"], context) == {"value": "child-created"}


async def test_uses_final_document_state_from_the_fork_entry_commit_while_excluding_later_entries() -> None:
    Doc = define_doc(
        {
            "kind": "fork.same-commit",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "initial"},
        }
    )
    _storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> tuple[EntryId, EntryId]:
        fork_at = (await tx.append_entry(parent_id, {"kind": "included"}))["id"]
        (await tx.doc(Doc, parent_id)).value = "final-state-of-commit"
        excluded = (await tx.append_entry(parent_id, {"kind": "excluded"}))["id"]
        return fork_at, excluded

    fork_at, excluded = await session.commit(create_parent, context)
    child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert await session.snapshot(Doc, child["id"], context) == {"value": "final-state-of-commit"}
    visible = await session.commit(
        lambda tx: tx.scan_entries({"conversationId": child["id"]}, 10), context
    )
    seen = [entry["id"] for entry in visible["items"]]
    assert fork_at in seen
    assert excluded not in seen


async def test_selects_the_entry_owning_ancestor_for_as_of_and_the_immediate_parent_for_current() -> None:
    AsOf = define_doc(
        {
            "kind": "fork.ancestry.as-of",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "initial"},
        }
    )
    Current = define_doc(
        {
            "kind": "fork.ancestry.current",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"value": "initial"},
        }
    )
    _storage, session, _publications = open_test_session()
    root_id = await create_conversation(session)

    async def create_root(tx: Any) -> EntryId:
        inherited = (await tx.append_entry(root_id, {"kind": "root"}))["id"]
        (await tx.doc(AsOf, root_id)).value = "root-at-entry"
        (await tx.doc(Current, root_id)).value = "root-current"
        return inherited

    inherited = await session.commit(create_root, context)
    parent = await session.commit(
        lambda tx: tx.fork_conversation(
            root_id, inherited, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )

    async def create_parent(tx: Any) -> EntryId:
        parent_entry = (await tx.append_entry(parent["id"], {"kind": "parent"}))["id"]
        (await tx.doc(AsOf, parent["id"])).value = "parent-at-own-entry"
        (await tx.doc(Current, parent["id"])).value = "parent-current"
        return parent_entry

    parent_entry = await session.commit(create_parent, context)

    inherited_fork = await session.commit(
        lambda tx: tx.fork_conversation(
            parent["id"], inherited, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert await session.snapshot(AsOf, inherited_fork["id"], context) == {"value": "root-at-entry"}
    assert await session.snapshot(Current, inherited_fork["id"], context) == {"value": "parent-current"}

    own_entry_fork = await session.commit(
        lambda tx: tx.fork_conversation(
            parent["id"], parent_entry, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert await session.snapshot(AsOf, own_entry_fork["id"], context) == {
        "value": "parent-at-own-entry"
    }
    assert await session.snapshot(Current, own_entry_fork["id"], context) == {"value": "parent-current"}


async def test_copies_the_stored_value_and_version_without_consulting_migration_definitions() -> None:
    V1 = define_doc(
        {
            "kind": "fork.stored-version",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"count": 1},
        }
    )
    migrations: list[int] = []

    def migrate(value: Any, from_version: int) -> Any:
        assert from_version == 1
        migrations.append(from_version)
        return {"count": value["count"], "migrated": True}

    V3 = define_doc(
        {
            "kind": "fork.stored-version",
            "version": 3,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"count": 0, "migrated": False},
            "migrate": migrate,
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        await tx.doc(V1, parent_id)
        return at

    fork_at = await session.commit(create_parent, context)
    await session.unload_documents()
    assert await session.snapshot(V3, parent_id, context) == {"count": 1, "migrated": True}
    assert len(migrations) == 1

    child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert len(migrations) == 1
    child_record = await storage.find_document(
        {"kind": V1.definition.kind, "scope": {"kind": "conversation", "conversationId": child["id"]}},
        "current",
        context,
    )
    assert child_record is not None
    child_stored = await storage.document(child_record["id"], "current", context)
    assert child_stored is not None
    assert child_stored["version"] == 1
    assert child_stored["value"] == {"count": 1}
    assert await session.snapshot(V3, child["id"], context) == {"count": 1, "migrated": True}
    assert len(migrations) == 2


async def test_coalesces_a_typed_migration_and_override_into_the_copied_creation_base() -> None:
    V1 = define_doc(
        {
            "kind": "fork.override",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"count": 2},
        }
    )
    checkpoints: list[int] = []

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        checkpoints.append(1)
        return False

    V2 = define_doc(
        {
            "kind": "fork.override",
            "version": 2,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"count": 0, "migrated": False},
            "migrate": lambda value, _from_version: {"count": value["count"], "migrated": True},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        await tx.doc(V1, parent_id)
        return at

    fork_at = await session.commit(create_parent, context)

    document_reads = storage.document_read_count

    async def fork_and_override(tx: Any) -> dict[str, Any]:
        created = await tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        )
        value = await tx.doc(V2, created["id"])
        value.count = 9
        return created

    child = await session.commit(fork_and_override, context)
    await flush()
    assert storage.document_read_count == document_reads + 1
    creates = document_creates(storage.admitted_commits[-1])
    assert len(creates) == 1
    assert creates[0]["content"]["kind"] == "base"
    assert creates[0]["content"]["version"] == 2
    assert creates[0]["content"]["value"] == {"count": 9, "migrated": True}
    assert checkpoints == []
    changes = document_changes(publications[-1])
    assert [change["version"] for change in changes] == [2]
    assert await session.snapshot(V2, child["id"], context) == {"count": 9, "migrated": True}
    parent_record = await storage.find_document(
        {"kind": V1.definition.kind, "scope": {"kind": "conversation", "conversationId": parent_id}},
        "current",
        context,
    )
    assert parent_record is not None
    parent_stored = await storage.document(parent_record["id"], "current", context)
    assert parent_stored is not None and parent_stored["version"] == 1


async def test_copies_the_incarnation_alive_at_the_fork_point_across_retirement_and_recreation() -> None:
    Doc = define_doc(
        {
            "kind": "fork.incarnations",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "initial"},
        }
    )
    _storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def write_old(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "old"}))["id"]
        (await tx.doc(Doc, parent_id)).value = "old"
        return at

    old_at = await session.commit(write_old, context)

    async def retire(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "retired"}))["id"]
        await tx.retire_doc(Doc, parent_id)
        return at

    retired_at = await session.commit(retire, context)

    async def write_new(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "new"}))["id"]
        (await tx.doc(Doc, parent_id)).value = "new"
        return at

    new_at = await session.commit(write_new, context)

    old_child = await session.commit(
        lambda tx: tx.fork_conversation(parent_id, old_at, {"ownership": {"kind": "ownerless"}}),
        context,
    )
    empty_child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, retired_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    new_child = await session.commit(
        lambda tx: tx.fork_conversation(parent_id, new_at, {"ownership": {"kind": "ownerless"}}),
        context,
    )
    assert await session.snapshot(Doc, old_child["id"], context) == {"value": "old"}
    assert await session.snapshot(Doc, empty_child["id"], context) is None
    assert await session.snapshot(Doc, new_child["id"], context) == {"value": "new"}


async def test_rejects_invisible_fork_points_before_admission_and_remains_usable() -> None:
    storage, session, publications = open_test_session()
    root_id = await create_conversation(session)

    async def create_root(tx: Any) -> tuple[EntryId, EntryId]:
        visible = (await tx.append_entry(root_id, {"kind": "visible"}))["id"]
        hidden = (await tx.append_entry(root_id, {"kind": "hidden"}))["id"]
        return visible, hidden

    visible, hidden = await session.commit(create_root, context)
    parent = await session.commit(
        lambda tx: tx.fork_conversation(
            root_id, visible, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    await flush()
    commits = len(storage.commits)
    published = len(publications)
    with pytest.raises(ValueError, match=f"Entry {hidden} is not visible"):
        await session.commit(
            lambda tx: tx.fork_conversation(
                parent["id"], hidden, {"ownership": {"kind": "ownerless"}}
            ),
            context,
        )
    await flush()
    assert len(storage.commits) == commits
    assert len(publications) == published
    independent = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
    )
    assert await storage.conversation(independent["id"], context) == independent


async def test_rejects_duplicate_as_of_and_current_selections_for_one_child_address() -> None:
    AsOf = define_doc(
        {
            "kind": "fork.duplicate-policy",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "as-of"},
        }
    )
    Current = define_doc(
        {
            "kind": "fork.duplicate-policy",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"value": "current"},
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "old"}))["id"]
        await tx.doc(AsOf, parent_id)
        return at

    old_at = await session.commit(create_parent, context)

    async def retire(tx: Any) -> None:
        await tx.retire_doc(AsOf, parent_id)

    await session.commit(retire, context)

    async def create_current(tx: Any) -> None:
        await tx.doc(Current, parent_id)

    await session.commit(create_current, context)
    commits = len(storage.commits)
    with pytest.raises(ValueError, match="Fork selects multiple source documents"):
        await session.commit(
            lambda tx: tx.fork_conversation(
                parent_id, old_at, {"ownership": {"kind": "ownerless"}}
            ),
            context,
        )
    assert len(storage.commits) == commits
    independent = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
    )
    assert await storage.conversation(independent["id"], context) == independent


async def test_rejects_current_and_as_of_source_writes_in_the_fork_transaction() -> None:
    Current = define_doc(
        {
            "kind": "fork.same-transaction-current",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"value": "committed"},
        }
    )
    AsOf = define_doc(
        {
            "kind": "fork.same-transaction-as-of",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "committed"},
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        await tx.doc(Current, parent_id)
        await tx.doc(AsOf, parent_id)
        return at

    fork_at = await session.commit(create_parent, context)
    commits = len(storage.commits)

    async def write_current_before(tx: Any) -> None:
        (await tx.doc(Current, parent_id)).value = "before-fork"
        await tx.fork_conversation(parent_id, fork_at, {"ownership": {"kind": "ownerless"}})

    with pytest.raises(ValueError, match="Cannot change fork source document"):
        await session.commit(write_current_before, context)

    async def write_current_after(tx: Any) -> None:
        await tx.fork_conversation(parent_id, fork_at, {"ownership": {"kind": "ownerless"}})
        (await tx.doc(Current, parent_id)).value = "after-fork"

    with pytest.raises(ValueError, match="Cannot change fork source document"):
        await session.commit(write_current_after, context)

    async def write_as_of(tx: Any) -> None:
        (await tx.doc(AsOf, parent_id)).value = "as-of-write"
        await tx.fork_conversation(parent_id, fork_at, {"ownership": {"kind": "ownerless"}})

    with pytest.raises(ValueError, match="Cannot change fork source document"):
        await session.commit(write_as_of, context)
    assert len(storage.commits) == commits
    assert await session.snapshot(Current, parent_id, context) == {"value": "committed"}
    child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert await session.snapshot(Current, child["id"], context) == {"value": "committed"}


async def test_rolls_every_copied_base_back_when_later_pre_admission_assembly_fails() -> None:
    Copied = define_doc(
        {
            "kind": "fork.rollback.copied",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "copied"},
        }
    )
    failing = {"value": True}

    def checkpoint_when(_value: Any, _ops: Any, _info: Any) -> bool:
        if failing["value"]:
            raise RuntimeError("checkpoint failed")
        return False

    Failure = define_doc(
        {
            "kind": "fork.rollback.failure",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 0},
            "checkpoint_when": checkpoint_when,
        }
    )
    storage, session, publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        await tx.doc(Copied, parent_id)
        await tx.doc(Failure)
        return at

    fork_at = await session.commit(create_parent, context)
    await flush()
    commits = len(storage.commits)
    published = len(publications)
    child_ids: list[ConversationId] = []

    async def fork_and_fail(tx: Any) -> None:
        created = await tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        )
        child_ids.append(created["id"])
        (await tx.doc(Failure)).count = 1

    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await session.commit(fork_and_fail, context)
    await flush()
    assert len(storage.commits) == commits
    assert len(publications) == published
    assert await storage.conversation(child_ids[0], context) is None
    failing["value"] = False

    async def change_failure(tx: Any) -> None:
        (await tx.doc(Failure)).count = 2

    await session.commit(change_failure, context)
    assert await session.snapshot(Failure, context) == {"count": 2}
    assert await session.snapshot(Copied, parent_id, context) == {"value": "copied"}


async def test_fails_the_session_on_a_rejected_fork_commit_which_leaves_no_effect() -> None:
    Doc = define_doc(
        {
            "kind": "fork.storage-rejected",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "source"},
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        await tx.doc(Doc, parent_id)
        return at

    fork_at = await session.commit(create_parent, context)
    rejected_ids: list[ConversationId] = []
    rejection = RuntimeError("copy rejected")
    storage.fail_next_commit(rejection)

    async def fork(tx: Any) -> None:
        created = await tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        )
        rejected_ids.append(created["id"])

    with pytest.raises(RuntimeError, match="copy rejected"):
        await session.commit(fork, context)
    with pytest.raises(SessionFailed) as failed:
        await session.commit(
            lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
        )
    assert failed.value.cause is rejection
    assert await session.closed == {"reason": "failed", "error": rejection}
    assert await storage.reopen().conversation(rejected_ids[0], context) is None


async def test_retires_a_copied_document_and_can_recreate_the_address_in_the_fork_transaction() -> None:
    Doc = define_doc(
        {
            "kind": "fork.retire-recreate",
            "version": 1,
            "scope": "conversation",
            "history": "rewindable",
            "fork": "asOf",
            "initial": lambda: {"value": "fresh"},
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> EntryId:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        (await tx.doc(Doc, parent_id)).value = "copied"
        return at

    fork_at = await session.commit(create_parent, context)

    async def fork_retire_and_recreate(tx: Any) -> dict[str, Any]:
        created = await tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        )
        await tx.retire_doc(Doc, created["id"])
        (await tx.doc(Doc, created["id"])).value = "replacement"
        return created

    child = await session.commit(fork_retire_and_recreate, context)
    writes = storage.commits[-1]
    assert len(document_copies(writes)) == 1
    assert len(document_creates(writes)) == 1
    assert sum(1 for write in writes if write["type"] == "document.retire") == 1
    assert await session.snapshot(Doc, child["id"], context) == {"value": "replacement"}


async def test_copies_only_conversation_documents_leaving_session_and_task_documents_in_place() -> None:
    Copied = define_doc(
        {
            "kind": "fork.scope.conversation",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"value": "conversation"},
        }
    )
    SessionOnly = define_doc(
        {
            "kind": "fork.scope.session",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"value": "session"},
        }
    )
    TaskOnly = define_doc(
        {
            "kind": "fork.scope.task",
            "version": 1,
            "scope": "task",
            "initial": lambda: {"value": "task"},
        }
    )
    Work = define_task(
        {
            "name": "fork.scope.work",
            "version": 1,
            "initial": lambda _input: {"phase": "start"},
            "phases": {"start": _started},
            "abort": _aborted,
        }
    )
    storage, session, publications = open_test_session()
    parent_id = await create_conversation(session)

    async def create_parent(tx: Any) -> tuple[EntryId, TaskId]:
        at = (await tx.append_entry(parent_id, {"kind": "point"}))["id"]
        task_id = await tx.create_task(
            Work, None, {"ownership": {"kind": "conversation"}, "conversationId": parent_id}
        )
        await tx.doc(Copied, parent_id)
        await tx.doc(SessionOnly)
        await tx.doc(TaskOnly, task_id)
        return at, task_id

    fork_at, task_id = await session.commit(create_parent, context)
    child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    await flush()
    copies = document_copies(storage.commits[-1])
    assert [copy["record"]["kind"] for copy in copies] == [Copied.definition.kind]
    assert [change["record"]["kind"] for change in document_copy_changes(publications[-1])] == [
        Copied.definition.kind
    ]
    assert await session.snapshot(Copied, child["id"], context) == {"value": "conversation"}
    assert await session.snapshot(SessionOnly, context) == {"value": "session"}
    assert await session.snapshot(TaskOnly, task_id, context) == {"value": "task"}


async def test_copies_every_family_member_across_storage_scan_pages() -> None:
    Family = define_doc_family(
        {
            "kind": "fork.pagination",
            "version": 1,
            "family": True,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda seed: {"value": seed},
        }
    )
    storage, session, _publications = open_test_session()
    parent_id = await create_conversation(session)

    async def append_point(tx: Any) -> EntryId:
        return (await tx.append_entry(parent_id, {"kind": "point"}))["id"]

    fork_at = await session.commit(append_point, context)

    async def create_members(tx: Any) -> None:
        for index in range(260):
            await tx.doc(Family, parent_id, f"member-{index}", index)

    await session.commit(create_members, context)
    child = await session.commit(
        lambda tx: tx.fork_conversation(
            parent_id, fork_at, {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert len(document_copies(storage.commits[-1])) == 260
    assert await session.snapshot(Family, child["id"], "member-0", context) == {"value": 0}
    assert await session.snapshot(Family, child["id"], "member-259", context) == {"value": 259}


async def _started(*_args: Any) -> None:
    return None


async def _aborted(*_args: Any) -> None:
    return None

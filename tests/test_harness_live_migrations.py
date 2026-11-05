"""The ``pi.live`` version 3 migration: stored documents open, migrate once, and read fresh.

Translated upstream ``pi.live migrations`` cases — the block at the end of v1.5.0
``harness-compaction.test.ts`` — plus the three traps that block carries (a run whose turn count is
missing, a renamed trigger key whose order is observable, and a member that was never there), the
version matrix at its definition sites, and the stored-fixture tests that open databases written by
this port's own v1.4.0 tag. See ``tests/fixtures/pre-1.5.0/README.md`` for how those fixtures were
produced.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from pathlib import Path
from typing import Any, cast

import pytest
from session_support import context, create_conversation, document_changes, open_test_session

from pi_durable import (
    AgentDoc,
    CompactionTask,
    GenerationTask,
    LiveDoc,
    MemoryStorage,
    NestedCallDoc,
    ToolTask,
    define_doc,
)
from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.harness.live import add_compaction_status, fail_compaction_status, start_run
from pi_durable.ids import ConversationId, SubmissionId, TaskId
from pi_durable.session.session import SessionImpl
from pi_durable.storage.jsonl.storage import FORMAT_VERSION, JsonlStorage
from pi_durable.storage.sqlite import CURRENT_SQLITE_SCHEMA_VERSION, SqliteStorage
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage
from pi_durable.types import DocumentAddress, Storage

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "pre-1.5.0"

#: The one model-issued call of the fixture's in-flight round, as v1.4.0 wrote its slot.
FIXTURE_SLOT = {"callId": "c1", "name": "echo", "taskId": 6, "status": "running", "output": "echo"}

#: What v1.4.0 wrote into the fixture's ``pi.live``: a run with no turn count, and each compaction
#: naming its trigger ``reason``.
STORED_VERSION_1 = {
    "run": {"taskId": 5, "inputs": [2]},
    "tools": [FIXTURE_SLOT],
    "compactions": [{"taskId": 901, "reason": "threshold", "blocking": True, "attempt": 2}],
    "compactionFailure": {"taskId": 902, "reason": "manual", "error": "no model"},
}

#: The same document read under version 3: the run counts its turns from 0 (#11043) and every
#: compaction carries its trigger under the new name, in its stored key order.
MIGRATED_VERSION_1 = {
    "run": {"taskId": 5, "inputs": [2], "turns": 0},
    "tools": [FIXTURE_SLOT],
    "compactions": [{"taskId": 901, "trigger": "threshold", "blocking": True, "attempt": 2}],
    "compactionFailure": {"taskId": 902, "trigger": "manual", "error": "no model"},
}

_directories: list[Path] = []
_open_storages: list[Any] = []


def _temp_directory() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-live-migrations-"))
    _directories.append(directory)
    return directory


def _copy_fixture(*parts: str) -> Path:
    """A copy of a checked-in pre-upgrade fixture, so a test may write to it."""

    source = FIXTURES / Path(*parts)
    target = _temp_directory() / Path(*parts)
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, target)
    return target


async def _open_sqlite(path: Path) -> SqliteStorage:
    storage = await open_posix_sqlite_storage(str(path))
    _open_storages.append(storage)
    return storage


async def _open_jsonl(directory: Path) -> JsonlStorage:
    from pi_durable.env.posix import PosixExecutionEnv

    storage = await JsonlStorage.open(
        str(directory), PosixExecutionEnv(cwd=str(directory)), BACKGROUND_CONTEXT, None
    )
    _open_storages.append(storage)
    return storage


def _session(storage: Storage) -> SessionImpl:
    return SessionImpl(storage, lambda: 1_700_000_000_000, lambda: 600_000)


async def _read(session: SessionImpl, conversation_id: ConversationId) -> dict[str, Any]:
    """``pi.live`` as this release reads it: migrated, detached, and typed for assertions."""

    value = await session.snapshot(LiveDoc, conversation_id, context)
    assert value is not None
    return cast(dict[str, Any], value)


async def _stored_live(storage: Storage, conversation_id: ConversationId) -> dict[str, Any]:
    """The stored ``pi.live`` revision as Storage holds it, unmigrated and key-for-key."""

    address: DocumentAddress = {
        "kind": "pi.live",
        "scope": {"kind": "conversation", "conversationId": conversation_id},
    }
    record = await storage.find_document(address, "current", context)
    assert record is not None
    stored = await storage.document(record["id"], "current", context)
    assert stored is not None
    return {"version": stored["version"], "value": json.loads(json.dumps(stored["value"]))}


def _written_by_earlier_version(version: int) -> Any:
    """A ``pi.live`` token of an earlier version, the way a stored document was written."""

    return define_doc(
        {
            "kind": "pi.live",
            "version": version,
            "scope": "conversation",
            "history": "latest",
            "fork": "initial",
            "initial": lambda: {},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )


def _without_minted_ids(value: dict[str, Any]) -> dict[str, Any]:
    """A live document without its minted IDs, so two writers of one state compare."""

    run = {key: field for key, field in value["run"].items() if key not in ("taskId", "inputs")}
    return {
        "run": run,
        "tools": [{k: v for k, v in slot.items() if k != "taskId"} for slot in value["tools"]],
        "compactions": [
            {k: v for k, v in status.items() if k != "taskId"} for status in value["compactions"]
        ],
        "compactionFailure": {k: v for k, v in value["compactionFailure"].items() if k != "taskId"},
    }


# ── The migrated read ───────────────────────────────────────────────────────


async def test_reads_a_version_1_value_with_each_trigger_renamed_and_stores_it_on_its_next_write() -> (
    None
):
    storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    LiveV1 = _written_by_earlier_version(1)

    async def write_v1(tx: Any) -> None:
        live = await tx.doc(LiveV1, conversation_id)
        live["run"] = {"taskId": 9, "inputs": [4]}
        live["compactions"] = [{"taskId": 7, "reason": "threshold", "blocking": True, "attempt": 2}]
        live["compactionFailure"] = {"taskId": 5, "reason": "manual", "error": "no model"}

    await session.commit(write_v1, context)

    assert await _read(session, conversation_id) == {
        "run": {"taskId": 9, "inputs": [4], "turns": 0},
        "compactions": [{"taskId": 7, "trigger": "threshold", "blocking": True, "attempt": 2}],
        "compactionFailure": {"taskId": 5, "trigger": "manual", "error": "no model"},
    }
    # A read migrates in memory only: Storage keeps the version 1 value under its old spelling.
    stored = await _stored_live(storage, conversation_id)
    assert stored["version"] == 1
    assert stored["value"]["compactions"][0]["reason"] == "threshold"
    assert stored["value"]["compactionFailure"]["reason"] == "manual"

    async def write_v3(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live["compactions"][0]["attempt"] = 3

    await session.commit(write_v3, context)
    stored = await _stored_live(storage, conversation_id)
    assert stored["version"] == 3
    assert stored["value"]["compactions"][0] == {
        "taskId": 7,
        "trigger": "threshold",
        "blocking": True,
        "attempt": 3,
    }
    assert stored["value"]["compactionFailure"] == {
        "taskId": 5,
        "trigger": "manual",
        "error": "no model",
    }


async def test_gives_a_version_2_run_without_a_turn_count_a_count_of_zero() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    LiveV2 = _written_by_earlier_version(2)

    async def write_v2(tx: Any) -> None:
        live = await tx.doc(LiveV2, conversation_id)
        live["run"] = {"taskId": 9, "inputs": [4]}
        live["compactions"] = [{"taskId": 7, "trigger": "threshold", "blocking": True, "attempt": 2}]

    await session.commit(write_v2, context)
    # #11043: an unreleased build stored version 2 without ``run.turns``, and a run in flight then
    # counted NaN turns. A run in flight across the upgrade counts its turns from 0.
    assert await _read(session, conversation_id) == {
        "run": {"taskId": 9, "inputs": [4], "turns": 0},
        "compactions": [{"taskId": 7, "trigger": "threshold", "blocking": True, "attempt": 2}],
    }

    async def write_counted(tx: Any) -> None:
        (await tx.doc(LiveV2, conversation_id))["run"] = {"taskId": 9, "inputs": [4], "turns": 3}

    await session.commit(write_counted, context)
    assert (await _read(session, conversation_id))["run"] == {
        "taskId": 9,
        "inputs": [4],
        "turns": 3,
    }


async def test_renames_the_trigger_without_reordering_a_stored_status_or_failure() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    LiveV1 = _written_by_earlier_version(1)

    async def write(tx: Any) -> None:
        live = await tx.doc(LiveV1, conversation_id)
        live["compactions"] = [{"taskId": 7, "reason": "manual", "blocking": False, "attempt": 1}]
        live["compactionFailure"] = {"error": "no model", "reason": "overflow", "taskId": 5}

    await session.commit(write, context)
    read = await _read(session, conversation_id)
    assert list(read["compactions"][0]) == ["taskId", "trigger", "blocking", "attempt"]
    assert list(read["compactionFailure"]) == ["error", "trigger", "taskId"]


async def test_leaves_an_absent_run_status_list_or_failure_absent_when_migrating() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    LiveV1 = _written_by_earlier_version(1)

    async def write(tx: Any) -> None:
        # Only a generation is in flight: no run, no compaction status, no failure.
        (await tx.doc(LiveV1, conversation_id))["generation"] = {"attempt": 2}

    await session.commit(write, context)
    # A migration never materialises a member the stored document did not have.
    assert await _read(session, conversation_id) == {"generation": {"attempt": 2}}


async def test_refuses_a_stored_live_document_newer_than_this_definition() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)
    LiveV4 = _written_by_earlier_version(4)

    async def write(tx: Any) -> None:
        (await tx.doc(LiveV4, conversation_id))["generation"] = {"attempt": 1}

    await session.commit(write, context)
    with pytest.raises(ValueError, match="newer version 4 than 3"):
        await session.snapshot(LiveDoc, conversation_id, context)


async def test_records_the_turn_count_as_the_run_takes_its_rounds() -> None:
    from tool_support import DONE, add_tool, calls, chat_setup, open_chat, tool

    setup = chat_setup()
    add_tool(setup.registry, tool("echo", lambda *_args: {"output": []}))
    setup.faux.set_responses([calls(["echo", {}, "c1"]), calls(["echo", {}, "c2"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    counts: list[int] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            value = change.get("value")
            if change["record"]["kind"] == "pi.live" and value and "run" in value:
                counts.append(value["run"]["turns"])

    harness.subscribe_commits(observe)
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    # The run starts at zero, and every tool round is one turn of it.
    assert counts[0] == 0
    assert max(counts) == 2
    await harness.close(context)


# ── The version matrix, verified at the definition sites ────────────────────


def test_the_persisted_version_constants_are_their_definition_sites_values() -> None:
    # Upstream ``v1.5.0``: live.ts:196, generation.ts:164, tool.ts:138, tool.ts:119,
    # compaction.ts:116, agent.ts:46.
    assert LiveDoc.definition.version == 3
    assert GenerationTask.definition.version == 2
    assert ToolTask.definition.version == 3
    assert NestedCallDoc.definition.version == 1
    assert CompactionTask.definition.version == 1
    assert AgentDoc.definition.version == 1


def test_the_database_schema_did_not_move_from_the_previous_baseline() -> None:
    # v1.4.0's own ``CURRENT_SQLITE_SCHEMA_VERSION`` and JSONL ``FORMAT_VERSION``: the schema and
    # the file format stand still while the live document moves to version 3.
    assert CURRENT_SQLITE_SCHEMA_VERSION == 2
    assert FORMAT_VERSION == 1


# ── A database written by the previous release ──────────────────────────────


async def test_a_stored_sqlite_database_from_the_previous_release_opens_and_migrates_once() -> None:
    path = _copy_fixture("sqlite", "storage.sqlite3")
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT version FROM durable_schema WHERE singleton = 1"
        ).fetchone()
    # The schema version the previous release wrote is the one this release still expects.
    assert row[0] == CURRENT_SQLITE_SCHEMA_VERSION

    storage = await _open_sqlite(path)
    session = _session(cast(Storage, storage))
    conversation_id = cast(ConversationId, 1)
    assert await _read(session, conversation_id) == MIGRATED_VERSION_1
    # Reads migrate in memory; the stored document keeps its version until something writes it.
    assert await _stored_live(storage, conversation_id) == {"version": 1, "value": STORED_VERSION_1}
    assert await _read(session, conversation_id) == MIGRATED_VERSION_1

    async def write(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        live["compactions"][0]["attempt"] = 3

    await session.commit(write, context)
    stored = await _stored_live(storage, conversation_id)
    assert stored["version"] == 3
    assert stored["value"]["compactions"][0] == {
        "taskId": 901,
        "trigger": "threshold",
        "blocking": True,
        "attempt": 3,
    }
    # Migrated once: nothing downstream of the rename still spells the trigger ``reason``.
    assert "reason" not in json.dumps(stored["value"])
    await session.close(context)


async def test_a_pre_upgrade_database_reads_identically_to_one_written_fresh() -> None:
    path = _copy_fixture("sqlite", "storage.sqlite3")
    storage = await _open_sqlite(path)
    session = _session(cast(Storage, storage))
    migrated = await _read(session, cast(ConversationId, 1))

    conversation_id = await create_conversation(session)

    async def write_fresh(tx: Any) -> None:
        live = await tx.doc(LiveDoc, conversation_id)
        await start_run(tx, conversation_id, live, [cast(SubmissionId, 2)], GenerationTask)
        live["tools"] = [dict(FIXTURE_SLOT)]
        add_compaction_status(
            live, {"taskId": 901, "trigger": "threshold", "blocking": True, "attempt": 2}
        )
        add_compaction_status(
            live, {"taskId": 902, "trigger": "manual", "blocking": False, "attempt": 1}
        )
        fail_compaction_status(live, cast(TaskId, 902), "no model")

    await session.commit(write_fresh, context)
    fresh = await _read(session, conversation_id)
    # Same members, same order, same names: the migrated read is the fresh read.
    assert json.dumps(_without_minted_ids(fresh)) == json.dumps(_without_minted_ids(migrated))
    await session.close(context)


async def test_a_stored_jsonl_directory_from_the_previous_release_opens_and_migrates() -> None:
    directory = _copy_fixture("jsonl")
    with (directory / "main.jsonl").open(encoding="utf-8") as handle:
        header = json.loads(handle.readline())
    assert header["format"] == FORMAT_VERSION

    storage = await _open_jsonl(directory)
    session = _session(cast(Storage, storage))
    assert await _read(session, cast(ConversationId, 1)) == MIGRATED_VERSION_1
    await session.close(context)


async def test_a_stored_schema_newer_than_this_release_still_refuses_to_open() -> None:
    path = _copy_fixture("sqlite", "storage.sqlite3")
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE durable_schema SET version = ? WHERE singleton = 1",
            (CURRENT_SQLITE_SCHEMA_VERSION + 1,),
        )
    with pytest.raises(RuntimeError, match="is newer than supported version"):
        await _open_sqlite(path)

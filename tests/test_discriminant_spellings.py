"""One discriminant spelling: the retired tag is refused, and only the migrate path still reads it (#37).

Upstream 1.5.0 moves the tag key of six unions onto ``kind``: the watch end and the session end
(``docs/upstream/spec-1.5.0.md`` §9.1 and §4), the report info and the report source (§10.1), the
document commit change (§2, which adds ``kind: "change" | "retire"`` beside the ``type`` that stays),
and the execution environment's file change (upstream ``src/env/index.ts:127``, where the union is
also renamed ``WatchChange`` → ``FileChange``). #33 expanded the port to read either spelling; this
file is the contract half and asserts the opposite of what it asserted then.

The contract has exactly one exception. Spec #30 promises that "a database written by this port's
previous release opens under 1.5.0 and its documents migrate in place", and a stored version 1 or 2
document is written in the retired spelling. A retired spelling is therefore refused everywhere the
port reads a live value, and accepted by no path but the document migrate path — ``LiveDoc.migrate``
renames a stored compaction ``reason`` to ``trigger``, and ``ToolTask.migrate`` tags a version 1 input
that carried no tag at all. Opening a real pre-upgrade database is
``tests/test_harness_live_migrations.py``; this file pins the rule at the two seams.

Refusal is read as, for a JSON record, what it physically is: the port reads the one tag key and
converts nothing. A value written under a retired key therefore yields nothing — the reader demands
``kind`` and raises when it finds none, where the same value written under ``kind`` reads fine — and
the port exposes no type and no reader for a retired spelling at all.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

import pytest
from chat_support import FauxModels
from session_support import context, document_changes, open_test_session
from task_support import completed, eventually

import pi_durable.env as env_module
import pi_durable.harness.types as harness_types_module
import pi_durable.types as core_types_module
from pi_durable import (
    LiveDoc,
    ToolTask,
    define_doc,
    define_extension,
    define_task,
)
from pi_durable.context import BACKGROUND_CONTEXT, with_cancel
from pi_durable.documents import DocDefinition
from pi_durable.env import (
    FileChange,
    WatchTarget,
    get_or_throw,
)
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import (
    Report,
    ReportFrom,
    ReportInfo,
    ReportTask,
)
from pi_durable.ids import ConversationId, DocumentId, Seq, SubmissionId, TaskId
from pi_durable.session.observation import RETIREMENT_OPERATIONS, CommittedWatch, observed_operations
from pi_durable.storage.memory import MemoryStorage
from pi_durable.tasks import TaskDefinition
from pi_durable.types import (
    DocumentCommitChange,
    DocumentCommittedChange,
    DocumentContent,
    DocumentRecord,
    DocumentRetiredChange,
    DocumentScope,
    StorageWrite,
    SubmissionRecord,
    TaskState,
)

#: Every name the expand phase (#33) added to read a retired spelling, by the module holding it.
#: The contract phase deletes all of them, and a released surface keeps no compatibility shim.
RETIRED_READERS: dict[str, list[str]] = {
    "core": [
        "DocumentCommitChangeLegacy",
        "DocumentCommitChangeInput",
        "to_document_commit_change",
        "WatchEndLegacy",
        "WatchEndInput",
        "to_watch_end",
        "SessionEndLegacy",
        "SessionEndInput",
        "to_session_end",
    ],
    "harness": [
        "ReportInfoLegacy",
        "ReportInfoInput",
        "to_report_info",
        "ReportFromLegacy",
        "ReportFromInput",
        "to_report_from",
    ],
    "env": [
        "FileChangePathsLegacy",
        "FileChangeOverflowLegacy",
        "FileChangeFailedLegacy",
        "FileChangeLegacy",
        "FileChangeInput",
        "to_file_change",
    ],
}

#: The one spelling each of those unions has now, still published by the same module.
SINGLE_SPELLINGS: dict[str, list[str]] = {
    "core": ["DocumentCommitChange", "WatchEnd", "SessionEnd"],
    "harness": ["ReportInfo", "ReportFrom"],
    "env": ["FileChange"],
}

_MODULES: dict[str, Any] = {
    "core": core_types_module,
    "harness": harness_types_module,
    "env": env_module,
}

record: DocumentRecord = {
    "id": DocumentId(6),
    "kind": "spelling.state",
    "createdAt": Seq(1),
    "scope": {"kind": "session"},
}
retirement: DocumentRetiredChange = {
    "type": "document",
    "kind": "retire",
    "record": record,
    "conversationId": None,
}
edit: DocumentCommittedChange = {
    "type": "document",
    "kind": "change",
    "record": record,
    "conversationId": None,
    "version": 3,
    "value": {"value": 1},
    "ops": [["s", "value", 1]],
}
copied: DocumentCommitChange = {
    "type": "document.copy",
    "record": record,
    "conversationId": ConversationId(1),
    "source": {"id": DocumentId(7), "at": "current"},
}


def written_the_old_way(change: DocumentCommittedChange | DocumentRetiredChange) -> dict[str, Any]:
    """One ``document`` change as 1.4.0 wrote it: the ``type`` alone, and no ``kind`` tag at all.

    A retirement was told apart by ``value: None`` and ``version: None``, an edit by their presence.
    """

    if change["kind"] == "retire":
        return {
            "type": "document",
            "record": change["record"],
            "conversationId": change["conversationId"],
            "version": None,
            "value": None,
            "ops": [],
        }
    return {
        "type": "document",
        "record": change["record"],
        "conversationId": change["conversationId"],
        "version": change["version"],
        "value": change["value"],
        "ops": change["ops"],
    }


EndStateDoc = define_doc(
    {
        "kind": "spelling.state",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"value": 0},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


async def open_state_session() -> tuple[Any, Any, list[Any]]:
    """A session holding one singleton document, so a watch of it can be ended every way."""

    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(EndStateDoc)

    await session.commit(create, context)
    return storage, session, publications


async def set_value(session: Any, value: int) -> None:
    async def change(tx: Any) -> None:
        (await tx.doc(EndStateDoc)).value = value

    await session.commit(change, context)


# ── The port publishes one spelling and holds no reader for any other ───────


def test_the_port_publishes_one_spelling_and_no_reader_for_a_retired_one() -> None:
    """The expand phase's dual readers, its ``*Input`` aliases and its ``*Legacy`` types are gone.

    They existed only to accept a retired tag key on the live read side; the released surface keeps
    no shim that would keep one green, so each name is simply absent from its module while the
    canonical name of the same union stays.
    """

    for group, names in RETIRED_READERS.items():
        module = _MODULES[group]
        for name in names:
            assert not hasattr(module, name), f"{module.__name__} still offers {name}"
    for group, names in SINGLE_SPELLINGS.items():
        module = _MODULES[group]
        for name in names:
            assert hasattr(module, name), f"{module.__name__} lost {name}"


# ── The watch end and the session end: `kind` alone (spec §9.1 and §4) ───────


async def test_every_way_a_watch_ends_emits_its_tag_under_kind() -> None:
    _storage, session, _publications = await open_state_session()

    stopped = await session.watch_doc(EndStateDoc, context)
    assert stopped is not None
    assert await stopped.stop() == {"kind": "stopped"}

    controller = with_cancel(context)
    aborted = await session.watch_doc(EndStateDoc, controller.context)
    assert aborted is not None
    controller.cancel()
    assert await aborted.closed == {"kind": "aborted"}

    failing = await session.watch_doc(EndStateDoc, context)
    assert failing is not None

    def throwing_listener(_call: Any, _delivery_context: Any) -> None:
        raise RuntimeError("listener failed")

    failing.start(throwing_listener)
    await set_value(session, 1)
    end = cast(dict[str, Any], await failing.closed)
    assert end["kind"] == "listener_error"
    assert str(end["error"]) == "listener failed"

    closed = await session.watch_doc(EndStateDoc, context)
    assert closed is not None
    retired = await session.watch_doc(EndStateDoc, context)
    assert retired is not None

    async def retire(tx: Any) -> None:
        await tx.retire_doc(EndStateDoc)

    await session.commit(retire, context)
    retired.start(lambda _call, _delivery_context: None)
    # A retirement is a change and not an end: the watch is the host's to stop (spec §9.1).
    assert await retired.stop() == {"kind": "stopped"}

    await session.close(context)
    assert await closed.closed == {"kind": "ended"}


async def test_a_session_that_closes_or_fails_emits_its_end_under_kind_alone() -> None:
    _storage, session, _publications = await open_state_session()
    await session.close(context)
    closed = cast(dict[str, Any], await session.closed)
    assert closed == {"kind": "closed"}
    # The retired tag key is not merely avoided: nothing settles an end carrying it.
    assert "reason" not in closed

    storage, failing, _publications = await open_state_session()
    failure = RuntimeError("disk failed")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk failed"):
        await set_value(failing, 1)

    end = cast(dict[str, Any], await asyncio.wait_for(failing.closed, 1))
    assert end == {"kind": "failed", "error": failure}
    assert "reason" not in end


async def test_a_session_failure_emits_its_watch_end_under_kind_with_the_failure() -> None:
    storage, session, _publications = await open_state_session()
    watch = await session.watch_doc(EndStateDoc, context)
    assert watch is not None
    failure = RuntimeError("disk failed")
    storage.fail_next_commit(failure)

    with pytest.raises(RuntimeError, match="disk failed"):
        await set_value(session, 1)

    end = cast(dict[str, Any], await watch.closed)
    assert end == {"kind": "failed", "error": failure}
    assert "reason" not in end


# ── The document commit change: `kind` beside the `type` that stays (spec §2) ─


def test_an_observer_refuses_a_document_change_written_without_its_kind_tag() -> None:
    """The reader demands the tag: an untagged change of either flavour is refused, not inferred.

    1.4.0 told a retirement from an edit by ``value: None``; reading those keys again would be the
    acceptance #37 deletes, so a change carrying no ``kind`` reaches nothing.
    """

    for canonical in (retirement, edit):
        retired_spelling = written_the_old_way(canonical)
        assert "kind" not in retired_spelling and retired_spelling["type"] == "document"
        with pytest.raises(KeyError, match="kind"):
            observed_operations(3, cast(DocumentCommitChange, retired_spelling))


async def test_a_watch_refuses_a_document_change_written_without_its_kind_tag() -> None:
    """The other live reader of a committed change refuses the same shape: no ``kind``, no delivery."""

    delivered: list[Any] = []
    watch = CommittedWatch({"value": 0}, lambda: None, lambda _error: None)
    watch.start(lambda call, _delivery_context: delivered.append(call))
    with pytest.raises(KeyError, match="kind"):
        watch.advance_document(cast(DocumentCommitChange, written_the_old_way(edit)), context)
    assert delivered == []
    # The very same change, tagged, reaches the listener: the refusal is the missing tag alone.
    watch.advance_document(edit, context)
    for _ in range(50):
        await asyncio.sleep(0.01)
        if delivered:
            break
    assert [call["kind"] for call in delivered] == ["value"]
    assert delivered[0]["value"] == {"value": 1}
    await watch.stop()


def test_a_document_change_tagged_by_kind_reads_as_itself() -> None:
    assert observed_operations(3, retirement) == (RETIREMENT_OPERATIONS, 3)
    assert observed_operations(2, edit) == ([["r", {"value": 1}]], 3)
    assert observed_operations(3, edit) == ([["s", "value", 1]], 3)


def test_a_copied_document_change_has_no_second_tag_to_move() -> None:
    # ``document.copy`` carries no ``kind`` upstream either: it is the keeper among these variants.
    assert observed_operations(3, copied) == ([], 3)


async def test_a_published_document_change_carries_kind_alone() -> None:
    _storage, session, publications = await open_state_session()
    await set_value(session, 1)
    (edited,) = document_changes(publications[-1])
    assert edited["kind"] == "change"

    async def retire(tx: Any) -> None:
        await tx.retire_doc(EndStateDoc)

    await session.commit(retire, context)
    (retired,) = document_changes(publications[-1])
    assert retired["type"] == "document" and retired["kind"] == "retire"
    assert retired["conversationId"] is None
    assert set(retired) == {"type", "kind", "record", "conversationId"}


# ── The report info and its source: `type` became `kind` (spec §10.1) ────────


async def open_reporting_harness(tasks: Sequence[Any] = ()) -> dict[str, Any]:
    """A Harness whose ``on_report`` collects every report's message and info as the host sees it."""

    registry = create_registry()
    if tasks:
        registry.install(define_extension({"name": "spelling", "tasks": list(tasks)}))
    reports: list[dict[str, Any]] = []

    def on_report(report: Report) -> None:
        reports.append(
            {
                "message": str(report["error"]),
                "info": {key: value for key, value in report.items() if key != "error"},
            }
        )

    harness = await Harness.open(
        MemoryStorage(),
        {"models": FauxModels(), "registry": registry, "on_report": on_report},
        context,
    )
    return {"harness": harness, "reports": reports}


async def test_a_report_the_host_sends_under_the_retired_type_key_is_never_read_into_a_kind() -> None:
    """The port holds no reader for a report info tagged by ``type``: it converts nothing.

    The host's own object reaches ``on_report`` exactly as it was written — no ``kind`` is invented
    for it — while the one report written in the single spelling reads as its own.
    """

    opened = await open_reporting_harness()
    harness = opened["harness"]
    error = RuntimeError("the host's own")
    # A host that ignores the declared type is modelled here by ``Any``, not by a cast of the port.
    retired: Any = {"type": "extension_error", "extension": "retired"}
    harness.report(error, retired)
    harness.report(error, {"kind": "extension_error", "extension": "current"})
    assert opened["reports"] == [
        {"message": "the host's own", "info": {"type": "extension_error", "extension": "retired"}},
        {"message": "the host's own", "info": {"kind": "extension_error", "extension": "current"}},
    ]
    # The refused report stays untagged: the port invents no ``kind`` for a spelling it no longer reads.
    assert "kind" not in opened["reports"][0]["info"]
    await harness.close(context)


def test_the_port_emits_every_report_tagged_by_kind() -> None:
    """The port's own report infos are the single spelling: the tag key of a report is ``kind``."""

    task: ReportTask = {"id": TaskId(4), "kind": "spelling.reporter", "conversationId": ConversationId(1)}
    infos: list[ReportInfo] = [
        {"kind": "listener_error"},
        {"kind": "storage_failed"},
        {"kind": "host_error", "callback": "env"},
        {"kind": "task_error", "task": task},
        {"kind": "task_faulted", "task": task, "reason": "threw"},
    ]
    for info in infos:
        assert "type" not in info
        assert info["kind"]
    source: ReportFrom = {"kind": "host", "callback": "env"}
    assert set(source) == {"kind", "callback"}


async def test_a_task_that_reports_its_source_under_the_retired_type_key_is_refused() -> None:
    """``runtime.report()`` reads the source's tag: the ``type`` spelling raises, the ``kind`` one reports."""

    read: list[tuple[bool, Any]] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        source: Mapping[str, Any] = task["input"]["from"]
        tagged = "kind" in source
        try:
            runtime.report(RuntimeError("whose code failed"), source)
            read.append((tagged, None))
        except KeyError as error:
            read.append((tagged, error.args[0]))
        await runtime.commit(lambda: completed("done"), ctx)

    Reporter = define_task(
        {
            "name": "spelling.reporter",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    opened = await open_reporting_harness([Reporter])
    harness = opened["harness"]
    root = await harness.root(context)
    ids: list[Any] = []
    for written in ({"type": "host", "callback": "env"}, {"kind": "host", "callback": "env"}):

        def admit(tx: Any, source: Mapping[str, Any] = written) -> Any:
            return tx.create_task(
                Reporter, {"from": source}, {"ownership": {"kind": "conversation"}}
            )

        ids.append(await root.commit(admit, context))
    harness.resume()
    for id in ids:
        assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    # The two tasks race, so what each read is compared as a set: the source written under ``type``
    # is refused for want of its ``kind``; the tagged one is read.
    assert set(read) == {(True, None), (False, "kind")}
    # Only the source written in the single spelling produced a report at all.
    await eventually(lambda: len(opened["reports"]) == 1)
    (info,) = [report["info"] for report in opened["reports"]]
    assert {key: value for key, value in info.items() if key != "task"} == {
        "kind": "host_error",
        "callback": "env",
    }
    await harness.close(context)


# ── The file change: `WatchChange` became the `kind`-tagged `FileChange` ──────


async def test_a_watched_change_is_emitted_under_kind(tmp_path: Any) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path), watch={"poll_interval_ms": 20})
    changes: list[FileChange] = []
    watcher = get_or_throw(
        await env.watch([WatchTarget({"path": "watched"})], changes.append, BACKGROUND_CONTEXT)
    )
    try:
        get_or_throw(await env.create_dir("watched", None, BACKGROUND_CONTEXT))
        deadline = asyncio.get_running_loop().time() + 3
        while not changes and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.02)
        assert changes
        assert all(change["kind"] == "paths" for change in changes)
        assert "overflow" not in changes[-1] and "error" not in changes[-1]
    finally:
        await watcher.stop(BACKGROUND_CONTEXT)


# ── The document migrate path: the retired spelling it must still read ──────


def _migrate_of(definition: DocDefinition | TaskDefinition) -> Callable[..., Any]:
    """The definition's own migrate: the one path allowed to read a retired spelling."""

    migrate = definition.migrate
    assert migrate is not None, "a built-in definition carries its migrate"
    return migrate


def test_the_stored_live_document_is_still_read_with_its_retired_reason_key() -> None:
    """A version 1 ``pi.live`` names its compaction trigger ``reason``; the migrate path reads it.

    This is the one place a retired key survives on purpose: deleting it here would destroy the
    upgrade path for every database this port's previous release wrote.
    """

    stored: dict[str, Any] = {
        "run": {"taskId": 5, "inputs": [2]},
        "compactions": [{"taskId": 901, "reason": "threshold", "blocking": True, "attempt": 2}],
        "compactionFailure": {"taskId": 902, "reason": "manual", "error": "no model"},
    }
    migrated = cast(dict[str, Any], _migrate_of(LiveDoc.definition)(cast(Any, stored), 1))
    assert migrated["compactions"][0] == {
        "taskId": 901,
        "trigger": "threshold",
        "blocking": True,
        "attempt": 2,
    }
    assert migrated["compactionFailure"] == {
        "taskId": 902,
        "trigger": "manual",
        "error": "no model",
    }
    assert "reason" not in str(migrated)


def test_a_stored_tool_input_is_still_read_without_its_kind_tag() -> None:
    """A version 1 ``pi.tool`` input carried no tag at all; the migrate path gives it ``model``."""

    migrated = cast(
        dict[str, Any],
        _migrate_of(ToolTask.definition)({"assistant": 3, "callId": "c1"}, None, 1),
    )
    assert migrated["input"] == {"kind": "model", "assistant": 3, "callId": "c1"}
    # A version 2 input already carries the tag, so the migrate path leaves it alone.
    tagged = cast(
        dict[str, Any],
        _migrate_of(ToolTask.definition)({"kind": "model", "assistant": 3, "callId": "c1"}, None, 2),
    )
    assert tagged["input"] == {"kind": "model", "assistant": 3, "callId": "c1"}


# ── The unions 1.5.0 leaves alone keep the tag key they have ───────────────


def test_the_unretagged_unions_keep_their_own_tag_key() -> None:
    write: StorageWrite = {"type": "document.retire", "id": DocumentId(6)}
    submission: SubmissionRecord = {
        "id": SubmissionId(5),
        "conversationId": ConversationId(1),
        "type": "input",
        "status": "queued",
    }
    state: TaskState = {"status": "pending", "checkpoint": {"phase": "ready"}}
    base: DocumentContent = {"kind": "base", "version": 1, "value": {}}
    scope: DocumentScope = {"kind": "session"}
    assert write["type"] == "document.retire"
    assert (submission["type"], submission["status"]) == ("input", "queued")
    assert state["status"] == "pending"
    assert base["kind"] == "base"
    assert scope["kind"] == "session"

"""Both discriminant spellings of every union 1.5.0 retags (#33, #35).

Upstream 1.5.0 moves the tag key of six unions from ``type``/``reason`` to ``kind``: the watch
end and the session end (``docs/upstream/spec-1.5.0.md`` §9.1 and §4), the report description and
the report source (§10.1), the document commit change (§2, which adds ``kind: "change"|"retire"``
beside the ``type`` that stays), and the execution environment's file change (upstream
``src/env/index.ts:127``, where the union is also renamed ``WatchChange`` → ``FileChange``). This is
the expand half of expand–migrate–contract: every reader of those unions accepts either spelling,
every writer emits only ``kind``, and #37 deletes the retired spelling again.

#35 settled the two pairs that move more than the key, and this file pins both halves: a watch end
written with the 1.4.0 tag *and* the 1.4.0 word reads as the end the shipped build carries
(``chord/src/watch.ts:16-19``), and a file change written untagged reads as the tagged one.

Each case below takes a value written in the retired spelling through the same reader that takes
the value written in the current spelling, and asserts the reader cannot tell them apart. The
unions whose tag key 1.5.0 leaves alone are asserted here too — a spelling sweep that converted
them by accident would fail this file, not pass unnoticed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any, cast

import pytest
from chat_support import FauxModels
from session_support import context, document_changes, open_test_session
from task_support import completed, eventually

from pi_durable import define_doc, define_extension, define_task
from pi_durable.context import BACKGROUND_CONTEXT, with_cancel
from pi_durable.env import (
    FileChange,
    FileChangeFailed,
    FileChangeFailedLegacy,
    FileChangeInput,
    FileChangeOverflow,
    FileChangeOverflowLegacy,
    FileChangePaths,
    FileChangePathsLegacy,
    FileError,
    WatchTarget,
    get_or_throw,
    to_file_change,
)
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import (
    Report,
    ReportFromInput,
    ReportInfoInput,
    ReportTask,
    to_report_from,
    to_report_info,
)
from pi_durable.ids import ConversationId, DocumentId, Seq, SubmissionId, TaskId
from pi_durable.session.observation import RETIREMENT_OPERATIONS, observed_operations
from pi_durable.storage.memory import MemoryStorage
from pi_durable.types import (
    DocumentCommitChangeInput,
    DocumentContent,
    DocumentRecord,
    DocumentScope,
    SessionEndInput,
    StorageWrite,
    SubmissionRecord,
    TaskState,
    WatchEnd,
    WatchEndInput,
    to_document_commit_change,
    to_session_end,
    to_watch_end,
)

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


# ── The watch end: `reason` became `kind`, and the ends were renamed (§9.1) ───


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


async def test_a_session_that_fails_emits_its_watch_end_under_kind_with_the_failure() -> None:
    storage, session, _publications = await open_state_session()
    watch = await session.watch_doc(EndStateDoc, context)
    assert watch is not None
    failure = RuntimeError("disk failed")
    storage.fail_next_commit(failure)

    with pytest.raises(RuntimeError, match="disk failed"):
        await set_value(session, 1)

    end = cast(dict[str, Any], await watch.closed)
    assert end == {"kind": "failed", "error": failure}


def test_a_watch_end_written_with_the_retired_reason_key_reads_as_the_same_end() -> None:
    """The retired tag key came with the retired words, so the reader moves both at once (#35)."""

    pairs: list[tuple[WatchEndInput, WatchEnd]] = [
        ({"reason": "stopped"}, {"kind": "stopped"}),
        ({"reason": "cancelled"}, {"kind": "aborted"}),
        ({"reason": "session_closed"}, {"kind": "ended"}),
    ]
    for retired_spelling, current in pairs:
        assert to_watch_end(retired_spelling) == to_watch_end(current) == current

    failure = RuntimeError("listener threw")
    errored: list[tuple[WatchEndInput, WatchEnd]] = [
        ({"reason": "listener_error", "error": failure}, {"kind": "listener_error", "error": failure}),
        ({"reason": "session_failed", "error": failure}, {"kind": "failed", "error": failure}),
    ]
    for retired_spelling, current in errored:
        assert to_watch_end(retired_spelling) == to_watch_end(current) == current

    # The 1.4.0 ``retired`` end has no successor to read it as: a retirement is a change now.
    with pytest.raises(ValueError, match="retirement is a change"):
        to_watch_end(cast(WatchEndInput, {"reason": "retired"}))


# ── The session end: `reason` became `kind` (spec §4, line 1378) ────────────


async def test_a_session_close_emits_its_end_under_kind() -> None:
    _storage, session, _publications = await open_state_session()
    await session.close(context)
    assert await session.closed == {"kind": "closed"}


async def test_a_session_failure_emits_its_end_under_kind_with_the_failure() -> None:
    storage, session, _publications = await open_state_session()
    failure = RuntimeError("disk failed")
    storage.fail_next_commit(failure)

    with pytest.raises(RuntimeError, match="disk failed"):
        await set_value(session, 1)

    assert await asyncio.wait_for(session.closed, 1) == {"kind": "failed", "error": failure}


def test_a_session_end_written_with_the_retired_reason_key_reads_as_the_same_end() -> None:
    failure = RuntimeError("disk failed")
    pairs: list[tuple[SessionEndInput, SessionEndInput]] = [
        ({"reason": "closed"}, {"kind": "closed"}),
        ({"reason": "failed", "error": failure}, {"kind": "failed", "error": failure}),
    ]
    for retired_spelling, current_spelling in pairs:
        assert to_session_end(retired_spelling) == to_session_end(current_spelling)


# ── The document commit change: `kind` beside the `type` that stays (spec §2) ─

record: DocumentRecord = {
    "id": DocumentId(6),
    "kind": "spelling.state",
    "createdAt": Seq(1),
    "scope": {"kind": "session"},
}
retirement: DocumentCommitChangeInput = {
    "type": "document",
    "kind": "retire",
    "record": record,
    "conversationId": None,
}
retirement_written_the_old_way: DocumentCommitChangeInput = {
    "type": "document",
    "record": record,
    "conversationId": None,
    "version": None,
    "value": None,
    "ops": [],
}
edit: DocumentCommitChangeInput = {
    "type": "document",
    "kind": "change",
    "record": record,
    "conversationId": None,
    "version": 3,
    "value": {"value": 1},
    "ops": [["s", "value", 1]],
}
edit_written_the_old_way: DocumentCommitChangeInput = {
    "type": "document",
    "record": record,
    "conversationId": None,
    "version": 3,
    "value": {"value": 1},
    "ops": [["s", "value", 1]],
}


def test_a_document_retirement_written_the_old_way_reads_as_the_same_change() -> None:
    assert (
        to_document_commit_change(retirement_written_the_old_way)
        == to_document_commit_change(retirement)
        == retirement
    )
    assert observed_operations(3, retirement_written_the_old_way) == observed_operations(
        3, retirement
    ) == (RETIREMENT_OPERATIONS, 3)


def test_a_document_edit_written_the_old_way_reads_as_the_same_change() -> None:
    assert to_document_commit_change(edit_written_the_old_way) == to_document_commit_change(edit) == edit
    assert observed_operations(2, edit_written_the_old_way) == observed_operations(2, edit) == (
        [["r", {"value": 1}]],
        3,
    )
    assert observed_operations(3, edit_written_the_old_way) == observed_operations(3, edit) == (
        [["s", "value", 1]],
        3,
    )


def test_a_copied_document_change_has_no_second_tag_to_move() -> None:
    copied: DocumentCommitChangeInput = {
        "type": "document.copy",
        "record": record,
        "conversationId": ConversationId(1),
        "source": {"id": DocumentId(7), "at": "current"},
    }
    assert to_document_commit_change(copied) == copied


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


# ── The report description and its source: `type` became `kind` (spec §10.1) ─


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


def test_a_report_info_written_with_the_retired_type_key_reads_as_the_same_info() -> None:
    task: ReportTask = {"id": TaskId(4), "kind": "spelling.reporter", "conversationId": ConversationId(1)}
    pairs: list[tuple[ReportInfoInput, ReportInfoInput]] = [
        ({"type": "listener_error"}, {"kind": "listener_error"}),
        ({"type": "storage_failed"}, {"kind": "storage_failed"}),
        ({"type": "host_error", "callback": "env"}, {"kind": "host_error", "callback": "env"}),
        ({"type": "task_error", "task": task}, {"kind": "task_error", "task": task}),
        (
            {"type": "task_faulted", "task": task, "reason": "threw"},
            {"kind": "task_faulted", "task": task, "reason": "threw"},
        ),
    ]
    for retired_spelling, current_spelling in pairs:
        assert to_report_info(retired_spelling) == to_report_info(current_spelling)
    assert to_report_info({"type": "listener_error"}) == {"kind": "listener_error"}


async def test_a_report_the_host_writes_the_old_way_reaches_the_host_tagged_by_kind() -> None:
    opened = await open_reporting_harness()
    harness = opened["harness"]
    error = RuntimeError("the host's own")
    harness.report(error, {"type": "extension_error", "extension": "retired"})
    harness.report(error, {"kind": "extension_error", "extension": "current"})
    assert opened["reports"] == [
        {"message": "the host's own", "info": {"kind": "extension_error", "extension": "retired"}},
        {"message": "the host's own", "info": {"kind": "extension_error", "extension": "current"}},
    ]
    await harness.close(context)


def test_a_report_from_written_with_the_retired_type_key_reads_as_the_same_source() -> None:
    pairs: list[tuple[ReportFromInput, ReportFromInput]] = [
        ({"type": "extension", "extension": "spelling"}, {"kind": "extension", "extension": "spelling"}),
        ({"type": "host", "callback": "env"}, {"kind": "host", "callback": "env"}),
    ]
    for retired_spelling, current_spelling in pairs:
        assert to_report_from(retired_spelling) == to_report_from(current_spelling)
    assert to_report_from({"type": "host", "callback": "env"}) == {"kind": "host", "callback": "env"}


async def test_a_task_that_reports_the_old_way_about_its_source_is_reported_identically() -> None:
    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        runtime.report(RuntimeError("whose code failed"), task["input"]["from"])
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
    await eventually(lambda: len(opened["reports"]) == 2)
    infos = [report["info"] for report in opened["reports"]]
    assert [info["kind"] for info in infos] == ["host_error", "host_error"]
    assert infos[0]["task"]["id"] != infos[1]["task"]["id"]
    assert {key: value for key, value in infos[0].items() if key != "task"} == {
        key: value for key, value in infos[1].items() if key != "task"
    } == {"kind": "host_error", "callback": "env"}
    await harness.close(context)


# ── The file change: `WatchChange` became the `kind`-tagged `FileChange` ──────


def test_a_file_change_written_the_old_way_reads_as_the_same_change() -> None:
    failure = FileError("invalid", "too many directories")
    pairs: list[tuple[FileChangeInput, FileChangeInput]] = [
        (
            FileChangePathsLegacy(paths=["a.txt", "dir"]),
            FileChangePaths(kind="paths", paths=["a.txt", "dir"]),
        ),
        (FileChangeOverflowLegacy(overflow=True), FileChangeOverflow(kind="overflow")),
        (FileChangeFailedLegacy(error=failure), FileChangeFailed(kind="failed", error=failure)),
    ]
    for retired_spelling, current_spelling in pairs:
        assert to_file_change(retired_spelling) == to_file_change(current_spelling)
    assert to_file_change(FileChangeOverflowLegacy(overflow=True)) == {"kind": "overflow"}
    assert to_file_change(FileChangeFailedLegacy(error=failure)) == {"kind": "failed", "error": failure}
    assert to_file_change(FileChangePathsLegacy(paths=[])) == {"kind": "paths", "paths": []}


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

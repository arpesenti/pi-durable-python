"""Translated upstream ``harness-read-only`` cases: a viewer of a session another process drives.

Upstream ``test/harness-read-only.test.ts`` holds 4 cases; all 4 are translated here. None is
telemetry-only, so none is excluded.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from tempfile import mkdtemp
from typing import Any

import pytest
from chat_support import all_entries, assistant_message, chat_setup, live_of, open_chat, unanswered
from session_support import context
from task_support import eventually

from pi_durable import Harness, LiveDoc, ReadOnlySession, UserEntry
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage

_directories: list[str] = []


def sqlite_path() -> Path:
    """A fresh file for one test's session; the directories are removed at process exit."""

    directory = mkdtemp(prefix="pi-durable-read-only-")
    _directories.append(directory)
    return Path(directory) / "session.sqlite"


def head(path: Path) -> int | None:
    """The next sequence the file would commit, read beside every open handle.

    ``None`` before the schema exists.
    """

    if not path.exists():
        return None
    connection = sqlite3.connect(str(path), isolation_level=None)
    try:
        exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'durable_metadata'"
        ).fetchone()
        if exists is None:
            return None
        row = connection.execute(
            "SELECT next_seq FROM durable_metadata WHERE singleton = 1"
        ).fetchone()
        return None if row is None else int(row[0])
    finally:
        connection.close()


async def open_read_only(
    path: Path,
    setup: Any,
    on_wake: Any = None,
) -> Harness:
    """A viewer of ``path``: the same options as a driver, with ``read_only`` set."""

    options: dict[str, Any] = {
        "models": setup.models,
        "registry": setup.registry,
        "read_only": True,
        "on_report": lambda report: setup.reports.append(report["error"]),
    }
    if on_wake is not None:
        options["on_wake"] = on_wake
    return await Harness.open(await open_posix_sqlite_storage(str(path)), options, context)


async def running_session(path: Path, setup: Any) -> dict[str, Any]:
    """A session whose root generation is running in another Harness, still open, waiting for the model."""

    setup.faux.set_responses([unanswered()])
    driver = await open_chat(await open_posix_sqlite_storage(str(path)), setup)
    await driver["root"].submit({"type": "input", "content": "slow"}, context)
    await eventually(lambda: setup.faux.call_count == 1)
    return driver


async def test_opens_a_session_with_work_running_elsewhere_without_writing() -> None:
    path = sqlite_path()
    setup = chat_setup()
    driver = await running_session(path, setup)
    before = head(path)
    wakes: list[Any] = []

    viewer = await open_read_only(path, chat_setup(), lambda wake: wakes.append(wake))
    root = await viewer.root(context)
    assert [entry["kind"] for entry in await all_entries(root)] == [UserEntry.kind]
    live = live_of(await viewer.snapshot(LiveDoc, root.id, context))
    assert live.get("run") is not None
    record = await viewer.get_task(live["run"]["taskId"], context)
    assert record is not None and record["state"]["status"] == "running"
    inspection = await viewer.inspect(context)
    assert inspection["scheduling"] == "paused"
    assert viewer.wake() == {"active": False}
    await viewer.close(context)

    assert head(path) == before
    assert wakes == []
    await driver["harness"].close(context)


async def test_refuses_writes_and_scheduling_and_stays_usable() -> None:
    path = sqlite_path()
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("one")])
    driver = await open_chat(await open_posix_sqlite_storage(str(path)), setup)
    await (await driver["root"].submit({"type": "input", "content": "hi"}, context)).wait(context)
    await driver["harness"].close(context)
    before = head(path)

    viewer_setup = chat_setup()
    viewer = await open_read_only(path, viewer_setup)
    root = await viewer.root(context)
    with pytest.raises(ReadOnlySession):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "app.note"}), context)
    with pytest.raises(ReadOnlySession):
        await root.submit({"type": "input", "content": "again"}, context)
    with pytest.raises(ReadOnlySession):
        viewer.resume()
    with pytest.raises(ReadOnlySession):
        await viewer.wait_for_idle(context)
    with pytest.raises(ReadOnlySession):
        await root.abort(context)
    with pytest.raises(ReadOnlySession):
        await viewer.create_conversation({"ownership": {"kind": "ownerless"}}, context)

    # Still healthy: reads work, and a commit that writes nothing succeeds.
    async def nothing(tx: Any) -> str:
        return "nothing"

    assert await root.commit(nothing, context) == "nothing"
    assert len(await all_entries(root)) == 2
    await viewer.close(context)
    assert head(path) == before
    assert viewer_setup.reports == []


async def test_refuses_to_create_the_root_of_an_empty_storage() -> None:
    path = sqlite_path()
    viewer = await open_read_only(path, chat_setup())
    with pytest.raises(ReadOnlySession):
        await viewer.root(context, {"agent": {"model": {"provider": "faux", "modelId": "faux-1"}}})
    await viewer.close(context)
    assert head(path) == 1


async def test_leaves_recovery_to_the_next_writable_open() -> None:
    path = sqlite_path()
    setup = chat_setup()
    driver = await running_session(path, setup)
    live = live_of(await driver["harness"].snapshot(LiveDoc, driver["root"].id, context))
    task_id = live["run"]["taskId"]

    viewer = await open_read_only(path, chat_setup())
    await viewer.close(context)

    # The writable open recovers the task once, as if the viewer had never opened it.
    writer = await open_chat(await open_posix_sqlite_storage(str(path)), chat_setup())
    record = await writer["harness"].get_task(task_id, context)
    assert record is not None and record["state"]["status"] == "pending"
    assert record.get("restarts") == 1
    await writer["harness"].close(context)
    await driver["harness"].close(context)

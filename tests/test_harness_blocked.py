"""Translated v1.4.0 ``harness-blocked`` suite: the crash-loop guard and parked tasks (spec §5.4).

Open counts every surviving ``running`` task as cut off and writes it back as ``pending`` with
``restarts`` one higher; a state the task commits that differs from its current one is progress and
clears the count, while a close writes the tasks it stopped back as ``pending`` uncounted. At
``settings.restart_limit`` the task is parked: blocked with reason ``restart_limit``, reported once
as ``task_parked``, not active in ``wake()``, and run again by ``Harness.retry_task()``. An abort
mark restarts the count and still runs the handler; an abort handler cut off that many times ends
the task ``faulted``. The task graph publishes why a task cannot run and follows registry changes.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
from session_support import context, flush
from task_support import completed, deferred, eventually, open_tasks

from pi_durable import define_extension, define_task
from pi_durable.context import Context
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage
from pi_durable.types import Seq, StorageWrite

_directories: list[Path] = []
#: Storages of killed Harnesses, never closed by them: closed after each test.
_killed: list[Any] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Any:
    yield
    while _killed:
        storage = _killed.pop()
        try:
            await storage.close(context)
        except Exception:
            pass
    while _directories:
        shutil.rmtree(_directories.pop(), ignore_errors=True)


def _sqlite_path() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-blocked-"))
    _directories.append(directory)
    return directory / "session.sqlite"


def task_write(record: Mapping[str, Any]) -> Any:
    """A task write of a record a test built by hand."""

    return record


async def _never() -> Any:
    await asyncio.Event().wait()


async def _hang_until_aborted(signal: Any) -> None:
    """Stay in the phase until the invocation is signalled, then return without committing."""

    if signal is None:
        return
    event = asyncio.Event()
    signal.add_listener(event.set)
    if signal.aborted:
        return
    await event.wait()


def _crash_loop(state: dict[str, Any]) -> Any:
    """A task that hangs in its phase while ``crashing``, as one killed every time would."""

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        state["started"] += 1
        if state["crashing"]:
            await _hang_until_aborted(runtime.signal)
        else:
            await runtime.commit(lambda: completed(None), ctx)

    return define_task(
        {
            "name": "test.crash-loop",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )


class _KillableStorage:
    """A Storage whose calls hang once it is killed: a crash writes nothing more."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.dead = False

    def kill(self) -> None:
        self.dead = True
        _killed.append(self._inner)

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._inner, name)
        if not callable(value):
            return value

        def call(*args: Any, **kwargs: Any) -> Any:
            if self.dead:
                return _never()
            return value(*args, **kwargs)

        return call


class _FailingOnceStorage:
    """A Storage whose first ``commit`` rejects, as on a platform reset."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.fail_next = True

    def __getattr__(self, name: str) -> Any:
        value = getattr(self._inner, name)
        if not callable(value):
            return value
        if name != "commit":
            return value

        async def commit(writes: Sequence[StorageWrite], commit_context: Context) -> Seq:
            if self.fail_next:
                self.fail_next = False
                raise RuntimeError("reset")
            return await value(writes, commit_context)

        return commit


async def _open_with(
    path: Path,
    registry: Any,
    settings: Mapping[str, Any] | None = None,
    inner: Any = None,
) -> dict[str, Any]:
    """Open a Harness over ``path``; ``kill()`` simulates a crash of the process running it."""

    wakes: list[dict[str, Any]] = []
    storage = inner if inner is not None else await open_posix_sqlite_storage(str(path))
    killable = _KillableStorage(storage)
    options: dict[str, Any] = {"registry": registry, "on_wake": wakes.append}
    if settings is not None:
        options["settings"] = settings
    opened = await open_tasks(killable, [], options)
    return {**opened, "wakes": wakes, "kill": killable.kill}


def _register(registry: Any, name: str, task: Any) -> None:
    registry.install(define_extension({"name": name, "tasks": [task]}))


async def _root_task(harness: Harness, task: Any, options: Mapping[str, Any] | None = None) -> Any:
    root = await harness.root(context)
    creation: dict[str, Any] = {"ownership": {"kind": "conversation"}}
    creation.update(options or {})

    async def create(tx: Any) -> Any:
        return await tx.create_task(task, None, creation)

    return await root.commit(create, context)


async def test_keeps_the_hosts_wake_active_until_opens_recovery_pass_has_landed() -> None:
    path = _sqlite_path()

    async def run(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    Held = define_task(
        {
            "name": "test.held",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    registry = create_registry()
    _register(registry, "held", Held)
    first = await _open_with(path, registry)
    id = await _root_task(first["harness"], Held)
    await first["harness"].close(context)
    # A crash left the task holding its outcome: the next open's recovery pass makes it terminal.
    raw = await open_posix_sqlite_storage(str(path))
    found = await raw.task(id, context)
    assert found is not None
    record = dict(found)
    record.pop("memos", None)
    record["state"] = {"status": "completing", "outcome": {"status": "completed", "result": None}}
    writes: list[StorageWrite] = [{"type": "task", "value": task_write(record)}]
    await raw.commit(writes, context)
    await raw.close(context)

    # That pass's commit fails, as on a platform reset.
    inner = await open_posix_sqlite_storage(str(path))
    failing = _FailingOnceStorage(inner)
    wakes: list[dict[str, Any]] = []
    opened = await open_tasks(
        failing, [], {"registry": registry, "on_wake": wakes.append}
    )
    harness = opened["harness"]
    closed = await harness.closed
    assert closed["reason"] == "failed"
    # The host was told work is underway, and never that it ended: its recovery alarm stays.
    assert wakes == [{"active": True}]
    await inner.close(context)


async def test_parks_a_task_cut_off_restart_limit_times_in_a_row_and_runs_it_again_on_retry() -> None:
    path = _sqlite_path()
    state = {"crashing": True, "started": 0}
    registry = create_registry()
    _register(registry, "loop", _crash_loop(state))
    settings: dict[str, Any] = {"restart_limit": 3}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], _crash_loop(state))
    # Each open runs the phase, and each crash cuts it off: three restarts.
    def reached(count: int) -> Callable[[], bool]:
        return lambda: state["started"] == count

    for run in range(1, 4):
        opened["harness"].resume()
        await eventually(reached(run))
        opened["kill"]()
        opened = await _open_with(path, registry, settings)
    harness = opened["harness"]
    record = await harness.get_task(id, context)
    assert record["restarts"] == 3
    assert opened["infos"] == [
        {
            "type": "task_parked",
            "task": {"id": id, "kind": "test.crash-loop", "conversationId": 1},
            "extension": "loop",
        }
    ]
    inspection = await harness.inspect(context)
    found = next(task for task in inspection["tasks"] if task["record"]["id"] == id)
    assert found["state"] == {"kind": "blocked", "reason": "restart_limit"}
    graph = await harness.task_graph(context)
    assert graph.value["tasks"][str(id)]["blocked"] == "restart_limit"
    # Parked work needs no process: nothing is active.
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False}
    harness.resume()
    # A scheduling pass runs within a few turns; it leaves the parked task out.
    for _turn in range(5):
        await flush()
    assert state["started"] == 3

    state["crashing"] = False
    await harness.retry_task(id, context)
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    assert state["started"] == 4
    graph.dispose()
    await harness.close(context)


async def test_counts_no_restart_for_a_task_a_close_stopped() -> None:
    path = _sqlite_path()
    state = {"crashing": True, "started": 0}
    registry = create_registry()
    Loop = _crash_loop(state)
    _register(registry, "loop", Loop)
    settings: dict[str, Any] = {"restart_limit": 1}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], Loop)
    opened["harness"].resume()
    await eventually(lambda: state["started"] == 1)
    await opened["harness"].close(context)
    opened = await _open_with(path, registry, settings)
    record = await opened["harness"].get_task(id, context)
    assert record["state"]["status"] == "pending"
    assert "restarts" not in record
    state["crashing"] = False
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    await opened["harness"].close(context)


async def test_keeps_an_abort_mark_admitted_before_close_in_the_record_close_writes_back() -> None:
    path = _sqlite_path()
    state = {"crashing": True, "started": 0}
    registry = create_registry()
    Loop = _crash_loop(state)
    _register(registry, "loop", Loop)
    opened = await _open_with(path, registry)
    harness = opened["harness"]
    id = await _root_task(harness, Loop)
    root = await harness.root(context)
    harness.resume()
    await eventually(lambda: state["started"] == 1)
    # Hold the line; the abort mark is admitted behind it, then close begins.
    gate = deferred()
    entered = asyncio.Event()

    async def hold(_tx: Any) -> None:
        entered.set()
        await gate.promise

    holding = asyncio.ensure_future(root.commit(hold, context))
    await entered.wait()
    aborting = asyncio.ensure_future(harness.abort_task(id, context))
    # Admitted before the seal: it checks admission, then waits for the line.
    await flush()
    closing = asyncio.ensure_future(harness.close(context))
    # The invocation ends while the line is held; its write-back waits behind the abort mark.
    gate.resolve()
    await holding
    assert await aborting == "marked"
    await closing
    raw = await open_posix_sqlite_storage(str(path))
    record = await raw.task(id, context)
    assert record is not None
    assert record["abortRequested"] is True
    assert record["state"]["status"] == "pending"
    await raw.close(context)


async def test_faults_a_task_whose_abort_handler_is_cut_off_restart_limit_times() -> None:
    path = _sqlite_path()
    aborts = [0]

    async def run(_task: Any, runtime: Any, _ctx: Any) -> None:
        await _hang_until_aborted(runtime.signal)

    async def abort(_task: Any, runtime: Any, _ctx: Any) -> None:
        aborts[0] += 1
        await _hang_until_aborted(runtime.signal)

    Stuck = define_task(
        {
            "name": "test.stuck-abort",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )
    registry = create_registry()
    _register(registry, "stuck", Stuck)
    settings: dict[str, Any] = {"restart_limit": 1}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], Stuck)
    opened["harness"].resume()
    asyncio.ensure_future(opened["harness"].abort_task(id, context))
    await eventually(lambda: aborts[0] == 1)
    opened["kill"]()  # a crash in the abort handler
    opened = await _open_with(path, registry, settings)
    settled = await opened["harness"].wait_for_task(id, context)
    assert settled["state"]["outcome"]["status"] == "faulted"
    assert settled["state"]["outcome"]["error"]["detail"] == {"reason": "restart_limit"}
    # The receipt keeps no restart count.
    assert "restarts" not in settled
    assert aborts[0] == 1
    await eventually(lambda: opened["infos"] != [])
    assert opened["infos"] == [
        {
            "type": "task_faulted",
            "task": {"id": id, "kind": "test.stuck-abort", "conversationId": 1},
            "extension": "stuck",
            "reason": "restart_limit",
        }
    ]
    await opened["harness"].close(context)


async def test_runs_a_parked_task_once_a_raised_restart_limit_no_longer_parks_it() -> None:
    path = _sqlite_path()
    state = {"crashing": True, "started": 0}
    registry = create_registry()
    Loop = _crash_loop(state)
    _register(registry, "loop", Loop)
    settings: dict[str, Any] = {"restart_limit": 1}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], Loop)
    opened["harness"].resume()
    await eventually(lambda: state["started"] == 1)
    opened["kill"]()  # a crash: a close would not count
    opened = await _open_with(path, registry, settings)
    harness = opened["harness"]
    graph = await harness.task_graph(context)
    harness.resume()
    await eventually(lambda: graph.value["tasks"][str(id)].get("blocked") == "restart_limit")
    # Let a scheduling pass leave the parked task out.
    for _turn in range(5):
        await flush()
    state["crashing"] = False
    # The host's live settings change; the next pass runs the task.
    settings["restart_limit"] = float("inf")
    harness.resume()
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    assert state["started"] == 2
    graph.dispose()
    await harness.close(context)


async def test_still_runs_the_abort_handler_of_a_parked_task() -> None:
    path = _sqlite_path()
    state = {"crashing": True, "started": 0}
    aborted: list[Any] = []

    async def run(_task: Any, runtime: Any, _ctx: Any) -> None:
        state["started"] += 1
        await _hang_until_aborted(runtime.signal)

    async def abort(task: Any, runtime: Any, ctx: Any) -> None:
        aborted.append(task["id"])
        await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)

    Loop = define_task(
        {
            "name": "test.crash-loop",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )
    registry = create_registry()
    _register(registry, "loop", Loop)
    settings: dict[str, Any] = {"restart_limit": 1}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], Loop)
    opened["harness"].resume()
    await eventually(lambda: state["started"] == 1)
    opened["kill"]()  # a crash: a close would not count
    opened = await _open_with(path, registry, settings)
    await opened["harness"].abort_task(id, context)
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"]["status"] == "aborted"
    assert aborted == [id]
    assert state["started"] == 1
    await opened["harness"].close(context)


async def test_does_not_count_the_same_checkpoint_committed_again_as_progress() -> None:
    path = _sqlite_path()
    state = {"started": 0}

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        state["started"] += 1
        # Intent before an effect, then the effect never returns: the checkpoint is unchanged.
        await runtime.commit(
            lambda: {"status": "running", "checkpoint": task["state"]["checkpoint"]}, ctx
        )
        await _hang_until_aborted(runtime.signal)

    Intent = define_task(
        {
            "name": "test.intent",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    registry = create_registry()
    _register(registry, "intent", Intent)
    settings: dict[str, Any] = {"restart_limit": 2}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], Intent)
    def started_once(count: int) -> Callable[[], bool]:
        return lambda: state["started"] == count

    for attempt in range(1, 3):
        opened["harness"].resume()
        await eventually(started_once(attempt))
        await eventually(_running(opened["harness"], id))
        opened["kill"]()  # a crash: a close would not count
        opened = await _open_with(path, registry, settings)
    record = await opened["harness"].get_task(id, context)
    assert record["restarts"] == 2
    inspection = await opened["harness"].inspect(context)
    assert inspection["tasks"][0]["state"] == {"kind": "blocked", "reason": "restart_limit"}
    await opened["harness"].close(context)


def _running(harness: Harness, id: Any) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        record = await harness.get_task(id, context)
        return record is not None and record["state"]["status"] == "running"

    return check


async def test_starts_the_count_over_when_the_task_makes_progress() -> None:
    path = _sqlite_path()
    state = {"crashing": True, "started": 0}

    async def one(_task: Any, runtime: Any, ctx: Any) -> None:
        state["started"] += 1
        if state["crashing"]:
            await _hang_until_aborted(runtime.signal)
        else:
            await runtime.commit(lambda: {"status": "running", "checkpoint": {"phase": "two"}}, ctx)

    async def two(_task: Any, runtime: Any, _ctx: Any) -> None:
        state["started"] += 1
        await _hang_until_aborted(runtime.signal)

    Steps = define_task(
        {
            "name": "test.steps",
            "version": 1,
            "initial": lambda _input: {"phase": "one"},
            "phases": {"one": one, "two": two},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    registry = create_registry()
    _register(registry, "steps", Steps)
    settings: dict[str, Any] = {"restart_limit": 2}
    opened = await _open_with(path, registry, settings)
    id = await _root_task(opened["harness"], Steps)
    opened["harness"].resume()
    await eventually(lambda: state["started"] == 1)
    opened["kill"]()  # a crash: a close would not count
    opened = await _open_with(path, registry, settings)
    assert (await opened["harness"].get_task(id, context))["restarts"] == 1
    # Progress: phase one commits phase two, which resets the count.
    state["crashing"] = False
    opened["harness"].resume()
    await eventually(lambda: state["started"] == 3)
    assert "restarts" not in (await opened["harness"].get_task(id, context))
    await opened["harness"].close(context)


async def test_tells_the_graph_and_the_host_when_a_migration_fails() -> None:
    path = _sqlite_path()

    async def run(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    V1 = define_task(
        {
            "name": "test.migrating",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )

    def migrate(*_args: Any) -> Any:
        raise RuntimeError("cannot migrate")

    V2 = define_task(
        {
            "name": "test.migrating",
            "version": 2,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
            "migrate": migrate,
        }
    )
    first = create_registry()
    _register(first, "v1", V1)
    opened = await _open_with(path, first)
    # Created, never run: scheduling stays off.
    id = await _root_task(opened["harness"], V1)
    await opened["harness"].close(context)
    second = create_registry()
    _register(second, "v2", V2)
    opened = await _open_with(path, second)
    harness = opened["harness"]
    graph = await harness.task_graph(context)
    harness.resume()
    await eventually(lambda: graph.value["tasks"][str(id)].get("blocked") == "migration_failed")
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False}
    await eventually(lambda: opened["wakes"][-1]["active"] is False)
    graph.dispose()
    await harness.close(context)


async def test_shows_an_abandoned_task_waiting_for_its_definition_as_blocked() -> None:
    path = _sqlite_path()

    async def run(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    Abandoned = define_task(
        {
            "name": "test.abandoned",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    registry = create_registry()
    _register(registry, "abandoned", Abandoned)
    opened = await _open_with(path, registry)
    id = await _root_task(opened["harness"], Abandoned, {"abandonOnRestart": True})
    await opened["harness"].close(context)
    # The next open marks it `restart`; without its definition, it waits for one to clean up.
    opened = await _open_with(path, create_registry())
    harness = opened["harness"]
    graph = await harness.task_graph(context)
    harness.resume()
    await eventually(_abort_reason(harness, id, "restart"))
    await eventually(lambda: graph.value["tasks"][str(id)].get("blocked") == "missing_task")
    graph.dispose()
    await harness.close(context)


def _abort_reason(harness: Harness, id: Any, reason: str) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        record = await harness.get_task(id, context)
        return record is not None and record.get("abortReason") == reason

    return check


async def test_shows_a_task_without_a_definition_as_blocked_and_wakes_the_host_once_unblocked() -> None:
    registry = create_registry()
    path = _sqlite_path()
    opened = await _open_with(path, registry)
    harness = opened["harness"]
    graph = await harness.task_graph(context)

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    Missing = define_task(
        {
            "name": "test.missing",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    id = await _root_task(harness, Missing)
    harness.resume()
    await eventually(lambda: graph.value["tasks"][str(id)].get("blocked") == "missing_task")
    await eventually(lambda: not harness.wake()["active"])
    registry.install(define_extension({"name": "late", "tasks": [Missing]}))
    # The graph follows the registry; the host hears it can run work now.
    await eventually(lambda: graph.value["tasks"].get(str(id), {}).get("blocked") is None)
    await eventually(lambda: any(wake["active"] for wake in opened["wakes"]))
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    graph.dispose()
    await harness.close(context)

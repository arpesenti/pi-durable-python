"""Translated v1.4.0 ``harness-sleep`` suite: durable sleeps, timers, and wakeups (spec §5.6).

A phase may commit ``{status: "sleeping", checkpoint, until}`` (or call ``sleep_until()``), which
ends its invocation without holding one; the task resumes at that checkpoint once the Harness clock
reaches ``until``. ``Harness.wake()`` says when the Session next needs a process, and
``HarnessOptions.on_wake`` replaces the built-in timer for sleeps. Every other timer is armed
through ``HarnessOptions.set_timer``: foreground ones must fire, background ones may be declined.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
from session_support import context, flush
from task_support import aborted, aborted_with, completed, eventually, open_tasks

from pi_durable import MemoryStorage, define_extension, define_task
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.harness.harness import Harness
from pi_durable.storage.jsonl import JsonlStorage
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage

_directories: list[Path] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Any:
    yield
    while _directories:
        shutil.rmtree(_directories.pop(), ignore_errors=True)


def _sqlite_path() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-sleep-"))
    _directories.append(directory)
    return directory / "session.sqlite"


class _Runs:
    """How often a nap task's second phase ran."""

    def __init__(self) -> None:
        self.after = 0


def _nap_task(delay: int, runs: _Runs | None = None) -> Any:
    """Sleeps ``delay`` ms on the Harness clock, then completes with the time it resumed at."""

    state = runs if runs is not None else _Runs()

    async def start(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.sleep_until(runtime.now() + delay, {"phase": "after", "slept": delay}, ctx)

    async def after(_task: Any, runtime: Any, ctx: Any) -> None:
        state.after += 1
        resumed = runtime.now()
        await runtime.commit(lambda: completed(resumed), ctx)

    async def abort(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: aborted_with("test"), ctx)

    return define_task(
        {
            "name": "test.nap",
            "version": 1,
            "initial": lambda _input: {"phase": "start"},
            "phases": {"start": start, "after": after},
            "abort": abort,
        }
    )


async def _start_nap(harness: Harness, task: Any) -> Any:
    root = await harness.root(context)
    return await root.commit(
        lambda tx: tx.create_task(task, None, {"ownership": {"kind": "conversation"}}), context
    )


async def _state_of(harness: Harness, id: Any) -> Any:
    record = await harness.get_task(id, context)
    return None if record is None else record["state"]


class _FakeTimers:
    """A host ``set_timer`` that records the timers that pace work and declines background ones."""

    def __init__(self) -> None:
        self.armed: list[dict[str, Any]] = []

    def __call__(
        self, callback: Callable[[], None], delay_ms: float, *, background: bool = False
    ) -> Callable[[], None] | None:
        if background:
            return None
        timer: dict[str, Any] = {"delay_ms": delay_ms, "callback": callback, "disarmed": False}
        self.armed.append(timer)

        def disarm() -> None:
            timer["disarmed"] = True

        return disarm

    def live(self) -> list[dict[str, Any]]:
        return [timer for timer in self.armed if not timer["disarmed"]]


async def _settle() -> None:
    """Let microtasks and a few timer turns run."""

    await flush()
    await asyncio.sleep(0.02)
    await flush()


async def test_sleeps_without_an_invocation_and_resumes_at_its_checkpoint_once_due() -> None:
    clock = [1_000]
    wakes: list[dict[str, Any]] = []
    runs = _Runs()
    Nap = _nap_task(1_000, runs)
    opened = await open_tasks(
        MemoryStorage(), [Nap], {"now": lambda: clock[0], "on_wake": wakes.append}
    )
    harness = opened["harness"]
    graph = await harness.task_graph(context)
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(_sleeping(harness, id))
    assert await _state_of(harness, id) == {
        "status": "sleeping",
        "checkpoint": {"phase": "after", "slept": 1_000},
        "until": 2_000,
    }
    inspection = await harness.inspect(context)
    found = next(task for task in inspection["tasks"] if task["record"]["id"] == id)
    assert found["state"] == {"kind": "sleeping", "until": 2_000}
    assert graph.value["tasks"][str(id)]["state"] == {
        "status": "sleeping",
        "phase": "after",
        "until": 2_000,
    }
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False, "at": 2_000}
    await _settle()
    assert wakes[-1] == {"active": False, "at": 2_000}

    # Early and repeated wakes run nothing.
    clock[0] = 1_999
    harness.resume()
    harness.resume()
    await _settle()
    assert runs.after == 0
    assert (await _state_of(harness, id))["status"] == "sleeping"

    clock[0] = 2_000
    harness.resume()
    harness.resume()
    receipt = await harness.wait_for_task(id, context)
    assert receipt["state"]["outcome"] == {"status": "completed", "result": 2_000}
    assert runs.after == 1
    await _settle()
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False}
    assert wakes[-1] == {"active": False}
    # Only changes: idle at open, running, asleep until 2000, running again, idle.
    for index in range(1, len(wakes)):
        assert wakes[index] != wakes[index - 1]
    assert {"active": True} in wakes
    assert {"active": False, "at": 2_000} in wakes
    graph.dispose()
    await harness.close(context)


def _sleeping(harness: Harness, id: Any) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        state = await _state_of(harness, id)
        return state is not None and state["status"] == "sleeping"

    return check


async def test_keeps_a_sleep_across_reopen(tmp_path: Path) -> None:
    path = _sqlite_path()
    clock = [1_000]
    Nap = _nap_task(60_000)

    async def open_storage() -> Any:
        return await open_posix_sqlite_storage(str(path))

    async def open(harness_path: Path) -> Any:
        return await open_tasks(
            await open_storage(), [Nap], {"now": lambda: clock[0], "on_wake": lambda _wake: None}
        )

    first = await open(path)
    harness = first["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(_sleeping(harness, id))
    await harness.close(context)

    second = await open(path)
    harness = second["harness"]
    # Known from storage before scheduling starts, once open's recovery pass is done.
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False, "at": 61_000}
    harness.resume()
    await _settle()
    assert (await _state_of(harness, id))["status"] == "sleeping"
    await harness.close(context)

    clock[0] = 61_000
    third = await open(path)
    harness = third["harness"]
    assert harness.wake() == {"active": True}
    harness.resume()
    assert (await harness.wait_for_task(id, context))["state"]["outcome"] == {
        "status": "completed",
        "result": 61_000,
    }
    await harness.close(context)


async def test_keeps_a_sleep_across_reopen_of_a_jsonl_storage_which_needs_no_migration() -> None:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-sleep-jsonl-"))
    _directories.append(directory)
    clock = [1_000]
    Nap = _nap_task(60_000)

    async def open_storage() -> Any:
        return await JsonlStorage.open(
            str(directory), PosixExecutionEnv(cwd=str(directory)), context, None
        )

    async def open(harness_path: Path) -> Any:
        return await open_tasks(
            await open_storage(), [Nap], {"now": lambda: clock[0], "on_wake": lambda _wake: None}
        )

    first = await open(directory)
    harness = first["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(_sleeping(harness, id))
    await harness.close(context)

    second = await open(directory)
    harness = second["harness"]
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False, "at": 61_000}
    clock[0] = 61_000
    harness.resume()
    assert (await harness.wait_for_task(id, context))["state"]["outcome"] == {
        "status": "completed",
        "result": 61_000,
    }
    await harness.close(context)


async def test_resumes_a_due_sleep_with_its_timer_without_on_wake_and_background_timers_declined() -> None:
    clock = [1_000]
    timers = _FakeTimers()
    Nap = _nap_task(500)
    opened = await open_tasks(
        MemoryStorage(), [Nap], {"now": lambda: clock[0], "set_timer": timers}
    )
    harness = opened["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(lambda: any(timer["delay_ms"] == 500 for timer in timers.live()))
    assert (await _state_of(harness, id))["status"] == "sleeping"
    clock[0] = 1_500
    next(timer for timer in timers.live() if timer["delay_ms"] == 500)["callback"]()
    # Polled: wait_for_task() would call resume(), which wakes due sleeps itself.
    await eventually(_terminal(harness, id))
    assert await _state_of(harness, id) == {
        "status": "terminal",
        "outcome": {"status": "completed", "result": 1_500},
    }
    await harness.close(context)


def _terminal(harness: Harness, id: Any) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        state = await _state_of(harness, id)
        return state is not None and state["status"] == "terminal"

    return check


async def test_runs_a_due_sleep_at_any_scheduling_pass_before_its_timer_fires() -> None:
    clock = [1_000]
    timers = _FakeTimers()
    Nap = _nap_task(500)
    opened = await open_tasks(
        MemoryStorage(), [Nap], {"now": lambda: clock[0], "set_timer": timers}
    )
    harness = opened["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(_sleeping(harness, id))
    # The deadline passes, and an unrelated change starts a pass before the timer fires.
    clock[0] = 1_500
    opened["registry"].install(define_extension({"name": "unrelated"}))
    await eventually(_terminal(harness, id))
    assert await _state_of(harness, id) == {
        "status": "terminal",
        "outcome": {"status": "completed", "result": 1_500},
    }
    await harness.close(context)


async def test_disarms_the_sleep_timer_when_the_sleep_is_aborted_and_when_the_harness_closes() -> None:
    clock = [1_000]
    timers = _FakeTimers()
    Short = _nap_task(500)
    opened = await open_tasks(
        MemoryStorage(), [Short], {"now": lambda: clock[0], "set_timer": timers}
    )
    harness = opened["harness"]
    aborted_id = await _start_nap(harness, Short)
    harness.resume()
    await eventually(lambda: any(timer["delay_ms"] == 500 for timer in timers.live()))
    await harness.abort_task(aborted_id, context)
    await harness.wait_for_task(aborted_id, context)
    await eventually(lambda: timers.live() == [])
    kept = await _start_nap(harness, Short)
    await eventually(_sleeping(harness, kept))
    assert [timer["delay_ms"] for timer in timers.live()] == [500]
    await harness.close(context)
    assert timers.live() == []


async def test_disarms_an_invocations_timers_when_it_ends_and_a_late_fire_runs_nothing() -> None:
    fired: list[str] = []

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        runtime.set_timer(lambda: fired.append("late"), 60_000)
        await runtime.commit(lambda: completed(None), ctx)

    Timed = define_task(
        {
            "name": "test.timed",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    timers = _FakeTimers()
    opened = await open_tasks(MemoryStorage(), [Timed], {"set_timer": timers})
    harness = opened["harness"]
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Timed, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()
    await harness.wait_for_task(id, context)
    timer = next(armed for armed in timers.armed if armed["delay_ms"] == 60_000)
    await eventually(lambda: timer["disarmed"])
    # A host timer that fires anyway: the ended invocation's callback does not run.
    timer["callback"]()
    assert fired == []
    await harness.close(context)


async def test_arms_no_timer_for_sleeps_with_on_wake() -> None:
    clock = [1_000]
    timers = _FakeTimers()
    Nap = _nap_task(500)
    opened = await open_tasks(
        MemoryStorage(),
        [Nap],
        {"now": lambda: clock[0], "set_timer": timers, "on_wake": lambda _wake: None},
    )
    harness = opened["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(_sleeping(harness, id))
    await _settle()
    assert timers.armed == []
    clock[0] = 1_500
    assert harness.wake() == {"active": True}
    harness.resume()
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    await harness.close(context)


async def test_arms_a_foreground_timer_the_host_declines_with_the_default_timer_reporting_it_once() -> None:
    Nap = _nap_task(10)
    opened = await open_tasks(MemoryStorage(), [Nap], {"set_timer": lambda *_args, **_kwargs: None})
    harness = opened["harness"]
    first = await _start_nap(harness, Nap)
    second = await _start_nap(harness, Nap)
    harness.resume()
    assert (await harness.wait_for_task(first, context))["state"]["outcome"]["status"] == "completed"
    assert (await harness.wait_for_task(second, context))["state"]["outcome"]["status"] == "completed"
    assert [str(error) for error in opened["reports"]] == [
        "HarnessOptions.set_timer declined a timer that paces work; using the default"
    ]
    await harness.close(context)


async def test_counts_neither_a_wait_on_a_sleeping_task_nor_an_outcome_held_over_one_as_active() -> None:
    clock = [1_000]
    Child = _nap_task(500)

    async def start(task: Any, runtime: Any, ctx: Any) -> None:
        async def create(tx: Any) -> dict[str, Any]:
            child = await tx.create_task(
                Child, None, {"ownership": {"kind": "task", "taskId": task["id"]}}
            )
            # Held: completes next, holding its outcome over the sleeping child. Otherwise waits.
            if task["input"]["hold"]:
                return {"status": "running", "checkpoint": {"phase": "joined", "child": child}}
            return {
                "status": "waiting",
                "checkpoint": {"phase": "joined", "child": child},
                "on": [child],
                "policy": "allSettled",
            }

        await runtime.commit(create, ctx)

    async def joined(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    Parent = define_task(
        {
            "name": "test.parent",
            "version": 1,
            "initial": lambda _input: {"phase": "start"},
            "phases": {"start": start, "joined": joined},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    for hold in (False, True):
        opened = await open_tasks(
            MemoryStorage(),
            [Parent, Child],
            {"now": lambda: clock[0], "on_wake": lambda _wake: None},
        )
        harness = opened["harness"]
        root = await harness.root(context)
        id = await root.commit(
            lambda tx, hold=hold: tx.create_task(
                Parent, {"hold": hold}, {"ownership": {"kind": "conversation"}}
            ),
            context,
        )
        harness.resume()
        parked = "completing" if hold else "waiting"
        await eventually(_status(harness, id, parked))
        await eventually(_wake_is(harness, {"active": False, "at": 1_500}))
        await eventually(_inactive(harness))
        assert harness.wake() == {"active": False, "at": 1_500}
        await harness.close(context)


def _wake_is(harness: Harness, wake: Mapping[str, Any]) -> Callable[[], bool]:
    return lambda: harness.wake() == wake


def _inactive(harness: Harness) -> Callable[[], bool]:
    return lambda: not harness.wake()["active"]


def _status(harness: Harness, id: Any, status: str) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        state = await _state_of(harness, id)
        return state is not None and state["status"] == status

    return check


async def test_runs_the_abort_handler_of_a_sleeping_task_without_waiting_for_its_time() -> None:
    clock = [1_000]
    Nap = _nap_task(60_000)
    opened = await open_tasks(
        MemoryStorage(), [Nap], {"now": lambda: clock[0], "on_wake": lambda _wake: None}
    )
    harness = opened["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(_sleeping(harness, id))
    await harness.abort_task(id, context)
    assert (await harness.wait_for_task(id, context))["state"]["outcome"] == {
        "status": "aborted",
        "reason": "test",
    }
    await harness.close(context)


async def test_rejects_a_sleep_until_something_that_is_not_a_time_and_a_sleep_in_an_abort_handler() -> None:
    errors: list[str] = []

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        for until in (float("nan"), -1, float("inf")):
            try:
                await runtime.sleep_until(until, {"phase": "next"}, ctx)
            except ValueError as error:
                errors.append(str(error))
        await runtime.commit(lambda: completed(None), ctx)

    Invalid = define_task(
        {
            "name": "test.invalid-sleep",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run, "next": lambda *_args: None},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )

    async def aborting(_task: Any, runtime: Any, ctx: Any) -> None:
        try:
            await runtime.sleep_until(runtime.now() + 10, {"phase": "run"}, ctx)
        except (ValueError, RuntimeError) as error:
            errors.append(str(error))
        await runtime.commit(lambda: aborted_with("test"), ctx)

    Aborting = define_task(
        {
            "name": "test.sleep-in-abort",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": lambda _task, runtime, _ctx: aborted(runtime.signal)},
            "abort": aborting,
        }
    )
    opened = await open_tasks(MemoryStorage(), [Invalid, Aborting])
    harness = opened["harness"]
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Invalid, None, {"ownership": {"kind": "conversation"}}), context
    )
    aborting_id = await root.commit(
        lambda tx: tx.create_task(Aborting, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "completed"
    await harness.abort_task(aborting_id, context)
    assert (await harness.wait_for_task(aborting_id, context))["state"]["outcome"]["status"] == "aborted"
    assert errors == [
        f"Task {id} cannot sleep until nan: not a time in milliseconds",
        f"Task {id} cannot sleep until -1: not a time in milliseconds",
        f"Task {id} cannot sleep until inf: not a time in milliseconds",
        f"Abort handler of task {aborting_id} cannot sleep",
    ]
    await harness.close(context)


async def test_does_not_count_a_task_without_a_definition_as_active() -> None:
    Missing = _nap_task(10)
    opened = await open_tasks(MemoryStorage(), [])
    harness = opened["harness"]
    root = await harness.root(context)
    # Created through a token the registry does not hold: nothing can run it.
    await root.commit(
        lambda tx: tx.create_task(Missing, None, {"ownership": {"kind": "conversation"}}), context
    )
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False}
    harness.resume()
    await _settle()
    await eventually(lambda: not harness.wake()["active"])
    assert harness.wake() == {"active": False}
    await harness.close(context)


async def test_reports_a_throwing_on_wake_and_offers_the_same_wake_again_at_the_next_pass() -> None:
    clock = [1_000]
    wakes: list[dict[str, Any]] = []
    failure = RuntimeError("alarm failed")
    failing = [True]
    Nap = _nap_task(1_000)

    def on_wake(wake: Mapping[str, Any]) -> None:
        wakes.append(dict(wake))
        if failing[0] and wake.get("at") is not None:
            raise failure

    opened = await open_tasks(
        MemoryStorage(), [Nap], {"now": lambda: clock[0], "on_wake": on_wake}
    )
    harness = opened["harness"]
    id = await _start_nap(harness, Nap)
    harness.resume()
    await eventually(lambda: any(wake.get("at") == 2_000 and not wake["active"] for wake in wakes))
    assert failure in opened["reports"]
    # The host recovers; an unrelated change offers the same, undelivered wake again.
    failing[0] = False
    offered = len(wakes)
    opened["registry"].install(define_extension({"name": "unrelated"}))
    await eventually(lambda: len(wakes) > offered)
    assert wakes[-1] == {"active": False, "at": 2_000}
    clock[0] = 2_000
    harness.resume()
    await harness.wait_for_task(id, context)
    await eventually(lambda: wakes[-1]["active"] is False and "at" not in wakes[-1])
    await harness.close(context)

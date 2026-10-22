"""Shared Harness test helpers: tasks, deferred gates, and eventual conditions."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from session_support import context, flush

from pi_durable.context import Context
from pi_durable.harness.define import define_extension
from pi_durable.harness.harness import Harness
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import Registry, RegistryReader, RegistrySnapshot
from pi_durable.harness.util import cancellation_error

__all__ = [
    "aborted",
    "aborted_with",
    "completed",
    "context",
    "deferred",
    "eventually",
    "flush",
    "open_tasks",
    "settled",
    "with_signal",
]


async def eventually(check: Callable[[], Any], timeout: float = 2.0) -> None:
    """Await until ``check`` is true, polling like the upstream test helper does."""

    import time

    deadline = time.monotonic() + timeout
    while True:
        value = check()
        if hasattr(value, "__await__"):
            value = await value
        if value:
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.01)


class Deferred:
    """A one-shot signal carrying a value."""

    def __init__(self) -> None:
        self._future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()

    @property
    def promise(self) -> asyncio.Future[Any]:
        return self._future

    def resolve(self, value: Any = None) -> None:
        if not self._future.done():
            self._future.set_result(value)

    def reject(self, error: BaseException) -> None:
        if not self._future.done():
            self._future.set_exception(error)


def deferred() -> Deferred:
    return Deferred()


def with_signal(signal: Any) -> Context:
    from pi_durable.context import with_abort_signal

    return with_abort_signal(signal, context)


async def aborted(signal: Any) -> None:
    """Wait for the signal and reject with its cancellation."""

    await signal.wait()
    raise cancellation_error(signal)


def completed(result: Any = None) -> dict[str, Any]:
    return {"status": "terminal", "outcome": {"status": "completed", "result": result}}


def aborted_with(reason: str) -> dict[str, Any]:
    return {"status": "terminal", "outcome": {"status": "aborted", "reason": reason}}


async def settled(promise: Awaitable[Any]) -> bool:
    """Whether ``promise`` settled after pending work flushes."""

    task = asyncio.ensure_future(promise)
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    await flush()
    return task.done()


class TaskOverrideSnapshot:
    """A registry snapshot with some tasks replaced, for suites that drive their own run loop."""

    def __init__(self, base: RegistrySnapshot, tasks: Sequence[Any]) -> None:
        self._base = base
        self._by_name = {task.definition.name: task for task in tasks}

    def installed(self) -> Any:
        return self._base.installed()

    def extension(self, name: str) -> Any:
        return self._base.extension(name)

    def tools(self) -> Any:
        return self._base.tools()

    def sections(self) -> Any:
        return self._base.sections()

    def tasks(self) -> Any:
        names = self._by_name
        return [names.get(task.definition.name, task) for task in self._base.tasks()]

    def task(self, name: str) -> Any:
        return self._by_name.get(name) or self._base.task(name)


class TaskOverrideReader:
    """A registry reader whose snapshots replace selected task definitions."""

    def __init__(self, registry: RegistryReader, tasks: Sequence[Any]) -> None:
        self._registry = registry
        self._tasks = list(tasks)

    def snapshot(self) -> RegistrySnapshot:
        return TaskOverrideSnapshot(self._registry.snapshot(), self._tasks)

    def subscribe(self, listener: Callable[[], Any]) -> Callable[[], None]:
        return self._registry.subscribe(listener)


async def open_tasks(
    storage: Any,
    tasks: Sequence[Any],
    options: Mapping[str, Any] | None = None,
    override_tasks: Sequence[Any] = (),
) -> dict[str, Any]:
    """Open a Harness whose registry holds ``tasks``; failures passed to on_report are collected.

    ``override_tasks`` replaces built-in definitions in the snapshot the Harness reads, so
    submission suites can keep their own generation probe without installing a colliding task.
    """

    values = dict(options or {})
    registry: Registry = values.get("registry") or create_registry()
    if tasks:
        registry.install(define_extension({"name": "tasks", "tasks": list(tasks)}))
    reader = TaskOverrideReader(registry, override_tasks) if override_tasks else registry
    reports: list[BaseException] = []
    harness = await Harness.open(
        storage,
        {
            "models": values.get("models", {}),
            "registry": reader,
            "on_report": reports.append,
            **({"now": values["now"]} if "now" in values else {}),
            **({"settings": values["settings"]} if "settings" in values else {}),
        },
        context,
    )
    return {"harness": harness, "registry": registry, "reports": reports}


def counting_reader(registry: RegistryReader) -> tuple[RegistryReader, Callable[[], int]]:
    """Registry reader that counts live subscriptions."""

    count = 0

    def subscribe(listener: Callable[[], Any]) -> Callable[[], None]:
        nonlocal count
        count += 1
        unsubscribe = registry.subscribe(listener)
        active = True

        def release() -> None:
            nonlocal active, count
            if not active:
                return
            active = False
            count -= 1
            unsubscribe()

        return release

    class Reader:
        def snapshot(self) -> RegistrySnapshot:
            return registry.snapshot()

        def subscribe(self, listener: Callable[[], Any]) -> Callable[[], None]:
            return subscribe(listener)

    return Reader(), lambda: count

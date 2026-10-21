"""Durable task definitions and invocation-scoped runtime operations.

A task is a full record replaced atomically at every checkpoint. A phase must commit a
changed checkpoint, a wait, a sleep, or an outcome; returning without durable progress
faults the task.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .json import copy_json, json_equal

PhaseHandler = Callable[[dict[str, Any], "TaskRuntime"], Awaitable[None] | None]
AbortHandler = Callable[[dict[str, Any], "TaskRuntime"], Awaitable[None] | None]
MigrateHandler = Callable[[Any, Any, int], Mapping[str, Any]]


class TaskFaulted(RuntimeError):
    pass


from .errors import InvocationEnded


class TaskAborted(InvocationEnded):
    pass


@dataclass(frozen=True)
class TaskDefinition:
    name: str
    version: int
    initial: Callable[[Any], Mapping[str, Any]]
    phases: Mapping[str, PhaseHandler]
    abort: AbortHandler
    migrate: MigrateHandler | None = None
    hooks: Any = None

    def validate(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise TypeError("Task name must be a non-empty string")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise TypeError(f"Task {self.name} version must be a positive integer")
        if not callable(self.initial) or not callable(self.abort):
            raise TypeError(f"Task {self.name} requires initial() and abort()")
        if not self.phases:
            raise TypeError(f"Task {self.name} requires at least one phase")


@dataclass(frozen=True)
class Task:
    definition: TaskDefinition


def define_task(
    *,
    name: str,
    version: int,
    initial: Callable[[Any], Mapping[str, Any]],
    phases: Mapping[str, PhaseHandler],
    abort: AbortHandler,
    migrate: MigrateHandler | None = None,
    hooks: Any = None,
) -> Task:
    definition = TaskDefinition(
        name=name, version=version, initial=initial, phases=phases, abort=abort, migrate=migrate, hooks=hooks
    )
    definition.validate()
    return Task(definition)


class _Missing:
    pass


_MISSING = _Missing()


@dataclass
class RuntimeResult:
    record: dict[str, Any] | None = None
    progressed: bool = False


class TaskRuntime:
    """Operations of one task invocation. Every operation rejects after the invocation ends."""

    def __init__(
        self,
        scheduler: Any,
        record: Mapping[str, Any],
        *,
        mode: str,
        signal: asyncio.Event,
    ) -> None:
        self._scheduler = scheduler
        self._record = copy_json(record)
        self._mode = mode
        self._signal = signal
        self._ended = False
        self._ended_event = asyncio.Event()
        self._timers: set[asyncio.TimerHandle] = set()
        self._last_record: dict[str, Any] | None = None
        self._progressed = False

    @property
    def task_id(self) -> int:
        return self._record["id"]

    @property
    def conversation_id(self) -> int:
        return self._record["conversationId"]

    @property
    def signal(self) -> asyncio.Event:
        return self._signal

    @property
    def registry(self) -> Any:
        registry = self._scheduler.registry
        return registry.snapshot() if hasattr(registry, "snapshot") else registry

    @property
    def settings(self) -> Any:
        return self._scheduler.settings

    @property
    def models(self) -> Any:
        return self._scheduler.models

    async def agent(self, context: Any = None) -> Any:
        return await self._scheduler.harness.resolve_agent(self.conversation_id)

    @property
    def hooks(self) -> Any:
        return self._record.get("hooks")

    def now(self) -> int:
        return self._scheduler.now()

    def check_active(self) -> None:
        if self._ended:
            raise InvocationEnded("Task invocation ended")
        if self._signal.is_set():
            raise TaskAborted("Task invocation was signalled")

    async def env(self) -> Any:
        self.check_active()
        return await self._scheduler.env(self.conversation_id)

    async def commit(
        self, change: Callable[[Any, dict[str, Any]], Any]
    ) -> None:
        """Commit on the Session line after rereading the task."""
        self.check_active()
        record = await self._commit(change, require_progress=True)
        if record is not None:
            self._last_record = record
            self._progressed = True

    async def _commit(
        self, change: Callable[[Any, dict[str, Any]], Any], *, require_progress: bool
    ) -> dict[str, Any] | None:
        session = self._scheduler.session
        holder: dict[str, Any] = {}

        async def run(tx: Any) -> None:
            current = await tx._current_task(self.task_id)
            if current is None:
                raise InvocationEnded(f"Task {self.task_id} no longer exists")
            status = current["state"]["status"]
            if status in ("completing", "terminal"):
                raise InvocationEnded(f"Task {self.task_id} is {status}")
            if (
                self._mode == "run"
                and current.get("abortRequested")
                and (current.get("abortReason") != "restart")
            ):
                raise TaskAborted(f"Task {self.task_id} is abort-marked")
            next_state = change(tx, current)
            if inspect.isawaitable(next_state):
                next_state = await next_state
            if next_state is None:
                if require_progress:
                    raise TaskFaulted(
                        f"Task {self.task_id} phase returned without committing a changed checkpoint or outcome"
                    )
                return
            next_state = copy_json(next_state)
            status = next_state.get("status")
            if status not in ("running", "waiting", "sleeping", "terminal"):
                raise TaskFaulted(f"Task {self.task_id} committed invalid state {status!r}")
            if status == "running" and json_equal(
                next_state.get("checkpoint"), current["state"].get("checkpoint")
            ):
                raise TaskFaulted(f"Task {self.task_id} committed an unchanged checkpoint")
            if status == "terminal":
                staged_children = [
                    task
                    for task in tx.staged_tasks()
                    if task.get("owner") == self.task_id
                    and task["state"]["status"] not in ("terminal", "completing")
                ]
                if staged_children or self._scheduler.has_live_owned_work(self.task_id):
                    next_state = {"status": "completing", "outcome": next_state["outcome"]}
            record = {**current, "state": next_state}
            # A state the task commits itself is progress: the restart count resets.
            record.pop("restarts", None)
            await tx.set_task(record)
            holder["record"] = record

        await session.commit_with(run, _scope(self.conversation_id, self.task_id))
        return holder.get("record")

    async def memo(self, name: str, candidate: Any = _MISSING, context: Any = None) -> Any:
        """First-writer-wins durable memo. Call without ``candidate`` to read."""
        self.check_active()
        if not isinstance(name, str) or not name:
            raise TypeError("memo name must be a non-empty string")
        current = await self.get_task(self.task_id)
        if current is None:
            raise InvocationEnded(f"Task {self.task_id} no longer exists")
        memos = current.get("memos") or {}
        if name in memos:
            return copy_json(memos[name])
        if candidate is _MISSING:
            return None
        stored: dict[str, Any] = {}

        async def run(tx: Any, task: dict[str, Any]) -> None:
            existing = task.get("memos") or {}
            if name in existing:
                stored["winner"] = copy_json(existing[name])
                return
            updated = {**task, "memos": {**existing, name: copy_json(candidate)}}
            await tx.set_task(updated)
            stored["winner"] = copy_json(candidate)
            return

        await self._commit(run, require_progress=False)
        self._progressed = True
        return stored.get("winner")

    async def get_task(self, task_id: int) -> dict[str, Any] | None:
        self.check_active()
        return await self._scheduler.get_task(task_id)

    async def wait_for_task(self, task_id: int) -> dict[str, Any]:
        self.check_active()
        if task_id == self.task_id:
            raise ValueError("A task cannot wait for itself")
        waiter = asyncio.ensure_future(self._scheduler.wait_for_task(task_id))
        ended = asyncio.ensure_future(self._ended_event.wait())
        try:
            done, _ = await asyncio.wait({waiter, ended}, return_when=asyncio.FIRST_COMPLETED)
            if waiter in done:
                return waiter.result()
            raise InvocationEnded("Task invocation ended while waiting")
        finally:
            waiter.cancel()
            ended.cancel()

    async def abort_owned(self, task_id: int, context: Any = None) -> None:
        self.check_active()
        target = await self._scheduler.get_task(task_id)
        if target is None:
            raise ValueError(f"Task {task_id} does not exist")
        if target.get("owner") != self.task_id:
            raise ValueError(f"Task {task_id} is not owned by task {self.task_id}")
        if target["state"]["status"] == "terminal":
            return
        await self._scheduler.abort_task(task_id)
        await self._scheduler.wait_for_task(task_id)

    async def owned_tasks(self, context: Any = None) -> list[dict[str, Any]]:
        self.check_active()
        return await self._scheduler.owned_tasks(self.task_id)

    async def outcomes(self, ids: Sequence[int], context: Any = None) -> list[dict[str, Any]]:
        self.check_active()
        results: list[dict[str, Any]] = []
        for task_id in ids:
            record = await self._scheduler.get_task(task_id)
            if record is None or record["state"]["status"] != "terminal":
                raise ValueError(f"Task {task_id} is not terminal")
            results.append(copy_json(record["state"]["outcome"]))
        return results

    async def conversation(self, conversation_id: int, context: Any = None) -> Any:
        self.check_active()
        factory = self._scheduler.conversation_handle
        if factory is None:
            raise InvocationEnded("This runtime has no conversation handles")
        return await factory(conversation_id, self)

    async def entry(self, entry_id: int, token: Any = None, context: Any = None) -> dict[str, Any] | None:
        self.check_active()

        async def read(tx: Any) -> Any:
            return await tx.entry(entry_id, token)

        return await self._scheduler.session.commit_with(read, _scope(self.conversation_id, self.task_id))

    async def context(self, conversation_id: int, at: int | None = None, context: Any = None) -> dict[str, Any]:
        self.check_active()
        return await self._scheduler.read_context(conversation_id, at)

    def set_timer(self, callback: Callable[[], None], delay_ms: int) -> Callable[[], None]:
        if self._ended:
            return lambda: None
        loop = asyncio.get_running_loop()
        handle = loop.call_later(max(0, delay_ms) / 1000, callback)
        self._timers.add(handle)

        def cancel() -> None:
            handle.cancel()
            self._timers.discard(handle)

        return cancel

    def report(self, error: BaseException, from_: Mapping[str, Any] | None = None) -> None:
        self._scheduler.report(error, {"type": "task_error", "task": self._scheduler.task_info(self._record)}, from_)

    async def sleep_until(self, until: int, checkpoint: Mapping[str, Any], context: Any = None) -> None:
        self.check_active()

        def change(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
            return {"status": "sleeping", "until": int(until), "checkpoint": copy_json(checkpoint)}

        await self.commit(change)

    def end(self) -> None:
        """End the invocation: reject later operations and cancel its timers."""
        if self._ended:
            return
        self._ended = True
        self._ended_event.set()
        for timer in list(self._timers):
            timer.cancel()
        self._timers.clear()

    @property
    def progressed(self) -> bool:
        return self._progressed

    @property
    def last_record(self) -> dict[str, Any] | None:
        return self._last_record




def _scope(conversation_id: int, task_id: int) -> Any:
    from .types import TransactionScope

    return TransactionScope(conversation_id=conversation_id, task_id=task_id)


__all__ = [
    "InvocationEnded",
    "PhaseHandler",
    "Task",
    "TaskAborted",
    "TaskDefinition",
    "TaskFaulted",
    "TaskRuntime",
    "define_task",
]

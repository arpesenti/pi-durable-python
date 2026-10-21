"""Durable task scheduler.

The scheduler reserves one task invocation at a time per task, runs its current phase off
the Session line, and owns reconciliation: waits, sleeps, completing parents, abort marks,
migration, restart parking, and close.

Blocked reasons are derived, never stored. Any task state change is committed atomically
with the work that caused it.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .json import copy_json, error_message
from .tasks import InvocationEnded, TaskAborted, TaskDefinition, TaskFaulted, TaskRuntime
from .types import TransactionScope

SCAN_PAGE_SIZE = 256


@dataclass
class SchedulerSettings:
    """The scheduler-relevant subset of harness settings."""

    restart_limit: int = 5


@dataclass
class Blocked:
    reason: str
    error: BaseException | None = None


class TaskScheduler:
    """Runs live task records from storage; all writes go through the Session."""

    def __init__(
        self,
        session: Any,
        *,
        registry: Any,
        models: Any,
        settings: Callable[[], Any],
        now: Callable[[], int],
        report: Callable[[BaseException, Mapping[str, Any], Mapping[str, Any] | None], None],
        env: Callable[[int], Awaitable[Any]],
        read_context: Callable[[int, int | None], Awaitable[dict[str, Any]]],
        settle_outcome: Callable[[Any, Mapping[str, Any], Mapping[str, Any]], Awaitable[None]] | None = None,
    ) -> None:
        self.session = session
        self.registry = registry
        self._models_source = models
        self._settings = settings
        self._now = now
        self._report = report
        self._env = env
        self._read_context = read_context
        self._settle_outcome = settle_outcome
        self.conversation_handle: Callable[[int, TaskRuntime], Awaitable[Any]] | None = None
        self.harness: Any = None
        self._live: dict[int, dict[str, Any]] = {}
        self._invocations: dict[int, asyncio.Task[None]] = {}
        self._runtimes: dict[int, TaskRuntime] = {}
        self._reserved: set[int] = set()
        self._waiters: dict[int, list[asyncio.Future[dict[str, Any]]]] = {}
        self._wake = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None
        self._running = False
        self._closing = False
        self._closed = False
        self._unsubscribe: Callable[[], None] | None = None
        self._parked_reported: set[int] = set()
        self._blocked: dict[int, Blocked] = {}
        self._conversation_owners: dict[int, int] = {}

    @property
    def settings(self) -> Any:
        value = self._settings()
        return value if value is not None else SchedulerSettings()

    @property
    def models(self) -> Any:
        source = self._models_source
        return source() if callable(source) and not hasattr(source, "get") else source

    @property
    def restart_limit(self) -> int:
        return getattr(self.settings, "restart_limit", 5)

    def now(self) -> int:
        return self._now()

    def report(
        self,
        error: BaseException,
        info: Mapping[str, Any],
        from_: Mapping[str, Any] | None = None,
    ) -> None:
        self._report(error, info, from_)

    async def env(self, conversation_id: int) -> Any:
        return await self._env(conversation_id)

    async def read_context(self, conversation_id: int, at: int | None = None) -> dict[str, Any]:
        return await self._read_context(conversation_id, at)

    def task_info(self, record: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "id": record["id"],
            "kind": record["kind"],
            "conversationId": record["conversationId"],
        }

    # ── Open / recover ─────────────────────────────────────────────────────

    async def open(self) -> None:
        """Load live tasks and reconcile what a previous process left behind."""
        self._unsubscribe = self.session.observe_commits(self._on_commit)
        records = await self._scan_all_tasks()
        for record in records:
            if record["state"]["status"] != "terminal":
                self._live[record["id"]] = record
        await self._load_conversation_owners()
        changes: list[dict[str, Any]] = []
        for record in list(self._live.values()):
            state = record["state"]
            if state["status"] != "running":
                continue
            updated = copy_json(record)
            updated["restarts"] = int(updated.get("restarts") or 0) + 1
            updated["state"] = {"status": "pending", "checkpoint": state.get("checkpoint")}
            changes.append(updated)
        if changes:
            await self._write_records(changes)
        for record in self._live.values():
            if self._parked(record):
                self._report_parked(record)

    async def _scan_all_tasks(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        cursor = None
        while True:
            page = await self.session.storage.scan_tasks({}, SCAN_PAGE_SIZE, cursor)
            records.extend(page["items"])
            cursor = page.get("next")
            if cursor is None:
                return records

    async def _load_conversation_owners(self) -> None:
        cursor = None
        while True:
            page = await self.session.storage.scan_conversations({}, SCAN_PAGE_SIZE, cursor)
            for record in page["items"]:
                owner = record.get("owner")
                if owner is not None:
                    self._conversation_owners[record["id"]] = owner["taskId"]
            cursor = page.get("next")
            if cursor is None:
                return

    async def _write_records(self, records: Sequence[Mapping[str, Any]]) -> None:
        """Directly persist recovered records; used during open, before scheduling."""
        async def run(tx: Any) -> None:
            for record in records:
                await tx.set_task(record)

        await self.session.commit_with(run)

    # ── Lifecycle ──────────────────────────────────────────────────────────

    def resume(self) -> None:
        if self._closing:
            raise RuntimeError("Scheduler is closing")
        self._running = True
        self._wake.set()
        if self._loop_task is None or self._loop_task.done():
            self._loop_task = asyncio.ensure_future(self._loop())

    async def shutdown(self) -> None:
        if self._closed:
            return
        self._closing = True
        self._wake.set()
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001, S110 - abort joins the invocation
                pass
        for runtime in list(self._runtimes.values()):
            runtime.signal.set()
        if self._invocations:
            await asyncio.gather(*list(self._invocations.values()), return_exceptions=True)
        for waiters in self._waiters.values():
            for waiter in waiters:
                if not waiter.done():
                    waiter.cancel()
        self._waiters.clear()
        await self.session.write_at_close(self._pending_writes)
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None
        self._closed = True

    def _pending_writes(self) -> list[dict[str, Any]]:
        writes: list[dict[str, Any]] = []
        for record in self._live.values():
            if record["state"]["status"] != "running":
                continue
            state = record["state"]
            updated = copy_json(record)
            updated["state"] = {"status": "pending", "checkpoint": state.get("checkpoint")}
            updated.pop("endedAt", None)
            writes.append({"type": "task", "value": updated})
        return writes

    async def wait_for_task(self, task_id: int) -> dict[str, Any]:
        record = await self.get_task(task_id)
        if record is None:
            raise ValueError(f"Task {task_id} does not exist")
        if record["state"]["status"] == "terminal":
            return record
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(task_id, []).append(future)
        try:
            return await future
        finally:
            waiters = self._waiters.get(task_id)
            if waiters is not None and future in waiters:
                waiters.remove(future)
            if waiters is not None and not waiters:
                self._waiters.pop(task_id, None)

    async def get_task(self, task_id: int) -> dict[str, Any] | None:
        cached = self._live.get(task_id)
        if cached is not None:
            return copy_json(cached)
        record = await self.session.storage.task(task_id)
        if record is not None and record["state"]["status"] == "terminal":
            return record
        return record

    async def owned_tasks(self, task_id: int) -> list[dict[str, Any]]:
        records = await self._scan_all_tasks()
        return [record for record in records if record.get("owner") == task_id and record["state"]["status"] != "terminal"]

    def has_live_owned_work(self, task_id: int) -> bool:
        for record in self._live.values():
            if record.get("owner") == task_id:
                return True
        owned_conversations = {
            conversation_id
            for conversation_id, owner in self._conversation_owners.items()
            if owner == task_id
        }
        for record in self._live.values():
            if record["conversationId"] in owned_conversations and not record.get("background"):
                return True
        return False

    async def retry_task(self, task_id: int) -> None:
        record = await self.get_task(task_id)
        if record is None:
            raise ValueError(f"Task {task_id} does not exist")
        if record["state"]["status"] == "terminal":
            return
        self._parked_reported.discard(task_id)

        self._blocked.pop(task_id, None)

        def change(current: dict[str, Any]) -> dict[str, Any]:
            updated = copy_json(current)
            updated["restarts"] = 0
            return updated

        await self._update_task(task_id, change)
        self._wake.set()

    # ── Abort ──────────────────────────────────────────────────────────────

    async def abort_task(self, task_id: int) -> str:
        record = await self.get_task(task_id)
        if record is None:
            raise ValueError(f"Task {task_id} does not exist")
        if record["state"]["status"] == "terminal":
            return "terminal"
        await self._mark(task_id, set(), reason=None)
        invocation = self._invocations.get(task_id)
        if invocation is not None:
            try:
                await asyncio.shield(invocation)
            except (asyncio.CancelledError, Exception):  # noqa: BLE001, S110 - abort joins the invocation
                pass
        # A blocked task with no live ordinary work can settle at once.
        record = await self.get_task(task_id)
        if record is not None and record["state"]["status"] in ("pending", "waiting", "sleeping"):
            definition = self._definition(record)
            if definition is None and not self.has_live_owned_work(task_id):
                await self._finish(
                    task_id,
                    {"status": "orphaned", "reason": "missing_task"},
                    report={"type": "task_outdated", "reason": "missing_task"},
                )
        self._wake.set()
        return "marked"

    async def _mark(self, task_id: int, visited: set[int], *, reason: str | None = None) -> None:
        if task_id in visited:
            return
        visited.add(task_id)
        record = await self.get_task(task_id)
        if record is None or record["state"]["status"] == "terminal":
            return
        if not record.get("abortRequested") or (
            reason is None and record.get("abortReason") is not None
        ):

            def mark(current: dict[str, Any], reason: str | None = reason) -> dict[str, Any]:
                updated = copy_json(current)
                was_marked = bool(current.get("abortRequested"))
                updated["abortRequested"] = True
                if reason is None:
                    updated.pop("abortReason", None)
                elif not was_marked:
                    updated["abortReason"] = "restart"
                return updated

            await self._update_task(task_id, mark)
        runtime = self._runtimes.get(task_id)
        if runtime is not None:
            runtime.signal.set()
        if reason is None:
            cascade_reason = None
        elif record.get("abortReason") == "restart" or not record.get("abortRequested"):
            cascade_reason = "restart"
        else:
            cascade_reason = record.get("abortReason")
        # Cascade to direct children and the conversations this task owns.
        for child in list(self._live.values()):
            if child.get("owner") == task_id and not child.get("background"):
                await self._mark(child["id"], visited, reason=cascade_reason)
        for conversation in await self._conversations_owned_by(task_id):
            for task in await self._tasks_in_conversation(conversation):
                if not task.get("background"):
                    await self._mark(task["id"], visited, reason=cascade_reason)

    async def _mark_owned(self, task_id: int, *, reason: str | None = None) -> None:
        """Mark the ordinary owned work of a task, without marking the task itself."""
        visited: set[int] = set()
        for child in list(self._live.values()):
            if child.get("owner") == task_id and not child.get("background"):
                await self._mark(child["id"], visited, reason=reason)
        for conversation in await self._conversations_owned_by(task_id):
            for task in await self._tasks_in_conversation(conversation):
                if not task.get("background"):
                    await self._mark(task["id"], visited, reason=reason)

    async def abort_scope(self, conversation_id: int, *, background: bool = False) -> None:
        visited: set[int] = set()
        for task in await self._tasks_in_conversation(conversation_id):
            if background or not task.get("background"):
                await self._mark(task["id"], visited, reason=None)
        self._wake.set()

    async def _conversations_owned_by(self, task_id: int) -> list[int]:
        ids: list[int] = []
        cursor = None
        while True:
            page = await self.session.storage.scan_conversations({"ownerTaskId": task_id}, SCAN_PAGE_SIZE, cursor)
            ids.extend(record["id"] for record in page["items"])
            cursor = page.get("next")
            if cursor is None:
                return ids

    async def _tasks_in_conversation(self, conversation_id: int) -> list[dict[str, Any]]:
        return [record for record in self._live.values() if record["conversationId"] == conversation_id]

    # ── Idle ───────────────────────────────────────────────────────────────

    async def wait_for_idle(self, conversation_id: int | None = None) -> None:
        while True:
            if not self._live_scope(conversation_id):
                return
            self._wake.clear()
            if not self._live_scope(conversation_id):
                return
            try:
                await asyncio.wait_for(self._wake.wait(), 0.2)
            except TimeoutError:
                pass

    def _live_scope(self, conversation_id: int | None) -> list[dict[str, Any]]:
        if conversation_id is None:
            candidates = [
                record
                for record in self._live.values()
                if not record.get("background") and not self._is_blocked(record)
            ]
            return candidates
        selected: dict[int, dict[str, Any]] = {}
        frontier = [conversation_id]
        seen_conversations: set[int] = set()
        while frontier:
            current = frontier.pop()
            if current in seen_conversations:
                continue
            seen_conversations.add(current)
            for record in self._live.values():
                if record["conversationId"] != current:
                    continue
                if record.get("background"):
                    continue
                if self._is_blocked(record):
                    continue
                selected[record["id"]] = record
                for child in self._live.values():
                    if child.get("owner") == record["id"]:
                        selected[child["id"]] = child
                frontier.extend(self._owned_conversation_ids_for(record["id"]))
        return list(selected.values())

    def _owned_conversation_ids_for(self, task_id: int) -> list[int]:
        # Conversation ownership is not part of the task record; the scheduler keeps this
        # mapping from committed conversation publications.
        return [cid for cid, owner in self._conversation_owners.items() if owner == task_id]

    # ── Wake ───────────────────────────────────────────────────────────────

    def notify(self) -> None:
        """Wake the scheduling loop; used by admission, abort, and retry paths."""
        self._wake.set()

    def wake(self) -> dict[str, Any]:
        active = False
        at: int | None = None
        for record in self._live.values():
            state = record["state"]
            if self._is_blocked(record):
                continue
            if state["status"] in ("pending", "running", "waiting", "completing"):
                active = True
            elif state["status"] == "sleeping":
                deadline = int(state["until"])
                at = deadline if at is None else min(at, deadline)
                if deadline <= self.now():
                    active = True
        return {"active": active, "at": at}

    # ── Loop ───────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while not self._closing:
            self._wake.clear()
            try:
                await self._pass()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - report and keep scheduling
                from .errors import SessionFailed

                if isinstance(error, SessionFailed):
                    return
                self.report(error, {"type": "task_error"})
                try:
                    await asyncio.wait_for(self._wake.wait(), 0.05)
                except TimeoutError:
                    pass
            if self._closing:
                return
            if self._work_ready():
                continue
            timeout = self._next_timeout()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout)
            except TimeoutError:
                pass

    async def _pass(self) -> None:
        await self._resolve_derived_states()
        await self._spawn_ready()

    async def _resolve_derived_states(self) -> None:
        for record in list(self._live.values()):
            state = record["state"]
            status = state["status"]
            if record.get("abortRequested") and status in ("waiting", "sleeping"):
                # An abort mark waits neither for time nor for dependencies.
                await self._set_state(
                    record["id"], {"status": "pending", "checkpoint": state["checkpoint"]}
                )
                continue
            if status == "waiting":
                deps = [await self.get_task(dep_id) for dep_id in state.get("on", [])]
                if all(dep is None or dep["state"]["status"] == "terminal" for dep in deps):
                    policy = state.get("policy", "allSettled")
                    failed = next(
                        (
                            dep
                            for dep in deps
                            if dep is not None and dep["state"]["outcome"]["status"] != "completed"
                        ),
                        None,
                    )
                    if policy == "failFast" and failed is not None:
                        await self._finish(
                            record["id"],
                            {
                                "status": "failed",
                                "error": {
                                    "message": f"Dependency task {failed['id']} ended {failed['state']['outcome']['status']}"
                                },
                            },
                        )
                    else:
                        await self._set_state(record["id"], {"status": "pending", "checkpoint": state["checkpoint"]})
            elif status == "sleeping" and int(state["until"]) <= self.now():
                await self._set_state(record["id"], {"status": "pending", "checkpoint": state["checkpoint"]})
            elif status == "completing":
                if state["outcome"].get("status") != "completed" and self.has_live_owned_work(record["id"]):
                    # A held non-completed outcome is cancellation intent for the work below.
                    await self._mark_owned(record["id"])
                await self._finalize_completing(record["id"])

    async def _spawn_ready(self) -> None:
        for record in list(self._live.values()):
            task_id = record["id"]
            if (
                record["state"]["status"] == "running"
                and task_id not in self._invocations
                and task_id not in self._reserved
            ):
                # An invocation ended without leaving a runnable state (for example a
                # scheduler commit was rejected): resume from the last checkpoint.
                await self._set_state_pending(task_id)
                record = self._live.get(task_id, record)
            if record["state"]["status"] != "pending":
                continue
            task_id = record["id"]
            if task_id in self._reserved or task_id in self._invocations:
                continue
            if record.get("abandonOnRestart") and not record.get("abortRequested"):
                # Opening alone marks nothing; the first reservation pass abandons the task.
                await self._mark(task_id, set(), reason="restart")
                continue
            owner_cancelled, owner_reason = self._owner_cancellation(record)
            if not record.get("abortRequested") and owner_cancelled:
                await self._mark(task_id, set(), reason=owner_reason)
                continue
            if self._parked(record):
                self._report_parked(record)
                continue
            definition = self._definition(record)
            if (
                record.get("abortRequested")
                and record.get("abortReason") != "restart"
                and definition is not None
                and int(record.get("restarts") or 0) >= self.restart_limit
            ):
                await self._finish(
                    task_id,
                    {
                        "status": "faulted",
                        "error": {
                            "message": (
                                f"Abort handler of task {task_id} was cut off "
                                f"{record.get('restarts')} times"
                            )
                        },
                    },
                    report={"type": "task_faulted", "reason": "restart_limit"},
                )
                continue
            if definition is None:
                if (
                    record.get("abortRequested")
                    and record.get("abortReason") != "restart"
                    and not self.has_live_owned_work(task_id)
                ):
                    await self._finish(
                        task_id,
                        {"status": "orphaned", "reason": "missing_task"},
                        report={"type": "task_outdated", "reason": "missing_task"},
                    )
                continue
            stored_version = int(record["version"])
            if stored_version > definition.version:
                if (
                    record.get("abortRequested")
                    and record.get("abortReason") != "restart"
                    and not self.has_live_owned_work(task_id)
                ):
                    await self._finish(
                        task_id,
                        {"status": "orphaned", "reason": "incompatible_task"},
                        report={"type": "task_outdated", "reason": "incompatible_task"},
                    )
                continue
            if stored_version < definition.version:
                if definition.migrate is None:
                    continue
                try:
                    migrated = definition.migrate(
                        copy_json(record["input"]), copy_json(record["state"]["checkpoint"]), stored_version
                    )
                    migrated_input = copy_json(migrated["input"])
                    migrated_checkpoint = copy_json(migrated["checkpoint"])
                except Exception as error:  # noqa: BLE001 - migration failure blocks the task
                    self.report(
                        error,
                        {"type": "migration_failed", "task": self.task_info(record)},
                    )
                    self._blocked[task_id] = Blocked("migration_failed", error)
                    continue

                def migrate_change(
                    current: dict[str, Any],
                    *,
                    version: int = definition.version,
                    new_input: Any = migrated_input,
                    checkpoint: Any = migrated_checkpoint,
                ) -> dict[str, Any]:
                    updated = copy_json(current)
                    updated["version"] = version
                    updated["input"] = new_input
                    updated["state"] = {"status": "pending", "checkpoint": checkpoint}
                    return updated

                await self._update_task(task_id, migrate_change)
                continue
            self._reserved.add(task_id)
            try:
                reserved = await self._reserve(task_id)
            except Exception:  # noqa: BLE001 - card may have settled concurrently
                self._reserved.discard(task_id)
                continue
            if not reserved:
                self._reserved.discard(task_id)
                continue
            mode = "abort" if reserved.get("abortRequested") else "run"
            self._invocations[task_id] = asyncio.ensure_future(self._invoke(task_id, mode))

    async def _reserve(self, task_id: int) -> dict[str, Any] | None:
        holder: dict[str, Any] = {}

        async def run(tx: Any) -> None:
            current = await tx._current_task(task_id)
            if current is None or current["state"]["status"] != "pending":
                return
            record = copy_json(current)
            record["state"] = {**record["state"], "status": "running"}
            await tx.set_task(record)
            holder["record"] = record

        await self.session.commit_with(run)
        return holder.get("record")

    async def _invoke(self, task_id: int, mode: str) -> None:
        record = self._live.get(task_id)
        if record is None:
            self._reserved.discard(task_id)
            self._invocations.pop(task_id, None)
            return
        definition = self._definition(record)
        signal = asyncio.Event()
        runtime = TaskRuntime(self, record, mode=mode, signal=signal)
        self._runtimes[task_id] = runtime
        try:
            if definition is None:
                raise TaskFaulted(f"Task definition {record['kind']} is not installed")
            if mode == "abort":
                result = definition.abort(record, runtime)
                if inspect.isawaitable(result):
                    await result
            else:
                checkpoint = record["state"].get("checkpoint") or {}
                phase = checkpoint.get("phase")
                handler = definition.phases.get(str(phase))
                if handler is None:
                    raise TaskFaulted(f"Task {task_id} has no phase {phase!r}")
                result = handler(record, runtime)
                if inspect.isawaitable(result):
                    await result
            if mode == "run" and not runtime.progressed:
                raise TaskFaulted(
                    f"Task {task_id} phase {record['state'].get('checkpoint', {}).get('phase')!r} returned without durable progress"
                )
            if mode == "abort":
                latest = runtime.last_record or await self.get_task(task_id)
                if latest is None or latest["state"]["status"] not in ("terminal", "completing"):
                    raise TaskFaulted(
                        f"Abort handler of task {task_id} returned without a terminal outcome"
                    )
        except TaskAborted:
            pass
        except asyncio.CancelledError:
            if self._closing:
                await self._set_state_pending(task_id)
                raise
            raise
        except InvocationEnded:
            pass
        except BaseException as error:  # noqa: BLE001 - phase failure becomes a durable outcome
            await self._finish(
                task_id,
                {"status": "faulted", "error": {"message": error_message(error)}},
                report={"type": "task_faulted", "reason": "threw"},
            )
        finally:
            runtime.end()
            self._runtimes.pop(task_id, None)
            self._reserved.discard(task_id)
            self._invocations.pop(task_id, None)
            if not self._closing:
                try:
                    await self._after_invocation(task_id)
                except Exception as error:  # noqa: BLE001 - cleanup must not escape the invocation
                    self.report(error, {"type": "task_error"})
            self._wake.set()

    async def _after_invocation(self, task_id: int) -> None:
        record = await self.get_task(task_id)
        if record is None:
            return
        state = record["state"]
        if state["status"] == "running":
            # A phase committed another running checkpoint: run the next phase; or the
            # invocation was interrupted before committing, and the abort handler runs.
            await self._set_state_pending(task_id)

    async def _set_state_pending(self, task_id: int) -> None:
        def change(current: dict[str, Any]) -> dict[str, Any] | None:
            if current["state"]["status"] != "running":
                return None
            return {**current, "state": {"status": "pending", "checkpoint": current["state"].get("checkpoint")}}

        await self._update_task(task_id, change)

    async def _set_state(self, task_id: int, state: Mapping[str, Any]) -> None:
        def change(current: dict[str, Any]) -> dict[str, Any] | None:
            if current["state"]["status"] == "terminal":
                return None
            return {**current, "state": copy_json(state)}

        await self._update_task(task_id, change)

    async def _finish(
        self,
        task_id: int,
        outcome: Mapping[str, Any],
        *,
        report: Mapping[str, Any] | None = None,
    ) -> None:
        """Write a scheduler-decided outcome, holding it while ordinary owned work is live."""

        async def run(tx: Any) -> None:
            current = await tx._current_task(task_id)
            if current is None or current["state"]["status"] in ("terminal", "completing"):
                return
            updated = copy_json(current)
            if self.has_live_owned_work(task_id):
                updated["state"] = {"status": "completing", "outcome": copy_json(outcome)}
            else:
                updated["state"] = {"status": "terminal", "outcome": copy_json(outcome)}
                if outcome.get("status") in ("faulted", "orphaned") and self._settle_outcome is not None:
                    await self._settle_outcome(tx, current, outcome)
            await tx.set_task(updated)

        await self.session.commit_with(run, TransactionScope(task_id=task_id))
        if outcome.get("status") != "completed" and self.has_live_owned_work(task_id):
            await self._mark_owned(task_id)
        if report is not None:
            record = await self.get_task(task_id)
            if record is not None:
                info = {**report, "task": self.task_info(record)}
                self.report(RuntimeError(str(outcome.get("error", outcome))), info)

    async def _finalize_completing(self, task_id: int) -> None:
        """Turn a held outcome terminal once no ordinary owned work is live."""
        if self.has_live_owned_work(task_id):
            return

        async def run(tx: Any) -> None:
            current = await tx._current_task(task_id)
            if current is None or current["state"]["status"] != "completing":
                return
            outcome = current["state"]["outcome"]
            updated = copy_json(current)
            updated["state"] = {"status": "terminal", "outcome": copy_json(outcome)}
            if outcome.get("status") in ("faulted", "orphaned") and self._settle_outcome is not None:
                await self._settle_outcome(tx, current, outcome)
            await tx.set_task(updated)

        await self.session.commit_with(run, TransactionScope(task_id=task_id))

    def _owner_cancellation(self, record: Mapping[str, Any]) -> tuple[bool, str | None]:
        """Whether an owner above this task carries cancellation intent, and why."""
        owner_id = record.get("owner")
        visited: set[int] = set()
        while owner_id is not None and owner_id not in visited:
            visited.add(owner_id)
            owner = self._live.get(owner_id)
            if owner is None:
                return False, None
            if owner.get("abortRequested"):
                reason = "restart" if owner.get("abortReason") == "restart" else None
                return True, reason
            state = owner["state"]
            if state["status"] == "completing" and state["outcome"].get("status") != "completed":
                return True, None
            owner_id = owner.get("owner")
        return False, None

    async def _update_task(
        self, task_id: int, change: Callable[[dict[str, Any]], dict[str, Any] | None]
    ) -> None:
        async def run(tx: Any) -> None:
            current = await tx._current_task(task_id)
            if current is None:
                return
            updated = change(current)
            if updated is None:
                return
            await tx.set_task(updated)

        try:
            await self.session.commit_with(run, TransactionScope(task_id=task_id))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            from .errors import SessionFailed

            if isinstance(error, SessionFailed):
                raise
            self.report(error, {"type": "task_error"})

    # ── Commit listener ────────────────────────────────────────────────────

    def _on_commit(self, publication: Mapping[str, Any]) -> None:
        for change in publication["changes"]:
            kind = change["type"]
            if kind == "task":
                value = change["value"]
                if value["state"]["status"] == "terminal":
                    self._live.pop(value["id"], None)
                    self._notify_terminal(value)
                else:
                    self._live[value["id"]] = copy_json(value)
                    if value["state"]["status"] != "pending":
                        self._blocked.pop(value["id"], None)
            elif kind == "conversation":
                owner = change["value"].get("owner")
                if owner is not None:
                    self._conversation_owners[change["value"]["id"]] = owner["taskId"]
        self._wake.set()

    def _notify_terminal(self, record: Mapping[str, Any]) -> None:
        for waiter in list(self._waiters.get(record["id"], [])):
            if not waiter.done():
                waiter.set_result(copy_json(record))

    # ── Inspection ─────────────────────────────────────────────────────────

    async def inspect_tasks(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for record in sorted(self._live.values(), key=lambda item: item["id"]):
            state = record["state"]
            status = state["status"]
            if record["id"] in self._invocations:
                view: dict[str, Any] = {"kind": "running"}
            elif status == "completing":
                view = {"kind": "completing"}
            elif status == "sleeping":
                view = {"kind": "sleeping", "until": int(state["until"])}
            elif status == "waiting":
                live = []
                for dep_id in state.get("on", []):
                    dep = await self.get_task(dep_id)
                    if dep is not None and dep["state"]["status"] != "terminal":
                        live.append(dep_id)
                view = {"kind": "waiting", "on": live}
            else:
                blocked = self._blocked_reason(record)
                if blocked is not None:
                    view = {"kind": "blocked", "reason": blocked.reason}
                    if blocked.error is not None:
                        view["error"] = str(blocked.error)
                else:
                    definition = self._definition(record)
                    view = {
                        "kind": "ready",
                        "migrates": definition is not None and definition.version > int(record["version"]),
                    }
            results.append({"record": copy_json(record), "state": view})
        return results

    # ── Derived states ─────────────────────────────────────────────────────

    def _definition(self, record: Mapping[str, Any]) -> TaskDefinition | None:
        snapshot = self.registry.snapshot() if hasattr(self.registry, "snapshot") else self.registry
        tasks = snapshot.tasks if hasattr(snapshot, "tasks") else snapshot
        task = tasks.get(record["kind"]) if isinstance(tasks, Mapping) else None
        if task is None:
            return None
        return task.definition if hasattr(task, "definition") else task

    def _parked(self, record: Mapping[str, Any]) -> bool:
        if record.get("abortRequested"):
            return False
        return int(record.get("restarts") or 0) >= self.restart_limit

    def _report_parked(self, record: Mapping[str, Any]) -> None:
        if record["id"] in self._parked_reported:
            return
        self._parked_reported.add(record["id"])
        self.report(
            RuntimeError(f"Task {record['id']} is parked after {record.get('restarts')} restarts"),
            {"type": "task_parked", "task": self.task_info(record)},
        )

    def _is_blocked(self, record: Mapping[str, Any]) -> bool:
        return self._blocked_reason(record) is not None

    def _blocked_reason(self, record: Mapping[str, Any]) -> Blocked | None:
        if record["state"]["status"] == "terminal":
            return None
        if self._parked(record):
            return Blocked("restart_limit")
        cached = self._blocked.get(record["id"])
        if cached is not None:
            return cached
        definition = self._definition(record)
        if definition is None:
            return Blocked("missing_task")
        if int(record["version"]) > definition.version:
            return Blocked("task_too_old")
        if int(record["version"]) < definition.version and definition.migrate is None:
            return Blocked("migration_failed")
        return None

    def _work_ready(self) -> bool:
        for record in self._live.values():
            if record["state"]["status"] == "pending" and record["id"] not in self._reserved:
                if self._parked(record):
                    continue
                definition = self._definition(record)
                if definition is None:
                    continue
                if int(record["version"]) > definition.version:
                    continue
                if int(record["version"]) < definition.version and definition.migrate is None:
                    continue
                return True
            if record["state"]["status"] == "sleeping" and int(record["state"]["until"]) <= self.now():
                return True
        return False

    def _next_timeout(self) -> float | None:
        earliest: int | None = None
        for record in self._live.values():
            if record["state"]["status"] == "sleeping":
                until = int(record["state"]["until"])
                earliest = until if earliest is None else min(earliest, until)
        if earliest is None:
            return None
        return max(0.0, (earliest - self.now()) / 1000)

__all__ = ["Blocked", "SchedulerSettings", "TaskScheduler"]

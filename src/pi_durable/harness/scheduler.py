"""Durable task scheduler: reservation, phases, abort, ownership, and finalization.

Port of v1.3.0 ``src/harness/scheduler.ts``. Every task transition is decided and written by one
callback serialized on the Session line: reservation, abort marks, runtime commits, finalization,
and the synchronous step before each phase. Handlers and joins run off the line.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Coroutine, Iterator, Mapping, Sequence
from typing import Any, Literal, cast

from ..context import (
    AbortController,
    AbortSignal,
    Context,
    await_with_context,
    with_abort_signal,
)
from ..errors import error_message, is_failure
from ..ids import ConversationId, TaskId
from ..json import JsonValue, copy_json
from ..session.session import SessionImpl
from ..session.transaction import Transaction
from ..types import (
    CommitPublication,
    NextTaskState,
    RunningTask,
    Storage,
    TaskOutcome,
    TaskRecord,
)
from .agent import agent_hooks
from .context import read_context_from
from .types import (
    Agent,
    AnyTask,
    ConversationHandle,
    RegistryReader,
    RegistrySnapshot,
    Settings,
    SettledTask,
    TaskInspection,
)
from .util import Waiters, closed_error, rejected, scan_all

_SCAN_PAGE_SIZE = 256
MAX_TIMER_DELAY_MS = 2_147_483_647
LIVE_STATUSES = ("pending", "running", "waiting", "completing")

BlockedReason = Literal["missing_task", "task_too_old", "migration_failed"]

AnyTaskRecord = TaskRecord
RunnableTaskRecord = TaskRecord
Checkpoint = dict[str, Any]
ErasedDefinition = Any
ErasedRuntime = Any
ErasedRunningTask = RunningTask

Node = int | str
Up = dict[str, Any]
Scope = dict[str, Any]


def conversation_node(conversation_id: ConversationId) -> str:
    return f"c{conversation_id}"


def conversation_of(node: str) -> ConversationId:
    return cast(ConversationId, int(node[1:]))


def parent_node(node: Mapping[str, Any]) -> Node:
    owner = node.get("owner")
    return owner if owner is not None else conversation_node(node["conversationId"])


def node_of(record: Mapping[str, Any]) -> dict[str, Any]:
    node = {"conversationId": record["conversationId"], "background": record["background"]}
    if record.get("owner") is not None:
        node["owner"] = record["owner"]
    return node


def parent_of(node: Mapping[str, Any]) -> Up:
    owner = node.get("owner")
    return {"task": owner} if owner is not None else {"conversation": node["conversationId"]}


def with_abort_mark(record: Mapping[str, Any], reason: str | None = None) -> AnyTaskRecord:
    result = dict(record)
    result.pop("abortReason", None)
    result["abortRequested"] = True
    if reason is not None:
        result["abortReason"] = reason
    return cast(AnyTaskRecord, result)


def failed_outcome(record: Mapping[str, Any]) -> bool:
    state = record["state"]
    return state["status"] in ("completing", "terminal") and state["outcome"]["status"] != "completed"


def cancellation_intent(record: Mapping[str, Any]) -> bool:
    return record["state"]["status"] != "terminal" and (
        record.get("abortRequested") is True or failed_outcome(record)
    )


def with_state(record: Mapping[str, Any], state: Mapping[str, Any]) -> AnyTaskRecord:
    result = dict(record)
    if state["status"] in ("terminal", "completing"):
        result.pop("memos", None)
    result["state"] = copy_json(state)
    return cast(AnyTaskRecord, result)


def can_reserve(task: AnyTask, record: Mapping[str, Any]) -> bool:
    definition = task.definition
    return definition.version == record["version"] or (
        definition.version > record["version"] and definition.migrate is not None
    )


_MISSING_MEMO = object()


def memo_of(record: Mapping[str, Any] | None, name: str) -> Any:
    memos = (record or {}).get("memos")
    if not isinstance(memos, Mapping) or name not in memos:
        return _MISSING_MEMO
    return memos[name]


def json_equal(left: Any, right: Any) -> bool:
    if left is right:
        return True
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, list) or isinstance(right, list):
        return (
            isinstance(left, list)
            and isinstance(right, list)
            and len(left) == len(right)
            and all(json_equal(a, b) for a, b in zip(left, right, strict=True))
        )
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        return type(left) is type(right) and left == right
    return left.keys() == right.keys() and all(json_equal(left[key], right[key]) for key in left)


def _add_to(mapping: dict[Any, set[Any]], key: Any, value: Any) -> None:
    mapping.setdefault(key, set()).add(value)


def _delete_from(mapping: dict[Any, set[Any]], key: Any, value: Any) -> None:
    values = mapping.get(key)
    if values is None:
        return
    values.discard(value)
    if not values:
        del mapping[key]


class _Invocation:
    """One in-memory execution of a task in run or abort mode."""

    __slots__ = (
        "task_id",
        "conversation_id",
        "mode",
        "controller",
        "context",
        "watches",
        "commits",
        "ended",
        "done",
        "finish",
    )

    def __init__(self, record: Mapping[str, Any], mode: Literal["run", "abort"], context: Context) -> None:
        self.task_id = cast(TaskId, record["id"])
        self.conversation_id = cast(ConversationId, record["conversationId"])
        self.mode = mode
        self.controller = AbortController()
        self.context = with_abort_signal(self.controller.signal, context)
        self.watches: set[Any] = set()
        self.commits: set[asyncio.Future[Any]] = set()
        self.ended = False
        self.done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.done.add_done_callback(lambda future: future.exception() if not future.cancelled() else None)
        self.finish = self._finish

    def _finish(self) -> None:
        if not self.done.done():
            self.done.set_result(None)


def _ended_error(invocation: _Invocation) -> Exception:
    return RuntimeError(f"Task {invocation.task_id} invocation has ended")


def _missing_migration(record: Mapping[str, Any], definition: Any) -> Exception:
    return RuntimeError(
        f"Task {record['kind']} version {definition.version} has no migration from {record['version']}"
    )


class _OldDefinitionError(RuntimeError):
    """A task that keeps running under its old definition; ``cause`` names why it could not hand over."""

    def __init__(self, task_id: TaskId, kind: str, cause: str) -> None:
        super().__init__(f"Task {task_id} keeps running under its old {kind} definition")
        self.cause = cause


def _overlay_of(tx: Transaction) -> dict[str, Any]:
    tasks = {record["id"]: record for record in tx.staged_tasks()}
    edges = {
        record["id"]: (record["owner"]["taskId"] if "owner" in record else None)
        for record in tx.staged_conversations()
    }
    return {"tasks": tasks, "edges": edges}


def _delay(ms: float, signal: AbortSignal) -> Awaitable[None]:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[None] = loop.create_future()

    def cancel() -> None:
        if not future.done():
            future.set_exception(_cancellation_error(signal))

    def done() -> None:
        if not future.done():
            future.set_result(None)

    handle = loop.call_later(max(0.0, ms) / 1000.0, done)
    remove = signal.add_listener(cancel)
    if signal.aborted:
        cancel()

    def cleanup(_: Any) -> None:
        handle.cancel()
        remove()

    future.add_done_callback(cleanup)
    future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
    return future


def _cancellation_error(signal: AbortSignal) -> BaseException:
    from .util import cancellation_error

    return cancellation_error(signal)


class TaskScheduler:
    """Durable task scheduler of one Harness."""

    def __init__(
        self,
        *,
        session: SessionImpl,
        storage: Storage,
        registry: RegistryReader,
        models: Any,
        agent: Callable[[ConversationId, RegistrySnapshot, Context], Awaitable[Agent]],
        settings: Callable[[], Settings],
        env: Callable[[ConversationId, Context], Awaitable[Any]],
        now: Callable[[], int],
        report: Callable[[BaseException], None],
        settle_outcome: Callable[[Transaction, AnyTaskRecord, Mapping[str, Any]], Awaitable[None]],
        withdraw_inputs: Callable[[Transaction, ConversationId], Awaitable[None]],
        conversation: Callable[[ConversationId, Any, Context], Awaitable[ConversationHandle | None]],
        context: Context,
    ) -> None:
        self._session = session
        self._storage = storage
        self._registry = registry
        self._models = models
        self._agent = agent
        self._settings = settings
        self._env = env
        self._now = now
        self._report = report
        self._settle_outcome = settle_outcome
        self._withdraw_inputs = withdraw_inputs
        self._conversation = conversation
        self._context = context
        self._live: dict[TaskId, AnyTaskRecord] = {}
        self._contexts: dict[ConversationId, dict[str, Any]] = {}
        self._expiry: dict[str, Any] | None = None
        self._invocations: dict[TaskId, _Invocation] = {}
        self._task_waiters: Waiters[TaskId, SettledTask] = Waiters()
        self._idle_waiters: Waiters[ConversationId | None, None] = Waiters()
        self._failed_migrations: dict[TaskId, dict[str, Any]] = {}
        self._edges: dict[ConversationId, TaskId | None] = {}
        self._settled: dict[TaskId, dict[str, Any]] = {}
        self._drop_queue: set[Node] = set()
        self._below: dict[Node, set[Node]] = {}
        self._roots: set[ConversationId] = set()
        self._unloaded: set[TaskId] = set()
        self._intent: set[TaskId] = set()
        self._runnable: set[TaskId] = set()
        self._finalize_checks: set[TaskId] = set()
        self._waiters: dict[TaskId, set[TaskId]] = {}
        self._fail_fast_checks: set[TaskId] = set()
        self._abandoned: set[TaskId] = set()
        self._reconcile_scheduled = False
        self._sweep_scheduled = False
        self._cascade_pending = False
        self._unsubscribe_registry: Callable[[], None] = lambda: None
        self._enabled = False
        self._closing = False
        self._dirty = False
        self._draining = False
        self._expiry_handle: asyncio.TimerHandle | None = None
        self._detached: set[asyncio.Task[Any]] = set()

    # ── Lifecycle ──────────────────────────────────────────────────────────

    async def open(self, context: Context) -> None:
        """Load live tasks and change surviving ``running`` tasks back to ``pending``."""

        self._session.observe_commits(self._observe)
        self._session.subscribe_close(self._seal)
        self._unsubscribe_registry = self._registry.subscribe(self._kick)

        async def load(tx: Transaction) -> None:
            scans: list[list[AnyTaskRecord]] = []
            for status in LIVE_STATUSES:
                scans.append(
                    await scan_all(
                        lambda cursor, status=status: tx.scan_tasks(
                            {"status": status}, _SCAN_PAGE_SIZE, cursor
                        )
                    )
                )
            for records in scans:
                for record in records:
                    self._track(None, record)
                    if record.get("abandonOnRestart") is True:
                        self._abandoned.add(record["id"])
                    if record["state"]["status"] == "running":
                        tx.set_task(
                            with_state(
                                record,
                                {
                                    "status": "pending",
                                    "checkpoint": record["state"]["checkpoint"],
                                },
                            )
                        )
                    if record["state"]["status"] == "waiting" and record["state"]["policy"] == "failFast":
                        self._fail_fast_checks.add(record["id"])

        await self._session.commit_with(load, context)
        self._cascade_pending = True
        self._schedule_reconcile()

    def resume(self) -> None:
        """Enable scheduling. Idempotent; the kick does nothing once closing."""

        self._enabled = True
        self._kick()

    async def join(self) -> None:
        """Signal every invocation and wait for them; writes nothing."""

        for invocation in list(self._invocations.values()):
            invocation.controller.abort()
        await asyncio.gather(
            *[invocation.done for invocation in list(self._invocations.values())],
            return_exceptions=True,
        )

    async def abort(
        self, id: TaskId, context: Context, keep_restart: bool = False
    ) -> Literal["marked", "terminal"]:
        """Commit the abort mark, or settle a task no definition can take as ``orphaned``."""

        async def mark(tx: Transaction) -> dict[str, Any]:
            current = await tx.task(id)
            if current is None:
                raise ValueError(f"Task {id} does not exist")
            if current["state"]["status"] == "terminal":
                return {"result": "terminal"}
            invocation = self._invocations.get(id)
            if keep_restart and current.get("abortReason") == "restart":
                return {"result": "marked"}
            if invocation is None and current["state"]["status"] != "completing":
                await self._load_scopes(False)
                if not self._has_owned_live(id):
                    resolution = self._resolve(cast(RunnableTaskRecord, current), self._registry.snapshot())
                    if resolution["kind"] == "blocked":
                        await self._terminate(
                            tx,
                            current,
                            {"status": "orphaned", "reason": resolution["reason"]},
                        )
                        return {"result": "marked"}
            if not current.get("abortRequested") or current.get("abortReason") is not None:
                tx.set_task(with_abort_mark(current))
            return {
                "result": "marked",
                "run": invocation if invocation is not None and invocation.mode == "run" else None,
            }

        marked = await self._session.commit_with(mark, context)
        invocation = marked.get("run")
        if invocation is not None:
            await await_with_context(_race(invocation.done, self._session.failed), context)
        return cast(Literal["marked", "terminal"], marked["result"])

    async def wait_for_task(self, id: TaskId, context: Context) -> SettledTask:
        """Resolve with the terminal receipt; cancelling ``context`` cancels only this wait."""

        async def find() -> Any:
            if self._closing:
                raise closed_error(self._session)
            if id in self._live:
                return {"promise": self._task_waiters.add(id, context)}
            record = await self._storage.task(id, context)
            if record is None:
                raise ValueError(f"Task {id} does not exist")
            return {"promise": _resolved(cast(SettledTask, record))}

        found = await self._session.read_on_line(find)
        return cast(SettledTask, await found["promise"])

    def wait_for_idle(self, conversation_id: ConversationId | None, context: Context) -> Awaitable[None]:
        """Resolve when ordinary traversal reaches no live non-background task."""

        if self._closing:
            return rejected(closed_error(self._session))
        if self._idle(conversation_id):
            return _resolved(None)
        self._schedule_reconcile()
        return self._idle_waiters.add(conversation_id, context)

    async def abort_conversation(
        self, conversation_id: ConversationId, background: bool, context: Context
    ) -> None:
        """Withdraw queued inputs, mark the ordinary scope, and wait until it is idle."""

        async def mark(tx: Transaction) -> list[TaskId]:
            queued = await self._load_scopes(True)
            scope = {"conversation": conversation_id}
            reached: list[TaskId] = []
            for step in self._walk_down(conversation_node(conversation_id), cross_background=background):
                node = step["node"]
                live = step["live"]
                if live is None or (node["background"] and not background):
                    continue
                reached.append(live["id"])
                if not live.get("abortRequested") or live.get("abortReason") is not None:
                    tx.set_task(with_abort_mark(live))
            for id in queued:
                if self._in_scope({"conversation": id}, scope, background) is True:
                    await self._withdraw_inputs(tx, id)
            return reached

        reached = await self._session.commit_with(mark, context)
        if background:
            for id in reached:
                await self.wait_for_task(id, context)
        await self.wait_for_idle(conversation_id, context)

    # ── Scheduling ─────────────────────────────────────────────────────────

    def _observe(self, publication: CommitPublication, context: Context) -> None:
        updated: list[AnyTaskRecord] = []
        failed: list[TaskId] = []
        changed = False
        for change in publication["changes"]:
            if change["type"] == "conversation" and change["value"]["id"] not in self._edges:
                owner = change["value"].get("owner")
                self._set_edge(change["value"]["id"], owner["taskId"] if owner is not None else None)
        for change in publication["changes"]:
            if change["type"] != "task":
                continue
            changed = True
            record = change["value"]
            previous = self._live.get(record["id"])
            if failed_outcome(record) and (previous is None or not failed_outcome(previous)):
                failed.append(record["id"])
            self._track(previous, record)
            if record["state"]["status"] == "terminal":
                self._failed_migrations.pop(record["id"], None)
                self._fail_fast_checks.discard(record["id"])
                self._task_waiters.resolve(record["id"], cast(SettledTask, record))
                self._schedule_reconcile()
                continue
            if record.get("abortRequested") and not (previous or {}).get("abortRequested"):
                self._cascade_pending = True
                invocation = self._invocations.get(record["id"])
                if invocation is not None and invocation.mode == "run":
                    invocation.controller.abort()
            if (previous or {}).get("abortReason") is not None and record.get("abortReason") is None:
                self._cascade_pending = True
            status = record["state"]["status"]
            if status == "completing" and (previous or {}).get("state", {}).get("status") != "completing":
                if cancellation_intent(record):
                    self._cascade_pending = True
                self._schedule_reconcile()
            if (
                status == "waiting"
                and record["state"]["policy"] == "failFast"
                and (previous or {}).get("state", {}).get("status") != "waiting"
            ):
                self._fail_fast_checks.add(record["id"])
                self._schedule_reconcile()
            if previous is None:
                updated.append(record)
        for id in failed:
            for waiter in self._waiters.get(id, ()):
                state = (self._live.get(waiter) or {}).get("state")
                if state is not None and state["status"] == "waiting" and state["policy"] == "failFast":
                    self._fail_fast_checks.add(waiter)
                    self._schedule_reconcile()
        for change in publication["changes"]:
            if change["type"] != "submission":
                continue
            value = change["value"]
            if value["status"] != "queued" or value["type"] != "input":
                continue
            if not self._intent:
                continue
            conversation = value["conversationId"]
            if not self._known(conversation_node(conversation)) or self._below_cancelled(
                {"conversation": conversation}
            ):
                self._cascade_pending = True
        for record in updated:
            if record["id"] in self._unloaded:
                self._schedule_reconcile()
            elif (
                not record["background"]
                and not record.get("abortRequested")
                and self._below_cancelled(parent_of(record))
            ):
                self._cascade_pending = True
        if self._cascade_pending:
            self._schedule_reconcile()
        self._sweep()
        if not changed:
            return
        self._settle_idle()
        self._kick()

    def _context_retention_ms(self) -> int:
        try:
            return self._settings().context_retention_ms
        except Exception as error:
            self._report(error)
            return 0

    def _settle_idle(self) -> None:
        for conversation_id in self._idle_waiters.keys():
            if self._idle(conversation_id):
                self._idle_waiters.resolve(conversation_id, None)
        now = self._now()
        retention = self._context_retention_ms()
        for conversation_id, kept in list(self._contexts.items()):
            if kept["idle_since"] is not None and now - kept["idle_since"] >= retention:
                del self._contexts[conversation_id]
            elif not self._idle(conversation_id):
                kept["idle_since"] = None
            elif retention > 0:
                if kept["idle_since"] is None:
                    kept["idle_since"] = now
            else:
                del self._contexts[conversation_id]
        self._schedule_expiry()

    def _schedule_expiry(self) -> None:
        retention = self._context_retention_ms()
        at: int | None = None
        for kept in self._contexts.values():
            if kept["idle_since"] is not None:
                candidate = kept["idle_since"] + retention
                if at is None or candidate < at:
                    at = candidate
        if self._expiry is not None and self._expiry["at"] == at:
            return
        if self._expiry_handle is not None:
            self._expiry_handle.cancel()
            self._expiry_handle = None
        self._expiry = None
        if at is None or self._closing:
            return
        delay = min(max(0, at - self._now()), MAX_TIMER_DELAY_MS)
        loop = asyncio.get_running_loop()
        self._expiry_handle = loop.call_later(delay / 1000.0, self._expire)
        self._expiry = {"at": at}

    def _expire(self) -> None:
        self._expiry = None
        self._expiry_handle = None
        self._settle_idle()

    def _schedule_reconcile(self) -> None:
        if self._reconcile_scheduled or self._closing:
            return
        self._reconcile_scheduled = True
        asyncio.get_running_loop().call_soon(lambda: self._spawn(self._reconcile()))

    async def _reconcile(self) -> None:
        self._reconcile_scheduled = False
        cascade = self._cascade_pending
        self._cascade_pending = False
        checks = list(self._fail_fast_checks)
        self._fail_fast_checks.clear()

        async def apply(tx: Transaction) -> None:
            if self._closing:
                return
            queued = await self._load_scopes(cascade)
            marks: dict[TaskId, dict[str, Any]] = {}

            def mark(record: Mapping[str, Any], reason: str | None) -> None:
                staged = marks.get(record["id"])
                if staged is not None and staged["reason"] is None:
                    return
                if record.get("abortRequested") and (record.get("abortReason") is None or reason is not None):
                    return
                marks[record["id"]] = {"record": record, "reason": reason}

            for id in list(self._intent):
                owner = self._live.get(id)
                if owner is None:
                    continue
                restart = owner.get("abortReason") == "restart" and not failed_outcome(owner)
                for step in self._walk_down(id, enter=lambda below: below not in self._intent):
                    live = step["live"]
                    if live is not None and not step["node"]["background"]:
                        mark(live, "restart" if restart else None)
            for id in checks:
                waiter = self._live.get(id)
                if waiter is None or waiter["state"]["status"] != "waiting":
                    continue
                if not await self._any_failed(waiter["state"]["on"]):
                    continue
                for member in waiter["state"]["on"]:
                    record = self._live.get(member)
                    if record is not None and not failed_outcome(record):
                        mark(record, None)
            for mark_value in marks.values():
                tx.set_task(with_abort_mark(mark_value["record"], mark_value["reason"]))
            for id in queued:
                if self._below_cancelled({"conversation": id}):
                    await self._withdraw_inputs(tx, id)
            await self._finalize(tx)

        try:
            await self._session.commit_with(apply, self._context)
        except Exception as error:
            self._fail_session(error)
        self._settle_idle()

    async def _any_failed(self, ids: Sequence[TaskId]) -> bool:
        for id in ids:
            record = self._live.get(id)
            if record is None:
                record = await self._storage.task(id, self._context)
            if record is not None and failed_outcome(record):
                return True
        return False

    async def _finalize(self, tx: Transaction) -> None:
        round_ids = list(self._finalize_checks)
        self._finalize_checks.clear()
        done: set[TaskId] = set()
        overlay = _overlay_of(tx)
        while round_ids:
            following: set[TaskId] = set()
            for id in round_ids:
                record = overlay["tasks"].get(id) or self._live.get(id)
                if record is None or record["state"]["status"] != "completing" or id in done:
                    continue
                if self._has_owned_live(id, overlay, done):
                    continue
                outcome = record["state"]["outcome"]
                terminal = with_state(record, {"status": "terminal", "outcome": outcome})
                tx.set_task(terminal)
                overlay["tasks"][id] = terminal
                done.add(id)
                if outcome["status"] in ("faulted", "orphaned"):
                    await self._settle_outcome(tx, record, outcome)
                above = self._nearest_live_above(record, overlay)
                if above is not None and above["state"]["status"] == "completing":
                    following.add(above["id"])
            round_ids = list(following)

    async def _load_scopes(self, queued: bool) -> list[ConversationId]:
        for id in list(self._unloaded):
            if id in self._unloaded:
                await self._load_chain({"task": id})
        if not queued:
            return []
        submissions = await scan_all(
            lambda cursor: self._storage.scan_submissions(
                {"status": "queued"}, _SCAN_PAGE_SIZE, cursor, self._context
            )
        )
        inputs = [submission for submission in submissions if submission["type"] == "input"]
        conversations = list({submission["conversationId"] for submission in inputs})
        for id in conversations:
            await self._load_chain({"conversation": id})
        return conversations

    async def _load_chain(self, start: Up, overlay: dict[str, Any] | None = None) -> None:
        passed: list[TaskId] = []
        at: Up | None = start
        while at is not None:
            if "task" in at:
                id = at["task"]
                node = self._live.get(id) or self._settled.get(id)
                if node is None and overlay is not None and id in overlay["tasks"]:
                    break
                if node is not None and id not in self._unloaded:
                    break
                if node is None:
                    record = await self._storage.task(id, self._context)
                    if record is None:
                        return
                    node = node_of(record)
                    if record["state"]["status"] == "terminal":
                        self._settle(id, node)
                    self._schedule_reconcile()
                passed.append(id)
                at = parent_of(node)
            else:
                conversation = at["conversation"]
                if overlay is not None and conversation in overlay["edges"]:
                    edge = overlay["edges"][conversation]
                elif conversation in self._edges:
                    edge = self._edges[conversation]
                else:
                    record = await self._storage.conversation(conversation, self._context)
                    edge = record["owner"]["taskId"] if record is not None and "owner" in record else None
                    self._set_edge(conversation, edge)
                    self._schedule_reconcile()
                at = None if edge is None else {"task": edge}
        for id in passed:
            self._unloaded.discard(id)

    def _set_edge(self, conversation_id: ConversationId, owner: TaskId | None) -> None:
        self._edges[conversation_id] = owner
        self._relist(conversation_node(conversation_id))
        self._drop_if_unneeded(conversation_node(conversation_id))

    def _settle(self, id: TaskId, node: dict[str, Any]) -> None:
        self._settled[id] = node
        self._unloaded.add(id)
        self._relist(id)
        self._drop_if_unneeded(id)

    def _drop_if_unneeded(self, node: Node) -> None:
        if not isinstance(node, str) and node in self._live:
            return
        if node in self._below:
            return
        self._drop_queue.add(node)
        self._schedule_sweep()

    def _schedule_sweep(self) -> None:
        if self._sweep_scheduled or self._closing:
            return
        self._sweep_scheduled = True
        loop = asyncio.get_running_loop()

        async def sweep() -> None:
            self._sweep_scheduled = False
            self._sweep()

        async def queued() -> None:
            try:
                await self._session.read_on_line(sweep)
            except Exception:
                pass

        loop.call_soon(lambda: self._spawn(queued()))

    def _sweep(self) -> None:
        for node in list(self._drop_queue):
            if node in self._below:
                continue
            if isinstance(node, str):
                conversation = conversation_of(node)
                self._edges.pop(conversation, None)
                self._roots.discard(conversation)
            elif node not in self._live:
                self._settled.pop(node, None)
                self._unloaded.discard(node)
        self._drop_queue.clear()

    def _track(self, previous: AnyTaskRecord | None, record: AnyTaskRecord) -> None:
        id = record["id"]
        if previous is not None and previous["state"]["status"] == "waiting":
            for on in previous["state"]["on"]:
                _delete_from(self._waiters, on, id)
        if record["state"]["status"] == "terminal":
            node = node_of(record)
            self._live.pop(id, None)
            self._intent.discard(id)
            self._runnable.discard(id)
            self._finalize_checks.discard(id)
            self._settled[id] = node
            self._release(node)
            for waiter in self._waiters.get(id, ()):
                self._runnable.add(waiter)
            self._relist(id)
            self._drop_if_unneeded(id)
            return
        self._live[id] = record
        if previous is None:
            if not self._known(parent_node(record)):
                self._unloaded.add(id)
            self._relist(id)
        if cancellation_intent(record):
            self._intent.add(id)
        else:
            self._intent.discard(id)
        if record["state"]["status"] == "completing":
            self._runnable.discard(id)
            self._finalize_checks.add(id)
        else:
            self._runnable.add(id)
        if record["state"]["status"] == "waiting":
            for on in record["state"]["on"]:
                _add_to(self._waiters, on, id)

    def _release(self, node: dict[str, Any], overlay: dict[str, Any] | None = None) -> AnyTaskRecord | None:
        above = self._nearest_live_above(node, overlay)
        if above is not None and above["state"]["status"] == "completing":
            self._finalize_checks.add(above["id"])
        elif above is not None and above.get("abortRequested") is True:
            self._runnable.add(above["id"])
        return above

    def _relist(self, start: Node) -> None:
        node = start
        while True:
            needed = (not isinstance(node, str) and node in self._live) or node in self._below
            parent: Node | None
            if isinstance(node, str):
                conversation = conversation_of(node)
                # An owner edge not loaded yet: the chain load relists once it knows it.
                if conversation not in self._edges:
                    return
                parent = self._edges[conversation]
            else:
                known = self._live.get(node) or self._settled.get(node)
                if known is None:
                    return
                parent = parent_node(known)
            if parent is None:
                root = conversation_of(cast(str, node))
                if needed:
                    self._roots.add(root)
                else:
                    self._roots.discard(root)
                return
            current = self._below.get(parent)
            has = current is not None and node in current
            if has == needed:
                return
            if needed:
                first = parent not in self._below
                _add_to(self._below, parent, node)
                if not first:
                    return
            else:
                _delete_from(self._below, parent, node)
                if parent in self._below:
                    return
                self._drop_if_unneeded(parent)
            node = parent

    def _nearest_live_above(
        self, node: dict[str, Any], overlay: dict[str, Any] | None = None
    ) -> AnyTaskRecord | None:
        current = node
        while not current["background"]:
            owner = current.get("owner")
            if owner is None:
                owner = self._edge(current["conversationId"], overlay)
            if owner is None:
                return None
            record = (overlay or {}).get("tasks", {}).get(owner) or self._live.get(owner)
            if record is not None and record["state"]["status"] != "terminal":
                return record
            above = record or self._settled.get(owner)
            if above is None:
                return None
            current = above
        return None

    def _known(self, node: Node) -> bool:
        if not isinstance(node, str):
            return (node in self._live or node in self._settled) and node not in self._unloaded
        conversation = conversation_of(node)
        if conversation not in self._edges:
            return False
        owner = self._edges[conversation]
        return owner is None or self._known(owner)

    def _edge(self, id: ConversationId, overlay: dict[str, Any] | None = None) -> TaskId | None | Any:
        edges = (overlay or {}).get("edges")
        if edges is not None and id in edges:
            return edges[id]
        return self._edges.get(id)

    def _node(self, id: TaskId, overlay: dict[str, Any] | None = None) -> dict[str, Any] | None:
        return ((overlay or {}).get("tasks", {}).get(id)) or self._live.get(id) or self._settled.get(id)

    def _above(self, start: Up, overlay: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        at: Up | None = start
        while at is not None:
            if "task" in at:
                node = self._node(at["task"], overlay)
                if node is None:
                    yield {"unknown": True}
                    return
                yield {"task": at["task"], "node": node}
                at = parent_of(node)
            else:
                yield {"conversation": at["conversation"]}
                conversation = at["conversation"]
                if overlay is not None and conversation in overlay["edges"]:
                    edge = overlay["edges"][conversation]
                elif conversation in self._edges:
                    edge = self._edges[conversation]
                else:
                    yield {"unknown": True}
                    return
                at = None if edge is None else {"task": edge}

    def _links(self, overlay: dict[str, Any]) -> dict[Node, set[Node]]:
        if "links" in overlay:
            return cast(dict[Node, set[Node]], overlay["links"])
        links: dict[Node, set[Node]] = {}
        for record in overlay["tasks"].values():
            if record["id"] in self._live or record["id"] in self._settled:
                continue
            child: Node = record["id"]
            parent: Node | None = parent_node(record)
            while parent is not None and child not in links.get(parent, set()):
                _add_to(links, parent, child)
                if not isinstance(parent, str) and (parent in self._live or parent in overlay["tasks"]):
                    break
                above: Node | None
                if isinstance(parent, str):
                    above = self._edge(conversation_of(parent), overlay)
                else:
                    fields = self._settled.get(parent)
                    above = None if fields is None else parent_node(fields)
                if above is not None and parent in self._below.get(above, set()):
                    break
                child = parent
                parent = above
        overlay["links"] = links
        return links

    def _walk_down(
        self,
        start: Node,
        *,
        overlay: dict[str, Any] | None = None,
        cross_background: bool = False,
        enter: Callable[[TaskId], bool] | None = None,
    ) -> Iterator[dict[str, Any]]:
        links = None if overlay is None else self._links(overlay)
        pending: list[Node] = [start]
        while pending:
            at = pending.pop()
            children_sets = (self._below.get(at), None if links is None else links.get(at))
            for children in children_sets:
                for child in children or ():
                    if isinstance(child, str):
                        pending.append(child)
                        continue
                    record = (overlay or {}).get("tasks", {}).get(child) or self._live.get(child)
                    node = record or self._settled.get(child)
                    if node is None:
                        continue
                    live = record if record is not None and record["state"]["status"] != "terminal" else None
                    yield {"id": child, "node": node, "live": live}
                    if (cross_background or not node["background"]) and (enter is None or enter(child)):
                        pending.append(child)

    def _owned_live(
        self,
        owner: TaskId,
        *,
        overlay: dict[str, Any] | None = None,
        first: bool = False,
        done: set[TaskId] | None = None,
    ) -> list[TaskId]:
        found: list[TaskId] = []
        walk = self._walk_down(
            owner,
            overlay=overlay,
            enter=(lambda id: id not in done) if done is not None else None,
        )
        for step in walk:
            if step["live"] is None or step["node"]["background"]:
                continue
            found.append(step["id"])
            if first:
                break
        return found

    def _has_owned_live(
        self,
        owner: TaskId,
        overlay: dict[str, Any] | None = None,
        done: set[TaskId] | None = None,
    ) -> bool:
        return bool(self._owned_live(owner, overlay=overlay, first=True, done=done))

    def _in_scope(self, start: Up, scope: Scope, cross_background: bool = False) -> bool | None:
        for step in self._above(start):
            if "unknown" in step:
                return None
            if "conversation" in step:
                if "conversation" in scope and step["conversation"] == scope["conversation"]:
                    return True
            elif step["node"]["background"] and not cross_background:
                return False
        return "roots" in scope

    def _below_cancelled(self, start: Up) -> bool:
        return self._cancelling_owner(start) is not None

    def _cancelling_owner(self, start: Up) -> AnyTaskRecord | None:
        if not self._intent:
            return None
        for step in self._above(start):
            if "unknown" in step:
                return None
            if "task" not in step:
                continue
            live = self._live.get(step["task"])
            if live is not None and cancellation_intent(live):
                return live
            if step["node"]["background"]:
                return None
        return None

    def _fail_session(self, error: BaseException) -> None:
        if not self._closing:
            self._session.fail(error)

    def _seal(self) -> None:
        self._closing = True
        try:
            self._unsubscribe_registry()
        except Exception as error:
            self._report(error)
        error = closed_error(self._session)
        self._task_waiters.reject_all(error)
        self._idle_waiters.reject_all(error)
        self._contexts.clear()
        if self._expiry_handle is not None:
            self._expiry_handle.cancel()
            self._expiry_handle = None
        self._expiry = None

    def _kick(self) -> None:
        self._dirty = True
        if self._draining or not self._enabled or self._closing:
            return
        self._draining = True
        asyncio.get_running_loop().call_soon(lambda: self._spawn(self._drain()))

    def _spawn(self, work: Coroutine[Any, Any, Any]) -> None:
        """Run detached scheduler work, keeping it referenced until it finishes.

        A teardown signal means the Harness is being collected, not that its work failed:
        nothing owns the coroutine anymore, so scheduling anything else would resurrect it.
        """

        async def guarded() -> None:
            try:
                await work
            except BaseException as error:
                if not is_failure(error):
                    self._seal()
                raise

        task = asyncio.ensure_future(guarded())
        self._detached.add(task)
        task.add_done_callback(self._detached.discard)

    async def _drain(self) -> None:
        try:
            while self._dirty and self._enabled and not self._closing:
                self._dirty = False
                for reservation in await self._reserve():
                    self._start(reservation)
        except Exception as error:
            self._fail_session(error)
        finally:
            self._draining = False
            if self._dirty:
                self._kick()

    async def _reserve(self) -> list[dict[str, Any]]:
        reservations: list[dict[str, Any]] = []
        abandoned: list[TaskId] = []

        async def reserve(tx: Transaction) -> None:
            if not self._enabled or self._closing:
                return
            for id in list(self._abandoned):
                record = self._live.get(id)
                if record is not None and not record.get("abortRequested"):
                    tx.set_task(with_abort_mark(record, "restart"))
            if self._abandoned:
                abandoned.extend(self._abandoned)
                return
            await self._load_scopes(False)
            snapshot: RegistrySnapshot | None = None
            for id in list(self._runnable):
                record = self._live.get(id)
                if record is None or record["state"]["status"] == "completing" or id in self._invocations:
                    self._runnable.discard(id)
                    continue
                if self._waiting_on(record, True):
                    self._runnable.discard(id)
                    continue
                runnable = record
                mode: Literal["run", "abort"] = "abort" if record.get("abortRequested") else "run"
                if mode == "run" and not record["background"] and self._below_cancelled(parent_of(record)):
                    self._cascade_pending = True
                    self._schedule_reconcile()
                    continue
                if snapshot is None:
                    snapshot = self._registry.snapshot()
                resolution = self._resolve(runnable, snapshot)
                if resolution["kind"] == "blocked":
                    if mode == "abort" and record.get("abortReason") is None:
                        await self._terminate(
                            tx,
                            record,
                            {"status": "orphaned", "reason": resolution["reason"]},
                        )
                    continue
                if resolution["record"] is not runnable or record["state"]["status"] != "running":
                    tx.set_task(
                        with_state(
                            resolution["record"],
                            {"status": "running", "checkpoint": resolution["record"]["state"]["checkpoint"]},
                        )
                    )
                self._runnable.discard(id)
                invocation = self._create_invocation(record, mode)
                reservations.append(
                    {"invocation": invocation, "task": resolution["task"], "snapshot": snapshot}
                )

        try:
            await self._session.commit_with(reserve, self._context)
        except Exception:
            for reservation in reservations:
                self._invocations.pop(reservation["invocation"].task_id, None)
                self._runnable.add(reservation["invocation"].task_id)
                reservation["invocation"].finish()
            raise
        for id in abandoned:
            self._abandoned.discard(id)
        if abandoned:
            self._dirty = True
        return reservations

    def _waiting_on(self, record: Mapping[str, Any], first: bool = False) -> list[TaskId]:
        if record.get("abortRequested"):
            return self._owned_live(record["id"], first=first)
        if record["state"]["status"] != "waiting":
            return []
        on = record["state"]["on"]
        if not first:
            return [id for id in on if id in self._live]
        live = next((id for id in on if id in self._live), None)
        return [] if live is None else [live]

    def _resolve(self, record: RunnableTaskRecord, snapshot: RegistrySnapshot) -> dict[str, Any]:
        fit = self._fit(record, snapshot.task(record["kind"]))
        if "reason" in fit:
            return {"kind": "blocked", "reason": fit["reason"]}
        if not fit["migrates"]:
            return {"kind": "ready", "task": fit["task"], "record": record}
        definition = fit["task"].definition
        try:
            if definition.migrate is None:
                raise _missing_migration(record, definition)
            migrated = definition.migrate(record["input"], record["state"]["checkpoint"], record["version"])
            state = dict(record["state"])
            state["checkpoint"] = copy_json(migrated["checkpoint"])
            migrated_record = dict(record)
            migrated_record["version"] = definition.version
            migrated_record["input"] = copy_json(migrated["input"])
            migrated_record["state"] = state
            return {"kind": "ready", "task": fit["task"], "record": migrated_record}
        except Exception as error:
            self._failed_migrations[record["id"]] = {"task": fit["task"], "error": error}
            self._report(error)
            return {"kind": "blocked", "reason": "migration_failed"}

    def _fit(self, record: Mapping[str, Any], task: AnyTask | None) -> dict[str, Any]:
        if task is None:
            return {"reason": "missing_task"}
        version = task.definition.version
        if version == record["version"]:
            return {"task": task, "migrates": False}
        if version < record["version"]:
            return {"reason": "task_too_old"}
        failed = self._failed_migrations.get(record["id"])
        if failed is not None and failed["task"] is task:
            return {"reason": "migration_failed", "error": failed["error"]}
        return {"task": task, "migrates": True}

    @property
    def index_sizes(self) -> dict[str, int]:
        """Sizes of the ownership indexes, for tests that bound the scheduler's memory."""

        return {
            "live": len(self._live),
            "settled": len(self._settled),
            "edges": len(self._edges),
            "below": sum(len(children) for children in self._below.values()),
            "roots": len(self._roots),
            "unloaded": len(self._unloaded),
            "drop_queue": len(self._drop_queue),
        }

    async def inspect(self, snapshot: RegistrySnapshot) -> tuple[str, list[TaskInspection]]:
        await self._load_scopes(False)
        tasks: list[TaskInspection] = [
            {
                "record": cast(AnyTaskRecord, copy_json(record)),
                "state": self._inspect_task(record, snapshot),
            }
            for record in self._live.values()
        ]
        scheduling = "closing" if self._closing else ("running" if self._enabled else "paused")
        return scheduling, tasks

    def _inspect_task(self, record: AnyTaskRecord, snapshot: RegistrySnapshot) -> dict[str, Any]:
        if record["id"] in self._invocations:
            return {"kind": "running"}
        if record["state"]["status"] == "completing":
            return {"kind": "completing"}
        on = self._waiting_on(record)
        if on:
            return {"kind": "waiting", "on": on}
        fit = self._fit(record, snapshot.task(record["kind"]))
        if "reason" in fit:
            return {"kind": "blocked", **fit}
        if fit["migrates"] and fit["task"].definition.migrate is None:
            return {
                "kind": "blocked",
                "reason": "migration_failed",
                "error": _missing_migration(record, fit["task"].definition),
            }
        return {"kind": "ready", "migrates": fit["migrates"]}

    def _create_invocation(self, record: Mapping[str, Any], mode: Literal["run", "abort"]) -> _Invocation:
        invocation = _Invocation(record, mode, self._context)
        self._invocations[invocation.task_id] = invocation
        return invocation

    def _start(self, reservation: dict[str, Any]) -> None:
        invocation: _Invocation = reservation["invocation"]

        async def run() -> None:
            try:
                if invocation.mode == "run":
                    await self._run(reservation)
                else:
                    await self._run_abort(reservation)
            except BaseException as error:
                if not is_failure(error):
                    raise
                self._report(error)
            finally:
                self._end(invocation)
                invocation.finish()
                self._kick()

        self._spawn(run())

    async def _run(self, reservation: dict[str, Any]) -> None:
        invocation: _Invocation = reservation["invocation"]
        state = {
            "task": reservation["task"],
            "snapshot": reservation["snapshot"],
            "reported": None,
        }
        phase: dict[str, Any] = {
            "snapshot": lambda: state["snapshot"],
            "task": lambda: state["task"],
            "agent": None,
        }
        runtime = _Runtime(self, invocation, phase)
        previous: dict[str, Any] | None = None
        while True:
            current = await self._step(
                invocation,
                lambda tx, current, previous=previous: self._decide(tx, current, previous, state),
            )
            if current is None or self._closing:
                return
            checkpoint = current["state"]["checkpoint"]
            phase["agent"] = None
            try:
                handler = state["task"].definition.phases[checkpoint["phase"]]
                await _invoke_handler(handler, current, runtime, invocation.context)
                previous = {"checkpoint": checkpoint}
            except BaseException as error:
                if not is_failure(error):
                    raise
                previous = {"checkpoint": checkpoint, "failure": {"error": error}}
            # Detached commits the handler started land before the step, as upstream promises.
            for commit in list(invocation.commits):
                try:
                    await commit
                except BaseException as error:
                    if not is_failure(error):
                        raise

    def _decide(
        self,
        tx: Transaction,
        current: ErasedRunningTask,
        previous: dict[str, Any] | None,
        state: dict[str, Any],
    ) -> Any:
        if current.get("abortRequested"):
            return False
        if not current["background"] and self._below_cancelled(parent_of(current)):
            self._cascade_pending = True
            self._schedule_reconcile()
            return False
        if previous is None:
            return True
        if previous.get("failure") is not None:
            return {"fault": previous["failure"]["error"]}
        if json_equal(current["state"]["checkpoint"], previous["checkpoint"]):
            message = (
                f"Task {current['kind']} phase {previous['checkpoint']['phase']} "
                "returned without durable progress"
            )
            return {"fault": RuntimeError(message)}
        state["snapshot"] = self._registry.snapshot()
        following = state["snapshot"].task(current["kind"])
        if following is not state["task"]:
            if following is not None and can_reserve(following, current):
                tx.set_task(
                    with_state(current, {"status": "pending", "checkpoint": current["state"]["checkpoint"]})
                )
                return False
            if state["reported"] is None or state["reported"]["task"] is not following:
                state["reported"] = {"task": following}
                cause = "missing_task" if following is None else "incompatible_task"
                self._report(_OldDefinitionError(current["id"], current["kind"], cause))
        return True

    async def _run_abort(self, reservation: dict[str, Any]) -> None:
        invocation: _Invocation = reservation["invocation"]
        current = self._live.get(invocation.task_id)
        if current is None or current["state"]["status"] != "running" or self._closing:
            return
        failure: BaseException | None = None
        try:
            phase = {
                "snapshot": lambda: reservation["snapshot"],
                "task": lambda: reservation["task"],
                "agent": None,
            }
            runtime = _Runtime(self, invocation, phase)
            await _invoke_handler(reservation["task"].definition.abort, current, runtime, invocation.context)
        except BaseException as error:
            if not is_failure(error):
                raise
            failure = error
        message = f"Abort handler of task {invocation.task_id} returned without a terminal outcome"
        await self._step(
            invocation,
            lambda _tx, _current: {"fault": failure if failure is not None else RuntimeError(message)},
        )

    async def _step(
        self,
        invocation: _Invocation,
        decide: Callable[[Transaction, ErasedRunningTask], Any],
    ) -> ErasedRunningTask | None:
        async def step(tx: Transaction) -> ErasedRunningTask | None:
            found = self._live.get(invocation.task_id)
            current = (
                cast(ErasedRunningTask, found)
                if found is not None and found["state"]["status"] == "running"
                else None
            )
            decision = decide(tx, current) if current is not None and not self._closing else False
            if decision is True:
                return current
            self._end(invocation)
            if decision is not False:
                message = error_message(decision["fault"])
                await self._terminate(
                    tx,
                    cast(AnyTaskRecord, current),
                    {"status": "faulted", "error": {"message": message}},
                )
            return None

        try:
            return await self._session.commit_with(step, self._context)
        except BaseException as error:
            self._end(invocation)
            if not is_failure(error):
                raise
            self._fail_session(error)
            return None

    async def _terminate(self, tx: Transaction, record: AnyTaskRecord, outcome: Mapping[str, Any]) -> None:
        await self._load_scopes(False)
        if self._has_owned_live(record["id"], _overlay_of(tx)):
            tx.set_task(with_state(record, {"status": "completing", "outcome": outcome}))
            return
        tx.set_task(with_state(record, {"status": "terminal", "outcome": outcome}))
        await self._settle_outcome(tx, record, outcome)

    async def _commit_state(
        self,
        tx: Transaction,
        invocation: _Invocation,
        current: ErasedRunningTask,
        following: NextTaskState,
    ) -> None:
        if following["status"] == "waiting":
            await self._validate_wait(tx, invocation, current, following["on"], following["policy"])
        if following["status"] == "terminal":
            overlay = _overlay_of(tx)
            await self._load_scopes(False)
            for record in overlay["tasks"].values():
                await self._load_chain(parent_of(record), overlay)
            if self._has_owned_live(current["id"], overlay):
                tx.set_task(with_state(current, {"status": "completing", "outcome": following["outcome"]}))
                return
        tx.set_task(with_state(current, following))

    async def _validate_wait(
        self,
        tx: Transaction,
        invocation: _Invocation,
        current: ErasedRunningTask,
        on: Sequence[TaskId],
        policy: str,
    ) -> None:
        if invocation.mode == "abort":
            raise RuntimeError(f"Abort handler of task {current['id']} cannot wait")
        overlay = _overlay_of(tx)
        targets: list[TaskId] = []
        for id in on:
            member = (
                overlay["tasks"].get(id)
                or self._live.get(id)
                or (await self._storage.task(id, self._context))
            )
            if member is None:
                raise ValueError(f"Task {id} does not exist")
            if policy == "failFast" and member.get("owner") != current["id"]:
                raise ValueError(
                    f"Task {current['id']} can wait failFast only on tasks it owns; {id} is not one"
                )
            if member["state"]["status"] != "terminal" and member.get("owner") != current["id"]:
                targets.append(id)
        if not targets:
            return
        cycle = await self._wait_cycle(current["id"], targets, overlay)
        if cycle is not None:
            path = " -> ".join(str(id) for id in [current["id"], *cycle])
            raise ValueError(f"Task {current['id']} cannot wait on {cycle[0]}: {path} is a wait cycle")

    async def _wait_cycle(
        self, task: TaskId, on: Sequence[TaskId], overlay: dict[str, Any]
    ) -> list[TaskId] | None:
        await self._load_scopes(False)
        from_map: dict[TaskId, TaskId | None] = {}
        walked: set[TaskId] = set()
        pending: list[TaskId] = []

        def reach(id: TaskId, via: TaskId | None) -> None:
            if id in from_map:
                return
            from_map[id] = via
            pending.append(id)

        def path(end: TaskId) -> list[TaskId]:
            steps: list[TaskId] = []
            at: TaskId | None = end
            while at is not None:
                steps.insert(0, at)
                at = from_map[at]
            return steps

        for id in on:
            reach(id, None)
        while pending:
            id = pending.pop()
            if id == task:
                return path(id)
            record = overlay["tasks"].get(id) or self._live.get(id)
            if record is None or record["state"]["status"] == "terminal":
                continue
            if record["state"]["status"] == "waiting" and not record.get("abortRequested"):
                for member in record["state"]["on"]:
                    reach(member, id)
            if id in walked:
                continue
            walked.add(id)
            found: list[TaskId] = []
            for step in self._walk_down(id, overlay=overlay, enter=lambda below: below not in walked):
                if step["live"] is None or step["node"]["background"]:
                    continue
                found.append(step["id"])
                reach(step["id"], id)
            walked.update(found)
        return None

    def _end(self, invocation: _Invocation) -> None:
        if invocation.ended:
            return
        invocation.ended = True
        if self._invocations.get(invocation.task_id) is invocation:
            del self._invocations[invocation.task_id]
            if invocation.task_id in self._live:
                self._runnable.add(invocation.task_id)
        for watch in list(invocation.watches):
            watch.stop()
        invocation.controller.abort(_ended_error(invocation))

    def _idle(self, conversation_id: ConversationId | None) -> bool:
        scope: Scope = {"roots": True} if conversation_id is None else {"conversation": conversation_id}
        for id in self._unloaded:
            record = self._live.get(id)
            if (
                record is not None
                and not record["background"]
                and self._in_scope(parent_of(record), scope) is not False
            ):
                return False
        starts = self._roots if conversation_id is None else [conversation_id]
        for start in starts:
            for step in self._walk_down(conversation_node(start)):
                if step["live"] is not None and not step["node"]["background"]:
                    return False
        return True

    # ── Invocation runtime ──────────────────────────────────────────────────


class _HookRunner:
    def __init__(self, scheduler: TaskScheduler, invocation: _Invocation, phase: dict[str, Any]) -> None:
        self._scheduler = scheduler
        self._invocation = invocation
        self._phase = phase

    async def each(self, name: str, invoke: Callable[[Any], Any]) -> None:
        agent = await self._agent()
        for handlers in agent_hooks(agent, self._phase["task"]().definition.name):
            handler = handlers.get(name)
            if not callable(handler):
                continue
            try:
                result = invoke(handler)
                if hasattr(result, "__await__"):
                    await cast(Awaitable[Any], result)
            except BaseException as error:
                if not is_failure(error) or self._invocation.controller.signal.aborted:
                    raise
                self._scheduler._report(error)

    async def _agent(self) -> Agent:
        runtime = _Runtime(self._scheduler, self._invocation, self._phase)
        return await runtime.agent(self._scheduler._context)


class _Runtime:
    """What a phase handler may do: committed reads, gated commits, waits, and the Harness clock."""

    def __init__(self, scheduler: TaskScheduler, invocation: _Invocation, phase: dict[str, Any]) -> None:
        self._scheduler = scheduler
        self._invocation = invocation
        self._phase = phase
        self.task_id = invocation.task_id
        self.conversation_id = invocation.conversation_id
        self.signal = invocation.controller.signal
        self.models = scheduler._models

    @property
    def registry(self) -> RegistrySnapshot:
        self._check()
        return self._phase["snapshot"]()

    @property
    def settings(self) -> Settings:
        return self._scheduler._settings()

    async def agent(self, context: Context) -> Agent:
        self._check()
        if self._phase["agent"] is None:
            future = asyncio.ensure_future(
                self._scheduler._agent(
                    self._invocation.conversation_id, self._phase["snapshot"](), self._invocation.context
                )
            )
            future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
            self._phase["agent"] = future
        return await await_with_context(self._phase["agent"], context)

    async def env(self, context: Context) -> Any:
        self._check()
        return await self._scheduler._env(self._invocation.conversation_id, context)

    @property
    def hooks(self) -> _HookRunner:
        return _HookRunner(self._scheduler, self._invocation, self._phase)

    def commit(self, change: Callable[..., Any], context: Context) -> asyncio.Future[None]:
        async def run() -> None:
            async def apply(tx: Transaction, current: ErasedRunningTask) -> None:
                result = _invoke_change(change, tx, current)
                if hasattr(result, "__await__"):
                    following = await cast(Awaitable[Any], result)
                else:
                    following = result
                if following is not None:
                    await self._scheduler._commit_state(tx, self._invocation, current, following)

            await self._gated(apply, context)

        commit = asyncio.ensure_future(run())
        self._invocation.commits.add(commit)
        commit.add_done_callback(lambda done: self._invocation.commits.discard(done))
        return commit

    async def memo(self, name: str, *args: Any) -> JsonValue | None:
        if len(args) == 1:
            context = args[0]

            def read() -> JsonValue | None:
                winner = memo_of(self._scheduler._live.get(self._invocation.task_id), name)
                return None if winner is _MISSING_MEMO else cast(JsonValue, winner)

            return await self._read(lambda: _resolved(read()), context)
        candidate, context = args

        def write(tx: Transaction, current: ErasedRunningTask) -> JsonValue | None:
            winner = memo_of(current, name)
            if winner is not _MISSING_MEMO:
                return cast(JsonValue, winner)
            record = dict(current)
            record["memos"] = {**(current.get("memos") or {}), name: candidate}
            tx.set_task(cast(AnyTaskRecord, record))
            return cast(JsonValue, candidate)

        return await self._gated(write, context)

    async def sleep(self, until: int, context: Context) -> None:
        self._check()
        signals = [self._invocation.controller.signal]
        if context.abort_signal is not None:
            signals.append(context.abort_signal)
        signal = AbortSignal.any(signals)
        while True:
            if signal.aborted:
                raise _cancellation_error(signal)
            remaining = until - self._scheduler._now()
            if remaining <= 0:
                return
            await _delay(min(remaining, MAX_TIMER_DELAY_MS), signal)

    async def watch_doc(self, *args: Any) -> Any:
        self._check()
        watch = await self._scheduler._session.watch_doc(*args)
        if watch is None:
            return None
        if self._invocation.ended:
            watch.stop()
            raise _ended_error(self._invocation)
        self._invocation.watches.add(watch)

        def remove(_: Any) -> None:
            self._invocation.watches.discard(watch)

        watch.closed.add_done_callback(remove)
        return watch

    async def snapshot(self, *args: Any) -> Any:
        self._check()
        return await self._scheduler._session.snapshot(*args)

    async def snapshot_as_of(self, *args: Any) -> Any:
        self._check()
        return await self._scheduler._session.snapshot_as_of(*args)

    async def get_task(self, id: TaskId, context: Context) -> TaskRecord | None:
        self._check()
        return await self._read(
            lambda: self._scheduler._session.read_on_line(lambda: self._scheduler._storage.task(id, context)),
            context,
        )

    async def wait_for_task(self, id: TaskId, context: Context) -> SettledTask:
        self._check()
        bound = with_abort_signal(self._invocation.controller.signal, context)
        return await self._read(
            lambda: self._scheduler.wait_for_task(id, bound),
            context,
        )

    async def owned_tasks(self, context: Context) -> list[TaskRecord]:
        self._check()

        async def read() -> list[TaskRecord]:
            if self._invocation.ended:
                raise _ended_error(self._invocation)
            if context.abort_signal is not None:
                context.abort_signal.raise_if_aborted()
            owned: list[TaskRecord] = []
            for node in self._scheduler._below.get(self._invocation.task_id, set()):
                if isinstance(node, str):
                    continue
                record = self._scheduler._live.get(node)
                if record is not None and record.get("owner") == self._invocation.task_id:
                    owned.append(record)
            return sorted(
                (cast(TaskRecord, copy_json(record)) for record in owned),
                key=lambda record: record["id"],
            )

        return await self._read(
            lambda: self._scheduler._session.read_on_line(read),
            context,
        )

    async def abort_owned(self, id: TaskId, context: Context) -> None:
        self._check()
        bound = with_abort_signal(self._invocation.controller.signal, context)

        async def abort() -> None:
            record = await self._scheduler._session.read_on_line(
                lambda: self._scheduler._storage.task(id, bound)
            )
            if record is None or record.get("owner") != self._invocation.task_id:
                raise ValueError(f"Task {id} is not owned by task {self._invocation.task_id}")
            if record["state"]["status"] == "terminal":
                return
            await self._scheduler.abort(id, bound, True)
            await self._scheduler.wait_for_task(id, bound)

        await self._read(abort, context)

    async def outcomes(self, ids: Sequence[TaskId], context: Context) -> list[TaskOutcome]:
        self._check()

        async def read() -> list[TaskOutcome]:
            outcomes: list[TaskOutcome] = []
            for id in ids:
                record = await self._scheduler._storage.task(id, context)
                state = None if record is None else record["state"]
                if state is None or state["status"] != "terminal":
                    raise ValueError(f"Task {id} is not terminal")
                outcomes.append(state["outcome"])
            return outcomes

        return await self._read(
            lambda: self._scheduler._session.read_on_line(read),
            context,
        )

    async def conversation(self, id: ConversationId, context: Context) -> ConversationHandle | None:
        self._check()

        async def read() -> Any:
            binding = {
                "signal": self._invocation.controller.signal,
                "check": lambda: self._check(),
            }
            return await self._scheduler._conversation(id, binding, context)

        return await self._read(read, context)

    async def entry(self, *args: Any) -> Any:
        self._check()
        if len(args) == 2:
            token, id, context = None, args[0], args[1]
        else:
            token, id, context = args[0], args[1], args[2]

        async def read() -> Any:
            found = await self._scheduler._session.read_on_line(
                lambda: self._scheduler._storage.entry(self._invocation.conversation_id, id, context)
            )
            entry = None if found is None else found["entry"]
            if token is None or entry is None or entry.get("kind") == token.kind:
                return entry
            return None

        return await self._read(read, context)

    async def context(self, conversation_id: ConversationId, context: Context, *args: Any) -> Any:
        self._check()
        options = args[0] if args else None
        at = options.get("at") if isinstance(options, Mapping) else None
        kept = self._scheduler._contexts.get(conversation_id)

        async def read() -> Any:
            view, context_range = await read_context_from(
                self._scheduler._session,
                self._scheduler._storage,
                conversation_id,
                context,
                at,
                None if kept is None else kept["range"],
            )
            current = self._scheduler._contexts.get(conversation_id)
            if (
                context_range is not None
                and not self._scheduler._closing
                and not self._invocation.ended
                and (current is None or current["range"]["bounds"]["tail"] <= context_range["bounds"]["tail"])
            ):
                if not self._scheduler._idle(conversation_id):
                    self._scheduler._contexts[conversation_id] = {
                        "range": context_range,
                        "idle_since": None,
                    }
                elif self._scheduler._context_retention_ms() > 0:
                    self._scheduler._contexts[conversation_id] = {
                        "range": context_range,
                        "idle_since": (
                            current["idle_since"]
                            if current is not None and current["idle_since"] is not None
                            else self._scheduler._now()
                        ),
                    }
                    self._scheduler._schedule_expiry()
            return view

        return await self._read(read, context)

    def now(self) -> int:
        self._check()
        return self._scheduler._now()

    def report(self, error: BaseException) -> None:
        self._check()
        self._scheduler._report(error)

    async def _read(self, read: Callable[[], Awaitable[Any]], context: Context) -> Any:
        self._check()
        result = await read()
        return await await_with_context(_resolved(result), context)

    async def _gated(
        self,
        change: Callable[[Transaction, ErasedRunningTask], Awaitable[Any] | Any],
        context: Context,
    ) -> Any:
        if self._invocation.ended:
            raise _ended_error(self._invocation)

        async def apply(tx: Transaction) -> Any:
            if self._invocation.ended:
                raise _ended_error(self._invocation)
            if self._scheduler._closing:
                raise closed_error(self._scheduler._session)
            found = self._scheduler._live.get(self._invocation.task_id)
            if found is None:
                raise ValueError(f"Task {self._invocation.task_id} is terminal")
            if found["state"]["status"] != "running":
                raise ValueError(f"Task {self._invocation.task_id} is {found['state']['status']}")
            current = cast(ErasedRunningTask, found)
            if self._invocation.mode == "run" and current.get("abortRequested"):
                raise ValueError(f"Task {self._invocation.task_id} has a durable abort mark")
            result = change(tx, current)
            if hasattr(result, "__await__"):
                result = await cast(Awaitable[Any], result)
            return result

        from ..session.transaction import TransactionScope

        try:
            return await self._scheduler._session.commit_with(
                apply,
                context,
                TransactionScope(
                    conversation_id=self._invocation.conversation_id,
                    task_id=self._invocation.task_id,
                ),
            )
        except BaseException:
            # Close seals admission, then signals handlers. A commit that was queued on the
            # line rejects as closed, not with its handler's cancellation reason.
            if self._scheduler._closing:
                raise closed_error(self._scheduler._session) from None
            raise

    def _check(self) -> None:
        if self._invocation.ended:
            raise _ended_error(self._invocation)


def _invoke_change(change: Callable[..., Any], tx: Transaction, current: ErasedRunningTask) -> Any:
    """Call a commit callback with the arguments it declares, as JavaScript ignores extras."""

    try:
        parameters = list(inspect.signature(change).parameters.values())
    except (TypeError, ValueError):
        return change(tx, current)
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if (
        any(parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in parameters)
        or len(positional) >= 2
    ):
        return change(tx, current)
    if len(positional) == 1:
        return change(tx)
    return change()


async def _invoke_handler(handler: Any, task: Any, runtime: Any, context: Context) -> Any:
    result = handler(task, runtime, context)
    if hasattr(result, "__await__"):
        return await cast(Awaitable[Any], result)
    return result


def _resolved(value: Any) -> asyncio.Future[Any]:
    future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
    future.set_result(value)
    return future


async def _race(first: Awaitable[Any], second: Awaitable[Any]) -> Any:
    tasks = [asyncio.ensure_future(first), asyncio.ensure_future(second)]
    done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in done:
        return task.result()
    raise AssertionError("unreachable")


__all__ = [
    "LIVE_STATUSES",
    "TaskScheduler",
    "can_reserve",
    "cancellation_intent",
    "conversation_node",
    "conversation_of",
    "failed_outcome",
    "json_equal",
    "memo_of",
    "node_of",
    "parent_node",
    "parent_of",
    "with_abort_mark",
    "with_state",
]

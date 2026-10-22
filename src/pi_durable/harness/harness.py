"""Durable Harness: a Session with task scheduling, conversations, and inspection."""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Mapping
from functools import partial
from typing import Any, Literal, cast

from ..context import Context, with_abort_signal, without_abort_signal
from ..entries import ResetEntry
from ..ids import ConversationId, EntryId, SubmissionId, TaskId
from ..session.session import SessionImpl, _now_ms, contained
from ..session.transaction import Transaction, TransactionScope
from ..types import (
    ROOT_CONVERSATION_ID,
    ConversationOwnership,
    ConversationRecord,
    EntryRecord,
    Page,
    Storage,
    SubmissionRecord,
    TaskOutcome,
    TaskRecord,
)
from .agent import AgentDoc, configure, create_agent, resolve_agent, resolve_settings
from .inbox import InboxDoc, QueueModes, withdraw_queued_inputs
from .live import GENERATION_TASK_NAME, LiveDoc, settle_scheduler_outcome, start_run
from .scheduler import TaskScheduler
from .submissions import AbortResult, Submissions
from .types import (
    Agent,
    ContextView,
    ConversationHandle,
    HarnessInspection,
    RegistryReader,
    RegistrySnapshot,
    SettledSubmissionRecord,
    SettledTask,
    Submission,
)
from .util import closed_error, scan_all

_SCAN_PAGE_SIZE = 256


def _safe_report(on_report: Callable[[BaseException], Any] | None) -> Callable[[BaseException], None]:
    def report(error: BaseException) -> None:
        def call() -> Any:
            return None if on_report is None else on_report(error)

        contained(call, lambda _error: None)

    return report


def safe_now(now: Callable[[], int] | None, report: Callable[[BaseException], None]) -> Callable[[], int]:
    if now is None:
        return _now_ms
    reported = False

    def clock() -> int:
        nonlocal reported
        try:
            return now()
        except Exception as error:
            if not reported:
                reported = True
                report(error)
            return _now_ms()

    return clock


class BoundConversation:
    """Invocation-bound conversation handle for tasks and tools."""

    __slots__ = ("id", "_harness", "_binding")

    def __init__(self, id: ConversationId, harness: Harness, binding: Mapping[str, Any]) -> None:
        self.id = id
        self._harness = harness
        self._binding = binding

    def _bind(self, context: Context) -> Context:
        return _bind_context(self._binding, context)

    async def submit(self, submission: Any, context: Context) -> Any:
        cast(Callable[[], None], self._binding["check"])()
        admitted = await self._harness.submit(self.id, submission, self._bind(context))
        return BoundSubmission(admitted, self._binding)

    async def abort(self, context: Context, options: Mapping[str, Any] | None = None) -> None:
        cast(Callable[[], None], self._binding["check"])()
        await self._harness._tasks.abort_conversation(
            self.id, (options or {}).get("background") is True, self._bind(context)
        )

    async def wait_for_idle(self, context: Context) -> None:
        cast(Callable[[], None], self._binding["check"])()
        await self._harness._tasks.wait_for_idle(self.id, self._bind(context))


class BoundSubmission:
    """Invocation-bound submission handle for tasks and tools."""

    __slots__ = ("id", "_submission", "_binding")

    def __init__(self, submission: Submission, binding: Mapping[str, Any]) -> None:
        self.id = submission.id
        self._submission = submission
        self._binding = binding

    def _bind(self, context: Context) -> Context:
        return _bind_context(self._binding, context)

    async def status(self, context: Context) -> SubmissionRecord:
        cast(Callable[[], None], self._binding["check"])()
        return await self._submission.status(self._bind(context))

    async def wait(self, context: Context) -> SettledSubmissionRecord:
        cast(Callable[[], None], self._binding["check"])()
        return await self._submission.wait(self._bind(context))

    async def abort(self, context: Context) -> AbortResult:
        cast(Callable[[], None], self._binding["check"])()
        return await self._submission.abort(self._bind(context))


class ConversationHandleImpl:
    """Stateless handle for one conversation, bound to the Harness that returned it."""

    __slots__ = ("id", "_harness")

    def __init__(self, id: ConversationId, harness: Harness) -> None:
        self.id = id
        self._harness = harness

    async def agent(self, context: Context) -> Agent:
        return await self._harness.resolve_agent(self.id, None, context)

    async def configure(self, change: Mapping[str, Any], context: Context) -> None:
        async def apply(tx: Transaction) -> None:
            await configure(tx, self.id, change)

        await self._harness.commit(apply, context)

    async def submit(self, submission: Any, context: Context) -> Any:
        return await self._harness.submit(self.id, submission, context)

    async def commit(self, change: Callable[[Transaction], Any], context: Context) -> Any:
        return await self._harness.commit_with(change, context, TransactionScope(conversation_id=self.id))

    async def context(self, context: Context, options: Mapping[str, Any] | None = None) -> ContextView:
        return await self._harness.context(self.id, context, (options or {}).get("at"))

    async def entries(
        self,
        query: Mapping[str, Any],
        limit: int,
        cursor: Any,
        context: Context,
    ) -> Page[EntryRecord, Any]:
        bounded = {"conversationId": self.id, **dict(query)}
        return await self._harness.read_on_line(
            lambda: self._harness.storage.scan_entries(cast(Any, bounded), limit, cursor, context)
        )

    async def fork(self, at: EntryId, options: Mapping[str, Any], context: Context) -> ConversationHandleImpl:
        async def create(tx: Transaction) -> ConversationId:
            record = await tx.fork_conversation(self.id, at, {"ownership": options["ownership"]})
            return record["id"]

        id = await self._harness.commit(create, context)
        return ConversationHandleImpl(id, self._harness)

    async def abort(self, context: Context, options: Mapping[str, Any] | None = None) -> None:
        self._harness.resume()
        await self._harness._tasks.abort_conversation(
            self.id, (options or {}).get("background") is True, context
        )

    async def wait_for_idle(self, context: Context) -> None:
        self._harness.resume()
        await self._harness._tasks.wait_for_idle(self.id, context)

    async def compact(self, instructions: str | None, context: Context) -> TaskId:
        raise NotImplementedError("Compaction arrives with the built-in tasks")

    async def reset(self, handoff: str | None, context: Context) -> None:
        entry: dict[str, Any] = {"kind": ResetEntry.kind, "head": "self"}
        if handoff is not None:
            entry["model"] = [{"role": "user", "content": handoff, "timestamp": self._harness.now}]
        await self.submit({"type": "write", "entry": entry}, context)

    async def view_state(self, context: Context) -> Any:
        raise NotImplementedError("Conversation view arrives with the observation surfaces")

    async def watch(self, context: Context) -> Any:
        raise NotImplementedError("Conversation watch arrives with the observation surfaces")


class Harness(SessionImpl):
    """Durable agent harness over one Session."""

    def __init__(self, raw_storage: Storage, options: Mapping[str, Any], context: Context) -> None:
        report = _safe_report(options.get("on_report"))
        now = safe_now(options.get("now"), report)
        settings_option = options.get("settings")
        super().__init__(
            raw_storage,
            now,
            lambda: _settings_value(settings_option, "document_retention_ms"),
        )
        self._options = dict(options)
        self._report_sink = report
        self._tasks = TaskScheduler(
            session=self,
            storage=self.storage,
            registry=cast(RegistryReader, options.get("registry")),
            models=options.get("models"),
            agent=lambda id, snapshot, call_context: self.resolve_agent(id, snapshot, call_context),
            settings=lambda: resolve_settings(settings_option),
            env=lambda id, call_context: self.build_env(id, call_context),
            now=now,
            report=report,
            settle_outcome=lambda tx, record, outcome: self._settle_outcome(tx, record, cast(Any, outcome)),
            withdraw_inputs=lambda tx, conversation_id: self._withdraw_inputs(tx, conversation_id),
            conversation=lambda id, binding, call_context: self._bound_conversation(
                id, binding, call_context
            ),
            context=without_abort_signal(context),
        )
        self._settings_option = settings_option
        self._submissions = Submissions(
            session=self,
            now=now,
            queue_modes=lambda: _queue_modes(settings_option),
            resume=self._tasks.resume,
            start_run=self._start_run,
        )

    # ── Construction ───────────────────────────────────────────────────────

    @classmethod
    async def open(
        cls,
        storage: Storage,
        options: Mapping[str, Any],
        context: Context,
    ) -> Harness:
        """Open a Harness over storage; the registry may keep changing while it runs."""

        signal = context.abort_signal
        if signal is not None:
            signal.raise_if_aborted()
        harness = cls(storage, options, context)
        try:
            await harness._tasks.open(context)
        except BaseException:
            try:
                await harness.close(without_abort_signal(context))
            except Exception:
                pass
            raise
        return harness

    # ── Harness surface ────────────────────────────────────────────────────

    def report(self, error: BaseException) -> None:
        self._report_sink(error)

    def close(self, context: Context) -> Coroutine[Any, Any, None]:
        """A Harness seals admission synchronously, before it signals and joins its handlers."""

        self._begin_close()

        async def wait() -> None:
            await self._wait_close(context)

        return wait()

    async def before_close(self) -> None:
        await self._tasks.join()

    def _closed_error(self) -> Exception:
        return cast(Exception, closed_error(self))

    async def conversation_created(self, tx: Transaction, record: ConversationRecord) -> None:
        await tx.doc(LiveDoc, record["id"])
        await tx.doc(InboxDoc, record["id"])
        await tx.doc(AgentDoc, record["id"])
        await create_agent(tx, record)
        custom = self._options.get("conversation_created") or self._options.get("conversationCreated")
        if custom is not None:
            result = custom(tx, record)
            if hasattr(result, "__await__"):
                await cast(Any, result)

    async def resolve_agent(
        self,
        id: ConversationId,
        snapshot: RegistrySnapshot | None,
        context: Context,
    ) -> Agent:
        registry = snapshot or cast(RegistryReader, self._options["registry"]).snapshot()
        state = await self.snapshot(AgentDoc, id, context)
        settings = resolve_settings(self._settings_option)
        return resolve_agent(state, registry, settings, self.report)

    async def build_env(self, id: ConversationId, context: Context) -> Any:
        build = self._options.get("env")
        if build is None:
            return None
        state = await self.snapshot(AgentDoc, id, context)
        target: dict[str, Any] = {"conversation_id": id, "read": self}
        if state is not None and "cwd" in state:
            target["cwd"] = state["cwd"]
        result = build(target, context)
        if hasattr(result, "__await__"):
            return await cast(Any, result)
        return result

    def resume(self) -> None:
        self.assert_usable()
        self._tasks.resume()

    async def get_task(self, id: TaskId, context: Context) -> TaskRecord | None:
        return await self.read_on_line(lambda: self.storage.task(id, context))

    @property
    def scheduler_index_sizes(self) -> dict[str, int]:
        """Internal, for tests: sizes of the scheduler's ownership indexes."""

        return self._tasks.index_sizes

    async def inspect(self, context: Context) -> HarnessInspection:
        async def read() -> HarnessInspection:
            scheduling, tasks = await self._tasks.inspect(
                cast(RegistryReader, self._options["registry"]).snapshot()
            )
            submissions: list[SubmissionRecord] = []

            async def scan(status: Literal["queued", "placed"], cursor: Any) -> Any:
                return await self.storage.scan_submissions(
                    {"status": status}, _SCAN_PAGE_SIZE, cursor, context
                )

            for status in ("queued", "placed"):
                submissions.extend(await scan_all(partial(scan, status)))
            submissions.sort(key=lambda record: record["id"])
            return {"scheduling": cast(Any, scheduling), "tasks": tasks, "submissions": submissions}

        return await self.read_on_line(read)

    async def abort_task(self, id: TaskId, context: Context) -> str:
        return await self._tasks.abort(id, context)

    async def wait_for_task(self, id: TaskId, context: Context) -> SettledTask:
        self._tasks.resume()
        return await self._tasks.wait_for_task(id, context)

    async def wait_for_idle(self, context: Context) -> None:
        self._tasks.resume()
        await self._tasks.wait_for_idle(None, context)

    async def root(
        self, context: Context, options: Mapping[str, Any] | None = None
    ) -> ConversationHandleImpl:
        return await self._create({"kind": "root"}, options or {}, context)

    async def conversation(self, id: ConversationId, context: Context) -> ConversationHandleImpl | None:
        self.assert_usable()
        record = await self.read_on_line(lambda: self.storage.conversation(id, context))
        return None if record is None else ConversationHandleImpl(record["id"], self)

    async def create_conversation(
        self, options: Mapping[str, Any], context: Context
    ) -> ConversationHandleImpl:
        return await self._create(
            {"kind": "independent", "ownership": options["ownership"]}, options, context
        )

    async def _create(
        self,
        target: Mapping[str, Any],
        options: Mapping[str, Any],
        context: Context,
    ) -> ConversationHandleImpl:
        self.assert_usable()

        async def create(tx: Transaction) -> ConversationId:
            if target["kind"] == "root":
                existing = await tx.conversation(ROOT_CONVERSATION_ID)
                if existing is not None:
                    return ROOT_CONVERSATION_ID
                record = await tx.create_root_conversation()
            elif target["kind"] == "fork":
                record = await tx.fork_conversation(
                    target["parent_id"], target["at"], {"ownership": target["ownership"]}
                )
            else:
                record = await tx.create_conversation(
                    {"ownership": cast(ConversationOwnership, target["ownership"])}
                )
            agent = options.get("agent")
            if agent is not None:
                await configure(tx, record["id"], agent)
            init = options.get("init")
            if init is not None:
                result = init(tx, record["id"])
                if hasattr(result, "__await__"):
                    await cast(Any, result)
            return record["id"]

        id = await self.commit(create, context)
        return ConversationHandleImpl(id, self)

    # ── Submissions ────────────────────────────────────────────────────────

    async def submit(
        self, conversation_id: ConversationId, submission: Any, context: Context
    ) -> Submission:
        return await self._submissions.submit(conversation_id, submission, context)

    async def submission(self, id: SubmissionId, context: Context) -> Submission | None:
        return await self._submissions.get(id, context)

    async def abort_submission(
        self, id: SubmissionId, context: Context, conversation_id: ConversationId | None = None
    ) -> AbortResult | Literal["not_found"]:
        return await self._submissions.abort(id, context, conversation_id)

    async def _start_run(
        self, tx: Transaction, conversation_id: ConversationId, live: Any, inputs: list[Any]
    ) -> None:
        task = cast(RegistryReader, self._options["registry"]).snapshot().task(
            GENERATION_TASK_NAME
        )
        if task is None:
            raise NotImplementedError("Generation arrives with the run-loop ticket")
        await start_run(tx, conversation_id, live, inputs, task)

    # ── Internal scheduler callbacks ───────────────────────────────────────

    async def _settle_outcome(self, tx: Transaction, record: TaskRecord, outcome: TaskOutcome) -> None:
        await settle_scheduler_outcome(tx, record, cast(Any, outcome))

    async def _withdraw_inputs(self, tx: Transaction, conversation_id: ConversationId) -> None:
        await withdraw_queued_inputs(tx, conversation_id)

    async def _bound_conversation(
        self,
        id: ConversationId,
        binding: Mapping[str, Any],
        context: Context,
    ) -> ConversationHandle | None:
        record = await self.read_on_line(lambda: self.storage.conversation(id, context))
        return None if record is None else BoundConversation(id, self, binding)

    # ── Not yet ported ─────────────────────────────────────────────────────

    async def usage(self, context: Context) -> Any:
        raise NotImplementedError("Usage arrives with the run-loop ticket")

    async def task_graph(self, context: Context) -> Any:
        raise NotImplementedError("Task graph arrives with the observation surfaces")

    async def watch_task_graph(self, context: Context) -> Any:
        raise NotImplementedError("Task graph arrives with the observation surfaces")


def _bind_context(binding: Mapping[str, Any], context: Context) -> Context:
    signal = binding["signal"]
    return context if signal is None else with_abort_signal(signal, context)


def _queue_modes(settings: Any) -> QueueModes:
    resolved = resolve_settings(settings)
    return QueueModes(
        steering_mode=resolved.steering_mode,
        follow_up_mode=resolved.follow_up_mode,
    )


def _settings_value(settings: Any, name: str) -> Any:
    if settings is None:
        return None
    if isinstance(settings, Mapping):
        return settings.get(name)
    return getattr(settings, name, None)


__all__ = ["Harness", "safe_now"]

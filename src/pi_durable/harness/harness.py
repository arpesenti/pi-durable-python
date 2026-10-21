"""Harness: a Session plus durable agent scheduling on top of it."""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ..context import read_context, user_message
from ..documents import materialize_document
from ..errors import ConversationBusy
from ..json import copy_json
from ..scheduler import TaskScheduler
from ..session import Session
from ..types import TaskOptions, TransactionScope
from .agent import Agent, configure, read_agent_state, resolve_agent
from .docs import AgentDoc, InboxDoc, LiveDoc, ProviderDoc, UsageDoc, add_usage
from .generation import (
    GenerationTask,
    ModelRequest,
    ToolTask,
    resolve_model,
)
from .registry import (
    Registry,
    create_registry,
    define_extension,
    define_tool,
    section,
)
from .settings import HarnessSettings, resolve_settings
from .submissions import (
    FOLLOW_UP,
    INPUT,
    STEER,
    WRITE,
    Submission,
    apply_boundary,
    input_item,
    queued_submission,
    withdraw_queued_inputs,
    write_item,
)

DEFAULT_SETTINGS = HarnessSettings()


@dataclass
class HarnessInspection:
    scheduling: str
    tasks: list[dict[str, Any]]
    submissions: list[dict[str, Any]]


class DocumentReader:
    """Committed document reads handed to the environment builder."""

    def __init__(self, harness: Harness) -> None:
        self._harness = harness

    async def snapshot(self, token: Any, *args: Any) -> Any:
        return await self._harness.snapshot(token, *args)

    async def snapshot_as_of(self, token: Any, conversation_id: int, *args: Any) -> Any:
        return await self._harness.snapshot_as_of(token, conversation_id, *args)


class Conversation:
    """Stateless handle to one transcript scope. Compare handles by ``id``."""

    def __init__(self, harness: Harness, conversation_id: int) -> None:
        self._harness = harness
        self.id = conversation_id

    def __repr__(self) -> str:
        return f"Conversation({self.id})"

    async def submit(self, submission: Mapping[str, Any], context: Any = None) -> Submission:
        return await self._harness._submit(self.id, submission)

    async def agent(self, context: Any = None) -> Agent:
        return await self._harness.resolve_agent(self.id)

    async def configure(self, change: Mapping[str, Any], context: Any = None) -> None:
        async def run(tx: Any) -> None:
            await configure(tx, self.id, change)

        await self._harness.commit_with(run, TransactionScope(conversation_id=self.id))

    async def commit(self, change: Callable[[Any], Any], context: Any = None) -> Any:
        return await self._harness.commit_with(change, TransactionScope(conversation_id=self.id))

    async def context(self, options: Mapping[str, Any] | None = None, context: Any = None) -> dict[str, Any]:
        at = (options or {}).get("at")
        return await self._harness.read_context(self.id, at)

    async def entries(
        self,
        query: Mapping[str, Any],
        limit: int,
        cursor: Mapping[str, Any] | None = None,
        context: Any = None,
    ) -> dict[str, Any]:
        bound = {**query, "conversationId": self.id}

        async def run(tx: Any) -> dict[str, Any]:
            return await tx.scan_entries(bound, limit, cursor)

        return await self._harness.commit_with(run, TransactionScope(conversation_id=self.id))

    async def fork(
        self,
        at: int,
        options: Mapping[str, Any],
        context: Any = None,
    ) -> Conversation:
        return await self._harness._create_conversation(
            options,
            parent_conversation_id=self.id,
            at=at,
        )

    async def compact(self, instructions: str | None = None, context: Any = None) -> int:
        raise NotImplementedError("Compaction is not implemented in this port")

    async def reset(self, handoff: str | None = None, context: Any = None) -> None:
        entry: dict[str, Any] = {"kind": "pi.reset", "head": "self"}
        if handoff:
            entry["model"] = [user_message(handoff, timestamp=self._harness.now())]
        await self.submit({"type": WRITE, "entry": entry})

    async def abort(self, context: Any = None, options: Mapping[str, Any] | None = None) -> None:
        await self._harness._abort_conversation(self.id, background=bool((options or {}).get("background")))

    async def wait_for_idle(self, context: Any = None) -> None:
        self._harness.resume()
        await self._harness.scheduler.wait_for_idle(self.id)

    async def view(self, context: Any = None) -> dict[str, Any]:
        """A structural snapshot of transcript, live run, inbox, and usage."""
        return await self._harness._view(self.id)

    async def view_state(self, context: Any = None) -> dict[str, Any]:
        return await self.view()

    async def watch(self, context: Any = None) -> Any:
        raise NotImplementedError("Conversation watches are not implemented in this port")


class Harness(Session):
    """One open storage plus the machinery that runs agents on it."""

    def __init__(
        self,
        storage: Any,
        *,
        models: Any,
        registry: Registry,
        settings: Any = None,
        env: Callable[[Mapping[str, Any]], Any] | None = None,
        now: Callable[[], int] | None = None,
        on_report: Callable[[BaseException, Mapping[str, Any]], None] | None = None,
        on_wake: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> None:
        self._models_source = models
        self._registry = registry
        self._settings_source = settings
        self._env_builder = env
        self._on_report = on_report
        self._on_wake = on_wake
        self._last_wake: dict[str, Any] | None = None
        resolved = resolve_settings(settings)
        clock = now or (lambda: int(time.time() * 1000))
        super().__init__(
            storage,
            now=clock,
            document_retention_ms=int(getattr(resolved, "document_retention_ms", 600_000)),
        )
        self.scheduler = TaskScheduler(
            self,
            registry=registry,
            models=lambda: self.models,
            settings=lambda: self.settings,
            now=self.now,
            report=self._scheduler_report,
            env=self._scheduler_env,
            read_context=lambda conversation_id, at: read_context(self, conversation_id, at),
            settle_outcome=self._settle_scheduler_outcome,
        )
        self.scheduler.harness = self
        self.scheduler.conversation_handle = self._runtime_conversation
        self._watch_unsubscribe: Callable[[], None] | None = None

    # ── Open ───────────────────────────────────────────────────────────────

    @classmethod
    async def open(
        cls,
        storage: Any,
        *,
        models: Any,
        registry: Registry,
        settings: Any = None,
        env: Callable[[Mapping[str, Any]], Any] | None = None,
        now: Callable[[], int] | None = None,
        on_report: Callable[[BaseException, Mapping[str, Any]], None] | None = None,
        on_wake: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> Harness:
        snapshot = registry.snapshot()
        for name in (GenerationTask.definition.name, ToolTask.definition.name):
            if name not in snapshot.tasks:
                raise ValueError(f"Registry is missing built-in task {name}")
        harness = cls(
            storage,
            models=models,
            registry=registry,
            settings=settings,
            env=env,
            now=now,
            on_report=on_report,
            on_wake=on_wake,
        )
        await harness.scheduler.open()
        await harness._after_start()
        return harness

    async def _after_start(self) -> None:
        self._watch_unsubscribe = self.subscribe_commits(self._wake_changed)
        if self._on_wake is not None:
            self._last_wake = self.wake()
            self._call_on_wake(self._last_wake)

    def _call_on_wake(self, wake: Mapping[str, Any]) -> None:
        if self._on_wake is None:
            return
        try:
            self._on_wake(copy_json(dict(wake)))
        except BaseException as error:  # noqa: BLE001 - host callback
            self.report(error, {"type": "host_error", "callback": "onWake"})

    def _wake_changed(self, publication: Mapping[str, Any]) -> None:
        wake = self.wake()
        if wake != self._last_wake:
            self._last_wake = wake
            self._call_on_wake(wake)

    # ── Settings / models / registry ───────────────────────────────────────

    @property
    def settings(self) -> HarnessSettings:
        return resolve_settings(self._settings_source)

    @property
    def models(self) -> Any:
        source = self._models_source
        return source() if callable(source) and not hasattr(source, "get") else source

    @property
    def registry(self) -> Registry:
        return self._registry

    def report(self, error: BaseException, info: Mapping[str, Any] | None = None) -> None:
        if self._on_report is not None:
            try:
                self._on_report(error, dict(info or {"type": "listener_error"}))
            except BaseException:  # noqa: BLE001, S110 - a report handler never fails its caller
                pass

    def _scheduler_report(
        self,
        error: BaseException,
        info: Mapping[str, Any],
        from_: Mapping[str, Any] | None,
    ) -> None:
        merged = dict(info)
        if from_ is not None and from_.get("type") == "extension":
            merged = {"type": "extension_error", **({"extension": from_.get("extension")} if from_.get("extension") else {})}
            if info.get("task") is not None:
                merged["task"] = info["task"]
        self.report(error, merged)

    # ── Document reader / env / models ─────────────────────────────────────

    def document_reader(self) -> DocumentReader:
        return DocumentReader(self)

    async def build_env(self, conversation_id: int, agent: Agent) -> Any:
        if self._env_builder is None:
            return None
        target = {
            "conversationId": conversation_id,
            "cwd": agent.cwd,
            "read": self.document_reader(),
        }
        try:
            value = self._call_builder(target, conversation_id)
            if inspect.isawaitable(value):
                value = await value
            return value
        except BaseException as error:  # noqa: BLE001 - a host callback failure is reported
            self.report(error, {"type": "host_error", "callback": "env"})
            return None

    def _call_builder(self, target: Mapping[str, Any], conversation_id: int) -> Any:
        builder = self._env_builder
        assert builder is not None
        try:
            signature = inspect.signature(builder)  # type: ignore[arg-type]
            positional = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
            ]
        except (TypeError, ValueError):
            positional = []
        if len(positional) >= 2:
            return builder(target, {"conversationId": conversation_id})  # type: ignore[call-arg]
        return builder(target)  # type: ignore[call-arg]

    async def _scheduler_env(self, conversation_id: int) -> Any:
        agent = await self.resolve_agent(conversation_id)
        return await self.build_env(conversation_id, agent)

    async def complete(self, agent: Agent, request: ModelRequest) -> Any:
        model = agent.model or {}
        provider = str(model.get("provider", ""))
        model_id = str(model.get("modelId", ""))
        selected = resolve_model(self.models, provider, model_id)
        result: Any
        if hasattr(selected, "complete"):
            result = selected.complete(request)
        elif callable(selected):
            result = selected(request)
        else:
            raise TypeError("Model must be callable or expose complete()")
        if inspect.isawaitable(result):
            result = await result
        return result

    # ── Agent ──────────────────────────────────────────────────────────────

    async def resolve_agent(self, conversation_id: int) -> Agent:
        state = await read_agent_state(self, conversation_id)
        return resolve_agent(self.registry.snapshot(), state, self.settings)

    async def provider_session_id(self, conversation_id: int) -> str:
        state = await self.snapshot(ProviderDoc, conversation_id)
        if state is None:
            async def create(tx: Any) -> None:
                await tx.doc(ProviderDoc, conversation_id)

            await self.commit_with(create, TransactionScope(conversation_id=conversation_id))
            state = await self.snapshot(ProviderDoc, conversation_id)
        return str((state or {}).get("sessionId") or "")

    # ── Built-in creation hook ─────────────────────────────────────────────

    async def conversation_created(self, tx: Any, record: Mapping[str, Any]) -> None:
        conversation_id = record["id"]
        await tx.doc(LiveDoc, conversation_id)
        await tx.doc(InboxDoc, conversation_id)
        await tx.doc(UsageDoc, conversation_id)
        await tx.doc(ProviderDoc, conversation_id)
        agent = await tx.doc(AgentDoc, conversation_id)
        if record.get("parent") is not None:
            # The fork copy (asOf) is already staged; an absent parent agent stays absent
            # unless acquisition created the empty default above.
            return
        owner = record.get("owner")
        if owner is not None:
            state = await self._stored_agent_state(owner["conversationId"])
            if state:
                agent.update(state)
        # An ownerless conversation keeps the empty {} document created by tx.doc().

    async def _stored_agent_state(self, conversation_id: int) -> dict[str, Any]:
        address = {
            "kind": AgentDoc.kind,
            "scope": {"kind": "conversation", "conversationId": conversation_id},
        }
        record = await self.storage.find_document(address, "current")
        if record is None:
            return {}
        stored = await self.storage.document(record["id"], "current")
        if stored is None:
            return {}
        return materialize_document(AgentDoc.definition, stored)

    # ── Conversations ──────────────────────────────────────────────────────

    async def root(
        self,
        context: Any = None,
        options: Mapping[str, Any] | None = None,
        *,
        agent: Mapping[str, Any] | None = None,
        init: Callable[[Any, int], Any] | None = None,
    ) -> Conversation:
        options = {**(options or {})}
        if agent is not None:
            options["agent"] = agent
        if init is not None:
            options["init"] = init

        async def run(tx: Any) -> dict[str, Any]:
            existing = await tx.conversation(1)
            if existing is not None:
                return existing
            record = await tx.create_root_conversation()
            if options.get("agent") is not None:
                await configure(tx, 1, options["agent"])
            init = options.get("init")
            if init is not None:
                result = init(tx, 1)
                if inspect.isawaitable(result):
                    await result
            return record

        record = await self.commit_with(run)
        return Conversation(self, record["id"])

    async def conversation(self, conversation_id: int, context: Any = None) -> Conversation | None:
        record = await self.commit_with(lambda tx: tx.conversation(conversation_id))
        return None if record is None else Conversation(self, conversation_id)

    async def create_conversation(self, options: Mapping[str, Any], context: Any = None) -> Conversation:
        return await self._create_conversation(options)

    async def _create_conversation(
        self,
        options: Mapping[str, Any],
        *,
        parent_conversation_id: int | None = None,
        at: int | None = None,
    ) -> Conversation:
        if "ownership" not in options:
            raise TypeError("Conversation creation requires explicit ownership")

        async def run(tx: Any) -> dict[str, Any]:
            if parent_conversation_id is None:
                record = await tx.create_conversation({"ownership": options["ownership"]})
            else:
                if at is None:
                    raise TypeError("Forking requires an entry ID")
                record = await tx.fork_conversation(
                    parent_conversation_id, int(at), {"ownership": options["ownership"]}
                )
            if options.get("agent") is not None:
                await configure(tx, record["id"], options["agent"])
            init = options.get("init")
            if init is not None:
                result = init(tx, record["id"])
                if inspect.isawaitable(result):
                    await result
            return record

        record = await self.commit_with(run)
        return Conversation(self, record["id"])

    # ── Submission admission ───────────────────────────────────────────────

    async def _submit(self, conversation_id: int, draft: Mapping[str, Any]) -> Submission:
        kind = draft.get("type")
        if kind not in (INPUT, WRITE):
            raise TypeError("Submission type must be 'input' or 'write'")
        when_busy = draft.get("whenBusy") or "followUp"
        if when_busy not in ("steer", "followUp", "reject"):
            raise ValueError(f"Unknown whenBusy mode {when_busy!r}")
        if kind == INPUT:
            if "content" not in draft:
                raise TypeError("An input submission requires content")
            custom = draft.get("entry")
            if custom is not None and (not isinstance(custom, dict) or not custom.get("kind")):
                raise TypeError("Input entry must be {kind, data?}")
            if custom is not None and str(custom["kind"]).startswith("pi."):
                raise ValueError("A custom input entry kind must not start with 'pi.'")
        else:
            if not isinstance(draft.get("entry"), dict):
                raise TypeError("A write submission requires an entry draft")
        request_id = draft.get("requestId")

        async def run(tx: Any) -> dict[str, Any]:
            if request_id is not None:
                existing = await tx.submission_by_request(conversation_id, request_id)
                if existing is not None:
                    return existing
            live = await tx.doc(LiveDoc, conversation_id)
            run_state = live.get("run")
            busy = False
            if run_state:
                task = await tx.task(run_state["taskId"])
                busy = task is not None and task["state"]["status"] != "terminal"
                if not busy:
                    live.pop("run", None)
            inbox = await tx.doc(InboxDoc, conversation_id)
            inbox_waiting = bool(inbox.get("items"))
            active_start = None
            if inbox_waiting or (
                kind == WRITE and draft["entry"].get("head") not in (None, "self")
            ):
                # Read the active range on the Session line, before the first table write.
                marker = await tx.latest_head_marker(conversation_id)
                active_start = marker["head"] if marker is not None else None
            record = await tx.create_submission(
                queued_submission(
                    conversation_id=conversation_id,
                    draft=draft,
                    request_id=request_id,
                )
            )
            if kind == INPUT:
                mode = STEER if when_busy == "steer" else FOLLOW_UP
                if busy:
                    if when_busy == "reject":
                        raise ConversationBusy(conversation_id)
                    inbox["items"].append(
                        input_item(
                            {
                                **record,
                                "content": draft.get("content"),
                                "inputEntry": draft.get("entry"),
                            },
                            mode,
                        )
                    )
                    return record
                if inbox_waiting:
                    # An idle conversation with a non-empty inbox queues the new input
                    # behind the waiting items and runs a final boundary in this commit.
                    inbox["items"].append(
                        input_item(
                            {
                                **record,
                                "content": draft.get("content"),
                                "inputEntry": draft.get("entry"),
                            },
                            mode,
                        )
                    )
                    await self._start_boundary_run(
                        tx, conversation_id, live, active_start=active_start
                    )
                    return record
                entry = await tx.append_entry(conversation_id, self._input_entry(conversation_id, draft))
                tx.place_submission(record["id"], entry["id"])
                task_id = await tx.create_task(
                    GenerationTask,
                    {"submissionIds": [record["id"]]},
                    TaskOptions(ownership={"kind": "conversation"}),
                )
                live["run"] = {"taskId": task_id, "inputs": [record["id"]]}
                return record
            if busy:
                inbox["items"].append(write_item({**record, "entry": draft["entry"]}))
                return record
            if inbox_waiting:
                inbox["items"].append(write_item({**record, "entry": draft["entry"]}))
                await self._start_boundary_run(
                    tx, conversation_id, live, active_start=active_start
                )
                return record
            head = draft["entry"].get("head")
            if (
                head is not None
                and head != "self"
                and active_start is not None
                and int(head) < active_start
            ):
                tx.settle_submission(record["id"], {"status": "unanswered", "reason": "stale"})
                return record
            entry = await tx.append_entry(conversation_id, copy_json(draft["entry"]))
            tx.place_submission(record["id"], entry["id"])
            return record

        record = await self.commit_with(run, TransactionScope(conversation_id=conversation_id))
        # Submitting enables scheduling, including for work queued behind a run.
        self.resume()
        return Submission(self, record["id"])

    async def _start_boundary_run(
        self, tx: Any, conversation_id: int, live: Any, active_start: int | None = None
    ) -> None:
        """Apply a final boundary and create the successor run it selects, in this commit."""
        boundary = await apply_boundary(
            tx,
            conversation_id=conversation_id,
            final=True,
            settings=self.settings,
            now=self.now(),
            active_start=active_start,
        )
        if boundary["inputs"]:
            task_id = await tx.create_task(
                GenerationTask,
                {"submissionIds": boundary["inputs"]},
                TaskOptions(ownership={"kind": "conversation"}),
            )
            live["run"] = {"taskId": task_id, "inputs": boundary["inputs"]}

    def _input_entry(self, conversation_id: int, draft: Mapping[str, Any]) -> dict[str, Any]:
        custom = draft.get("entry")
        kind = custom["kind"] if custom else "pi.user"
        entry: dict[str, Any] = {
            "kind": kind,
            "model": [user_message(copy_json(draft.get("content")), timestamp=self.now())],
        }
        if custom is not None and custom.get("data") is not None:
            entry["data"] = copy_json(custom["data"])
        return entry

    # ── Submissions ────────────────────────────────────────────────────────

    async def submission_record(self, submission_id: int) -> dict[str, Any] | None:
        return await self.storage.submission(submission_id)

    async def submission(self, submission_id: int, context: Any = None) -> Submission | None:
        record = await self.submission_record(submission_id)
        return None if record is None else Submission(self, submission_id)

    async def wait_for_submission_change(self, submission_id: int) -> None:
        event = asyncio.Event()

        def listener(publication: Mapping[str, Any]) -> None:
            for change in publication["changes"]:
                if change["type"] == "submission" and change["value"]["id"] == submission_id:
                    event.set()
                    return

        unsubscribe = self.subscribe_commits(listener)
        try:
            await event.wait()
        finally:
            unsubscribe()

    async def abort_submission(self, submission_id: int, context: Any = None) -> str:
        record = await self.storage.submission(submission_id)
        if record is None:
            return "not_found"
        if record["status"] in ("done", "unanswered"):
            return "settled"
        if record["status"] == "placed":
            return "already_placed"
        conversation_id = record["conversationId"]

        async def run(tx: Any) -> None:
            inbox = await tx.doc(InboxDoc, conversation_id)
            inbox["items"] = [item for item in inbox["items"] if item["id"] != submission_id]
            tx.settle_submission(submission_id, {"status": "unanswered", "reason": "aborted"})

        await self.commit_with(run, TransactionScope(conversation_id=conversation_id))
        self.scheduler.notify()
        return "aborted"

    async def active_start(self, conversation_id: int) -> int | None:
        marker = await self.storage.find_latest_head_marker(conversation_id, None)
        return None if marker is None else int(marker["head"])

    async def placed_input_ids(self, conversation_id: int) -> list[int]:
        records = await self._scan_submissions(conversation_id, "placed")
        return [record["id"] for record in records if record["type"] == "input"]

    async def _scan_submissions(self, conversation_id: int, status: str | None = None) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        cursor = None
        while True:
            page = await self.storage.scan_submissions(
                {"conversationId": conversation_id, **({"status": status} if status else {})}, 256, cursor
            )
            records.extend(page["items"])
            cursor = page.get("next")
            if cursor is None:
                return records

    # ── Abort / idle ───────────────────────────────────────────────────────

    async def _abort_conversation(self, conversation_id: int, *, background: bool = False) -> None:
        async def run(tx: Any) -> None:
            await withdraw_queued_inputs(tx, conversation_id, reason="aborted")

        await self.commit_with(run, TransactionScope(conversation_id=conversation_id))
        self.scheduler.resume()
        await self.scheduler.abort_scope(conversation_id, background=background)
        await self.scheduler.wait_for_idle(conversation_id)

    def resume(self) -> None:
        self.scheduler.resume()

    def wake(self) -> dict[str, Any]:
        return self.scheduler.wake()

    async def get_task(self, task_id: int, context: Any = None) -> dict[str, Any] | None:
        return await self.scheduler.get_task(task_id)

    async def wait_for_task(self, task_id: int, context: Any = None) -> dict[str, Any]:
        self.resume()
        return await self.scheduler.wait_for_task(task_id)

    async def wait_for_idle(self, context: Any = None) -> None:
        self.resume()
        await self.scheduler.wait_for_idle(None)

    async def abort_task(self, task_id: int, context: Any = None) -> str:
        self.resume()
        return await self.scheduler.abort_task(task_id)

    async def retry_task(self, task_id: int, context: Any = None) -> None:
        self.resume()
        await self.scheduler.retry_task(task_id)

    async def inspect(self, context: Any = None) -> HarnessInspection:
        tasks = await self.scheduler.inspect_tasks()
        submissions: list[dict[str, Any]] = []
        for status in ("queued", "placed"):
            submissions.extend(await self._scan_submissions_all(status))
        submissions.sort(key=lambda record: record["id"])
        scheduling = "closing" if self.scheduler._closing or self.scheduler._closed else (
            "running" if self.scheduler._running else "paused"
        )
        return HarnessInspection(scheduling=scheduling, tasks=tasks, submissions=submissions)

    async def _scan_submissions_all(self, status: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        cursor = None
        while True:
            page = await self.storage.scan_submissions({"status": status}, 256, cursor)
            records.extend(page["items"])
            cursor = page.get("next")
            if cursor is None:
                return records

    async def usage(self, context: Any = None) -> dict[str, Any]:
        total = {"input": 0, "output": 0, "total": 0, "cost": 0.0}
        conversations: list[dict[str, Any]] = []
        cursor = None
        while True:
            page = await self.storage.scan_conversations({}, 256, cursor)
            conversations.extend(page["items"])
            cursor = page.get("next")
            if cursor is None:
                break
        for conversation in conversations:
            state = await self.snapshot(UsageDoc, conversation["id"])
            if state is None:
                continue
            for bucket in (state.get("models") or {}).values():
                add_usage(total, bucket)
            for bucket in (state.get("tools") or {}).values():
                add_usage(total, bucket)
        return total

    async def task_graph(self, context: Any = None) -> dict[str, Any]:
        """Every live task as one structural graph, keyed by decimal task ID."""
        inspection = await self.inspect()
        tasks: dict[str, Any] = {}
        for entry in inspection.tasks:
            record = entry["record"]
            status = record["state"]["status"]
            checkpoint = record["state"].get("checkpoint") or {}
            phase = checkpoint.get("phase")
            if status in ("pending", "running"):
                state: dict[str, Any] = {"status": status, "phase": phase}
            elif status == "waiting":
                state = {
                    "status": "waiting",
                    "phase": phase,
                    "on": list(record["state"].get("on") or []),
                    "policy": record["state"].get("policy", "allSettled"),
                }
            elif status == "sleeping":
                state = {"status": "sleeping", "phase": phase, "until": int(record["state"]["until"])}
            elif status == "completing":
                state = {"status": "completing", "outcome": record["state"]["outcome"]["status"]}
            else:
                continue
            node: dict[str, Any] = {
                "id": record["id"],
                "kind": record["kind"],
                "conversationId": record["conversationId"],
                "background": bool(record.get("background")),
                "abortRequested": bool(record.get("abortRequested")),
                "state": state,
                "conversations": await self._owned_conversation_ids(record["id"]),
            }
            if record.get("owner") is not None:
                node["owner"] = record["owner"]
            if entry["state"]["kind"] == "blocked":
                node["blocked"] = entry["state"]["reason"]
            tasks[str(record["id"])] = node
        return {"tasks": tasks}

    async def _owned_conversation_ids(self, task_id: int) -> list[int]:
        ids: list[int] = []
        cursor = None
        while True:
            page = await self.storage.scan_conversations({"ownerTaskId": task_id}, 256, cursor)
            ids.extend(record["id"] for record in page["items"])
            cursor = page.get("next")
            if cursor is None:
                return sorted(ids)

    # ── View ───────────────────────────────────────────────────────────────

    async def _view(self, conversation_id: int) -> dict[str, Any]:
        context = await self.read_context(conversation_id)
        live = await self.snapshot(LiveDoc, conversation_id) or {}
        inbox_state = await self.snapshot(InboxDoc, conversation_id) or {"items": []}
        usage = await self.snapshot(UsageDoc, conversation_id) or {}
        agent_state = await read_agent_state(self, conversation_id)
        return {
            "id": conversation_id,
            "head": context["head"],
            "entries": context["entries"],
            "messages": context["messages"],
            "live": live,
            "inbox": inbox_state.get("items", []),
            "usage": usage,
            "agent": agent_state,
            "busy": bool(live.get("run")),
        }

    async def _runtime_conversation(self, conversation_id: int, runtime: Any) -> Conversation | None:
        record = await self.storage.conversation(conversation_id)
        return None if record is None else Conversation(self, conversation_id)

    # ── Scheduler-written outcome cleanup ──────────────────────────────────

    async def _settle_scheduler_outcome(
        self, tx: Any, record: Mapping[str, Any], outcome: Mapping[str, Any]
    ) -> None:
        """Clean up the effects a faulted or orphaned built-in task can no longer settle."""
        conversation_id = record["conversationId"]
        kind = record["kind"]
        if kind == ToolTask.definition.name:
            live = await tx.doc(LiveDoc, conversation_id)
            for slot in live.get("tools") or []:
                if slot.get("taskId") == record["id"]:
                    slot["status"] = "done"
            live.pop("nestedTools", None)
            return
        if kind != GenerationTask.definition.name:
            return
        live = await tx.doc(LiveDoc, conversation_id)
        run = live.get("run")
        if not run or run.get("taskId") != record["id"]:
            return
        faulted = outcome.get("status") == "faulted"
        reason = "faulted" if faulted else str(outcome.get("reason") or "orphaned")
        detail = outcome.get("error", {}).get("message") if faulted else None
        for submission_id in run.get("inputs") or run.get("submissionIds") or []:
            settlement: dict[str, Any] = {"status": "unanswered", "reason": reason}
            if detail is not None:
                settlement["detail"] = {"message": detail}
            tx.settle_submission(submission_id, settlement)
        live.pop("run", None)
        live.pop("generation", None)
        live.pop("tools", None)

    # ── Close ──────────────────────────────────────────────────────────────

    async def _before_close(self) -> None:
        await self.scheduler.shutdown()

    async def read_context(self, conversation_id: int, at: int | None = None) -> dict[str, Any]:
        return await read_context(self, conversation_id, at)


__all__ = [
    "Conversation",
    "DocumentReader",
    "Harness",
    "HarnessInspection",
    "create_registry",
    "define_extension",
    "define_tool",
    "section",
]

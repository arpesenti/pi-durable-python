"""Task graph view: every live task, its waits, and the conversations it owns (spec §9.5).

The graph is to the Session's tasks what the conversation view is to one conversation: a
structural mount built on the Session line by its first observer, advanced only from committed
publications, and dropped with its last observer. A node holds the committed task record without
its input, checkpoint payload, outcome payload, or memos.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal, NotRequired, TypeAlias, TypedDict, cast

from ..context import BACKGROUND_CONTEXT, Context, without_abort_signal
from ..delta import Op
from ..ids import ConversationId, TaskId
from ..json import JsonValue
from ..session.observation import CommittedState, CommittedWatch
from ..session.session import SessionImpl, contained
from ..types import (
    CommitPublication,
    JoinPolicy,
    Storage,
    TaskOutcome,
    TaskRecord,
)
from .scheduler import BlockedReason, blocked_subject, json_equal
from .util import cancellation_error, closed_error, scan_all

LIVE_STATUSES: tuple[Literal["pending", "running", "waiting", "sleeping", "completing"], ...] = (
    "pending",
    "running",
    "waiting",
    "sleeping",
    "completing",
)
"""Durable statuses of a live task; terminal tasks leave the graph."""

SCAN_PAGE_SIZE = 256


class TaskGraphStatePending(TypedDict):
    status: Literal["pending", "running"]
    phase: str


class TaskGraphStateWaiting(TypedDict):
    status: Literal["waiting"]
    phase: str
    on: list[TaskId]
    policy: JoinPolicy


class TaskGraphStateSleeping(TypedDict):
    """Asleep until the Harness clock reaches ``until`` (spec §5.6)."""

    status: Literal["sleeping"]
    phase: str
    until: int


class TaskGraphStateCompleting(TypedDict):
    """Outcome held until its ordinary owned work drains (spec §5.5)."""

    status: Literal["completing"]
    outcome: str


TaskGraphState = (
    TaskGraphStatePending | TaskGraphStateWaiting | TaskGraphStateSleeping | TaskGraphStateCompleting
)
"""One live task's durable state without its checkpoint or outcome payloads."""


class TaskGraphNode(TypedDict):
    id: TaskId
    kind: str
    conversationId: ConversationId
    owner: NotRequired[TaskId]
    background: bool
    abortRequested: bool
    state: TaskGraphState
    blocked: NotRequired[str]
    """Why it cannot run here, when it cannot; follows registry changes (spec §5.4)."""

    conversations: list[ConversationId]


class TaskGraph(TypedDict):
    """Every live task of the Session, keyed by its decimal ID."""

    tasks: dict[str, TaskGraphNode]


TaskGraphWatch: TypeAlias = CommittedWatch
"""A serialized exact-frame watch of the task graph."""


@dataclass(slots=True)
class GraphMount:
    value: TaskGraph
    observers: set[Any] = field(default_factory=set)
    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    """What the blocked reason reads of each live task, so a registry change derives it again."""


class TaskGraphView:
    """The Harness's single task-graph mount, alive while observed."""

    def __init__(
        self,
        session: SessionImpl,
        storage: Storage,
        *,
        blocked: Callable[[dict[str, Any]], BlockedReason | None] | None = None,
        subscribe_blocked: Callable[[Callable[[], None]], Callable[[], None]] | None = None,
    ) -> None:
        self._session = session
        self._storage = storage
        self._blocked: Callable[[dict[str, Any]], BlockedReason | None] = blocked or (
            lambda _record: None
        )
        self._mount: GraphMount | None = None
        self._closed = False
        session.observe_commits(self._on_publication)
        if subscribe_blocked is not None:
            subscribe_blocked(self._refresh_blocked)
        session.subscribe_close(self._on_close)

    def report(self, error: BaseException) -> None:
        self._session.report(error)

    async def state(self, context: Context) -> CommittedState:
        """A disposable read-only state of the graph."""

        return await self._attach(_state_observer, context)

    async def watch(self, context: Context) -> CommittedWatch:
        """A serialized exact-frame watch of the graph; cancelling ``context`` stops it."""

        observer = await self._attach(_watch_observer, context)
        signal = context.abort_signal
        if signal is not None and signal.aborted:
            observer.cancel()
            raise cancellation_error(signal)
        if signal is not None:
            observer.observe_cancellation(signal)
        return observer

    async def _attach(
        self,
        create: Callable[[TaskGraph, Callable[[], None], TaskGraphView], Any],
        context: Context,
    ) -> Any:
        async def job() -> Any:
            self._session.assert_usable()
            mount = self._mount
            if mount is None:
                value, records = await self._build(context)
                mount = GraphMount(value=value, records=records)
            detached = False

            def detach() -> None:
                nonlocal detached
                if detached:
                    return
                detached = True
                mount.observers.discard(observer)
                if not mount.observers and self._mount is mount:
                    self._mount = None

            observer = create(mount.value, detach, self)
            if asyncio.iscoroutine(observer):
                observer = await observer
            if self._closed:
                raise closed_error(self._session)
            self._session.assert_usable()
            signal = context.abort_signal
            if signal is not None and signal.aborted:
                raise cancellation_error(signal)
            self._mount = mount
            mount.observers.add(observer)
            return observer

        return await self._session.read_on_line(job)

    def _refresh_blocked(self) -> None:
        """On the Session line: derive every node's blocked reason again, and publish the changes."""

        if self._mount is None or self._closed:
            return

        async def job() -> None:
            mount = self._mount
            if mount is None:
                return
            ops: list[Op] = []
            for key, record in mount.records.items():
                blocked = self._blocked(record)
                current = mount.value["tasks"].get(key)
                if current is None or current.get("blocked") == blocked:
                    continue
                if blocked is None:
                    ops.append(["d", ["tasks", key, "blocked"]])
                else:
                    ops.append(["s", ["tasks", key, "blocked"], blocked])
            self._publish_blocked(ops)

        try:
            task = asyncio.ensure_future(self._session.read_on_line(job))
        except RuntimeError:
            return
        # Rejected only once the Session is closing or failed.
        task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)

    def _publish_blocked(self, ops: list[Op]) -> None:
        mount = self._mount
        if mount is None or not ops:
            return
        tasks = dict(mount.value["tasks"])
        for op in ops:
            key = op[1][1]
            current = tasks.get(key)
            if current is None:
                continue
            if op[0] == "d":
                following = dict(current)
                following.pop("blocked", None)
                tasks[key] = cast(TaskGraphNode, following)
            else:
                tasks[key] = cast(TaskGraphNode, {**current, "blocked": op[2]})
        mount.value = {"tasks": tasks}
        frame_context = without_abort_signal(BACKGROUND_CONTEXT)
        for observer in list(mount.observers):
            advance = getattr(observer, "advance_frame", None) or observer.advance
            contained(partial(advance, mount.value, ops, frame_context), self.report)

    async def _build(self, context: Context) -> tuple[TaskGraph, dict[str, dict[str, Any]]]:
        records: list[TaskRecord] = []
        for status in LIVE_STATUSES:
            records.extend(
                await scan_all(
                    partial(
                        self._storage.scan_tasks,
                        {"status": status},
                        SCAN_PAGE_SIZE,
                        context=context,
                    )
                )
            )
        records.sort(key=lambda record: record["id"])
        tasks: dict[str, TaskGraphNode] = {}
        live: dict[str, dict[str, Any]] = {}
        for record in records:
            owned = await scan_all(
                partial(
                    self._storage.scan_conversations,
                    {"ownerTaskId": record["id"]},
                    SCAN_PAGE_SIZE,
                    context=context,
                )
            )
            conversations = sorted(conversation["id"] for conversation in owned)
            subject = blocked_subject(record)
            tasks[str(record["id"])] = _node_of(
                record, conversations, self._blocked(subject)
            )
            live[str(record["id"])] = subject
        return {"tasks": tasks}, live

    def _on_publication(self, publication: CommitPublication, context: Context) -> None:
        if self._mount is not None:
            advance_graph(self._mount, publication, context, self.report, self._blocked)

    def _on_close(self) -> None:
        self._closed = True
        failure = self._session.failure
        mount = self._mount
        if mount is not None:
            for observer in list(mount.observers):
                contained(partial(observer.close_session, failure), self.report)
        self._mount = None


def _state_observer(
    value: TaskGraph, release: Callable[[], None], view: TaskGraphView
) -> CommittedState:
    return CommittedState(value, release, view.report)


def _watch_observer(
    value: TaskGraph, release: Callable[[], None], view: TaskGraphView
) -> CommittedWatch:
    return CommittedWatch(value, release, view.report)


def advance_graph(
    mount: GraphMount,
    publication: CommitPublication,
    context: Context,
    report: Callable[[BaseException], None],
    blocked: Callable[[dict[str, Any]], BlockedReason | None],
) -> None:
    """Derive the mount's operations from one publication and publish the new revision."""

    ops: list[Op] = []
    changed: dict[str, TaskGraphNode | None] = {}

    def node(key: str) -> TaskGraphNode | None:
        if key in changed:
            return changed[key]
        return mount.value["tasks"].get(key)

    for change in publication["changes"]:
        if change["type"] != "task":
            continue
        record = change["value"]
        key = str(record["id"])
        previous = node(key)
        if record["state"]["status"] == "terminal":
            mount.records.pop(key, None)
            if previous is None:
                continue
            ops.append(["d", ["tasks", key]])
            changed[key] = None
            continue
        subject = blocked_subject(record)
        mount.records[key] = subject
        next_node = _node_of(
            record,
            previous["conversations"] if previous is not None else [],
            blocked(subject),
        )
        if previous is not None and json_equal(next_node, previous):
            continue
        ops.append(["s", ["tasks", key], next_node])
        changed[key] = next_node

    # After the tasks, so a conversation created with its owner task in one commit finds the owner's node.
    created: dict[str, list[ConversationId]] = {}
    for change in publication["changes"]:
        if change["type"] != "conversation":
            continue
        owner = change["value"].get("owner")
        if owner is None:
            continue
        key = str(owner["taskId"])
        if node(key) is not None:
            created.setdefault(key, []).append(change["value"]["id"])
    for key, ids in created.items():
        current = node(key)
        assert current is not None
        conversations = sorted([*current["conversations"], *ids])
        ops.append(["s", ["tasks", key, "conversations"], conversations])
        changed[key] = {**current, "conversations": conversations}

    if not ops:
        return
    tasks = dict(mount.value["tasks"])
    for op in ops:
        path = op[1]
        if op[0] == "d":
            tasks.pop(path[1], None)
        elif len(path) == 2:
            tasks[path[1]] = op[2]
        else:
            current = tasks[path[1]]
            tasks[path[1]] = {**current, "conversations": op[2]}
    mount.value = {"tasks": tasks}
    frame_context = without_abort_signal(context)
    for observer in list(mount.observers):
        advance = getattr(observer, "advance_frame", None) or observer.advance
        contained(partial(advance, mount.value, ops, frame_context), report)


def _node_of(
    record: TaskRecord,
    conversations: list[ConversationId],
    blocked: BlockedReason | None = None,
) -> TaskGraphNode:
    node = TaskGraphNode(
        id=record["id"],
        kind=record["kind"],
        conversationId=record["conversationId"],
        background=record["background"],
        abortRequested=record["abortRequested"],
        state=_state_of(record),
        conversations=list(conversations),
    )
    owner = record.get("owner")
    if owner is not None:
        node["owner"] = owner
    if blocked is not None:
        node["blocked"] = blocked
    return node


def _state_of(record: TaskRecord) -> TaskGraphState:
    state = cast(dict[str, Any], record["state"])
    status = state["status"]
    if status in ("pending", "running"):
        return {"status": status, "phase": _phase_of(state["checkpoint"])}
    if status == "waiting":
        return {
            "status": "waiting",
            "phase": _phase_of(state["checkpoint"]),
            "on": list(state["on"]),
            "policy": state["policy"],
        }
    if status == "sleeping":
        return {
            "status": "sleeping",
            "phase": _phase_of(state["checkpoint"]),
            "until": state["until"],
        }
    outcome = _outcome_status(state["outcome"])
    return {"status": "completing", "outcome": outcome}


def _phase_of(checkpoint: JsonValue) -> str:
    return str(cast(dict[str, Any], checkpoint)["phase"])


def _outcome_status(outcome: TaskOutcome) -> str:
    return outcome["status"]


__all__ = [
    "LIVE_STATUSES",
    "TaskGraph",
    "TaskGraphNode",
    "TaskGraphState",
    "TaskGraphView",
    "TaskGraphWatch",
    "advance_graph",
]

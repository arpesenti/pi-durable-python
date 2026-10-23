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

from ..context import Context, without_abort_signal
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
from .scheduler import json_equal
from .util import cancellation_error, closed_error, scan_all

LIVE_STATUSES: tuple[Literal["pending", "running", "waiting", "completing"], ...] = (
    "pending",
    "running",
    "waiting",
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


class TaskGraphStateCompleting(TypedDict):
    """Outcome held until its ordinary owned work drains (spec §5.5)."""

    status: Literal["completing"]
    outcome: str


TaskGraphState = TaskGraphStatePending | TaskGraphStateWaiting | TaskGraphStateCompleting
"""One live task's durable state without its checkpoint or outcome payloads."""


class TaskGraphNode(TypedDict):
    id: TaskId
    kind: str
    conversationId: ConversationId
    owner: NotRequired[TaskId]
    background: bool
    abortRequested: bool
    state: TaskGraphState
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


class TaskGraphView:
    """The Harness's single task-graph mount, alive while observed."""

    def __init__(self, session: SessionImpl, storage: Storage) -> None:
        self._session = session
        self._storage = storage
        self._mount: GraphMount | None = None
        self._closed = False
        session.observe_commits(self._on_publication)
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
                mount = GraphMount(value=await self._build(context))
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

    async def _build(self, context: Context) -> TaskGraph:
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
            tasks[str(record["id"])] = _node_of(record, conversations)
        return {"tasks": tasks}

    def _on_publication(self, publication: CommitPublication, context: Context) -> None:
        if self._mount is not None:
            advance_graph(self._mount, publication, context, self.report)

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
            if previous is None:
                continue
            ops.append(["d", ["tasks", key]])
            changed[key] = None
            continue
        next_node = _node_of(record, previous["conversations"] if previous is not None else [])
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


def _node_of(record: TaskRecord, conversations: list[ConversationId]) -> TaskGraphNode:
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

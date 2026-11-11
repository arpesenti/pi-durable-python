"""What a renderer reads of a conversation: every ``pi.live`` value it publishes, and nested results.

A renderer renders state. These helpers attach the synchronous observers of spec §9.1 and §9.3 and
hold every value they are handed, unfolded, so a test can ask when a run appeared, when a slot ended,
and what a nested call returned — the questions the deleted agent event stream used to answer.
"""

from __future__ import annotations

from typing import Any

from session_support import context, document_changes, observe_commits

from pi_durable import NestedCallDoc
from pi_durable.ids import TaskId


class _Recorder:
    """An :class:`~pi_durable.types.Observer` that keeps every value it is handed."""

    def __init__(self) -> None:
        self.values: list[Any] = []
        self.frames: list[dict[str, Any]] = []
        self.ends: list[Any] = []

    def advance(self, call: Any, _context: Any = None) -> None:
        if call["kind"] == "value":
            self.values.append(call["value"])
            self.frames.append({"value": call["value"], "ops": list(call["ops"])})

    def end(self, end: Any) -> None:
        self.ends.append(end)


async def view_history(harness: Any, conversation_id: Any) -> dict[str, Any]:
    """Every view of ``conversation_id`` from now on, one per commit that changes it, unfolded.

    A synchronous observer, so no transition is skipped: the frames a renderer that keeps up gets.
    ``live()`` lists their ``pi.live`` values, in the same order as ``views``.
    """

    recorder = _Recorder()
    observation = await harness.observe_conversation(conversation_id, recorder, context)
    # The acquisition revision is the first value the observer would have been handed.
    recorder.values.insert(0, observation.value)
    recorder.frames.insert(0, {"value": observation.value, "ops": []})
    return {
        "views": recorder.values,
        "frames": recorder.frames,
        "live": lambda: [dict(view["docs"].get("pi.live") or {}) for view in recorder.values],
        "detach": observation.detach,
    }


def slots_of(history: list[dict[str, Any]], call_id: str) -> list[dict[str, Any] | None]:
    """The slot of tool call ``call_id``, model-issued or nested, in each ``pi.live`` value."""

    return [
        next(
            (
                slot
                for slot in [*(state.get("tools") or ()), *(state.get("nestedTools") or ())]
                if slot["callId"] == call_id
            ),
            None,
        )
        for state in history
    ]


def ended_at(history: list[dict[str, Any]], call_id: str) -> int:
    """The index of the first value where call ``call_id`` ended: done, or gone after being listed."""

    slots = slots_of(history, call_id)
    listed = next((index for index, slot in enumerate(slots) if slot is not None), -1)
    if listed < 0:
        return -1
    return next(
        (
            index
            for index, slot in enumerate(slots)
            if index >= listed and (slot is None or slot["status"] == "done")
        ),
        -1,
    )


def start_order(history: list[dict[str, Any]]) -> list[str]:
    """The calls whose slot went ``running``, in the order they started."""

    started: list[str] = []
    for state in history:
        for slot in [*(state.get("tools") or ()), *(state.get("nestedTools") or ())]:
            if slot["status"] == "running" and slot["callId"] not in started:
                started.append(slot["callId"])
    return started


def nested_results(harness: Any) -> dict[TaskId, Any]:
    """The result of every nested call from now on, by its tool task, read from its ``NestedCallDoc``.

    The place a client reads a live caller's nested results from (spec §7.3): the document its
    terminal commit writes.
    """

    results: dict[TaskId, Any] = {}

    def observer(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] != NestedCallDoc.definition.kind or change.get("kind") == "retire":
                continue
            state = change.get("value") or {}
            if state.get("taskId") is not None and state.get("result") is not None:
                results[TaskId(state["taskId"])] = state["result"]

    observe_commits(harness, observer)
    return results


def nested_result_of(
    history: list[dict[str, Any]],
    results: dict[TaskId, Any],
    call_id: str,
) -> Any:
    """The result of nested call ``call_id``, found by the task its slot names."""

    slot = next((each for each in slots_of(history, call_id) if each and each.get("taskId")), None)
    return None if slot is None else results.get(slot["taskId"])


__all__ = [
    "ended_at",
    "nested_result_of",
    "nested_results",
    "slots_of",
    "start_order",
    "view_history",
]

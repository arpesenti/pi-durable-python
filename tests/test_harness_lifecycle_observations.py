"""Observation cases of the upstream ``harness-lifecycle`` suite.

Covers the lifecycle promises the observation surfaces own: no frame from a commit that settles
during close, queued acquisitions that reject at the seal, and read-only viewers that never enable
scheduling.
"""

from __future__ import annotations

import asyncio
from typing import Any

from session_support import ControlledStorage, Mirror, context, flush
from task_support import completed, open_tasks

from pi_durable import MemoryStorage, define_doc
from pi_durable.tasks import define_task

Notes = define_doc(
    {
        "kind": "test.lifecycle-notes",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"text": ""},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


class Recorder:
    """A host observer naming itself in a shared list for every call and recording its ends."""

    def __init__(self, name: str, frames: list[str]) -> None:
        self.name = name
        self.frames = frames
        self.value: Any = None
        self.ends: list[Any] = []

    def advance(self, call: Any, _context: Any = None) -> None:
        self.frames.append(self.name)
        if call["kind"] == "value" and not call["retired"]:
            self.value = call["value"]

    def end(self, end: Any) -> None:
        self.ends.append(end)


def one_step(name: str) -> Any:
    async def handler(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": handler},
            "abort": lambda _task, _runtime, _ctx: asyncio.sleep(0),
        }
    )


async def test_publishes_no_frame_to_observation_from_a_commit_that_settles_during_close() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    harness = opened["harness"]
    root = await harness.root(context)
    frames: list[str] = []

    async def before(tx: Any) -> None:
        (await tx.doc(Notes)).text = "before"

    await harness.commit(before, context)
    doc_mirror = Recorder("docObserver", frames)
    view_mirror = Recorder("viewObserver", frames)
    graph_mirror = Recorder("graphObserver", frames)
    doc_observation = await harness.observe_doc(Notes, doc_mirror, context)
    doc_watch = await harness.watch_doc(Notes, context)
    assert doc_observation is not None and doc_watch is not None
    view_observation = await harness.observe_conversation(root.id, view_mirror, context)
    view_watch = await root.watch(context)
    graph_observation = await harness.observe_task_graph(graph_mirror, context)
    graph_watch = await harness.watch_task_graph(context)
    doc_watch.start(lambda _call, _ctx: frames.append("docWatch"))
    view_watch.start(lambda _call, _ctx: frames.append("viewWatch"))
    graph_watch.start(lambda _call, _ctx: frames.append("graphWatch"))
    held = storage.hold_commits()

    async def during(tx: Any) -> None:
        (await tx.doc(Notes)).text = "during close"
        await tx.append_entry(root.id, {"kind": "note"})
        await tx.create_task(
            one_step("test.close-graph"),
            None,
            {"ownership": {"kind": "conversation"}, "conversationId": root.id},
        )

    committing = asyncio.ensure_future(harness.commit(during, context))
    await held.entered.wait()
    closing = asyncio.ensure_future(harness.close(context))
    held.release()
    await committing
    await closing
    await flush()
    assert frames == []
    assert doc_observation.value == {"text": "before"}
    assert [entry["kind"] for entry in view_observation.value["entries"]] == []
    assert graph_observation.value == {"tasks": {}}
    assert await graph_watch.closed == {"kind": "ended"}
    assert await doc_watch.closed == {"kind": "ended"}
    assert await view_watch.closed == {"kind": "ended"}
    # The commit itself settled.
    assert any(write["type"] == "entry" for write in storage.commits[-1])


async def test_queued_view_and_graph_acquisitions_reject_at_the_seal() -> None:
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    harness = opened["harness"]
    root = await harness.root(context)
    held = storage.hold_commits()

    async def blocker(tx: Any) -> None:
        await tx.append_entry(root.id, {"kind": "blocker"})

    blocking = asyncio.ensure_future(harness.commit(blocker, context))
    await held.entered.wait()
    queued = {
        "viewObserver": asyncio.ensure_future(
            harness.observe_conversation(root.id, Mirror(), context)
        ),        "watch": asyncio.ensure_future(root.watch(context)),
        "taskGraphObserver": asyncio.ensure_future(
            harness.observe_task_graph(Mirror(), context)
        ),
        "watchTaskGraph": asyncio.ensure_future(harness.watch_task_graph(context)),
    }
    await asyncio.sleep(0)
    closing = asyncio.ensure_future(harness.close(context))
    held.release()
    await blocking
    outcomes: dict[str, str] = {}
    for name, outcome in queued.items():
        try:
            await outcome
            outcomes[name] = "resolved"
        except Exception as error:
            outcomes[name] = "closed" if "closed" in str(error) else str(error)
    assert outcomes == {name: "closed" for name in queued}
    await closing


async def test_read_only_viewers_never_enable_scheduling() -> None:
    ran: list[bool] = []

    async def mark(_task: Any, runtime: Any, ctx: Any) -> None:
        ran.append(True)
        await runtime.commit(lambda: completed(None), ctx)

    Marker = define_task(
        {
            "name": "test.read-only-marker",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": mark},
            "abort": lambda _task, _runtime, _ctx: asyncio.sleep(0),
        }
    )
    opened = await open_tasks(MemoryStorage(), [Marker])
    harness = opened["harness"]
    root = await harness.root(context)
    task_id = await root.commit(
        lambda tx: tx.create_task(
            Marker, None, {"ownership": {"kind": "conversation"}, "background": True}
        ),
        context,
    )

    async def queued_write(tx: Any) -> None:
        doc = await tx.doc(Notes)
        doc["text"] = "queued"
        await tx.create_submission(
            {
                "conversationId": root.id,
                "type": "write",
                "status": "queued",
                "entry": {"kind": "note"},
            }
        )

    await harness.commit(queued_write, context)
    await harness.inspect(context)
    await harness.get_task(task_id, context)
    await harness.usage(context)
    await harness.conversation(root.id, context)
    await harness.snapshot(Notes, context)
    doc_observation = await harness.observe_doc(Notes, Mirror(), context)
    assert doc_observation is not None
    doc_observation.detach()
    doc_watch = await harness.watch_doc(Notes, context)
    assert doc_watch is not None
    await doc_watch.stop()
    await root.agent(context)
    await root.context(context)
    await root.entries({}, 10, None, context)
    view_observation = await harness.observe_conversation(root.id, Mirror(), context)
    view_observation.detach()
    view_watch = await root.watch(context)
    await view_watch.stop()
    graph_observation = await harness.observe_task_graph(Mirror(), context)
    graph_observation.detach()
    graph_watch = await harness.watch_task_graph(context)
    await graph_watch.stop()
    await flush()
    await flush()
    assert (await harness.inspect(context))["scheduling"] == "paused"
    assert ran == []
    await harness.close(context)

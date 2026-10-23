"""Observation cases of the upstream ``harness-lifecycle`` suite.

Covers the lifecycle promises the observation surfaces own: no frame from a commit that settles
during close, queued acquisitions that reject at the seal, and read-only viewers that never enable
scheduling.
"""

from __future__ import annotations

import asyncio
from typing import Any

from session_support import ControlledStorage, context, flush
from task_support import completed, open_tasks

from pi_durable import MemoryStorage, define_doc
from pi_durable.tasks import define_task

Notes = define_doc(
    {
        "kind": "test.lifecycle-notes",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"text": ""},
    }
)


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

    async def before(tx: Any) -> None:
        (await tx.doc(Notes)).text = "before"

    await harness.commit(before, context)
    doc_state = await harness.document_state(Notes, context)
    doc_watch = await harness.watch_doc(Notes, context)
    assert doc_state is not None and doc_watch is not None
    view_state = await root.view_state(context)
    view_watch = await root.watch(context)
    graph_state = await harness.task_graph(context)
    graph_watch = await harness.watch_task_graph(context)
    frames: list[str] = []

    def state_listener(_value: Any, _context: Any, delivery: Any, name: str) -> None:
        if delivery.kind == "update":
            frames.append(name)

    doc_state.subscribe(lambda value, ctx, delivery: state_listener(value, ctx, delivery, "docState"))
    view_state.subscribe(
        lambda value, ctx, delivery: state_listener(value, ctx, delivery, "viewState")
    )
    graph_state.subscribe(
        lambda value, ctx, delivery: state_listener(value, ctx, delivery, "graphState")
    )
    doc_watch.start(lambda _value, _ops, _ctx: frames.append("docWatch"))
    view_watch.start(lambda _value, _ops, _ctx: frames.append("viewWatch"))
    graph_watch.start(lambda _value, _ops, _ctx: frames.append("graphWatch"))
    doc_value = doc_state.value
    view_value = view_state.value

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
    assert doc_state.value is doc_value
    assert view_state.value is view_value
    assert graph_state.value == {"tasks": {}}
    assert await graph_watch.closed == {"reason": "session_closed"}
    assert await doc_watch.closed == {"reason": "session_closed"}
    assert await view_watch.closed == {"reason": "session_closed"}
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
        "viewState": asyncio.ensure_future(root.view_state(context)),
        "watch": asyncio.ensure_future(root.watch(context)),
        "taskGraph": asyncio.ensure_future(harness.task_graph(context)),
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
    doc_state = await harness.document_state(Notes, context)
    assert doc_state is not None
    doc_state.dispose()
    doc_watch = await harness.watch_doc(Notes, context)
    assert doc_watch is not None
    await doc_watch.stop()
    await root.agent(context)
    await root.context(context)
    await root.entries({}, 10, None, context)
    view_state = await root.view_state(context)
    view_state.dispose()
    view_watch = await root.watch(context)
    await view_watch.stop()
    graph_state = await harness.task_graph(context)
    graph_state.dispose()
    graph_watch = await harness.watch_task_graph(context)
    await graph_watch.stop()
    await flush()
    await flush()
    assert (await harness.inspect(context))["scheduling"] == "paused"
    assert ran == []
    await harness.close(context)

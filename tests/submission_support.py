"""Shared helpers for the translated submission and inbox suites.

The real generation task arrives with the run-loop ticket. These tests install a probe under the
same built-in name so admission and boundaries can run against durable task records now.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

from session_support import context
from task_support import completed, deferred, eventually, open_tasks

from pi_durable import InboxDoc, LiveDoc, MemoryStorage
from pi_durable.harness.inbox import QueueModes, apply_boundary, prepare_boundary
from pi_durable.harness.live import end_run, start_run
from pi_durable.tasks import define_task

gates: dict[Any, Any] = {}
failing: set[Any] = set()


async def _wait_gate(gate: Any, signal: Any) -> bool:
    gate_task = asyncio.ensure_future(gate.promise)
    signal_task = asyncio.ensure_future(signal.wait())
    done, pending = await asyncio.wait(
        [gate_task, signal_task], return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    return gate_task in done


async def _run(task: Any, runtime: Any, ctx: Any) -> None:
    gate = gates.setdefault(task["id"], deferred())
    if not await _wait_gate(gate, runtime.signal):
        return
    if task["id"] in failing:
        raise RuntimeError("model_error")

    async def finish(tx: Any) -> Any:
        settings = runtime.settings
        boundary = await prepare_boundary(
            tx,
            task["conversationId"],
            QueueModes(settings.steering_mode, settings.follow_up_mode),
        )
        live = await tx.doc(LiveDoc, task["conversationId"])
        entry = await tx.append_entry(
            task["conversationId"],
            {
                "kind": "pi.assistant",
                "model": [
                    {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "answer"}],
                        "api": "probe",
                        "provider": "probe",
                        "model": "probe",
                        "usage": {
                            "input": 0,
                            "output": 0,
                            "cacheRead": 0,
                            "cacheWrite": 0,
                            "totalTokens": 0,
                            "cost": {
                                "input": 0,
                                "output": 0,
                                "cacheRead": 0,
                                "cacheWrite": 0,
                                "total": 0,
                            },
                        },
                        "stopReason": "stop",
                        "timestamp": 1,
                    }
                ],
            },
        )
        end_run(tx, live, task["id"], {"status": "done", "answer": entry["id"]})
        placed = await apply_boundary(tx, boundary, "final", runtime.now())
        if placed.users:
            following = runtime.registry.task("pi.generation")
            assert following is not None
            await start_run(tx, task["conversationId"], live, placed.users, following)
        return completed(None)

    await runtime.commit(finish, ctx)


async def _abort(task: Any, runtime: Any, ctx: Any) -> None:
    async def finish(tx: Any) -> Any:
        live = await tx.doc(LiveDoc, task["conversationId"])
        end_run(tx, live, task["id"], {"status": "unanswered", "reason": "aborted"})
        return {"status": "terminal", "outcome": {"status": "aborted", "reason": "aborted"}}

    await runtime.commit(finish, ctx)


GenerationProbe = define_task(
    {
        "name": "pi.generation",
        "version": 1,
        "initial": lambda _input: {"phase": "run"},
        "phases": {"run": _run},
        "abort": _abort,
    }
)


def reset() -> None:
    gates.clear()
    failing.clear()


async def open_submissions(
    storage: Any | None = None,
    settings: Mapping[str, Any] | None = None,
    tasks: Sequence[Any] = (),
) -> dict[str, Any]:
    opened = await open_tasks(
        storage or MemoryStorage(),
        [GenerationProbe, *tasks],
        {"settings": settings} if settings is not None else None,
    )
    opened["root"] = await opened["harness"].root(context)
    return opened


async def live(harness: Any, root: Any) -> Mapping[str, Any]:
    return await harness.snapshot(LiveDoc, root.id, context) or {}


async def wait_busy(harness: Any, root: Any) -> Any:
    found: dict[str, Any] = {}

    async def check() -> bool:
        state = await live(harness, root)
        run = state.get("run")
        if run is None:
            return False
        record = await harness.get_task(run["taskId"], context)
        if record is None or record["state"]["status"] != "running":
            return False
        found["task"] = run["taskId"]
        return True

    await eventually(check)
    return found["task"]


def release(task_id: Any) -> None:
    gates.setdefault(task_id, deferred()).resolve()


def fail(task_id: Any) -> None:
    failing.add(task_id)
    release(task_id)


async def inbox(harness: Any, root: Any) -> list[Any]:
    state = await harness.snapshot(InboxDoc, root.id, context)
    return [] if state is None else list(state["items"])


async def all_entries(root: Any) -> list[Any]:
    page = await root.entries({}, 1000, None, context)
    return list(reversed(page["items"]))


def transcript(entries: Sequence[Any]) -> list[str]:
    result: list[str] = []
    for entry in entries:
        if entry["kind"] == "pi.system":
            continue
        message = (entry.get("model") or [None])[0]
        if message is None or message.get("role") == "toolResult":
            result.append(entry["kind"])
            continue
        content = message.get("content")
        text: Any
        if isinstance(content, str):
            text = content
        else:
            text = next(
                (part.get("text") for part in content if part.get("type") == "text"), None
            )
        result.append(entry["kind"] if text is None else f"{entry['kind']}:{text}")
    return result


__all__ = [
    "GenerationProbe",
    "all_entries",
    "fail",
    "inbox",
    "live",
    "open_submissions",
    "release",
    "reset",
    "transcript",
    "wait_busy",
]

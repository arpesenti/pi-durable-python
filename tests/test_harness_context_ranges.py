"""Translated kept-range cases of the upstream ``harness-context`` suite."""

from __future__ import annotations

from typing import Any

import pytest
from session_support import context
from task_support import completed, open_tasks

from pi_durable import MemoryStorage
from pi_durable.tasks import define_task


class CountingStorage(MemoryStorage):
    """Memory storage that counts entry rows scanned."""

    def __init__(self) -> None:
        super().__init__()
        self.rows = 0

    async def scan_entries(self, query: Any, limit: int, cursor: Any, scan_context: Any) -> Any:
        page = await super().scan_entries(query, limit, cursor, scan_context)
        self.rows += len(page["items"])
        return page


def user(text: str) -> dict[str, Any]:
    return {"role": "user", "content": text, "timestamp": 1}


def describe(message: Any) -> str:
    if message["role"] == "user":
        return f"user:{message['content']}"
    if message["role"] == "assistant":
        text = next((item for item in message["content"] if item["type"] == "text"), None)
        return f"assistant:{'' if text is None else text['text']}"
    return message["role"]


def abort_state() -> dict[str, Any]:
    return {"status": "terminal", "outcome": {"status": "aborted"}}


async def test_extends_a_tasks_context_read_with_only_newer_entries() -> None:
    storage = CountingStorage()
    seen: dict[str, Any] = {}
    holder: dict[str, Any] = {}

    async def reads_run(task: Any, runtime: Any, ctx: Any) -> None:
        root = holder["root"]

        async def write(draft: Any) -> None:
            async def change(tx: Any) -> None:
                await tx.append_entry(root.id, draft)

            await runtime.commit(change, ctx)

        async def read(at: Any = None) -> dict[str, Any]:
            before = storage.rows
            view = await runtime.context(root.id, ctx, {"at": at})
            return {"view": view, "rows": storage.rows - before}

        seen["initial"] = (await read())["view"]
        assert seen["initial"] == await root.context(ctx)
        await write({"kind": "message", "model": [user("new")]})
        await write({"kind": "note", "data": {"text": "display only"}})
        await write(
            {
                "kind": "message",
                "model": [{"role": "assistant", "content": [], "timestamp": 2}],
            }
        )
        extended = await read()
        assert extended["rows"] == 4
        assert extended["view"] == await root.context(ctx)
        seen["extended"] = extended["view"]
        await write({"kind": "edit", "edits": [{"target": holder["first"]["id"], "action": "omit"}]})
        edited = await read()
        assert edited["rows"] == 2
        assert edited["view"] == await root.context(ctx)
        assert "user:first" not in [describe(message) for message in edited["view"]["messages"]]
        seen["edited"] = edited["view"]
        cutoff = await read(extended["view"]["entries"][-1]["id"])
        assert cutoff["rows"] == 0
        assert cutoff["view"] == extended["view"]
        seen["cutoff"] = cutoff["view"]
        await write({"kind": "reset", "head": "self", "model": [user("fresh")]})
        reset = (await read())["view"]
        assert reset == await root.context(ctx)
        assert [describe(message) for message in reset["messages"]] == ["user:fresh"]
        seen["reset"] = reset
        await runtime.commit(lambda: completed(None), ctx)

    Reads = define_task(
        {
            "name": "test.context-reads",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": reads_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    opened = await open_tasks(storage, [Reads])
    root = await opened["harness"].root(context)
    holder["root"] = root
    holder["first"] = await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "message", "model": [user("first")]}),
        context,
    )
    id = await opened["harness"].commit(
        lambda tx: tx.create_task(
            Reads, {}, {"ownership": {"kind": "conversation"}, "conversationId": root.id}
        ),
        context,
    )
    opened["harness"].resume()
    settled = await opened["harness"].wait_for_task(id, context)
    assert settled["state"]["outcome"] == {"status": "completed", "result": None}
    assert seen["cutoff"] == seen["extended"]
    await opened["harness"].close(context)


async def test_keeps_returned_views_and_kept_ranges_apart() -> None:
    storage = CountingStorage()

    async def mutate_run(task: Any, runtime: Any, ctx: Any) -> None:
        view = await runtime.context(runtime.conversation_id, ctx)
        view["messages"].clear()
        view["entries"].pop()
        with pytest.raises(TypeError):
            view["contributions"][0][0]["content"] = "changed"
        again = await runtime.context(runtime.conversation_id, ctx)
        assert again["messages"] == [user("hello")]
        await runtime.commit(lambda: completed(None), ctx)

    Mutate = define_task(
        {
            "name": "test.context-mutate",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": mutate_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    opened = await open_tasks(storage, [Mutate])
    root = await opened["harness"].root(context)
    await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "message", "model": [user("hello")]}),
        context,
    )
    id = await opened["harness"].commit(
        lambda tx: tx.create_task(
            Mutate, {}, {"ownership": {"kind": "conversation"}, "conversationId": root.id}
        ),
        context,
    )
    opened["harness"].resume()
    assert (await opened["harness"].wait_for_task(id, context))["state"]["outcome"] == {
        "status": "completed",
        "result": None,
    }
    await opened["harness"].close(context)


async def test_drops_the_kept_range_when_retention_is_zero() -> None:
    storage = CountingStorage()
    rows: dict[str, int] = {}

    async def probe_read(name: str, runtime: Any, ctx: Any) -> None:
        before = storage.rows
        await runtime.context(runtime.conversation_id, ctx)
        rows[name] = storage.rows - before

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        await probe_read(task["input"]["name"], runtime, ctx)
        await runtime.commit(lambda: completed(None), ctx)

    Probe = define_task(
        {
            "name": "test.context-probe",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    opened = await open_tasks(storage, [Probe], {"settings": {"context_retention_ms": 0}})
    root = await opened["harness"].root(context)

    async def write(tx: Any) -> None:
        for text in ("one", "two", "three"):
            await tx.append_entry(root.id, {"kind": "message", "model": [user(text)]})

    await root.commit(write, context)
    opened["harness"].resume()
    for name in ("first", "second"):
        id = await opened["harness"].commit(
            lambda tx, name=name: tx.create_task(
                Probe,
                {"name": name},
                {"ownership": {"kind": "conversation"}, "conversationId": root.id},
            ),
            context,
        )
        await opened["harness"].wait_for_task(id, context)
        await root.wait_for_idle(context)
    assert rows == {"first": 4, "second": 4}
    await opened["harness"].close(context)

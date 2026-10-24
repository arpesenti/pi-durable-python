"""Translated kept-range and retention cases of the upstream ``harness-context`` suite.

The derivation cases live in ``test_harness_context.py``; this file adds the Harness-level cases:
extended reads, equivalent derivation under random edits, baseline discovery in added entries,
returned-view and freeze guarantees, eviction on a timer or zero retention, and the late read a
finished task must not keep.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from harness_support import assistant, describe_message, system, tool_result, user
from session_support import context
from task_support import completed, deferred, open_tasks

from pi_durable import MemoryStorage
from pi_durable.tasks import define_task


class CountingStorage(MemoryStorage):
    """Memory storage that counts entry rows scanned and can hold the next range scan."""

    def __init__(self) -> None:
        super().__init__()
        self.rows = 0
        self.hold_next_range_scan: asyncio.Future[Any] | None = None

    async def scan_entries(self, query: Any, limit: int, cursor: Any, scan_context: Any) -> Any:
        held = self.hold_next_range_scan if limit > 1 else None
        if held is not None:
            self.hold_next_range_scan = None
            await held
        page = await super().scan_entries(query, limit, cursor, scan_context)
        self.rows += len(page["items"])
        return page


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
        assert "user:first" not in [describe_message(message) for message in edited["view"]["messages"]]
        seen["edited"] = edited["view"]
        cutoff = await read(extended["view"]["entries"][-1]["id"])
        assert cutoff["rows"] == 0
        assert cutoff["view"] == extended["view"]
        seen["cutoff"] = cutoff["view"]
        await write({"kind": "reset", "head": "self", "model": [user("fresh")]})
        reset = (await read())["view"]
        assert reset == await root.context(ctx)
        assert [describe_message(message) for message in reset["messages"]] == ["user:fresh"]
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


async def counting_setup(
    tasks: tuple[Any, ...] = (),
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A Harness over CountingStorage with a 21-entry root transcript."""

    storage = CountingStorage()
    opened = await open_tasks(storage, list(tasks), options)
    harness = opened["harness"]
    root = await harness.root(context)
    first = await root.commit(
        lambda tx: tx.append_entry(
            root.id, {"kind": "message", "model": [user("first")]}
        ),
        context,
    )
    for index in range(20):
        await root.commit(
            lambda tx, index=index: tx.append_entry(
                root.id,
                {"kind": "message", "model": [assistant(f"old {index}")]},
            ),
            context,
        )
    return {**opened, "storage": storage, "root": root, "first": first}


def _lcg(seed: int) -> Any:
    state = {"seed": seed}

    def random(n: int) -> int:
        state["seed"] = (state["seed"] * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF
        return int(state["seed"] / 2**32 * n)

    return random


async def test_keeps_a_context_read_across_tasks_and_through_the_retention_period() -> None:
    clock = {"now": 1_000}
    rows: dict[str, int] = {}
    holder: dict[str, Any] = {}

    async def read(name: str, runtime: Any, ctx: Any) -> None:
        before = holder["storage"].rows
        view = await runtime.context(holder["root"].id, ctx)
        rows[name] = holder["storage"].rows - before
        assert view == await holder["root"].context(ctx)

    async def child_run(_task: Any, runtime: Any, ctx: Any) -> None:
        async def write(tx: Any) -> None:
            await tx.append_entry(
                holder["root"].id, {"kind": "message", "model": [user("from child")]}
            )

        await runtime.commit(write, ctx)
        await read("child", runtime, ctx)
        await runtime.commit(lambda: completed(None), ctx)

    Child = define_task(
        {
            "name": "test.context-child",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": child_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )

    async def parent_start(_task: Any, runtime: Any, ctx: Any) -> None:
        await read("parent", runtime, ctx)

        async def create(tx: Any) -> Any:
            child = await tx.create_task(
                Child,
                {},
                {
                    "ownership": {"kind": "task", "taskId": runtime.task_id},
                    "conversationId": holder["root"].id,
                },
            )
            return {
                "status": "waiting",
                "checkpoint": {"phase": "after"},
                "on": [child],
                "policy": "allSettled",
            }

        await runtime.commit(create, ctx)

    async def parent_after(_task: Any, runtime: Any, ctx: Any) -> None:
        await read("after", runtime, ctx)
        await runtime.commit(lambda: completed(None), ctx)

    Parent = define_task(
        {
            "name": "test.context-parent",
            "version": 1,
            "initial": lambda _input: {"phase": "start"},
            "phases": {"start": parent_start, "after": parent_after},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )

    async def probe_run(task: Any, runtime: Any, ctx: Any) -> None:
        await read(task["input"]["name"], runtime, ctx)
        await runtime.commit(lambda: completed(None), ctx)

    Probe = define_task(
        {
            "name": "test.context-probe",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": probe_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )

    opened = await counting_setup(
        (Child, Parent, Probe),
        {"settings": {"context_retention_ms": 600_000}, "now": lambda: clock["now"]},
    )
    holder["storage"], holder["root"] = opened["storage"], opened["root"]
    harness = opened["harness"]
    harness.resume()
    options = {"ownership": {"kind": "conversation"}, "conversationId": opened["root"].id}
    parent = await harness.commit(
        lambda tx: tx.create_task(Parent, {}, options), context
    )
    assert (await harness.wait_for_task(parent, context))["state"]["outcome"] == {
        "status": "completed",
        "result": None,
    }
    await opened["root"].wait_for_idle(context)

    async def probe(name: str) -> None:
        id = await harness.commit(
            lambda tx: tx.create_task(Probe, {"name": name}, options), context
        )
        assert (await harness.wait_for_task(id, context))["state"]["outcome"] == completed(None)[
            "outcome"
        ]
        await opened["root"].wait_for_idle(context)

    await probe("idle")
    clock["now"] += 600_000
    await probe("expired")
    assert rows == {"parent": 22, "child": 2, "after": 1, "idle": 1, "expired": 23}
    await harness.close(context)


async def test_derives_the_same_view_from_extended_reads_as_from_whole_reads() -> None:
    random = _lcg(7)
    seen: dict[str, int] = {}

    def count(category: str) -> None:
        seen[category] = seen.get(category, 0) + 1

    async def steps_run(_task: Any, runtime: Any, ctx: Any) -> None:
        root = holder["root"]
        written: list[Any] = []
        calls: list[str] = []
        counter = {"next": 0}

        def next_value() -> int:
            counter["next"] += 1
            return counter["next"]

        def draft() -> dict[str, Any]:
            pick = random(14)
            if pick < 3:
                count("user")
                return {
                    "kind": "message",
                    "model": [user(f"u{next_value()}")],
                }
            if pick < 5:
                ids = [f"c{next_value()}" for _ in range(random(3))]
                calls.extend(ids)
                reason = (["aborted", "error", "deferred"] + [None] * 6)[random(9)]
                count(reason or "assistant")
                options: dict[str, Any] = {"calls": ids}
                if reason is not None:
                    options["stop_reason"] = reason
                return {
                    "kind": "message",
                    "model": [assistant(f"a{next_value()}", options)],
                }
            if pick < 8:
                matched = bool(calls) and random(5) > 0
                count("result" if matched else "stray result")
                id = calls.pop(random(len(calls))) if matched else f"stray{next_value()}"
                return {"kind": "message", "model": [tool_result(id)]}
            if pick < 9:
                kind = "pi.system" if random(2) == 0 else "message"
                count("baseline system" if kind == "pi.system" else "system")
                return {"kind": kind, "model": [system({"s": f"v{next_value()}"})]}
            if pick < 10:
                count("note")
                return {"kind": "note", "data": {"n": next_value()}}
            target = written[random(len(written))] if written else None
            if pick < 12 and target is not None:
                omit = random(2) == 0
                count("omit" if omit else "replace")
                action = (
                    {"action": "omit"}
                    if omit
                    else {"action": "replace", "messages": [user(f"r{next_value()}")]}
                )
                return {"kind": "edit", "edits": [{"target": target["id"], **action}]}
            if pick < 13 and target is not None:
                count("head")
                return {
                    "kind": "summary",
                    "head": target["id"],
                    "model": [user(f"h{next_value()}")],
                }
            count("user")
            return {"kind": "message", "model": [user(f"u{next_value()}")]}

        async def append(conversation_id: Any) -> None:
            async def change(tx: Any) -> None:
                for _ in range(1 + random(3)):
                    written.append(await tx.append_entry(conversation_id, draft()))

            await runtime.commit(change, ctx)

        for _step in range(150):
            await append(root.id)
            whole = await root.context(ctx)
            assert await runtime.context(root.id, ctx) == whole
            contributed = [message for messages in whole["contributions"] for message in messages]
            first = next(
                (index for index, message in enumerate(contributed) if message["role"] == "system"),
                -1,
            )
            if (
                first > 0
                and whole["messages"][0] == contributed[first]
                and any(message["role"] == "assistant" for message in contributed[:first])
            ):
                count("baseline past assistant")

        fork = await root.fork(
            written[random(len(written))]["id"],
            {"ownership": {"kind": "ownerless"}},
            ctx,
        )
        for _step in range(20):
            assert await runtime.context(fork.id, ctx) == await fork.context(ctx)
            await append(fork.id)
        await runtime.commit(lambda: completed(None), ctx)

    Steps = define_task(
        {
            "name": "test.context-steps",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": steps_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    holder: dict[str, Any] = {}
    opened = await counting_setup((Steps,))
    holder["root"] = opened["root"]
    opened["harness"].resume()
    id = await opened["harness"].commit(
        lambda tx: tx.create_task(
            Steps,
            {},
            {"ownership": {"kind": "conversation"}, "conversationId": opened["root"].id},
        ),
        context,
    )
    settled = await opened["harness"].wait_for_task(id, context)
    assert settled["state"]["outcome"] == {"status": "completed", "result": None}
    for category in [
        "user",
        "assistant",
        "aborted",
        "error",
        "deferred",
        "result",
        "stray result",
        "system",
        "baseline system",
        "baseline past assistant",
        "note",
    ]:
        assert seen.get(category, 0) > 0, category
    for category in ["omit", "replace", "head"]:
        assert seen.get(category, 0) > 0, category
    await opened["harness"].close(context)


async def test_finds_the_baseline_among_the_entries_an_extended_read_adds() -> None:
    async def steps_run(_task: Any, runtime: Any, ctx: Any) -> None:
        root = holder["root"]

        async def append(conversation: Any, draft: dict[str, Any]) -> Any:
            added: dict[str, Any] = {}

            async def change(tx: Any) -> None:
                added["entry"] = await tx.append_entry(conversation.id, draft)

            await runtime.commit(change, ctx)
            return added["entry"]

        async def read(conversation: Any) -> dict[str, Any]:
            before = holder["storage"].rows
            view = await runtime.context(conversation.id, ctx)
            rows = holder["storage"].rows - before
            whole = await conversation.context(ctx)
            assert view == whole
            return {
                "messages": [describe_message(message) for message in view["messages"]],
                "rows": rows,
            }

        result = await read(root)
        assert result["messages"][0] == "user:first"
        await append(root, {"kind": "pi.system", "model": [system({"plan": "Plan."})]})
        extended = await read(root)
        assert extended["rows"] == 2
        assert extended["messages"][:3] == ["system:plan", "user:first", "assistant:old 0"]

        fork = await root.fork(
            holder["first"]["id"], {"ownership": {"kind": "ownerless"}}, ctx
        )
        await append(fork, {"kind": "message", "model": [assistant("answer")]})
        await append(
            fork, {"kind": "host.system", "model": [system({"note": "Note."})]}
        )
        assert (await read(fork))["messages"] == [
            "user:first",
            "assistant:answer",
            "system:note",
        ]
        await append(fork, {"kind": "pi.system", "model": [system({"note": None})]})
        extended = await read(fork)
        assert extended["rows"] == 2
        assert extended["messages"] == [
            "user:first",
            "assistant:answer",
            "system:note",
            "system:note",
        ]

        omitted = await root.fork(
            holder["first"]["id"], {"ownership": {"kind": "ownerless"}}, ctx
        )
        await append(omitted, {"kind": "message", "model": [assistant("answer")]})
        host = await append(
            omitted, {"kind": "host.system", "model": [system({"legacy": "Legacy."})]}
        )
        await append(
            omitted,
            {"kind": "edit", "edits": [{"target": host["id"], "action": "omit"}]},
        )
        assert (await read(omitted))["messages"] == ["user:first", "assistant:answer"]
        await append(omitted, {"kind": "pi.system", "model": [system({"legacy": None})]})
        extended = await read(omitted)
        assert extended["rows"] == 2
        assert extended["messages"] == [
            "user:first",
            "assistant:answer",
            "system:legacy",
        ]

        empty = await root.fork(
            holder["first"]["id"], {"ownership": {"kind": "ownerless"}}, ctx
        )
        await append(empty, {"kind": "pi.system"})
        await append(empty, {"kind": "message", "model": [assistant("answer")]})
        assert (await read(empty))["messages"] == ["user:first", "assistant:answer"]
        await append(empty, {"kind": "pi.system", "model": [system({"s": "Delta."})]})
        extended = await read(empty)
        assert extended["rows"] == 2
        assert extended["messages"] == ["user:first", "assistant:answer", "system:s"]
        await runtime.commit(lambda: completed(None), ctx)

    Steps = define_task(
        {
            "name": "test.context-baseline",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": steps_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    holder: dict[str, Any] = {}
    opened = await counting_setup((Steps,))
    holder["root"], holder["first"], holder["storage"] = (
        opened["root"],
        opened["first"],
        opened["storage"],
    )
    opened["harness"].resume()
    id = await opened["harness"].commit(
        lambda tx: tx.create_task(
            Steps,
            {},
            {"ownership": {"kind": "conversation"}, "conversationId": opened["root"].id},
        ),
        context,
    )
    settled = await opened["harness"].wait_for_task(id, context)
    assert settled["state"]["outcome"] == {"status": "completed", "result": None}
    await opened["harness"].close(context)


def _thaw(value: Any) -> Any:
    return json.loads(json.dumps(value))


class ShallowStorage(CountingStorage):
    """Storage whose entries have only the record itself frozen: their messages stay mutable."""

    async def scan_entries(self, query: Any, limit: int, cursor: Any, scan_context: Any) -> Any:
        page = await super().scan_entries(query, limit, cursor, scan_context)
        return {**page, "items": [_thaw(item) for item in page["items"]]}


async def test_freezes_kept_entries_deeply_even_when_storage_freezes_them_shallowly() -> None:
    async def mutate_run(_task: Any, runtime: Any, ctx: Any) -> None:
        view = await runtime.context(runtime.conversation_id, ctx)
        with pytest.raises(TypeError):
            view["messages"][0]["content"] = "changed"
        await runtime.commit(lambda: completed(None), ctx)

    Mutate = define_task(
        {
            "name": "test.context-shallow",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": mutate_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    storage = ShallowStorage()
    opened = await open_tasks(storage, [Mutate])
    harness = opened["harness"]
    root = await harness.root(context)
    await root.commit(
        lambda tx: tx.append_entry(
            root.id, {"kind": "message", "model": [user("hello")]}
        ),
        context,
    )
    harness.resume()
    id = await harness.commit(
        lambda tx: tx.create_task(
            Mutate,
            {},
            {"ownership": {"kind": "conversation"}, "conversationId": root.id},
        ),
        context,
    )
    settled = await harness.wait_for_task(id, context)
    assert settled["state"]["outcome"] == {"status": "completed", "result": None}
    await harness.close(context)


async def test_does_not_keep_a_read_that_finishes_after_its_task_ended() -> None:
    scans = deferred()
    probe_start = deferred()
    rows: dict[str, int] = {}
    holder: dict[str, Any] = {}

    async def late_run(_task: Any, runtime: Any, ctx: Any) -> None:
        holder["storage"].hold_next_range_scan = scans.promise
        late = asyncio.ensure_future(runtime.context(holder["root"].id, ctx))
        late.add_done_callback(
            lambda done: done.exception() if not done.cancelled() else None
        )
        holder["late"] = late
        await runtime.commit(lambda: completed(None), ctx)

    Late = define_task(
        {
            "name": "test.context-late",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": late_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )

    async def probe_run(_task: Any, runtime: Any, ctx: Any) -> None:
        await probe_start.promise
        before = holder["storage"].rows
        await runtime.context(runtime.conversation_id, ctx)
        rows["probe"] = holder["storage"].rows - before
        await runtime.commit(lambda: completed(None), ctx)

    Probe = define_task(
        {
            "name": "test.context-probe",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": probe_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    opened = await counting_setup(
        (Late, Probe), {"settings": {"context_retention_ms": 0}}
    )
    harness, root = opened["harness"], opened["root"]
    holder["root"], holder["storage"] = root, opened["storage"]
    harness.resume()
    options = {"ownership": {"kind": "conversation"}, "conversationId": root.id}
    late_id = await harness.commit(
        lambda tx: tx.create_task(Late, {}, options), context
    )
    assert (await harness.wait_for_task(late_id, context))["state"]["outcome"] == {
        "status": "completed",
        "result": None,
    }
    probe_id = await harness.commit(
        lambda tx: tx.create_task(Probe, {}, options), context
    )
    scans.resolve()
    await asyncio.gather(holder["late"], return_exceptions=True)
    probe_start.resolve()
    assert (await harness.wait_for_task(probe_id, context))["state"]["outcome"] == {
        "status": "completed",
        "result": None,
    }
    assert rows == {"probe": 22}
    await harness.close(context)


async def test_drops_an_idle_conversations_context_read_on_a_timer_when_nothing_else_runs() -> None:
    rows: dict[str, int] = {}
    storage = CountingStorage()

    async def probe_run(task: Any, runtime: Any, ctx: Any) -> None:
        before = storage.rows
        await runtime.context(runtime.conversation_id, ctx)
        rows[task["input"]["name"]] = storage.rows - before
        await runtime.commit(lambda: completed(None), ctx)

    Probe = define_task(
        {
            "name": "test.context-probe",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": probe_run},
            "abort": lambda task, runtime, ctx: runtime.commit(abort_state, ctx),
        }
    )
    opened = await open_tasks(
        storage, [Probe], {"settings": {"context_retention_ms": 40}}
    )
    harness = opened["harness"]
    root = await harness.root(context)
    first = await root.commit(
        lambda tx: tx.append_entry(
            root.id, {"kind": "message", "model": [user("first")]}
        ),
        context,
    )
    for index in range(20):
        await root.commit(
            lambda tx, index=index: tx.append_entry(
                root.id,
                {"kind": "message", "model": [assistant(f"old {index}")]},
            ),
            context,
        )
    harness.resume()
    options = {"ownership": {"kind": "conversation"}, "conversationId": root.id}

    async def probe(name: str) -> None:
        id = await harness.commit(
            lambda tx: tx.create_task(Probe, {"name": name}, options), context
        )
        assert (await harness.wait_for_task(id, context))["state"]["outcome"] == {
            "status": "completed",
            "result": None,
        }
        await root.wait_for_idle(context)

    await probe("first")
    # A second probe inside the retention period reuses the kept range: one bounds probe only.
    await probe("warm")
    await asyncio.sleep(0.12)
    await probe("second")
    assert rows == {"first": 22, "warm": 1, "second": 22}
    assert first["id"] is not None
    await harness.close(context)

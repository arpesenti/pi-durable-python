"""Translated ``harness-idle-scopes`` suite: idle checks without per-commit scans.

Upstream ``test/harness-idle-scopes.test.ts``, case for case. A task change settles only the
conversations above it (idle waiters and kept context ranges), expiry runs oldest first from an
ordered index, and ``wake()`` reads only sleeping tasks while work is active. Every case that can
checks the incremental idle state against a full recompute (``Harness.scheduler_idle_state``, the
port of ``SCHEDULER_IDLE_STATE``): ``contexts``, ``idleContexts``, ``idleMismatches``,
``fullIdleChecks``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from functools import partial
from typing import Any

from session_support import ControlledStorage, context, flush, settle_line
from task_support import aborted, completed, deferred, eventually, open_tasks, settled

from pi_durable import MemoryStorage, define_task
from pi_durable.harness.harness import Harness
from pi_durable.ids import ROOT_CONVERSATION_ID
from pi_durable.testing import commit_writes
from pi_durable.types import StorageWrite

#: Gates one ``Work`` task waits behind, by the name in its input.
gates: dict[str, Any] = {}
#: Resolves once the named task's context read returned.
reads: dict[str, Any] = {}


def named(map_: dict[str, Any], name: str) -> Any:
    found = map_.get(name)
    if found is None:
        found = deferred()
        map_[name] = found
    return found


def reset() -> None:
    gates.clear()
    reads.clear()


async def _run(task: Any, runtime: Any, ctx: Any) -> None:
    value = task["input"]
    if value.get("read") is not None:
        await runtime.context(value["read"], ctx)
        named(reads, value["name"]).resolve()
    opened = asyncio.ensure_future(aborted(runtime.signal))
    gate = asyncio.ensure_future(named(gates, value["name"]).promise)
    try:
        await asyncio.wait({opened, gate}, return_when=asyncio.FIRST_COMPLETED)
        if not gate.done():
            return
    finally:
        opened.cancel()
        gate.cancel()
    await runtime.commit(lambda: completed(None), ctx)


async def _abort(_task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)


Work = define_task(
    {
        "name": "test.idle.work",
        "version": 1,
        "initial": lambda _input: {"phase": "run"},
        "phases": {"run": _run},
        "abort": _abort,
    }
)


class ScannedMemory(MemoryStorage):
    """Memory storage counting the entry rows each scan returns."""

    def __init__(self) -> None:
        super().__init__()
        self.rows = 0

    async def scan_entries(self, query: Any, limit: int, cursor: Any, scan_context: Any) -> Any:
        page = await super().scan_entries(query, limit, cursor, scan_context)
        self.rows += len(page["items"])
        return page


def decline_background(
    callback: Callable[[], None], delay_ms: float, *, background: bool = False
) -> Any:
    """Background timers declined, as Cloudflare Workers do; foreground ones on the event loop."""

    if background:
        return None
    timer = asyncio.get_running_loop().call_later(delay_ms / 1000, callback)
    return timer.cancel


async def eventually_state(harness: Harness, id: Any, status: str) -> None:
    """Await until task ``id``'s state reports ``status``."""

    async def check() -> bool:
        record = await harness.get_task(id, context)
        return record is not None and record["state"]["status"] == status

    await eventually(check)


async def quiet(harness: Harness | None = None) -> None:
    """Flush until the Session line is quiet, so scheduler passes started by the last changes settled.

    Upstream's ``quiet()`` is four flushes, which is enough there; here a pass takes several loop
    turns (a ``call_soon``, a line job, its publication), so the port waits on the Session's own
    bound -- nothing outstanding and nothing unpublished -- and flushes after it.
    """

    if harness is not None:
        await settle_line(harness)
    for _round in range(4):
        await flush()


async def open_harness(
    options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """A resumed Harness running ``Work`` over ``ScannedMemory``.

    ``clock`` is its time, ``retention`` returns its context retention, and background timers are
    declined unless ``timers``.
    """

    values = dict(options or {})
    storage = ScannedMemory()
    settings = values.get("retention")
    opened = await open_tasks(
        storage,
        [Work],
        {
            **({"now": values["clock"]} if "clock" in values else {}),
            **({"settings": lambda: {"context_retention_ms": settings()}} if settings is not None else {}),
            **({} if values.get("timers") else {"set_timer": decline_background}),
        },
    )
    harness = opened["harness"]
    harness.resume()
    root = await harness.root(context)

    async def conversation(entries: int = 2) -> Any:
        created = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
        for index in range(entries):
            await created.commit(
                lambda tx, index=index, id=created.id: tx.append_entry(
                    id, {"kind": "note", "data": {"index": index}}
                ),
                context,
            )
        return created

    async def work(name: str, into: Any, read: Any = None) -> Any:
        value: dict[str, Any] = {"name": name} if read is None else {"name": name, "read": read}
        id = await into.commit(
            lambda tx: tx.create_task(
                Work,
                value,
                {"ownership": {"kind": "conversation"}, "conversationId": into.id},
            ),
            context,
        )
        if read is not None:
            await named(reads, name).promise
        return id

    async def end(name: str, id: Any) -> None:
        named(gates, name).resolve()
        await harness.wait_for_task(id, context)

    async def rows_of(name: str, into: Any, read: Any) -> int:
        before = storage.rows
        id = await work(name, into, read)
        rows = storage.rows - before
        await end(name, id)
        return rows

    return {
        **opened,
        "storage": storage,
        "root": root,
        "conversation": conversation,
        "work": work,
        "end": end,
        "rows_of": rows_of,
    }


# ── idle waiters by scope ──────────────────────────────────────────────────


async def test_resolves_waiters_through_owner_chains_owned_conversations_and_ended_tasks() -> None:
    """Upstream ``:157``."""

    reset()
    opened = await open_harness()
    harness, root = opened["harness"], opened["root"]

    async def build(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Work, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        b = await tx.create_task(Work, {"name": "b"}, {"ownership": {"kind": "task", "taskId": p}})
        a = await tx.create_task(Work, {"name": "a"}, {"ownership": {"kind": "task", "taskId": b}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": a}})
        d = await tx.create_task(
            Work,
            {"name": "d"},
            {"ownership": {"kind": "conversation"}, "conversationId": c["id"]},
        )
        g = await tx.create_task(
            Work,
            {"name": "g"},
            {"ownership": {"kind": "conversation"}, "conversationId": c["id"], "background": True},
        )
        return {"p": p, "b": b, "a": a, "c": c["id"], "d": d, "g": g}

    tree = await root.commit(build, context)
    c = await harness.conversation(tree["c"], context)
    idle_c = asyncio.ensure_future(c.wait_for_idle(context))
    idle_root = asyncio.ensure_future(root.wait_for_idle(context))
    idle_all = asyncio.ensure_future(harness.wait_for_idle(context))
    # The owners A and B hold for D: live, though their handlers returned.
    named(gates, "a").resolve()
    named(gates, "b").resolve()
    await eventually_state(harness, tree["b"], "completing")
    assert [await settled(idle_c), await settled(idle_root), await settled(idle_all)] == [False, False, False]
    # D ends: C is idle (G is background); A and B finalize; P still holds the root.
    named(gates, "d").resolve()
    await idle_c
    await harness.wait_for_task(tree["b"], context)
    assert [await settled(idle_root), await settled(idle_all)] == [False, False]
    named(gates, "p").resolve()
    await idle_root
    await idle_all
    assert (await harness.get_task(tree["g"], context))["state"]["status"] != "terminal"
    await harness.close(context)


async def test_counts_work_whose_chain_is_not_loaded_as_inside_until_the_open_reconcile() -> None:
    """Upstream ``:197``."""

    reset()
    storage = ControlledStorage()
    first = await open_tasks(storage, [Work])
    first_root = await first["harness"].root(context)

    async def build(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Work, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": p}})
        d = await tx.create_task(
            Work,
            {"name": "d"},
            {"ownership": {"kind": "conversation"}, "conversationId": c["id"]},
        )
        other = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        return {"p": p, "c": c["id"], "d": d, "other": other["id"]}

    tree = await first_root.commit(build, context)
    await first["harness"].close(context)
    # Reopened, no chain is loaded: P and D count as inside every scope until the reconcile pass loads theirs.
    opened = await open_tasks(storage.reopen(), [Work])
    harness = opened["harness"]
    other = await harness.conversation(tree["other"], context)
    idle_other = asyncio.ensure_future(other.wait_for_idle(context))
    idle_root = asyncio.ensure_future((await harness.root(context)).wait_for_idle(context))
    await idle_other
    assert await settled(idle_root) is False
    harness.resume()
    named(gates, "d").resolve()
    await harness.wait_for_task(tree["d"], context)
    assert await settled(idle_root) is False
    named(gates, "p").resolve()
    await idle_root
    await harness.close(context)


async def test_resolves_a_waiter_on_an_unrelated_conversation_without_settling_the_busy_one() -> None:
    """Upstream ``:228``."""

    reset()
    opened = await open_harness()
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    busy, other = await conversation(0), await conversation(0)
    held = await work("held", busy)
    quick = await work("quick", other)
    idle_busy = asyncio.ensure_future(busy.wait_for_idle(context))
    idle_other = asyncio.ensure_future(other.wait_for_idle(context))
    await end("quick", quick)
    await idle_other
    assert await settled(idle_busy) is False
    await end("held", held)
    await idle_busy
    await harness.close(context)


# ── kept context ranges ────────────────────────────────────────────────────


async def test_settles_a_run_end_whose_sweep_drops_the_chain_without_a_check_of_every_scope() -> None:
    """Upstream ``:245``."""

    reset()
    opened = await open_harness()
    harness, root = opened["harness"], opened["root"]

    async def build(tx: Any) -> dict[str, Any]:
        p = await tx.create_task(Work, {"name": "p"}, {"ownership": {"kind": "conversation"}})
        t = await tx.create_task(Work, {"name": "t"}, {"ownership": {"kind": "task", "taskId": p}})
        c = await tx.create_conversation({"ownership": {"kind": "task", "taskId": t}})
        # A context range keeps entries: an empty conversation has none to keep.
        await tx.append_entry(c["id"], {"kind": "note"})
        return {"p": p, "t": t, "c": c["id"]}

    tree = await root.commit(build, context)
    c = await harness.conversation(tree["c"], context)
    d = await c.commit(
        lambda tx: tx.create_task(
            Work,
            {"name": "d", "read": tree["c"]},
            {"ownership": {"kind": "conversation"}, "conversationId": tree["c"]},
        ),
        context,
    )
    await named(reads, "d").promise
    named(gates, "t").resolve()
    await eventually_state(harness, tree["t"], "completing")
    await quiet(harness)
    before = harness.scheduler_idle_state
    assert before["contexts"] == 1
    assert before["idleContexts"] == 0
    assert before["idleMismatches"] == 0
    # D's end makes C idle, T finalizes, and the sweep forgets C's chain: none of it checks every scope.
    named(gates, "d").resolve()
    await harness.wait_for_task(d, context)
    await harness.wait_for_task(tree["t"], context)
    await quiet(harness)
    after = harness.scheduler_idle_state
    assert after == {**before, "idleContexts": 1}
    named(gates, "p").resolve()
    await harness.wait_for_task(tree["p"], context)
    await harness.close(context)


async def test_drops_a_range_past_its_retention_when_a_task_change_makes_it_busy_again() -> None:
    """Upstream ``:282``."""

    reset()
    clock = {"now": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: 1_000})
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    x, z = await conversation(), await conversation(0)
    await end("reader", await work("reader", z, x.id))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (1, 1, 0)
    clock["now"] = 2_500
    # No timer expired it; the task change that makes X busy checks expiry first.
    busy = await work("busy", x)
    await quiet(harness)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (0, 0, 0)
    await end("busy", busy)
    await harness.close(context)


async def test_drops_a_range_past_its_retention_at_a_reconcile_pass_with_nothing_to_do() -> None:
    """Upstream ``:297``."""

    reset()
    clock = {"now": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: 1_000})
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    x, z = await conversation(), await conversation(0)
    await end("reader", await work("reader", z, x.id))
    held = await work("held", z)
    # Held runs: no task changes from here.
    await quiet(harness)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"]) == (1, 1)
    clock["now"] = 2_500
    # A wait on busy Z asks for a reconcile pass, which has no work and changes no task: it checks expiry.
    idle = asyncio.ensure_future(z.wait_for_idle(context))
    await quiet(harness)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleMismatches"]) == (0, 0)
    await end("held", held)
    await idle
    await harness.close(context)


async def test_expires_in_idle_order_when_the_clock_goes_back() -> None:
    """Upstream ``:316``."""

    reset()
    clock = {"now": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: 1_000})
    harness, conversation, work, end, rows_of = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
        opened["rows_of"],
    )
    a, b, z = await conversation(3), await conversation(5), await conversation(0)
    await end("read a", await work("read a", z, a.id))
    clock["now"] = 500
    await end("read b", await work("read b", z, b.id))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (2, 2, 0)
    # B went idle at 500 and expires first, though it went idle last.
    clock["now"] = 1_600
    await end("unrelated", await work("unrelated", z))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleMismatches"]) == (1, 0)
    assert await rows_of("again b", z, b.id) == 5
    assert await rows_of("again a", z, a.id) == 0
    await harness.close(context)


async def test_drops_every_idle_range_when_retention_becomes_zero() -> None:
    """Upstream ``:336``."""

    reset()
    clock = {"now": 2_000}
    retention = {"ms": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: retention["ms"]})
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    a, z = await conversation(), await conversation(0)
    await end("read", await work("read", z, a.id))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"]) == (1, 1)
    # The clock goes back below A's idle start; then retention 0 and an unrelated task change.
    clock["now"] = 1_000
    retention["ms"] = 0
    await end("unrelated", await work("unrelated", z))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleMismatches"]) == (0, 0)
    await harness.close(context)


async def test_keeps_an_idle_ranges_expiry_when_another_task_reads_it_again() -> None:
    """Upstream ``:351``."""

    reset()
    clock = {"now": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: 1_000})
    harness, conversation, work, end, rows_of = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
        opened["rows_of"],
    )
    a, b, z = await conversation(3), await conversation(5), await conversation(0)
    await end("read a", await work("read a", z, a.id))
    clock["now"] = 1_500
    await end("read b", await work("read b", z, b.id))
    clock["now"] = 1_800
    assert await rows_of("again a", z, a.id) == 0
    clock["now"] = 2_100
    await end("unrelated", await work("unrelated", z))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleMismatches"]) == (1, 0)
    assert await rows_of("late b", z, b.id) == 0
    await harness.close(context)


async def test_drops_a_range_from_the_expiry_order_when_an_entry_commit_finds_it_expired() -> None:
    """Upstream ``:370``."""

    reset()
    clock = {"now": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: 1_000})
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    a, z = await conversation(), await conversation(0)
    await end("read", await work("read", z, a.id))
    clock["now"] = 2_500
    # An entry-only commit settles no idleness: the range is dropped where it would be extended.
    await a.commit(lambda tx: tx.append_entry(a.id, {"kind": "note", "data": {"late": True}}), context)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (0, 0, 0)
    await harness.close(context)


async def test_drops_a_range_from_the_expiry_order_when_an_entry_commit_cannot_extend_it() -> None:
    """Upstream ``:382``."""

    reset()
    opened = await open_harness()
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    a, z = await conversation(0), await conversation(0)
    first = await a.commit(lambda tx: tx.append_entry(a.id, {"kind": "note"}), context)
    second = await a.commit(lambda tx: tx.append_entry(a.id, {"kind": "note"}), context)
    await a.commit(lambda tx: tx.append_entry(a.id, {"kind": "summary", "head": second["id"]}), context)
    await end("read", await work("read", z, a.id))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (1, 1, 0)
    # A marker whose head is older than the range's: the range cannot be extended and is dropped.
    await a.commit(lambda tx: tx.append_entry(a.id, {"kind": "summary", "head": first["id"]}), context)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (0, 0, 0)
    await harness.close(context)


async def test_applies_a_shorter_retention_to_idle_ranges_at_an_unrelated_commit() -> None:
    """Upstream ``:396``, row ``shorter``."""

    await _applies_a_retention(400, 0)


async def test_applies_a_longer_retention_to_idle_ranges_at_an_unrelated_commit() -> None:
    """Upstream ``:396``, row ``longer``."""

    await _applies_a_retention(10_000, 1)


async def test_applies_a_zero_retention_to_idle_ranges_at_an_unrelated_commit() -> None:
    """Upstream ``:396``, row ``zero``."""

    await _applies_a_retention(0, 0)


async def _applies_a_retention(changed: int, kept: int) -> None:
    reset()
    clock = {"now": 1_000}
    retention = {"ms": 1_000}
    opened = await open_harness({"clock": lambda: clock["now"], "retention": lambda: retention["ms"]})
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    a, z = await conversation(), await conversation(0)
    await end("read", await work("read", z, a.id))
    clock["now"] = 1_500
    retention["ms"] = changed
    await end("unrelated", await work("unrelated", z))
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleMismatches"]) == (kept, 0)
    await harness.close(context)


async def test_drops_a_range_that_goes_idle_with_zero_retention() -> None:
    """Upstream ``:413``."""

    reset()
    opened = await open_harness({"retention": lambda: 0})
    harness, conversation, work, end = (
        opened["harness"],
        opened["conversation"],
        opened["work"],
        opened["end"],
    )
    a = await conversation()
    own = await work("own", a, a.id)
    await quiet(harness)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleContexts"], state["idleMismatches"]) == (1, 0, 0)
    await end("own", own)
    await quiet(harness)
    state = harness.scheduler_idle_state
    assert (state["contexts"], state["idleMismatches"]) == (0, 0)
    await harness.close(context)


# ── incremental idle state ─────────────────────────────────────────────────


async def _owned_task(tx: Any, *, name: str, owner_id: Any, into: Any) -> dict[str, Any]:
    """A child task of ``owner_id``, created in its owner's conversation."""

    added = await tx.create_task(
        Work, {"name": name}, {"ownership": {"kind": "task", "taskId": owner_id}}
    )
    return {"id": added, "into": into, "read": False}


async def _owned_conversation_with_reader(
    tx: Any, *, name: str, owner_id: Any, into: Any
) -> dict[str, Any]:
    """A conversation owned by ``owner_id``, with a task in it that reads it."""

    child = await tx.create_conversation({"ownership": {"kind": "task", "taskId": owner_id}})
    await tx.append_entry(child["id"], {"kind": "note"})
    added = await tx.create_task(
        Work,
        {"name": name, "read": child["id"]},
        {"ownership": {"kind": "conversation"}, "conversationId": child["id"]},
    )
    return {"id": added, "into": child["id"], "read": True}


def seeded(seed: int) -> Callable[[], float]:
    """A random source with a fixed seed."""

    state = {"seed": seed}

    def random() -> float:
        state["seed"] = (state["seed"] * 1_103_515_245 + 12_345) % 2_147_483_648
        return state["seed"] / 2_147_483_648

    return random


async def test_equals_a_full_recompute_after_every_step_of_random_nested_work() -> None:
    """Upstream ``:436``."""

    reset()
    random = seeded(42)

    def pick(items: list[Any]) -> Any:
        return items[int(random() * len(items))]

    opened = await open_harness()
    harness, root = opened["harness"], opened["root"]
    # A context range keeps entries: every conversation gets one.
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    conversations = [root.id]
    live: dict[str, dict[str, Any]] = {}
    count = {"n": 0}
    full_checks = harness.scheduler_idle_state["fullIdleChecks"]
    for step in range(200):
        action = random()
        name = f"t{count['n']}"
        count["n"] += 1
        if action < 0.1:
            created = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
            new_id = created.id
            await created.commit(
                lambda tx, id=new_id: tx.append_entry(id, {"kind": "note"}), context
            )
            conversations.append(new_id)
        elif action < 0.45 or not live:
            # A task in any conversation, also one whose owner chain ended and was swept; it may read one.
            into = pick(conversations)
            read = pick(conversations) if random() < 0.6 else None
            background = random() < 0.15
            target = await harness.conversation(into, context)
            value = {"name": name} if read is None else {"name": name, "read": read}
            id = await target.commit(
                lambda tx, into=into, value=value, background=background: tx.create_task(
                    Work,
                    value,
                    {
                        "ownership": {"kind": "conversation"},
                        "conversationId": into,
                        "background": background,
                    },
                ),
                context,
            )
            live[name] = {"id": id, "conversation": into}
            if read is not None:
                await named(reads, name).promise
        elif action < 0.6:
            # Work a live task owns: a child task, or an owned conversation with a task that reads it.
            owner_name, owner = pick(list(live.items()))
            record = await harness.get_task(owner["id"], context)
            status = None if record is None else record["state"]["status"]
            if status is None or status in ("terminal", "completing"):
                live.pop(owner_name, None)
                continue
            parent_handle = await harness.conversation(owner["conversation"], context)
            made = (
                _owned_task
                if random() < 0.5
                else _owned_conversation_with_reader
            )
            created = await parent_handle.commit(
                partial(made, name=name, owner_id=owner["id"], into=owner["conversation"]), context
            )
            if created["read"]:
                conversations.append(created["into"])
            live[name] = {"id": created["id"], "conversation": created["into"]}
            if created["read"]:
                await named(reads, name).promise
        else:
            ended_name = pick(list(live))[0]
            live.pop(ended_name, None)
            named(gates, ended_name).resolve()
        await quiet(harness)
        state = harness.scheduler_idle_state
        assert state["idleMismatches"] == 0, f"step {step}"
        full_checks = state["fullIdleChecks"]
    state = harness.scheduler_idle_state
    assert state["contexts"] > 5
    assert state["idleContexts"] > 0
    assert state["idleContexts"] < state["contexts"]
    assert full_checks == 0
    for gate in list(gates.values()):
        gate.resolve()
    await harness.close(context)


# ── wake() ─────────────────────────────────────────────────────────────────


def _record(id: int, state: Any, extra: dict[str, Any] | None = None) -> Any:
    value = {
        "id": id,
        "conversationId": ROOT_CONVERSATION_ID,
        "kind": Work.definition.name,
        "version": 1,
        "input": {"name": f"seeded{id}"},
        "state": state,
        "background": False,
        "abortRequested": False,
        **(extra or {}),
    }
    return {"type": "task", "value": value}


def _sleeping(until: int) -> dict[str, Any]:
    return {"status": "sleeping", "checkpoint": {"phase": "run"}, "until": until}


SLEEPS = _record(10, _sleeping(5_000))
PENDING = {"status": "pending", "checkpoint": {"phase": "run"}}


async def _wake_of(writes: list[StorageWrite], expected: dict[str, Any], clock: int = 1_000) -> None:
    reset()
    storage = MemoryStorage()
    await commit_writes(
        storage, [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}, *writes], context
    )
    opened = await open_tasks(storage, [Work], {"now": lambda: clock})
    harness = opened["harness"]
    await quiet(harness)
    assert harness.wake() == expected
    for gate in list(gates.values()):
        gate.resolve()
    await harness.close(context)


async def test_wake_with_a_sleeping_task() -> None:
    """Upstream ``:533``, row ``a sleeping task``."""

    await _wake_of([SLEEPS], {"active": False, "at": 5_000})


async def test_wake_with_an_abort_marked_sleeping_task_which_runs_its_handler() -> None:
    """Upstream ``:533``, row ``an abort-marked sleeping task``."""

    await _wake_of(
        [SLEEPS, _record(11, _sleeping(3_000), {"abortRequested": True})],
        {"active": True, "at": 5_000},
    )


async def test_wake_with_a_sleeping_task_that_is_due() -> None:
    """Upstream ``:533``, row ``a sleeping task that is due``."""

    await _wake_of([_record(11, _sleeping(500))], {"active": True})


async def test_wake_with_a_task_waiting_on_a_live_one() -> None:
    """Upstream ``:533``, row ``a task waiting on a live one``."""

    await _wake_of(
        [
            SLEEPS,
            _record(
                11,
                {"status": "waiting", "checkpoint": {"phase": "run"}, "on": [10], "policy": "allSettled"},
            ),
        ],
        {"active": False, "at": 5_000},
    )


async def test_wake_with_a_completing_task_holding_for_a_sleeping_child() -> None:
    """Upstream ``:533``, row ``a completing task holding for a sleeping child``."""

    await _wake_of(
        [
            _record(11, {"status": "completing", "outcome": {"status": "completed", "result": None}}),
            _record(10, _sleeping(5_000), {"owner": 11}),
        ],
        {"active": False, "at": 5_000},
    )


async def test_wake_with_a_task_no_definition_can_take() -> None:
    """Upstream ``:533``, row ``a task no definition can take``."""

    await _wake_of(
        [SLEEPS, _record(11, PENDING, {"kind": "test.idle.missing"})],
        {"active": False, "at": 5_000},
    )


async def test_wake_with_a_pending_task() -> None:
    """Upstream ``:533``, row ``a pending task``."""

    await _wake_of([SLEEPS, _record(11, PENDING)], {"active": True, "at": 5_000})


async def test_wake_with_an_invocation_running_finds_the_earliest_sleep() -> None:
    """Upstream ``:573``."""

    reset()
    storage = MemoryStorage()
    await commit_writes(
        storage, [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}, SLEEPS], context
    )
    opened = await open_tasks(storage, [Work], {"now": lambda: 1_000})
    harness = opened["harness"]
    harness.resume()
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(
            Work, {"name": "running"}, {"ownership": {"kind": "conversation"}}
        ),
        context,
    )

    async def running() -> bool:
        record = await harness.get_task(id, context)
        return record is not None and record["state"]["status"] == "running"

    await eventually(running)
    assert harness.wake() == {"active": True, "at": 5_000}
    named(gates, "running").resolve()
    await harness.wait_for_task(id, context)
    await quiet(harness)
    assert harness.wake() == {"active": False, "at": 5_000}
    await harness.close(context)


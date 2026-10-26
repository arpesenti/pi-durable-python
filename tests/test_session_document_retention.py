"""Translated v1.3.0 ``session-document-retention`` suite.

The Session's document cache is bounded by use: unattached documents leave it after the retention
period and load from Storage again; documents with a watch or a document state never leave it.
The two Harness-level cases at the end follow a settings change at its next use and compare agent
event streams with documents kept and evicted right after use.
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, cast

import pytest
from chat_support import assistant_message, chat_setup, faux_tool_call, open_chat
from session_support import (
    ControlledStorage,
    Deferred,
    context,
    create_conversation,
    eventually,
)
from tool_support import add_tool, tool

from pi_durable import MemoryStorage, define_doc, define_doc_family, watch_events
from pi_durable.ids import ConversationId
from pi_durable.session import session as session_module
from pi_durable.session.session import SessionImpl, set_default_document_retention
from pi_durable.types import Storage

CounterDoc = define_doc(
    {
        "kind": "test.retention.counter",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"count": 0},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)
RewindableDoc = define_doc(
    {
        "kind": "test.retention.rewindable",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": lambda: {"count": 0},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)
#: A third-party conversation family: one member per event.
EventDoc = define_doc_family(
    {
        "kind": "test.retention.event",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda seq: {"seq": seq},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)
SessionCounterDoc = define_doc(
    {
        "kind": "test.retention.session-counter",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"count": 0},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


def _failing_checkpoint(_value: Any, _ops: Any, _info: Any) -> bool:
    raise RuntimeError("checkpoint failed")


#: A session document whose checkpoint predicate throws, so the commit that changes it fails.
Failing = define_doc(
    {
        "kind": "test.retention.failing",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"count": 0},
        "checkpoint_when": _failing_checkpoint,
    }
)


class Clock:
    """A controllable wall clock."""

    now: int = 1_000


async def _noop() -> None:
    return None


async def noop_commit(_tx: Any) -> None:
    return None


def open_session(
    retention_ms: Any, set_timer: Any = None
) -> tuple[ControlledStorage, SessionImpl, Clock]:
    """A Session with a controllable clock and retention."""

    clock = Clock()
    storage = ControlledStorage()
    session = SessionImpl(
        cast(Storage, storage), lambda: clock.now, lambda: retention_ms, set_timer
    )
    return storage, session, clock


async def settle_line(session: SessionImpl) -> None:
    """Let the line run what is queued on it: an eviction job, then return."""

    await session.read_on_line(_noop)


async def bump(session: SessionImpl, conversation_id: ConversationId, by: int = 1) -> None:
    async def change(tx: Any) -> None:
        (await tx.doc(CounterDoc, conversation_id)).count += by

    await session.commit(change, context)


async def settled_empty(session: SessionImpl) -> bool:
    await settle_line(session)
    return session.document_cache_sizes["cached"] == 0


async def contains(values: list[int], value: int) -> bool:
    return value in values

#: The default retention of this run, as the suite setup set it.
_DEFAULT_FOR_RUN = (
    600_000
    if os.environ.get("PI_DURABLE_DOCUMENT_RETENTION_MS") is None
    else int(os.environ["PI_DURABLE_DOCUMENT_RETENTION_MS"])
)


async def test_evicts_an_unattached_document_once_its_retention_passed_and_loads_it_again() -> None:
    storage, session, clock = open_session(60_000)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id, 3)
    assert session.document_cache_sizes["cached"] > 0
    # Not yet: inside the retention period.
    clock.now += 59_000
    await bump(session, conversation_id, 0)
    await settle_line(session)
    assert session.document_cache_sizes["cached"] > 0
    # Past it, with no use since: the next commit evicts it.
    clock.now += 61_000
    await session.commit(noop_commit, context)
    await session.commit(lambda tx: tx.doc(SessionCounterDoc), context)
    await settle_line(session)
    reads = storage.document_read_count
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 3}
    assert storage.document_read_count > reads
    await session.close(context)


async def test_evicts_with_retention_0_right_after_use_on_the_line() -> None:
    _storage, session, _clock = open_session(0)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    await eventually(lambda: settled_empty(session))
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 1}
    await session.close(context)


async def test_never_evicts_an_observed_document_and_evicts_it_once_the_last_detaches() -> None:
    storage, session, clock = open_session(1_000)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    watch = await session.watch_doc(CounterDoc, conversation_id, context)
    state = await session.document_state(CounterDoc, conversation_id, context)
    assert watch is not None and state is not None
    seen: list[int] = []
    watch.start(
        lambda value, _ops, _context: seen.append(cast(int, value["count"]) if value else -1)
    )
    clock.now += 1_000_000
    await bump(session, conversation_id)
    await settle_line(session)
    # Still cached: a read long after its retention passed loads nothing from Storage.
    assert state.value is not None and state.value["count"] == 2
    await eventually(lambda: contains(seen, 2))
    reads = storage.document_read_count
    clock.now += 1_000_000
    await session.commit(lambda tx: tx.doc(SessionCounterDoc), context)
    await settle_line(session)
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 2}
    assert storage.document_read_count == reads
    await watch.stop()
    clock.now += 1_000_000
    await bump(session, conversation_id)
    await settle_line(session)
    assert state.value is not None and state.value["count"] == 3
    state.dispose()
    clock.now += 1_000_000
    await session.commit(noop_commit, context)
    await session.commit(lambda tx: tx.doc(SessionCounterDoc), context)
    await settle_line(session)
    # The session document was just used; the counter is gone.
    assert session.document_cache_sizes["cached"] == 1
    await session.close(context)


async def test_never_takes_a_tracker_from_a_transaction_that_holds_it() -> None:
    _storage, session, clock = open_session(0)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    inside = Deferred()
    release = Deferred()

    async def hold(tx: Any) -> None:
        draft = await tx.doc(CounterDoc, conversation_id)
        inside.resolve()
        await release.promise
        draft.count += 10

    commit = asyncio.ensure_future(session.commit(hold, context))
    await inside.promise
    # Retention passes while the transaction holds the document; eviction waits on the line.
    clock.now += 1_000_000
    release.resolve()
    await commit
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 11}
    await bump(session, conversation_id)
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 12}
    await session.close(context)


async def test_writes_after_a_reload_continue_the_stored_deltas_and_bases() -> None:
    storage, session, _clock = open_session(0)
    conversation_id = await create_conversation(session)
    writes: list[list[Any]] = []
    original = storage.commit

    async def record(batch: Any, commit_context: Any) -> Any:
        writes.append(list(batch))
        return await original(batch, commit_context)

    storage.commit = record  # type: ignore[assignment]
    for _ in range(5):
        await bump(session, conversation_id)
        await settle_line(session)
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 5}
    # Every bump after the first is a change of the same incarnation, never a second creation.
    creations = [
        write
        for batch in writes
        for write in batch
        if write["type"] == "document.create" and write["record"]["kind"] == CounterDoc.definition.kind
    ]
    assert len(creations) == 1
    await session.close(context)
    # And a fresh Session over the same storage reads the same value.
    reopened = SessionImpl(cast(Storage, storage.reopen()), None, lambda: 0)
    assert await reopened.snapshot(CounterDoc, conversation_id, context) == {"count": 5}
    await reopened.close(context)


async def test_keeps_history_and_forks_of_rewindable_documents_after_eviction() -> None:
    _storage, session, _clock = open_session(0)
    conversation_id = await create_conversation(session)

    async def first_commit(tx: Any) -> Any:
        (await tx.doc(RewindableDoc, conversation_id)).count = 1
        return (await tx.append_entry(conversation_id, {"kind": "note"}))["id"]

    first = await session.commit(first_commit, context)
    await settle_line(session)

    async def second_commit(tx: Any) -> Any:
        (await tx.doc(RewindableDoc, conversation_id)).count = 2
        return (await tx.append_entry(conversation_id, {"kind": "note"}))["id"]

    second = await session.commit(second_commit, context)
    await settle_line(session)
    assert await session.snapshot_as_of(RewindableDoc, conversation_id, first, context) == {"count": 1}
    assert await session.snapshot_as_of(RewindableDoc, conversation_id, second, context) == {"count": 2}

    async def fork(tx: Any) -> Any:
        child = await tx.fork_conversation(
            conversation_id, first, {"ownership": {"kind": "ownerless"}}
        )
        return child["id"]

    fork_id = await session.commit(fork, context)
    await settle_line(session)
    assert await session.snapshot(RewindableDoc, fork_id, context) == {"count": 1}
    assert await session.snapshot(RewindableDoc, conversation_id, context) == {"count": 2}
    await session.close(context)


async def test_bounds_a_third_party_family_with_a_member_per_event() -> None:
    _storage, session, _clock = open_session(0)
    conversation_id = await create_conversation(session)
    for seq in range(200):
        async def event(tx: Any, value: int = seq) -> None:
            await tx.doc(EventDoc, conversation_id, str(value), value)

        await session.commit(event, context)
    await eventually(lambda: settled_empty(session))
    assert await session.snapshot(EventDoc, conversation_id, "123", context) == {"seq": 123}
    await session.close(context)


async def test_keeps_the_cache_of_a_long_lived_session_bounded_across_many_conversations() -> None:
    _storage, session, clock = open_session(10_000)
    for _ in range(300):
        conversation_id = await create_conversation(session)
        await bump(session, conversation_id)
        clock.now += 1_000
    await settle_line(session)
    # Only what was used in the last 10 s, about ten conversations' worth.
    assert session.document_cache_sizes["cached"] <= 12
    await session.close(context)


async def test_never_changes_the_outcome_of_a_commit_when_the_clock_throws() -> None:
    clock: dict[str, Any] = {"now": 1_000, "fail": False}
    storage = ControlledStorage()

    def now() -> int:
        if clock["fail"]:
            raise RuntimeError("clock")
        return cast(int, clock["now"])

    session = SessionImpl(cast(Storage, storage), now, lambda: 0)
    conversation_id = await create_conversation(session)
    clock["fail"] = True
    await bump(session, conversation_id, 42)
    clock["fail"] = False
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 42}
    await session.close(context)


async def test_evicts_after_a_commit_whose_settlement_fails() -> None:
    _storage, session, clock = open_session(10_000)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    # Created first: the predicate runs only for changes to an existing document.
    await session.commit(lambda tx: tx.doc(Failing), context)
    await settle_line(session)

    async def failing_change(tx: Any) -> None:
        (await tx.doc(Failing)).count += 1
        clock.now += 1_000_000

    # Retention passes inside a commit whose checkpoint predicate throws while it settles.
    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await session.commit(failing_change, context)
    assert session.document_cache_sizes["cached"] == 0
    await session.close(context)


async def test_evicts_promptly_once_the_retention_is_shortened() -> None:
    retention: dict[str, Any] = {"ms": 600_000}
    storage = ControlledStorage()
    clock = Clock()
    session = SessionImpl(
        cast(Storage, storage), lambda: clock.now, lambda: cast(int, retention["ms"])
    )
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    retention["ms"] = 0
    clock.now += 2_000
    # A read, no commit: the sweep pending for ten minutes from now moves up.
    await session.snapshot(CounterDoc, conversation_id, context)
    await eventually(lambda: settled_empty(session))
    await session.close(context)


async def test_does_not_keep_a_replacement_cached_for_an_observer_of_the_retired_incarnation() -> None:
    _storage, session, _clock = open_session(0)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    state = await session.document_state(CounterDoc, conversation_id, context)
    assert state is not None

    async def retire(tx: Any) -> None:
        await tx.retire_doc(CounterDoc, conversation_id)

    await session.commit(retire, context)
    assert state.value is None
    await bump(session, conversation_id, 5)
    await eventually(lambda: settled_empty(session))
    assert await session.snapshot(CounterDoc, conversation_id, context) == {"count": 5}
    state.dispose()
    await session.close(context)


async def test_leaves_no_pin_behind_when_an_observers_load_loses_a_race_with_close() -> None:
    storage, session, _clock = open_session(0)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    await eventually(lambda: settled_empty(session))
    gate = storage.hold_find_document()
    attaching = asyncio.ensure_future(
        session.document_state(CounterDoc, conversation_id, context)
    )
    await gate.entered.wait()
    closing = asyncio.ensure_future(session.close(context))
    gate.release()
    with pytest.raises(RuntimeError, match="Session is closed"):
        await attaching
    await closing
    cache = session.document_cache_sizes
    assert cache["cached"] == 0
    assert cache["idle"] == 0
    assert cache["attached"] == 0


async def test_evicts_on_its_timer_without_a_commit_and_tolerates_a_detach_called_twice() -> None:
    storage = ControlledStorage()
    session = SessionImpl(cast(Storage, storage), None, lambda: 20)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    state = await session.document_state(CounterDoc, conversation_id, context)
    assert state is not None
    state.dispose()
    state.dispose()
    async def cache_empty() -> bool:
        return session.document_cache_sizes["cached"] == 0

    await eventually(cache_empty)
    await session.close(context)


async def test_migrates_a_document_again_after_its_eviction() -> None:
    V1 = define_doc(
        {
            "kind": "test.retention.migrating",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 1},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    migrations = 0

    def migrate(value: Any, _from_version: int) -> dict[str, Any]:
        nonlocal migrations
        migrations += 1
        return {"count": value["count"], "migrated": True}

    V2 = define_doc(
        {
            "kind": "test.retention.migrating",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0, "migrated": True},
            "migrate": migrate,
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    _storage, session, _clock = open_session(0)
    await session.commit(lambda tx: tx.doc(V1), context)
    await settle_line(session)
    assert await session.snapshot(V2, context) == {"count": 1, "migrated": True}
    await settle_line(session)
    assert await session.snapshot(V2, context) == {"count": 1, "migrated": True}
    # Not written back, so each cold load migrates again.
    assert migrations == 2
    await session.close(context)


async def test_treats_a_retention_that_is_not_a_number_of_milliseconds_as_the_default() -> None:
    # The default as shipped, also in the suite run that forces another.
    set_default_document_retention(600_000)
    try:
        for retention in (float("nan"), -5, "soon"):
            _storage, session, clock = open_session(retention)
            conversation_id = await create_conversation(session)
            await bump(session, conversation_id)
            clock.now += 60_000
            await session.commit(noop_commit, context)
            await settle_line(session)
            # Not dropped at once, as a retention of 0 would: the ten-minute default holds it.
            assert session.document_cache_sizes["cached"] == 1
            await session.close(context)
        _storage, session, clock = open_session(float("inf"))
        conversation_id = await create_conversation(session)
        await bump(session, conversation_id)
        clock.now += 10**15
        await session.commit(noop_commit, context)
        await settle_line(session)
        assert session.document_cache_sizes["cached"] == 1
        await session.close(context)
    finally:
        set_default_document_retention(_DEFAULT_FOR_RUN)


async def test_keeps_no_timer_where_the_host_declines_background_timers_and_evicts_on_commits() -> None:
    attempts = 0

    def declining(_callback: Any, _delay_ms: float, *, background: bool = False) -> Any:
        nonlocal attempts
        if background:
            attempts += 1
        return None

    # Where the host declines background timers, only commits evict (spec §4).
    _storage, session, clock = open_session(1_000, declining)
    conversation_id = await create_conversation(session)
    await bump(session, conversation_id)
    await bump(session, conversation_id)
    assert attempts <= 1
    clock.now += 10_000
    await session.commit(noop_commit, context)
    await settle_line(session)
    assert session.document_cache_sizes["cached"] == 0
    await session.close(context)


async def test_keeps_a_reloaded_newer_version_cached_while_an_older_observer_is_attached() -> None:
    V1 = define_doc(
        {
            "kind": "test.retention.versioned",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"count": 3},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    V2 = define_doc(
        {
            "kind": "test.retention.versioned",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"count": 0, "v2": True},
            "migrate": lambda value, _from_version: {"count": value["count"], "v2": True},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    _storage, session, _clock = open_session(0)
    await session.commit(lambda tx: tx.doc(V1), context)
    state = await session.document_state(V1, context)
    assert state is not None
    assert await session.snapshot(V2, context) == {"count": 3, "v2": True}
    await settle_line(session)
    assert session.document_cache_sizes["cached"] == 1
    state.dispose()
    await eventually(lambda: settled_empty(session))
    await session.close(context)


async def test_queues_one_sweep_job_not_one_per_document_while_a_commit_holds_the_line() -> None:
    _storage, session, _clock = open_session(0)
    conversation_ids: list[ConversationId] = []
    for _ in range(50):
        conversation_id = await create_conversation(session)
        await bump(session, conversation_id)
        conversation_ids.append(conversation_id)
    await eventually(lambda: settled_empty(session))
    before = session.document_cache_sizes["sweeps"]

    # One commit loads 50 evicted documents from storage, each touched as it loads; the sweep job
    # stays pending while the line is held.
    async def touch_all(tx: Any) -> None:
        for conversation_id in conversation_ids:
            (await tx.doc(CounterDoc, conversation_id)).count += 1
            await asyncio.sleep(0)

    await session.commit(touch_all, context)
    await settle_line(session)
    assert session.document_cache_sizes["cached"] == 0
    assert session.document_cache_sizes["sweeps"] - before <= 2
    await session.close(context)


async def test_follows_a_harness_settings_change_at_the_next_use() -> None:
    previous = session_module._default_document_retention
    set_default_document_retention(600_000)
    try:
        setup = chat_setup()
        opened = await open_chat(MemoryStorage(), setup)
        harness, root = opened["harness"], opened["root"]

        async def bump(tx: Any) -> None:
            (await tx.doc(CounterDoc, root.id))["count"] += 1

        await root.commit(bump, context)
        assert harness.document_cache_sizes["cached"] > 0
        setup.settings["document_retention_ms"] = 0
        await harness.snapshot(CounterDoc, root.id, context)

        async def settled() -> bool:
            await harness.read_on_line(lambda: asyncio.sleep(0))
            return harness.document_cache_sizes["cached"] == 0

        await eventually(settled)
        await harness.close(context)
    finally:
        set_default_document_retention(previous)


async def test_gives_the_same_events_with_documents_evicted_right_after_use_as_with_documents_kept() -> (
    None
):
    async def run(document_retention_ms: int) -> list[str]:
        setup = chat_setup()
        setup.settings["document_retention_ms"] = document_retention_ms
        setup.settings["progress"] = {"output_interval_ms": 0}

        async def execute(_args: Any, api: Any, _context: Any) -> dict[str, Any]:
            for step in ("a\n", "b\n", "c\n"):
                api.output(step)
                await asyncio.sleep(0.005)
            return {}

        add_tool(setup.registry, tool("steps", execute))
        setup.faux.set_responses(
            [
                assistant_message(
                    [
                        faux_tool_call("steps", {}, id="s1"),
                    ],
                    stop_reason="toolUse",
                ),
                assistant_message([{"type": "text", "text": "done"}]),
            ]
        )
        opened = await open_chat(MemoryStorage(), setup)
        harness, root = opened["harness"], opened["root"]
        events: list[Any] = []
        stream = await watch_events(harness, root.id, context)
        stream.start(lambda batch, _context: events.extend(batch))
        await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
        await harness.wait_for_idle(context)
        await stream.stop()
        await harness.close(context)
        # Ids and times differ between runs; the sequence of event types and tool outputs must not.
        return [
            f"update:{json.dumps(event['output'], sort_keys=True)}"
            if event["type"] == "tool_execution_update"
            else event["type"]
            for event in events
        ]

    assert await run(0) == await run(600_000)

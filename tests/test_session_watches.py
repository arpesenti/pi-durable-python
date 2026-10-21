"""Translated v1.3.0 ``session-watches`` suite.

Covers exact committed frames, serialized callbacks, bounded pending frames with full-value
overflow, retirement, close, cancellation, listener failure, and continued observation after the
tracker cache unloads.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from session_support import (
    Deferred,
    context,
    document_changes,
    flush,
    open_test_session,
)

from pi_durable import define_doc
from pi_durable.context import (
    BACKGROUND_CONTEXT,
    AbortController,
    AbortError,
    ContextKey,
    create_context_key,
    with_abort_signal,
    with_cancel,
    with_context_value,
)
from pi_durable.delta import apply_immutable
from pi_durable.session.session import SessionImpl

StateDoc = define_doc(
    {
        "kind": "watch.state",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"value": 0, "items": ["a", "b"], "retained": {"label": "stable"}},
    }
)


async def create_state() -> tuple[Any, SessionImpl, list[Any]]:
    storage, session, publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(StateDoc)

    await session.commit(create, context)
    return storage, session, publications


async def set_value(session: SessionImpl, value: int) -> None:
    async def change(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = value

    await session.commit(change, context)


async def test_never_creates_an_absent_document() -> None:
    storage, session, _publications = open_test_session()
    commits = len(storage.commits)
    assert await session.watch_doc(StateDoc, context) is None
    assert len(storage.commits) == commits
    assert storage.mint_count == 0


async def test_keeps_the_acquisition_revision_until_start_and_delivers_exact_committed_frames() -> None:
    _storage, session, publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    initial = watch.value
    await set_value(session, 1)
    await flush()
    first_published = document_changes(publications[-1])[0]
    await set_value(session, 2)
    await flush()
    second_published = document_changes(publications[-1])[0]
    assert watch.value is initial

    inline = True
    deliveries: list[tuple[Any, Any]] = []

    def listener(value: Any, ops: Any, _delivery_context: Any) -> None:
        assert inline is False
        assert watch.value is value
        deliveries.append((value, ops))

    watch.start(listener)
    inline = False
    assert deliveries == []
    await flush()
    assert [value["value"] for value, _ops in deliveries] == [1, 2]
    assert deliveries[0][0] is first_published["value"]
    assert deliveries[0][1] is first_published["ops"]
    assert deliveries[1][0] is second_published["value"]
    assert deliveries[1][1] is second_published["ops"]
    assert initial is not None and initial["value"] == 0
    await watch.stop()


async def test_serializes_callbacks_and_buffers_exact_frames_committed_while_one_is_in_flight() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    values: list[int] = []
    active = 0
    max_active = 0

    async def listener(value: Any, _ops: Any, _delivery_context: Any) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        assert value is not None
        values.append(value["value"])
        if len(values) == 1:
            entered.resolve()
            await release.promise
        active -= 1

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise
    for value in range(2, 21):
        await set_value(session, value)
    assert values == [1]
    release.resolve()
    await flush()
    assert max_active == 1
    assert values == list(range(1, 21))
    await watch.stop()


async def test_allows_a_listener_to_initiate_a_later_session_commit() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    completed = Deferred()
    values: list[int] = []

    async def listener(value: Any, _ops: Any, _delivery_context: Any) -> None:
        assert value is not None
        current = value["value"]
        values.append(current)
        if current == 1:
            await set_value(session, 2)
        else:
            completed.resolve()

    watch.start(listener)
    await set_value(session, 1)
    await completed.promise
    assert values == [1, 2]
    await watch.stop()


async def test_collapses_101_pending_commits_to_one_root_replacement() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    for value in range(1, 102):
        await set_value(session, value)
    deliveries: list[dict[str, Any]] = []

    def listener(value: Any, ops: Any, _delivery_context: Any) -> None:
        deliveries.append({"value": value, "ops": ops})

    watch.start(listener)
    await flush()
    assert len(deliveries) == 1
    assert deliveries[0]["value"] is watch.value
    assert cast(dict[str, Any], watch.value)["value"] == 101
    assert deliveries[0]["ops"] == [["r", watch.value]]
    await watch.stop()


async def test_never_folds_the_in_flight_frame_into_an_overflow_reset() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    deliveries: list[tuple[Any, Any]] = []

    async def listener(value: Any, ops: Any, _delivery_context: Any) -> None:
        deliveries.append((value, ops))
        if len(deliveries) == 1:
            entered.resolve()
            await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise
    for value in range(2, 103):
        await set_value(session, value)
    release.resolve()
    await flush()
    assert deliveries[0][0]["value"] == 1
    assert len(deliveries) == 2
    assert deliveries[1][0] is watch.value
    assert cast(dict[str, Any], watch.value)["value"] == 102
    assert deliveries[1][1] == [["r", watch.value]]
    await watch.stop()


async def test_folds_retirement_into_an_overflow_reset_and_then_closes() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    for value in range(1, 101):
        await set_value(session, value)

    async def retire(tx: Any) -> None:
        await tx.retire_doc(StateDoc)

    await session.commit(retire, context)
    deliveries: list[tuple[Any, Any]] = []

    def listener(value: Any, ops: Any, _delivery_context: Any) -> None:
        deliveries.append((value, ops))

    watch.start(listener)
    assert await watch.closed == {"reason": "retired"}
    assert deliveries == [(None, [["r", None]])]


async def test_delivers_replayable_structural_no_op_commits_instead_of_suppressing_them() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    initial = watch.value

    async def reorder(tx: Any) -> None:
        state = await tx.doc(StateDoc)
        first = state["items"].shift()
        state["items"].unshift(first)

    await session.commit(reorder, context)
    newest = await session.snapshot(StateDoc, context)
    assert newest is not initial
    assert newest == initial
    batches: list[Any] = []

    def listener(value: Any, ops: Any, _delivery_context: Any) -> None:
        assert value is newest
        batches.append(ops)

    watch.start(listener)
    await flush()
    assert len(batches) == 1
    assert len(batches[0]) > 0
    await watch.stop()


async def test_preserves_commit_context_values_without_inheriting_producer_cancellation() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    key: ContextKey[str] = create_context_key("watch-test")
    parent_controller = AbortController()
    commit_context = with_context_value(
        key,
        "newest-commit",
        with_abort_signal(parent_controller.signal, BACKGROUND_CONTEXT),
    )
    entered = Deferred()
    release = Deferred()
    delivery_context: Any = None

    async def listener(_value: Any, _ops: Any, delivered: Any) -> None:
        nonlocal delivery_context
        delivery_context = delivered
        entered.resolve()
        await release.promise

    watch.start(listener)

    async def change(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 1

    await session.commit(change, commit_context)
    await entered.promise
    assert delivery_context.value(key) == "newest-commit"
    assert delivery_context.abort_signal is None
    parent_controller.abort("caller finished")
    stopped = watch.stop()
    assert await stopped == {"reason": "stopped"}
    assert delivery_context.abort_signal is None
    release.resolve()


async def test_keeps_earlier_immutable_revisions_stable() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    initial = watch.value
    delivered: Any = None

    def listener(value: Any, _ops: Any, _delivery_context: Any) -> None:
        nonlocal delivered
        delivered = value

    watch.start(listener)
    await set_value(session, 1)
    await flush()
    assert delivered is watch.value
    assert delivered is not initial
    assert cast(dict[str, Any], delivered)["retained"] is cast(dict[str, Any], initial)["retained"]
    assert cast(dict[str, Any], initial)["value"] == 0
    await watch.stop()


async def test_delivers_retirement_and_does_not_follow_recreation() -> None:
    _storage, session, _publications = await create_state()
    old_watch = await session.watch_doc(StateDoc, context)
    assert old_watch is not None
    values: list[Any] = []

    def listener(value: Any, _ops: Any, _delivery_context: Any) -> None:
        values.append(value)

    old_watch.start(listener)

    async def replace(tx: Any) -> None:
        await tx.retire_doc(StateDoc)
        (await tx.doc(StateDoc)).value = 10

    await session.commit(replace, context)
    assert await old_watch.closed == {"reason": "retired"}
    assert values == [None]
    assert old_watch.value is None

    replacement = await session.watch_doc(StateDoc, context)
    assert replacement is not None
    assert replacement.value is not None and replacement.value["value"] == 10
    await set_value(session, 11)
    await flush()
    assert old_watch.value is None
    await replacement.stop()


async def test_session_close_discards_retirement_buffered_before_start() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    baseline = watch.value

    async def retire(tx: Any) -> None:
        await tx.retire_doc(StateDoc)

    await session.commit(retire, context)
    await session.close(context)
    assert await watch.closed == {"reason": "session_closed"}
    assert watch.value is baseline
    with pytest.raises(RuntimeError, match="stopped"):
        watch.start(lambda _value, _ops, _context: None)


async def test_session_close_discards_retirement_behind_an_in_flight_callback() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    values: list[Any] = []

    async def listener(value: Any, _ops: Any, _delivery_context: Any) -> None:
        values.append(value)
        entered.resolve()
        await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise

    async def retire(tx: Any) -> None:
        await tx.retire_doc(StateDoc)

    await session.commit(retire, context)
    await session.close(context)
    assert await watch.closed == {"reason": "session_closed"}
    release.resolve()
    await flush()
    assert values[0] is not None and values[0]["value"] == 1
    assert len(values) == 1


async def test_supports_idempotent_stop_and_rejects_repeated_or_late_start() -> None:
    _storage, session, _publications = await create_state()
    started = await session.watch_doc(StateDoc, context)
    assert started is not None
    started.start(lambda _value, _ops, _context: None)
    with pytest.raises(RuntimeError, match="already started"):
        started.start(lambda _value, _ops, _context: None)
    first = started.stop()
    second = started.stop()
    assert second is first
    assert await first == {"reason": "stopped"}

    stopped = await session.watch_doc(StateDoc, context)
    assert stopped is not None
    await stopped.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        stopped.start(lambda _value, _ops, _context: None)


async def test_cancels_acquisition_without_leaking_a_registered_watch() -> None:
    storage, session, _publications = await create_state()
    await session.unload_documents()
    gate = storage.hold_find_document()
    child = with_cancel(context)
    acquisition = asyncio.ensure_future(session.watch_doc(StateDoc, child.context))
    await gate.entered.wait()
    child.cancel(RuntimeError("cancel acquisition"))
    gate.release()
    with pytest.raises(AbortError) as cancelled:
        await acquisition
    assert str(cancelled.value.reason) == "cancel acquisition"
    await session.close(context)


async def test_leaves_no_watch_behind_when_an_acquisitions_load_loses_a_race_with_close() -> None:
    storage, session, _publications = await create_state()
    await session.unload_documents()
    gate = storage.hold_find_document()
    attaching = asyncio.ensure_future(session.watch_doc(StateDoc, context))
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


async def test_cancels_future_delivery_without_aborting_an_in_flight_callback() -> None:
    _storage, session, _publications = await create_state()
    child = with_cancel(context)
    watch = await session.watch_doc(StateDoc, child.context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    delivery_context: Any = None

    async def listener(_value: Any, _ops: Any, delivered: Any) -> None:
        nonlocal delivery_context
        delivery_context = delivered
        entered.resolve()
        await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise
    child.cancel()
    assert await watch.closed == {"reason": "cancelled"}
    assert delivery_context.abort_signal is None
    release.resolve()


async def test_session_close_stops_future_delivery_without_joining_an_in_flight_callback() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    delivery_context: Any = None

    async def listener(_value: Any, _ops: Any, delivered: Any) -> None:
        nonlocal delivery_context
        delivery_context = delivered
        entered.resolve()
        await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise
    await session.close(context)
    assert delivery_context.abort_signal is None
    assert await watch.closed == {"reason": "session_closed"}
    release.resolve()


async def test_settles_listener_failure_on_only_the_affected_watch() -> None:
    _storage, session, _publications = await create_state()
    failed = await session.watch_doc(StateDoc, context)
    healthy = await session.watch_doc(StateDoc, context)
    assert failed is not None and healthy is not None

    def failing_listener(_value: Any, _ops: Any, _delivery_context: Any) -> None:
        raise RuntimeError("listener failed")

    failed.start(failing_listener)
    healthy_calls = 0

    def healthy_listener(_value: Any, _ops: Any, _delivery_context: Any) -> None:
        nonlocal healthy_calls
        healthy_calls += 1

    healthy.start(healthy_listener)
    await set_value(session, 1)
    end = cast(dict[str, Any], await failed.closed)
    assert end["reason"] == "listener_error"
    assert str(end["error"]) == "listener failed"
    await flush()
    assert healthy_calls == 1
    await healthy.stop()


async def test_hydrates_migration_without_writing_and_observes_the_later_exact_edit() -> None:
    Old = define_doc(
        {"kind": "watch.migration", "version": 1, "scope": "session", "initial": lambda: {"value": 3}}
    )
    Current = define_doc(
        {
            "kind": "watch.migration",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"value": 0, "migrated": False},
            "migrate": lambda value, _from_version: {"value": value["value"], "migrated": True},
        }
    )
    storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    await session.unload_documents()
    commits = len(storage.commits)
    watch = await session.watch_doc(Current, context)
    assert watch is not None
    assert watch.value == {"value": 3, "migrated": True}
    assert len(storage.commits) == commits
    values: list[int] = []

    def listener(value: Any, _ops: Any, _delivery_context: Any) -> None:
        assert value is not None
        values.append(value["value"])

    watch.start(listener)

    async def touch(tx: Any) -> None:
        await tx.doc(Current)

    await session.commit(touch, context)
    await flush()
    assert values == []

    async def edit(tx: Any) -> None:
        (await tx.doc(Current)).value = 4

    await session.commit(edit, context)
    await flush()
    assert values == [4]
    await watch.stop()


async def test_can_replay_every_delivered_exact_operation_batch_from_the_acquisition_revision() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    replica: Any = watch.value

    def listener(value: Any, ops: Any, _delivery_context: Any) -> None:
        nonlocal replica
        replica = apply_immutable(replica, ops)
        assert replica == value

    watch.start(listener)

    async def mutate(tx: Any) -> None:
        state = await tx.doc(StateDoc)
        state.value = 3
        del state["items"][0]

    await session.commit(mutate, context)
    await flush()
    assert replica == watch.value
    await watch.stop()


async def test_continues_after_the_tracker_cache_unloads() -> None:
    storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    baseline = watch.value
    reads = storage.document_read_count
    await session.unload_documents()
    reloaded = await session.snapshot(StateDoc, context)
    assert reloaded == baseline
    assert reloaded is not baseline
    assert storage.document_read_count > reads
    watch.start(lambda _value, _ops, _context: None)
    await set_value(session, 7)
    await flush()
    assert watch.value is not None and watch.value["value"] == 7
    await watch.stop()


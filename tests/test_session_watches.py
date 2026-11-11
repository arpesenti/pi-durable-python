"""Translated v1.5.0 ``session-watches`` suite.

Covers coalesced deliveries with folded operations, the pending-operation root replacement,
serialized callbacks, retirement as a change and a watch that follows its address through it, close,
cancellation, listener failure, and continued observation after the tracker cache unloads. The five
watch ends are the shipped build's (``chord/src/watch.ts:16-19``): ``stopped``, ``aborted``,
``ended``, ``listener_error``, and ``failed``.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from functools import partial
from typing import Any, cast

import pytest
from session_support import (
    Deferred,
    context,
    create_conversation,
    document_changes,
    flush,
    open_test_session,
)

from pi_durable import MemoryStorage, define_doc, define_doc_family
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
from pi_durable.session.observation import CommittedWatch
from pi_durable.session.session import SessionImpl
from pi_durable.types import Storage

StateDoc = define_doc(
    {
        "kind": "watch.state",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"value": 0, "items": ["a", "b"], "retained": {"label": "stable"}},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


KeysDoc = define_doc(
    {
        "kind": "watch.keys",
        "version": 1,
        "scope": "session",
        "initial": lambda: {"keys": {}},
        "checkpoint_when": lambda _value, _ops, _info: False,
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


async def test_keeps_the_acquisition_revision_until_start_then_delivers_commits_since_in_one_call() -> None:
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
    deliveries: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        assert inline is False
        assert watch.value is call["value"]
        deliveries.append({"value": call["value"], "ops": call["ops"]})

    watch.start(listener)
    inline = False
    assert deliveries == []
    await flush()
    # Both commits set `value`: folded, the second set replaces the first.
    assert first_published["ops"] == [["s", ["value"], 1]]
    assert deliveries == [{"value": second_published["value"], "ops": second_published["ops"]}]
    assert deliveries[0]["value"] is second_published["value"]
    assert initial is not None and initial["value"] == 0

    # A listener that keeps up gets each commit's ops as published.
    await set_value(session, 3)
    await flush()
    third_published = document_changes(publications[-1])[0]
    assert len(deliveries) == 2
    assert deliveries[1]["value"] is third_published["value"]
    assert deliveries[1]["ops"] is third_published["ops"]
    await watch.stop()


async def test_serializes_callbacks_and_folds_the_commits_made_while_one_is_in_flight_into_the_next() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    values: list[int] = []
    batches: list[Any] = []
    active = 0
    max_active = 0

    async def listener(call: Any, _delivery_context: Any) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        assert call["value"] is not None
        values.append(call["value"]["value"])
        batches.append(call["ops"])
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
    assert values == [1, 20]
    assert batches[1] == [["s", ["value"], 20]]
    await watch.stop()


async def test_joins_the_appends_made_while_one_is_in_flight_under_the_newest_commit_context() -> None:
    """What waits behind the listener arrives as one batch, under one context (spec §9.2).

    Upstream's suite pins the folded per-field set and the 10,000-operation root replacement, but
    never that appends join nor that the folded call carries the newest commit's ``Context``, so the
    port pins both: two grown strings become one ``["a", path, text]``, and the delivery context is
    the last commit's.
    """

    Transcript = define_doc(
        {
            "kind": "watch.transcript",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    _storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Transcript)

    await session.commit(create, context)
    watch = await session.watch_doc(Transcript, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    key: ContextKey[str] = create_context_key("folded")
    calls: list[dict[str, Any]] = []
    delivered: list[Any] = []

    async def listener(call: Any, delivery_context: Any) -> None:
        calls.append({"value": call["value"], "ops": call["ops"]})
        delivered.append(delivery_context)
        if len(calls) == 1:
            entered.resolve()
            await release.promise

    watch.start(listener)

    async def append_a(tx: Any) -> None:
        (await tx.doc(Transcript)).text += "a"

    async def append_b(tx: Any) -> None:
        (await tx.doc(Transcript)).text += "b"

    async def append_c(tx: Any) -> None:
        (await tx.doc(Transcript)).text += "c"

    await session.commit(append_a, with_context_value(key, "first", context))
    await entered.promise
    await session.commit(append_b, with_context_value(key, "second", context))
    await session.commit(append_c, with_context_value(key, "third", context))
    release.resolve()
    await flush()
    assert [call["ops"] for call in calls] == [[["a", ["text"], "a"]], [["a", ["text"], "bc"]]]
    assert calls[1]["value"] == {"text": "abc"}
    assert delivered[1].value(key) == "third"
    await watch.stop()


async def test_allows_a_listener_to_initiate_a_later_session_commit() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    completed = Deferred()
    values: list[int] = []

    async def listener(call: Any, _delivery_context: Any) -> None:
        value = call["value"]
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


async def test_folds_101_pending_commits_of_one_field_into_one_set() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    for value in range(1, 102):
        await set_value(session, value)
    deliveries: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        assert call["value"] is not None
        deliveries.append({"value": call["value"]["value"], "ops": call["ops"]})

    watch.start(listener)
    await flush()
    assert deliveries == [{"value": 101, "ops": [["s", ["value"], 101]]}]
    await watch.stop()


async def test_never_folds_the_in_flight_frame_into_the_next_call() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    deliveries: list[dict[str, Any]] = []

    async def listener(call: Any, _delivery_context: Any) -> None:
        assert call["value"] is not None
        deliveries.append({"value": call["value"]["value"], "ops": call["ops"]})
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
    assert deliveries[0]["value"] == 1
    assert deliveries[1] == {"value": 102, "ops": [["s", ["value"], 102]]}
    assert len(deliveries) == 2
    await watch.stop()


async def test_replaces_the_root_once_more_than_10000_ops_are_pending_after_folding() -> None:
    _storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(KeysDoc)

    await session.commit(create, context)
    watch = await session.watch_doc(KeysDoc, context)
    assert watch is not None
    # Sets of distinct keys, in commits small enough to stay operations: 4,000 + 4,000 + 2,001.
    for start, stop in ((0, 4_000), (4_000, 8_000), (8_000, 10_001)):

        async def fill(tx: Any, start: int = start, stop: int = stop) -> None:
            state = await tx.doc(KeysDoc)
            for index in range(start, stop):
                state["keys"][f"k{index}"] = index

        await session.commit(fill, context)
    deliveries: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        deliveries.append({"value": call["value"], "ops": call["ops"]})

    watch.start(listener)
    await flush()
    assert deliveries == [{"value": watch.value, "ops": [["r", watch.value]]}]
    assert len(cast(dict[str, Any], watch.value)["keys"]) == 10_001
    await watch.stop()


async def test_folds_a_retirement_into_the_pending_commits_call_with_the_last_value_and_goes_on() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    for value in range(1, 101):
        await set_value(session, value)
    last = await session.snapshot(StateDoc, context)
    assert last is not None

    async def retire(tx: Any) -> None:
        await tx.retire_doc(StateDoc)

    await session.commit(retire, context)
    changes: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        changes.append(dict(call))

    watch.start(listener)
    await flush()
    assert changes == [
        {"kind": "value", "value": last, "ops": [["s", ["value"], 100]], "retired": True}
    ]
    assert changes[0]["value"] is last
    assert watch.value is last

    async def recreate(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 7

    await session.commit(recreate, context)
    await flush()
    recreated = {**last, "value": 7}
    assert changes[1] == {
        "kind": "value",
        "value": recreated,
        "ops": [["r", recreated]],
        "retired": False,
    }
    await watch.stop()


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

    def listener(call: Any, _delivery_context: Any) -> None:
        assert call["value"] is newest
        batches.append(call["ops"])

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

    async def listener(_call: Any, delivered: Any) -> None:
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
    assert await stopped == {"kind": "stopped"}
    assert delivery_context.abort_signal is None
    release.resolve()


async def test_keeps_earlier_immutable_revisions_stable() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    initial = watch.value
    delivered: Any = None

    def listener(call: Any, _delivery_context: Any) -> None:
        nonlocal delivered
        delivered = call["value"]

    watch.start(listener)
    await set_value(session, 1)
    await flush()
    assert delivered is watch.value
    assert delivered is not initial
    assert cast(dict[str, Any], delivered)["retained"] is cast(dict[str, Any], initial)["retained"]
    assert cast(dict[str, Any], initial)["value"] == 0
    await watch.stop()


async def test_reports_a_change_and_a_retirement_in_one_commit_as_the_retirement() -> None:
    storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    before = watch.value
    changes: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        changes.append(dict(call))

    watch.start(listener)

    async def change_and_retire(tx: Any) -> None:
        (await tx.doc(StateDoc)).value = 5
        await tx.retire_doc(StateDoc)

    await session.commit(change_and_retire, context)
    await flush()
    # Storage got the change, then the retirement; the commit publishes the retirement only.
    assert [write["type"] for write in storage.commits[-1]] == [
        "document.change",
        "document.retire",
    ]
    assert changes == [{"kind": "value", "value": before, "ops": [], "retired": True}]
    assert changes[0]["value"] is before
    await watch.stop()


async def test_follows_its_address_across_a_retirement_and_a_recreation_in_one_commit_in_one_call() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    ended: list[Any] = []
    changes: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        changes.append(dict(call))

    watch.closed.add_done_callback(lambda future: ended.append(future.result()))
    watch.start(listener)

    async def retire_and_recreate(tx: Any) -> None:
        await tx.retire_doc(StateDoc)
        (await tx.doc(StateDoc)).value = 10

    await session.commit(retire_and_recreate, context)
    await flush()
    recreated = await session.snapshot(StateDoc, context)
    assert recreated is not None
    assert len(changes) == 1
    assert changes[0] == {
        "kind": "value",
        "value": recreated,
        "ops": [["r", recreated]],
        "retired": True,
    }

    await set_value(session, 11)
    await flush()
    assert changes[1] == {
        "kind": "value",
        "value": watch.value,
        "ops": [["s", ["value"], 11]],
        "retired": False,
    }
    assert cast(dict[str, Any], watch.value)["value"] == 11
    # A watch follows its address, so neither the retirement nor the recreation ended it.
    assert ended == []
    await watch.stop()


async def test_ignores_the_same_kind_in_another_conversation_and_under_another_family_key() -> None:
    Notes = define_doc(
        {
            "kind": "watch.scoped-notes",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "initial",
            "initial": lambda: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    Members = define_doc_family(
        {
            "kind": "watch.members",
            "version": 1,
            "family": True,
            "scope": "session",
            "initial": lambda _seed: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    _storage, session, _publications = open_test_session()
    watched = await create_conversation(session)
    other = await create_conversation(session)

    async def seed(tx: Any) -> None:
        (await tx.doc(Notes, watched)).text = "a"
        (await tx.doc(Members, "watched", None)).text = "a"

    await session.commit(seed, context)
    notes = await session.watch_doc(Notes, watched, context)
    member = await session.watch_doc(Members, "watched", context)
    assert notes is not None and member is not None
    changes: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        changes.append(dict(call))

    notes.start(listener)
    member.start(listener)
    # The same kinds at other addresses: created, changed, retired, and created again.
    async def change_other(tx: Any) -> None:
        (await tx.doc(Notes, other)).text = "other"
        (await tx.doc(Members, "other", None)).text = "other"

    async def retire_other(tx: Any) -> None:
        await tx.retire_doc(Notes, other)
        await tx.retire_doc(Members, "other")

    async def recreate_other(tx: Any) -> None:
        (await tx.doc(Notes, other)).text = "again"
        (await tx.doc(Members, "other", None)).text = "again"

    await session.commit(change_other, context)
    await session.commit(retire_other, context)
    await session.commit(recreate_other, context)
    await flush()
    assert changes == []
    assert session.document_cache_sizes["attached"] == 2
    await notes.stop()
    await member.stop()
    await session.close(context)


async def test_takes_the_version_of_a_recreation_written_by_a_newer_definition_then_its_ops() -> None:
    Old = define_doc(
        {
            "kind": "watch.recreated-version",
            "version": 1,
            "scope": "session",
            "initial": lambda: {"value": 1},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    Current = define_doc(
        {
            "kind": "watch.recreated-version",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"value": 0, "migrated": False},
            "migrate": lambda value, _from_version: {
                "value": value["value"],
                "migrated": True,
            },
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    _storage, session, _publications = open_test_session()

    async def create(tx: Any) -> None:
        await tx.doc(Old)

    await session.commit(create, context)
    watch = await session.watch_doc(Old, context)
    assert watch is not None
    changes: list[dict[str, Any]] = []

    def listener(call: Any, _delivery_context: Any) -> None:
        changes.append(dict(call))

    watch.start(listener)

    async def recreate(tx: Any) -> None:
        await tx.retire_doc(Current)
        (await tx.doc(Current)).value = 5

    await session.commit(recreate, context)
    await flush()

    async def edit(tx: Any) -> None:
        (await tx.doc(Current)).value = 6

    await session.commit(edit, context)
    await flush()
    recreated = {"value": 5, "migrated": False}
    assert changes == [
        {"kind": "value", "value": recreated, "ops": [["r", recreated]], "retired": True},
        {
            "kind": "value",
            "value": {"value": 6, "migrated": False},
            "ops": [["s", ["value"], 6]],
            "retired": False,
        },
    ]
    await watch.stop()
    await session.close(context)


async def test_session_close_discards_retirement_buffered_before_start() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    baseline = watch.value

    async def retire(tx: Any) -> None:
        await tx.retire_doc(StateDoc)

    await session.commit(retire, context)
    await session.close(context)
    assert await watch.closed == {"kind": "ended"}
    assert watch.value is baseline
    with pytest.raises(RuntimeError, match="stopped"):
        watch.start(lambda _call, _context: None)


async def test_session_close_discards_retirement_behind_an_in_flight_callback() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    values: list[Any] = []

    async def listener(call: Any, _delivery_context: Any) -> None:
        values.append(call["value"])
        entered.resolve()
        await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise

    async def retire(tx: Any) -> None:
        await tx.retire_doc(StateDoc)

    await session.commit(retire, context)
    await session.close(context)
    assert await watch.closed == {"kind": "ended"}
    release.resolve()
    await flush()
    assert values[0] is not None and values[0]["value"] == 1
    assert len(values) == 1


async def test_supports_idempotent_stop_and_rejects_repeated_or_late_start() -> None:
    _storage, session, _publications = await create_state()
    started = await session.watch_doc(StateDoc, context)
    assert started is not None
    started.start(lambda _call, _context: None)
    with pytest.raises(RuntimeError, match="already started"):
        started.start(lambda _call, _context: None)
    first = started.stop()
    second = started.stop()
    assert second is first
    assert await first == {"kind": "stopped"}

    stopped = await session.watch_doc(StateDoc, context)
    assert stopped is not None
    await stopped.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        stopped.start(lambda _call, _context: None)


async def test_stop_and_the_acquisition_abort_are_two_distinct_ends() -> None:
    """``stop()`` yields ``stopped`` and the acquisition context's abort yields ``aborted``.

    The changelog calls the second end ``cancelled``; the shipped build (``chord/src/watch.ts:16-19``
    and ``:46-55``) keeps them apart, and so does the port: whichever comes first is the end, and
    the other path reports it rather than overwriting it.
    """

    _storage, session, _publications = await create_state()

    stopped_first = with_cancel(context)
    by_stop = await session.watch_doc(StateDoc, stopped_first.context)
    assert by_stop is not None
    assert await by_stop.stop() == {"kind": "stopped"}
    stopped_first.cancel()
    assert await by_stop.closed == {"kind": "stopped"}

    aborted_first = with_cancel(context)
    by_abort = await session.watch_doc(StateDoc, aborted_first.context)
    assert by_abort is not None
    aborted_first.cancel()
    assert await by_abort.closed == {"kind": "aborted"}
    assert await by_abort.stop() == {"kind": "aborted"}


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

    async def listener(_call: Any, delivered: Any) -> None:
        nonlocal delivery_context
        delivery_context = delivered
        entered.resolve()
        await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise
    child.cancel()
    assert await watch.closed == {"kind": "aborted"}
    assert delivery_context.abort_signal is None
    release.resolve()


async def test_session_close_stops_future_delivery_without_joining_an_in_flight_callback() -> None:
    _storage, session, _publications = await create_state()
    watch = await session.watch_doc(StateDoc, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    delivery_context: Any = None

    async def listener(_call: Any, delivered: Any) -> None:
        nonlocal delivery_context
        delivery_context = delivered
        entered.resolve()
        await release.promise

    watch.start(listener)
    await set_value(session, 1)
    await entered.promise
    await session.close(context)
    assert delivery_context.abort_signal is None
    assert await watch.closed == {"kind": "ended"}
    release.resolve()


async def test_settles_listener_failure_on_only_the_affected_watch() -> None:
    _storage, session, _publications = await create_state()
    failed = await session.watch_doc(StateDoc, context)
    healthy = await session.watch_doc(StateDoc, context)
    assert failed is not None and healthy is not None

    def failing_listener(_call: Any, _delivery_context: Any) -> None:
        raise RuntimeError("listener failed")

    failed.start(failing_listener)
    healthy_calls = 0

    def healthy_listener(_call: Any, _delivery_context: Any) -> None:
        nonlocal healthy_calls
        healthy_calls += 1

    healthy.start(healthy_listener)
    await set_value(session, 1)
    end = cast(dict[str, Any], await failed.closed)
    assert end["kind"] == "listener_error"
    assert str(end["error"]) == "listener failed"
    await flush()
    assert healthy_calls == 1
    await healthy.stop()


async def test_hydrates_migration_without_writing_and_observes_the_later_exact_edit() -> None:
    Old = define_doc(
        {
            "kind": "watch.migration", "version": 1, "scope": "session", "initial": lambda: {"value": 3},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    Current = define_doc(
        {
            "kind": "watch.migration",
            "version": 2,
            "scope": "session",
            "initial": lambda: {"value": 0, "migrated": False},
            "migrate": lambda value, _from_version: {"value": value["value"], "migrated": True},
            "checkpoint_when": lambda _value, _ops, _info: False,
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

    def listener(call: Any, _delivery_context: Any) -> None:
        assert call["value"] is not None
        values.append(call["value"]["value"])

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

    def listener(call: Any, _delivery_context: Any) -> None:
        nonlocal replica
        replica = apply_immutable(replica, call["ops"])
        assert replica == call["value"]

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
    watch.start(lambda _call, _context: None)
    await set_value(session, 7)
    await flush()
    assert watch.value is not None and watch.value["value"] == 7
    await watch.stop()



def _constant(value: int) -> int:
    return value


async def collect() -> None:
    """Give the collection the upstream cases force with ``--expose-gc`` its CPython equivalent.

    CPython frees at the last reference and breaks cycles in ``gc.collect()``, which is the same
    guarantee V8 gives after ``gc()``: what nothing holds is gone.
    """

    for _round in range(5):
        await asyncio.sleep(0)
        gc.collect()


def survives(identity: int) -> bool:
    """Whether the object whose ``id`` one recorded is still reachable from the collector."""

    return any(id(candidate) == identity for candidate in gc.get_objects())


async def test_keeps_no_earlier_incarnation_alive_after_a_retirement_and_a_recreation_and_goes_on() -> None:
    """A watch follows its address without keeping the incarnation it started on alive.

    The upstream case holds the first incarnation behind a ``WeakRef`` and forces V8 to collect;
    CPython tracks the same JSON containers, so the identity scan proves the same property.
    """

    async def follow(
        session: SessionImpl, drop: bool
    ) -> tuple[CommittedWatch, int]:
        async def retain(tx: Any) -> None:
            state = await tx.doc(StateDoc)
            state.retained = {"label": "first incarnation"}

        await session.commit(retain, context)
        watch = await session.watch_doc(StateDoc, context)
        assert watch is not None
        first = id(cast(dict[str, Any], watch.value)["retained"])
        watch.start(lambda _call, _context: None)

        async def replace(tx: Any) -> None:
            await tx.retire_doc(StateDoc)
            (await tx.doc(StateDoc)).value = 2

        await session.commit(replace, context)
        await flush()
        if drop:
            await session.unload_documents()
        return watch, first

    # Retention 0 evicts the retired incarnation at once; the default keeps the cache until unloaded.
    for retention, drop in ((0, False), (600_000, True)):
        session = SessionImpl(MemoryStorage(), lambda: 1_000, partial(_constant, retention))
        watch, first = await follow(session, drop)
        await collect()
        assert cast(dict[str, Any], watch.value)["value"] == 2
        assert cast(dict[str, Any], watch.value)["retained"] == {"label": "stable"}
        assert not survives(first)
        await watch.stop()
        await session.close(context)


async def test_keeps_a_watch_of_a_retired_conversation_document_until_stopped_then_nothing_of_it() -> None:
    """A forgotten watch of a retired address stays until stopped, and then holds nothing."""

    Notes = define_doc(
        {
            "kind": "watch.notes",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "initial",
            "initial": lambda: {"text": ""},
            "checkpoint_when": lambda _value, _ops, _info: False,
        }
    )
    storage = MemoryStorage()
    session = SessionImpl(cast(Storage, storage), lambda: 1_000, lambda: 600_000)
    conversation_id = await create_conversation(session)

    async def acquire(
        stop: bool,
    ) -> tuple[weakref.ReferenceType[CommittedWatch], weakref.ReferenceType[Any]]:
        async def write(tx: Any) -> None:
            (await tx.doc(Notes, conversation_id)).text = "kept"

        await session.commit(write, context)
        watch = await session.watch_doc(Notes, conversation_id, context)
        assert watch is not None

        async def listener(_call: Any, _delivery_context: Any) -> None:
            return None

        watch.start(listener)

        async def retire(tx: Any) -> None:
            await tx.retire_doc(Notes, conversation_id)

        await session.commit(retire, context)
        await flush()
        if stop:
            await watch.stop()
        return weakref.ref(watch), weakref.ref(listener)

    # A watch follows its address, so a forgotten one stays until it is stopped or the Session ends.
    forgotten_watch, forgotten_listener = await acquire(False)
    await collect()
    held = forgotten_watch()
    assert held is not None
    assert forgotten_listener() is not None
    await held.stop()

    stopped_watch, stopped_listener = await acquire(True)
    await collect()
    assert stopped_watch() is None
    assert stopped_listener() is None
    await session.close(context)


async def test_retains_neither_its_session_nor_its_listener_once_stopped_while_still_referenced() -> None:
    """Termination drops every reference a watch holds into its Session and its listener."""

    async def acquire(
        stop: bool,
    ) -> tuple[CommittedWatch, weakref.ReferenceType[Any], weakref.ReferenceType[Any]]:
        # No eviction timer, so nothing but the watch can keep the Session alive.
        session = SessionImpl(
            cast(Storage, MemoryStorage()),
            lambda: 1_000,
            lambda: float("inf"),
            lambda _callback, _delay_ms, *, background=False: (
                (lambda: None) if background else None
            ),
        )

        async def create(tx: Any) -> None:
            await tx.doc(StateDoc)

        await session.commit(create, context)
        watch = await session.watch_doc(StateDoc, context)
        assert watch is not None

        async def listener(_call: Any, _delivery_context: Any) -> None:
            return None

        watch.start(listener)
        await set_value(session, 1)
        await flush()
        if stop:
            await watch.stop()
        return watch, weakref.ref(session), weakref.ref(listener)

    # Control: a running watch keeps its Session, so the collection below is real.
    running, running_session, _running_listener = await acquire(False)
    await collect()
    assert running_session() is not None
    await running.stop()

    stopped, stopped_session, stopped_listener = await acquire(True)
    await collect()
    assert stopped_session() is None
    assert stopped_listener() is None
    # What it delivered last stays readable.
    assert cast(dict[str, Any], stopped.value)["value"] == 1
    assert await stopped.closed == {"kind": "stopped"}

"""Translated v1.5.0 ``harness-observers`` suite.

A Harness observer is mounted the way a watch is — one attachment on the Session line, admitted with
the load of what it observes — and then mirrors publications: a conversation's view and the Session's
task graph, each arriving as the same :class:`WatchCall` a watch of the same mount would receive, in
line order and unfolded (spec §9.1, §9.3, §9.5).

Nothing upstream is held back: all fifteen ``it()`` cases of the v1.5.0 file are translated here, one
for one — the seven of ``observeConversation and observeTaskGraph``, landed by #44 along with the
observer half of ``:308``, and the eight of ``task documents``, whose seven task-document-watch cases
#54 landed. A deferral block once named those seven as carried to #54 because the ending they waited
on was unbuilt; #54 built it — the commit that makes the owner task of a task document terminal
retires its document, releases the watch's pin, and ends the watch once that retirement was delivered
— so no upstream case of this suite sits unnamed on a list here.

The task-document cases are one subject, not one behaviour in seven spellings. A watch may be **never
started**, **busy in an earlier call**, or **waiting on its retirement call**; ending the watch the
instant its task ends, collapsing the task's end and the watch's release into one event, gets the
never-started and in-flight dispositions wrong, and dropping what the terminal commit queued instead
of delivering it leaves the listener short of its last call. An earlier list of six case names,
recorded from a worker report rather than read from upstream, was rejected while #44 ran: no such
string exists in v1.5.0's suites, and the titles used here are the shipped ones.

One case here has no upstream counterpart and is named for what it proves:
``test_catches_an_entry_dropped_by_the_first_page``. The bounded window is #45's surface and upstream
proves a drop from a watch only; because an observer of a mount is handed the change its watch is
handed, the drop is proved on the observing face too rather than left to one side.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from collections.abc import Callable
from typing import Any

import pytest
from session_support import (
    ControlledStorage,
    Deferred,
    Mirror,
    context,
    flush,
)
from task_support import completed, eventually, open_tasks, settled

from pi_durable import MemoryStorage, define_doc
from pi_durable.context import AbortController, with_abort_signal
from pi_durable.harness.define import define_extension
from pi_durable.json import copy_json, is_json_value
from pi_durable.session.observation import CommittedWatch
from pi_durable.tasks import define_task

Progress = define_doc(
    {
        "kind": "observe.progress",
        "version": 1,
        "scope": "task",
        "initial": lambda: {"lines": []},
        "checkpoint_when": lambda *_args: False,
    }
)


class Recorder:
    """A host observer recording the calls and ends it was handed, in the order they arrived."""

    def __init__(self) -> None:
        self.calls: list[Any] = []
        self.ends: list[Any] = []

    def advance(self, call: Any, _context: Any = None) -> None:
        self.calls.append(call)

    def end(self, end: Any) -> None:
        self.ends.append(end)


class Throwing:
    """An observer whose every ``advance`` raises ``error``."""

    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.calls = 0
        self.ends: list[Any] = []

    def advance(self, _call: Any, _context: Any = None) -> None:
        self.calls += 1
        raise self.error

    def end(self, end: Any) -> None:
        self.ends.append(end)


async def append(handle: Any, text: str) -> None:
    """One note appended to a conversation, on the line and in storage.

    Through the conversation handle, as a host appends: the handle carries the transaction's
    conversation scope, which the bare Session commit does not.
    """

    async def change(tx: Any) -> None:
        await tx.append_entry(handle.id, {"kind": "note", "data": {"text": text}})

    await handle.commit(change, context)


async def test_starts_at_the_current_value_and_advances_with_the_change_a_watch_gets() -> None:
    """Upstream: "starts an observer at the current value and advances it at the publication with the
    same change a watch gets" (§9.1, §9.3).

    The observer is the synchronous face of the same mounted document as the watch: same line, same
    change, same order.
    """

    harness = (await open_tasks(MemoryStorage(), []))["harness"]
    root = await harness.root(context)
    mirror = Mirror()
    watch = Mirror()
    observation = await harness.observe_conversation(root.id, mirror, context)
    watched = await root.watch(context)
    watch.bind(watched)
    # A watch records through its listener, which is where its changes arrive.
    watched.start(lambda call, _ctx: watch.calls.append(call))
    assert observation is not None
    assert observation.value == watch.value

    # The watch drains between the commits, so both faces see both changes rather than a coalesced
    # pair: coalescing is the watch's own contract, not a difference between the two.
    await append(root, "one")
    await flush()
    await append(root, "two")
    await flush()
    assert mirror.calls == watch.calls
    assert [call["value"]["entries"][-1]["data"]["text"] for call in mirror.calls] == [
        "one",
        "two",
    ]
    assert mirror.value == mirror.calls[-1]["value"]
    assert watch.calls[-1]["value"] == mirror.value
    assert mirror.ends == []
    observation.detach()
    await watched.stop()
    await harness.close(context)


async def test_catches_an_entry_dropped_by_the_first_page() -> None:
    """The observing face of the window's drop (spec §9.3); no upstream case, named for what it proves.

    The bounded window is #45's surface, and upstream proves a drop only from a watch. An observer of
    a mount is handed the same change its watch is handed, so the entry the window opened on has to
    leave this value too — and leave by the splice that says so, in the very change that appends the
    newest entry. That is what keeps an observer of a long conversation bounded rather than growing.
    """

    harness = (await open_tasks(MemoryStorage(), []))["harness"]
    root = await harness.root(context)
    mirror = Mirror()
    watch = Mirror()
    window = {"min": 2, "max": 4}
    observation = await harness.observe_conversation(root.id, mirror, context, window)
    watched = await root.watch(context, window)
    watch.bind(watched)
    watched.start(lambda call, _ctx: watch.calls.append(call))
    assert observation is not None

    await append(root, "one")
    await flush()
    first = mirror.value["entries"][0]
    for text in ("two", "three", "four", "five", "six", "seven"):
        await append(root, text)
        await flush()

    # The observer is handed the same changes the watch is handed, the drop among them, unfolded.
    assert mirror.calls == watch.calls
    assert [entry["data"]["text"] for entry in mirror.value["entries"]] == [
        "four",
        "five",
        "six",
        "seven",
    ]
    handed = {entry["data"]["text"]: entry for call in mirror.calls for entry in call["value"]["entries"]}
    assert first is handed["one"]
    assert first not in mirror.value["entries"]
    # It left in one splice, and the same change moved the marker of what the window passed over.
    drops = [
        op
        for call in mirror.calls
        for op in call["ops"]
        if op[0] == "p" and op[1] == ["entries"] and op[2] == 0 and op[4] == []
    ]
    assert len(drops) == 1
    markers = [
        op
        for call in mirror.calls
        for op in call["ops"]
        if op[0] == "s" and op[1] == ["before"]
    ]
    assert markers == [["s", ["before"], handed["three"]["id"]]]
    assert mirror.value["before"] == handed["three"]["id"]
    observation.detach()
    await watched.stop()
    await harness.close(context)


async def test_ends_with_the_error_that_failed_the_harness() -> None:
    """Upstream: "ends with the error that failed the Harness"."""

    storage = ControlledStorage()
    harness = (await open_tasks(storage, []))["harness"]
    root = await harness.root(context)
    recorder = Recorder()
    observation = await harness.observe_conversation(root.id, recorder, context)
    assert observation is not None
    failure = RuntimeError("storage is gone")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="storage is gone"):
        await append(root, "never")
    assert recorder.calls == []
    assert recorder.ends == []
    await harness.close(context)
    assert recorder.ends == [{"kind": "failed", "error": failure}]


async def test_reports_a_throwing_observer_detaches_it_and_keeps_the_view_going() -> None:
    """Upstream: "reports a throwing observer, detaches it and ends it with the error, and keeps the
    view going"."""

    harness = (await open_tasks(MemoryStorage(), []))["harness"]
    root = await harness.root(context)
    bug = RuntimeError("observer is broken")
    broken = Throwing(bug)
    keeper = Recorder()
    observation = await harness.observe_conversation(root.id, broken, context)
    assert observation is not None
    held = await harness.observe_conversation(root.id, keeper, context)
    assert held is not None

    await append(root, "first")
    await append(root, "second")

    # The fault is reported once, the observer is detached, and its end carries the error.
    assert broken.calls == 1
    assert broken.ends == [{"kind": "listener_error", "error": bug}]
    # The view keeps going for the observers beside it, and the changes it missed are not replayed.
    assert [call["value"]["entries"][-1]["data"]["text"] for call in keeper.calls] == [
        "first",
        "second",
    ]
    assert keeper.ends == []
    held.detach()
    await harness.close(context)


async def test_rejects_on_a_closed_harness_and_never_throws_into_an_observer() -> None:
    """Upstream: "rejects, never throwing, on a closed Harness"."""

    harness = (await open_tasks(MemoryStorage(), []))["harness"]
    root = await harness.root(context)
    recorder = Recorder()
    await harness.close(context)
    with pytest.raises(RuntimeError, match="closed"):
        await harness.observe_conversation(root.id, recorder, context)
    with pytest.raises(RuntimeError, match="closed"):
        await harness.observe_task_graph(recorder, context)
    assert recorder.calls == [] and recorder.ends == []


async def test_rejects_an_attachment_cancelled_while_it_waits_on_the_line() -> None:
    """Upstream: "rejects an attachment cancelled while it waits on the line, and attaches nothing"."""

    storage = ControlledStorage()
    harness = (await open_tasks(storage, []))["harness"]
    root = await harness.root(context)
    view = Recorder()
    graph = Recorder()
    held = storage.hold_commits()
    committing = asyncio.create_task(append(root, "held"))
    await held.entered.wait()
    controller = AbortController()
    cancelled = with_abort_signal(controller.signal, context)
    attaching_view = asyncio.create_task(
        harness.observe_conversation(root.id, view, cancelled)
    )
    attaching_graph = asyncio.create_task(harness.observe_task_graph(graph, cancelled))
    await flush()
    controller.abort(RuntimeError("gone"))
    held.release()
    await committing
    for attachment in (attaching_view, attaching_graph):
        with pytest.raises(RuntimeError, match="gone"):
            await attachment
    # The line is free and the mounts are unattached: a later change reaches neither recorder.
    await append(root, "after")
    await harness.close(context)
    assert view.calls == [] and view.ends == []
    assert graph.calls == [] and graph.ends == []


async def test_advances_a_graph_observer_with_a_blocked_refresh_outside_any_commit() -> None:
    """Upstream: "advance a graph observer with a blocked refresh, outside any commit" (§9.5).

    A reason that changes without a commit — the registry gained the definition — still reaches the
    observer on the line, because the graph publishes its own blocked state; it is not an adoption
    and storage sees nothing of it.
    """

    missing = define_task(
        {
            "name": "observe.missing",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {
                "run": lambda _task, runtime, ctx: runtime.commit(lambda: completed(None), ctx)
            },
            "abort": async_no_op,
        }
    )
    storage = ControlledStorage()
    opened = await open_tasks(storage, [])
    harness, registry = opened["harness"], opened["registry"]
    graph = Recorder()
    observation = await harness.observe_task_graph(graph, context)
    assert observation is not None
    root = await harness.root(context)
    # Scheduling is not resumed: the task stays pending, blocked while its definition is missing.
    task_id = await root.commit(
        lambda tx: tx.create_task(missing, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    key = str(task_id)
    assert len(graph.calls) == 1
    blocked = graph.calls[0]
    assert blocked["kind"] == "value" and blocked["retired"] is False
    assert blocked["ops"] == [["s", ["tasks", key], blocked["value"]["tasks"][key]]]
    assert blocked["value"]["tasks"][key]["blocked"] == "missing_task"

    commits_before = len(storage.commits)
    registry.install(define_extension({"name": "late", "tasks": [missing]}))
    await eventually(lambda: len(graph.calls) == 2)
    second = graph.calls[1]
    assert second["ops"] == [["d", ["tasks", key, "blocked"]]]
    assert len(storage.commits) == commits_before
    observation.detach()
    await harness.close(context)


async def async_no_op(*_args: Any) -> None:
    return None


async def test_keeps_no_detached_observer_alive() -> None:
    """Upstream: "keeps no detached observer alive"."""

    harness = (await open_tasks(MemoryStorage(), []))["harness"]
    root = await harness.root(context)

    async def attach() -> weakref.ReferenceType[Recorder]:
        recorder = Recorder()
        observation = await harness.observe_conversation(root.id, recorder, context)
        assert observation is not None
        observation.detach()
        return weakref.ref(recorder)

    held = await attach()
    await collect()
    assert held() is None
    await harness.close(context)


async def test_gets_the_retirement_then_its_end_at_the_commit_that_ends_the_task() -> None:
    """Upstream: "an observer gets the retirement, then its end, at the publication of the commit
    that ends the task" (§9.1).

    This is the one end an observer learns from the state it observes rather than from the Session's
    close: the retirement of the observed document arrives first, and the end follows it on the same
    publication.
    """

    gates = {"written": Deferred(), "late": Deferred()}
    task = writer_task(gates)
    opened = await open_tasks(MemoryStorage(), [task])
    harness = opened["harness"]
    root = await harness.root(context)
    task_id = await root.commit(
        lambda tx: tx.create_task(
            task,
            None,
            {"ownership": {"kind": "conversation", "conversationId": root.id}},
        ),
        context,
    )
    recorder = Recorder()
    # The task's first commit is what opens its document: attach only once the line has it.
    harness.resume()
    await eventually(lambda: harness.snapshot(Progress, task_id, context))
    observation = await harness.observe_doc(Progress, task_id, recorder, context)
    assert observation is not None
    assert observation.value == {"lines": ["opened"]}

    gates["written"].resolve()
    await eventually(lambda: len(recorder.calls) == 1)
    assert recorder.calls[0]["kind"] == "value"
    assert recorder.calls[0]["retired"] is False
    assert recorder.calls[0]["value"] == {"lines": ["opened", "more"]}

    gates["late"].resolve()
    await harness.wait_for_task(task_id, context)
    await flush()
    # The retirement of the observed document, then its end, in that order, from one publication.
    assert recorder.calls[1:] == [
        {
            "kind": "value",
            "value": {"lines": ["opened", "more"]},
            "ops": [],
            "retired": True,
        }
    ]
    assert recorder.ends == [{"kind": "ended"}]
    # The address is free again: a fresh observer of it sees the absent document.
    assert await harness.observe_doc(Progress, task_id, Recorder(), context) is None
    # And nothing stays pinned for the ended observer.
    assert harness.document_cache_sizes["attached"] == 0
    await harness.close(context)


def writer_task(gates: dict[str, Deferred]) -> Any:
    """A task that writes its own document, then holds its outcome until the test lets it finish."""

    async def open_progress(tx: Any, task_id: Any) -> dict[str, Any]:
        (await tx.doc(Progress, task_id)).lines.append("opened")
        return {"status": "running", "checkpoint": {"phase": "started"}}

    async def write_more(tx: Any, task_id: Any) -> None:
        (await tx.doc(Progress, task_id)).lines.append("more")

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        task_id = task["id"]
        await runtime.commit(lambda tx: open_progress(tx, task_id), ctx)
        await gates["written"].promise
        await runtime.commit(lambda tx: write_more(tx, task_id), ctx)
        await gates["late"].promise
        await runtime.commit(lambda: completed(None), ctx)

    async def abort(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    return define_task(
        {
            "name": "observe.writer",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )


def progress_writer(gate: Deferred, written: Callable[[], None]) -> Any:
    """Upstream's ``writer``: write the task's document, signal, hold on ``gate``, then complete.

    Completing is what retires the task's document, and so what ends a watch of it (#54).
    """

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        task_id = task["id"]

        async def started(tx: Any) -> None:
            (await tx.doc(Progress, task_id)).lines.append("started")

        await runtime.commit(started, ctx)
        written()
        await gate.promise
        await runtime.commit(lambda: completed(None), ctx)

    async def abort(_task: Any, _runtime: Any, _ctx: Any) -> None:
        return None

    return define_task(
        {
            "name": "observe.progress.writer",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": abort,
        }
    )


async def open_writer(storage: ControlledStorage) -> dict[str, Any]:
    """Upstream's ``openWriter``: a Harness with a writer task whose document is already written.

    The task holds its outcome on ``gate``: handing back ``finish`` is how a case ends the task and
    retires its document.
    """

    gate = Deferred()
    written = Deferred()
    task = progress_writer(gate, written.resolve)
    opened = await open_tasks(storage, [task])
    harness = opened["harness"]
    root = await harness.root(context)
    task_id = await root.commit(
        lambda tx: tx.create_task(
            task,
            None,
            {"ownership": {"kind": "conversation", "conversationId": root.id}},
        ),
        context,
    )
    harness.resume()
    await written.promise
    return {"harness": harness, "storage": storage, "root": root, "id": task_id, "finish": gate.resolve}


async def collect() -> None:
    """Give the collector a few turns, as the upstream helper does before dereferencing."""

    for _ in range(4):
        await asyncio.sleep(0)
        gc.collect()


async def test_delivers_the_retirement_of_the_commit_that_ends_its_task_then_ends_ended() -> None:
    """Upstream: "a watch delivers the retirement of the commit that ends its task, then ends
    ``ended``" (§9.1).

    This is the delivery the other six task-document cases do not prove: the watch's last call is the
    retirement of the very commit that ended its task — the value the watch already held, no
    operations, the retirement flag set — and the end follows it. A build that ends the watch and
    drops what the terminal commit queued delivers nothing here, and a build that folds a root
    replacement in where the empty operation list belongs fails the ``ops`` half.
    """

    opened = await open_writer(ControlledStorage())
    harness, task_id, finish = opened["harness"], opened["id"], opened["finish"]
    watch = await harness.watch_doc(Progress, task_id, context)
    assert watch is not None
    assert watch.value == {"lines": ["started"]}
    delivered: list[Any] = []
    watch.start(lambda call, _delivery_context: delivered.append(call))

    finish()
    await harness.wait_for_task(task_id, context)
    assert await watch.closed == {"kind": "ended"}
    assert delivered == [{"kind": "value", "value": {"lines": ["started"]}, "ops": [], "retired": True}]
    # The delivered value is the very revision the watch now answers from, not a copy of it.
    assert delivered[0]["value"] is watch.value
    # The task is terminal: its address is absent for good.
    assert await harness.watch_doc(Progress, task_id, context) is None
    await harness.close(context)


async def test_keeps_no_task_document_watch_alive_once_its_task_ended() -> None:
    """Upstream: "keeps no task document watch alive once its task ended" (§9.1).

    The watch is held by nothing but the Harness while its task runs, which is what the control half
    proves; once the task has ended the watch ends and the Session stops holding it, so the object
    itself is collected and the cache pins no incarnation for it.
    """

    opened = await open_writer(ControlledStorage())
    harness, task_id, finish = opened["harness"], opened["id"], opened["finish"]

    async def acquire() -> weakref.ReferenceType[CommittedWatch]:
        """A started watch of the task's document, which only the Harness references."""

        watch = await harness.watch_doc(Progress, task_id, context)
        assert watch is not None
        watch.start(lambda _call, _delivery_context: None)
        return weakref.ref(watch)

    watch = await acquire()
    # Control: the watch stays while its task runs, so the collection below is real — and the pin it
    # took for the document it follows is what the last assertion has to take back.
    await collect()
    assert watch() is not None
    assert harness.document_cache_sizes["attached"] > 0

    finish()
    await harness.wait_for_task(task_id, context)
    await collect()
    assert watch() is None
    # The Harness is the Session kernel; nothing stays pinned for the ended watch.
    assert harness.document_cache_sizes["attached"] == 0
    await harness.close(context)


async def test_a_watch_never_started_when_its_task_ends_ends_with_the_close_keeping_nothing() -> None:
    """Upstream: "a watch never started when its task ends still ends with the Harness's close,
    keeping nothing of it" (§9.1).

    Ending a task and releasing its watch are not one event: the never-started listener owes the
    retirement the commit ending the task published, so the watch is still open after the task ended
    and only the Harness's close ends it — with the retirement dropped, as for every watch, and with
    nothing of the Harness left behind in the watch.
    """

    async def acquire() -> tuple[CommittedWatch, weakref.ReferenceType[Any]]:
        """Acquire a task document's watch, never start it, end its task, and close the Harness."""

        opened = await open_writer(ControlledStorage())
        harness, task_id, finish = opened["harness"], opened["id"], opened["finish"]
        watch = await harness.watch_doc(Progress, task_id, context)
        assert watch is not None
        finish()
        await harness.wait_for_task(task_id, context)
        # Its retirement is pending until a start that never comes: the watch is open.
        await flush()
        assert await settled(watch.closed) is False
        await harness.close(context)
        return watch, weakref.ref(harness)

    watch, harness = await acquire()
    # Closed at the Harness's close, the pending retirement dropped, as for every watch.
    assert await watch.closed == {"kind": "ended"}
    with pytest.raises(RuntimeError, match="Watch is stopped"):
        watch.start(lambda _call, _delivery_context: None)
    await collect()
    assert harness() is None


async def test_a_watch_never_started_when_its_task_ends_fails_with_the_harness() -> None:
    """Upstream: "a watch never started when its task ends fails with the Harness" (§9.1).

    The task's end is not the watch's: the never-started listener still owed the retirement, and what
    ends the watch here is the Session failing, which ends it ``failed`` with the error that failed
    it and drops that call. ``ended`` stays what only an owner that ended carries.
    """

    storage = ControlledStorage()
    opened = await open_writer(storage)
    harness, root, task_id, finish = opened["harness"], opened["root"], opened["id"], opened["finish"]
    watch = await harness.watch_doc(Progress, task_id, context)
    assert watch is not None
    finish()
    await harness.wait_for_task(task_id, context)
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk gone"):
        await append(root, "never")
    assert await watch.closed == {"kind": "failed", "error": failure}


async def test_a_watch_busy_in_an_earlier_call_when_its_task_ends_fails_before_its_retirement_call() -> (
    None
):
    """Upstream: "a watch busy in an earlier call when its task ends fails with the Harness before its
    retirement call" (§9.1).

    The retirement the commit ending the task published is queued behind the call already running, so
    it is still owed when the Session fails — and a failure ends the watch at once, dropping that
    call rather than delivering it first. The listener being released afterwards proves it: the value
    it was never given is never given.
    """

    storage = ControlledStorage()
    opened = await open_writer(storage)
    harness, root, task_id, finish = (
        opened["harness"],
        opened["root"],
        opened["id"],
        opened["finish"],
    )
    watch = await harness.watch_doc(Progress, task_id, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    values: list[Any] = []

    async def listener(call: Any, _delivery_context: Any) -> None:
        values.append(call["value"])
        entered.resolve()
        await release.promise

    watch.start(listener)

    async def write_more(tx: Any) -> None:
        (await tx.doc(Progress, task_id)).lines.append("more")

    await harness.commit(write_more, context)
    await entered.promise
    # The task ends while the listener is busy: its retirement waits behind the running call.
    finish()
    await harness.wait_for_task(task_id, context)
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    with pytest.raises(RuntimeError, match="disk gone"):
        await append(root, "never")
    assert await watch.closed == {"kind": "failed", "error": failure}
    release.resolve()
    await flush()
    # The one call the watch was given is all it ever got: the retirement was dropped, not delivered.
    assert values == [{"lines": ["started", "more"]}]


async def test_a_watch_whose_retirement_call_runs_when_its_task_ends_ends_only_after_it_returns() -> (
    None
):
    """Upstream: "a watch whose retirement call runs when its task ends ends only after that call
    returns" (§9.1).

    Ordering, not company, is what is under test. The retirement call is still running when the commit
    ending the task lands, so ``closed`` has to be unsettled after the line has drained — that
    assertion is the whole case, and a port that ends the watch at the task's end fails it there. The
    end then arrives only once the released call has returned, with the flag the listener raises on its
    way out already set.
    """

    opened = await open_writer(ControlledStorage())
    harness, task_id, finish = opened["harness"], opened["id"], opened["finish"]
    watch = await harness.watch_doc(Progress, task_id, context)
    assert watch is not None
    entered = Deferred()
    release = Deferred()
    returned = False

    async def listener(_call: Any, _delivery_context: Any) -> None:
        nonlocal returned
        entered.resolve()
        await release.promise
        returned = True

    watch.start(listener)

    # An explicit retirement first: its call runs while the task ends, and nothing more is pending.
    async def retire(tx: Any) -> None:
        await tx.retire_doc(Progress, task_id)

    await harness.commit(retire, context)
    await entered.promise
    finish()
    await harness.wait_for_task(task_id, context)
    await flush()
    # The task is terminal and the retirement was already taken: the end still waits on the call.
    assert await settled(watch.closed) is False
    release.resolve()
    assert await watch.closed == {"kind": "ended"}
    assert returned is True
    await harness.close(context)


async def test_hands_listeners_changes_that_keep_nothing_of_the_harness() -> None:
    """Upstream: "hands listeners changes that keep nothing of the Harness" (§9.1).

    Both changes a listener of a task document's watch is handed across its task's end — the write and
    the retirement that ends it — carry exactly the four members of a
    :data:`~pi_durable.types.WatchCall` in plain JSON, and they outlive the Harness without keeping it:
    the weakref below is the proof, not a field that was nulled. A build that drops the queued
    retirement hands the listener one change here instead of two.
    """

    async def acquire() -> tuple[list[Any], weakref.ReferenceType[Any]]:
        """Watch a task document until its task ends, keeping every change, then close the Harness."""

        opened = await open_writer(ControlledStorage())
        harness, task_id, finish = opened["harness"], opened["id"], opened["finish"]
        watch = await harness.watch_doc(Progress, task_id, context)
        assert watch is not None
        changes: list[Any] = []
        watch.start(lambda call, _delivery_context: changes.append(call))

        async def write_more(tx: Any) -> None:
            (await tx.doc(Progress, task_id)).lines.append("more")

        await harness.commit(write_more, context)
        await flush()
        finish()
        await harness.wait_for_task(task_id, context)
        assert await watch.closed == {"kind": "ended"}
        await harness.close(context)
        return changes, weakref.ref(harness)

    changes, harness = await acquire()
    # Both changes arrive: the write, and the retirement of the commit that ended the task.
    assert len(changes) == 2
    for change in changes:
        assert list(change) == ["kind", "value", "ops", "retired"]
        # Strict JSON in plain containers, so nothing of the Session it came from rides along, and a
        # copy of it stands on its own.
        assert is_json_value(change)
        assert copy_json(change) == change
    await collect()
    assert harness() is None

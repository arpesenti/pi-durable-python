"""Translated v1.5.0 ``harness-observers`` suite.

A Harness observer is mounted the way a watch is — one attachment on the Session line, admitted with
the load of what it observes — and then mirrors publications: a conversation's view and the Session's
task graph, each arriving as the same :class:`WatchCall` a watch of the same mount would receive, in
line order and unfolded (spec §9.1, §9.3, §9.5).

Deferral block — the cases this ticket does not translate, named, with their reason, and the ticket
that holds them. Every one needs a task document's **watch** to be retired and ended by the commit
that ends its owning task. That retirement is unbuilt Session behaviour under Chord's
``endAfterDelivery``; no :file:`PARITY.md` row and no ticket held it while #44 ran, so all of them are
carried to **#54**, "A task document's watch ends when its owning task goes terminal".

The upstream cases, at the line each sits on in ``test/harness-observers.test.ts`` of v1.5.0 — the
reason is the same for all seven, and each one is named in :data:`EXCLUDED_WATCH_CASES` so the list
cannot rot silently:

- ``:290`` "a watch delivers the retirement of the commit that ends its task, then ends ``ended``"
- ``:331`` "keeps no task document watch alive once its task ended"
- ``:352`` "a watch never started when its task ends still ends with the Harness's close, keeping
  nothing of it"
- ``:373`` "a watch never started when its task ends fails with the Harness"
- ``:386`` "a watch busy in an earlier call when its task ends fails with the Harness before its
  retirement call"
- ``:417`` "a watch whose retirement call runs when its task ends ends only after that call returns"
- ``:441`` "hands listeners changes that keep nothing of the Harness" — gated on the same
  ``watch.closed == {kind: ended}``, so it waits too; the half of it about the delivered change is
  held by the watch suite's snapshot case and, for an observer, by
  ``test_session_observers.py::test_hands_the_very_committed_value_the_publication_and_the_snapshot_carry``

Neither ``:331`` nor ``:417`` is already satisfied by #43's "an ended watch drops its listener and
every reference into the session": that case ends a watch the **Session** ended, through ``stop()`` or
a close, while both of these end on their **task** ending, which is the retirement #54 has to deliver.
The one case of this family that is about an observer rather than a watch — ``:308``, "an observer
gets the retirement, then its end, at the publication of the commit that ends the task" — is
implemented here for real, because an ``ended`` an observed owner can never reach is not an end.

The task-document watch cases the audit named for #54 are recorded here too, verbatim, so no
untranslated case goes unnamed while #54 is still ahead. They are the audit's naming, not a string
found in upstream v1.5.0's suites at this baseline — #54 should reconcile them against the upstream
case list above rather than treat the two spellings as different work:

- "harness observes the task graph through a task-document watch"
- "an observer sees a task-document watch end when a read fails"
- "an observer sees a task-document watch end when the document is deleted"
- "a task-document watch does not end for another conversation's commit"
- "an ended task-document watch unsubscribes from commits"
- "an observer does not resurrect a deleted document"

One case here has no upstream counterpart and is named for what it proves:
``test_catches_an_entry_dropped_by_the_first_page``. The bounded window is #45's surface and upstream
proves a drop from a watch only; because an observer of a mount is handed the change its watch is
handed, the drop is proved on the observing face too rather than left to one side.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from typing import Any

import pytest
from session_support import (
    ControlledStorage,
    Deferred,
    Mirror,
    context,
    flush,
)
from task_support import completed, eventually, open_tasks

from pi_durable import MemoryStorage, define_doc
from pi_durable.context import AbortController, with_abort_signal
from pi_durable.harness.define import define_extension
from pi_durable.tasks import define_task

#: The upstream cases this ticket does not translate, named as upstream names them, each with the
#: reason it is held back and the ticket that now holds it. Every one of them needs the task
#: document's *watch* to be retired and ended by the commit that ends its owner, which is #54's
#: behaviour to build; none of them can be translated into a passing test without it, and a skip
#: that pretended otherwise would be worth less than this table.
EXCLUDED_WATCH_CASES: dict[str, str] = {
    "a watch delivers the retirement of the commit that ends its task, then ends `ended` (#290)": (
        "the retirement that ends a task document's watch is unbuilt Session behaviour under Chord's "
        "`endAfterDelivery`; no PARITY row and no ticket held it at the time of #44 — carried to #54"
    ),
    "keeps no task document watch alive once its task ended (#331)": (
        "the watch is only released once the commit that ends its task ends it, so the case is the "
        "same unbuilt retirement seen from the cache — carried to #54"
    ),
    (
        "a watch never started when its task ends still ends with the Harness's close, keeping "
        "nothing of it (#352)"
    ): (
        "an unstarted watch's `ended` is the task-ending retirement this ticket does not build — "
        "carried to #54"
    ),
    "a watch never started when its task ends fails with the Harness (#373)": (
        "the `failed` arm of the same unbuilt task-ending retirement — carried to #54"
    ),
    (
        "a watch busy in an earlier call when its task ends fails with the Harness before its "
        "retirement call (#386)"
    ): (
        "the retirement call that never arrives is the one #54 has to deliver — carried to #54"
    ),
    (
        "a watch whose retirement call runs when its task ends ends only after that call returns "
        "(#417)"
    ): (
        "`endAfterDelivery` for watches is #54's half; #43's "
        '"an ended watch drops its listener and every reference into the session" covers only a '
        "watch the Session itself ended, not one its task ended — carried to #54"
    ),
    "hands listeners changes that keep nothing of the Harness (#441)": (
        "the case is gated on `watch.closed == {kind: ended}` at the task's end, so it cannot run "
        "without #54; the half of it that is about the delivered change — a plain JSON snapshot "
        "carrying nothing of its owner — is already held by the watch suite's snapshot case "
        "against the same commit path — carried to #54"
    ),
}

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


async def collect() -> None:
    """Give the collector a few turns, as the upstream helper does before dereferencing."""

    for _ in range(4):
        await asyncio.sleep(0)
        gc.collect()


async def test_excluded_watch_cases_are_named_not_silently_dropped() -> None:
    """The seven held-back upstream cases stay visible: named, reasoned, and pointing at #54."""

    assert len(EXCLUDED_WATCH_CASES) == 7
    for case, reason in EXCLUDED_WATCH_CASES.items():
        assert "(#" in case, case
        assert "#54" in reason, case

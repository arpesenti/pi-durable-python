"""Translated upstream ``harness-storage-failure`` cases for the task core and the failure latch."""

from __future__ import annotations

import asyncio
from typing import Any

from session_support import ControlledStorage, context, flush
from task_support import aborted, deferred, eventually, open_tasks

from pi_durable import LiveDoc, MemoryStorage
from pi_durable.context import AbortController, with_abort_signal
from pi_durable.errors import SessionFailed, StorageRequestError
from pi_durable.harness.define import define_extension
from pi_durable.harness.harness import Harness, safe_now
from pi_durable.harness.registry import create_registry
from pi_durable.ids import TaskId
from pi_durable.session.session import contained
from pi_durable.tasks import define_task


async def _capture(awaitable: Any) -> BaseException:
    try:
        await awaitable
    except BaseException as error:
        return error
    raise AssertionError("expected a rejection")


async def _aborted_handler(task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)


signals: list[str] = []


async def _waiting_work(task: Any, runtime: Any, ctx: Any) -> None:
    try:
        await aborted(runtime.signal)
    except BaseException:
        signals.append("aborted")


Waiting = define_task(
    {
        "name": "test.waiting",
        "version": 1,
        "initial": lambda _input: {"phase": "work"},
        "phases": {"work": _waiting_work},
        "abort": _aborted_handler,
    }
)


class FailingCalls(ControlledStorage):
    """Fails each Storage method named in ``failing`` once, with ``error``."""

    def __init__(self) -> None:
        super().__init__()
        self.error = RuntimeError("disk gone")
        self.failing: set[str] = set()
        self.held_range: dict[str, Any] | None = None

    def _fail(self, method: str) -> None:
        if method in self.failing:
            self.failing.discard(method)
            raise self.error

    async def task(self, id: Any, read_context: Any) -> Any:
        self._fail("task")
        return await super().task(id, read_context)

    async def scan_entries(self, query: Any, limit: int, cursor: Any, read_context: Any) -> Any:
        if limit > 1:
            held = self.held_range
            if held is not None:
                held["entered"].resolve()
                await held["release"].promise
            self._fail("scanRange")
        return await super().scan_entries(query, limit, cursor, read_context)

    async def mint_id(self) -> int:
        self._fail("mintId")
        return await super().mint_id()


async def started(storage: Any) -> dict[str, Any]:
    opened = await open_tasks(storage, [Waiting])
    root = await opened["harness"].root(context)
    return {**opened, "root": root}


async def running(harness: Any, id: TaskId) -> None:
    while True:
        record = await harness.get_task(id, context)
        if record is not None and record["state"]["status"] == "running":
            return
        await asyncio.sleep(0.001)


async def test_fails_it_from_a_read_off_the_session_line_as_a_context_read_makes() -> None:
    storage = FailingCalls()
    opened = await started(storage)
    await opened["root"].commit(
        lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
    )
    storage.failing.add("scanRange")
    error = await _capture(opened["root"].context(context))
    assert error is storage.error
    assert await opened["harness"].closed == {"reason": "failed", "error": storage.error}
    assert opened["reports"] == [storage.error]
    await opened["harness"].close(context)


async def test_ends_storage_calls_underway_with_session_failed_and_closes_once_they_settle() -> None:
    storage = FailingCalls()
    opened = await started(storage)
    release = deferred()
    await opened["root"].commit(
        lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
    )
    storage.held_range = {"entered": deferred(), "release": release}
    reading = asyncio.ensure_future(opened["root"].context(context))
    await storage.held_range["entered"].promise
    # Another call fails the Session while the context read is underway.
    storage.failing.add("mintId")
    error = await _capture(
        opened["root"].commit(
            lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
        )
    )
    assert error is storage.error
    for _ in range(5):
        await asyncio.sleep(0)
    assert opened["harness"].closed.done() is False
    # The read succeeds at the backend, yet its caller gets the failure: nothing it read is used.
    release.resolve()
    read_error = await _capture(reading)
    assert isinstance(read_error, SessionFailed)
    assert read_error.cause is storage.error
    assert await opened["harness"].closed == {"reason": "failed", "error": storage.error}
    await opened["harness"].close(context)


async def test_closes_the_backend_only_after_a_paged_read_underway_which_cannot_start_another_page() -> None:
    pages: list[str] = []
    first_page = deferred()
    release_first = deferred()

    class Paged(ControlledStorage):
        async def scan_entries(
            self, query: Any, limit: int, cursor: Any, read_context: Any
        ) -> Any:
            if limit > 1:
                pages.append("first" if cursor is None else "next")
                if cursor is None:
                    first_page.resolve()
                    await release_first.promise
            return await super().scan_entries(query, limit, cursor, read_context)

        async def close(self, close_context: Any) -> None:
            pages.append("close")
            return await super().close(close_context)

    storage = Paged()
    opened = await started(storage)

    # More entries than one page of the context read.
    async def append(tx: Any) -> None:
        for _ in range(300):
            await tx.append_entry(opened["root"].id, {"kind": "note"})

    await opened["root"].commit(append, context)
    reading = asyncio.ensure_future(opened["root"].context(context))
    await first_page.promise
    closing = opened["harness"].close(context)
    release_first.resolve()
    error = await _capture(reading)
    assert "is closed" in str(error)
    await closing
    assert pages == ["first", "close"]
    assert await opened["harness"].closed == {"reason": "closed"}


async def test_fails_it_when_storage_cannot_close_and_an_earlier_failure_stays_the_cause() -> None:
    close_error = RuntimeError("close failed")

    class FailingClose(ControlledStorage):
        async def close(self, close_context: Any) -> None:
            await super().close(close_context)
            raise close_error

    healthy = await started(FailingClose())
    error = await _capture(healthy["harness"].close(context))
    assert error is close_error
    assert await healthy["harness"].closed == {"reason": "failed", "error": close_error}
    assert healthy["reports"] == [close_error]

    storage = FailingClose()
    failed = await started(storage)
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    commit_error = await _capture(
        failed["root"].commit(
            lambda tx: tx.append_entry(failed["root"].id, {"kind": "note"}), context
        )
    )
    assert commit_error is failure
    assert await failed["harness"].closed == {"reason": "failed", "error": failure}
    assert failed["reports"] == [failure]


async def test_rejects_the_harnesss_own_calls_with_session_failed_after_a_failure() -> None:
    storage = ControlledStorage()
    opened = await started(storage)
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    assert (
        await _capture(
            opened["root"].commit(
                lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
            )
        )
        is failure
    )
    try:
        opened["harness"].resume()
    except SessionFailed as error:
        assert error.cause is failure
    else:
        raise AssertionError("expected resume() to throw")
    for call in (
        lambda: opened["harness"].root(context),
        lambda: opened["harness"].conversation(opened["root"].id, context),
        lambda: opened["harness"].create_conversation(
            {"ownership": {"kind": "ownerless"}}, context
        ),
    ):
        call_error = await _capture(call())
        assert isinstance(call_error, SessionFailed) and call_error.cause is failure
    await opened["harness"].close(context)


async def test_ends_what_a_task_holds_with_session_failed_not_as_cancelled_by_its_signal() -> None:
    ends: list[Any] = []
    watching = deferred()

    async def work(task: Any, runtime: Any, phase_context: Any) -> None:
        handle = await runtime.conversation(runtime.conversation_id, phase_context)
        assert handle is not None
        watching.resolve()
        try:
            await handle.wait_for_idle(phase_context)
            ends.append("resolved")
        except BaseException as error:
            ends.append(error)

    Watcher = define_task(
        {
            "name": "test.watcher",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    storage = ControlledStorage()
    opened = await open_tasks(storage, [Watcher])
    root = await opened["harness"].root(context)
    await root.commit(
        lambda tx: tx.create_task(Watcher, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    await watching.promise
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    await _capture(root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context))
    await opened["harness"].closed
    assert len(ends) == 1
    assert isinstance(ends[0], SessionFailed)
    assert ends[0].cause is failure
    await opened["harness"].close(context)


async def test_fails_it_from_a_failed_id_mint() -> None:
    storage = FailingCalls()
    opened = await started(storage)
    storage.failing.add("mintId")
    assert (
        await _capture(
            opened["root"].commit(
                lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
            )
        )
        is storage.error
    )
    assert await opened["harness"].closed == {"reason": "failed", "error": storage.error}
    await opened["harness"].close(context)


async def test_does_not_commit_what_a_callback_did_after_it_caught_a_failed_read() -> None:
    storage = FailingCalls()
    opened = await started(storage)
    written = len(storage.admitted_commits)
    storage.failing.add("task")

    async def change(tx: Any) -> None:
        await _capture(tx.task(1))
        await tx.append_entry(opened["root"].id, {"kind": "note"})

    error = await _capture(opened["root"].commit(change, context))
    assert isinstance(error, SessionFailed)
    assert error.cause is storage.error
    assert len(storage.admitted_commits) == written
    await opened["harness"].close(context)


async def test_fails_it_for_a_storage_request_error_from_a_commit_whose_effect_is_unknown() -> None:
    storage = ControlledStorage()
    opened = await started(storage)
    rejection = StorageRequestError("bad batch")
    storage.fail_next_commit(rejection)
    assert (
        await _capture(
            opened["root"].commit(
                lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
            )
        )
        is rejection
    )
    assert await opened["harness"].closed == {"reason": "failed", "error": rejection}
    await opened["harness"].close(context)


async def test_fails_only_the_call_for_an_invalid_request() -> None:
    opened = await started(MemoryStorage())
    root = opened["root"]
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    invalid: list[Any] = [
        lambda: root.entries({}, 10, {"after": "x"}, context),
    ]

    async def conflicting() -> Any:
        page = await root.entries({"order": "ascending"}, 1, None, context)
        return await root.entries(
            {"order": "descending"}, 1, page.get("next") or {"after": 1, "order": "ascending"}, context
        )

    invalid.append(conflicting)

    async def unknown_conversation(tx: Any) -> Any:
        return await tx.scan_entries({"conversationId": 999_999}, 10)

    invalid.append(lambda: root.commit(unknown_conversation, context))
    for request in invalid:
        error = await _capture(request())
        assert isinstance(error, StorageRequestError), error
    assert await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    await flush()
    assert opened["harness"].closed.done() is False
    assert opened["reports"] == []
    await opened["harness"].close(context)


async def test_fails_it_for_a_throw_in_its_own_commit_listeners() -> None:
    storage = ControlledStorage()
    opened = await started(storage)
    bug = RuntimeError("listener bug")
    opened["harness"].observe_commits(lambda publication, _context: _raise(bug))
    # The commit itself is durable and resolves; the Session fails behind it.
    assert await opened["root"].commit(
        lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
    )
    assert await opened["harness"].closed == {"reason": "failed", "error": bug}
    assert opened["reports"] == [bug]
    await opened["harness"].close(context)


async def _raise(error: BaseException) -> None:
    raise error


async def test_fails_it_for_a_storage_error_during_a_host_close_before_the_backend_closes() -> None:
    storage = ControlledStorage()
    opened = await started(storage)
    held = storage.hold_commits()
    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    committing = asyncio.ensure_future(
        opened["root"].commit(
            lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
        )
    )
    await held.entered.wait()
    closing = opened["harness"].close(context)
    held.release()
    assert await _capture(committing) is failure
    await closing
    assert await opened["harness"].closed == {"reason": "failed", "error": failure}
    assert opened["reports"] == [failure]


async def test_ends_an_abort_that_joins_a_run_ignoring_its_signal_and_a_reentrant_report() -> None:
    release = deferred()
    entered = deferred()

    async def work(task: Any, runtime: Any, ctx: Any) -> None:
        entered.resolve()
        await release.promise

    Stubborn = define_task(
        {
            "name": "test.stubborn",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": work},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    storage = ControlledStorage()
    registry = create_registry()
    registry.install(define_extension({"name": "tasks", "tasks": [Stubborn]}))
    reports: list[BaseException] = []
    reentered: list[Any] = []
    holder: dict[str, Any] = {}

    def on_report(error: BaseException) -> None:
        reports.append(error)
        reentered.append(holder["harness"].wait_for_idle(context))

    harness = await Harness.open(
        storage,
        {"models": {}, "registry": registry, "on_report": on_report},
        context,
    )
    holder["harness"] = harness
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Stubborn, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    harness.resume()
    await entered.promise
    held = storage.hold_commits()
    aborting = asyncio.ensure_future(harness.abort_task(id, context))
    await held.entered.wait()
    held.release()
    # The abort mark commits and the abort joins the run, which ignores its signal; then the Session fails.
    failure = RuntimeError("disk gone")
    await asyncio.sleep(0.005)
    storage.fail_next_commit(failure)
    assert (
        await _capture(root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context))
        is failure
    )
    abort_error = await _capture(aborting)
    assert isinstance(abort_error, SessionFailed)
    assert abort_error.cause is failure
    assert reentered
    assert isinstance(await _capture(reentered[0]), SessionFailed)
    release.resolve()
    await harness.close(context)


async def test_does_not_fail_it_for_a_read_its_caller_cancelled() -> None:
    controller = AbortController()
    cancelled = RuntimeError("caller gave up")

    class CancelledRead(MemoryStorage):
        async def scan_entries(
            self, query: Any, limit: int, cursor: Any, read_context: Any
        ) -> Any:
            if read_context.abort_signal is controller.signal:
                controller.abort(cancelled)
                raise cancelled
            return await super().scan_entries(query, limit, cursor, read_context)

    opened = await started(CancelledRead())
    cancelling = with_abort_signal(controller.signal, context)
    assert await _capture(opened["root"].context(cancelling)) is cancelled
    assert await opened["root"].commit(
        lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
    )
    await opened["harness"].close(context)


async def test_reports_a_listener_or_on_report_whose_promise_rejects_without_an_unhandled_rejection() -> None:
    async def async_throw(*_args: Any) -> None:
        raise RuntimeError("async listener")

    opened = await started(MemoryStorage())
    opened["harness"].subscribe_commits(async_throw)
    opened["harness"].subscribe_close(async_throw)
    await opened["root"].commit(
        lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
    )
    await opened["harness"].close(context)
    await eventually(lambda: len(opened["reports"]) == 2)
    assert [str(report) for report in opened["reports"]] == [
        "async listener",
        "async listener",
    ]

    registry = create_registry()
    rejecting = await Harness.open(
        MemoryStorage(),
        {"models": {}, "registry": registry, "on_report": async_throw},
        context,
    )
    rejecting.subscribe_commits(lambda publication, _context: _raise(RuntimeError("sync listener")))
    again = await rejecting.root(context)
    await again.commit(lambda tx: tx.append_entry(again.id, {"kind": "note"}), context)
    await rejecting.close(context)


async def test_reports_a_throwing_commit_or_close_listener_and_runs_the_others() -> None:
    storage = MemoryStorage()
    opened = await started(storage)
    seen: list[str] = []

    def commit_listener(publication: Any, _context: Any) -> None:
        raise RuntimeError("commit listener")

    def close_listener() -> None:
        raise RuntimeError("close listener")

    opened["harness"].subscribe_commits(commit_listener)
    opened["harness"].subscribe_commits(lambda publication, _context: seen.append("commit"))
    opened["harness"].subscribe_close(close_listener)
    opened["harness"].subscribe_close(lambda: seen.append("close"))
    assert await opened["root"].commit(
        lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
    )
    await opened["harness"].close(context)
    assert seen == ["commit", "close"]
    assert [str(report) for report in opened["reports"]] == [
        "commit listener",
        "close listener",
    ]


async def test_contains_a_fulfilled_thenable_and_a_rejected_awaitable() -> None:
    unhandled: list[BaseException] = []
    errors: list[BaseException] = []

    class Thenable:
        def __await__(self) -> Any:
            async def settle() -> None:
                return None

            return settle().__await__()

    async def reject() -> None:
        raise RuntimeError("foreign")

    contained(lambda: Thenable(), unhandled.append)
    contained(reject, errors.append)
    await eventually(lambda: len(errors) == 1)
    await asyncio.sleep(0)
    assert unhandled == []
    assert [str(error) for error in errors] == ["foreign"]


def test_reports_a_throwing_clock_once_also_when_the_report_handler_reads_the_clock_again() -> None:
    reports: list[BaseException] = []

    def broken() -> int:
        raise RuntimeError("clock broke")

    clock: Any = None

    def report(error: BaseException) -> None:
        reports.append(error)
        # Reentry, as a handler that commits does: the clock is read again before this report returns.
        if len(reports) < 5:
            clock()

    clock = safe_now(broken, report)
    assert isinstance(clock(), int)
    clock()
    assert [str(error) for error in reports] == ["clock broke"]


async def test_keeps_working_when_on_report_throws_and_falls_back_when_the_clock_throws() -> None:
    registry = create_registry()
    registry.install(define_extension({"name": "tasks", "tasks": [Waiting]}))

    def broken_clock() -> int:
        raise RuntimeError("clock broke")

    def broken_report(error: BaseException) -> None:
        raise RuntimeError("report broke")

    harness = await Harness.open(
        MemoryStorage(),
        {
            "models": {},
            "registry": registry,
            "now": broken_clock,
            "on_report": broken_report,
        },
        context,
    )
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Waiting, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    harness.resume()
    await running(harness, id)
    record = await harness.get_task(id, context)
    assert record is not None and isinstance(record.get("startedAt"), int)
    await harness.abort_task(id, context)
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "aborted"
    await harness.close(context)


async def test_ends_waits_and_running_tasks_with_the_first_error() -> None:
    signals.clear()
    storage = ControlledStorage()
    opened = await started(storage)
    id = await opened["root"].commit(
        lambda tx: tx.create_task(Waiting, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    opened["harness"].resume()
    await running(opened["harness"], id)
    task_wait = asyncio.ensure_future(opened["harness"].wait_for_task(id, context))
    idle = asyncio.ensure_future(opened["harness"].wait_for_idle(context))

    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    # The call that hit it gets the storage error itself.
    assert (
        await _capture(
            opened["root"].commit(
                lambda tx: tx.append_entry(opened["root"].id, {"kind": "note"}), context
            )
        )
        is failure
    )
    for wait in (task_wait, idle):
        error = await _capture(wait)
        assert isinstance(error, SessionFailed) and error.cause is failure
    # Every later call, and a wait that starts now.
    for call in (
        lambda: opened["harness"].get_task(id, context),
        lambda: opened["harness"].wait_for_idle(context),
    ):
        error = await _capture(call())
        assert isinstance(error, SessionFailed) and error.cause is failure
    assert await opened["harness"].closed == {"reason": "failed", "error": failure}
    await eventually(lambda: signals == ["aborted"])
    assert opened["reports"] == [failure]
    await opened["harness"].close(context)


async def test_keeps_feeding_the_other_observers_of_a_view_when_one_throws() -> None:
    opened = await started(MemoryStorage())
    harness, root = opened["harness"], opened["root"]
    failure = RuntimeError("observer broke")

    class FailingObserver:
        def advance_frame(self, *_args: Any) -> None:
            raise failure

        def close_session(self, _failure: Any) -> None:
            pass

    async def attach(_value: Any, _release: Any, _storage: Any, _views: Any) -> Any:
        return FailingObserver()

    await harness._views.attach(root.id, attach, context)
    watch = await root.watch(context)
    seen: list[Any] = []
    watch.start(lambda value, _ops, _context: seen.append(value))
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    await eventually(lambda: len(seen) > 0)
    assert opened["reports"] == [failure]
    await harness.close(context)
    assert (await watch.closed)["reason"] == "session_closed"


async def test_faults_only_the_task_and_ends_the_watch_for_an_unformattable_value() -> None:
    class Unformattable(Exception):
        def __str__(self) -> str:
            raise RuntimeError("cannot format")

    unformattable = Unformattable()

    async def throws_run(_task: Any, _runtime: Any, _ctx: Any) -> None:
        raise unformattable

    Throws = define_task(
        {
            "name": "test.throws-unformattable",
            "version": 1,
            "initial": lambda _input: {"phase": "work"},
            "phases": {"work": throws_run},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    opened = await open_tasks(MemoryStorage(), [Throws])
    harness = opened["harness"]
    root = await harness.root(context)
    watch = await harness.watch_doc(LiveDoc, root.id, context)
    assert watch is not None

    async def listener(_value: Any, _ops: Any, _context: Any) -> None:
        raise unformattable

    watch.start(listener)
    id = await root.commit(
        lambda tx: tx.create_task(Throws, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    harness.resume()
    outcome = (await harness.wait_for_task(id, context))["state"]["outcome"]
    assert outcome["status"] == "faulted"
    assert isinstance(outcome["error"]["message"], str)

    async def touch(tx: Any) -> None:
        live = await tx.doc(LiveDoc, root.id)
        live["compactions"] = []

    await root.commit(touch, context)
    assert (await watch.closed)["reason"] == "listener_error"
    assert await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "note"}), context
    )
    await harness.close(context)

"""Translated upstream ``harness-restart-failure`` suite: what a crash does to work that was running.

Upstream ``test/harness-restart-failure.test.ts`` (spec sections 5.4 and 8.3). A generation whose
request or deferred poll was in flight when the host died is a failed attempt under ``settings.retry``;
every other task, once it was cut off ``settings.restart_limit`` times **in a row**, ends visibly
instead of running again: a tool call settles with an error result and its ``restart_limit``
diagnostic, a compaction fails into ``pi.live.compactionFailure``, a generation ends its run with the
``restart_limit`` reason, and a task of any other kind is ended ``faulted`` by the scheduler. A built-in
task that ends itself that way is reported ``task_interrupted`` with its ``restarts``.

Crashes are simulated the way upstream's ``open()`` does it: every later Storage call of the killed
Harness hangs, so it writes nothing more, not even the ``pending`` records a close writes back for the
tasks it stopped. A file-backed SQLite database is the medium, because the reopened Harness must see
exactly what the dead one stored.

Upstream's helpers arrive under the names this tree already uses for them: its ``unanswered()`` is
:func:`chat_support.unanswered_reached`, which carries the ``reached`` signal the port's plain
``unanswered()`` does not, and its ``stored()`` -- a bare 50 ms wait for a deferred progress commit to
reach Storage -- is replaced by waiting for that commit to be there, which is what the timer only
hoped for.

The ledger against upstream's file, so the count is checkable: it holds 18 cases and this file runs all
18 -- :141, :180, :204, :242, :254, :275, :330, :367, :438, :523, :554, :574, :596, :621, :648, :671,
:712 and :737. Six came with the built-in kinds (:330, :367, :438, :523, :554, :574); the twelve named
below are what #49 added, so a case is never counted twice. Where a counterpart exists elsewhere in
this tree it is named after the case, and neither was counted as coverage: a case that stays green with
the mechanism it names stubbed does not prove that mechanism.

    "charges the attempt: keeps the partial, waits for the retry backoff, and answers on the next
    attempt" (:141), "sleeps the backoff of ``settings.retry`` before the next attempt" (:180), "ends
    the run interrupted once no retry is left, keeping every partial" (:204), "ends the run at once when
    retries are disabled" (:242; the request variant of it is asserted by
    ``test_harness_continue.py::test_answers_a_run_interrupted_by_a_host_that_died_while_the_model_answered``),
    "resends after a clean close: a close is not a crash" (:254), "does not charge a generation that
    sleeps in a retry or waits for its tools" (:275), "charges a request once when a crash is followed by
    a clean close before the charge" (:596), "ends a run restart_limit when charging the interrupted
    request keeps being cut off" (:621), "drops the partial past the limit, where converting it may be
    what keeps failing" (:648), "ends a run someone aborted as aborted when its abort handler is cut off
    restartLimit times" (:671; the task half of it is
    ``test_harness_blocked.py::test_faults_a_task_whose_abort_handler_is_cut_off_restart_limit_times``),
    "reports task_interrupted for a run that ends interrupted and for a task that ends at the limit"
    (:712; this file asserts the report for the tasks it cuts off, and
    ``test_harness_after_run.py`` for a generation whose ``finish`` was cut off), and "resolves the
    settings no more often than before: only a task that was cut off reads the limit" (:737).

:554 and :574 were named in this docstring as not run until #48 landed them: the first with ``retry``
switched off, so the run ends ``interrupted`` and ``models.cancelDeferred()`` is called once; the second
with a host clock the test moves backwards between the two hosts, so a poll that looks not due yet is
still charged because its fetch was cut off.

The file runs **19** cases, one more than upstream's 18. The extra one is
``test_removes_a_cut_off_compaction_without_failing_it``, **port-authored, not translated**: upstream's
file reaches neither half of ``live.ts:421-447`` for a compaction, so the branch that removes a cut-off
compaction's status instead of failing it had no case at all -- the whole suite stayed green with it
deleted. It is named here and marked in the 1.5.0 parity report as the port's own, so it is never
counted as upstream coverage.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from chat_support import (
    ChatSetup,
    FauxModels,
    all_entries,
    assistant_message,
    chat_setup,
    close_killed,
    faux_text,
    hang_after_partial,
    live_of,
    only_failure,
    open_chat,
    open_killable,
    text_of,
    unanswered_reached,
)
from harness_support import add_section
from session_support import context, document_changes, eventually, observe_commits
from task_support import aborted, deferred
from tool_support import add_tool, calls, tool

from pi_durable import LiveDoc, MemoryStorage
from pi_durable.ids import ROOT_CONVERSATION_ID, ConversationId, SubmissionId
from pi_durable.providers import Model
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage
from pi_durable.testing import commit_writes
from pi_durable.types import TaskRecord, TaskWrite

_directories: list[Path] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Any:
    yield
    await close_killed()
    while _directories:
        shutil.rmtree(_directories.pop(), ignore_errors=True)


def sqlite_path() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-restart-"))
    _directories.append(directory)
    return directory / "session.sqlite"


async def open(path: Path, setup: ChatSetup) -> dict[str, Any]:
    """Open a chat over the SQLite file at ``path``; ``kill()`` simulates a crash of its process."""

    return await open_killable(open_posix_sqlite_storage, path, setup)


async def _hang() -> Any:
    """Stay in the phase, as a section that exhausts the host's memory would not."""

    await asyncio.Event().wait()


async def live(harness: Any, conversation_id: ConversationId = ROOT_CONVERSATION_ID) -> dict[str, Any]:
    """``pi.live`` as the Harness has it, empty when the conversation is idle."""

    return live_of(await harness.snapshot(LiveDoc, conversation_id, context))


async def stored_live(path: Path, conversation_id: ConversationId) -> dict[str, Any]:
    """``pi.live`` as Storage holds it, read on a second connection past the Harness that wrote it."""

    raw = await open_posix_sqlite_storage(str(path))
    try:
        address: dict[str, Any] = {
            "kind": LiveDoc.definition["kind"],
            "scope": {"kind": "conversation", "conversationId": conversation_id},
        }
        record = await raw.find_document(cast(Any, address), "current", context)
        if record is None:
            return {}
        found = await raw.document(record["id"], "current", context)
        return {} if found is None else dict(found["value"])
    finally:
        await raw.close(context)


async def settled(harness: Any, id: SubmissionId) -> dict[str, Any]:
    """The settlement of a submission, as upstream's helper reads it."""

    submission = await harness.submission(id, context)
    return cast(dict[str, Any], await submission.wait(context))


def streaming() -> ChatSetup:
    """A setup whose model streams a long answer; its partials are stored as soon as they are committed.

    Upstream's ``streaming()`` streams 400 one-token tokens at twenty a second and the case crashes
    partway through. What the cases rest on is only that a request is still in flight with a partial
    stored, which is :func:`hang_after_partial`'s shape: it commits one partial and then stays in the
    request until its signal ends, so no wall-clock streaming window can make the case flaky.
    """

    setup = chat_setup()
    setup.faux.set_responses([hang_after_partial("z" * 400)])
    setup.settings["progress"] = {"persist_within_ms": 0}
    return setup


async def stored_generation(
    path: Path, conversation_id: ConversationId, attempt: int
) -> dict[str, Any]:
    """Wait until the attempt's partial reached Storage, and return the stored ``pi.live`` then.

    Upstream's ``stored()`` is a bare 50 ms wait hoping the deferred progress commit landed; this is
    the wait for the commit itself, read on a second connection past the Harness that wrote it.
    ``attempt`` is pinned because a reopened Harness still holds the *previous* attempt's partial
    until its own first commit replaces it.
    """

    seen: dict[str, Any] = {}

    async def stored() -> bool:
        state = await stored_live(path, conversation_id)
        generation = state.get("generation") or {}
        if generation.get("attempt") != attempt or generation.get("message") is None:
            return False
        seen["state"] = state
        return True

    await eventually(stored)
    return cast(dict[str, Any], seen["state"])


async def crash_while_streaming(path: Path, setup: ChatSetup) -> tuple[SubmissionId, Any]:
    """Submit an input to a fresh chat at ``path`` and crash once its streamed partial is stored."""

    opened = await open(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    id = (await root.submit({"type": "input", "content": "hi"}, context)).id

    async def streaming_partial() -> bool:
        return (await live(harness)).get("generation", {}).get("message") is not None

    await eventually(streaming_partial)
    task_id = (await live(harness))["run"]["taskId"]
    await stored_generation(path, root.id, 1)
    opened["kill"]()
    return id, task_id


async def crash_again(path: Path, setup: ChatSetup, task_id: Any) -> None:
    """Reopen after a crash in the middle of a request, with another streaming answer, and crash again."""

    opened = await open(path, setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    # The first attempt was charged; the second one streams.
    async def second_attempt() -> bool:
        record = await harness.get_task(task_id, context)
        state = record["state"] if record is not None else None
        if state is None or state["status"] != "running":
            return False
        checkpoint = state["checkpoint"]
        return checkpoint["phase"] == "request" and checkpoint["attempt"] == 2

    await eventually(second_attempt)
    await stored_generation(path, root.id, 2)
    opened["kill"]()


def roles_of(request: dict[str, Any]) -> list[str]:
    """The roles of a request's messages, as upstream's cases read the context that was sent."""

    return [str(message["role"]) for message in request["messages"]]


class _FirstFetchInFlight(FauxModels):
    """The faux models with the first deferred fetch held in flight until its signal ends.

    Upstream wraps the faux models in a Proxy for the one method; a subclass that keeps the setup's
    own scripted state is this port's shape of the same thing, so ``call_count`` and
    ``cancelled_deferred`` still read what the Harness actually did.
    """

    fetching: Any
    fetches: int

    def adopt(self, inner: FauxModels, fetching: Any) -> None:
        """Take ``inner``'s scripted state, so the setup keeps one model of the same kind."""

        self.__dict__.update(inner.__dict__)
        self.fetching = fetching
        self.fetches = 0

    async def fetch_deferred(
        self, model: Any, handle: Any, options: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        self.fetches += 1
        if self.fetches > 1:
            return await super().fetch_deferred(model, handle, options)
        self.fetching.resolve()
        await aborted(cast(Mapping[str, Any], options)["signal"])
        raise AssertionError("the first fetch ends only when the host stops")


def deferred_setup(poll_after_ms: int = 10) -> tuple[ChatSetup, Any, Callable[[], int]]:
    """A setup whose model answers deferred and whose first fetch of each deferred response is in
    flight until the Harness stops; later fetches answer.

    Upstream's ``deferredSetup``: ``fetching`` settles when the first fetch began, ``fetches()``
    counts them, and the in-flight fetch ends only through the invocation's signal -- the same
    ``Promise.race`` the port's ``task_support.aborted`` is.
    """

    base = chat_setup({"deferred": {"pollAfterMs": poll_after_ms}})
    base.settings["stream"] = {"deferred": True}
    base.faux.set_responses([assistant_message("deferred answer")])
    fetching = deferred()
    models = _FirstFetchInFlight()
    models.adopt(base.models, fetching)
    base.models = models
    return base, fetching, lambda: models.fetches


async def crash_while_polling(
    path: Path, setup: ChatSetup, fetching: Any
) -> tuple[SubmissionId, Any]:
    """Submit to a fresh chat at ``path``, let its deferred response be fetched, crash during that fetch."""

    opened = await open(path, setup)
    opened["harness"].resume()
    submission = await opened["root"].submit({"type": "input", "content": "hi"}, context)
    id = submission.id
    await fetching
    task_id = (await live(opened["harness"]))["run"]["taskId"]
    record = await opened["harness"].get_task(task_id, context)
    assert record["state"]["status"] == "running"
    assert record["state"]["checkpoint"]["phase"] == "poll"
    opened["kill"]()
    return id, task_id


def live_values(harness: Any) -> list[dict[str, Any]]:
    """Every ``pi.live`` the Harness publishes, oldest first -- upstream's ``liveValues``."""

    values: list[dict[str, Any]] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] == "pi.live" and change["value"] is not None:
                values.append(change["value"])

    observe_commits(harness, observe)
    return values


async def test_charges_a_crash_during_a_polls_fetch_and_polls_the_same_deferred_response_again() -> None:
    """Upstream "charges a crash during a poll's fetch, then polls the same deferred response again".

    The poll's fetch was in flight when the host died, so the attempt is charged (``interrupted``
    reads the ``poll`` checkpoint, not the ``prepare`` one) and the next attempt polls the same
    handle: one request, polled twice, with the provider's job neither dropped nor resubmitted.
    """

    path = sqlite_path()
    setup, fetching, fetches = deferred_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 1}
    id, task_id = await crash_while_polling(path, setup, fetching.promise)

    opened = await open(path, setup)
    values = live_values(opened["harness"])
    assert (await opened["harness"].get_task(task_id, context))["restarts"] == 1
    before = setup.now()
    opened["harness"].resume()
    answer = await settled(opened["harness"], id)
    assert answer["status"] == "done"
    retried = [value["generation"] for value in values if value.get("generation", {}).get("retry")]
    assert len(retried) == 1
    assert retried[0]["attempt"] == 1
    assert retried[0]["retry"]["error"] == (
        "The host stopped while the deferred answer was fetched, on attempt 1"
    )
    assert isinstance(retried[0]["retry"]["at"], int)
    # The backoff is the 1ms this case asked for, not the 2000ms default -- the bound sits far below the
    # default and far above the scheduling jitter between `resume()` and the commit that writes it. Host-
    # facing settings use the port's PEP 8 names (`harness/util.py:180-186`); a camelCase key merges as an
    # unknown field and the default silently answers for it, as `maxRetries`/`baseDelayMs` used to here.
    assert retried[0]["retry"]["at"] - before < 1_000
    assert any(
        value.get("generation", {}).get("attempt") == 2 and "deferred" in value["generation"]
        for value in values
    )
    # One request, polled twice: the provider's job is neither dropped nor submitted again.
    assert setup.faux.call_count == 1
    assert fetches() == 2
    assert setup.faux.cancelled_deferred == []
    entries = await all_entries(opened["root"])
    assert text_of(entries[-1]["model"][0]) == "deferred answer"
    await opened["harness"].close(context)


async def test_cancels_the_deferred_response_when_a_crash_during_its_poll_ends_the_run() -> None:
    """Upstream "cancels the deferred response when a crash during its poll ends the run".

    With no retry left the run ends ``interrupted`` on the charged attempt, and the provider's job for
    the answer that was being polled is cancelled -- once, and only because the poll was cut off.
    """

    path = sqlite_path()
    setup, fetching, _ = deferred_setup()
    setup.settings["retry"] = {"enabled": False}
    id, task_id = await crash_while_polling(path, setup, fetching.promise)

    opened = await open(path, setup)
    opened["harness"].resume()
    text = "The host stopped while the deferred answer was fetched, on attempt 1"
    settlement = await settled(opened["harness"], id)
    assert settlement["status"] == "unanswered"
    assert settlement["reason"] == "interrupted"
    assert settlement["detail"] == text
    only_failure(await live(opened["harness"]), task_id, [id], "interrupted", text)
    assert len(setup.faux.cancelled_deferred) == 1
    await opened["harness"].close(context)


async def test_charges_an_interrupted_poll_even_when_the_clock_moved_backwards() -> None:
    """Upstream "charges an interrupted poll even when the clock moved backwards".

    The replacement host's clock is behind the one that died, so the poll is not due yet by its own
    ``until``: the charge does not come from the clock reading the record, it comes from the fetch that
    was cut off, so the run still ends ``interrupted`` rather than sleeping on a poll no one will make.
    """

    path = sqlite_path()
    setup, fetching, _ = deferred_setup(10)
    clock = {"now": 1000}
    setup.now = lambda: clock["now"]
    setup.settings["retry"] = {"enabled": False}
    first = await open(path, setup)
    first["harness"].resume()
    id = (await first["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def polling() -> bool:
        return (await live(first["harness"])).get("generation", {}).get("deferred") is not None

    await eventually(polling)
    clock["now"] = 1010
    first["harness"].resume()
    await fetching.promise
    first["kill"]()

    # A replacement host whose clock is behind: the poll looks not due yet, but its fetch was cut off.
    clock["now"] = 1000
    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await settled(opened["harness"], id)
    assert settlement["status"] == "unanswered"
    assert settlement["reason"] == "interrupted"
    await opened["harness"].close(context)


async def test_fails_a_compaction_into_compaction_failure() -> None:
    """Upstream ``harness-restart-failure.test.ts``: "fails a compaction into compactionFailure"."""

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    # Little recent context is kept, so the first turn is summarized.
    setup.settings["compaction"] = {"enabled": False, "keep_recent_tokens": 10}
    setup.faux.set_responses([assistant_message("a1"), assistant_message("a2")])
    opened = await open(path, setup)
    opened["harness"].resume()
    await (await opened["root"].submit({"type": "input", "content": "u1 " * 200}, context)).wait(context)
    await (await opened["root"].submit({"type": "input", "content": "u2 " * 200}, context)).wait(context)
    # The summary request never returns.
    hanging = unanswered_reached()
    setup.faux.set_responses([hanging["step"]])
    compaction = await opened["root"].compact(None, context)
    await hanging["reached"].promise
    opened["kill"]()

    opened = await open(path, setup)
    opened["harness"].resume()
    error = "Compaction was interrupted once"
    record = await opened["harness"].wait_for_task(compaction, context)
    assert record["state"]["outcome"] == {
        "status": "failed",
        "error": {"message": error, "detail": {"reason": "restart_limit"}},
    }
    state = await live(opened["harness"])
    assert state["compactionFailure"] == {"taskId": compaction, "trigger": "manual", "error": error}
    assert state.get("compactions") is None

    async def reported() -> bool:
        return any(info.get("kind") == "task_interrupted" for info in setup.infos)

    await eventually(reported)
    assert setup.infos == [
        {
            "kind": "task_interrupted",
            "task": {"id": compaction, "kind": "pi.compaction", "conversationId": 1},
            "reason": "restart_limit",
            "restarts": 1,
        }
    ]
    await opened["harness"].close(context)


async def test_removes_a_cut_off_compaction_without_failing_it() -> None:
    """Port-authored, not upstream-translated: the ``cutOff`` half of ``live.ts:421-447`` for a compaction.

    Upstream's file has no case that reaches this branch, so nothing tested it: a compaction whose
    **abort handler** is cut off ``settings.restart_limit`` times is faulted by the scheduler for the
    limit, and the harness cleanup that follows must remove its ``pi.live`` status rather than record a
    ``compactionFailure`` -- an aborted compaction is no failure, and past the limit the cleanup the
    handler was doing may be what cut it off. ``test_fails_a_compaction_into_compaction_failure`` above
    is the other side: a compaction whose own phase ends it failed does record the failure.

    Deliberately not counted as upstream coverage: the ledger in this module's docstring runs 18
    translated cases and this nineteenth is the port's own.
    """

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    setup.settings["compaction"] = {"enabled": False, "keep_recent_tokens": 10}
    setup.faux.set_responses([assistant_message("a1"), assistant_message("a2")])
    opened = await open(path, setup)
    opened["harness"].resume()
    await (await opened["root"].submit({"type": "input", "content": "u1 " * 200}, context)).wait(context)
    await (await opened["root"].submit({"type": "input", "content": "u2 " * 200}, context)).wait(context)
    # The summary request never returns, and the host dies with it: the crash charges the attempt.
    hanging = unanswered_reached()
    setup.faux.set_responses([hanging["step"]])
    compaction = await opened["root"].compact(None, context)
    await hanging["reached"].promise
    opened["kill"]()

    # The host asked to abort the compaction and died with the mark set. ``Harness.abort_task`` cannot
    # leave it that way here: marking writes a new state, and progress clears the restart count, which
    # is what would let the abort handler run and remove the status itself. So the mark is written to
    # Storage directly -- the same sanctioned seed the cut-off-partial case above uses for the count --
    # and the count itself comes from the real crash, not from a seed.
    raw = await open_posix_sqlite_storage(str(path))
    try:
        running = await raw.task(compaction, context)
        assert running is not None
        marked = cast(TaskRecord, {**running, "abortRequested": True})
        await commit_writes(raw, [TaskWrite(type="task", value=marked)], context)
    finally:
        await raw.close(context)

    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await opened["harness"].wait_for_task(compaction, context)
    # The scheduler faulted the abort handler for the limit rather than running it.
    assert settlement["state"]["outcome"]["status"] == "faulted"
    assert settlement["state"]["outcome"]["error"]["detail"] == {"reason": "restart_limit"}
    assert settlement["state"]["outcome"]["error"]["message"] == (
        f"Abort handler of task {compaction} (pi.compaction) was cut off 1 times"
    )
    # The status is gone and no failure was recorded: an aborted compaction cut off the limit is not a
    # failed one. Deleting the branch in ``settle_scheduler_outcome`` writes `compactionFailure` here.
    state = await live(opened["harness"])
    assert state.get("compactions") is None
    assert "compactionFailure" not in state
    await opened["harness"].close(context)


async def test_ends_a_tool_call_with_an_error_result_instead_of_running_it_again() -> None:
    """Upstream "ends a tool call with an error result instead of running it again".

    The call was cut off ``restart_limit`` times in a row, so its ``execute`` phase settles it from the
    slot instead of running the tool a third time: an error result the model reads, carrying the
    ``restart_limit`` diagnostic, and a task that failed with that detail.
    """

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 2
    setup.settings["progress"] = {"output_interval_ms": 0, "persist_within_ms": 0}
    runs = {"count": 0}
    running = {"current": deferred()}

    async def heavy(_args: Any, api: Any, call_context: Any) -> Any:
        runs["count"] += 1
        api.output("partial\n")
        running["current"].resolve()
        await aborted(call_context.abort_signal)
        return {}

    add_tool(
        setup.registry,
        tool(
            "heavy",
            heavy,
            description="Never finishes",
            parameters={"type": "object", "properties": {}},
            replay="safe",
        ),
    )

    def saw(request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        return assistant_message([faux_text(f"saw: {text_of(request['messages'][-1])}")])

    setup.faux.set_responses([calls(("heavy", {}, "c1")), saw])
    opened = await open(path, setup)
    opened["harness"].resume()
    root_id = opened["root"].id
    id = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id
    for _crash in (1, 2):
        await running["current"].promise
        running["current"] = deferred()

        async def stored() -> bool:
            state = await stored_live(path, root_id)
            return any(slot.get("output") == "partial\n" for slot in state.get("tools") or ())

        await eventually(stored)
        opened["kill"]()
        opened = await open(path, setup)
        opened["harness"].resume()

    assert (await settled(opened["harness"], id))["status"] == "done"
    assert runs["count"] == 2
    result = next(entry for entry in await all_entries(opened["root"]) if entry["kind"] == "pi.tool-result")
    message = cast(Mapping[str, Any], result["model"][0])
    assert message["isError"] is True
    diagnostics = cast(dict[str, Any], result["data"])["diagnostics"]
    assert diagnostics == [
        {
            "severity": "error",
            "code": "restart_limit",
            "message": "Tool heavy was interrupted 2 times in a row; it is not run again",
        }
    ]
    # The slot's partial output is kept, as for an interrupted call.
    assert "partial" in (text_of(message) or "")
    page = await opened["harness"].commit(lambda tx: tx.scan_tasks({"kind": "pi.tool"}, 10), context)
    tool_task = page["items"][0]
    state = tool_task["state"]
    assert state["status"] == "terminal"
    assert state["outcome"]["status"] == "failed"
    assert state["outcome"]["error"]["detail"] == {"reason": "restart_limit"}
    assert (
        {
            "kind": "task_interrupted",
            "task": {"id": tool_task["id"], "kind": "pi.tool", "conversationId": root_id},
            "reason": "restart_limit",
            "restarts": 2,
        }
        in setup.infos
    )
    await opened["harness"].close(context)


async def test_ends_a_run_whose_generation_is_cut_off_restart_limit_times_outside_a_request() -> None:
    """Upstream "ends a run whose generation is cut off restartLimit times outside a request".

    Preparation never finishes, as one that exhausts the host's memory would not. At the limit its
    ``prepare`` phase ends the run with the ``restart_limit`` reason instead of starting again, and the
    submission settles ``unanswered`` with it.
    """

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    reached = deferred()

    async def preamble(*_args: Any) -> Any:
        reached.resolve()
        await _hang()

    add_section(setup.registry, "preamble", preamble)
    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id
    await reached.promise
    task_id = (await live(opened["harness"]))["run"]["taskId"]
    opened["kill"]()

    opened = await open(path, setup)
    opened["harness"].resume()
    text = "The generation was interrupted once in its prepare phase"
    settlement = await settled(opened["harness"], id)
    assert {key: settlement.get(key) for key in ("status", "reason", "detail")} == {
        "status": "unanswered",
        "reason": "restart_limit",
        "detail": text,
    }
    state = await live(opened["harness"])
    only_failure(state, task_id, [id], "restart_limit", text)

    async def reported() -> bool:
        return len(setup.infos) > 0

    await eventually(reported)
    assert any(
        info.get("kind") == "task_interrupted"
        and info.get("reason") == "restart_limit"
        and info.get("restarts") == 1
        for info in setup.infos
    )
    await opened["harness"].close(context)


async def test_charges_the_attempt_keeps_the_partial_waits_for_the_backoff_and_answers_next() -> None:
    """Upstream "charges the attempt: keeps the partial, waits for the retry backoff, and answers".

    The full upstream title is "charges the attempt: keeps the partial, waits for the retry backoff,
    and answers on the next attempt". The request was in flight when the host died, so the attempt is a
    failed one under ``settings.retry``: the partial stays as an aborted assistant entry excluded from
    the next request, ``pi.live`` shows the backoff with why, and attempt 2 answers.
    """

    path = sqlite_path()
    first = streaming()
    first.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 1}
    id, task_id = await crash_while_streaming(path, first)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 1}
    sent: list[list[str]] = []

    def answer(request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        sent.append(roles_of(request))
        return assistant_message("answer")

    setup.faux.set_responses([answer])
    opened = await open(path, setup)
    values = live_values(opened["harness"])
    partial = text_of((await live(opened["harness"])).get("generation", {}).get("message"))
    assert (await opened["harness"].get_task(task_id, context))["restarts"] == 1
    opened["harness"].resume()
    assert (await settled(opened["harness"], id))["status"] == "done"
    # The backoff showed in `pi.live` with why.
    retried = [
        value["generation"] for value in values if value.get("generation", {}).get("retry") is not None
    ]
    assert len(retried) == 1
    assert set(retried[0]) == {"attempt", "retry"}
    assert retried[0]["attempt"] == 1
    assert retried[0]["retry"]["error"] == (
        "The host stopped while the model was answering, on attempt 1"
    )
    assert isinstance(retried[0]["retry"]["at"], int)
    assert any(value.get("generation", {}).get("attempt") == 2 for value in values)
    entries = await all_entries(opened["root"])
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant", "pi.assistant"]
    converted = entries[1]["model"][0]
    assert converted["stopReason"] == "aborted"
    assert text_of(converted) == partial
    # The aborted partial is excluded from the next request.
    assert sent == [["user"]]
    assert await live(opened["harness"]) == {}
    await opened["harness"].close(context)


async def test_sleeps_the_backoff_of_settings_retry_before_the_next_attempt() -> None:
    """Upstream "sleeps the backoff of ``settings.retry`` before the next attempt".

    The charged attempt sleeps until ``now + baseDelayMs`` with the ``retry`` checkpoint and shows the
    same in ``pi.live``; the restart count is spent, and the Session's next needed wake is that moment.
    """

    path = sqlite_path()
    first = streaming()
    first.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 60_000}
    _, task_id = await crash_while_streaming(path, first)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 2, "base_delay_ms": 60_000}
    now = 1_000_000
    setup.now = lambda: now
    opened = await open(path, setup)
    opened["harness"].resume()

    async def sleeping() -> bool:
        record = await opened["harness"].get_task(task_id, context)
        return record is not None and record["state"]["status"] == "sleeping"

    await eventually(sleeping)
    record = await opened["harness"].get_task(task_id, context)
    assert record["state"]["status"] == "sleeping"
    assert record["state"]["until"] == now + 60_000
    assert record["state"]["checkpoint"]["phase"] == "retry"
    # The charge is spent: the sleep is the retry, not another cut-off.
    assert "restarts" not in record
    assert (await live(opened["harness"])).get("generation") == {
        "attempt": 1,
        "retry": {
            "at": now + 60_000,
            "error": "The host stopped while the model was answering, on attempt 1",
        },
    }
    assert opened["harness"].wake() == {"active": False, "at": now + 60_000}
    await opened["harness"].close(context)


async def test_ends_the_run_interrupted_once_no_retry_is_left_keeping_every_partial() -> None:
    """Upstream "ends the run interrupted once no retry is left, keeping every partial".

    Both attempts were cut off, so the retry policy is spent and the run ends ``interrupted`` with the
    second attempt's text; each committed partial became its own aborted assistant entry and no further
    request is sent.
    """

    path = sqlite_path()
    first = streaming()
    first.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    id, task_id = await crash_while_streaming(path, first)
    # Attempt 2 is interrupted too.
    second = streaming()
    second.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    await crash_again(path, second, task_id)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    opened = await open(path, setup)
    opened["harness"].resume()
    text = "The host stopped while the model was answering, on attempt 2"
    settlement = await settled(opened["harness"], id)
    assert settlement["status"] == "unanswered"
    assert settlement["reason"] == "interrupted"
    assert settlement["detail"] == text
    only_failure(await live(opened["harness"]), task_id, [id], "interrupted", text)
    record = await opened["harness"].wait_for_task(task_id, context)
    assert record["state"]["outcome"] == {
        "status": "failed",
        "error": {"message": text, "detail": {"reason": "interrupted"}},
    }
    entries = await all_entries(opened["root"])
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant", "pi.assistant"]
    assert [entry["model"][0]["stopReason"] for entry in entries[1:]] == ["aborted", "aborted"]
    # No request was sent after the second crash.
    assert setup.faux.call_count == 0
    await opened["harness"].close(context)


async def test_ends_the_run_at_once_when_retries_are_disabled() -> None:
    """Upstream "ends the run at once when retries are disabled".

    With ``settings.retry`` off the cut-off attempt is not retried at all: the reopened host ends the
    run ``interrupted`` on the first charge, without sending anything.
    """

    path = sqlite_path()
    first = streaming()
    id, _task_id = await crash_while_streaming(path, first)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": False}
    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await settled(opened["harness"], id)
    assert settlement["status"] == "unanswered"
    assert settlement["reason"] == "interrupted"
    assert setup.faux.call_count == 0
    await opened["harness"].close(context)


async def test_resends_after_a_clean_close_a_close_is_not_a_crash() -> None:
    """Upstream "resends after a clean close: a close is not a crash".

    A close hands the in-flight attempt back ``pending`` with no restart count, so the next host resends
    it as the same attempt rather than charging a cut-off one.
    """

    path = sqlite_path()
    first = streaming()
    first.settings["retry"] = {"enabled": False}
    opened = await open(path, first)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def streaming_partial() -> bool:
        return (await live(opened["harness"])).get("generation", {}).get("message") is not None

    await eventually(streaming_partial)
    task_id = (await live(opened["harness"]))["run"]["taskId"]
    await opened["harness"].close(context)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": False}
    setup.faux.set_responses([assistant_message("answer")])
    reopened = await open(path, setup)
    # A close writes the task back `pending`: nothing was cut off, so nothing is charged.
    assert "restarts" not in await reopened["harness"].get_task(task_id, context)
    reopened["harness"].resume()
    assert (await settled(reopened["harness"], id))["status"] == "done"
    await reopened["harness"].close(context)


async def test_does_not_charge_a_generation_that_sleeps_in_a_retry_or_waits_for_its_tools() -> None:
    """Upstream "does not charge a generation that sleeps in a retry or waits for its tools".

    Only a task found ``running`` at open was cut off. Asleep in its retry backoff and waiting for a
    tool round are both not running, so neither crash charges the generation and the run answers.
    """

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 60_000}
    gate = deferred()
    started = deferred()

    async def hold(_args: Any, _api: Any, call_context: Any) -> Any:
        started.resolve()
        await gate.promise
        return {}

    registration = tool(
        "hold",
        hold,
        description="Waits",
        parameters={"type": "object", "properties": {}},
        replay="safe",
    )
    add_tool(setup.registry, registration)
    error = assistant_message([], stop_reason="error", error_message="503 Service Unavailable")
    setup.faux.set_responses([error])
    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id
    task_id = (await live(opened["harness"]))["run"]["taskId"]

    async def sleeping() -> bool:
        record = await opened["harness"].get_task(task_id, context)
        return record is not None and record["state"]["status"] == "sleeping"

    await eventually(sleeping)
    opened["kill"]()

    # Asleep in its retry: not running, so not cut off.
    later = chat_setup()
    later.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 60_000}
    later.now = lambda: int(time.time() * 1000) + 120_000
    add_tool(later.registry, registration)
    later.faux.set_responses(
        [calls(("hold", {}, "c1")), assistant_message([faux_text("done")])]
    )
    opened = await open(path, later)
    assert "restarts" not in await opened["harness"].get_task(task_id, context)
    opened["harness"].resume()
    # Waiting for its tool round: not running either.
    await started.promise
    waiting = await opened["harness"].get_task(task_id, context)
    assert waiting["state"]["status"] == "waiting"
    assert waiting["state"]["checkpoint"]["phase"] == "tools"
    opened["kill"]()

    opened = await open(path, later)
    assert "restarts" not in await opened["harness"].get_task(task_id, context)
    gate.resolve()
    opened["harness"].resume()
    assert (await settled(opened["harness"], id))["status"] == "done"
    await opened["harness"].close(context)


async def test_charges_a_request_once_when_a_crash_is_followed_by_a_clean_close_before_the_charge() -> None:
    """Upstream "charges a request once when a crash is followed by a clean close before the charge".

    The first host died with the request in flight, so the next host charges that attempt at open; a
    close of that host, before it resumed anything, hands the task back without a second count. One
    charge, one ``retry`` in ``pi.live``, one aborted partial.
    """

    path = sqlite_path()
    first = streaming()
    first.settings["retry"] = {"enabled": True, "max_retries": 3, "base_delay_ms": 1}
    id, task_id = await crash_while_streaming(path, first)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 3, "base_delay_ms": 1}
    setup.faux.set_responses([assistant_message("answer")])
    opened = await open(path, setup)
    await opened["harness"].close(context)

    opened = await open(path, setup)
    assert (await opened["harness"].get_task(task_id, context))["restarts"] == 1
    values = live_values(opened["harness"])
    opened["harness"].resume()
    assert (await settled(opened["harness"], id))["status"] == "done"
    charged = [
        value["generation"] for value in values if value.get("generation", {}).get("retry") is not None
    ]
    assert [value["attempt"] for value in charged] == [1]
    assert [entry["kind"] for entry in await all_entries(opened["root"])] == [
        "pi.user",
        "pi.assistant",
        "pi.assistant",
    ]
    await opened["harness"].close(context)


async def test_ends_a_run_restart_limit_when_charging_the_interrupted_request_keeps_being_cut_off() -> None:
    """Upstream "ends a run ``restart_limit`` when charging the interrupted request keeps being cut off".

    At the limit the charge is what the generation does first, so it ends the run with the
    ``restart_limit`` reason -- and, being *at* the limit rather than past it, it still keeps the
    partial the dead host committed.
    """

    path = sqlite_path()
    first = streaming()
    first.settings["restart_limit"] = 1
    id, task_id = await crash_while_streaming(path, first)
    # At the limit: the partial is still kept.
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    opened = await open(path, setup)
    partial = text_of((await live(opened["harness"])).get("generation", {}).get("message"))
    opened["harness"].resume()
    text = "The generation was interrupted once in its request phase"
    settlement = await settled(opened["harness"], id)
    assert {key: settlement.get(key) for key in ("status", "reason", "detail")} == {
        "status": "unanswered",
        "reason": "restart_limit",
        "detail": text,
    }
    only_failure(await live(opened["harness"]), task_id, [id], "restart_limit", text)
    entries = await all_entries(opened["root"])
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant"]
    assert text_of(entries[1]["model"][0]) == partial
    assert setup.faux.call_count == 0
    await opened["harness"].close(context)


async def test_drops_the_partial_past_the_limit_where_converting_it_may_be_what_keeps_failing() -> None:
    """Upstream "drops the partial past the limit, where converting it may be what keeps failing".

    Past the limit the run ends without the cleanup: converting the partial may itself be what cut the
    task off, so nothing is appended and ``pi.live`` keeps no partial.
    """

    path = sqlite_path()
    first = streaming()
    id, task_id = await crash_while_streaming(path, first)
    # As if the charge had been cut off twice before: the open after this crash counts 3, past the limit of 2.
    raw = await open_posix_sqlite_storage(str(path))
    try:
        record = await raw.task(task_id, context)
        assert record is not None
        seeded = cast(TaskRecord, {**record, "restarts": 2})
        await commit_writes(raw, [TaskWrite(type="task", value=seeded)], context)
    finally:
        await raw.close(context)

    setup = chat_setup()
    setup.settings["restart_limit"] = 2
    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await settled(opened["harness"], id)
    assert {key: settlement.get(key) for key in ("status", "reason")} == {
        "status": "unanswered",
        "reason": "restart_limit",
    }
    assert settlement["detail"] == (
        "The generation was interrupted 3 times in a row in its request phase"
    )
    assert [entry["kind"] for entry in await all_entries(opened["root"])] == ["pi.user"]
    assert (await live(opened["harness"])).get("generation") is None
    await opened["harness"].close(context)


class _HangingCancelModels(FauxModels):
    """The faux models whose deferred cancel is held in flight until the invocation's signal ends.

    A polling generation's abort handler cancels the response it polled; holding that call open is what
    cuts the handler off. A subclass keeps the provider's real ``cancel_deferred`` signature -- assigning
    over the method would let the case drift from what the Harness actually calls.
    """

    cancels: int

    def __init__(self, *, deferred: Mapping[str, Any] | None = None) -> None:
        super().__init__(deferred=deferred)
        self.cancels = 0

    async def cancel_deferred(
        self, model: Model, handle: Mapping[str, Any], options: Mapping[str, Any] | None = None
    ) -> None:
        # Counting and then hanging is upstream's `cancels++; await waitFor(() => cancels === crash)`.
        self.cancels += 1
        await aborted((options or {})["signal"])


async def test_ends_an_aborted_run_as_aborted_when_its_abort_handler_is_cut_off_restart_limit_times() -> None:
    """Upstream "ends a run someone aborted as aborted when its abort handler is cut off restartLimit times".

    Cancelling the deferred answer is part of the generation's abort handler; the handler is cut off the
    limit times, so the scheduler faults the task for the limit -- but the task was aborted, and keeps
    that meaning: the run ends ``aborted`` with the limit as its detail.
    """

    path = sqlite_path()
    setup = chat_setup()
    # The model answers deferred and its poll is due an hour out, so the generation sleeps in the poll
    # and the abort handler is the only thing left to run.
    models = _HangingCancelModels(deferred={"pollAfterMs": 3_600_000})
    setup.models = models
    setup.settings["restart_limit"] = 2
    setup.settings["stream"] = {"deferred": True}
    setup.faux.set_responses([assistant_message("never")])

    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id
    task_id = (await live(opened["harness"]))["run"]["taskId"]

    async def sleeping() -> bool:
        record = await opened["harness"].get_task(task_id, context)
        return record is not None and record["state"]["status"] == "sleeping"

    await eventually(sleeping)
    aborting = asyncio.ensure_future(opened["root"].abort(context))
    aborting.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)

    for crash in (1, 2):

        async def cancelled(count: int = crash) -> bool:
            return models.cancels == count

        await eventually(cancelled)
        opened["kill"]()
        opened = await open(path, setup)
        assert (await opened["harness"].get_task(task_id, context))["restarts"] == crash
        opened["harness"].resume()

    # Upstream drops the abort it fired (`void root.abort().catch(() => {})`): its host died with the
    # abort handler in flight, so that wait for idle never settles. asyncio has no abandoned promise --
    # the task the port fired has to be reaped while its loop can still run what cancelling it starts.
    aborting.cancel()
    await asyncio.gather(aborting, return_exceptions=True)

    detail = f"Abort handler of task {task_id} (pi.generation) was cut off 2 times"
    settlement = await settled(opened["harness"], id)
    assert {key: settlement.get(key) for key in ("status", "reason", "detail")} == {
        "status": "unanswered",
        "reason": "aborted",
        "detail": detail,
    }
    only_failure(await live(opened["harness"]), task_id, [id], "aborted", detail)
    record = await opened["harness"].wait_for_task(task_id, context)
    assert record["state"]["outcome"]["status"] == "faulted"
    assert record["state"]["outcome"]["error"]["detail"] == {"reason": "restart_limit"}

    async def reported() -> bool:
        return any(info.get("kind") == "task_faulted" for info in setup.infos)

    await eventually(reported)
    assert any(
        info.get("kind") == "task_faulted" and info.get("reason") == "restart_limit"
        for info in setup.infos
    )
    await opened["harness"].close(context)


async def test_reports_task_interrupted_for_a_run_that_ends_interrupted() -> None:
    """Upstream "reports task_interrupted for a run that ends interrupted and ... at the limit".

    The full upstream title is "reports task_interrupted for a run that ends interrupted and for a task
    that ends at the limit". A built-in task that ends itself because the host stopped is reported
    ``task_interrupted`` with its reason and its restart count, so a host keeps a crash-loop signal; the
    one report this host makes is that report, and its error carries the text the run ended with. (The
    ``restart_limit`` half of the case is asserted by the cut-off cases above, which check their own
    ``task_interrupted`` report.)
    """

    path = sqlite_path()
    first = streaming()
    first.settings["retry"] = {"enabled": False}
    _id, task_id = await crash_while_streaming(path, first)

    setup = chat_setup()
    setup.settings["retry"] = {"enabled": False}
    opened = await open(path, setup)
    opened["harness"].resume()
    await opened["harness"].wait_for_task(task_id, context)

    async def reported() -> bool:
        return len(setup.infos) > 0

    await eventually(reported)
    assert setup.infos == [
        {
            "kind": "task_interrupted",
            "task": {"id": task_id, "kind": "pi.generation", "conversationId": ROOT_CONVERSATION_ID},
            "reason": "interrupted",
            "restarts": 1,
        }
    ]
    assert "The host stopped while the model was answering, on attempt 1" in str(setup.reports[0])
    await opened["harness"].close(context)


class _CountingSettings(dict):
    """Host settings that count every resolution -- upstream's ``Proxy`` over ``extensions``.

    Empty, so every setting is its default; ``resolve_settings`` reads ``extensions`` once per
    resolution, which is what makes the count a count of resolutions.
    """

    def __init__(self) -> None:
        super().__init__()
        self.resolutions = 0

    def get(self, key: Any, default: Any = None) -> Any:
        if key == "extensions":
            self.resolutions += 1
        return super().get(key, default)


async def test_resolves_the_settings_no_more_often_than_before() -> None:
    """Upstream "resolves the settings no more often than before: only a task that was cut off reads it".

    The full upstream title is "resolves the settings no more often than before: only a task that was
    cut off reads the limit". The restart limit reaching every task kind must not cost the normal path a
    single extra resolution of the host's settings: an ordinary round, never cut off, resolves them no
    more often than before the limit applied to every kind.
    """

    setup = chat_setup()
    settings = _CountingSettings()
    setup.settings = settings
    add_tool(
        setup.registry,
        tool("t", lambda *_args: {}, description="t", parameters={"type": "object", "properties": {}}),
    )
    rounds = 5
    responses: list[Any] = []
    for round in range(rounds + 1):
        responses.append(calls(("t", {}, f"c{round}")))
        responses.append(assistant_message([faux_text("done")]))
    setup.faux.set_responses(responses)
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "warm up"}, context)).wait(context)
    settings.resolutions = 0
    for round in range(rounds):
        await (await root.submit({"type": "input", "content": f"round {round}"}, context)).wait(context)
    # One generation round with one tool call, no more often than before the restart limit applied to
    # every kind (#11327: 49; group commit coalesces some reconcile passes).
    assert settings.resolutions / rounds <= 49
    await harness.close(context)

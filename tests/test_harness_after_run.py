"""Translated upstream ``harness-after-run`` cases: a run that answered waits for its run-end tasks.

Upstream ``test/harness-after-run.test.ts`` holds 29 cases -- 27 under ``describe("afterRun")`` and two
unit cases under ``describe("endRun")`` -- and all 29 are translated here, as 30 tests: upstream's
"asks once a run really ends: not while onYield continues it, and when a queued steer drops the
continuation" is one case carrying two runs, so it is two tests.

A run whose answer's ``afterRun`` hooks ask for tasks keeps ``pi.live.run``, stays busy, and has already
settled its inputs ``done`` with the answer while they run (spec section 8.3): commit A is ``hold_run``,
the hooks are asked in ``finish``, the tasks are created as the generation's children in the commit that
moves to ``drain``, and commit B -- ``end_answered_run`` -- places the final boundary and ends the run
without a failure however those tasks ended.

Two readings of upstream that this file settles, because they are easy to get wrong from the outside:
a hook's own assertion is not evidence, since ``hooks.each`` reports a hook that throws and lets the run
continue -- what a hook read is therefore collected and asserted by the test (:656 upstream asserts
inside the hook); and the turn limit an ``afterRun`` case leans on is the agent's ``maxTurns``, set with
``configure()``, never a host setting (:1045, :858).
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest
from chat_support import (
    all_entries,
    assistant_message,
    chat_setup,
    close_killed,
    faux_tool_call,
    open_chat,
    open_killable,
    slow_message,
    text_of,
)
from session_support import context
from task_support import aborted, completed, deferred, eventually, settled
from tool_support import add_hooks, add_tool, tool

import pi_durable
from pi_durable import (
    CompactionTask,
    ConversationBusy,
    GenerationTask,
    InboxDoc,
    LiveDoc,
    MemoryStorage,
    RunEndTask,
    RunSummary,
    define_entry,
    define_extension,
    define_task,
    hook,
    run_end_task,
)
from pi_durable.harness import types as types_module
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage

TestsEntry: Any = define_entry("test.tests")

_directories: list[Path] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Any:
    yield
    await close_killed()
    while _directories:
        shutil.rmtree(_directories.pop(), ignore_errors=True)


def _sqlite_path() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-after-run-"))
    _directories.append(directory)
    return directory / "session.sqlite"



def probe() -> dict[str, Any]:
    """What the translated cases watch: what ran, what the hooks were asked with, what aborted."""

    return {"runs": [], "asked": [], "aborted": 0, "gate": None, "hook_gate": None, "admit": None}


def after_run_task(state: dict[str, Any]) -> Any:
    """A task that records its label, waits for ``gate`` while one is set, then completes.

    ``admit`` runs inside the task's own commit, before its entry, as upstream's probe's ``admit``
    does: a test uses it to place work for the run through the conversation handle.
    """

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        state["runs"].append(task["input"]["label"])
        if state["gate"] is not None:
            await asyncio.wait_for(asyncio.shield(_race(state["gate"], runtime.signal)), timeout=5)
        handle = (
            None if state["admit"] is None else await runtime.conversation(runtime.conversation_id, ctx)
        )

        async def finish(tx: Any) -> Any:
            if state["admit"] is not None:
                await state["admit"](handle, tx)
            await tx.append_entry(
                runtime.conversation_id, {"kind": "test.tests", "data": task["input"]["label"]}
            )
            return completed(task["input"]["label"])

        await runtime.commit(finish, ctx)

    async def stop(_task: Any, runtime: Any, ctx: Any) -> None:
        state["aborted"] += 1
        await runtime.commit(lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, ctx)

    return define_task(
        {
            "name": "test.tests",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": stop,
        }
    )


async def _race(gate: Any, signal: Any) -> None:
    """Upstream's ``Promise.race([gate.promise, aborted(signal)])``."""

    waiting = asyncio.ensure_future(gate.promise)
    stopping = asyncio.ensure_future(aborted(signal))
    try:
        await asyncio.wait({waiting, stopping}, return_when=asyncio.FIRST_COMPLETED)
        if waiting.done() and not waiting.cancelled():
            waiting.result()
        elif stopping.done() and not stopping.cancelled():
            stopping.result()
    finally:
        waiting.cancel()
        stopping.cancel()
        for pending in (waiting, stopping):
            if pending.done() and not pending.cancelled():
                pending.exception()


def install(setup: Any, state: dict[str, Any], after_run: Any = None) -> Any:
    """Install the after-run extension: its task and an ``afterRun`` hook asking for one task a run."""

    Tests = after_run_task(state)
    if after_run is None:

        async def after_run(run: Any, _runtime: Any, _context: Any) -> dict[str, Any]:
            state["asked"].append(dict(run))
            if state["hook_gate"] is not None:
                await state["hook_gate"].promise
            return {"tasks": [run_end_task(Tests, {"label": f"run {len(state['asked'])}"})]}

    setup.registry.install(
        define_extension(
            {
                "name": "after-run",
                "tasks": [Tests],
                "hooks": [hook(GenerationTask, {"afterRun": after_run})],
            }
        )
    )
    return Tests


def editing_tools() -> list[Any]:
    """``edit``, ``finish`` (terminates), and ``handoff`` (terminates in a new context)."""

    return [
        tool("edit", lambda *_args: {"output": [{"type": "text", "text": "edited"}]}),
        tool(
            "finish",
            lambda *_args: {
                "output": [{"type": "text", "text": "finished"}],
                "control": {"terminate": True},
            },
        ),
        tool(
            "handoff",
            lambda *_args: {
                "output": [{"type": "text", "text": "handed off"}],
                "control": {"terminate": True, "handoff": "fresh start"},
            },
        ),
    ]


def calling(name: str, id: str) -> dict[str, Any]:
    return assistant_message([faux_tool_call(name, {}, id=id)], stop_reason="toolUse")


def kinds(entries: list[Any], kind: str) -> list[Any]:
    return [entry for entry in entries if entry["kind"] == kind]


def user_texts(entries: list[Any]) -> list[str]:
    """Texts of the user messages of the conversation's entries, in order -- upstream's ``userTexts``."""

    return [
        text_of(message) or ""
        for entry in entries
        for message in (entry.get("model") or [])
        if message["role"] == "user"
    ]


async def live(harness: Any, conversation_id: Any) -> dict[str, Any]:
    value = await harness.snapshot(LiveDoc, conversation_id, context)
    return {} if value is None else dict(value)


async def test_waits_for_the_tasks_an_answers_hooks_ask_for() -> None:
    """Upstream ``harness-after-run.test.ts``:181, the answer-resolves-while-busy rule."""

    setup = chat_setup()
    state = probe()
    state["gate"] = deferred()
    install(setup, state)
    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.faux.set_responses([calling("edit", "a"), assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "go"}, context)
    # The input settles with the answer, before the task finishes.
    assert (await submission.wait(context))["status"] == "done"
    await eventually(lambda: len(state["runs"]) == 1)
    held = await live(harness, root.id)
    assert held["run"]["inputs"] == []
    assert len(held["run"]["afterRun"]["tasks"]) == 1
    child = await harness.get_task(held["run"]["afterRun"]["tasks"][0], context)
    assert child is not None and child["owner"] == held["run"]["taskId"]
    assert child["background"] is False
    idle = asyncio.ensure_future(root.wait_for_idle(context))
    assert await settled(idle) is False

    entries = await all_entries(root)
    answer = kinds(entries, "pi.assistant")[-1]
    assert state["asked"] == [
        {
            "reason": "answer",
            "first": kinds(entries, "pi.user")[0]["id"],
            "answer": answer["id"],
            "last": answer["id"],
        }
    ]
    state["gate"].resolve()
    await idle
    state_now = await live(harness, root.id)
    assert "run" not in state_now
    assert "runFailure" not in state_now
    assert (await all_entries(root))[-1]["kind"] == "test.tests"
    generation = await harness.get_task(held["run"]["taskId"], context)
    assert generation is not None
    assert generation["state"]["status"] == "terminal"
    assert generation["state"]["outcome"] == {
        "status": "completed",
        "result": {"entryId": answer["id"]},
    }
    await harness.close(context)


async def test_ends_the_run_in_one_commit_without_an_after_run_hook() -> None:
    """Upstream ``harness-after-run.test.ts``:251."""

    plain = chat_setup()
    state = probe()
    plain.registry.install(define_extension({"name": "tasks-only", "tasks": [after_run_task(state)]}))
    plain.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), plain)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    # No hook: the answer's commit ended the run.
    assert await live(opened["harness"], opened["root"].id) == {}
    await opened["harness"].close(context)

    setup = chat_setup()
    asked_state = probe()

    async def asks_nothing(run: Any, _runtime: Any, _context: Any) -> None:
        asked_state["asked"].append(dict(run))
        return None

    install(setup, asked_state, asks_nothing)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    assert len(asked_state["asked"]) == 1
    assert asked_state["runs"] == []
    assert await live(harness, root.id) == {}
    await harness.close(context)


async def test_queues_input_during_the_tasks_and_places_it_after_them() -> None:
    """Upstream ``harness-after-run.test.ts``:278: writes first, then steers and follow-ups.

    The run is busy while its run-end tasks run, so a steer queues instead of being refused, a write
    lands at once, and the items placed during the tasks are ordered after the tasks' own entry and
    the entry the task admitted.
    """

    setup = chat_setup()
    state = probe()
    state["gate"] = deferred()
    Tests = after_run_task(state)

    async def after_run(run: Any, _runtime: Any, _context: Any) -> dict[str, Any] | None:
        state["asked"].append(dict(run))
        # Only the first run gets a task.
        if len(state["asked"]) != 1:
            return None
        return {"tasks": [run_end_task(Tests, {"label": "run 1"})]}

    setup.registry.install(
        define_extension(
            {
                "name": "after-run",
                "tasks": [Tests],
                "hooks": [hook(GenerationTask, {"afterRun": after_run})],
            }
        )
    )

    # The task admits its result for the model in its own commit.
    async def admit(handle: Any, tx: Any) -> None:
        await handle.submit(
            tx,
            {
                "type": "write",
                "entry": {
                    "kind": "test.result",
                    "model": [{"role": "user", "content": "tests passed", "timestamp": 1}],
                },
            },
        )

    state["admit"] = admit
    setup.faux.set_responses(
        [assistant_message("first"), assistant_message("second"), assistant_message("third")]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["runs"]) == 1)

    steer = await root.submit({"type": "input", "content": "steer", "whenBusy": "steer"}, context)
    follow_up = await root.submit({"type": "input", "content": "later"}, context)
    with pytest.raises(ConversationBusy):
        await root.submit({"type": "input", "content": "x", "whenBusy": "reject"}, context)
    with pytest.raises(ConversationBusy):
        await root.submit({"type": "continue"}, context)
    # Busy, so not idle: it queues and starts the next run after the tasks.
    idle_steer = await root.submit(
        {"type": "input", "content": "steer 2", "whenBusy": "steer", "whenIdle": "reject"}, context
    )
    note = await root.submit({"type": "write", "entry": {"kind": "test.note"}}, context)
    assert (await note.status(context))["status"] == "done"
    assert (await steer.status(context))["status"] == "queued"
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert len(inbox["items"]) == 3

    state["gate"].resolve()
    await root.wait_for_idle(context)
    for submission in (steer, follow_up, idle_steer):
        assert (await submission.wait(context))["status"] == "done"
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries if entry["kind"] != "pi.system"] == [
        "pi.user",
        "pi.assistant",
        "test.note",
        "test.tests",
        "test.result",
        "pi.user",
        "pi.user",
        "pi.assistant",
        "pi.user",
        "pi.assistant",
    ]
    assert user_texts(entries) == ["go", "tests passed", "steer", "later", "steer 2"]
    await harness.close(context)


async def test_places_a_reset_queued_during_the_tasks_after_them() -> None:
    """Upstream ``harness-after-run.test.ts``:353."""

    setup = chat_setup()
    state = probe()
    state["gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["runs"]) == 1)
    await root.reset(None, context)
    assert kinds(await all_entries(root), "pi.reset") == []
    state["gate"].resolve()
    await root.wait_for_idle(context)
    entry_kinds = [entry["kind"] for entry in await all_entries(root)]
    assert entry_kinds[-2:] == ["test.tests", "pi.reset"]
    await harness.close(context)


async def test_asks_once_a_run_really_ends_not_while_on_yield_continues_it() -> None:
    """Upstream ``harness-after-run.test.ts``:372."""

    setup = chat_setup()
    state = probe()
    install(setup, state)
    yields = {"count": 0}

    async def on_yield(*_args: Any) -> dict[str, str] | None:
        yields["count"] += 1
        return {"continue": "again"} if yields["count"] <= 2 else None

    setup.registry.install(
        define_extension(
            {"name": "again", "hooks": [hook(GenerationTask, {"onYield": on_yield})]}
        )
    )
    setup.faux.set_responses(
        [assistant_message("1"), assistant_message("2"), assistant_message("3")]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    entries = await all_entries(root)
    # The two continuations were the same run: the hooks were asked once, for its last answer.
    assert len(state["asked"]) == 1
    assert state["asked"][0]["reason"] == "answer"
    assert state["asked"][0]["answer"] == kinds(entries, "pi.assistant")[-1]["id"]
    assert state["asked"][0]["first"] == kinds(entries, "pi.user")[0]["id"]
    assert len(state["runs"]) == 1
    await harness.close(context)


async def test_drops_the_continuation_a_queued_steer_outranks_and_asks_after_the_task() -> None:
    """Upstream ``harness-after-run.test.ts``:372, the second half.

    A steer queued while the answer's hooks run: the continuation is dropped, and the run ends after
    its task.
    """

    setup = chat_setup()
    state = probe()
    install(setup, state)
    root_box: dict[str, Any] = {}

    async def on_yield(*_args: Any) -> dict[str, str] | None:
        if state["asked"]:
            return None
        await root_box["root"].submit(
            {"type": "input", "content": "steer", "whenBusy": "steer"}, context
        )
        return {"continue": "again"}

    setup.registry.install(
        define_extension({"name": "steering", "hooks": [hook(GenerationTask, {"onYield": on_yield})]})
    )
    setup.faux.set_responses([assistant_message("1"), assistant_message("2")])
    opened = await open_chat(MemoryStorage(), setup)
    root_box["root"] = opened["root"]
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    # The continuation lost to the steer: the run ended after its task, and the steer answered in the
    # run that followed it.
    assert len(state["asked"]) == 2
    assert [asked["reason"] for asked in state["asked"]] == ["answer", "answer"]
    assert state["runs"] == ["run 1", "run 2"]
    texts = [
        message["content"]
        for entry in await all_entries(root)
        for message in entry.get("model") or []
        if message["role"] == "user"
    ]
    assert texts == ["go", "steer"]
    assert await live(harness, root.id) == {}
    await harness.close(context)


async def test_asks_the_hooks_again_after_a_crash_before_the_tasks_exist_and_never_once_they_do() -> None:
    """Upstream ``harness-after-run.test.ts``:429.

    A crash while the hook runs, in ``finish``: the reopened host asks the hooks again and creates the
    task. A crash while the task runs, in ``drain``: the task runs again under the ``afterRun`` tasks
    the first host already recorded, and the hooks are not asked a third time. An orderly close in
    ``drain`` resumes the same way.
    """

    path = _sqlite_path()
    setup = chat_setup()
    state = probe()
    state["hook_gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["asked"]) == 1)
    opened["kill"]()  # A crash while the hook runs, in `finish`.
    state["hook_gate"] = None
    state["gate"] = deferred()

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    opened["harness"].resume()
    await eventually(lambda: len(state["runs"]) == 1)
    assert len(state["asked"]) == 2
    opened["kill"]()  # A crash while the task runs, in `drain`.
    state["gate"] = None

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await opened["root"].wait_for_idle(context)
    # The task ran again; the hook was not asked again, and only one task exists.
    assert len(state["asked"]) == 2
    assert state["runs"] == ["run 2", "run 2"]
    assert len(kinds(await all_entries(opened["root"]), "test.tests")) == 1
    assert (await opened["harness"].inspect(context))["tasks"] == []
    await opened["harness"].close(context)

    # An orderly close in `drain` resumes the same way.
    closing = _sqlite_path()
    other = probe()
    other["gate"] = deferred()
    setup2 = chat_setup()
    install(setup2, other)
    setup2.faux.set_responses([assistant_message("done")])
    reopened = await open_killable(open_posix_sqlite_storage, closing, setup2)
    await (await reopened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(other["runs"]) == 1)
    await reopened["harness"].close(context)
    other["gate"] = None

    reopened = await open_killable(open_posix_sqlite_storage, closing, setup2)
    await reopened["root"].wait_for_idle(context)
    assert len(other["asked"]) == 1
    assert other["runs"] == ["run 1", "run 1"]
    await reopened["harness"].close(context)


async def _stop_during(stage: str) -> None:
    setup = chat_setup()
    state = probe()
    Tests = after_run_task(state)
    state["gate"] = deferred()

    async def after_run(run: Any, _runtime: Any, ctx: Any) -> dict[str, Any]:
        state["asked"].append(dict(run))
        if stage == "finish":
            await aborted(ctx.abort_signal)
        return {"tasks": [run_end_task(Tests, {"label": "run 1"})]}

    setup.registry.install(
        define_extension(
            {
                "name": "after-run",
                "tasks": [Tests],
                "hooks": [hook(GenerationTask, {"afterRun": after_run})],
            }
        )
    )
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await submission.wait(context)
    if stage == "finish":
        await eventually(lambda: len(state["asked"]) == 1)
    else:
        await eventually(lambda: len(state["runs"]) == 1)
    await root.abort(context)
    # The run answered: Stop ends it without a failure, however it ends.
    state_now = await live(harness, root.id)
    assert "run" not in state_now
    assert "runFailure" not in state_now
    assert (await submission.status(context))["status"] == "done"
    assert state["aborted"] == (1 if stage == "drain" else 0)
    assert state["runs"] == (["run 1"] if stage == "drain" else [])
    if stage == "drain":
        state["gate"].resolve()
    await harness.close(context)


async def test_ends_an_answered_run_without_a_failure_when_stop_comes() -> None:
    """Upstream ``harness-after-run.test.ts``:478: Stop during the hooks or during the tasks."""

    for stage in ("finish", "drain"):
        await _stop_during(stage)


async def test_ends_the_run_normally_when_one_of_its_tasks_is_aborted_on_its_own() -> None:
    """Upstream ``harness-after-run.test.ts``:516."""

    setup = chat_setup()
    state = probe()
    state["gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["runs"]) == 1)
    held = await live(harness, root.id)
    await harness.abort_task(held["run"]["afterRun"]["tasks"][0], context)
    state["gate"].resolve()
    await root.wait_for_idle(context)
    assert state["aborted"] == 1
    assert await live(harness, root.id) == {}
    generation = await harness.get_task(held["run"]["taskId"], context)
    assert generation is not None
    assert generation["state"]["status"] == "terminal"
    assert generation["state"]["outcome"]["status"] == "completed"
    await harness.close(context)


async def test_reports_broken_requests_and_hooks_and_creates_the_rest() -> None:
    """Upstream ``harness-after-run.test.ts``:538: a broken request is skipped, the rest are created."""

    setup = chat_setup()
    state = probe()
    Tests = after_run_task(state)
    Throwing = define_task(
        {
            "name": "test.throwing",
            "version": 1,
            "initial": _throwing_initial,
            "phases": {"run": lambda *_args: None},
            "abort": lambda *_args: None,
        }
    )
    BadStart = define_task(
        {
            "name": "test.bad-start",
            "version": 1,
            "initial": lambda _input: {"phase": "nowhere"},
            "phases": {"run": lambda *_args: None},
            "abort": lambda *_args: None,
        }
    )
    Absent = define_task(
        {
            "name": "test.unknown",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": lambda *_args: None},
            "abort": lambda *_args: None,
        }
    )
    requests = [
        run_end_task(Absent, None),
        run_end_task(cast(Any, GenerationTask), {}),
        run_end_task(Tests, {"label": lambda: "not JSON"}),
        run_end_task(Throwing, None),
        run_end_task(BadStart, None),
        run_end_task(Tests, {"label": "ok"}),
    ]

    async def asks(run: Any, _runtime: Any, _context: Any) -> dict[str, Any]:
        state["asked"].append(dict(run))
        return {"tasks": requests}

    async def broken(run: Any, _runtime: Any, _context: Any) -> None:
        raise RuntimeError("hook failed")

    async def not_a_list(run: Any, _runtime: Any, _context: Any) -> dict[str, Any]:
        return {"tasks": "x"}

    setup.registry.install(
        define_extension(
            {
                "name": "after-run",
                "tasks": [Tests, Throwing, BadStart],
                "hooks": [hook(GenerationTask, {"afterRun": asks})],
            }
        )
    )
    setup.registry.install(
        define_extension({"name": "broken", "hooks": [hook(GenerationTask, {"afterRun": broken})]})
    )
    setup.registry.install(
        define_extension(
            {"name": "not-a-list", "hooks": [hook(GenerationTask, {"afterRun": not_a_list})]}
        )
    )
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    # The request that passed every check ran; the broken ones and the broken hooks were reported.
    assert state["runs"] == ["ok"]
    rejected = setup.reports[0]
    assert isinstance(rejected, ExceptionGroup)
    assert [str(error) for error in rejected.exceptions] == [
        "Task test.unknown is not installed",
        "An afterRun hook cannot start the built-in task pi.generation",
        "Value contains a non-JSON function; expected strict JSON",
        "initial failed",
        "Task test.bad-start starts at the unknown phase nowhere",
    ]
    assert any(str(report) == "hook failed" for report in setup.reports)
    stopped = [r for r in setup.reports if isinstance(r, (TypeError, AttributeError, KeyError))]
    assert stopped, setup.reports
    assert await live(harness, root.id) == {}
    await harness.close(context)


def _throwing_initial(_input: Any) -> dict[str, Any]:
    raise RuntimeError("initial failed")


async def test_creates_a_requested_task_from_the_installed_definition_calling_initial_once() -> None:
    """Upstream ``harness-after-run.test.ts``:617: ``initial()`` runs once, off the line."""

    setup = chat_setup()
    state = probe()
    calls = {"initial": 0}

    def initial(input: Any) -> dict[str, Any]:
        calls["initial"] += 1
        return {"phase": "run", "seen": input["label"]}

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        state["runs"].append([task["input"]["label"], task_checkpoint_value(task)["seen"]])

        async def finish(tx: Any) -> Any:
            await tx.append_entry(
                runtime.conversation_id, {"kind": "test.tests", "data": task["input"]["label"]}
            )
            return completed(task["input"]["label"])

        await runtime.commit(finish, ctx)

    Installed = define_task(
        {
            "name": "test.installed",
            "version": 1,
            "initial": initial,
            "phases": {"run": run},
            "abort": lambda *_args: None,
        }
    )

    async def after_run(run: Any, runtime: Any, _context: Any) -> dict[str, Any]:
        state["asked"].append(dict(run))
        installed = runtime.registry.task("test.installed")
        assert installed is not None
        return {"tasks": [run_end_task(installed, {"label": "from the hook"})]}

    setup.registry.install(
        define_extension(
            {
                "name": "installed",
                "tasks": [Installed],
                "hooks": [hook(GenerationTask, {"afterRun": after_run})],
            }
        )
    )
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    assert calls["initial"] == 1
    assert state["runs"] == [["from the hook", "from the hook"]]
    await harness.close(context)


def task_checkpoint_value(task: Any) -> dict[str, Any]:
    return cast(dict[str, Any], task["state"]["checkpoint"])


async def test_lets_a_hook_read_the_runs_history_tool_results_included() -> None:
    """Upstream ``harness-after-run.test.ts``:656: the hook reads the run's own history off the runtime.

    The page is the ``[run.first, run.last]`` range the summary names, tool results included, and
    ``entry()`` finds the answer by its own id -- so the summary's bounds are the run's real entries,
    not a window that stopped short of the round that ended it.
    """

    setup = chat_setup()
    seen: list[str] = []
    fetched: list[Any] = []

    async def read_history(run: Any, runtime: Any, ctx: Any) -> None:
        page = await runtime.entries(
            {"minEntryId": run["first"], "maxEntryId": run["last"], "order": "ascending"},
            100,
            None,
            ctx,
        )
        seen.extend(entry["kind"] for entry in page["items"])
        # What the hook read, asserted by the test rather than in the hook: a hook that throws is
        # reported and the run goes on, so an assertion raised there would prove nothing.
        fetched.append(await runtime.entry(run["answer"], ctx))
        return None

    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.registry.install(
        define_extension({"name": "reader", "hooks": [hook(GenerationTask, {"afterRun": read_history})]})
    )
    setup.faux.set_responses([calling("edit", "a"), assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    assert [kind for kind in seen if kind != "pi.system"] == [
        "pi.user",
        "pi.assistant",
        "pi.tool-result",
        "pi.assistant",
    ]
    assert len(fetched) == 1
    assert fetched[0] is not None and fetched[0]["kind"] == "pi.assistant"
    await harness.close(context)


async def test_lets_a_hook_read_the_runs_history_after_a_compaction_summarized_the_start() -> None:
    """Upstream ``harness-after-run.test.ts``:694: the run's history is its own, not the active context's.

    A compaction asked for mid-round places its summary inside the run, so the active context has lost
    the run's input to it -- while ``[run.first, run.last]`` still pages the input, the round, the
    summary and the answer.
    """

    setup = chat_setup()
    setup.settings["compaction"] = {
        "enabled": False,
        "reserve_tokens": 1000,
        "keep_recent_tokens": 1,
        "background_tokens": 0,
    }
    gate = deferred()
    started = deferred()
    runs: list[list[str]] = []

    async def edit_slowly(*_args: Any) -> dict[str, Any]:
        started.resolve()
        await gate.promise
        return {"output": [{"type": "text", "text": "edited"}]}

    async def read_history(run: Any, runtime: Any, ctx: Any) -> None:
        page = await runtime.entries(
            {"minEntryId": run["first"], "maxEntryId": run["last"], "order": "ascending"},
            100,
            None,
            ctx,
        )
        runs.append([entry["kind"] for entry in page["items"]])
        return None

    add_tool(setup.registry, tool("edit", edit_slowly))
    add_hooks(setup.registry, CompactionTask, {"beforeCompact": lambda *_args: {"summary": "earlier work"}})
    setup.registry.install(
        define_extension({"name": "reader", "hooks": [hook(GenerationTask, {"afterRun": read_history})]})
    )
    setup.faux.set_responses([calling("edit", "a"), assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    # Its summary is placed at the round's post-tools boundary, in the middle of the run.
    compaction = await root.compact(None, context)
    await harness.wait_for_task(compaction, context)
    gate.resolve()
    await submission.wait(context)
    await root.wait_for_idle(context)
    active = [entry["kind"] for entry in (await root.context(context))["entries"]]
    # The active context lost the run's input to the summary; the run's history did not.
    assert "pi.user" not in active
    assert len(runs) == 1
    assert [kind for kind in runs[0] if kind != "pi.system"] == [
        "pi.user",
        "pi.assistant",
        "pi.tool-result",
        "pi.compaction",
        "pi.assistant",
    ]
    await harness.close(context)


async def test_asks_after_a_terminate_round_with_its_last_entry() -> None:
    """Upstream ``harness-after-run.test.ts``:221: terminate asks too, its last entry the newest result."""

    setup = chat_setup()
    state = probe()
    install(setup, state)
    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.faux.set_responses([calling("finish", "a")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    entries = await all_entries(root)
    results = kinds(entries, "pi.tool-result")
    assert state["asked"] == [
        {
            "reason": "terminate",
            "first": kinds(entries, "pi.user")[0]["id"],
            "answer": kinds(entries, "pi.assistant")[-1]["id"],
            "last": results[-1]["id"],
        }
    ]
    assert state["runs"] == ["run 1"]
    assert await live(harness, root.id) == {}
    await harness.close(context)


async def test_asks_after_a_terminate_round_at_the_turn_limit_terminate_wins() -> None:
    """Upstream ``harness-after-run.test.ts``:1045.

    The bound is the agent's ``maxTurns``, set with ``configure`` as upstream sets it -- it is not a
    host setting, and a run that never reaches the limit would ask the hooks for any reason.
    """

    setup = chat_setup()
    state = probe()
    install(setup, state)
    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.faux.set_responses([calling("finish", "a")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"max_turns": 1}, context)
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    # The round asked to terminate: the run ends answered with its after-run task, not ``turn_limit``.
    assert [run["reason"] for run in state["asked"]] == ["terminate"]
    assert state["runs"] == ["run 1"]
    assert await live(harness, root.id) == {}
    await harness.close(context)


async def test_leaves_a_run_it_does_not_own_alone_and_ends_an_answered_run_without_a_failure() -> None:
    """Upstream ``harness-after-run.test.ts``:1188, the unit case on ``hold_run``."""

    from pi_durable.harness.generation import hold_run

    # A run owned by another generation is left alone: nothing is settled and nothing is held.
    live_state: dict[str, Any] = {"run": {"taskId": 99, "inputs": [1], "turns": 1}}
    calls: list[Any] = []
    tx = cast(Any, type("Tx", (), {"settle_submission": lambda _self, id, value: calls.append(id)})())
    held_none = hold_run(
        tx, live_state, cast(Any, SimpleTaskId(7)), {"reason": "answer", "answer": 5, "last": 5}
    )
    assert held_none is None
    assert calls == []
    assert live_state["run"]["inputs"] == [1]

    # The run it owns keeps ``pi.live.run`` with its empty input list and no failure.
    owned: dict[str, Any] = {"run": {"taskId": 7, "inputs": [3], "turns": 1}, "generation": {"attempt": 1}}
    held = hold_run(
        tx, owned, cast(Any, SimpleTaskId(7)), {"reason": "answer", "answer": 5, "last": 5}
    )
    assert held is not None
    assert held["checkpoint"]["phase"] == "finish"
    assert held["checkpoint"]["run"] == {"reason": "answer", "first": 5, "answer": 5, "last": 5}
    assert owned["run"]["inputs"] == []
    assert owned["run"]["afterRun"] == {}
    assert "generation" not in owned
    assert "runFailure" not in owned
    assert calls == [3]


class SimpleTaskId:
    """The generation's own identity, as ``hold_run`` reads it off the runtime."""

    def __init__(self, task_id: Any) -> None:
        self.task_id = task_id
        self.taskId = task_id


async def test_copies_a_failed_run_first_entry_into_run_failure() -> None:
    """Upstream ``harness-after-run.test.ts``:1200, the unit case on ``end_run``."""

    from pi_durable.harness.live import end_run

    live_state: dict[str, Any] = {
        "run": {"taskId": 7, "inputs": [3], "turns": 1, "first": 4},
        "generation": {"attempt": 1},
    }
    settled: list[Any] = []
    tx = cast(
        Any,
        type("Tx", (), {"settle_submission": lambda _self, id, value: settled.append((id, value))})(),
    )
    end_run(
        cast(Any, tx),
        cast(Any, live_state),
        cast(Any, 7),
        {"status": "unanswered", "reason": "model_error", "detail": "boom"},
    )
    assert live_state["runFailure"] == {
        "taskId": 7,
        "inputs": [3],
        "reason": "model_error",
        "detail": "boom",
        "first": 4,
    }
    assert settled == [(3, {"status": "unanswered", "reason": "model_error", "detail": "boom"})]
    assert "run" not in live_state
    assert "generation" not in live_state


async def test_ends_an_answered_run_completed_without_a_failure_once_finish_is_cut_off() -> None:
    """Upstream ``harness-after-run.test.ts``: "ends an answered run completed, without a failure, once
    finish was cut off restartLimit times".

    The answer is already committed, so the run ends ``completed`` with no failure and no run, however
    its ``finish`` phase was cut off; the queued item stays, as after a failed run, and the scheduler
    still reports ``task_interrupted`` for a generation that crashes ended.
    """

    path = _sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    state = probe()
    state["hook_gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["asked"]) == 1)
    await opened["root"].submit({"type": "input", "content": "later"}, context)
    opened["kill"]()

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await opened["root"].wait_for_idle(context)
    assert len(state["asked"]) == 1
    state_now = await live(opened["harness"], opened["root"].id)
    assert "run" not in state_now
    assert "runFailure" not in state_now
    inbox = await opened["harness"].snapshot(InboxDoc, opened["root"].id, context)
    assert len(inbox["items"]) == 1
    assert any(
        info.get("kind") == "task_interrupted" and info.get("reason") == "restart_limit"
        for info in setup.infos
    )
    await opened["harness"].close(context)


async def test_keeps_the_run_busy_while_its_after_run_task_cannot_run_until_stop_orphans_it() -> None:
    """Upstream ``harness-after-run.test.ts``:785: a run-end task that cannot run keeps the run busy.

    The host dies while the task runs and reopens without the extension that defines it, so the task
    is blocked: ``run.afterRun`` still names it, idle does not settle, and Stop ends the run cleanly and
    orphans the task it could not run.
    """

    path = _sqlite_path()
    setup = chat_setup()
    state = probe()
    state["gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["runs"]) == 1)
    opened["kill"]()

    # Reopened without the extension: its task is blocked.
    opened = await open_killable(open_posix_sqlite_storage, path, chat_setup())
    opened["harness"].resume()
    held = await live(opened["harness"], opened["root"].id)
    assert len(held["run"]["afterRun"]["tasks"]) == 1
    child = held["run"]["afterRun"]["tasks"][0]
    async def blocked() -> bool:
        inspection = await opened["harness"].inspect(context)
        return any(task["state"]["kind"] == "blocked" for task in inspection["tasks"])

    await eventually(blocked)
    assert await settled(opened["root"].wait_for_idle(context)) is False
    await opened["root"].abort(context)
    after = await live(opened["harness"], opened["root"].id)
    assert "run" not in after
    assert "runFailure" not in after
    record = await opened["harness"].get_task(child, context)
    assert record is not None
    assert record["state"]["status"] == "terminal"
    assert record["state"]["outcome"]["status"] == "orphaned"
    await opened["harness"].close(context)


async def test_ends_the_run_normally_when_its_after_run_task_is_cut_off_restart_limit_times() -> None:
    """Upstream "ends the run normally when its after-run task is cut off restartLimit times".

    The after-run task is cut off and the generation waiting for it is not: the scheduler faults the
    task, the drain ends the run as it would for any terminal child, and no failure is recorded.
    """

    path = _sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    state = probe()
    state["gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["runs"]) == 1)
    generation = (await live(opened["harness"], opened["root"].id))["run"]["taskId"]
    opened["kill"]()
    state["gate"] = None

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await opened["root"].wait_for_idle(context)
    record = await opened["harness"].get_task(generation, context)
    assert record["state"]["status"] == "terminal"
    assert record["state"]["outcome"]["status"] == "completed"
    assert "runFailure" not in await live(opened["harness"], opened["root"].id)
    assert state["runs"] == ["run 1"]
    assert any(
        info.get("kind") == "task_faulted" and info.get("reason") == "restart_limit"
        for info in setup.infos
    )
    await opened["harness"].close(context)


async def test_reports_no_task_interrupted_for_a_task_ended_normally_after_a_lowered_limit() -> None:
    """Upstream "reports no task_interrupted for a task that ends normally after the limit was lowered".

    The rerun had started under the old limit and ends by its own ``completed``, so nothing crashes
    ended it: ``runtime.commit`` reads the state it is about to write and finds no report there. The
    lowered limit is what makes the read matter -- the record carries one restart, and a host that
    stopped mid-phase is the case a report is owed for.
    """

    path = _sqlite_path()
    setup = chat_setup()
    state = {"started": 0}

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        state["started"] += 1
        if state["started"] == 1:
            await asyncio.Event().wait()  # the run a crash cuts off
        # The host lowers the limit while the rerun runs.
        setup.settings["restart_limit"] = 1
        await runtime.commit(lambda: completed(None), ctx)

    Worker = define_task(
        {
            "name": "test.worker",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda *_args: None,
        }
    )
    setup.registry.install(define_extension({"name": "worker", "tasks": [Worker]}))
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    id = await opened["root"].commit(
        lambda tx: tx.create_task(Worker, None, {"ownership": {"kind": "conversation"}}), context
    )
    opened["harness"].resume()
    await eventually(lambda: state["started"] == 1)
    opened["kill"]()

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    record = await opened["harness"].wait_for_task(id, context)
    assert record["state"]["outcome"]["status"] == "completed"
    setup.settings["restart_limit"] = None
    # ``close()`` joins the invocations, and the report a commit owes rides the invocation that made
    # it: it is delivered after its terminal record publishes, so a host that asked right after
    # ``wait_for_task`` could read an empty sink. Asking once the invocations are joined is what makes
    # "nothing was reported" a statement about the run rather than about how fast the loop ran.
    await opened["harness"].close(context)
    assert [info for info in setup.infos if info.get("kind") == "task_interrupted"] == []


async def test_reports_no_task_interrupted_for_a_generation_ended_normally_after_a_lowered_limit() -> None:
    """Upstream "reports no task_interrupted for a generation that ends normally after the limit was
    lowered in finish".

    The phase had started under the old limit and the hooks finish their work, so the run ends as it
    always does: lowering the limit while ``finish`` runs owes no report for crashes.
    """

    path = _sqlite_path()
    setup = chat_setup()
    state = probe()
    state["hook_gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["asked"]) == 1)
    opened["kill"]()  # `finish` cut off once.
    rerun = deferred()
    state["hook_gate"] = rerun

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    opened["harness"].resume()
    await eventually(lambda: len(state["asked"]) == 2)
    # The host lowers the limit while the hook runs; the phase ends normally.
    setup.settings["restart_limit"] = 1
    rerun.resolve()
    await opened["root"].wait_for_idle(context)
    assert state["runs"] == ["run 2"]
    assert [info for info in setup.infos if info.get("kind") == "task_interrupted"] == []
    await opened["harness"].close(context)


async def test_starts_a_fix_run_with_a_follow_up_a_task_admits_after_the_tasks() -> None:
    """Upstream ``harness-after-run.test.ts``:818: a task admits a follow-up that starts its own run.

    The follow-up is submitted from inside the task's own commit, so it queues behind the answer it is
    a follow-up to; the boundary places it only when the run ends, and the run it starts takes its own
    input as ``first`` -- not the run that asked for the fix.
    """

    setup = chat_setup()
    state = probe()
    Tests = after_run_task(state)

    async def after_run(run: Any, _runtime: Any, _context: Any) -> dict[str, Any] | None:
        state["asked"].append(dict(run))
        return None if len(state["asked"]) != 1 else {"tasks": [run_end_task(Tests, {"label": "run 1"})]}

    setup.registry.install(
        define_extension(
            {
                "name": "after-run",
                "tasks": [Tests],
                "hooks": [hook(GenerationTask, {"afterRun": after_run})],
            }
        )
    )

    async def admit(handle: Any, tx: Any) -> None:
        await handle.submit(
            tx,
            {
                "type": "input",
                "content": "fix the tests",
                "requestId": "fix-1",
                "whenBusy": "followUp",
            },
        )

    state["admit"] = admit
    setup.faux.set_responses([assistant_message("done"), assistant_message("fixed")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries if entry["kind"] != "pi.system"] == [
        "pi.user",
        "pi.assistant",
        "test.tests",
        "pi.user",
        "pi.assistant",
    ]
    assert user_texts(entries) == ["go", "fix the tests"]
    assert len(state["asked"]) == 2
    # The fix run's history starts with its own input.
    assert state["asked"][1]["first"] == kinds(entries, "pi.user")[1]["id"]
    await harness.close(context)


async def test_gives_a_continue_after_a_failed_run_the_failed_runs_history() -> None:
    """Upstream ``harness-after-run.test.ts``:858: a continue after a failed run carries that run's history.

    The turn limit ends the first run with ``turn_limit`` and its ``first``; no hook is asked, because
    the run never answered. The continue then answers and its summary still starts at the failed run's
    input, so a hook reads what that run did.
    """

    setup = chat_setup()
    state = probe()
    install(setup, state)
    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.faux.set_responses([calling("edit", "a"), assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await root.configure({"max_turns": 1}, context)
    failed = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert failed["status"] == "unanswered"
    user = kinds(await all_entries(root), "pi.user")[0]
    failure = (await live(harness, root.id)).get("runFailure")
    assert failure is not None
    assert failure["reason"] == "turn_limit" and failure["first"] == user["id"]
    assert state["asked"] == []
    await harness.commit(lambda tx: root.submit(tx, {"type": "continue"}), context)
    await root.wait_for_idle(context)
    answer = kinds(await all_entries(root), "pi.assistant")[-1]
    assert state["asked"] == [
        {"reason": "answer", "first": user["id"], "answer": answer["id"], "last": answer["id"]}
    ]
    await harness.close(context)


async def test_ends_the_run_without_asking_when_the_hooks_are_deselected_before_finish() -> None:
    """Upstream ``harness-after-run.test.ts``:882: the hooks are asked by the agent that selects them.

    The first host reaches ``finish`` and dies with the hook in hand. The hooks are then uninstalled,
    so the reopened host -- whose agent no longer selects an ``afterRun`` hook -- ends the answered run
    as it would for a run that never had one: the hook is not asked again, no task runs, and the live
    document is empty.
    """

    path = _sqlite_path()
    setup = chat_setup()
    state = probe()
    state["hook_gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["asked"]) == 1)
    opened["kill"]()
    setup.registry.uninstall(define_extension({"name": "after-run"}))

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await opened["root"].wait_for_idle(context)
    assert len(state["asked"]) == 1
    assert state["runs"] == []
    assert await live(opened["harness"], opened["root"].id) == {}
    await opened["harness"].close(context)


async def test_starts_a_fork_of_a_conversation_running_its_after_run_tasks_idle() -> None:
    """Upstream ``harness-after-run.test.ts``:903: a fork of a busy conversation starts without its run.

    The source is running its run-end tasks and holds a queued input. Its fork, taken at the answer and
    left ownerless, has no run of its own and nothing queued: the run and its inbox belong to the
    conversation that started them.
    """

    setup = chat_setup()
    state = probe()
    state["gate"] = deferred()
    install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["runs"]) == 1)
    await root.submit({"type": "input", "content": "later"}, context)
    answer = kinds(await all_entries(root), "pi.assistant")[-1]
    fork = await root.fork(answer["id"], {"ownership": {"kind": "ownerless"}}, context)
    assert "run" not in await live(harness, fork.id)
    inbox = await harness.snapshot(InboxDoc, fork.id, context)
    assert (inbox or {}).get("items", []) == []
    state["gate"].resolve()
    await root.wait_for_idle(context)
    await harness.close(context)


async def test_gives_a_run_without_a_recorded_first_entry_its_answer_as_first() -> None:
    """Upstream ``harness-after-run.test.ts``:923: a run started before ``first`` existed still has a history.

    A host that recorded no ``run.first`` -- one from before the field existed -- gets the answer as the
    first entry of its summary, so the hooks are handed a range they can page rather than a hole.
    """

    setup = chat_setup()
    state = probe()
    install(setup, state)
    root_box: dict[str, Any] = {}

    async def forget_first(*_args: Any) -> None:
        # As a run started before ``first`` existed.
        async def forget(tx: Any) -> None:
            del (await tx.doc(LiveDoc, root_box["root"].id))["run"]["first"]

        await root_box["root"].commit(forget, context)
        return None

    setup.registry.install(
        define_extension({"name": "legacy", "hooks": [hook(GenerationTask, {"onYield": forget_first})]})
    )
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    root = opened["root"]
    root_box["root"] = root
    harness = opened["harness"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    answer = kinds(await all_entries(root), "pi.assistant")[-1]
    assert state["asked"] == [
        {"reason": "answer", "first": answer["id"], "answer": answer["id"], "last": answer["id"]}
    ]
    await harness.close(context)


async def test_starts_a_continue_s_history_at_its_first_tool_round() -> None:
    """Upstream ``harness-after-run.test.ts``:1062: a continue with no failure starts at its own first round.

    The host wrote the user message itself and continues, so the run places no input of its own and
    nothing failed: its history begins where the run itself begins, at the assistant of its first tool
    round, and the page runs through the round's result to the answer.
    """

    setup = chat_setup()
    seen: list[list[str]] = []

    async def read_history(run: Any, runtime: Any, ctx: Any) -> None:
        page = await runtime.entries(
            {"minEntryId": run["first"], "maxEntryId": run["last"], "order": "ascending"},
            100,
            None,
            ctx,
        )
        seen.append([entry["kind"] for entry in page["items"]])
        return None

    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.registry.install(
        define_extension({"name": "reader", "hooks": [hook(GenerationTask, {"afterRun": read_history})]})
    )
    setup.faux.set_responses([calling("edit", "a"), assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    # A host writes the user message itself, then continues.
    await root.submit(
        {
            "type": "write",
            "entry": {"kind": "app.prompt", "model": [{"role": "user", "content": "go", "timestamp": 1}]},
        },
        context,
    )
    await (await root.submit({"type": "continue"}, context)).wait(context)
    await root.wait_for_idle(context)
    assert seen == [["pi.assistant", "pi.tool-result", "pi.assistant"]]
    await harness.close(context)


async def test_leaves_a_summary_queued_during_the_tasks_stale_behind_a_handoff() -> None:
    """Upstream ``harness-after-run.test.ts``:1094: a summary that cuts behind a handoff settles stale.

    The handoff's reset lands with the answer, and the compaction asked for while the round waited
    cuts before it: the run's end settles the summary stale rather than placing it after the reset, so
    the conversation ends on the reset and the task's own entry, and the hook is asked with the reset
    as the run's last entry.
    """

    setup = chat_setup()
    setup.settings["compaction"] = {
        "enabled": False,
        "reserve_tokens": 1000,
        "keep_recent_tokens": 1,
        "background_tokens": 0,
    }
    state = probe()
    install(setup, state)
    gate = deferred()
    started = deferred()

    async def hand_off_slowly(*_args: Any) -> dict[str, Any]:
        started.resolve()
        await gate.promise
        return {
            "output": [{"type": "text", "text": "handed off"}],
            "control": {"terminate": True, "handoff": "fresh start"},
        }

    add_tool(setup.registry, tool("handoff", hand_off_slowly))
    add_hooks(setup.registry, CompactionTask, {"beforeCompact": lambda *_args: {"summary": "earlier work"}})
    setup.faux.set_responses([calling("handoff", "a")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    compaction = await root.compact(None, context)
    await harness.wait_for_task(compaction, context)
    gate.resolve()
    await submission.wait(context)
    await root.wait_for_idle(context)
    entries = await all_entries(root)
    # The handoff's reset landed with the answer; the summary, cutting before it, settled stale at the
    # run's end.
    assert kinds(entries, "pi.compaction") == []
    assert [entry["kind"] for entry in entries][-2:] == ["pi.reset", "test.tests"]
    reset = kinds(entries, "pi.reset")[0]
    assert state["asked"][0]["reason"] == "terminate"
    assert state["asked"][0]["last"] == reset["id"]
    await harness.close(context)


async def test_stores_the_answers_held_progress_with_commit_a_and_asks_again_after_a_close() -> None:
    """Upstream ``harness-after-run.test.ts``:1140: commit A stores the answer; finish asks again.

    Nothing was stored while the answer streamed -- ``persist_within_ms`` is infinite -- so the answer's
    entry reaches Storage only because commit A, which holds the run, is an ordinary commit. The host
    dies with the hook in hand, and both a reopened host and one that follows an orderly close in
    ``finish`` ask the hooks again; the task is created once.
    """

    path = _sqlite_path()
    setup = chat_setup()
    setup.settings["progress"] = {"persist_within_ms": float("inf")}
    state = probe()
    state["hook_gate"] = deferred()
    Tests = after_run_task(state)

    async def after_run(run: Any, _runtime: Any, ctx: Any) -> dict[str, Any]:
        state["asked"].append(dict(run))
        if state["hook_gate"] is not None:
            await _race(state["hook_gate"], ctx.abort_signal)
        return {"tasks": [run_end_task(Tests, {"label": "run"})]}

    setup.registry.install(
        define_extension(
            {
                "name": "after-run",
                "tasks": [Tests],
                "hooks": [hook(GenerationTask, {"afterRun": after_run})],
            }
        )
    )
    setup.faux.set_responses([slow_message(assistant_message("x" * 40), chunks=8, interval=0.02)])
    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    await eventually(lambda: len(state["asked"]) == 1)
    opened["kill"]()

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    stored = await live(opened["harness"], opened["root"].id)
    assert "generation" not in stored
    assert stored["run"]["afterRun"] == {}
    answer = kinds(await all_entries(opened["root"]), "pi.assistant")[0]
    assert text_of(answer["model"][0]) == "x" * 40
    opened["harness"].resume()
    await eventually(lambda: len(state["asked"]) == 2)
    # An orderly close while the hook runs: what ``finish`` owed is owed again.
    await opened["harness"].close(context)
    state["hook_gate"] = None

    opened = await open_killable(open_posix_sqlite_storage, path, setup)
    await opened["root"].wait_for_idle(context)
    assert len(state["asked"]) == 3
    assert state["runs"] == ["run"]
    await opened["harness"].close(context)


async def test_the_hook_is_handed_the_surface_run_summary_and_asks_with_the_surface_task() -> None:
    """Port-authored, not translated: ``RunSummary`` and ``RunEndTask`` as the surface has them.

    Upstream declares both where a host can reach them (``src/index.ts:117-118``) and shapes them at
    ``src/harness/types.ts:1011-1026``: the object ``afterRun`` is handed has exactly ``reason``,
    ``first``, ``answer`` and ``last``, and a task it asks for is exactly ``{ task, input }``. The
    translated cases of this suite assert what those fields *mean*; this one pins that the objects
    carry nothing else and nothing less, that :func:`run_end_task` is what builds the second, and that
    a host reaches both where upstream's ``src/index.ts:117-118`` puts them — the package surface, not
    only the module that declares them.
    """

    setup = chat_setup()
    state = probe()
    tests = install(setup, state)
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.wait_for_idle(context)
    assert len(state["asked"]) == 1
    assert RunSummary is types_module.RunSummary
    assert RunEndTask is types_module.RunEndTask
    assert {"RunSummary", "RunEndTask"} <= set(pi_durable.__all__)
    assert set(state["asked"][0]) == {"reason", "first", "answer", "last"}
    assert state["runs"] == ["run 1"]
    asked_for = run_end_task(tests, {"label": "run 1"})
    assert asked_for == {"task": tests, "input": {"label": "run 1"}}
    assert set(asked_for) == {"task", "input"}
    await harness.close(context)


if TYPE_CHECKING:
    # Both are total TypedDicts, as upstream declares every member required.
    _short_summary: RunSummary = {"reason": "answer", "first": 1, "answer": 2}  # type: ignore[typeddict-item]
    _short_end_task: RunEndTask = {"task": None}  # type: ignore[typeddict-item]

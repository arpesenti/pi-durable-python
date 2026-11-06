"""Translated upstream ``harness-after-run`` cases: a run that answered waits for its run-end tasks.

Upstream ``test/harness-after-run.test.ts`` holds 29 cases: 11 of them are translated here, as 12 tests
(one upstream case is translated as two). What still does not run is named below, with what it needs.

A run whose answer's ``afterRun`` hooks ask for tasks keeps ``pi.live.run``, stays busy, and has already
settled its inputs ``done`` with the answer while they run (spec section 8.3): commit A is ``hold_run``,
the hooks are asked in ``finish``, the tasks are created as the generation's children in the commit that
moves to ``drain``, and commit B -- ``end_answered_run`` -- places the final boundary and ends the run
without a failure however those tasks ended.

Still not run, named so the gap cannot be lost. Each needs only the mechanism above plus what is named
after it: a queue that fills while the tasks run; a crash before the tasks exist or in ``finish``; a
hook reading the run's history, and that history after a compaction summarized its start; the cutoff
reached in ``finish`` or in an after-run task, and a limit lowered so no ``task_interrupted`` is owed;
a task that cannot run until Stop orphans it; a follow-up a task admits; a fork run idle on its
after-run tasks; a run whose ``first`` was never recorded; a continue's history taken from a failed run
or from its first tool round; a summary queued behind a terminate round's handoff; the held progress
stored with commit A and asked again after an orderly close; and a failed run's first entry carried
into ``runFailure`` alongside the run it failed on.

Not this suite's gap, but named here so it stays owned: ``restart_limit`` is in the port's
``RunFailureReason`` union and no run end produces it. It is the run end the restart cutoff owes, and
#49 owns that -- see the deferral record in ``tests/test_harness_blocked.py`` and the note at
``scheduler.py:504``.

The upstream cases still not run, in upstream order:
    "queues input during the tasks and places it after them: writes first, then steers and follow-ups",
    "places a reset queued during the tasks after them",
    "asks the hooks again after a crash before the tasks exist, and never once they do",
    "lets a hook read the run's history, tool results included",
    "lets a hook read the run's history after a compaction summarized the start of the run",
    "ends an answered run completed, without a failure, once finish was cut off restartLimit times",
    "keeps the run busy while an after-run task cannot run, until Stop orphans it",
    "starts a fix run with a follow-up a task admits, after the tasks",
    "gives a continue after a failed run the failed run's history",
    "ends the run without asking when the hooks are deselected before finish runs",
    "starts a fork of a conversation running its after-run tasks idle",
    "gives a run without a recorded first entry its answer as first",
    "ends the run normally when its after-run task is cut off restartLimit times",
    "reports no task_interrupted for a task that ends normally after the limit was lowered",
    "reports no task_interrupted for a generation that ends normally after the limit was lowered in finish",
    "starts a continue's history at its first tool round when nothing failed and nothing was placed",
    "leaves a summary queued during the tasks stale behind a terminate round's handoff",
    "stores the answer's held progress with commit A, and asks again after an orderly close in finish",
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from chat_support import (
    all_entries,
    assistant_message,
    chat_setup,
    faux_tool_call,
    open_chat,
)
from session_support import context
from task_support import aborted, completed, deferred, eventually, settled
from tool_support import add_tool, tool

from pi_durable import (
    GenerationTask,
    LiveDoc,
    MemoryStorage,
    define_entry,
    define_extension,
    define_task,
    hook,
    run_end_task,
)

TestsEntry: Any = define_entry("test.tests")


def probe() -> dict[str, Any]:
    """What the translated cases watch: what ran, what the hooks were asked with, what aborted."""

    return {"runs": [], "asked": [], "aborted": 0, "gate": None, "hook_gate": None}


def after_run_task(state: dict[str, Any]) -> Any:
    """A task that records its label, waits for ``gate`` while one is set, then completes."""

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        state["runs"].append(task["input"]["label"])
        if state["gate"] is not None:
            await asyncio.wait_for(asyncio.shield(_race(state["gate"], runtime.signal)), timeout=5)

        async def finish(tx: Any) -> Any:
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
    """Upstream ``harness-after-run.test.ts``:1045."""

    setup = chat_setup()
    setup.settings["maxTurns"] = 1
    state = probe()
    install(setup, state)
    for registration in editing_tools():
        add_tool(setup.registry, registration)
    setup.faux.set_responses([calling("finish", "a")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
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

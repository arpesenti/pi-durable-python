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
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest
from chat_support import (
    ChatSetup,
    all_entries,
    assistant_message,
    chat_setup,
    close_killed,
    faux_text,
    live_of,
    only_failure,
    open_killable,
    text_of,
    unanswered_reached,
)
from harness_support import add_section
from session_support import context, eventually
from task_support import aborted, deferred
from tool_support import add_tool, calls, tool

from pi_durable import LiveDoc
from pi_durable.ids import ROOT_CONVERSATION_ID, ConversationId, SubmissionId
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage

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

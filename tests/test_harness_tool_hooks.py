"""Translated upstream ``harness-tool-hooks`` suite: ``afterTool`` on every result a tool task writes.

Upstream ``test/harness-tool-hooks.test.ts`` at tag ``v1.5.0`` (spec section 8.4, #11043): the
``afterTool`` chain runs on *every* result a tool task writes, also the ones the Harness decides
without executing the tool — an unavailable, invalid, or blocked call; an interrupted call; an abort;
an abandoned nested call; and the restart-limit cutoff. The upstream ``it()`` body is the authority
for each case's premise and assertions; the line numbers below are that file's.

Crashes are simulated the way the port simulates them everywhere: every later Storage call of the
killed Harness hangs, so it writes nothing more, and a file-backed SQLite database is the medium,
because the reopened Harness must see exactly what the dead one stored. The port's hosts start
paused, so every open here resumes its Harness; upstream resumes only the host it reopens after a
crash, its fresh hosts running on the host's own always-on loop. Where upstream's `stored()` is a
bare 50 ms wait for a deferred progress commit to reach Storage, this file waits for the commit
itself, read on a second connection past the Harness that wrote it.

The ledger against upstream's file, so the count is checkable: it holds 18 cases and this file runs
all 18 -- :137, :200, :240, :260, :313, :372, :413, :443, :466, :508, :541, :585, :633, :657, :675,
:704, :726 and :784. Where a sibling proves the same mechanism from another suite it is named in
the case's docstring and is not counted twice: the hook-``terminate`` round in
``test_harness_turn_limit.py`` (:232 there), the nested-call hooks in ``test_harness_nested_tools.py``,
``structuredOutput`` kept through a replacement in ``test_harness_structured_output.py``, and
``details()`` still waiting when the call is aborted inside ``afterTool`` in
``test_harness_tools.py::test_rejects_details_still_waiting_when_the_call_is_aborted_during_after_tool``.
"""

from __future__ import annotations

import asyncio
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pytest
from chat_support import ChatSetup, all_entries, chat_setup, close_killed, open_chat, open_killable
from session_support import context, eventually
from task_support import aborted, deferred
from tool_support import (
    DONE,
    add_hooks,
    add_tool,
    calls,
    codes,
    result_text,
    results,
    tool,
)

from pi_durable import LiveDoc, MemoryStorage, ResetEntry, ToolResultEntry, ToolTask
from pi_durable.ids import ROOT_CONVERSATION_ID
from pi_durable.storage.sqlite.posix import open_posix_sqlite_storage
from pi_durable.testing import commit_writes
from pi_durable.types import EntryRecord

_DIRECTORIES: list[Path] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Any:
    yield
    await close_killed()
    while _DIRECTORIES:
        shutil.rmtree(_DIRECTORIES.pop(), ignore_errors=True)


def sqlite_path() -> Path:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-tool-hooks-"))
    _DIRECTORIES.append(directory)
    return directory / "session.sqlite"


async def open(path: Path, setup: ChatSetup) -> dict[str, Any]:
    """Open a chat over the SQLite file at ``path``; ``kill()`` simulates a crash of its process."""

    return await open_killable(open_posix_sqlite_storage, path, setup)


def blocking(name: str, **extra: Any) -> tuple[Any, Any, dict[str, int]]:
    """A tool that writes output, then blocks until its invocation is cancelled (upstream's ``blocking``).

    Returns the registration, the signal that settles once it ran, and its run count.
    """

    started = deferred()
    state = {"runs": 0}

    async def execute(_args: Any, api: Any, call_context: Any) -> Any:
        state["runs"] += 1
        api.output(f"{name} {'o' * 40}\n")
        await api.details({"run": state["runs"]}, call_context)
        started.resolve()
        await aborted(call_context.abort_signal)

    return tool(name, execute, **extra), started.promise, state


async def tool_task_id(harness: Any, index: int = 0) -> Any:
    """The id of the ``index``th tool task the live document names (upstream's ``toolTaskId``)."""

    found: dict[str, Any] = {}

    async def started() -> bool:
        live = await harness.snapshot(LiveDoc, ROOT_CONVERSATION_ID, context)
        slots = dict(live).get("tools") if live is not None else None
        slot = slots[index] if slots is not None and len(slots) > index else None
        found["id"] = slot.get("taskId") if slot is not None else None
        return found["id"] is not None

    await eventually(started)
    return found["id"]


async def stored_tool_progress(path: Path, check: Callable[[list[Any]], bool]) -> None:
    """Wait until the stored ``pi.live``'s tool slots satisfy ``check``.

    The port's idiom for upstream's ``stored()`` -- a bare 50 ms wait for a deferred progress commit
    to reach Storage: this waits for the commit itself, read on a second connection past the Harness
    that wrote it, so a kill cannot lose what the case rests on.
    """

    async def stored() -> bool:
        raw = await open_posix_sqlite_storage(str(path))
        try:
            address: dict[str, Any] = {
                "kind": LiveDoc.definition["kind"],
                "scope": {"kind": "conversation", "conversationId": ROOT_CONVERSATION_ID},
            }
            record = await raw.find_document(cast(Any, address), "current", context)
            found = await raw.document(record["id"], "current", context) if record is not None else None
            live = dict(found["value"]) if found is not None and found["value"] is not None else {}
            return check(list(cast(Sequence[Any], live.get("tools") or [])))
        finally:
            await raw.close(context)

    await eventually(stored)


def messages_of(entries: Sequence[EntryRecord]) -> dict[str, Any]:
    """The round's tool-result messages keyed by call id (upstream's ``results``)."""

    return {cast(str, item["toolCallId"]): cast(dict[str, Any], item) for item in results(entries)}


def diagnostics_of(entries: Sequence[EntryRecord], call_id: str) -> list[str | None]:
    """The diagnostic codes stored for ``call_id`` (upstream's ``diagnosticsOf``)."""

    for entry in entries:
        if ToolResultEntry.is_(entry) and cast(Mapping[str, Any], entry["model"][0])["toolCallId"] == call_id:
            return codes(cast(Mapping[str, Any], entry["data"]))
    return []


async def test_sees_and_replaces_unavailable_invalid_and_blocked_results_with_the_raw_arguments() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :137."""

    setup = chat_setup()
    ran = [0]

    async def echo_execute(*_args: Any) -> Any:
        ran[0] += 1
        return {"output": [{"type": "text", "text": "ran"}]}

    echo = tool("echo", echo_execute)
    add_tool(setup.registry, echo)
    add_tool(setup.registry, tool("gone", lambda *_args: {"output": []}))
    seen: list[dict[str, Any]] = []

    def before_tool(call: Any) -> Any:
        if call["id"] == "block":
            return {"block": "not today"}
        if call["id"] == "rewrite":
            return {"arguments": {"text": {"not": "a string"}}}
        return None

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        seen.append({"call": call, "result": result})
        return {
            **result,
            "output": [{"type": "text", "text": f"hooked {call['id']}"}],
            "details": {"by": "hook"},
        }

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool, "afterTool": after_tool})
    root: Any = None

    def first(_request: Any, _options: Any) -> Any:
        async def build() -> Any:
            # The tool is offered but no longer resolves when its task runs.
            await root.configure({"tools": [echo]}, context)
            return calls(
                ["gone", {}, "unavailable"],
                ["echo", {"text": {"not": "a string"}}, "invalid"],
                ["echo", {"text": "x"}, "rewrite"],
                ["echo", {}, "block"],
            )

        return build()

    setup.faux.set_responses([first, DONE])
    opened = await open_chat(MemoryStorage(), setup)
    root = opened["root"]
    opened["harness"].resume()
    settled = await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    assert ran[0] == 0
    by_id = {item["call"]["id"]: item for item in seen}
    assert sorted(by_id) == ["block", "invalid", "rewrite", "unavailable"]
    assert codes(by_id["unavailable"]["result"]) == ["tool_unavailable"]
    assert codes(by_id["invalid"]["result"]) == ["invalid_arguments"]
    assert codes(by_id["rewrite"]["result"]) == ["invalid_arguments"]
    assert codes(by_id["block"]["result"]) == ["blocked"]
    # The hooks see the call as the model made it, also when it is invalid or `beforeTool` rewrote it.
    assert by_id["invalid"]["call"]["arguments"] == {"text": {"not": "a string"}}
    assert by_id["rewrite"]["call"]["arguments"] == {"text": "x"}
    messages = messages_of(await all_entries(root))
    for call_id in ["unavailable", "invalid", "rewrite", "block"]:
        message = messages[call_id]
        assert message["isError"] is True
        assert message["details"] == {"by": "hook"}
        assert result_text(message).startswith(f"hooked {call_id}|<harness>")
    await opened["harness"].close(context)


async def test_bounds_a_replacement_output_with_the_tools_limits_or_the_defaults() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :200, "bounds a replacement output with the tool's
    limits, or the defaults for an unresolved tool"."""

    setup = chat_setup()
    add_tool(
        setup.registry,
        tool("small", lambda *_args: {"output": []}, output_limits={"max_bytes": 10}),
    )
    add_hooks(
        setup.registry,
        ToolTask,
        {
            "beforeTool": lambda _call: {"block": "no"},
            "afterTool": lambda call, result: {
                **result,
                "output": [
                    {"type": "text", "text": "x" * 100 if call["id"] == "c2" else "y" * (60 * 1024)}
                ],
            },
        },
    )
    root: Any = None

    def first(_request: Any, _options: Any) -> Any:
        async def build() -> Any:
            await root.configure({"tools": []}, context)
            return calls(["small", {}, "c1"])

        return build()

    setup.faux.set_responses([first, DONE])
    opened = await open_chat(MemoryStorage(), setup)
    root = opened["root"]
    opened["harness"].resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await root.configure({"tools": None}, context)
    setup.faux.set_responses([calls(["small", {}, "c2"]), DONE])
    await (await root.submit({"type": "input", "content": "again"}, context)).wait(context)
    entries = await all_entries(root)
    messages = messages_of(entries)
    # Unresolved: the default 50 KB.
    unresolved = messages["c1"]["content"][0]
    assert unresolved["type"] == "text" and len(unresolved["text"]) == 50 * 1024
    assert diagnostics_of(entries, "c1") == ["tool_unavailable", "truncated"]
    bounded = messages["c2"]["content"][0]
    assert bounded["type"] == "text" and bounded["text"] == "x" * 10
    assert diagnostics_of(entries, "c2") == ["blocked", "truncated"]
    await opened["harness"].close(context)


async def test_lets_a_hooks_terminate_on_a_blocked_call_count_toward_the_round() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :240.

    The hook-``terminate`` round is also proven on an executed call by
    ``test_harness_turn_limit.py::test_lets_terminate_settle_done_at_the_limit_and_writes_a_handoffs_reset``
    (``harness-turn-limit.test.ts:232``); this case is the blocked-call half, where the ``terminate``
    rides a result the Harness itself decides.
    """

    setup = chat_setup()
    add_tool(
        setup.registry,
        tool("echo", lambda *_args: {"output": [], "control": {"terminate": True}}),
    )
    add_hooks(
        setup.registry,
        ToolTask,
        {
            "beforeTool": lambda call: {"block": "no"} if call["id"] == "c2" else None,
            "afterTool": lambda _call, result: {**result, "control": {"terminate": True}},
        },
    )
    setup.faux.set_responses([calls(["echo", {}, "c1"], ["echo", {}, "c2"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    settled = await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    # The round terminated: no second request.
    assert setup.faux.call_count == 1
    await opened["harness"].close(context)


async def test_runs_on_nested_invalid_and_blocked_calls_with_diagnostics_and_details_not_output() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :260, "runs on nested invalid and blocked calls;
    the caller gets diagnostics and details, not output"."""

    setup = chat_setup()
    add_tool(setup.registry, tool("echo", lambda *_args: {"output": []}))
    nested: list[Any] = []

    async def batch(_args: Any, api: Any, call_context: Any) -> Any:
        nested.append(await api.execute_tool("echo", {"text": {"not": "a string"}}, call_context))
        nested.append(await api.execute_tool("echo", {"text": "secret"}, call_context))
        nested.append(await api.execute_tool("missing", {}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    seen: list[Any] = []

    def before_tool(call: Any) -> Any:
        if call["arguments"].get("text") == "secret":
            return {"block": "no secrets"}
        return None

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        if call.get("parent") is None:
            return None
        seen.append(call)
        return {
            **result,
            "output": [{"type": "text", "text": "dropped"}],
            "structuredOutput": {"dropped": True},
            "details": {"hooked": call["id"]},
            "diagnostics": [
                *(result.get("diagnostics") or []),
                {"severity": "info", "code": "hooked", "message": "seen"},
            ],
        }

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool, "afterTool": after_tool})
    setup.faux.set_responses([calls(["batch", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert [call["id"] for call in seen] == ["c1/1", "c1/2", "c1/3"]
    assert all(call["parent"]["callId"] == "c1" for call in seen)
    assert [[result["isError"], codes(result)] for result in nested] == [
        [True, ["invalid_arguments", "hooked"]],
        [True, ["blocked", "hooked"]],
        [True, ["tool_unavailable", "hooked"]],
    ]
    assert [result["details"] for result in nested] == [
        {"hooked": "c1/1"},
        {"hooked": "c1/2"},
        {"hooked": "c1/3"},
    ]
    # A Harness error result has no structured output, even when a hook supplies one.
    assert nested[1].get("structuredOutput") is None
    assert nested[2].get("structuredOutput") is None
    await opened["harness"].close(context)


async def test_gives_a_hooked_success_its_output_as_value_unless_the_tool_has_a_schema() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :313, "gives a nested call a hook turns into a success
    its output as value, unless the tool has a schema"."""

    setup = chat_setup()
    add_tool(setup.registry, tool("plain", lambda *_args: {"output": []}))
    add_tool(
        setup.registry,
        tool(
            "typed",
            lambda *_args: {"output": [], "structuredOutput": {"ok": True}},
            structured_output_schema={
                "type": "object",
                "properties": {"ok": {"type": "boolean"}},
            },
        ),
    )
    nested: list[Any] = []

    async def batch(_args: Any, api: Any, call_context: Any) -> Any:
        nested.append(await api.execute_tool("plain", {}, call_context))
        nested.append(await api.execute_tool("typed", {}, call_context))
        return {}

    add_tool(setup.registry, tool("batch", batch))
    add_hooks(
        setup.registry,
        ToolTask,
        {
            "beforeTool": lambda call: None if call.get("parent") is None else {"block": "no"},
            "afterTool": lambda call: None
            if call.get("parent") is None
            else {
                "output": [{"type": "text", "text": f"fine {call['name']}"}],
                "structuredOutput": {"ok": False},
            },
        },
    )
    setup.faux.set_responses([calls(["batch", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert [[result["isError"], result.get("structuredOutput")] for result in nested] == [
        [False, "fine plain"],
        [False, None],
    ]
    await opened["harness"].close(context)


async def test_runs_on_an_interrupted_call_and_a_replaced_output_loses_the_truncation_diagnostic() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :372, "runs on an interrupted call; a replaced output
    loses the slot's truncation diagnostic"."""

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["progress"] = {"output_interval_ms": 0, "persist_within_ms": 0}
    limits = {"output_limits": {"max_bytes": 10, "retain": "tail"}}
    work, work_started, _ = blocking("work", **limits)
    keep, keep_started, _ = blocking("keep", **limits)
    add_tool(setup.registry, work)
    add_tool(setup.registry, keep)
    seen: list[dict[str, Any]] = []

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        seen.append({"call": call, "result": result})
        return (
            {**result, "output": [{"type": "text", "text": "redacted"}]}
            if call["name"] == "work"
            else None
        )

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["work", {}, "c1"], ["keep", {}, "c2"]), DONE])
    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id
    await work_started
    await keep_started
    await stored_tool_progress(
        path, lambda slots: len(slots) == 2 and all("output" in slot and "details" in slot for slot in slots)
    )
    opened["kill"]()

    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await (await opened["harness"].submission(id, context)).wait(context)
    assert settlement["status"] == "done"
    assert sorted([(item["call"]["id"], codes(item["result"])) for item in seen]) == [
        ("c1", ["interrupted"]),
        ("c2", ["interrupted"]),
    ]
    entries = await all_entries(opened["root"])
    messages = messages_of(entries)
    assert result_text(messages["c1"]).startswith("redacted|")
    assert messages["c1"]["isError"] is True
    assert messages["c1"]["details"] == {"run": 1}
    assert diagnostics_of(entries, "c1") == ["interrupted"]
    # The kept output keeps its truncation diagnostic, after the hooks' result.
    assert diagnostics_of(entries, "c2") == ["interrupted", "truncated"]
    await opened["harness"].close(context)


async def test_runs_on_an_abort_result_but_not_on_the_aborted_results_of_calls_that_never_started() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :413.

    The abort-handler half of ``PARITY.md`` row 12 (API footguns)'s last bullet: a tool task's abort
    handler runs the ``afterTool`` chain on its ``aborted`` result.
    """

    setup = chat_setup()
    setup.settings["tool_execution"] = "sequential"
    first, first_started, _ = blocking("first")
    add_tool(setup.registry, first)
    add_tool(setup.registry, tool("second", lambda *_args: {"output": []}))
    seen: list[dict[str, Any]] = []

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        seen.append({"call": call, "result": result})
        return {**result, "details": {"hooked": True}}

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["first", {}, "c1"], ["second", {}, "c2"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    submission = await opened["root"].submit({"type": "input", "content": "go"}, context)
    await first_started
    await opened["root"].abort(context)
    assert (await submission.wait(context))["status"] == "unanswered"
    assert [(item["call"]["id"], codes(item["result"])) for item in seen] == [("c1", ["aborted"])]
    messages = messages_of(await all_entries(opened["root"]))
    assert messages["c1"]["isError"] is True
    assert messages["c1"]["details"] == {"hooked": True}
    assert (messages.get("c2") or {}).get("details") is None
    await opened["harness"].close(context)


async def test_reports_a_throwing_hook_on_an_abort_result_and_settles_the_result_as_built() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :443."""

    setup = chat_setup()
    work, work_started, _ = blocking("work")
    add_tool(setup.registry, work)

    def after_tool(_call: Any, _result: Any) -> Any:
        raise RuntimeError("hook failed")

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    submission = await opened["root"].submit({"type": "input", "content": "go"}, context)
    await work_started
    await opened["root"].abort(context)
    assert (await submission.wait(context))["status"] == "unanswered"
    entries = await all_entries(opened["root"])
    assert result_text(messages_of(entries)["c1"]).startswith(f"work {'o' * 40}")
    assert diagnostics_of(entries, "c1") == ["aborted"]
    assert any("hook failed" in str(error) for error in setup.reports)
    await opened["harness"].close(context)


async def test_settles_an_abort_without_the_hooks_in_the_abort_handlers_last_attempt() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :466.

    The other half of ``PARITY.md`` row 12 (API footguns)'s last bullet: the last attempt before
    the restart limit runs without hooks.
    """

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 2
    setup.settings["progress"] = {"output_interval_ms": 0, "persist_within_ms": 0}
    work, work_started, _ = blocking("work")
    add_tool(setup.registry, work)
    seen: list[dict[str, Any]] = []
    hanging = deferred()

    async def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        seen.append({"call": call, "result": result})
        hanging.resolve()
        # Never returns: the host dies inside the hook.
        await asyncio.Event().wait()

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id
    await work_started
    task_id = await tool_task_id(opened["harness"])
    await stored_tool_progress(
        path, lambda slots: len(slots) == 1 and "output" in slots[0] and "details" in slots[0]
    )
    abort = asyncio.ensure_future(opened["root"].abort(context))
    abort.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    await hanging.promise
    opened["kill"]()

    # One restart, at the limit minus one: the attempt the scheduler would fault when cut off runs no hooks.
    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await (await opened["harness"].submission(id, context)).wait(context)
    assert settlement["status"] == "unanswered"
    assert [codes(item["result"]) for item in seen] == [["aborted"]]
    entries = await all_entries(opened["root"])
    assert result_text(messages_of(entries)["c1"]).startswith(f"work {'o' * 40}")
    assert diagnostics_of(entries, "c1") == ["aborted"]
    record = await opened["harness"].get_task(task_id, context)
    assert record["state"]["status"] == "terminal"
    assert record["state"]["outcome"]["status"] == "aborted"
    await opened["harness"].close(context)


async def test_sees_the_held_progress_of_an_aborted_call_and_the_result_stores_it() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :508."""

    path = sqlite_path()
    setup = chat_setup()
    # Progress commits are held in memory until the result commit stores them.
    setup.settings["progress"] = {"output_interval_ms": 0, "persist_within_ms": 60_000}
    work, work_started, _ = blocking("work")
    add_tool(setup.registry, work)
    seen: list[dict[str, Any]] = []

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        seen.append({"call": call, "result": result})
        return None

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    opened = await open(path, setup)
    opened["harness"].resume()
    submission = await opened["root"].submit({"type": "input", "content": "go"}, context)
    await work_started
    await opened["root"].abort(context)
    assert (await submission.wait(context))["status"] == "unanswered"
    assert len(seen) == 1
    assert seen[0]["result"]["details"] == {"run": 1}
    assert result_text({"content": seen[0]["result"]["output"]}) == f"work {'o' * 40}\n"
    await opened["harness"].close(context)

    opened = await open(path, setup)
    opened["harness"].resume()
    message = messages_of(await all_entries(opened["root"]))["c1"]
    assert message["isError"] is True
    assert message["details"] == {"run": 1}
    assert result_text(message).startswith(f"work {'o' * 40}")
    await opened["harness"].close(context)


async def test_runs_on_an_abandoned_nested_calls_result() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :541."""

    path = sqlite_path()
    setup = chat_setup()
    gated = deferred()
    gate = [True]
    add_tool(setup.registry, tool("gated", lambda *_args: {"output": []}))

    async def batch(_args: Any, api: Any, call_context: Any) -> Any:
        await api.execute_tool("gated", {}, call_context)
        return {}

    add_tool(setup.registry, tool("batch", batch))
    seen: list[dict[str, Any]] = []

    async def before_tool(call: Any, _api: Any, hook_context: Any) -> Any:
        # Holds the nested call in its `call` phase, before intent, until the Harness closes.
        if call["name"] != "gated" or not gate[0]:
            return None
        gated.resolve()
        await aborted(hook_context.abort_signal)

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        if call.get("parent") is None:
            return None
        seen.append({"call": call, "result": result})
        return {**result, "details": {"hooked": True}}

    add_hooks(setup.registry, ToolTask, {"beforeTool": before_tool, "afterTool": after_tool})
    setup.faux.set_responses([calls(["batch", {}, "c1"]), DONE])
    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id
    await gated.promise
    await opened["harness"].close(context)
    gate[0] = False

    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await (await opened["harness"].submission(id, context)).wait(context)
    assert settlement["status"] == "done"
    assert [(item["call"]["id"], codes(item["result"])) for item in seen] == [("c1/1", ["abandoned"])]
    await opened["harness"].close(context)


async def test_runs_on_a_restart_limit_result_at_the_limit_and_not_once_the_hooks_were_cut_off() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :585, "runs on a restart_limit result at the limit,
    and not once the hooks themselves were cut off"."""

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["restart_limit"] = 1
    setup.settings["progress"] = {"output_interval_ms": 0, "persist_within_ms": 0}
    work, work_started, work_state = blocking("work")
    add_tool(setup.registry, work)
    seen: list[dict[str, Any]] = []
    hanging = deferred()

    async def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        seen.append({"call": call, "result": result})
        hanging.resolve()
        # Never returns: the host dies inside the hook.
        await asyncio.Event().wait()

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    opened = await open(path, setup)
    opened["harness"].resume()
    id = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id
    await work_started
    task_id = await tool_task_id(opened["harness"])
    await stored_tool_progress(
        path, lambda slots: len(slots) == 1 and "output" in slots[0] and "details" in slots[0]
    )
    opened["kill"]()

    # Cut off once, at the limit: the task ends itself, and the hooks run on its result.
    opened = await open(path, setup)
    opened["harness"].resume()
    await hanging.promise
    assert [codes(item["result"]) for item in seen] == [["restart_limit"]]
    opened["kill"]()

    # Past the limit: ends without the hooks.
    opened = await open(path, setup)
    opened["harness"].resume()
    settlement = await (await opened["harness"].submission(id, context)).wait(context)
    assert settlement["status"] == "done"
    assert len(seen) == 1
    assert work_state["runs"] == 1
    entries = await all_entries(opened["root"])
    assert diagnostics_of(entries, "c1") == ["restart_limit"]
    record = await opened["harness"].get_task(task_id, context)
    assert record["state"]["status"] == "terminal"
    assert record["state"]["outcome"]["status"] == "failed"
    assert record["state"]["outcome"]["error"]["detail"] == {"reason": "restart_limit"}
    await opened["harness"].close(context)


async def test_runs_after_tool_inside_their_own_tool_task() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :633.

    A call to a tool the request did not offer runs the ``afterTool`` chain inside its own
    ``pi.tool`` task.
    """

    setup = chat_setup()
    seen: list[dict[str, Any]] = []

    def after_tool(call: Any, result: Any, api: Any, _ctx: Any) -> Any:
        seen.append({"id": call["id"], "task_id": api.task_id})
        return {**result, "output": [{"type": "text", "text": "try another tool"}]}

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["ghost", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert [item["id"] for item in seen] == ["c1"]
    record = await opened["harness"].get_task(seen[0]["task_id"], context)
    assert record["kind"] == "pi.tool"
    assert record["input"]["kind"] == "model"
    assert record["input"]["callId"] == "c1"
    assert record["input"]["unoffered"] is True
    entries = await all_entries(opened["root"])
    assert result_text(messages_of(entries)["c1"]).startswith("try another tool|")
    assert diagnostics_of(entries, "c1") == ["tool_unavailable"]
    await opened["harness"].close(context)


async def test_counts_toward_a_rounds_terminate() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :657."""

    setup = chat_setup()
    add_tool(setup.registry, tool("echo", lambda *_args: {"output": []}))
    add_hooks(
        setup.registry,
        ToolTask,
        {"afterTool": lambda _call, result: {**result, "control": {"terminate": True}}},
    )
    setup.faux.set_responses([calls(["echo", {}, "c1"], ["ghost", {}, "c2"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    settled = await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    assert setup.faux.call_count == 1
    await opened["harness"].close(context)


async def test_takes_their_turn_in_a_sequential_round_and_the_last_handoff_in_call_order_wins() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :675."""

    setup = chat_setup()
    setup.settings["tool_execution"] = "sequential"
    order: list[str] = []
    for name in ["a", "b"]:

        def execute(*_args: Any, _name: str = name) -> Any:
            order.append(_name)
            return {"output": []}

        add_tool(setup.registry, tool(name, execute))

    def after_tool(call: Any, result: Any, _api: Any, _ctx: Any) -> Any:
        order.append(f"hook {call['id']}")
        return None if call["id"] == "c1" else {**result, "control": {"handoff": f"from {call['id']}"}}

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses([calls(["a", {}, "c1"], ["ghost", {}, "c2"], ["b", {}, "c3"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert order == ["a", "hook c1", "hook c2", "b", "hook c3"]
    resets = [entry for entry in await all_entries(opened["root"]) if ResetEntry.is_(entry)]
    assert [cast(dict[str, Any], entry["model"][0])["content"] for entry in resets] == ["from c3"]
    await opened["harness"].close(context)


async def test_gives_each_unoffered_call_a_tool_task_in_a_parallel_round_of_only_such_calls() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :704."""

    setup = chat_setup()
    seen: list[str] = []

    def after_tool(call: Any) -> Any:
        seen.append(call["id"])
        return None

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    setup.faux.set_responses(
        [calls(["ghost", {}, "c1"], ["spirit", {}, "c2"], ["ghost", {}, "c3"]), DONE]
    )
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    settled = await (await opened["root"].submit({"type": "input", "content": "go"}, context)).wait(context)
    assert settled["status"] == "done"
    assert sorted(seen) == ["c1", "c2", "c3"]
    entries = await all_entries(opened["root"])
    for call_id in ["c1", "c2", "c3"]:
        assert diagnostics_of(entries, call_id) == ["tool_unavailable"]
    # The round's results go to the next request.
    assert setup.faux.call_count == 2
    await opened["harness"].close(context)


async def unoffered_flag_generation(version: int) -> None:
    """The :726 case over one stored database: generation ``version`` (2, or 1 migrated in place)."""

    path = sqlite_path()
    setup = chat_setup()
    setup.settings["tool_execution"] = "sequential"
    started = deferred()
    runs = [0]

    async def a_execute(_args: Any, _api: Any, call_context: Any) -> Any:
        runs[0] += 1
        if runs[0] > 1:
            return {"output": []}
        started.resolve()
        await aborted(call_context.abort_signal)

    add_tool(setup.registry, tool("a", a_execute, replay="safe"))
    add_tool(
        setup.registry,
        tool("ghost", lambda *_args: {"output": [{"type": "text", "text": "ran"}]}),
    )
    setup.faux.set_responses([calls(["a", {}, "c1"], ["ghost", {}, "c2"]), DONE])
    opened = await open(path, setup)
    root = opened["root"]
    opened["harness"].resume()
    # `ghost` is registered, but not offered to this request.
    await root.configure({"tools": ["a"]}, context)
    id = (await root.submit({"type": "input", "content": "go"}, context)).id
    await started.promise
    live = await opened["harness"].snapshot(LiveDoc, ROOT_CONVERSATION_ID, context)
    generation = dict(live)["run"]["taskId"]
    assert [(slot["callId"], slot.get("unoffered")) for slot in dict(live)["tools"]] == [
        ("c1", None),
        ("c2", True),
    ]
    assert (await opened["harness"].get_task(generation, context))["version"] == 2
    # The tools are offered when the round goes on: only the flag keeps `ghost` from running.
    await root.configure({"tools": None}, context)
    await opened["harness"].close(context)
    if version == 1:
        storage = await open_posix_sqlite_storage(str(path))
        try:
            record = await storage.task(generation, context)
            assert record is not None
            await commit_writes(
                storage,
                [{"type": "task", "value": cast(Any, {**record, "version": 1})}],
                context,
            )
        finally:
            await storage.close(context)

    opened = await open(path, setup)
    opened["harness"].resume()
    root = opened["root"]
    settlement = await (await opened["harness"].submission(id, context)).wait(context)
    assert settlement["status"] == "done"
    assert (await opened["harness"].get_task(generation, context))["version"] == 2
    entries = await all_entries(root)
    assert diagnostics_of(entries, "c2") == ["tool_unavailable"]
    assert "ran" not in result_text(messages_of(entries)["c2"])
    await opened["harness"].close(context)


async def test_keeps_their_flag_in_a_version_2_sequential_round_also_one_migrated_from_version_1() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :726, "keep their flag in a version 2 generation's
    sequential round, also one migrated from version 1"."""

    for version in [2, 1]:
        await unoffered_flag_generation(version)


async def test_settles_unavailable_not_aborted_when_the_round_is_aborted_before_they_start() -> None:
    """Upstream ``harness-tool-hooks.test.ts`` :784, "settle unavailable, not aborted, when their
    sequential round is aborted before they start"."""

    setup = chat_setup()
    setup.settings["tool_execution"] = "sequential"
    started = deferred()

    async def wait_execute(_args: Any, _api: Any, call_context: Any) -> Any:
        started.resolve()
        await aborted(call_context.abort_signal)

    add_tool(setup.registry, tool("wait", wait_execute))
    setup.faux.set_responses([calls(["wait", {}, "c1"], ["ghost", {}, "c2"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    opened["harness"].resume()
    submission = await opened["root"].submit({"type": "input", "content": "go"}, context)
    await started.promise
    await opened["root"].abort(context)
    assert (await submission.wait(context))["status"] == "unanswered"
    entries = await all_entries(opened["root"])
    assert diagnostics_of(entries, "c1") == ["aborted"]
    assert diagnostics_of(entries, "c2") == ["tool_unavailable"]
    await opened["harness"].close(context)

"""Translated upstream ``harness-live-deltas`` cases.

Throttled text partials as appends, the complete-base rule of ``pi.live``, and the tool cases: the
generation-to-round handover, head and tail output windows, whole-window writes beyond the overlap
scan, leaf-wise details with appended diagnostics, and a sequential two-tool round's bases.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from chat_support import all_entries, assistant_message, chat_setup, open_chat, slow_message
from session_support import context, document_changes

from pi_durable import LiveDoc, MemoryStorage


def live_commit_ops(harness: Any) -> list[list[Any]]:
    commits: list[list[Any]] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] == "pi.live" and change["ops"]:
                commits.append(list(change["ops"]))

    harness.subscribe_commits(observe)
    return commits


async def test_streams_partial_text_as_appends() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [slow_message(assistant_message("word " * 250), chunks=20, interval=0.05)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    commits = live_commit_ops(harness)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    partials = [
        ops
        for ops in commits
        if any(op[0] == "s" and op[1] == ["generation", "message"] for op in ops)
        or any(
            op[0] == "a" and op[1][:3] == ["generation", "message", "content"]
            for op in ops
        )
    ]
    assert len(partials) > 2
    assert partials[0] == [["s", ["generation", "message"], partials[0][0][2]]]
    for ops in partials[1:]:
        assert ops == [
            ["a", ["generation", "message", "content", 0, "text"], ops[0][2]]
        ], ops
    assert [entry["kind"] for entry in await all_entries(root)][-1] == "pi.assistant"
    await harness.close(context)


async def test_stores_a_complete_base_exactly_in_the_commits_where_nothing_runs() -> None:
    written: dict[int, str] = {}
    live_id: dict[str, Any] = {}

    class RecordingStorage(MemoryStorage):
        async def commit(self, writes: Any, call_context: Any) -> Any:
            kind: str | None = None
            for write in writes:
                if (
                    write["type"] == "document.change"
                    and write["id"] == live_id.get("id")
                ):
                    kind = write["content"]["kind"]
            seq = await super().commit(writes, call_context)
            if kind is not None:
                written[seq] = kind
            return seq

    setup = chat_setup()
    setup.faux.set_responses(
        [slow_message(assistant_message("streamed answer"), chunks=8, interval=0.03)]
    )
    opened = await open_chat(RecordingStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values: dict[int, dict[str, Any]] = {}

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] != "pi.live":
                continue
            live_id["id"] = change["record"]["id"]
            if change["value"] is not None:
                values[publication["seq"]] = change["value"]

    harness.subscribe_commits(observe)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)

    def nothing_runs(value: dict[str, Any]) -> bool:
        return "generation" not in value and not any(
            slot.get("status") == "running" for slot in value.get("tools") or ()
        )

    kinds = []
    for seq, kind in written.items():
        assert (kind == "base") is nothing_runs(values[seq]), (kind, values[seq])
        kinds.append(kind)
    # The generation-only path writes a base when the run ends (and at the submission commit before
    # this subscription); the tool-handover bases arrive with the tool-execution ticket.
    assert kinds.count("base") >= 1
    assert "delta" in kinds
    await harness.close(context)


def test_keeps_a_complete_base_exactly_while_nothing_runs() -> None:
    definition = LiveDoc.definition
    check = definition.checkpoint_when
    assert check is not None

    def base(value: dict[str, Any]) -> bool:
        return check(value, [], {"deltasSinceBase": 1000})

    def slot(status: str) -> dict[str, Any]:
        return {"callId": "c", "name": "n", "status": status}

    run = {"taskId": 1, "inputs": []}
    assert base({}) is True
    assert base({"run": run, "generation": {"attempt": 1}}) is False
    assert base({"run": run, "tools": [slot("pending"), slot("done")]}) is True
    assert base({"run": run, "tools": [slot("done"), slot("running")]}) is False
    assert base({"run": run}) is True


async def drive(output_limits: dict[str, Any] | None = None) -> dict[str, Any]:
    """Drive one tool call step by step and capture the operations of every ``pi.live`` commit."""

    from task_support import eventually
    from tool_support import DONE, add_tool, calls, chat_setup, tool

    setup = chat_setup()
    actions: asyncio.Queue[Any] = asyncio.Queue()

    async def execute(_args: Any, api: Any, _context: Any) -> Any:
        while True:
            action = await actions.get()
            if action is None:
                return {}
            await _maybe_await(action(api))

    add_tool(setup.registry, tool("drive", execute, output_limits=output_limits or {}))
    setup.faux.set_responses([calls(["drive", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    commits = live_commit_ops(harness)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)

    async def running() -> bool:
        value = await harness.snapshot(LiveDoc, root.id, context)
        slots = (value or {}).get("tools") or []
        return bool(slots) and slots[0].get("status") == "running"

    await eventually(running)

    async def step(action: Any) -> list[Any]:
        before = len(commits)
        await actions.put(action)
        await eventually(lambda: len(commits) > before)
        return commits[-1]

    async def finish() -> list[list[Any]]:
        before = len(commits)
        await actions.put(None)
        await submission.wait(context)
        return commits[before:]

    async def current() -> dict[str, Any]:
        return cast(dict[str, Any], await harness.snapshot(LiveDoc, root.id, context) or {})

    return {
        "harness": harness,
        "root": root,
        "commits": commits,
        "step": step,
        "finish": finish,
        "current": current,
    }


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value


def contains(ops: list[Any], *expected: Any) -> bool:
    return all(item in ops for item in expected)


async def test_hands_a_generation_over_to_its_tool_round_with_one_field_write_each() -> None:
    from tool_support import DONE, add_tool, calls, chat_setup, tool

    setup = chat_setup()
    add_tool(setup.registry, tool("noop", lambda *_args: {"output": []}))
    setup.faux.set_responses([calls(["noop", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    commits = live_commit_ops(harness)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    tasks = (
        await harness.commit(lambda tx: tx.scan_tasks({"conversationId": root.id}, 20), context)
    )["items"]

    def ids(kind: str) -> list[int]:
        return sorted(task["id"] for task in tasks if task["kind"] == kind)

    first_generation, second_generation = ids("pi.generation")
    [tool_task] = ids("pi.tool")
    page = await root.entries({}, 10, None, context)
    result = next(entry["id"] for entry in page["items"] if entry["kind"] == "pi.tool-result")

    assert len(commits) == 8
    # submission
    assert commits[0][0][0] == "s" and commits[0][0][1] == ["run"]
    assert commits[0][0][2]["taskId"] == first_generation
    assert len(commits[0][0][2]["inputs"]) == 1
    # request
    assert commits[1] == [["s", ["generation"], {"attempt": 1}]]
    # the generation starts its tool round and keeps the run
    assert contains(
        commits[2],
        ["d", ["generation"]],
        [
            "s",
            ["tools"],
            [{"callId": "c1", "name": "noop", "taskId": tool_task, "status": "pending"}],
        ],
    )
    # intent
    assert commits[3] == [["s", ["tools", 0, "status"], "running"]]
    # result
    assert contains(
        commits[4],
        ["s", ["tools", 0, "status"], "done"],
        ["s", ["tools", 0, "entry"], result],
    )
    # the generation's tools phase hands the run to the next generation
    assert contains(
        commits[5],
        ["d", ["tools"]],
        ["s", ["run", "taskId"], second_generation],
    )
    assert commits[6] == [["s", ["generation"], {"attempt": 1}]]
    # the answer ends the run
    assert contains(commits[7], ["d", ["run"]], ["d", ["generation"]])
    assert [len(commits[index]) for index in (2, 4, 5)] == [2, 2, 2]
    await harness.close(context)


async def test_appends_head_output_and_then_only_updates_the_dropped_counts_once_full() -> None:
    run = await drive({"max_lines": 2})
    output = ["tools", 0, "output"]
    assert await run["step"](lambda api: api.output("one\n")) == [["s", output, "one\n"]]
    assert await run["step"](lambda api: api.output("two\n")) == [["a", output, "two\n"]]
    # The window is full: the retained text stays; only the counts change.
    assert await run["step"](lambda api: api.output("three\n")) == [
        ["s", ["tools", 0, "droppedBytes"], 6],
        ["s", ["tools", 0, "droppedLines"], 1],
    ]
    assert await run["step"](lambda api: api.output("four\n")) == [
        ["s", ["tools", 0, "droppedBytes"], 11],
        ["s", ["tools", 0, "droppedLines"], 2],
    ]
    await run["finish"]()
    await run["harness"].close(context)


async def test_slides_a_tail_window_as_a_front_trim_plus_an_append() -> None:
    run = await drive({"max_lines": 3, "retain": "tail"})
    output = ["tools", 0, "output"]
    assert await run["step"](lambda api: api.output("line 1\nline 2\nline 3\n")) == [
        ["s", output, "line 1\nline 2\nline 3\n"]
    ]
    assert await run["step"](lambda api: api.output("line 4\n")) == [
        ["t", output, 7],
        ["a", output, "line 4\n"],
        ["s", ["tools", 0, "droppedBytes"], 7],
        ["s", ["tools", 0, "droppedLines"], 1],
    ]
    # The buffer keeps only the window, and later slides stay minimal and exact.
    assert await run["step"](lambda api: api.output("line 5\nline 6\n")) == [
        ["t", output, 14],
        ["a", output, "line 5\nline 6\n"],
        ["s", ["tools", 0, "droppedBytes"], 21],
        ["s", ["tools", 0, "droppedLines"], 3],
    ]
    assert (await run["current"]())["tools"][0]["output"] == "line 4\nline 5\nline 6\n"
    await run["finish"]()
    await run["harness"].close(context)


async def test_writes_the_whole_window_when_the_overlap_search_finds_no_shared_part() -> None:
    wide = await drive({"max_bytes": 100 * 1024, "max_lines": 1_000_000, "retain": "tail"})
    output = ["tools", 0, "output"]

    def line(index: int) -> str:
        return f"{str(index).zfill(10)} {'x' * 989}\n"

    text = "".join(line(index) for index in range(100))
    await wide["step"](lambda api: api.output(text))
    slid = await wide["step"](lambda api: api.output(line(100) + line(101) + line(102) + line(103)))
    assert [op[0] for op in slid if op[1] == output] == ["s"]
    await wide["finish"]()
    await wide["harness"].close(context)

    repetitive = await drive({"max_lines": 50, "retain": "tail"})
    await repetitive["step"](lambda api: api.output("y\n" * 50))
    repeated = await repetitive["step"](lambda api: api.output("z\n"))
    assert [op for op in repeated if op[1] == output] == [
        ["t", output, 2],
        ["a", output, "z\n"],
    ]
    await repetitive["finish"]()
    await repetitive["harness"].close(context)


async def test_diffs_details_leaf_by_leaf_and_appends_diagnostics() -> None:
    run = await drive()
    details = ["tools", 0, "details"]
    assert await run["step"](lambda api: api.details({"step": 1, "log": "a"}, context)) == [
        ["s", details, {"step": 1, "log": "a"}]
    ]
    second = await run["step"](lambda api: api.details({"step": 2, "log": "ab"}, context))
    assert contains(second, ["s", [*details, "step"], 2], ["a", [*details, "log"], "b"])
    assert await run["step"](lambda api: api.details({"step": 2}, context)) == [
        ["d", [*details, "log"]]
    ]
    diagnostics = ["tools", 0, "diagnostics"]
    first = {"severity": "info", "message": "first"}
    second_diagnostic = {"severity": "warn", "message": "second"}
    assert await run["step"](lambda api: api.diagnostic(first)) == [["s", diagnostics, [first]]]
    assert await run["step"](lambda api: api.diagnostic(second_diagnostic)) == [
        ["p", diagnostics, 1, 0, [second_diagnostic]]
    ]
    # Settlement moves everything into the result entry and keeps the slot small.
    settled = (await run["finish"]())[0]
    assert contains(
        settled,
        ["s", ["tools", 0, "status"], "done"],
        ["d", details],
        ["d", diagnostics],
    )
    await run["harness"].close(context)


async def test_stores_a_complete_base_exactly_in_the_commits_where_nothing_runs_with_tools() -> None:
    from tool_support import DONE, add_tool, calls, chat_setup, tool

    written: dict[int, str] = {}
    live_id: dict[str, Any] = {}

    class RecordingStorage(MemoryStorage):
        async def commit(self, writes: Any, call_context: Any) -> Any:
            kind: str | None = None
            for write in writes:
                if write["type"] == "document.change" and write["id"] == live_id.get("id"):
                    kind = write["content"]["kind"]
            seq = await super().commit(writes, call_context)
            if kind is not None:
                written[seq] = kind
            return seq

    setup = chat_setup()
    for name in ("first", "second"):

        async def execute(_args: Any, api: Any, name: str = name) -> Any:
            api.output(f"{name} output\n")
            await asyncio.sleep(0.15)
            api.output(f"{name} more\n")
            return {}

        add_tool(setup.registry, tool(name, execute))
    setup.faux.set_responses([calls(["first", {}, "a"], ["second", {}, "b"]), DONE])
    setup.settings["tool_execution"] = "sequential"
    opened = await open_chat(RecordingStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    values: dict[int, dict[str, Any]] = {}

    def observe(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            if change["record"]["kind"] != "pi.live":
                continue
            live_id["id"] = change["record"]["id"]
            if change["value"] is not None:
                values[publication["seq"]] = change["value"]

    harness.subscribe_commits(observe)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)

    def nothing_runs(value: dict[str, Any]) -> bool:
        return "generation" not in value and not any(
            slot.get("status") == "running" for slot in value.get("tools") or ()
        )

    for seq, kind in written.items():
        assert (kind == "base") is nothing_runs(values[seq]), (kind, values[seq])
    # The handover between the generation and its tool round writes a base with pending slots.
    assert any(
        kind == "base" and "generation" not in values[seq] and values[seq].get("tools")
        for seq, kind in written.items()
    )
    assert any(
        kind == "delta" and any(slot.get("status") == "running" for slot in values[seq].get("tools") or ())
        for seq, kind in written.items()
    )
    await harness.close(context)

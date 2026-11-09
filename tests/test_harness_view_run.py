"""Translated v1.5.0 ``harness-view-run`` suite: a run as the conversation view shows it.

What a renderer sees of a run is the transcript window and ``pi.live``, and every change a commit
makes, rebuilt from operations alone. This is where the assertions that rode on the deleted agent
event adapter live now: the same facts about a run, read through the windowed view and the live
document instead of through translated events (spec §9.3).

Every one of the upstream file's 13 cases is here, beside two the port added of its own. The
streamed-partials tail -- "streams a tool-call partial as appends at its argument paths", "commits a
growing text and tool call at a cost that follows the new text, not the string lengths", and "keeps a
watcher's memory proportional to a long streamed text and tool call, not to their commits" -- is told
with the port's measures: upstream weighs V8's heap, which has no counterpart here, so the cases weigh
what the commits carry and what the watcher is handed. Each docstring names the measure it took and
``docs/parity-report-1.5.0.md`` records the half this tree cannot weigh.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

from chat_support import (
    AssistantStream,
    Step,
    assistant_message,
    chat_setup,
    faux_text,
    faux_tool_call,
    open_chat,
    slow_message,
    text_of,
)
from live_support import slots_of, view_history
from session_support import context, document_changes, flush, observe_commits
from task_support import aborted, deferred, eventually
from tool_support import DONE, add_tool, tool

from pi_durable import AgentDoc, LiveDoc, MemoryStorage, UsageDoc
from pi_durable.delta import apply_immutable
from pi_durable.json import copy_json


def live_of(view: Any) -> dict[str, Any]:
    """The view's ``pi.live``, read as a renderer reads it."""

    return dict(view["docs"].get("pi.live") or {})


def replay(frames: list[dict[str, Any]]) -> Any:
    """Every frame reached from the acquisition value by its operations alone; the frames must agree."""

    value = frames[0]["value"]
    for frame in frames[1:]:
        value = apply_immutable(value, frame["ops"])
        assert value == frame["value"]
    return value


async def drained() -> None:
    await flush()


def partials_of(harness: Any) -> list[dict[str, Any]]:
    """Committed partials of the Harness's generations, one per commit that has one."""

    partials: list[dict[str, Any]] = []

    def observer(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            message = (change["value"] or {}).get("generation", {}).get("message")
            if change["record"]["kind"] == "pi.live" and message is not None:
                partials.append(message)

    observe_commits(harness, observer)
    return partials


def partials_in(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The distinct in-flight partials in ``states``, in order."""

    partials: list[dict[str, Any]] = []
    for state in states:
        message = (state.get("generation") or {}).get("message")
        if message is not None and message != (partials[-1] if partials else None):
            partials.append(message)
    return partials


def slot_at(states: list[dict[str, Any]], index: int, call_id: str) -> dict[str, Any] | None:
    """The slot of ``call_id`` in ``states[index]``, among the model-issued calls."""

    return next(
        (slot for slot in states[index].get("tools") or () if slot["callId"] == call_id), None
    )


def running_output(states: list[dict[str, Any]], call_id: str) -> str | None:
    """The output the running slot of ``call_id`` last showed."""

    outputs = running_outputs(states, call_id)
    return outputs[-1] if outputs else None


def running_outputs(states: list[dict[str, Any]], call_id: str) -> list[str]:
    """Every output the running slot of ``call_id`` showed, in the order it showed it."""

    outputs: list[str] = []
    for index in range(len(states)):
        slot = slot_at(states, index, call_id)
        if slot is not None and slot["status"] == "running" and "output" in slot:
            output = str(slot["output"])
            if output != (outputs[-1] if outputs else None):
                outputs.append(output)
    return outputs


async def test_streams_the_answer_as_operations_that_rebuild_every_partial_then_appends_it() -> None:
    setup = chat_setup()
    text = "streamed answer text " * 20
    # One second of stream against the 100 ms partial throttle: several partials commit before the
    # answer, which the operation replay needs. A 200 ms stream left the second flush a 10 ms margin
    # and Linux runners that sleep precisely committed only the first.
    setup.faux.set_responses(
        [slow_message(assistant_message([faux_text(text)]), chunks=20, interval=0.05)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    partials = partials_of(harness)
    history = await view_history(harness, root.id)
    # What a consumer holds the moment it attaches: its own window over the conversation, empty here,
    # and no live state at all.
    assert history["views"][0]["entries"] == []
    assert live_of(history["views"][0]) == {}
    assert (history["views"][0]["docs"].get("pi.inbox") or {}).get("items") == []
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await submission.wait(context)
    await drained()
    history["detach"]()
    # Every value is the one before it advanced by its own operations: nothing is skipped or invented.
    assert len(history["frames"]) > 1
    replay(history["frames"])
    states = history["live"]()
    assert len(partials) > 1
    assert [text_of(partial) for partial in partials_in(states)] == [
        text_of(partial) for partial in partials
    ]
    # The run shows from the input's commit until the commit that appends the answer and clears the
    # partial: the same span the lifecycle events traced, read as state.
    started = next(index for index, state in enumerate(states) if state.get("run") is not None)
    assert states[started]["run"]["inputs"] == [submission.id]
    assert states[started]["run"]["turns"] == 0
    assert isinstance(states[started]["run"]["taskId"], int)
    assert history["views"][started]["entries"][-1]["kind"] == "pi.user"
    ended = next(index for index, state in enumerate(states) if index > started and not state.get("run"))
    assert states[ended].get("generation") is None
    assert text_of(history["views"][ended]["entries"][-1]["model"][0]) == text
    assert states[-1] == {}
    await harness.close(context)


async def test_shows_a_tool_calls_slot_from_its_listing_to_its_running_output_to_its_result() -> None:
    setup = chat_setup()
    gate = deferred()

    async def execute(_args: Any, api: Any, _context: Any) -> dict[str, Any]:
        api.output("one\n")
        await asyncio.sleep(0.15)
        api.output("two\n")
        await gate.promise
        return {}

    add_tool(setup.registry, tool("print", execute))
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "print", "arguments": {"text": "print"}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    history = await view_history(harness, root.id)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await eventually(lambda: running_output(history["live"](), "c1") == "one\ntwo\n", timeout=5)
    gate.resolve()
    await submission.wait(context)
    await drained()
    history["detach"]()
    replay(history["frames"])
    states = history["live"]()
    listed = next(index for index, state in enumerate(states) if slot_at(states, index, "c1"))
    # Listed with the tool-calling answer, in the same commit, carrying the arguments the model gave.
    slot = slot_at(states, listed, "c1")
    assert slot is not None
    assert slot["name"] == "print"
    assert isinstance(slot["taskId"], int)
    answer = history["views"][listed]["entries"][-1]
    assert answer["kind"] == "pi.assistant"
    assert answer["model"][0]["content"][0] == {
        "type": "toolCall",
        "id": "c1",
        "name": "print",
        "arguments": {"text": "print"},
    }
    # The running output reaches the view as it is written, in order.
    assert running_outputs(states, "c1") == ["one\n", "one\ntwo\n"]
    # Done with its result entry, appended in the same commit: the slot names the entry that ends it,
    # which is where a renderer draws the tool's result from.
    done = next(
        index
        for index in range(len(states))
        if (slot_at(states, index, "c1") or {}).get("status") == "done"
    )
    result_entry = history["views"][done]["entries"][-1]
    assert (slot_at(states, done, "c1") or {})["entry"] == result_entry["id"]
    assert result_entry["kind"] == "pi.tool-result"
    assert result_entry["model"][0]["role"] == "toolResult"
    assert result_entry["model"][0]["toolCallId"] == "c1"
    # The run's two generations, the tool round and the answer, hand the run over.
    assert len({state["run"]["taskId"] for state in states if state.get("run")}) == 2
    await harness.close(context)


async def test_shows_queued_inputs_in_the_inbox_a_retry_backoff_and_the_next_run() -> None:
    setup = chat_setup()
    release = deferred()

    async def held(_request: Any, _options: Any) -> Any:
        await release.promise
        return assistant_message([faux_text("first")])

    error = assistant_message((), stop_reason="error", error_message="503 Service Unavailable")
    setup.faux.set_responses([held, error, assistant_message([faux_text("second")])])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    setup.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    history = await view_history(harness, root.id)
    harness.resume()
    first = await root.submit({"type": "input", "content": "a"}, context)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    await drained()
    # The queued follow-up shows in the mounted inbox document, with the mode it was queued under.
    assert _inbox(history["views"][-1]) == [(follow_up.id, "followUp")]
    release.resolve()
    await follow_up.wait(context)
    await drained()
    history["detach"]()
    states = history["live"]()
    # The retry backoff shows in the generation, and the retried attempt runs.
    assert any(
        (state.get("generation") or {}).get("retry", {}).get("error") == "503 Service Unavailable"
        for state in states
    )
    retried = next(
        index
        for index, state in enumerate(states)
        if (state.get("generation") or {}).get("retry") is not None
    )
    assert any(
        (state.get("generation") or {}).get("attempt") == 2 for state in states[retried:]
    )
    runs = _runs(states)
    assert [first.id, follow_up.id] == [runs[0][0], runs[-1][-1]]
    assert len(runs) == 2
    assert _inbox(history["views"][-1]) == []
    await harness.close(context)


def _inbox(view: Any) -> list[tuple[Any, str]]:
    return [(item["id"], item["mode"]) for item in (view["docs"].get("pi.inbox") or {}).get("items") or ()]


def _runs(states: list[dict[str, Any]]) -> list[list[Any]]:
    """The inputs of each run the values show, in order, one entry per run."""

    runs: list[list[Any]] = []
    for index, state in enumerate(states):
        run = state.get("run")
        if run is None:
            continue
        previous = (states[index - 1].get("run") or {}).get("inputs") if index else None
        if run["inputs"] != previous:
            runs.append(list(run["inputs"]))
    return runs


async def test_ends_a_run_and_starts_the_queued_follow_up_in_one_value() -> None:
    setup = chat_setup()
    release = deferred()

    async def held(_request: Any, _options: Any) -> Any:
        await release.promise
        return assistant_message([faux_text("first")])

    setup.faux.set_responses([held, assistant_message([faux_text("second")])])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    input_submission = await root.submit({"type": "input", "content": "a"}, context)
    follow_up = await root.submit({"type": "input", "content": "f"}, context)
    history = await view_history(harness, root.id)
    release.resolve()
    await follow_up.wait(context)
    await drained()
    history["detach"]()
    states = history["live"]()
    # One value carries the handover: the answer and the follow-up's entry appended, the run of the
    # first input gone, the follow-up's begun, and the inbox emptied.
    boundary = next(
        index
        for index, state in enumerate(states)
        if (state.get("run") or {}).get("inputs") == [follow_up.id]
    )
    assert (states[boundary - 1].get("run") or {}).get("inputs") == [input_submission.id]
    before, after = history["views"][boundary - 1], history["views"][boundary]
    appended = after["entries"][len(before["entries"]) :]
    assert [(entry["kind"], text_of(entry["model"][0])) for entry in appended] == [
        ("pi.assistant", "first"),
        ("pi.user", "f"),
    ]
    assert _inbox(after) == []
    await harness.close(context)


async def test_keeps_a_steer_in_the_current_run_shows_nothing_for_other_conversations() -> None:
    setup = chat_setup()
    gate = deferred()

    async def hold(_args: Any, _api: Any, _context: Any) -> dict[str, Any]:
        await gate.promise
        return {}

    add_tool(setup.registry, tool("hold", hold))
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "hold", "arguments": {}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    other = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    history = await view_history(harness, root.id)
    input_submission = await root.submit({"type": "input", "content": "a"}, context)
    await eventually(lambda: _running(history["live"](), "c1"), timeout=5)
    steer = await root.submit({"type": "input", "content": "s", "when_busy": "steer"}, context)
    await drained()
    frames = len(history["views"])
    await other.commit(lambda tx: tx.append_entry(other.id, {"kind": "note"}), context)
    await drained()
    # A commit of another conversation reaches no value of this one.
    assert len(history["views"]) == frames
    gate.resolve()
    await input_submission.wait(context)
    await root.reset(None, context)
    await drained()
    history["detach"]()
    states = history["live"]()
    # One run: the steer joined it rather than starting another.
    runs = {tuple(state["run"]["inputs"]) for state in states if state.get("run")}
    assert (input_submission.id,) in runs
    assert any(steer.id in inputs for inputs in runs)
    # The reset is appended in place, with no run.
    assert history["views"][-1]["entries"][-1]["kind"] == "pi.reset"
    assert states[-1] == {}
    await harness.close(context)


async def test_converts_a_streamed_partial_into_one_aborted_answer_entry() -> None:
    setup = chat_setup()
    setup.faux.set_responses(
        [slow_message(assistant_message([faux_text("x" * 400)]), chunks=20, interval=0.01)]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    history = await view_history(harness, root.id)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "hi"}, context)
    await eventually(
        lambda: any(
            (state.get("generation") or {}).get("message") is not None
            for state in history["live"]()
        ),
        timeout=5,
    )
    live = await harness.snapshot(LiveDoc, root.id, context)
    await harness.abort_task(live["run"]["taskId"], context)
    await submission.wait(context)
    await drained()
    history["detach"]()
    states = history["live"]()
    # The partial the view showed is converted, once, into the answer entry the abort commits.
    answers = [
        entry
        for view in history["views"]
        for entry in view["entries"]
        if entry["kind"] == "pi.assistant"
    ]
    assert len(answers) == 1
    assert answers[0]["model"][0]["role"] == "assistant"
    assert answers[0]["model"][0]["stopReason"] == "aborted"
    # Upstream's own tail assertion for this case (harness-view-run.test.ts:436): the run that ended
    # leaves its reason where a renderer reads it, in the value that shows the run gone.
    ended = next(
        index
        for index, state in enumerate(states)
        if index > 0 and state.get("run") is None
    )
    failure = states[ended].get("runFailure")
    assert failure is not None
    assert failure["reason"] == "aborted"
    await harness.close(context)


async def test_ends_a_call_that_never_runs_and_one_aborted_with_its_generation() -> None:
    setup = chat_setup()

    async def wait(_args: Any, _api: Any, call_context: Any) -> Any:
        await aborted(call_context.abort_signal)

    add_tool(setup.registry, tool("wait", wait))
    setup.faux.set_responses(
        [
            assistant_message(
                [
                    {"type": "toolCall", "id": "c1", "name": "ghost", "arguments": {}},
                    {"type": "toolCall", "id": "c2", "name": "wait", "arguments": {}},
                ],
                stop_reason="toolUse",
            )
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    history = await view_history(harness, root.id)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await eventually(lambda: _running(history["live"](), "c2"), timeout=5)
    live = await harness.snapshot(LiveDoc, root.id, context)
    await harness.abort_task(live["run"]["taskId"], context)
    await submission.wait(context)
    await drained()
    history["detach"]()
    states = history["live"]()
    # The call the request never offered is listed with its own task, beside the answer that named it,
    # and ends done with the result entry its task appended.
    ghost = slots_of(states, "c1")
    listed = next(index for index, slot in enumerate(ghost) if slot is not None)
    listed_ghost = ghost[listed]
    assert listed_ghost is not None
    assert listed_ghost["unoffered"] is True
    assert isinstance(listed_ghost["taskId"], int)
    assert history["views"][listed]["entries"][-1]["kind"] == "pi.assistant"
    ended = next(index for index, slot in enumerate(ghost) if (slot or {}).get("status") == "done")
    ended_ghost = ghost[ended]
    assert ended_ghost is not None
    assert ended_ghost["entry"] == history["views"][ended]["entries"][-1]["id"]
    assert history["views"][ended]["entries"][-1]["kind"] == "pi.tool-result"
    # The aborted tool ends done with its own aborted result entry.
    wait_slots = slots_of(states, "c2")
    assert any(slot is not None and slot["status"] == "running" for slot in wait_slots)
    done = next(index for index, slot in enumerate(wait_slots) if (slot or {}).get("status") == "done")
    done_wait = wait_slots[done]
    assert done_wait is not None
    assert done_wait["entry"] == history["views"][done]["entries"][-1]["id"]
    assert history["views"][done]["entries"][-1]["model"][0]["role"] == "toolResult"
    # Upstream's own tail assertion for this case (harness-view-run.test.ts:372): the newest state
    # keeps the failure of the run that never answered, naming the inputs it was answering.
    failure = states[-1].get("runFailure")
    assert failure is not None
    assert failure["reason"] == "aborted"
    assert failure["inputs"] == [submission.id]
    await harness.close(context)


MESSAGE_PATH = ["docs", "pi.live", "generation", "message"]
"""Where the answer being generated lives in the viewed document."""


def _message_ops(frames: list[dict[str, Any]]) -> list[tuple[str, list[Any], Any]]:
    """Every operation the frames made on the in-flight message, each path relative to the message.

    Upstream's ``onMessage`` (`harness-view-run.test.ts:529-536`): a root replacement carries no
    path, and an operation elsewhere in the document is not this stream's.
    """

    ops: list[tuple[str, list[Any], Any]] = []
    for frame in frames:
        for op in frame["ops"]:
            if op[0] == "r":
                ops.append(("r", [], None))
                continue
            path = list(op[1])
            if path[: len(MESSAGE_PATH)] == MESSAGE_PATH:
                ops.append((op[0], path[len(MESSAGE_PATH) :], op[2] if len(op) > 2 else None))
    return ops


def _token_stream(message: dict[str, Any], *, piece: int, interval: float) -> Step:
    """A stream that grows each block ``piece`` characters at a time, as upstream's faux does per token.

    Upstream's faux answers at ``tokensPerSecond`` with a ``tokenSize`` (``chat-support.ts``); the
    port's faux has no token clock, so the case hands it the piece size and the pause between events
    -- the same stand-in ``slow_message`` uses. Each event carries the partial as far as it has
    streamed and the piece that arrived as ``delta``, which is what upstream's events carry even
    though this tree's stream reads only the partial (``harness/generation.py``'s ``stream_response``).

    A tool call's arguments grow member by member, parsed as far as their text has reached, which is
    what upstream's ``StreamingJsonParser`` hands durable per ``toolcall_delta``
    (``harness/partial-changes.ts:64-84``). The port's own adapters hold a call's arguments at ``{}``
    until its whole text parses (the deviation ``PARITY.md`` records as "Streaming tool-call
    arguments"), so this is the shape a provider that reports what has parsed so far gives durable;
    what the cases below pin is durable's half -- that such a call is committed as appends at its
    argument paths, and that no commit carries the raw argument text.
    """

    def build(_request: Any, _options: Any) -> AssistantStream:
        async def events() -> Any:
            base = cast(dict[str, Any], copy_json(message))
            base["content"] = []
            base["stopReason"] = "pending"
            yield {"type": "start", "partial": base}
            blocks: list[dict[str, Any]] = []
            for block in message["content"]:
                if block["type"] == "thinking":
                    blocks.append({"type": "thinking", "thinking": ""})
                elif block["type"] == "text":
                    blocks.append({"type": "text", "text": ""})
                else:
                    blocks.append(
                        {
                            "type": "toolCall",
                            "id": block["id"],
                            "name": block["name"],
                            "arguments": {},
                        }
                    )
            partial = cast(dict[str, Any], copy_json(base))
            partial["content"] = blocks
            for index, block in enumerate(message["content"]):
                if block["type"] == "toolCall":
                    arguments = cast(dict[str, Any], block["arguments"])
                    for name in list(arguments):
                        value = str(arguments[name])
                        for end in range(piece, len(value) + piece, piece):
                            grown = value[:end]
                            blocks[index]["arguments"] = {
                                **blocks[index]["arguments"],
                                name: grown,
                            }
                            yield {
                                "type": "toolcall_delta",
                                "delta": value[max(0, end - piece) : end],
                                "partial": copy_json(partial),
                            }
                            await asyncio.sleep(interval)
                        blocks[index]["arguments"] = {
                            **blocks[index]["arguments"],
                            name: copy_json(arguments[name]),
                        }
                    continue
                field = "text" if block["type"] == "text" else "thinking"
                value = str(block[field])
                kind = "text_delta" if field == "text" else "thinking_delta"
                for end in range(piece, len(value) + piece, piece):
                    blocks[index][field] = value[:end]
                    yield {
                        "type": kind,
                        "delta": value[max(0, end - piece) : end],
                        "partial": copy_json(partial),
                    }
                    await asyncio.sleep(interval)
                blocks[index][field] = value

        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        future.set_result(message)
        return AssistantStream(events(), future)

    return Step(build)


def _running(states: list[dict[str, Any]], call_id: str) -> bool:
    """Whether any value showed call ``call_id`` running, model-issued or nested."""

    return any(
        slot is not None and slot["status"] == "running" for slot in slots_of(states, call_id)
    )


async def test_rebuilds_a_sliding_tail_window_from_output_trims_and_appends() -> None:
    setup = chat_setup()
    gate = deferred()

    async def execute(_args: Any, api: Any, _context: Any) -> dict[str, Any]:
        for line in range(6):
            api.output(f"line {line}\n")
            await asyncio.sleep(0.02)
        await gate.promise
        return {}

    add_tool(
        setup.registry,
        tool("tail", execute, output_limits={"max_lines": 3, "retain": "tail"}),
    )
    setup.faux.set_responses(
        [
            assistant_message(
                [{"type": "toolCall", "id": "c1", "name": "tail", "arguments": {}}],
                stop_reason="toolUse",
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    outputs: list[str] = []

    def observer(publication: Any, _context: Any) -> None:
        for change in document_changes(publication):
            slots = (change["value"] or {}).get("tools") if isinstance(change["value"], dict) else None
            output = slots[0].get("output") if slots else None
            if output is not None and output != (outputs[-1] if outputs else None):
                outputs.append(output)

    observe_commits(harness, observer)
    history = await view_history(harness, root.id)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await eventually(lambda: bool(outputs) and outputs[-1].endswith("line 5\n"), timeout=5)
    gate.resolve()
    await submission.wait(context)
    await drained()
    history["detach"]()
    # What the view shows of the window is what Storage stored of it, in order.
    shown: list[str] = []
    for frame in history["frames"]:
        slots = live_of(frame["value"]).get("tools") or []
        output = slots[0].get("output") if slots else None
        if output is not None and output != (shown[-1] if shown else None):
            shown.append(output)
    assert shown == outputs
    # The window slides by a front trim: the operations carry it, not a rewrite of the whole string.
    assert any(op[0] == "t" for frame in history["frames"] for op in frame["ops"])
    await harness.close(context)


async def test_rebuilds_thinking_text_and_tool_call_arguments_from_the_views_operations() -> None:
    setup = chat_setup()
    message = assistant_message(
        [
            {"type": "thinking", "thinking": "thinking about it " * 10},
            {"type": "text", "text": "some text " * 10},
            {
                "type": "toolCall",
                "id": "c1",
                "name": "missing",
                "arguments": {"path": "a/long/path/" * 10, "note": "x" * 60},
            },
        ],
        stop_reason="toolUse",
    )
    setup.settings["progress"] = {"partial_interval_ms": 0}
    setup.faux.set_responses(
        [_token_stream(message, piece=10, interval=0.005), assistant_message([faux_text("done")])]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    partials = partials_of(harness)
    history = await view_history(harness, root.id)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await drained()
    history["detach"]()
    # Replaying the operations alone, from the value the observer attached at, gives every committed
    # partial: thinking, text, and the tool-call arguments assembled as they streamed.
    assert replay(history["frames"]) == history["views"][-1]
    assert len(partials) > 2
    assert [
        partial["content"] for partial in partials_in(history["live"]())
    ] == [partial["content"] for partial in partials]
    # Growing text and thinking travel as appends, not as rewrites of the whole block.
    appends = [
        op
        for frame in history["frames"]
        for op in frame["ops"]
        if op[0] == "a" and op[1][:4] == ["docs", "pi.live", "generation", "message"]
    ]
    assert len(appends) > 2
    await harness.close(context)


async def test_applies_later_operations_to_an_overflow_replacement_holding_a_partial() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    partial = assistant_message([faux_text("hel")])
    watch = await root.watch(context, {"min": 1, "max": 1})

    async def change(edit: Any) -> None:
        async def apply(tx: Any) -> None:
            await edit(await tx.doc(LiveDoc, root.id))

        await root.commit(apply, context)

    async def start_partial(live: Any) -> None:
        live["generation"] = {"attempt": 1, "message": copy_json(partial)}

    await change(start_partial)
    # Past the pending-operation bound: the next delivery is one root replacement.
    for _index in range(4000):
        await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    frames: list[dict[str, Any]] = []

    async def listener(call: Any, _context: Any) -> None:
        if call["kind"] == "value":
            frames.append({"value": call["value"], "ops": list(call["ops"])})

    watch.start(listener)
    await drained()
    assert frames[0]["ops"] == [["r", frames[0]["value"]]]
    async def extend(live: Any) -> None:
        block = live["generation"]["message"]["content"][0]
        block["text"] = block["text"] + "lo"

    await change(extend)
    await drained()
    update = frames[-1]
    assert update["ops"] == [
        ["a", ["docs", "pi.live", "generation", "message", "content", 0, "text"], "lo"]
    ]
    rebuilt = apply_immutable(frames[0]["value"], update["ops"])
    assert (live_of(rebuilt).get("generation") or {})["message"]["content"] == [
        {"type": "text", "text": "hello"}
    ]
    await watch.stop()
    await harness.close(context)


async def test_streams_a_tool_call_partial_as_appends_at_its_argument_paths() -> None:
    """Upstream: "streams a tool-call partial as appends at its argument paths".

    The committed partial is written as appends at each growing argument, and never carries the raw
    argument text beside the parsed arguments.
    """

    setup = chat_setup()
    setup.settings["progress"] = {"partial_interval_ms": 20, "output_interval_ms": 100}
    args = {"path": "src/file.ts", "content": "const a = 1;\n" * 150}
    setup.faux.set_responses(
        [
            _token_stream(
                assistant_message(
                    [faux_tool_call("missing", args, id="c1")], stop_reason="toolUse"
                ),
                piece=4,
                interval=0.002,
            ),
            assistant_message([faux_text("done")]),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    partials = partials_of(harness)
    history = await view_history(harness, root.id)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await drained()
    history["detach"]()
    on_message = _message_ops(history["frames"])
    argument_appends = [
        op for op in on_message if op[0] == "a" and len(op[1]) > 2 and op[1][2] == "arguments"
    ]
    assert len(argument_appends) > 10
    assert all(str(op[1][-1]) in ("path", "content") for op in argument_appends)
    # The call is set whole only when a member appears (at most once per argument); growing strings
    # are appends. With the raw argument text on the block, every commit resent the whole call.
    block_sets = [op for op in on_message if op[0] != "a" and len(op[1]) == 2]
    assert len(block_sets) <= 2
    # The committed partials carry only the parsed arguments, never the raw argument text.
    assert partials
    for partial in partials:
        for block in partial["content"]:
            assert sorted(block) == ["arguments", "id", "name", "type"]
    await harness.close(context)


async def test_commits_a_growing_text_at_a_cost_that_follows_the_new_text_not_the_string_lengths() -> None:
    """Upstream: "commits a growing text and tool call at a cost that follows the new text, not the
    string lengths". The cost of committing a growing answer follows the text it adds.

    Upstream measures this with V8's heap (``GCProfiler`` and ``used_heap_size``,
    `harness-view-run.test.ts:476-492`): reassigning a growing string made every commit allocate and
    carry the whole thing. This tree cannot take that measure -- ``tracemalloc`` is disabled on the
    free-threaded builds of the verified set, and a commit allocates its own share of plain Python
    strings however the operations read -- so it weighs what the commits carry instead: the bytes of
    the operations each one writes. A commit that resent the whole string would grow its
    bytes with the answer exactly as upstream's heap did, which is what makes this bound red. The
    two answers are a tenth of upstream's 40 KB and 400 KB -- the 10x scale the case argues from is
    what matters, and each commit costs this port more than it costs V8.
    """

    async def stream(size: int) -> dict[str, float]:
        setup = chat_setup()
        setup.settings["progress"] = {"partial_interval_ms": 1, "output_interval_ms": 100}
        # Keep `pi.live` cached between commits, as the default retention does. Evicted after each
        # use, every commit replays the document's delta chain: a cost of eviction, not of appending.
        setup.settings["document_retention_ms"] = 60_000
        text = 'streamed text with "quotes" and \\ escapes\n' * (size // 40)
        setup.faux.set_responses(
            [
                _token_stream(
                    assistant_message(
                        [
                            faux_text(text),
                            faux_tool_call("missing", {"path": "a.ts", "content": text}, id="c1"),
                        ],
                        stop_reason="toolUse",
                    ),
                    piece=64,
                    interval=0.0003,
                ),
                assistant_message([faux_text("done")]),
            ]
        )
        opened = await open_chat(MemoryStorage(), setup)
        harness, root = opened["harness"], opened["root"]
        history = await view_history(harness, root.id)
        harness.resume()
        await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
        await drained()
        history["detach"]()
        await harness.close(context)
        commits = 0
        written = 0
        appends = 0
        for frame in history["frames"]:
            ops = _message_ops([frame])
            if not ops:
                continue
            commits += 1
            written += sum(len(op[2]) for op in ops if isinstance(op[2], str))
            appends += sum(1 for op in ops if op[0] == "a")
        return {"commits": commits, "bytes": written, "appends": appends}

    small = await stream(10_000)
    large = await stream(100_000)
    # The answer arrives as appends, most commit carrying one: a commit that rewrote the message
    # whole carries none, and no bound below would notice what it costs.
    assert small["appends"] > small["commits"] // 2 and large["appends"] > large["commits"] // 2
    assert large["commits"] > small["commits"] * 5
    # Per commit, the cost stays flat: comparing the whole strings on every commit made it grow with
    # their length.
    per_small = small["bytes"] / small["commits"]
    per_large = large["bytes"] / large["commits"]
    assert per_large / per_small < 2


async def test_keeps_a_watchers_memory_proportional_to_a_long_streamed_text_not_to_its_commits() -> None:
    """Upstream: "keeps a watcher's memory proportional to a long streamed text and tool call, not to
    their commits". A watcher's operations follow a long streamed answer, not its commit count.

    Upstream weighs the watcher's heap: two 140 KB strings, over 500 commits, and 20 MB of slack for
    the 3 MB they cost (`harness-view-run.test.ts:581-618`); sliced append operations are what keeps
    it there, where a copy of the text per commit cost well over 100 MB. The port can weigh the
    operations a watcher holds -- the bytes each commit hands it stay proportional to the text -- but
    not the values it holds with them: CPython has no substring views over an immutable string, so
    applying an append builds a fresh copy of the whole text, and a watcher that keeps every frame
    keeps one copy per frame. ``PARITY.md`` records that half as a deviation; what follows is the
    half this tree does keep.
    """

    setup = chat_setup()
    setup.settings["progress"] = {"partial_interval_ms": 1, "output_interval_ms": 100}

    def line(index: int) -> str:
        return f'line {index}: some streamed source text with "quotes" and \\ escapes\n'

    text = "".join(line(index) for index in range(2_000))
    content = "".join(line(index) for index in range(2_000))
    setup.faux.set_responses(
        [
            _token_stream(
                assistant_message(
                    [
                        faux_text(text),
                        faux_tool_call("missing", {"path": "a.ts", "content": content}, id="c1"),
                    ],
                    stop_reason="toolUse",
                ),
                piece=32,
                interval=0.0002,
            ),
            assistant_message([faux_text("done")]),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    history = await view_history(harness, root.id)
    harness.resume()
    submission = await root.submit({"type": "input", "content": "go"}, context)
    await submission.wait(context)
    await drained()
    history["detach"]()

    commits = 0
    operations: list[tuple[str, list[Any], Any]] = []
    for frame in history["frames"]:
        ops = _message_ops([frame])
        if ops:
            commits += 1
            operations.extend(ops)
    assert commits > 500
    appends = [op for op in operations if op[0] == "a" and isinstance(op[2], str)]
    written = sum(len(op[2]) for op in appends)
    # Every byte of the two strings reaches the watcher once, as appends: nothing of the answer
    # arrives any other way, and a copy of the text per commit would exceed this by the number of
    # commits.
    assert written >= (len(text) + len(content)) * 0.9
    assert written <= 4 * (len(text) + len(content))
    # And no single commit re-sends either string whole.
    assert max(len(op[2]) for op in appends) < (len(text) + len(content)) // 10
    await harness.close(context)


async def test_a_commit_that_changes_only_a_submission_record_leaves_the_view_alone() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    history = await view_history(harness, root.id)

    async def stale(tx: Any) -> dict[str, Any]:
        return await tx.create_submission(
            {
                "conversationId": root.id,
                "type": "write",
                "status": "unanswered",
                "reason": "stale",
                "entry": {"kind": "note"},
            }
        )

    record = await root.commit(stale, context)
    await drained()
    # A submission is not mounted: its commit publishes no view frame, and its record is still the
    # record Storage stored, readable by ID.
    assert len(history["views"]) == 1
    submission = await harness.submission(record["id"], context)
    assert (await submission.status(context))["status"] == "unanswered"
    assert (await submission.status(context))["reason"] == "stale"
    history["detach"]()
    await harness.close(context)


async def test_shows_every_change_to_the_live_document_as_one_value_per_commit() -> None:
    opened = await open_chat(MemoryStorage(), chat_setup())
    harness, root = opened["harness"], opened["root"]
    history = await view_history(harness, root.id)
    partial = assistant_message([faux_text("partial")])

    async def change(edit: Any) -> None:
        async def apply(tx: Any) -> None:
            await edit(await tx.doc(LiveDoc, root.id))

        await root.commit(apply, context)

    async def set_message(live: Any) -> None:
        live["generation"] = {"attempt": 1, "message": copy_json(partial)}

    await change(set_message)

    async def set_usage(live: Any) -> None:
        live["generation"]["message"]["usage"]["input"] = 42

    await change(set_usage)

    async def set_deferred(live: Any) -> None:
        live["generation"] = {"attempt": 1, "deferred": {"pollAt": 1}}

    await change(set_deferred)

    async def move_poll(live: Any) -> None:
        live["generation"]["deferred"]["pollAt"] = 2

    await change(move_poll)

    async def start_tool(live: Any) -> None:
        live["tools"] = [
            {
                "callId": "c1",
                "name": "t",
                "status": "running",
                "details": {"n": 1},
                "diagnostics": [],
            }
        ]

    await change(start_tool)

    async def clear_progress(live: Any) -> None:
        del live["tools"][0]["details"]
        del live["tools"][0]["diagnostics"]

    await change(clear_progress)

    async def finish_tool(live: Any) -> None:
        live["tools"][0]["status"] = "done"

    await change(finish_tool)
    await root.commit(lambda tx: tx.retire_doc(UsageDoc, root.id), context)
    await root.commit(lambda tx: tx.retire_doc(AgentDoc, root.id), context)
    await drained()
    history["detach"]()
    # One value per touching commit, each showing exactly what that commit stored of `pi.live`.
    assert len(history["frames"]) == 10
    replay(history["frames"])
    assert [
        (
            (state.get("generation") or {}).get("message", {}).get("usage", {}).get("input"),
            (state.get("generation") or {}).get("deferred", {}).get("pollAt"),
            [
                (slot["status"], "details" in slot, "diagnostics" in slot)
                for slot in state.get("tools") or ()
            ],
        )
        for state in history["live"]()
    ] == [
        (None, None, []),
        (0, None, []),  # the partial appears, with the zeroed usage a message carries
        (42, None, []),  # a usage-only change still reaches the consumer
        (None, 1, []),
        (None, 2, []),
        (None, 2, [("running", True, True)]),
        (None, 2, [("running", False, False)]),  # cleared progress reads as absent
        (None, 2, [("done", False, False)]),
        (None, 2, [("done", False, False)]),
        (None, 2, [("done", False, False)]),
    ]
    # A retired document leaves the mount: the commit that retires it is a change of its own, and a
    # consumer reads a retired document as its initial value (test_harness_view.py's retirement case).
    assert "pi.usage" not in history["views"][-1]["docs"]
    assert "pi.agent" not in history["views"][-1]["docs"]
    await harness.close(context)

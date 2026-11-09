"""Deferred progress commits (``PROGRESS_COMMIT``): the partial answer and running tool output reach
watches at once and Storage at the next commit that reads or writes ``pi.live``, or within
``settings.progress.persist_within_ms``.

Translated case for case from upstream ``test/harness-deferred-progress.test.ts``. Crashes are
simulated by failing the Harness: from that moment nothing more reaches Storage, as when the process
dies, and the reopened Harness resumes from what was stored.

All 11 cases of upstream's file are here, each naming the upstream title it translates. The
two audiences this ticket rests on are pinned from both sides: the Session's own follower is handed
every partial as memory adopts it (`memory_partials`), and the host's ``observe_commits()`` observer
is handed nothing at all for a partial that has been adopted and not yet stored
(:func:`test_hands_the_host_commit_observer_nothing_for_a_partial_that_was_only_adopted`), which is
the direction most easily lost by a later change that publishes a held revision twice.

Two cases are the port's own, because upstream has none where the tool side asks to be deferred: a
running call's ``details()`` value riding the held commit, and a tool's own ``api.commit`` honouring
the persistence mark it was handed. Both are named at the case and on the 7.3 row of ``PARITY.md``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, cast

from chat_support import (
    all_entries,
    assistant_message,
    chat_setup,
    faux_text,
    open_chat,
    slow_message,
    text_of,
    usage,
)
from session_support import ControlledStorage, context, eventually, observe_commits, observe_commits_as_host
from task_support import aborted, deferred
from tool_support import DONE, add_task, add_tool, calls, tool

from pi_durable import AssistantEntry, DocDefinition, LiveDoc, UsageDoc
from pi_durable.delta import apply_immutable
from pi_durable.harness.live import PROGRESS_COMMIT
from pi_durable.tasks import define_task
from pi_durable.types import ConversationId, JsonObject


def slow_setup(persist_within_ms: float) -> Any:
    """A slow stream -- 4 characters at a time, 80 times a second -- and a store every `persist_within_ms`.

    Upstream drives the rate through the faux provider's ``tokensPerSecond``; this tree paces a stream
    through :func:`slow_message`, so the same 2 s of streaming is the same number of text deltas.
    """

    setup = chat_setup()
    setup.settings["progress"] = {"persist_within_ms": persist_within_ms}
    # A request a crash interrupted is a failed attempt (#11139): retry at once.
    setup.settings["retry"] = {"enabled": True, "max_retries": 5, "base_delay_ms": 1}
    return setup


def stores_of(batches: Sequence[Sequence[Any]]) -> list[Sequence[Any]]:
    """The storage calls that wrote a document: what a held revision rides to disk on."""

    return [batch for batch in batches if any(write["type"] == "document.change" for write in batch)]


LONG = "streamed " * 400


def streamed(text: str, *, chunks: int = 40, interval: float = 0.05) -> Any:
    """The queued answer, streamed in enough deltas for the throttle to drop some of them.

    A case that acts while the answer is still streaming passes a slower pace: upstream's faux streams
    at 80 tokens a second, which leaves seconds of headroom, and the pace here is what buys the same
    headroom.
    """

    return slow_message(assistant_message(text), chunks=chunks, interval=interval)


async def open(storage: ControlledStorage, setup: Any) -> dict[str, Any]:
    """Open (or reopen) a Harness over `storage` and let it run tasks."""

    opened = await open_chat(storage.reopen(), setup)
    opened["harness"].resume()
    opened["memory"] = memory_partials(opened["harness"])
    return opened


def memory_partials(harness: Any) -> list[str]:
    """The partial text of every ``pi.live`` revision memory adopted, the last one last."""

    texts: list[str] = []

    def observe(publication: Any, _context: Any) -> None:
        for change in publication["changes"]:
            if change["type"] != "document" or change["kind"] != "change":
                continue
            if change["record"]["kind"] != LiveDoc.definition["kind"]:
                continue
            message = generation_of(cast("JsonObject | None", change["value"])).get("message")
            if message is not None:
                texts.append(partial_text(message) or "")

    observe_commits(harness, observe)
    return texts


async def crash(harness: Any) -> None:
    """Fail the Harness now: nothing more is stored, held changes are lost with it."""

    harness.fail(RuntimeError("crash"))
    await harness.closed


async def stored_doc(
    storage: ControlledStorage, conversation_id: ConversationId, definition: DocDefinition
) -> JsonObject | None:
    """One document's stored value, read past the Session, also after a crash closed it."""

    storage.reopen()
    address = {
        "kind": definition["kind"],
        "scope": {"kind": "conversation", "conversationId": conversation_id},
    }
    record = await storage.find_document(cast(Any, address), "current", context)
    if record is None:
        return None
    found = await storage.document(record["id"], "current", context)
    return None if found is None else found["value"]


async def stored_live(storage: ControlledStorage, conversation_id: ConversationId) -> JsonObject | None:
    """``pi.live`` as Storage has it."""

    return await stored_doc(storage, conversation_id, LiveDoc.definition)


async def stored_usage(storage: ControlledStorage, conversation_id: ConversationId) -> JsonObject | None:
    """``pi.usage`` as Storage has it, which is the only way to read it after a crash."""

    return await stored_doc(storage, conversation_id, UsageDoc.definition)


def generation_of(live: JsonObject | None) -> dict[str, Any]:
    """The ``generation`` box of a ``pi.live`` value, which Storage hands untyped."""

    box = (live or {}).get("generation")
    return {} if not isinstance(box, dict) else cast(dict[str, Any], box)


def partial_text(message: Any) -> str | None:
    """The text of a partial message's first text block."""

    for block in (message or {}).get("content") or ():
        if block.get("type") == "text":
            return cast(str, block.get("text"))
    return None


def stored_text(live: JsonObject | None) -> str | None:
    message = generation_of(live).get("message")
    return None if message is None else (partial_text(message) or "")


def answers(entries: Any) -> list[dict[str, Any]]:
    """Assistant entries: their text and stop reason."""

    found = []
    for entry in entries:
        if entry["kind"] != "pi.assistant":
            continue
        message = entry["model"][0]
        found.append({"text": text_of(message), "stopReason": message.get("stopReason")})
    return found


async def test_holds_partials_in_memory_and_stores_them_together_at_most_every_persist_within() -> None:
    """Upstream: "hold partials in memory and store them together, at most every persistWithinMs"."""

    storage = ControlledStorage()
    setup = slow_setup(400)
    setup.faux.set_responses([streamed(LONG[:640])])
    opened = await open(storage, setup)
    from_index = len(storage.commits)
    await (await opened["root"].submit({"type": "input", "content": "hi"}, context)).wait(context)
    writes_live = stores_of(storage.commits[from_index:])
    # About 2 s of streaming at 100 ms per partial: some 20 partials in memory, a store about every 400 ms.
    assert len(opened["memory"]) > 12
    assert len(writes_live) < len(opened["memory"]) / 1.5
    await opened["harness"].close(context)


async def test_converts_what_was_stored_into_the_aborted_entry_after_a_crash() -> None:
    """Upstream: "converts what was stored into the aborted entry after a crash, and retries the
    charged attempt"."""

    storage = ControlledStorage()
    setup = slow_setup(150)
    setup.faux.set_responses([streamed(LONG)])
    opened = await open(storage, setup)
    # A replica fed by observe_commits() alone, as a replication feed would be.
    replica: Any = await stored_live(storage, opened["root"].id)

    def follow(publication: Any, _context: Any) -> None:
        nonlocal replica
        for change in publication["changes"]:
            if (
                change["type"] == "document"
                and change["kind"] == "change"
                and change["record"]["kind"] == LiveDoc.definition["kind"]
            ):
                replica = apply_immutable(replica, change["ops"])

    observe_commits_as_host(opened["harness"], follow)
    submission = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def stored_some() -> bool:
        text = stored_text(await stored_live(storage, opened["root"].id))
        return text is not None and len(text) > 40

    await eventually(stored_some)
    in_memory = opened["memory"][-1]
    await crash(opened["harness"])
    stored = stored_text(await stored_live(storage, opened["root"].id))
    # What Storage has is a state memory had, behind it by at most the window, and what the replica has.
    assert stored in opened["memory"]
    assert in_memory.startswith(cast(str, stored))
    assert replica == await stored_live(storage, opened["root"].id)

    setup.faux.set_responses([assistant_message("answer")])
    opened = await open(storage, setup)
    settled = await (await opened["harness"].submission(submission, context)).wait(context)
    assert settled["status"] == "done"
    assert answers(await all_entries(opened["root"])) == [
        {"text": stored, "stopReason": "aborted"},
        {"text": "answer", "stopReason": "stop"},
    ]
    assert await opened["harness"].snapshot(LiveDoc, opened["root"].id, context) == {}
    await opened["harness"].close(context)


async def test_leaves_no_aborted_entry_for_a_stream_that_crashed_before_its_first_store() -> None:
    """Upstream: "leaves no aborted entry for a stream that crashed before its first store".

    The port adds the second half of the clause upstream's case leaves out: such an attempt also
    records no usage. Usage is added in the commit that appends the provider's result
    (`harness/generation.py:1679-1688`), and for a stream that died before its first store there is no
    such commit. The reopened answer carries usage so the read below cannot pass by being vacuous.
    """

    storage = ControlledStorage()
    setup = slow_setup(float("inf"))
    setup.faux.set_responses([streamed(LONG)])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def streamed_some() -> bool:
        return len(opened["memory"][-1]) > 40 if opened["memory"] else False

    await eventually(streamed_some)
    await crash(opened["harness"])
    # The attempt is presentation, deferred like the partials: none of it was stored.
    assert generation_of(await stored_live(storage, opened["root"].id)) == {}
    assert stored_text(await stored_live(storage, opened["root"].id)) is None
    # And it spent nothing: usage rides the commit that appends the result, which never happened. The
    # ledger is there -- the conversation opens it -- but holds no bucket for the attempt that died.
    assert await stored_usage(storage, opened["root"].id) in (None, {"models": {}, "tools": {}})

    setup.faux.set_responses([assistant_message("answer", usage=usage(input=3, output=2))])
    opened = await open(storage, setup)
    settled = await (await opened["harness"].submission(submission, context)).wait(context)
    assert settled["status"] == "done"
    assert answers(await all_entries(opened["root"])) == [{"text": "answer", "stopReason": "stop"}]
    # The attempt that did store has its usage, which is what makes the read above a proof.
    assert (await opened["harness"].snapshot(UsageDoc, opened["root"].id, context))["models"][
        "faux/faux-1"
    ] == usage(input=3, output=2)
    await opened["harness"].close(context)


async def test_stores_everything_held_at_a_clean_close() -> None:
    """Upstream: "stores everything held at a clean close"."""

    storage = ControlledStorage()
    setup = slow_setup(float("inf"))
    setup.faux.set_responses([streamed(LONG)])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def streamed_some() -> bool:
        return len(opened["memory"][-1]) > 40 if opened["memory"] else False

    await eventually(streamed_some)
    await opened["harness"].close(context)
    at_close = opened["memory"][-1]
    assert stored_text(await stored_live(storage, opened["root"].id)) == at_close

    setup.faux.set_responses([assistant_message("answer")])
    opened = await open(storage, setup)
    await (await opened["harness"].submission(submission, context)).wait(context)
    assert answers(await all_entries(opened["root"]))[0] == {"text": at_close, "stopReason": "aborted"}
    await opened["harness"].close(context)


async def test_converts_a_stored_partial_once_through_two_crashes() -> None:
    """Upstream: "converts a stored partial once through two crashes, before and after the conversion"."""

    storage = ControlledStorage()
    setup = slow_setup(150)
    setup.faux.set_responses([streamed(LONG)])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def stored_some() -> bool:
        text = stored_text(await stored_live(storage, opened["root"].id))
        return text is not None and len(text) > 40

    await eventually(stored_some)
    await crash(opened["harness"])
    stored = stored_text(await stored_live(storage, opened["root"].id))

    # Crash again before anything ran: nothing was converted, so nothing is lost or doubled.
    opened = await open_chat(storage.reopen(), setup)
    opened["memory"] = []
    await crash(opened["harness"])
    assert stored_text(await stored_live(storage, opened["root"].id)) == stored

    # Crash again while the resend streams: its conversion is stored, its own partials only held.
    setup.settings["progress"] = {"persist_within_ms": float("inf")}
    setup.faux.set_responses([streamed(LONG)])
    opened = await open(storage, setup)

    async def streamed_some() -> bool:
        return len(opened["memory"][-1]) > 40 if opened["memory"] else False

    await eventually(streamed_some)
    await crash(opened["harness"])
    assert stored_text(await stored_live(storage, opened["root"].id)) is None

    setup.faux.set_responses([assistant_message("answer")])
    opened = await open(storage, setup)
    settled = await (await opened["harness"].submission(submission, context)).wait(context)
    assert settled["status"] == "done"
    assert answers(await all_entries(opened["root"])) == [
        {"text": stored, "stopReason": "aborted"},
        {"text": "answer", "stopReason": "stop"},
    ]
    await opened["harness"].close(context)


async def test_hands_the_host_commit_observer_nothing_for_a_partial_that_was_only_adopted() -> None:
    """No upstream counterpart: the direction of the two audiences that rots most easily.

    ``test_leaves_no_aborted_entry_for_a_stream_that_crashed_before_its_first_store`` shows the same
    fact through Storage; this one pins it on the observer seam itself, where a regression that
    published a held revision to the host as well as to memory would otherwise stay invisible. It reads
    the published *values* rather than the operations, because a held revision can reach Storage folded
    into another commit's base write, where no per-path operation names it.
    """

    storage = ControlledStorage()
    setup = slow_setup(float("inf"))
    setup.faux.set_responses([streamed(LONG)])
    opened = await open(storage, setup)
    stored: list[Any] = []
    observe_commits_as_host(
        opened["harness"],
        lambda publication, _context: stored.append(publication),
    )

    def stored_partials() -> list[str]:
        """The partial text of every ``pi.live`` value the host's observer was handed."""

        texts = []
        for publication in stored:
            for change in publication["changes"]:
                if (
                    change["type"] == "document"
                    and change["kind"] == "change"
                    and change["record"]["kind"] == LiveDoc.definition["kind"]
                ):
                    message = generation_of(cast("JsonObject | None", change["value"])).get("message")
                    if message is not None:
                        texts.append(partial_text(message) or "")
        return texts

    await opened["root"].submit({"type": "input", "content": "hi"}, context)

    async def streamed_some() -> bool:
        return len(opened["memory"]) > 4 and opened["memory"][-1].startswith("streamed")

    await eventually(streamed_some)
    # Memory was shown the answer as it streamed; the host was shown none of it, because Storage was
    # never told either of them.
    assert stored_partials() == []
    await opened["harness"].close(context)
    # Close is another trigger: only now does the held revision reach the host observer.
    assert stored_partials() == [opened["memory"][-1]]


def streaming_tool(replay: str) -> dict[str, Any]:
    """A tool that writes a line every 20 ms until its invocation is cancelled on its first run.

    A later run waits for cancellation while `stall` is set (`stalled` resolves then), and otherwise
    writes one line and returns.
    """

    lines: list[str] = []
    state: dict[str, Any] = {"runs": 0, "lines": lines, "stall": False, "stalled": deferred()}

    async def execute(_args: Any, api: Any, call_context: Any) -> Any:
        run = state["runs"] + 1
        state["runs"] = run
        signal = call_context.abort_signal
        if run > 1 and state["stall"]:
            state["stalled"].resolve()
            await aborted(signal)
        if run > 1:
            api.output(f"rerun {run}\n")
            return {}
        while not signal.aborted:
            line = len(state["lines"])
            api.output(f"line {line}\n")
            state["lines"].append(f"line {line}\n")
            await asyncio.sleep(0.02)
        await aborted(signal)
        return {}

    return {"registration": tool("work", execute, replay=replay), "state": state}


async def stored_output(storage: ControlledStorage, conversation_id: ConversationId) -> str | None:
    """The first tool slot's output as Storage has it."""

    live = await stored_live(storage, conversation_id)
    slots = cast("list[dict[str, Any]]", (live or {}).get("tools") or [])
    return None if not slots else cast("str | None", slots[0].get("output"))


def results(entries: Any) -> list[str]:
    """Tool-result entries: their text blocks joined as upstream's helper joins them."""

    found = []
    for entry in entries:
        if entry["kind"] != "pi.tool-result":
            continue
        message = entry["model"][0]
        if message.get("role") != "toolResult":
            found.append("")
            continue
        found.append("|".join(item.get("text", "") for item in message.get("content") or ()))
    return found


async def test_answers_a_tool_without_replay_that_crashed_with_the_output_stored_by_then() -> None:
    """Upstream: "answers a tool without replay that crashed with the output stored by then"."""

    storage = ControlledStorage()
    setup = slow_setup(150)
    streaming = streaming_tool("unsafe")
    add_tool(setup.registry, streaming["registration"])
    setup.faux.set_responses([calls(["work", {}, "c1"])])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id

    async def stored_some() -> bool:
        output = await stored_output(storage, opened["root"].id)
        return output is not None and len(output) > 20

    await eventually(stored_some)
    await crash(opened["harness"])
    stored = await stored_output(storage, opened["root"].id)
    assert "".join(streaming["state"]["lines"]).startswith(cast(str, stored))

    setup.faux.set_responses([assistant_message([faux_text("done")])])
    opened = await open(storage, setup)
    settled = await (await opened["harness"].submission(submission, context)).wait(context)
    assert settled["status"] == "done"
    assert streaming["state"]["runs"] == 1
    assert results(await all_entries(opened["root"])) == [
        f"{stored}|<harness>\n[error] Tool work was interrupted and may have partially run\n</harness>"
    ]
    await opened["harness"].close(context)


async def test_reruns_a_replay_safe_tool_after_a_crash_with_its_stored_progress_cleared() -> None:
    """Upstream: "reruns a replay-safe tool after a crash with its stored progress cleared, also through
    a second crash"."""

    storage = ControlledStorage()
    setup = slow_setup(150)
    streaming = streaming_tool("safe")
    add_tool(setup.registry, streaming["registration"])
    setup.faux.set_responses([calls(["work", {}, "c1"])])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id

    async def stored_some() -> bool:
        output = await stored_output(storage, opened["root"].id)
        return output is not None and len(output) > 20

    await eventually(stored_some)
    await crash(opened["harness"])

    # The rerun's first commit stores the cleared progress; it crashes again before writing any of its own.
    streaming["state"]["stall"] = True
    streaming["state"]["stalled"] = deferred()
    opened = await open(storage, setup)
    await streaming["state"]["stalled"].promise
    assert await stored_output(storage, opened["root"].id) is None
    await crash(opened["harness"])
    assert await stored_output(storage, opened["root"].id) is None

    streaming["state"]["stall"] = False
    setup.faux.set_responses([assistant_message([faux_text("done")])])
    opened = await open(storage, setup)
    settled = await (await opened["harness"].submission(submission, context)).wait(context)
    assert settled["status"] == "done"
    assert streaming["state"]["runs"] == 3
    assert results(await all_entries(opened["root"])) == ["rerun 3\n"]
    await opened["harness"].close(context)


async def test_resumes_polling_a_deferred_response_after_a_crash() -> None:
    """Upstream: "resumes polling a deferred response after a crash"."""

    storage = ControlledStorage()
    clock = {"now": 1_000}
    setup = chat_setup({"deferred": {"pollAfterMs": 60_000}})
    setup.now = lambda: clock["now"]
    setup.faux.set_responses([assistant_message("deferred answer")])
    opened = await open(storage, setup)
    setup.settings["stream"] = {"deferred": True}
    submission = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def polling() -> bool:
        live = await stored_live(storage, opened["root"].id)
        return "deferred" in generation_of(live)

    await eventually(polling)
    await crash(opened["harness"])
    opened = await open(storage, setup)
    clock["now"] = 61_000
    opened["harness"].resume()
    settled = await (await opened["harness"].submission(submission, context)).wait(context)
    assert settled["status"] == "done"
    assert settled["type"] == "input"
    answer = await opened["root"].commit(lambda tx: tx.entry(AssistantEntry, settled["answer"]), context)
    assert text_of(answer["model"][0]) == "deferred answer"
    assert setup.faux.deferred_fetch_count == 1
    await opened["harness"].close(context)


async def test_never_counts_progress_commits_as_progress_of_a_task_cut_off_again_and_again() -> None:
    """Upstream: "never counts progress commits as progress of a task cut off again and again" (#11139).

    A generation's request is charged at once after a crash, so its partials never stream with a count;
    a replay-safe tool reruns and streams its output with one.
    """

    storage = ControlledStorage()
    setup = slow_setup(50)
    runs = {"n": 0}
    api_seen: list[int] = []

    async def execute(_args: Any, api: Any, call_context: Any) -> Any:
        runs["n"] += 1
        run = runs["n"]
        signal = call_context.abort_signal
        while not signal.aborted:
            api.output(f"run {run} line {len(api_seen)}\n")
            api_seen.append(run)
            await asyncio.sleep(0.02)
        await aborted(signal)
        return {}

    add_tool(setup.registry, tool("work", execute, replay="safe"))
    setup.faux.set_responses([calls(["work", {}, "c1"])])
    opened = await open(storage, setup)
    await opened["root"].submit({"type": "input", "content": "go"}, context)
    for crashes in (1, 2):
        # Bound at definition: `opened` is rebound below, and a late-bound closure would watch the
        # harness of the next round.
        current = opened

        async def stored_this_run(crashes: int = crashes, current: dict[str, Any] = current) -> bool:
            output = await stored_output(storage, current["root"].id)
            return output is not None and output.startswith(f"run {crashes} ")

        await eventually(stored_this_run)
        # This run stored output of its own, a progress commit, before it was cut off.
        await crash(opened["harness"])
        opened = await open(storage, setup)
        tasks = await opened["harness"].commit(
            lambda tx: tx.scan_tasks({"kind": "pi.tool"}, 1), context
        )
        assert tasks["items"][0]["restarts"] == crashes
    await opened["harness"].close(context)


async def test_reruns_a_summarization_cut_off_by_a_crash_from_its_checkpoint() -> None:
    """Upstream: "reruns a summarization cut off by a crash from its checkpoint"."""

    storage = ControlledStorage()
    setup = chat_setup()
    setup.settings["compaction"] = {
        "enabled": False,
        "reserve_tokens": 1000,
        "keep_recent_tokens": 150,
        "background_tokens": 0,
    }
    summarizing = deferred()

    async def summarizing_step(_request: dict[str, Any], options: dict[str, Any]) -> Any:
        summarizing.resolve()
        await aborted(options["signal"])
        return {}

    setup.faux.set_responses(
        [assistant_message(f"{label} {'word ' * 100}") for label in ("a1", "a2", "a3")] + [summarizing_step]
    )
    opened = await open(storage, setup)
    for label in ("u1", "u2", "u3"):
        input = f"{label} {'word ' * 100}"
        await (await opened["root"].submit({"type": "input", "content": input}, context)).wait(context)
    task_id = await opened["root"].compact(None, context)
    await summarizing.promise
    await crash(opened["harness"])

    setup.faux.set_responses([assistant_message("SUMMARY")])
    opened = await open(storage, setup)
    settled_task = await opened["harness"].wait_for_task(task_id, context)
    assert settled_task["state"]["outcome"]["status"] == "completed"

    async def compacted() -> bool:
        return any(entry["kind"] == "pi.compaction" for entry in await all_entries(opened["root"]))

    await eventually(compacted)
    assert await opened["harness"].snapshot(LiveDoc, opened["root"].id, context) == {}
    await opened["harness"].close(context)


async def test_stores_the_held_partial_a_third_party_tasks_commit_read() -> None:
    """Upstream: "stores the held partial a third-party task's commit read, with what it derived"."""

    storage = ControlledStorage()
    setup = slow_setup(float("inf"))
    audit = define_task(
        {
            "name": "test.audit",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {
                "run": _audit_run,
            },
            "abort": _audit_abort,
        }
    )
    add_task(setup.registry, audit)
    # Slow enough that the audit's read lands while the answer is still streaming, as upstream's
    # 80-tokens-a-second faux guarantees.
    setup.faux.set_responses([streamed(LONG, chunks=120, interval=0.05)])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "hi"}, context)).id

    async def streamed_some() -> bool:
        return len(opened["memory"][-1]) > 40 if opened["memory"] else False

    await eventually(streamed_some)
    conversation_id = opened["root"].id
    task_id = await opened["harness"].commit(
        lambda tx: tx.create_task(
            audit,
            {"conversationId": conversation_id},
            {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
        ),
        context,
    )
    await opened["harness"].wait_for_task(task_id, context)
    derived = str(
        next(entry for entry in await all_entries(opened["root"]) if entry["kind"] == "test.audit")["data"]
    )
    assert len(derived) > 40
    # The read stored the partial it saw, in the same storage transaction as the entry.
    batch = next(
        writes
        for writes in storage.commits
        if any(write["type"] == "entry" and write["value"]["kind"] == "test.audit" for write in writes)
    )
    assert any(write["type"] == "document.change" for write in batch)
    await crash(opened["harness"])
    assert stored_text(await stored_live(storage, conversation_id)) == derived

    setup.faux.set_responses([assistant_message("answer")])
    opened = await open(storage, setup)
    await (await opened["harness"].submission(submission, context)).wait(context)
    assert answers(await all_entries(opened["root"]))[0] == {"text": derived, "stopReason": "aborted"}
    await opened["harness"].close(context)


async def _audit_run(task: Any, runtime: Any, call_context: Any) -> None:
    """Read ``pi.live`` and derive an entry from what it shows: the read is the trigger."""

    async def read(tx: Any) -> dict[str, Any]:
        # Inside a transaction `tx.doc` hands back the document as it is being written, not the plain
        # value Storage would return, so the box is read as it comes rather than through the value
        # narrowing `generation_of` does.
        live = await tx.doc(LiveDoc, task["input"]["conversationId"])
        generation = live.get("generation") or {}
        text = partial_text(generation.get("message")) or ""
        await tx.append_entry(task["input"]["conversationId"], {"kind": "test.audit", "data": text})
        return {"status": "terminal", "outcome": {"status": "completed", "result": None}}

    await runtime.commit(read, call_context)


async def _audit_abort(_task: Any, _runtime: Any, _context: Any) -> None:
    return None


def held_tool(name: str, gate: Any, write: Any) -> Any:
    """A tool that runs ``write`` against its api, then waits for ``gate`` to end the call."""

    async def execute(_args: Any, api: Any, call_context: Any) -> Any:
        await write(api, call_context)
        await gate.promise
        return {}

    return tool(name, execute)


def slot_by_call_id(live: Any, call_id: str) -> dict[str, Any] | None:
    """The slot of ``call_id`` in a ``pi.live`` value, model-issued calls only."""

    for slot in (live or {}).get("tools") or ():
        if slot.get("callId") == call_id:
            return cast(dict[str, Any], slot)
    return None


async def shown_slot(harness: Any, conversation_id: Any, call_id: str) -> dict[str, Any] | None:
    """The slot of ``call_id`` as memory shows it, the way a watch of ``pi.live`` does."""

    return slot_by_call_id(await harness.snapshot(LiveDoc, conversation_id, context), call_id)


async def stored_slot(
    storage: ControlledStorage, conversation_id: Any, call_id: str
) -> dict[str, Any] | None:
    """The slot of ``call_id`` as Storage has it."""

    return slot_by_call_id(await stored_live(storage, conversation_id), call_id)


async def test_shows_a_running_tools_details_at_once_and_stores_them_with_the_next_trigger() -> None:
    """No upstream counterpart: the `details()` half of the deferred window (`PARITY.md` 7.3).

    Upstream's progress writer commits a running call's last `details()` value inside the same deferred
    commit as its output (`harness/tool.ts:634-655`), and this tree's writer does the same
    (`harness/tool.py:945-957`). Upstream's suites prove what a reported `details()` value *diffs into*
    -- `harness-live-deltas.test.ts:236-262`, translated here as
    `tests/test_harness_live_deltas.py::test_diffs_details_leaf_by_leaf_and_appends_diagnostics` -- but
    none of them puts it on the deferred window, so the row's claim rests on this case: what a running
    call reports through `details()` is shown to memory as it is reported, and reaches Storage only with
    a trigger -- here, the next commit that reads the document.
    """

    storage = ControlledStorage()
    setup = slow_setup(float("inf"))
    gate = deferred()
    details = {"step": 1, "log": "working"}
    add_tool(
        setup.registry,
        held_tool("work", gate, lambda api, call_context: api.details(details, call_context)),
    )
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id

    async def reported() -> bool:
        slot = await shown_slot(opened["harness"], opened["root"].id, "c1")
        return slot is not None and slot.get("details") == details

    await eventually(reported)
    # Memory shows the reported details. Storage has the slot -- it arrived with the round's commit,
    # which is immediate because it creates the tool's task (`generation.py:1038`) -- but not what was
    # reported into it.
    stored = await stored_slot(storage, opened["root"].id, "c1")
    assert stored is not None and "details" not in stored
    # A commit that reads the document is the trigger: it takes the held revision with it, beside its
    # own writes. Appending the entry alone would be no trigger at all -- it leaves `pi.live` alone.
    async def touch(tx: Any) -> None:
        await tx.doc(LiveDoc, opened["root"].id)
        await tx.append_entry(opened["root"].id, {"kind": "note"})

    await opened["root"].commit(touch, context)
    stored = await stored_slot(storage, opened["root"].id, "c1")
    assert stored is not None and stored["details"] == details
    gate.resolve()
    await (await opened["harness"].submission(submission, context)).wait(context)
    await opened["harness"].close(context)


async def test_defers_a_running_tools_own_commit_only_when_it_asks_for_it() -> None:
    """No upstream counterpart: `ToolApi.commit(change, context, options)` (`PARITY.md` 7.3).

    Upstream's tool api hands the running tool its task's commit with the persistence mark passed
    through (`harness/types.ts:282-283`, `harness/tool.ts:487-496`); this tree's does the same
    (`harness/tool.py:762-774`). No upstream case commits through that api with a mark, so the meaning
    of the parameter rests on this case: a deferred commit of a running call's own slot is shown and
    not stored, and the next commit of the same document -- unmarked, as every tool commit that asks
    for nothing is -- stores it.
    """

    storage = ControlledStorage()
    setup = slow_setup(float("inf"))
    held = deferred()
    finish = deferred()

    async def write(api: Any, call_context: Any) -> None:
        async def mark(tx: Any, key: str) -> None:
            live = await tx.doc(LiveDoc, api.conversation_id)
            slot = next(slot for slot in live["tools"] if slot["callId"] == "c1")
            slot[key] = True

        await api.commit(lambda tx: mark(tx, "shown"), call_context, PROGRESS_COMMIT)
        await held.promise
        await api.commit(lambda tx: mark(tx, "stored"), call_context)
        await finish.promise

    add_tool(setup.registry, held_tool("work", finish, write))
    setup.faux.set_responses([calls(["work", {}, "c1"]), DONE])
    opened = await open(storage, setup)
    submission = (await opened["root"].submit({"type": "input", "content": "go"}, context)).id

    async def shown() -> bool:
        slot = await shown_slot(opened["harness"], opened["root"].id, "c1")
        return slot is not None and slot.get("shown") is True

    await eventually(shown)
    # The mark is in memory and nowhere else: what Storage has of the slot is what the round's commit
    # stored, and the tool's own commit carried the deferred mark.
    stored = await stored_slot(storage, opened["root"].id, "c1")
    assert stored is not None and "shown" not in stored
    # The tool's next commit of the same document is unmarked, so it stores its own write and the one
    # that was held.
    held.resolve()

    async def stored_too() -> bool:
        slot = await stored_slot(storage, opened["root"].id, "c1")
        return slot is not None and slot.get("stored") is True

    await eventually(stored_too)
    held_slot = await stored_slot(storage, opened["root"].id, "c1")
    assert held_slot is not None and held_slot["shown"] is True
    finish.resolve()
    await (await opened["harness"].submission(submission, context)).wait(context)
    await opened["harness"].close(context)

"""Translated upstream ``harness-submit-in-commit`` cases: ``whenIdle: "reject"``, and ``submit(tx, draft)``.

Upstream ``test/harness-submit-in-commit.test.ts`` holds 23 cases. 19 are translated here.

Excluded, telemetry-only (ADR 0002 keeps telemetry out of the port, so there is no span, attribute,
link, or event to assert):
"records the trace carrier and links a steer to its caller and the running trace",
"starts an idle input's run in the admission's trace",
"makes a write a child of its caller and leaves the commit's events to the caller's commit",
"ends its span before the commit, and a later throw leaves no submission". The last case's one
non-telemetry claim, that a later throw leaves no submission, is what
``test_is_rolled_back_with_the_commit_when_the_callback_throws_later`` asserts.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest
from chat_support import (
    assistant_message,
    chat_setup,
    faux_text,
    faux_tool_call,
    open_chat,
    unanswered,
)
from session_support import ControlledStorage, context
from task_support import deferred, eventually
from tool_support import add_task, add_tool, tool

from pi_durable import (
    ConversationBusy,
    ConversationIdle,
    InboxDoc,
    LiveDoc,
    ReadAfterWrite,
    UserEntry,
    define_doc,
    define_task,
)
from pi_durable.context import AbortError
from pi_durable.harness.types import SettledSubmissionRecord

MODEL = {"provider": "faux", "modelId": "faux-1"}

failure = assistant_message([], stop_reason="error", error_message="Invalid request")

#: A host's own conversation document.
Sent: Any = define_doc(
    {
        "kind": "test.sent",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "initial",
        "initial": lambda: {"ids": []},
        "checkpoint_when": lambda _value, _ops, _info: False,
    }
)


async def open_busy(storage: Any | None = None) -> dict[str, Any]:
    """Open a chat whose run hangs until aborted, and wait until it works on its first input."""

    setup = chat_setup()
    setup.faux.set_responses([unanswered()])
    values = storage or ControlledStorage()
    opened = await open_chat(values, setup)
    first = await opened["root"].submit({"type": "input", "content": "work"}, context)
    await eventually(lambda: setup.faux.call_count == 1)
    return {**opened, "setup": setup, "storage": values, "first": first}


async def settled_status(harness: Any, id: Any) -> SettledSubmissionRecord:
    """Status of submission ``id``, polled without asking for progress (``wait()`` would)."""

    submission = await harness.submission(id, context)
    assert submission is not None

    async def check() -> bool:
        record = await submission.status(context)
        return record["status"] in ("done", "unanswered")

    await eventually(check)
    return cast(SettledSubmissionRecord, await submission.status(context))


def add_hold(setup: Any) -> dict[str, Any]:
    """Install tool ``hold``, which reports its call and waits for ``release``."""

    reached = deferred()
    release = deferred()

    def hold(_args: Any, _api: Any, _context: Any) -> Any:
        async def wait() -> dict[str, Any]:
            reached.resolve()
            await release.promise
            return {"output": []}

        return wait()

    add_tool(setup.registry, tool("hold", hold))
    call = assistant_message([faux_tool_call("hold", {}, id="c1")], stop_reason="toolUse")
    return {"reached": reached, "release": release, "call": call}


# ── whenIdle reject ─────────────────────────────────────────────────────────


async def test_rejects_input_to_an_idle_conversation_without_writing() -> None:
    storage = ControlledStorage()
    opened = await open_chat(storage, chat_setup())
    harness, root = opened["harness"], opened["root"]
    commits = len(storage.commits)
    with pytest.raises(ConversationIdle) as rejected:
        await root.submit(
            {"type": "input", "content": "hi", "when_busy": "steer", "when_idle": "reject"},
            context,
        )
    assert rejected.value.conversation_id == root.id
    assert len(storage.commits) == commits
    assert (await harness.snapshot(LiveDoc, root.id, context) or {}).get("run") is None
    await harness.close(context)


async def test_rejects_input_to_an_idle_conversation_that_keeps_queued_items_after_a_failed_run() -> None:
    setup = chat_setup()
    failing = deferred()
    requested = deferred()

    async def first_step(_request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        requested.resolve()
        await failing.promise
        return failure

    setup.faux.set_responses([first_step])
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "first"}, context)
    await requested.promise
    queued = await root.submit({"type": "input", "content": "queued"}, context)
    failing.resolve()
    settled = await first.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"
    await harness.wait_for_idle(context)
    with pytest.raises(ConversationIdle):
        await root.submit(
            {"type": "input", "content": "steer", "when_busy": "steer", "when_idle": "reject"},
            context,
        )
    assert (await queued.status(context))["status"] == "queued"
    assert (await harness.snapshot(LiveDoc, root.id, context) or {}).get("run") is None
    await harness.close(context)


async def test_steers_a_working_run() -> None:
    opened = await open_busy()
    harness, root, first = opened["harness"], opened["root"], opened["first"]
    steer = await root.submit(
        {"type": "input", "content": "also this", "when_busy": "steer", "when_idle": "reject"},
        context,
    )
    record = await steer.status(context)
    assert record["type"] == "input" and record["status"] == "queued"
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert inbox is not None
    assert inbox["items"] == [{"id": steer.id, "mode": "steer", "content": "also this"}]
    live = await harness.snapshot(LiveDoc, root.id, context)
    assert live is not None and live["run"]["inputs"] == [first.id]
    await harness.close(context)


async def test_returns_the_submission_of_a_known_request_id_also_once_idle() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message([faux_text("done")])])
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "work", "request_id": "r1"}, context)
    await first.wait(context)
    draft = {
        "type": "input",
        "content": "work",
        "request_id": "r1",
        "when_busy": "steer",
        "when_idle": "reject",
    }
    assert (await root.submit(cast(Any, draft), context)).id == first.id
    # Deduplication runs before the idle rejection in the caller's commit too.
    assert await harness.commit(lambda tx: root.submit(tx, cast(Any, draft)), context) == first.id
    await harness.close(context)


async def test_rejects_through_a_tools_conversation_handle() -> None:
    setup = chat_setup()
    other: dict[str, Any] = {}
    rejection: dict[str, Any] = {}

    def nudge(_args: Any, api: Any, call_context: Any) -> Any:
        async def work() -> dict[str, Any]:
            handle = await api.conversation(other["id"], call_context)
            draft = {
                "type": "input",
                "content": "nudge",
                "when_busy": "steer",
                "when_idle": "reject",
            }
            try:
                rejection["error"] = await handle.submit(cast(Any, draft), call_context)
            except BaseException as error:
                rejection["error"] = error
            return {"output": [{"type": "text", "text": "nudged"}]}

        return work()

    add_tool(setup.registry, tool("nudge", nudge))
    setup.faux.set_responses(
        [
            assistant_message([faux_tool_call("nudge", {}, id="c1")], stop_reason="toolUse"),
            assistant_message([faux_text("ok")]),
        ]
    )
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    child = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}, "agent": {"model": MODEL}}, context
    )
    other["id"] = child.id
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert isinstance(rejection["error"], ConversationIdle)
    assert (await harness.snapshot(LiveDoc, child.id, context) or {}).get("run") is None
    await harness.close(context)


# ── submit inside the caller's commit ───────────────────────────────────────


async def test_admits_with_the_hosts_own_check_and_writes_in_one_commit() -> None:
    opened = await open_busy()
    harness, root, storage, first = (
        opened["harness"],
        opened["root"],
        opened["storage"],
        opened["first"],
    )
    commits = len(storage.commits)

    async def change(tx: Any) -> Any:
        if (await tx.doc(LiveDoc, root.id)).get("run") is None:
            raise RuntimeError("not working")
        # The host's own document write may come before the admission.
        sent = await tx.doc(Sent, root.id)
        admitted = await root.submit(
            tx, {"type": "input", "content": "steer", "when_busy": "steer"}
        )
        cast(Any, sent)["ids"].append(admitted)
        # The host's own table write comes after it.
        await tx.append_entry(root.id, {"kind": "app.sent", "data": {"submission": admitted}})
        return admitted

    id = await harness.commit(change, context)
    assert len(storage.commits) == commits + 1
    writes = storage.commits[-1]
    assert any(write["type"] == "submission" and write["value"]["id"] == id for write in writes)
    assert any(write["type"] == "entry" and write["value"]["kind"] == "app.sent" for write in writes)
    sent = await harness.snapshot(cast(Any, Sent), root.id, context)
    assert sent is not None and sent["ids"] == [id]
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert inbox is not None
    assert inbox["items"] == [{"id": id, "mode": "steer", "content": "steer"}]
    live = await harness.snapshot(LiveDoc, root.id, context)
    assert live is not None and live["run"]["inputs"] == [first.id]
    await harness.close(context)


async def test_is_rolled_back_with_the_commit_when_the_callback_throws_later() -> None:
    storage = ControlledStorage()
    opened = await open_chat(storage, chat_setup())
    harness, root = opened["harness"], opened["root"]
    commits = len(storage.commits)

    async def change(tx: Any) -> None:
        await root.submit(tx, {"type": "input", "content": "hi", "request_id": "r1"})
        raise RuntimeError("host check failed")

    with pytest.raises(RuntimeError, match="host check failed"):
        await harness.commit(change, context)
    assert len(storage.commits) == commits
    assert (await harness.snapshot(LiveDoc, root.id, context) or {}).get("run") is None
    assert await harness.commit(lambda tx: tx.submission_by_request(root.id, "r1"), context) is None
    await harness.close(context)


async def test_leaves_no_submission_and_no_run_when_storage_fails_the_commit_also_after_reopening() -> None:
    storage = ControlledStorage()
    setup = chat_setup()
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    error = RuntimeError("disk full")
    storage.fail_next_commit(error)

    async def change(tx: Any) -> None:
        await root.submit(tx, {"type": "input", "content": "hi", "request_id": "r1"})
        await tx.append_entry(root.id, {"kind": "app.sent"})

    with pytest.raises(RuntimeError, match="disk full"):
        await harness.commit(change, context)
    await harness.closed
    reopened = await open_chat(storage.reopen(), setup)
    assert reopened["root"].id == root.id

    async def look(tx: Any) -> Any:
        submission = await tx.submission_by_request(root.id, "r1")
        tasks = await tx.scan_tasks({"conversationId": root.id}, 10)
        entries = await tx.scan_entries({"conversationId": root.id}, 10)
        return {"submission": submission, "tasks": tasks["items"], "entries": entries["items"]}

    assert await reopened["harness"].commit(look, context) == {
        "submission": None,
        "tasks": [],
        "entries": [],
    }
    assert (await reopened["harness"].snapshot(LiveDoc, root.id, context) or {}).get("run") is None
    await reopened["harness"].close(context)


async def test_starts_the_run_of_an_idle_input_once_the_commit_is_stored() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message([faux_text("answer")])])
    # No `resume()`: the admission asks for progress itself.
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    id = await harness.commit(
        lambda tx: root.submit(tx, {"type": "input", "content": "hi"}), context
    )
    settled = await settled_status(harness, id)
    assert settled["type"] == "input" and settled["status"] == "done"
    await harness.close(context)


async def test_queues_a_model_visible_write_while_busy_and_places_it_at_the_next_boundary() -> None:
    setup = chat_setup()
    hold = add_hold(setup)
    setup.faux.set_responses([hold["call"], assistant_message([faux_text("answer")])])
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "work"}, context)
    await hold["reached"].promise
    note = {"role": "user", "content": "a note the model sees", "timestamp": 1}
    id = await harness.commit(
        lambda tx: root.submit(tx, {"type": "write", "entry": {"kind": "test.note", "model": [note]}}),
        context,
    )
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert inbox is not None and [item["id"] for item in inbox["items"]] == [id]
    hold["release"].resolve()
    settled = await first.wait(context)
    assert settled["status"] == "done"
    write = await settled_status(harness, id)
    assert write["type"] == "write" and write["status"] == "done" and write["entry"] > 0
    await harness.close(context)


async def test_deduplicates_request_ids() -> None:
    opened = await open_busy()
    harness, root, storage = opened["harness"], opened["root"], opened["storage"]
    draft = {"type": "input", "content": "steer", "when_busy": "steer", "request_id": "r1"}
    first = await harness.commit(lambda tx: root.submit(tx, cast(Any, draft)), context)
    commits = len(storage.commits)
    again = await harness.commit(lambda tx: root.submit(tx, cast(Any, draft)), context)
    assert again == first
    assert len(storage.commits) == commits
    await harness.close(context)


async def test_rejects_before_any_write_so_the_callback_may_catch_the_rejection() -> None:
    storage = ControlledStorage()
    opened = await open_chat(storage, chat_setup())
    harness, root = opened["harness"], opened["root"]
    commits = len(storage.commits)

    async def change(tx: Any) -> str:
        try:
            await root.submit(
                tx,
                {"type": "input", "content": "hi", "when_busy": "steer", "when_idle": "reject"},
            )
            return "admitted"
        except ConversationIdle:
            return "idle"
        except BaseException:
            return "other"

    assert await harness.commit(change, context) == "idle"
    assert len(storage.commits) == commits
    assert (await harness.snapshot(LiveDoc, root.id, context) or {}).get("run") is None
    await harness.close(context)


async def test_throws_read_after_write_after_a_table_write_of_the_commit_on_every_path() -> None:
    opened = await open_busy()
    harness, root = opened["harness"], opened["root"]

    # A busy input without request ID or trace reads no table, and is still refused.
    async def after_append(tx: Any) -> None:
        await tx.append_entry(root.id, {"kind": "app.note"})
        await root.submit(tx, {"type": "input", "content": "steer", "when_busy": "steer"})

    with pytest.raises(
        ReadAfterWrite,
        match="submit\\(tx, draft\\) cannot read tables after the first table write",
    ):
        await harness.commit(after_append, context)

    async def two_admissions(tx: Any) -> None:
        await root.submit(tx, {"type": "input", "content": "one", "when_busy": "steer"})
        await root.submit(tx, {"type": "input", "content": "two", "when_busy": "steer"})

    with pytest.raises(ReadAfterWrite):
        await harness.commit(two_admissions, context)
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert inbox is not None and inbox["items"] == []

    idle = await open_chat(ControlledStorage(), chat_setup())

    async def late(tx: Any) -> None:
        await tx.append_entry(idle["root"].id, {"kind": "app.note"})
        await idle["root"].submit(tx, {"type": "input", "content": "hi"})

    with pytest.raises(ReadAfterWrite):
        await idle["harness"].commit(late, context)
    await idle["harness"].close(context)
    await harness.close(context)


async def test_continues_a_failed_run_and_must_precede_the_commits_table_writes() -> None:
    setup = chat_setup()
    setup.faux.set_responses([failure, assistant_message([faux_text("recovered")])])
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    first = await root.submit({"type": "input", "content": "Analyze this"}, context)
    settled = await first.wait(context)
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error"

    async def late(tx: Any) -> None:
        await tx.append_entry(root.id, {"kind": "app.note"})
        await root.submit(tx, {"type": "continue"})

    with pytest.raises(ReadAfterWrite):
        await harness.commit(late, context)

    async def admitted(tx: Any) -> Any:
        continued = await root.submit(tx, {"type": "continue"})
        await tx.append_entry(root.id, {"kind": "app.note"})
        return continued

    id = await harness.commit(admitted, context)
    settled = await settled_status(harness, id)
    assert settled["type"] == "continue" and settled["status"] == "done"
    await harness.close(context)


async def test_rejects_a_busy_continue_and_steers_with_when_idle_inside_the_commit() -> None:
    opened = await open_busy()
    harness, root = opened["harness"], opened["root"]
    with pytest.raises(ConversationBusy):
        await harness.commit(lambda tx: root.submit(tx, {"type": "continue"}), context)
    steer = await harness.commit(
        lambda tx: root.submit(
            tx,
            {"type": "input", "content": "steer", "when_busy": "steer", "when_idle": "reject"},
        ),
        context,
    )
    inbox = await harness.snapshot(InboxDoc, root.id, context)
    assert inbox is not None and [item["id"] for item in inbox["items"]] == [steer]
    await harness.close(context)


async def test_throws_for_a_transaction_of_another_session_or_a_wrapped_one() -> None:
    a = await open_chat(ControlledStorage(), chat_setup())
    b = await open_chat(ControlledStorage(), chat_setup())
    draft = {"type": "input", "content": "hi"}

    class Wrapped:
        """A wrapper that forwards every read: it is not the Session's own transaction."""

        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    async def foreign(tx: Any) -> Any:
        return await a["root"].submit(tx, cast(Any, draft))

    with pytest.raises(ValueError, match="The transaction belongs to another Session"):
        await b["harness"].commit(foreign, context)

    async def proxied(tx: Any) -> Any:
        return await a["root"].submit(Wrapped(tx), cast(Any, draft))

    with pytest.raises(ValueError, match="The transaction belongs to another Session"):
        await a["harness"].commit(proxied, context)

    async def plain(_tx: Any) -> Any:
        return await a["root"].submit({"doc": lambda *_args: None}, cast(Any, draft))

    with pytest.raises(ValueError, match="The transaction belongs to another Session"):
        await a["harness"].commit(plain, context)
    assert (await a["harness"].snapshot(LiveDoc, a["root"].id, context) or {}).get("run") is None
    await a["harness"].close(context)
    await b["harness"].close(context)


async def test_admits_atomically_from_a_tools_commit_through_its_conversation_handle() -> None:
    setup = chat_setup()
    other: dict[str, Any] = {}
    tool_task: dict[str, Any] = {}

    def delegate(_args: Any, api: Any, call_context: Any) -> Any:
        async def work() -> dict[str, Any]:
            tool_task["id"] = api.task_id
            handle = await api.conversation(other["id"], call_context)

            async def change(tx: Any) -> Any:
                admitted = await handle.submit(tx, {"type": "input", "content": "from the tool"})
                await tx.append_entry(
                    other["id"], {"kind": "app.delegated", "data": {"submission": admitted}}
                )
                return admitted

            id = await api.commit(change, call_context)
            return {"output": [{"type": "text", "text": f"submitted {id}"}]}

        return work()

    add_tool(setup.registry, tool("delegate", delegate))

    def reply(request: dict[str, Any], _options: dict[str, Any]) -> dict[str, Any]:
        last = request["messages"][-1] if request["messages"] else None
        if last is not None and last["role"] == "user" and last["content"] == "delegate":
            return assistant_message([faux_tool_call("delegate", {}, id="c1")], stop_reason="toolUse")
        return assistant_message([faux_text("ok")])

    setup.faux.set_responses([reply, reply, reply])
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    child = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}, "agent": {"model": MODEL}}, context
    )
    other["id"] = child.id
    run = await root.submit({"type": "input", "content": "delegate"}, context)
    settled = await run.wait(context)
    assert settled["status"] == "done"
    await harness.wait_for_idle(context)
    page = await child.entries({}, 100, None, context)
    entries = list(reversed(page["items"]))
    delegated = next(entry for entry in entries if entry["kind"] == "app.delegated")
    submission = delegated["data"]["submission"]
    status = await settled_status(harness, submission)
    assert status["type"] == "input" and status["status"] == "done"
    # The placed input carries the tool task's byTaskId, as the commit's own appends do.
    user = await child.commit(lambda tx: tx.entry(cast(Any, UserEntry), status["entry"]), context)
    assert user is not None and user["byTaskId"] == tool_task["id"]
    await harness.close(context)


async def test_admits_inside_a_tasks_terminal_commit() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message([faux_text("answer")])])

    async def run_phase(_task: Any, runtime: Any, task_context: Any) -> None:
        handle = await runtime.conversation(runtime.conversation_id, task_context)

        async def change(tx: Any) -> Any:
            id = await handle.submit(tx, {"type": "input", "content": "from the task"})
            return {"status": "terminal", "outcome": {"status": "completed", "result": id}}

        await runtime.commit(change, task_context)

    async def abort_phase(_task: Any, runtime: Any, task_context: Any) -> None:
        await runtime.commit(
            lambda: {"status": "terminal", "outcome": {"status": "aborted"}}, task_context
        )

    Admitting = define_task(
        {
            "name": "test.admitting",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run_phase},
            "abort": abort_phase,
        }
    )
    add_task(setup.registry, Admitting)
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    task = await root.commit(
        lambda tx: tx.create_task(Admitting, None, {"ownership": {"kind": "conversation"}}),
        context,
    )
    harness.resume()
    settled = await harness.wait_for_task(task, context)
    assert settled["state"]["outcome"]["status"] == "completed"
    status = await settled_status(harness, settled["state"]["outcome"]["result"])
    assert status["type"] == "input" and status["status"] == "done"
    user = await root.commit(lambda tx: tx.entry(cast(Any, UserEntry), status["entry"]), context)
    assert user is not None and user["byTaskId"] == task
    await harness.close(context)


async def test_rejects_through_a_handle_once_its_invocation_is_aborted_and_once_it_ended() -> None:
    setup = chat_setup()
    other: dict[str, Any] = {}
    handle: dict[str, Any] = {}
    started = deferred()
    abort_seen = deferred()
    release = deferred()

    def hold(_args: Any, api: Any, call_context: Any) -> Any:
        async def work() -> dict[str, Any]:
            handle["value"] = await api.conversation(other["id"], call_context)
            call_context.abort_signal.add_listener(lambda: abort_seen.resolve())
            started.resolve()
            await release.promise
            return {"output": []}

        return work()

    add_tool(setup.registry, tool("hold", hold))
    setup.faux.set_responses(
        [
            assistant_message([faux_tool_call("hold", {}, id="c1")], stop_reason="toolUse"),
            assistant_message([faux_text("ok")]),
        ]
    )
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    child = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}, "agent": {"model": MODEL}}, context
    )
    other["id"] = child.id
    run = await root.submit({"type": "input", "content": "go"}, context)
    await started.promise
    inspection = await harness.inspect(context)
    tool_task = next(
        task["record"]["id"] for task in inspection["tasks"] if task["record"]["kind"] == "pi.tool"
    )
    aborting = asyncio.ensure_future(harness.abort_task(tool_task, context))
    await abort_seen.promise
    draft = {"type": "input", "content": "late"}
    # The invocation's own abort signal refuses the call, in a commit as outside one.
    with pytest.raises(AbortError):
        await handle["value"].submit(cast(Any, draft), context)
    with pytest.raises(AbortError):
        await harness.commit(lambda tx: handle["value"].submit(tx, cast(Any, draft)), context)
    assert (await harness.snapshot(LiveDoc, child.id, context) or {}).get("run") is None
    release.resolve()
    await aborting
    await run.wait(context)
    # Once the invocation ended, its bound handle is closed to further calls.
    with pytest.raises(RuntimeError, match="invocation has ended"):
        await harness.commit(lambda tx: handle["value"].submit(tx, cast(Any, draft)), context)
    assert (await harness.snapshot(LiveDoc, child.id, context) or {}).get("run") is None
    await harness.close(context)

import asyncio

from pi_durable import MemoryStorage, TaskOptions, define_task
from pi_durable.harness import (
    FauxModel,
    Harness,
    InboxDoc,
    LiveDoc,
    Models,
    ToolTask,
    UsageDoc,
    create_registry,
    define_extension,
    faux_assistant_message,
    faux_tool_call,
)


async def open_harness(responses, *, settings=None, tools=(), reports=None):
    registry = create_registry()
    registry.install(define_extension(name="test", tools=tuple(tools)))
    faux = FauxModel(responses)
    models = Models().set("faux", "faux-1", faux)
    harness = await Harness.open(
        MemoryStorage(),
        models=models,
        registry=registry,
        settings=settings,
        on_report=(lambda error, info: reports.append((error, info))) if reports is not None else None,
    )
    root = await harness.root(agent={"model": {"provider": "faux", "modelId": "faux-1"}})
    return harness, root, faux


async def test_scheduler_fault_settles_inputs_and_clears_the_run():
    # A malformed assistant message makes the generation phase throw, so the scheduler,
    # not the task, writes the faulted outcome.
    malformed = {"role": "assistant", "content": {"not": "a list"}, "stopReason": "stop"}
    harness, root, _faux = await open_harness([malformed, "recovered"])
    first = await root.submit({"type": "input", "content": "first"})
    record = await asyncio.wait_for(first.wait(), 5)
    assert record["status"] == "unanswered"
    assert record["reason"] == "faulted"
    assert record["detail"]["message"]
    live = await harness.snapshot(LiveDoc, root.id)
    assert live == {}
    assert (await harness.inspect()).tasks == []

    second = await root.submit({"type": "input", "content": "second"})
    record = await asyncio.wait_for(second.wait(), 5)
    assert record["status"] == "done"
    await harness.close()


async def test_abort_handler_without_a_terminal_outcome_faults_instead_of_looping():
    async def run(task, runtime):
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "run"})

    async def abort(task, runtime):
        return None

    TestTask = define_task(
        name="app.bad-abort",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort,
    )
    registry = create_registry()
    registry.install(define_extension(name="test", tasks=[TestTask]))
    harness = await Harness.open(
        MemoryStorage(),
        models=Models().set("faux", "faux-1", FauxModel([])),
        registry=registry,
    )
    conversation = await harness.create_conversation({"ownership": {"kind": "ownerless"}})
    task_id = await harness.commit_with(
        lambda tx: tx.create_task(
            TestTask,
            None,
            TaskOptions(ownership={"kind": "conversation"}, conversation_id=conversation.id),
        )
    )
    harness.resume()
    for _ in range(100):
        record = await harness.get_task(task_id)
        if record["state"]["status"] == "sleeping":
            break
        await asyncio.sleep(0.01)
    await harness.abort_task(task_id)
    settled = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert settled["state"]["outcome"]["status"] == "faulted"
    assert "terminal outcome" in settled["state"]["outcome"]["error"]["message"]
    assert (await harness.inspect()).tasks == []
    await harness.close()


async def test_idle_submission_with_non_empty_inbox_queues_behind_waiting_items():
    released = asyncio.Event()

    async def failing(request):
        await released.wait()
        raise RuntimeError("boom")

    harness, root, faux = await open_harness([failing, "answer second", "answer third"])
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.02)
    second = await root.submit({"type": "input", "content": "second"})
    released.set()
    first_record = await asyncio.wait_for(first.wait(), 5)
    assert first_record["status"] == "unanswered"
    assert (await harness.submission_record(second.id))["status"] == "queued"

    third = await root.submit({"type": "input", "content": "third"})
    # The older follow-up is selected before the new input.
    assert (await harness.submission_record(second.id))["status"] == "placed"
    assert (await harness.submission_record(third.id))["status"] == "queued"

    second_record = await asyncio.wait_for(second.wait(), 5)
    third_record = await asyncio.wait_for(third.wait(), 5)
    assert second_record["status"] == "done"
    assert third_record["status"] == "done"
    assert len(faux.requests) == 3
    second_request = faux.requests[1].messages
    third_request = faux.requests[2].messages
    second_users = [message["content"] for message in second_request if message["role"] == "user"]
    third_users = [message["content"] for message in third_request if message["role"] == "user"]
    assert second_users[-1] == "second"
    assert "third" not in second_users
    assert third_users[-1] == "third"
    await harness.close()


async def test_tool_abort_appends_an_aborted_result_entry():
    harness, root, _faux = await open_harness([])
    task_id = await harness.commit_with(
        lambda tx: tx.create_task(
            ToolTask,
            {
                "call": faux_tool_call("c1", "slow", {}),
                "assistantEntryId": 0,
                "toolName": "slow",
            },
            TaskOptions(ownership={"kind": "conversation"}, conversation_id=root.id),
        )
    )
    await harness.abort_task(task_id)
    harness.resume()
    settled = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert settled["state"]["outcome"]["status"] == "aborted"
    entries = (await root.entries({}, 10))["items"]
    result = next(entry for entry in entries if entry["kind"] == "pi.tool-result")
    assert result["model"][0]["isError"] is True
    assert result["data"]["diagnostics"][0]["code"] == "aborted"
    await harness.close()


async def test_builtin_documents_start_empty_and_usage_has_no_session_total():
    harness, root, _faux = await open_harness([])
    assert await harness.snapshot(LiveDoc, root.id) == {}
    assert await harness.snapshot(UsageDoc, root.id) == {"models": {}, "tools": {}}
    assert await harness.snapshot(InboxDoc, root.id) == {"items": []}
    await harness.close()


async def test_faulted_or_aborted_hold_cancels_owned_work_instead_of_waiting_for_it():
    async def child_sleep(task, runtime):
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "run"})

    async def child_abort(task, runtime):
        await runtime.commit(
            lambda tx, current: {"status": "terminal", "outcome": {"status": "aborted"}}
        )

    Child = define_task(
        name="app.held-child",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": child_sleep},
        abort=child_abort,
    )

    async def spawn(task, runtime):
        async def commit(tx, current):
            await tx.create_task(Child, None, TaskOptions(ownership={"kind": "task", "taskId": task["id"]}))
            return {"status": "running", "checkpoint": {"phase": "finish"}}

        await runtime.commit(commit)

    async def finish(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "aborted", "reason": "parent gave up"},
            }
        )

    Parent = define_task(
        name="app.held-parent",
        version=1,
        initial=lambda value: {"phase": "spawn"},
        phases={"spawn": spawn, "finish": finish},
        abort=lambda task, runtime: runtime.commit(
            lambda tx, current: {"status": "terminal", "outcome": {"status": "aborted"}}
        ),
    )
    registry = create_registry()
    registry.install(define_extension(name="test", tasks=[Parent, Child]))
    harness = await Harness.open(
        MemoryStorage(),
        models=Models().set("faux", "faux-1", FauxModel([])),
        registry=registry,
    )
    conversation = await harness.create_conversation({"ownership": {"kind": "ownerless"}})
    parent_id = await harness.commit_with(
        lambda tx: tx.create_task(
            Parent,
            None,
            TaskOptions(ownership={"kind": "conversation"}, conversation_id=conversation.id),
        )
    )
    harness.resume()
    settled = await asyncio.wait_for(harness.wait_for_task(parent_id), 5)
    assert settled["state"]["outcome"]["status"] == "aborted"
    assert (await harness.inspect()).tasks == []
    await harness.close()


async def test_stale_head_writes_settle_unanswered_without_resurrecting_history():
    released = asyncio.Event()

    async def slow(request):
        await released.wait()
        return faux_assistant_message("answer")

    harness, root, _faux = await open_harness([slow])
    old_entry = await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "app.note", "data": "before reset"})
    )
    await root.reset("handoff")
    reset_entry = next(
        entry
        for entry in (await root.entries({}, 20))["items"]
        if entry["kind"] == "pi.reset"
    )

    # Direct admission: a head older than the active range is stale.
    stale = await root.submit(
        {"type": "write", "entry": {"kind": "pi.compaction", "head": old_entry["id"]}}
    )
    record = await asyncio.wait_for(stale.wait(), 2)
    assert record["status"] == "unanswered"
    assert record["reason"] == "stale"

    # A head at the active range start is fresh and placed.
    fresh = await root.submit(
        {"type": "write", "entry": {"kind": "pi.compaction", "head": reset_entry["id"]}}
    )
    record = await asyncio.wait_for(fresh.wait(), 2)
    assert record["status"] == "done"

    # Queued at a turn boundary: the same staleness rule applies.
    await root.reset("second handoff")
    submission = await root.submit({"type": "input", "content": "question"})
    await asyncio.sleep(0.02)
    queued_stale = await root.submit(
        {"type": "write", "entry": {"kind": "pi.compaction", "head": old_entry["id"]}}
    )
    released.set()
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "done", {k: record.get(k) for k in ("status", "reason", "detail", "entry", "answer")}
    record = await asyncio.wait_for(queued_stale.wait(), 5)
    assert record["status"] == "unanswered"
    assert record["reason"] == "stale"
    await harness.close()


async def test_idle_non_empty_inbox_boundary_still_rejects_stale_heads():
    released = asyncio.Event()

    async def failing(request):
        await released.wait()
        raise RuntimeError("boom")

    harness, root, _faux = await open_harness([failing, faux_assistant_message("answer")])
    await root.reset("handoff")
    old_entry = await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "app.note", "data": "before reset"})
    )
    await root.reset("second handoff")
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.02)
    stale = await root.submit(
        {"type": "write", "entry": {"kind": "pi.compaction", "head": old_entry["id"]}}
    )
    released.set()
    assert (await asyncio.wait_for(first.wait(), 5))["status"] == "unanswered"
    assert (await harness.submission_record(stale.id))["status"] == "queued"

    # Idle now, with the stale write waiting in the inbox. The admission runs a final
    # boundary in the same commit, which must still reject the stale head.
    second = await root.submit({"type": "input", "content": "second"})
    stale_record = await asyncio.wait_for(stale.wait(), 5)
    assert stale_record["status"] == "unanswered"
    assert stale_record["reason"] == "stale"
    assert (await asyncio.wait_for(second.wait(), 5))["status"] == "done"
    await harness.close()

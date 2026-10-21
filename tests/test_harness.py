import asyncio

import pytest

from pi_durable import ConversationBusy, MemoryStorage
from pi_durable.harness import (
    FauxModel,
    Harness,
    InboxDoc,
    LiveDoc,
    Models,
    ToolResult,
    UsageDoc,
    create_registry,
    define_extension,
    define_tool,
    faux_assistant_message,
    faux_tool_call,
    section,
)


async def open_harness(responses, *, tools=(), settings=None, reports=None):
    extension = define_extension(
        name="test",
        tools=tuple(tools),
        sections=[section("preamble", lambda context: "You are a test assistant.")],
    )
    registry = create_registry()
    registry.install(extension)
    faux = FauxModel(responses)
    models = Models().set("faux", "faux-1", faux)
    harness = await Harness.open(
        MemoryStorage(),
        models=models,
        registry=registry,
        settings=settings,
        on_report=(lambda error, info: reports.append((error, info))) if reports is not None else None,
    )
    return harness, faux


async def root_with_model(harness):
    return await harness.root(agent={"model": {"provider": "faux", "modelId": "faux-1"}})


async def test_root_is_lazy_and_idempotent():
    harness, _ = await open_harness([])
    first = await harness.root(agent={"model": {"provider": "faux", "modelId": "faux-1"}})
    second = await harness.root(agent={"model": {"provider": "other", "modelId": "other"}})
    assert first.id == second.id == 1
    agent = await second.agent()
    assert agent.model == {"provider": "faux", "modelId": "faux-1"}
    conversations = await harness.commit_with(lambda tx: tx.scan_conversations({}, 10))
    assert len(conversations["items"]) == 1
    await harness.close()


async def test_answer_settles_the_submission_and_records_entries():
    harness, faux = await open_harness(["Paris is the capital."])
    root = await root_with_model(harness)
    submission = await root.submit({"type": "input", "content": "Capital of France?"})
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "done"
    assert record["type"] == "input"
    assert record["answer"] is not None

    entries = (await root.entries({}, 20))["items"]
    assert [entry["kind"] for entry in reversed(entries)] == ["pi.user", "pi.system", "pi.assistant"]
    messages = (await root.context())["messages"]
    assert [message["role"] for message in messages] == ["system", "user", "assistant"]
    assert messages[2]["content"][0]["text"] == "Paris is the capital."
    assert len(faux.requests) == 1
    assert faux.requests[0].session_id
    await harness.close()


async def test_custom_input_entry_kind_and_data():
    harness, _ = await open_harness(["ok"])
    root = await root_with_model(harness)
    submission = await root.submit(
        {
            "type": "input",
            "content": "question",
            "entry": {"kind": "app.ask", "data": {"author": "me"}},
        }
    )
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "done"
    entries = (await root.entries({}, 20))["items"]
    ask = next(entry for entry in entries if entry["kind"] == "app.ask")
    assert ask["data"] == {"author": "me"}
    assert ask["model"][0]["role"] == "user"
    await harness.close()


async def test_request_id_makes_retry_exactly_once():
    harness, faux = await open_harness(["answer"])
    root = await root_with_model(harness)
    first = await root.submit({"type": "input", "content": "hi", "requestId": "greeting-1"})
    second = await root.submit({"type": "input", "content": "hi again", "requestId": "greeting-1"})
    assert second.id == first.id
    await asyncio.wait_for(first.wait(), 5)
    assert len(faux.requests) == 1
    await harness.close()


async def test_tool_call_runs_as_child_task_and_result_returns_to_model():
    async def echo(args, api):
        api.output("echoing")
        return ToolResult(
            output=f"echo:{args['text']}",
            details={"seen": args["text"]},
            usage={"input": 2, "output": 3, "total": 5, "cost": 0.5},
        )

    echo_tool = define_tool(
        name="echo",
        description="Echo text",
        parameters={"type": "object", "properties": {"text": {"type": "string"}}},
        execute=echo,
    )
    harness, faux = await open_harness(
        [
            faux_assistant_message(
                tool_calls=[faux_tool_call("c1", "echo", {"text": "hi"})],
                stop_reason="toolUse",
                usage={"input": 1, "output": 1, "total": 2},
            ),
            "final answer",
        ],
        tools=[echo_tool],
    )
    root = await root_with_model(harness)
    submission = await root.submit({"type": "input", "content": "please echo"})
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "done"

    entries = (await root.entries({}, 30))["items"]
    kinds = [entry["kind"] for entry in reversed(entries)]
    assert kinds == ["pi.user", "pi.system", "pi.assistant", "pi.tool-result", "pi.assistant"]
    tool_result = next(entry for entry in entries if entry["kind"] == "pi.tool-result")
    assert tool_result["model"][0]["content"][0]["text"] == "echo:hi"
    assert tool_result["data"] == {"diagnostics": []}
    messages = (await root.context())["messages"]
    assert [message["role"] for message in messages] == [
        "system",
        "user",
        "assistant",
        "toolResult",
        "assistant",
    ]
    assert [tool["name"] for tool in faux.requests[1].tools] == ["echo"]
    usage = await harness.usage()
    assert usage["total"] == 7
    assert usage["cost"] == 0.5
    task_usage = await harness.snapshot(UsageDoc, root.id)
    assert task_usage["tools"]["echo"]["total"] == 5
    await harness.close()


async def test_agent_change_emits_a_system_delta_with_tool_removal():
    async def noop(args, api):
        return ToolResult(output="ok")

    tool = define_tool(name="noop", description="Does nothing", parameters={"type": "object"}, execute=noop)
    harness, _faux = await open_harness(["one", "two"], tools=[tool])
    root = await root_with_model(harness)
    await asyncio.wait_for((await root.submit({"type": "input", "content": "first"})).wait(), 5)
    first_system = next(
        entry for entry in (await root.entries({}, 20))["items"] if entry["kind"] == "pi.system"
    )
    assert first_system["model"][0]["sections"] == {"preamble": "You are a test assistant."}
    assert [item["name"] for item in first_system["model"][0]["toolsAdded"]] == ["noop"]

    await root.configure({"tools": {"remove": [tool]}})
    assert (await root.agent()).tools == ()
    await asyncio.wait_for((await root.submit({"type": "input", "content": "second"})).wait(), 5)

    systems = [
        entry
        for entry in (await root.entries({}, 30))["items"]
        if entry["kind"] == "pi.system"
    ]
    assert len(systems) == 2
    delta = systems[0]["model"][0]
    assert delta["toolsRemoved"] == ["noop"]
    assert "toolsAdded" not in delta
    messages = (await root.context())["messages"]
    assert [message["role"] for message in messages] == [
        "system",
        "user",
        "assistant",
        "user",
        "system",
        "assistant",
    ]
    await harness.close()


async def test_busy_conversation_queues_steers_followups_and_writes():
    held = asyncio.Event()

    async def slow(request):
        await held.wait()
        return faux_assistant_message("first answer")

    harness, faux = await open_harness([slow, "second answer"], settings={"followUpMode": "all"})
    root = await root_with_model(harness)
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.05)
    follow_up = await root.submit({"type": "input", "content": "follow-up"})
    steer = await root.submit({"type": "input", "content": "steer", "whenBusy": "steer"})
    note = await root.submit(
        {"type": "write", "entry": {"kind": "app.note", "data": "noted while busy"}}
    )
    withdrawn = await root.submit({"type": "input", "content": "never mind"})
    with pytest.raises(ConversationBusy):
        await root.submit({"type": "input", "content": "now", "whenBusy": "reject"})
    assert await withdrawn.abort() == "aborted"

    held.set()
    for submission in (first, follow_up, steer, note, withdrawn):
        record = await asyncio.wait_for(submission.wait(), 5)
        assert record["status"] == ("unanswered" if submission is withdrawn else "done")

    transcript = [entry["kind"] for entry in reversed((await root.entries({}, 30))["items"])]
    assert transcript == [
        "pi.user",
        "pi.system",
        "pi.assistant",
        "app.note",
        "pi.user",
        "pi.user",
        "pi.assistant",
    ]
    assert (await harness.snapshot(InboxDoc, root.id))["items"] == []
    assert (await harness.snapshot(LiveDoc, root.id))["run"] is None
    assert len(faux.requests) == 2
    await harness.close()


async def test_conversation_abort_withdraws_queued_inputs_and_aborts_the_run():
    held = asyncio.Event()

    async def slow(request):
        await held.wait()
        return "never"

    harness, _ = await open_harness([slow])
    root = await root_with_model(harness)
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.05)
    queued = await root.submit({"type": "input", "content": "queued"})

    async def release():
        await asyncio.sleep(0.2)
        held.set()

    asyncio.ensure_future(release())
    await asyncio.wait_for(root.abort(), 5)
    first_record = await harness.submission_record(first.id)
    queued_record = await harness.submission_record(queued.id)
    assert first_record["status"] == "unanswered"
    assert first_record["reason"] == "aborted"
    assert queued_record["status"] == "unanswered"
    assert queued_record["reason"] == "aborted"
    assert (await harness.snapshot(LiveDoc, root.id))["run"] is None
    assert [task["record"]["kind"] for task in (await harness.inspect()).tasks] == []
    await harness.close()


async def test_failed_run_policies_keep_continue_and_withdraw():
    async def failing(request):
        raise RuntimeError("model exploded")

    async def slow_failing(request):
        await asyncio.sleep(0.05)
        raise RuntimeError("model exploded")

    # keep: the queued input stays queued.
    harness, _ = await open_harness([slow_failing])
    root = await root_with_model(harness)
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.01)
    second = await root.submit({"type": "input", "content": "second"})
    first_record = await asyncio.wait_for(first.wait(), 5)
    assert first_record["status"] == "unanswered"
    assert first_record["reason"] == "model_error"
    assert (await harness.submission_record(second.id))["status"] == "queued"
    assert [item["content"] for item in (await harness.snapshot(InboxDoc, root.id))["items"]] == ["second"]
    await harness.close()

    # continue: the queued input starts the next run.
    harness, faux = await open_harness([slow_failing, "recovered"], settings={"afterFailedRun": "continue"})
    root = await root_with_model(harness)
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.01)
    second = await root.submit({"type": "input", "content": "second"})
    await asyncio.wait_for(first.wait(), 5)
    second_record = await asyncio.wait_for(second.wait(), 5)
    assert second_record["status"] == "done"
    assert len(faux.requests) == 2
    await harness.close()

    # withdraw: the queued input is terminally unanswered.
    harness, _ = await open_harness([slow_failing], settings={"afterFailedRun": "withdraw"})
    root = await root_with_model(harness)
    first = await root.submit({"type": "input", "content": "first"})
    await asyncio.sleep(0.01)
    second = await root.submit({"type": "input", "content": "second"})
    await asyncio.wait_for(first.wait(), 5)
    second_record = await asyncio.wait_for(second.wait(), 5)
    assert second_record["status"] == "unanswered"
    assert second_record["reason"] == "run_failed"
    await harness.close()


async def test_agent_without_model_fails_the_run_durably():
    harness, _ = await open_harness([])
    root = await harness.root()
    submission = await root.submit({"type": "input", "content": "hello"})
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "unanswered"
    assert record["reason"] == "no_model"
    assert (await harness.snapshot(LiveDoc, root.id))["run"] is None
    await harness.close()


async def test_fork_copies_the_agent_as_of_the_cutoff():
    harness, _ = await open_harness(["answer"])
    root = await root_with_model(harness)
    submission = await root.submit({"type": "input", "content": "hi"})
    record = await asyncio.wait_for(submission.wait(), 5)
    branch = await root.fork(record["answer"], {"ownership": {"kind": "ownerless"}})
    assert (await branch.agent()).model == {"provider": "faux", "modelId": "faux-1"}
    assert len((await branch.entries({}, 10))["items"]) == 3
    await harness.close()


async def test_inspect_and_task_graph_report_live_work():
    held = asyncio.Event()

    async def slow(request):
        await held.wait()
        return "done"

    harness, _ = await open_harness([slow])
    root = await root_with_model(harness)
    submission = await root.submit({"type": "input", "content": "hi"})
    await asyncio.sleep(0.05)
    inspection = await harness.inspect()
    assert inspection.scheduling == "running"
    assert len(inspection.tasks) == 1
    assert inspection.tasks[0]["state"]["kind"] == "running"
    assert [item["id"] for item in inspection.submissions] == [submission.id]
    graph = await harness.task_graph()
    assert any(node["type"] == "task" for node in graph["nodes"])
    assert any(edge["kind"] == "runs-in" for edge in graph["edges"])
    held.set()
    await asyncio.wait_for(submission.wait(), 5)
    assert (await harness.inspect()).tasks == []
    await harness.close()

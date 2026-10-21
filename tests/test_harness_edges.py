import asyncio

from pi_durable import MemoryStorage, TaskOptions, define_task
from pi_durable.harness import (
    AgentDoc,
    FauxModel,
    Harness,
    InboxDoc,
    Models,
    create_registry,
    define_extension,
    define_tool,
    faux_assistant_message,
    faux_tool_call,
)


async def open_harness(responses, *, tools=(), task=None, reports=None):
    registry = create_registry()
    if tools or task is not None:
        registry.install(define_extension(name="test", tools=tuple(tools), tasks=(task,) if task else ()))
    faux = FauxModel(responses)
    models = Models().set("faux", "faux-1", faux)
    harness = await Harness.open(
        MemoryStorage(),
        models=models,
        registry=registry,
        on_report=(lambda error, info: reports.append((error, info))) if reports is not None else None,
    )
    root = await harness.root(agent={"model": {"provider": "faux", "modelId": "faux-1"}})
    return harness, root, faux


async def test_new_task_owned_conversation_copies_the_owner_agent():
    async def idle(task, runtime):
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "wait"})

    Idle = define_task(
        name="app.idle",
        version=1,
        initial=lambda value: {"phase": "wait"},
        phases={"wait": idle},
        abort=lambda task, runtime: runtime.commit(
            lambda tx, current: {"status": "terminal", "outcome": {"status": "aborted"}}
        ),
    )
    harness, root, _ = await open_harness([], task=Idle)
    task_id = await root.commit(
        lambda tx: tx.create_task(
            Idle, None, TaskOptions(ownership={"kind": "conversation"}, conversation_id=root.id)
        )
    )
    child = await root.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "task", "taskId": task_id}})
    )
    state = await harness.snapshot(AgentDoc, child["id"])
    assert state["model"] == {"provider": "faux", "modelId": "faux-1"}
    assert await harness.snapshot(InboxDoc, child["id"]) == {"items": []}
    await harness.close()


async def test_idle_write_submission_appends_an_entry_without_starting_a_run():
    harness, root, faux = await open_harness([])
    submission = await root.submit(
        {"type": "write", "entry": {"kind": "app.note", "data": "passive"}}
    )
    record = await asyncio.wait_for(submission.wait(), 2)
    assert record["status"] == "done"
    entries = (await root.entries({}, 10))["items"]
    assert [entry["kind"] for entry in entries] == ["app.note"]
    assert faux.requests == []
    assert (await harness.inspect()).tasks == []
    await harness.close()


async def test_unavailable_tool_returns_an_error_result_and_the_run_continues():
    harness, root, faux = await open_harness(
        [
            faux_assistant_message(
                tool_calls=[faux_tool_call("c1", "missing", {"x": 1})], stop_reason="toolUse"
            ),
            "recovered",
        ]
    )
    submission = await root.submit({"type": "input", "content": "call a missing tool"})
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "done"
    entries = (await root.entries({}, 20))["items"]
    result = next(entry for entry in entries if entry["kind"] == "pi.tool-result")
    message = result["model"][0]
    assert message["isError"] is True
    assert result["data"]["diagnostics"][0]["code"] == "tool_unavailable"
    assert len(faux.requests) == 2
    await harness.close()


async def test_tool_throw_becomes_an_error_result():
    async def broken(args, api):
        raise RuntimeError("tool exploded")

    tool = define_tool(name="broken", description="Fails", parameters={"type": "object"}, execute=broken)
    harness, root, _faux = await open_harness(
        [
            faux_assistant_message(
                tool_calls=[faux_tool_call("c1", "broken", {})], stop_reason="toolUse"
            ),
            "recovered",
        ],
        tools=[tool],
    )
    submission = await root.submit({"type": "input", "content": "break"})
    record = await asyncio.wait_for(submission.wait(), 5)
    assert record["status"] == "done"
    entries = (await root.entries({}, 20))["items"]
    result = next(entry for entry in entries if entry["kind"] == "pi.tool-result")
    assert result["model"][0]["isError"] is True
    assert result["data"]["diagnostics"][0]["code"] == "tool_error"
    assert "tool exploded" in result["model"][0]["content"][0]["text"]
    await harness.close()


async def test_reset_writes_a_head_marker_and_handoff():
    harness, root, _ = await open_harness(["first answer", "second answer"])
    await asyncio.wait_for((await root.submit({"type": "input", "content": "first"})).wait(), 5)
    await root.reset("handoff text")
    entries = (await root.entries({}, 20))["items"]
    reset = next(entry for entry in entries if entry["kind"] == "pi.reset")
    assert reset["head"] == reset["id"]
    view = await root.context()
    assert [message["role"] for message in view["messages"]] == ["user"]
    assert view["messages"][0]["content"] == "handoff text"

    await asyncio.wait_for((await root.submit({"type": "input", "content": "after reset"})).wait(), 5)
    view = await root.context()
    assert [message["role"] for message in view["messages"]] == ["user", "user", "assistant"]
    await harness.close()

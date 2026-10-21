import asyncio

from pi_durable import (
    MemoryStorage,
    TaskOptions,
    define_doc,
    define_task,
)
from pi_durable.harness import FauxModel, Harness, Models, create_registry, define_extension


def abort_to_aborted():
    async def abort(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "aborted", "reason": "aborted"},
            }
        )

    return abort


async def open_harness(tasks=(), *, settings=None, reports=None):
    registry = create_registry()
    if tasks:
        registry.install(define_extension(name="test", tasks=tuple(tasks)))
    models = Models().set("faux", "faux-1", FauxModel([]))
    return await Harness.open(
        MemoryStorage(),
        models=models,
        registry=registry,
        settings=settings,
        on_report=(lambda error, info: reports.append((error, info))) if reports is not None else None,
    )


async def create_task(harness, task, input_value=None, *, conversation_id=None):
    if conversation_id is None:
        conversation = await harness.create_conversation({"ownership": {"kind": "ownerless"}})
        conversation_id = conversation.id
    return await harness.commit_with(
        lambda tx: tx.create_task(
            task,
            input_value,
            TaskOptions(ownership={"kind": "conversation"}, conversation_id=conversation_id),
        )
    )


async def test_phase_commits_terminal_outcome():
    async def run(task, runtime):
        assert task["state"]["checkpoint"] == {"phase": "run"}
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": {"ok": True}},
            }
        )

    TestTask = define_task(
        name="app.terminal",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([TestTask])
    task_id = await create_task(harness, TestTask)
    harness.resume()
    record = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert record["state"]["status"] == "terminal"
    assert record["state"]["outcome"] == {"status": "completed", "result": {"ok": True}}
    assert record["startedAt"] is not None
    assert record["endedAt"] is not None
    await harness.close()


async def test_phase_without_durable_progress_faults_the_task():
    async def run(task, runtime):
        return None

    TestTask = define_task(
        name="app.noprogress",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([TestTask])
    task_id = await create_task(harness, TestTask)
    harness.resume()
    record = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert record["state"]["outcome"]["status"] == "faulted"
    assert "durable progress" in record["state"]["outcome"]["error"]["message"]
    await harness.close()


async def test_sleep_holds_no_invocation_until_due():
    slept = asyncio.Event()

    async def wait(task, runtime):
        await runtime.sleep_until(runtime.now() + 100, {"phase": "deliver"})
        slept.set()

    async def deliver(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": "done"},
            }
        )

    TestTask = define_task(
        name="app.sleep",
        version=1,
        initial=lambda value: {"phase": "wait"},
        phases={"wait": wait, "deliver": deliver},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([TestTask])
    task_id = await create_task(harness, TestTask)
    harness.resume()
    await asyncio.wait_for(slept.wait(), 2)
    task = await harness.get_task(task_id)
    assert task["state"]["status"] == "sleeping"
    assert harness.wake()["at"] == task["state"]["until"]
    record = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert record["state"]["outcome"]["result"] == "done"
    await harness.close()


async def test_parent_completes_only_after_owned_work_drains():
    async def child_sleep(task, runtime):
        await runtime.sleep_until(runtime.now() + 120, {"phase": "done"})

    async def child_done(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": "child"},
            }
        )

    Child = define_task(
        name="app.child",
        version=1,
        initial=lambda value: {"phase": "sleep"},
        phases={"sleep": child_sleep, "done": child_done},
        abort=abort_to_aborted(),
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
                "outcome": {"status": "completed", "result": "parent"},
            }
        )

    Parent = define_task(
        name="app.parent",
        version=1,
        initial=lambda value: {"phase": "spawn"},
        phases={"spawn": spawn, "finish": finish},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([Parent, Child])
    conversation = await harness.create_conversation({"ownership": {"kind": "ownerless"}})
    parent_id = await harness.commit_with(
        lambda tx: tx.create_task(
            Parent,
            None,
            TaskOptions(ownership={"kind": "conversation"}, conversation_id=conversation.id),
        )
    )
    harness.resume()
    await asyncio.sleep(0.06)
    inspection = await harness.inspect()
    parent = next(item for item in inspection.tasks if item["record"]["id"] == parent_id)
    assert parent["record"]["state"]["status"] == "completing"
    assert [item["state"]["kind"] for item in inspection.tasks if item["record"].get("owner") == parent_id] == [
        "sleeping"
    ]
    record = await asyncio.wait_for(harness.wait_for_task(parent_id), 5)
    assert record["state"]["outcome"]["result"] == "parent"
    await harness.close()


async def test_waiting_resumes_when_dependencies_settle():
    async def child_done(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": "child"},
            }
        )

    Child = define_task(
        name="app.quick-child",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": child_done},
        abort=abort_to_aborted(),
    )

    async def spawn(task, runtime):
        async def commit(tx, current):
            child = await tx.create_task(
                Child, None, TaskOptions(ownership={"kind": "task", "taskId": task["id"]})
            )
            return {"status": "running", "checkpoint": {"phase": "wait", "child": child}}

        await runtime.commit(commit)

    async def wait(task, runtime):
        child = task["state"]["checkpoint"]["child"]

        async def commit(tx, current):
            return {
                "status": "waiting",
                "on": [child],
                "policy": "allSettled",
                "checkpoint": {"phase": "finish"},
            }

        await runtime.commit(commit)

    async def finish(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": "parent"},
            }
        )

    Parent = define_task(
        name="app.waiter",
        version=1,
        initial=lambda value: {"phase": "spawn"},
        phases={"spawn": spawn, "wait": wait, "finish": finish},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([Parent, Child])
    parent_id = await create_task(harness, Parent)
    harness.resume()
    record = await asyncio.wait_for(harness.wait_for_task(parent_id), 5)
    assert record["state"]["outcome"]["result"] == "parent"
    await harness.close()


async def test_explicit_abort_runs_the_abort_handler():
    async def run(task, runtime):
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "run"})

    async def abort(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "aborted", "reason": "user said so"},
            }
        )

    TestTask = define_task(
        name="app.abortable",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort,
    )
    harness = await open_harness([TestTask])
    task_id = await create_task(harness, TestTask)
    harness.resume()
    for _ in range(50):
        record = await harness.get_task(task_id)
        if record["state"]["status"] == "sleeping":
            break
        await asyncio.sleep(0.01)
    assert (await harness.abort_task(task_id)) == "marked"
    record = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert record["state"]["outcome"] == {"status": "aborted", "reason": "user said so"}
    assert (await harness.abort_task(task_id)) == "terminal"
    await harness.close()


async def test_abort_cascades_to_owned_children():
    async def child_sleep(task, runtime):
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "run"})

    Child = define_task(
        name="app.abort-child",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": child_sleep},
        abort=abort_to_aborted(),
    )
    child_id_holder = {}

    async def parent_spawn(task, runtime):
        async def commit(tx, current):
            child = await tx.create_task(
                Child, None, TaskOptions(ownership={"kind": "task", "taskId": task["id"]})
            )
            child_id_holder["id"] = child
            return {"status": "waiting", "on": [child], "policy": "allSettled", "checkpoint": {"phase": "run"}}

        await runtime.commit(commit)

    async def parent_run(task, runtime):
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "run"})

    Parent = define_task(
        name="app.abort-parent",
        version=1,
        initial=lambda value: {"phase": "spawn"},
        phases={"spawn": parent_spawn, "run": parent_run},
        abort=lambda task, runtime: runtime.commit(
            lambda tx, current: {"status": "terminal", "outcome": {"status": "aborted"}}
        ),
    )
    harness = await open_harness([Parent, Child])
    parent_id = await create_task(harness, Parent)
    harness.resume()
    for _ in range(100):
        if child_id_holder.get("id") and (await harness.get_task(child_id_holder["id"])):
            break
        await asyncio.sleep(0.01)
    await harness.abort_task(parent_id)
    child = await asyncio.wait_for(harness.wait_for_task(child_id_holder["id"]), 5)
    assert child["state"]["outcome"]["status"] == "aborted"
    parent = await asyncio.wait_for(harness.wait_for_task(parent_id), 5)
    assert parent["state"]["outcome"]["status"] == "aborted"
    await harness.close()


async def test_memos_are_first_writer_wins():
    observed = {}

    async def run(task, runtime):
        observed["initial"] = await runtime.memo("key")
        observed["first"] = await runtime.memo("key", "first")
        observed["second"] = await runtime.memo("key", "second")
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": None},
            }
        )

    TestTask = define_task(
        name="app.memo",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([TestTask])
    task_id = await create_task(harness, TestTask)
    harness.resume()
    await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert observed == {"initial": None, "first": "first", "second": "first"}
    await harness.close()


async def test_task_documents_retire_when_the_task_becomes_terminal():
    TaskNotes = define_doc(
        kind="app.task-notes",
        version=1,
        scope="task",
        initial=lambda: {"text": ""},
        checkpoint_when=lambda value, ops, info: True,
    )

    async def run(task, runtime):
        async def commit(tx, current):
            (await tx.doc(TaskNotes, task["id"]))["text"] = "temporary"
            return {"status": "terminal", "outcome": {"status": "completed", "result": None}}

        await runtime.commit(commit)

    TestTask = define_task(
        name="app.task-doc",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort_to_aborted(),
    )
    harness = await open_harness([TestTask])
    task_id = await create_task(harness, TestTask)
    harness.resume()
    await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert await harness.snapshot(TaskNotes, task_id) is None
    await harness.close()

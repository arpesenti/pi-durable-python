import asyncio

from pi_durable import TaskOptions, create_session, define_task
from pi_durable.harness import FauxModel, Harness, Models, create_registry, define_extension


def make_task(runs, *, abort_reason="aborted"):
    async def run(task, runtime):
        runs.append(task["id"])
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "completed", "result": {"runs": len(runs)}},
            }
        )

    async def abort(task, runtime):
        await runtime.commit(
            lambda tx, current: {
                "status": "terminal",
                "outcome": {"status": "aborted", "reason": abort_reason},
            }
        )

    return define_task(
        name="app.recovery",
        version=1,
        initial=lambda value: {"phase": "run"},
        phases={"run": run},
        abort=abort,
    )


async def seed_running_task(storage_factory, task, *, abandon=False):
    storage = await storage_factory.open()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    task_id = await session.commit(
        lambda tx: tx.create_task(
            task,
            None,
            TaskOptions(
                ownership={"kind": "conversation"},
                conversation_id=conversation["id"],
                abandon_on_restart=abandon,
            ),
        )
    )
    record = await storage.task(task_id)
    running = {
        **record,
        "state": {"status": "running", "checkpoint": {"phase": "run"}},
    }
    if abandon:
        running["abandonOnRestart"] = True
    await session.commit(lambda tx: tx.set_task(running))
    if storage_factory.kind == "memory":
        return storage, task_id
    await session.close()
    return await storage_factory.open(), task_id


async def open_harness(storage, task, *, limit=5, reports=None):
    registry = create_registry()
    registry.install(define_extension(name="test", tasks=[task]))
    models = Models().set("faux", "faux-1", FauxModel([]))
    return await Harness.open(
        storage,
        models=models,
        registry=registry,
        settings={"restartLimit": limit},
        on_report=(lambda error, info: reports.append((error, info))) if reports is not None else None,
    )


async def test_open_counts_a_restart_and_resumes_the_task(storage_factory):
    runs = []
    task = make_task(runs)
    storage, task_id = await seed_running_task(storage_factory, task)
    harness = await open_harness(storage, task)
    record = await harness.get_task(task_id)
    assert record["restarts"] == 1
    assert record["state"]["status"] == "pending"
    harness.resume()
    settled = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert settled["state"]["outcome"]["result"] == {"runs": 1}
    assert runs == [task_id]
    await harness.close()


async def test_restart_limit_parks_the_task_until_retry(storage_factory):
    runs = []
    reports = []
    task = make_task(runs)
    storage, task_id = await seed_running_task(storage_factory, task)
    harness = await open_harness(storage, task, limit=1, reports=reports)
    harness.resume()
    await asyncio.sleep(0.1)
    assert runs == []
    inspection = await harness.inspect()
    entry = next(item for item in inspection.tasks if item["record"]["id"] == task_id)
    assert entry["state"] == {"kind": "blocked", "reason": "restart_limit"}
    assert any(info["type"] == "task_parked" for _, info in reports)
    assert harness.wake()["active"] is False

    await harness.retry_task(task_id)
    settled = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert settled["state"]["outcome"]["status"] == "completed"
    assert runs == [task_id]
    await harness.close()


async def test_abandon_on_restart_marks_and_aborts_the_task(storage_factory):
    runs = []
    task = make_task(runs)
    storage, task_id = await seed_running_task(storage_factory, task, abandon=True)
    harness = await open_harness(storage, task)
    harness.resume()
    settled = await asyncio.wait_for(harness.wait_for_task(task_id), 5)
    assert settled["state"]["outcome"] == {"status": "aborted", "reason": "aborted"}
    assert runs == []
    await harness.close()

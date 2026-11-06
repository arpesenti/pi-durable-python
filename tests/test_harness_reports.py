"""Translated v1.5.0 ``harness-reports`` suite: one typed report per failure (spec §10.1).

``on_report(report)`` receives a single :class:`Report`: the fields of a ``ReportInfo`` tagged by
``kind``, beside the ``error``. A failed Session, a faulted task, a task crashes interrupted, a
failed migration, a running task that kept an outdated definition, a task's own failure, an
extension's code, a host callback, or a listener each say what happened in its ``kind``. Every task
report carries the task — ID, kind, conversation — and names the extension whose code failed, when
known. The port reports against every host-callback name it can reach; the telemetry member
upstream adds is excluded (ADR 0003) and asserted absent here rather than silently omitted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any, get_args, get_type_hints

from chat_support import FauxModels, assistant_message, chat_setup
from session_support import ControlledStorage, context, observe_commits_as_host
from task_support import completed, deferred, eventually

from pi_durable import (
    GenerationTask,
    Harness,
    MemoryStorage,
    define_extension,
    define_task,
    define_tool,
    hook,
    section,
    wrap_tool,
)
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import (
    HostCallback,
    Registry,
    Report,
    ReportInfo,
    ReportKind,
    ReportReason,
)

ReportEntry = dict[str, Any]


async def open_harness(
    tasks: Sequence[Any],
    options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Open a Harness with ``tasks`` installed that collects every report's message and info."""

    values = dict(options or {})
    registry: Registry = values.get("registry") or create_registry()
    if tasks:
        registry.install(define_extension({"name": "test", "tasks": list(tasks)}))
    reports: list[ReportEntry] = []

    def on_report(report: Report) -> None:
        # One object, split the way the upstream suite splits it: the message, and the info beside it.
        reports.append(
            {
                "message": str(report["error"]),
                "info": {key: value for key, value in report.items() if key != "error"},
            }
        )

    harness = await Harness.open(
        values.get("storage") or MemoryStorage(),
        {
            "models": values.get("models") or FauxModels(),
            "registry": registry,
            "on_report": on_report,
            **({"on_wake": values["on_wake"]} if "on_wake" in values else {}),
            **({"set_timer": values["set_timer"]} if "set_timer" in values else {}),
        },
        context,
    )
    return {"harness": harness, "registry": registry, "reports": reports}


def info_of(reports: Sequence[ReportEntry], message: str) -> Report:
    return next(report["info"] for report in reports if report["message"] == message)


async def test_say_which_task_reported_a_failure_or_faulted() -> None:
    async def run(_task: Any, runtime: Any, _ctx: Any) -> None:
        runtime.report(RuntimeError("noted"))
        raise RuntimeError("broken")

    Faulty = define_task(
        {
            "name": "test.faulty",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    opened = await open_harness([Faulty])
    harness = opened["harness"]
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Faulty, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()
    assert (await harness.wait_for_task(id, context))["state"]["outcome"]["status"] == "faulted"
    await eventually(lambda: len(opened["reports"]) == 2)
    # The task's own failures name the extension that installed it.
    task = {"id": id, "kind": "test.faulty", "conversationId": root.id}
    assert opened["reports"] == [
        {"message": "noted", "info": {"kind": "task_error", "task": task, "extension": "test"}},
        {
            "message": "broken",
            "info": {"kind": "task_faulted", "task": task, "extension": "test", "reason": "threw"},
        },
    ]
    await harness.close(context)


async def test_name_the_extension_whose_definition_ran_also_after_another_one_replaced_it() -> None:
    gate = deferred()
    started = deferred()

    def definition(name: str) -> Any:
        async def run(_task: Any, _runtime: Any, _ctx: Any) -> None:
            started.resolve()
            await gate.promise
            raise RuntimeError(f"broken in {name}")

        return define_task(
            {
                "name": "test.replaced",
                "version": 1,
                "initial": lambda _input: {"phase": "run"},
                "phases": {"run": run},
                "abort": lambda _task, _runtime, _ctx: None,
            }
        )

    Old = definition("old")
    opened = await open_harness([])
    harness = opened["harness"]
    registry = opened["registry"]
    old = define_extension({"name": "old-extension", "tasks": [Old]})
    registry.install(old)
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Old, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()
    await started.promise
    registry.uninstall(old)
    registry.install(define_extension({"name": "new-extension", "tasks": [definition("new")]}))
    gate.resolve()
    await harness.wait_for_task(id, context)

    def faulted(report: ReportEntry) -> bool:
        return report["info"]["kind"] == "task_faulted"

    await eventually(lambda: any(faulted(report) for report in opened["reports"]))
    found = next(report for report in opened["reports"] if faulted(report))
    assert found["message"] == "broken in old"
    assert found["info"]["extension"] == "old-extension"
    await harness.close(context)


async def test_names_the_extension_of_a_failing_section_or_hook_and_of_a_failing_env() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("answer")])

    def broken_section(*_args: Any) -> str:
        raise RuntimeError("section")

    def broken_hook(*_args: Any) -> None:
        raise RuntimeError("hook")

    setup.registry.install(
        define_extension({"name": "sections", "sections": [section("broken", broken_section)]})
    )
    setup.registry.install(
        define_extension(
            {"name": "hooks", "hooks": [hook(GenerationTask, {"afterResponse": broken_hook})]}
        )
    )
    reports: list[ReportEntry] = []

    def build_env(_target: Any, _context: Any) -> Any:
        raise RuntimeError("env")

    def on_report(report: Report) -> None:
        reports.append(
            {
                "message": str(report["error"]),
                "info": {key: value for key, value in report.items() if key != "error"},
            }
        )

    harness = await Harness.open(
        MemoryStorage(),
        {
            "models": setup.models,
            "registry": setup.registry,
            "env": build_env,
            "on_report": on_report,
        },
        context,
    )
    root = await harness.root(
        context, {"agent": {"model": {"provider": "faux", "modelId": "faux-1"}}}
    )
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    generation = {"kind": "pi.generation", "conversationId": root.id}
    env = info_of(reports, "env")
    assert env["kind"] == "host_error"
    assert env["callback"] == "env"
    assert env["task"]["kind"] == generation["kind"]
    assert env["task"]["conversationId"] == generation["conversationId"]
    failed_section = info_of(reports, "section")
    assert failed_section["kind"] == "extension_error"
    assert failed_section["extension"] == "sections"
    assert failed_section["task"]["kind"] == "pi.generation"
    failed_hook = info_of(reports, "hook")
    assert failed_hook["kind"] == "extension_error"
    assert failed_hook["extension"] == "hooks"
    await harness.close(context)


async def test_tell_extension_listener_wake_and_timer_failures_from_a_failed_session() -> None:
    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        # The host's set_timer throws: reported, and armed with the default timer instead.
        runtime.set_timer(lambda: None, 10)
        await runtime.sleep_until(runtime.now() + 60_000, {"phase": "done"}, ctx)

    async def done(_task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: completed(None), ctx)

    Sleeper = define_task(
        {
            "name": "test.sleeper",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run, "done": done},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    storage = ControlledStorage()

    def on_wake(_wake: Any) -> None:
        raise RuntimeError("alarm")

    def set_timer(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("no timers")

    opened = await open_harness(
        [Sleeper], {"storage": storage, "on_wake": on_wake, "set_timer": set_timer}
    )
    harness = opened["harness"]
    reports = opened["reports"]
    registry = opened["registry"]

    async def echo(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {}

    tool = define_tool(
        {"name": "echo", "description": "echo", "parameters": {"type": "object"}, "execute": echo}
    )
    registry.install(define_extension({"name": "tools", "tools": [tool]}))
    registry.install(
        define_extension(
            {
                "name": "wraps",
                "wraps": [
                    wrap_tool(tool, lambda inner: replace(inner, name="renamed")),
                ],
            }
        )
    )

    def broken_listener(_publication: Any, _context: Any) -> None:
        raise RuntimeError("listener")

    observe_commits_as_host(harness, broken_listener)
    root = await harness.root(context)
    await root.agent(context)
    await root.commit(
        lambda tx: tx.create_task(Sleeper, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()
    await eventually(
        lambda: all(
            any(report["message"] == message for report in reports)
            for message in ("alarm", "no timers")
        )
    )
    renamed = info_of(reports, "Wrapper renamed echo to renamed")
    assert renamed["extension"] == "wraps"
    assert info_of(reports, "listener") == {"kind": "listener_error"}
    assert info_of(reports, "alarm") == {"kind": "host_error", "callback": "onWake"}
    assert info_of(reports, "no timers") == {"kind": "host_error", "callback": "setTimer"}

    failure = RuntimeError("disk gone")
    storage.fail_next_commit(failure)
    try:
        await root.commit(
            lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
        )
    except Exception:
        pass
    await harness.closed
    assert info_of(reports, "disk gone") == {"kind": "storage_failed"}


async def test_one_report_carries_the_error_beside_the_kind_rather_than_beside_an_argument() -> None:
    """The host's sink is handed one object: a callback of one parameter, and the error itself."""

    async def run(_task: Any, runtime: Any, _ctx: Any) -> None:
        raise RuntimeError("broken")

    Faulty = define_task(
        {
            "name": "test.one-report",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _ctx: None,
        }
    )
    registry = create_registry()
    registry.install(define_extension({"name": "test", "tasks": [Faulty]}))
    received: list[Report] = []

    def on_report(report: Report) -> None:
        # Declared with one parameter: were the error and the info still two arguments, this call
        # would raise TypeError, the report would be dropped, and nothing would be collected.
        received.append(report)

    harness = await Harness.open(
        MemoryStorage(), {"models": FauxModels(), "registry": registry, "on_report": on_report}, context
    )
    root = await harness.root(context)
    id = await root.commit(
        lambda tx: tx.create_task(Faulty, None, {"ownership": {"kind": "conversation"}}), context
    )
    harness.resume()
    await harness.wait_for_task(id, context)
    await eventually(lambda: len(received) == 1)
    report = received[0]
    assert isinstance(report, Mapping)
    # The error is carried in the report, and is the exception the phase raised, not its message.
    assert isinstance(report["error"], RuntimeError)
    assert str(report["error"]) == "broken"
    assert {key: value for key, value in report.items() if key != "error"} == {
        "kind": "task_faulted",
        "task": {"id": id, "kind": "test.one-report", "conversationId": root.id},
        "extension": "test",
        "reason": "threw",
    }
    await harness.close(context)


def test_the_report_kinds_are_the_set_the_vendored_specification_carries() -> None:
    """Spec §10.1: the kinds, and the reason fields each of them explains."""

    assert get_args(ReportKind) == (
        "storage_failed",
        "task_faulted",
        "task_interrupted",
        "migration_failed",
        "task_outdated",
        "task_error",
        "extension_error",
        "host_error",
        "listener_error",
    )
    # The reasons, grouped as spec §10.1 groups them under each kind.
    reasons: dict[str, tuple[str, ...]] = {
        "task_faulted": ("threw", "restart_limit"),
        "task_interrupted": ("restart_limit", "interrupted"),
        "task_outdated": ("missing_task", "task_too_old", "migration_failed"),
    }
    assert {reason for group in reasons.values() for reason in group} == set(get_args(ReportReason))
    # A task crashes interrupted carries its restart count; the report is its info plus the error.
    assert "restarts" in get_type_hints(ReportInfo)
    assert "error" in get_type_hints(Report)
    assert "task_parked" not in get_args(ReportKind)
    assert "session_failed" not in get_args(ReportKind)


def test_the_host_callbacks_are_the_exhaustive_set_the_port_can_report_against() -> None:
    """Spec §10.1 names the host callbacks by their ``HarnessOptions`` name; ADR 0003 excludes one."""

    assert get_args(HostCallback) == (
        "now",
        "settings",
        "registry",
        "env",
        "models",
        "setTimer",
        "onWake",
    )
    # Upstream's ninth member is a telemetry adapter's throw while a commit is published; the port
    # carries no telemetry adapter, so no report can ever be made against it (ADR 0003).
    assert "telemetry" not in get_args(HostCallback)

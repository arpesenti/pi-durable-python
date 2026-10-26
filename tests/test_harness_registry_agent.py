"""Translated upstream ``harness-registry`` and registry-level ``harness-callers`` cases.

The end-to-end ``harness-callers`` cases that need a generation request and tool execution arrive
with #14/#18; the resolution they exercise is covered here through ``resolve_agent``.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from session_support import context
from task_support import aborted_with, completed, deferred, open_tasks

from pi_durable import CompactionTask, GenerationTask, MemoryStorage, ToolTask
from pi_durable.harness.agent import (
    DEFAULT_COMPACTION_POLICY,
    DEFAULT_RESTART_LIMIT,
    DEFAULT_RETRY_POLICY,
    AgentDoc,
    agent_hooks,
    resolve_agent,
    resolve_settings,
)
from pi_durable.harness.define import (
    define_extension,
    define_tool,
    hook,
    section,
    wrap_section,
    wrap_tool,
)
from pi_durable.harness.registry import create_registry
from pi_durable.tasks import define_task


def tool(name: str, extra: dict[str, Any] | None = None) -> Any:
    return define_tool(
        {"name": name, "description": f"{name} tool", "execute": lambda: None, **(extra or {})}
    )


def task(name: str, version: int = 1) -> Any:
    return define_task(
        {
            "name": name,
            "version": version,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": lambda task, runtime, ctx: None},
            "abort": lambda task, runtime, ctx: None,
        }
    )


def names(items: Any) -> list[str]:
    return [item.name for item in items]


def resolve(
    state: dict[str, Any] | None,
    snapshot: Any,
    settings: dict[str, Any] | None = None,
    reports: list[tuple[BaseException, str]] | None = None,
) -> Any:
    """Resolve ``state`` against ``snapshot`` and ``settings``, collecting reports and the extension."""

    sink = reports if reports is not None else []
    def report(error: BaseException, name: str) -> None:
        sink.append((error, name))

    return resolve_agent(state, snapshot, resolve_settings(settings), report)


def rendered_sections(agent: Any) -> list[tuple[str, str | None]]:
    return [(item.key, item.render({}, context)) for item in agent.sections]


def one_step(name: str, run: Any) -> Any:
    async def default_abort(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.commit(lambda: aborted_with("test"), ctx)

    return define_task(
        {
            "name": name,
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": default_abort,
        }
    )


async def start(conversation: Any, definition: Any) -> Any:
    return await conversation.commit(
        lambda tx: tx.create_task(
            definition,
            None,
            {"ownership": {"kind": "conversation"}, "background": False},
        ),
        context,
    )


# ── Registry ────────────────────────────────────────────────────────────────


def test_installs_replaces_in_place_and_uninstalls_extensions_by_name() -> None:
    registry = create_registry()
    published: list[str] = []
    registry.subscribe(lambda: published.append(",".join(names(registry.snapshot().installed()))))
    first = define_extension({"name": "a", "tools": [tool("read")]})
    second = define_extension({"name": "b", "tools": [tool("read"), tool("bash")]})
    registry.install(first)
    registry.install(second)
    before = registry.snapshot()
    # A new object with an installed name replaces it at its position.
    replacement = define_extension({"name": "a", "tools": [tool("grep")]})
    registry.install(replacement)
    assert names(registry.snapshot().installed()) == ["a", "b"]
    assert registry.snapshot().extension("a") is replacement
    assert [
        f"{item['tool'].name}@{item['extension'].name}" for item in registry.snapshot().tools()
    ] == ["grep@a", "read@b", "bash@b"]
    # Old snapshots stay as they were.
    assert before.extension("a") is first
    assert before.tools()[0]["tool"].name == "read"
    # Uninstall matches the name, whichever object; a later install appends.
    registry.uninstall(first)
    registry.uninstall(first)
    assert names(registry.snapshot().installed()) == ["b"]
    registry.install(first)
    assert names(registry.snapshot().installed()) == ["b", "a"]
    assert published == ["a", "a,b", "a,b", "b", "b,a"]


def test_validates_the_registry_as_it_would_be_after_an_install() -> None:
    registry = create_registry()
    installed = define_extension({"name": "tasks", "tasks": [task("app.index")]})
    registry.install(installed)
    published: list[int] = []
    registry.subscribe(lambda: published.append(1))
    before = registry.snapshot()
    invalid = [
        (define_extension({"name": "x", "tools": [tool("read"), tool("read")]}), "two tools named read"),
        (
            define_extension(
                {
                    "name": "x",
                    "sections": [
                        section("a", lambda *_args: "1"),
                        section("a", lambda *_args: "2"),
                    ],
                }
            ),
            "two sections",
        ),
        (define_extension({"name": "x", "sections": [section("Bad Key", lambda *_args: "")]}), "must match"),
        (
            define_extension({"name": "x", "sections": [section("instructions", lambda *_args: "")]}),
            "reserved",
        ),
        (define_extension({"name": "x", "tasks": [task("app.index")]}), "already installed"),
        (define_extension({"name": "x", "tasks": [task("pi.generation")]}), "already installed"),
        (define_extension({"name": "x", "tasks": [task("same"), task("same")]}), "already installed"),
    ]
    for extension, message in invalid:
        with pytest.raises((ValueError, TypeError), match=message):
            registry.install(extension)
    assert registry.snapshot() is before
    assert published == []
    # Replacing the extension that holds a task name is valid: the check runs after replacement.
    registry.install(define_extension({"name": "tasks", "tasks": [task("app.index", 2)]}))
    replaced = registry.snapshot().task("app.index")
    assert replaced is not None
    assert replaced.definition.version == 2


def test_holds_installed_task_definitions_and_removes_them_on_uninstall() -> None:
    registry = create_registry()
    assert [each.definition.name for each in registry.snapshot().tasks()] == [
        "pi.generation",
        "pi.tool",
        "pi.compaction",
    ]
    custom = task("app.custom")
    registry.install(define_extension({"name": "custom", "tasks": [custom]}))
    assert registry.snapshot().tasks() == [GenerationTask, ToolTask, CompactionTask, custom]
    assert registry.snapshot().task("app.custom") is custom
    registry.uninstall(define_extension({"name": "custom"}))
    assert registry.snapshot().task("app.custom") is None
    assert registry.snapshot().task("pi.generation") is GenerationTask
    assert registry.snapshot().task("pi.tool") is ToolTask


def test_keeps_the_default_of_a_progress_interval_given_as_undefined() -> None:
    settings = resolve_settings({"progress": {"partial_interval_ms": None, "output_interval_ms": 250}})
    assert settings.progress == {"partial_interval_ms": 100, "output_interval_ms": 250}


def test_resolves_settings_defaults_and_merges_object_fields() -> None:
    assert DEFAULT_RETRY_POLICY == {
        "enabled": True,
        "max_retries": 3,
        "base_delay_ms": 2_000,
        "max_agent_delay_ms": 60_000,
    }
    assert DEFAULT_COMPACTION_POLICY == {
        "enabled": True,
        "reserve_tokens": 16_384,
        "keep_recent_tokens": 20_000,
        "background_tokens": 32_768,
    }
    defaults = resolve_settings(None)
    assert defaults.retry == DEFAULT_RETRY_POLICY
    assert defaults.compaction == DEFAULT_COMPACTION_POLICY
    merged = resolve_settings(
        {"retry": {"max_retries": 1}, "compaction": {"enabled": False}}
    )
    assert merged.retry == {**DEFAULT_RETRY_POLICY, "max_retries": 1}
    assert merged.compaction == {**DEFAULT_COMPACTION_POLICY, "enabled": False}
    # The 1.4.0 defaults: a failed run keeps its inbox, and a task parks after five restarts.
    assert defaults.after_failed_run == "keep"
    assert defaults.restart_limit == DEFAULT_RESTART_LIMIT == 5
    assert resolve_settings({"after_failed_run": "withdraw"}).after_failed_run == "withdraw"
    # A restart limit is a number of restarts, rounded down; `inf` never parks, anything else
    # means the default.
    assert resolve_settings({"restart_limit": 2.9}).restart_limit == 2
    assert resolve_settings({"restart_limit": float("inf")}).restart_limit == float("inf")
    assert resolve_settings({"restart_limit": 0}).restart_limit == DEFAULT_RESTART_LIMIT
    assert resolve_settings({"restart_limit": None}).restart_limit == DEFAULT_RESTART_LIMIT


# ── Agent resolution ────────────────────────────────────────────────────────


def test_selects_the_default_an_array_or_the_default_edited_by_add_and_remove() -> None:
    coding = define_extension({"name": "coding", "tools": [tool("read")]})
    skills = define_extension({"name": "skills", "sections": [section("skills", lambda *_args: "S")]})
    reviewer = define_extension({"name": "reviewer", "sections": [section("role", lambda *_args: "R")]})
    registry = create_registry()
    for extension in (coding, skills, reviewer):
        registry.install(extension)
    snapshot = registry.snapshot()

    assert names(resolve(None, snapshot).extensions) == ["coding", "skills", "reviewer"]
    settings = {"extensions": [coding, skills]}
    assert names(resolve({}, snapshot, settings).extensions) == ["coding", "skills"]
    assert names(resolve({"extensions": ["reviewer", "coding"]}, snapshot, settings).extensions) == [
        "reviewer",
        "coding",
    ]
    # Add appends, remove drops, duplicates keep their first position, uninstalled names are skipped.
    edited = {"extensions": {"add": ["reviewer", "coding", "gone"], "remove": ["skills"]}}
    assert names(resolve(edited, snapshot, settings).extensions) == ["coding", "reviewer"]
    # An old object stands for its name: the installed extension is selected.
    stale = {"extensions": [define_extension({"name": "skills"}), skills]}
    assert resolve({}, snapshot, stale).extensions == [skills]


def test_skips_uninstalled_names_and_resolves_them_once_installed() -> None:
    coding = define_extension({"name": "coding", "tools": [tool("read")]})
    skills = define_extension({"name": "skills", "sections": [section("skills", lambda *_args: "S")]})
    registry = create_registry()
    registry.install(coding)
    state = {"extensions": ["coding", "skills"]}
    assert names(resolve(state, registry.snapshot()).extensions) == ["coding"]
    registry.install(skills)
    assert names(resolve(state, registry.snapshot()).extensions) == ["coding", "skills"]


def test_replaces_same_name_tools_in_place_wraps_the_winner_then_applies_the_filter() -> None:
    registry = create_registry()
    coding = define_extension(
        {"name": "coding", "tools": [tool("read"), tool("bash"), tool("edit")]}
    )
    venv = define_extension(
        {"name": "venv", "tools": [tool("bash", {"description": "venv bash"})]}
    )
    calls: list[str] = []

    def run(_args: Any, _api: Any, _context: Any) -> None:
        calls.append("grep")

    timing = define_extension(
        {
            "name": "timing",
            "wraps": [
                wrap_tool(
                    tool("bash"),
                    lambda item: replace(item, description=f"{item.description} (timed)"),
                ),
                wrap_tool(
                    tool("bash"),
                    lambda item: replace(item, description=f"{item.description} [2]"),
                ),
                # No `grep` is selected: the wrapper does nothing and reports nothing.
                wrap_tool(tool("grep", {"execute": run}), lambda item: item),
            ],
        }
    )
    for extension in (coding, venv, timing):
        registry.install(extension)
    reports: list[tuple[BaseException, str]] = []
    agent = resolve(None, registry.snapshot(), {}, reports)
    assert [(item.name, item.description) for item in agent.tools] == [
        ("read", "read tool"),
        ("bash", "venv bash (timed) [2]"),
        ("edit", "edit tool"),
    ]
    assert reports == []
    assert calls == []

    # An array keeps exactly these names in its order, a repeated name at its first position.
    filtered = resolve({"tools": ["edit", "missing", "read", "edit"]}, registry.snapshot())
    assert names(filtered.tools) == ["edit", "read"]
    assert names(resolve({"tools": {"remove": ["bash"]}}, registry.snapshot()).tools) == [
        "read",
        "edit",
    ]


def test_drops_a_tool_or_section_whose_wrapper_throws_or_renames_it_and_reports() -> None:
    registry = create_registry()
    coding = define_extension(
        {
            "name": "coding",
            "tools": [tool("read"), tool("bash"), tool("edit")],
            "sections": [
                section("preamble", lambda *_args: "You code.", tag=False),
                section("cwd", lambda *_args: "/repo"),
            ],
        }
    )

    def broken_tool(_item: Any) -> Any:
        raise RuntimeError("wrapper failed")

    def broken_section(_item: Any) -> Any:
        raise RuntimeError("section wrapper failed")

    broken = define_extension(
        {
            "name": "broken",
            "wraps": [
                wrap_tool(tool("read"), broken_tool),
                wrap_tool(tool("edit"), lambda item: replace(item, name="renamed")),
                wrap_section("cwd", broken_section),
            ],
        }
    )
    registry.install(coding)
    registry.install(broken)
    reports: list[tuple[BaseException, str]] = []
    agent = resolve(None, registry.snapshot(), {}, reports)
    assert names(agent.tools) == ["bash"]
    assert [item.key for item in agent.sections] == ["preamble"]
    assert [(str(error), extension) for error, extension in reports] == [
        ("wrapper failed", "broken"),
        ("Wrapper renamed edit to renamed", "broken"),
        ("section wrapper failed", "broken"),
    ]


def test_orders_sections_by_extension_replaces_same_keys_in_place_and_renders_instructions_last() -> None:
    registry = create_registry()
    coding = define_extension(
        {
            "name": "coding",
            "sections": [
                section("preamble", lambda *_args: "You code.", tag=False),
                section("cwd", lambda *_args: "/repo"),
            ],
        }
    )
    skills = define_extension({"name": "skills", "sections": [section("skills", lambda *_args: "S")]})

    def never(_item: Any) -> Any:
        raise AssertionError("instructions are not wrapped")

    override = define_extension(
        {
            "name": "override",
            "sections": [section("preamble", lambda *_args: "You review.", tag=False)],
            "wraps": [
                wrap_section(
                    "cwd",
                    lambda item: replace(
                        item,
                        render=lambda input, ctx: f"{item.render(input, ctx)}!",
                    ),
                ),
                wrap_section("instructions", never),
            ],
        }
    )
    for extension in (coding, skills, override):
        registry.install(extension)
    reports: list[tuple[BaseException, str]] = []
    agent = resolve({"instructions": "Be terse."}, registry.snapshot(), {}, reports)
    assert rendered_sections(agent) == [
        ("preamble", "You review."),
        ("cwd", "/repo!"),
        ("skills", "S"),
        ("instructions", "Be terse."),
    ]
    assert agent.sections[-1].tag is None
    assert reports == []


def test_collects_hooks_of_selected_extensions_in_shape_order_and_applies_field_defaults() -> None:
    registry = create_registry()
    first = {"beforeTool": lambda: None}
    second = {"beforeTool": lambda: None}
    on_yield = {"onYield": lambda: None}
    tool_task = task("pi.tool")
    generation_task = GenerationTask
    registry.install(
        define_extension(
            {
                "name": "a",
                "hooks": [hook(tool_task, first), hook(generation_task, on_yield)],
            }
        )
    )
    registry.install(define_extension({"name": "b", "hooks": [hook(tool_task, second)]}))
    a = {"extension": "a", "handlers": first}
    b = {"extension": "b", "handlers": second}
    assert agent_hooks(resolve(None, registry.snapshot()), "pi.tool") == [a, b]
    assert agent_hooks(resolve({"extensions": ["b", "a"]}, registry.snapshot()), "pi.tool") == [
        b,
        a,
    ]
    assert agent_hooks(resolve({"extensions": ["b"]}, registry.snapshot()), "pi.generation") == []

    defaults = resolve(None, registry.snapshot())
    assert defaults.model is None
    assert defaults.thinking_level == "off"
    assert defaults.cwd is None
    stored = resolve(
        {"model": {"provider": "p", "modelId": "m"}, "thinkingLevel": "high", "cwd": "/w"},
        registry.snapshot(),
    )
    assert stored.model == {"provider": "p", "modelId": "m"}
    assert stored.thinking_level == "high"
    assert stored.cwd == "/w"


# ── Callers and deferred tools ──────────────────────────────────────────────


def test_offers_the_model_only_tools_it_may_call_and_lets_tools_call_only_theirs() -> None:
    script = tool("script", {"callers": ["model"]})
    hidden = tool("hidden", {"callers": ["tools"]})
    both = tool("both")
    registry = create_registry()
    registry.install(
        define_extension({"name": "tools", "tools": [script, hidden, both, tool("probe")]})
    )
    agent = resolve(None, registry.snapshot())
    assert names(agent.tools) == ["script", "both", "probe"]
    assert names(agent.callable) == ["hidden", "both", "probe"]


def test_narrows_the_offered_tools_with_model_tools_without_narrowing_what_tools_may_call() -> None:
    read, bash, probe = tool("read"), tool("bash"), tool("probe")
    registry = create_registry()
    registry.install(define_extension({"name": "tools", "tools": [read, bash, probe]}))

    narrowed = resolve({"modelTools": ["probe"]}, registry.snapshot())
    assert names(narrowed.tools) == ["probe"]
    assert names(narrowed.callable) == ["read", "bash", "probe"]

    removed = resolve({"modelTools": {"remove": ["bash"]}}, registry.snapshot())
    assert names(removed.tools) == ["read", "probe"]

    assert names(resolve({"modelTools": None}, registry.snapshot()).tools) == [
        "read",
        "bash",
        "probe",
    ]


def test_never_offers_a_tool_that_is_not_enabled_nor_lets_tools_call_it() -> None:
    read, bash, probe = tool("read"), tool("bash"), tool("probe")
    registry = create_registry()
    registry.install(define_extension({"name": "tools", "tools": [read, bash, probe]}))
    # modelTools cannot bring back what tools disabled.
    agent = resolve(
        {"tools": {"remove": ["bash"]}, "modelTools": ["bash", "probe"]},
        registry.snapshot(),
    )
    assert names(agent.tools) == ["probe"]
    assert names(agent.callable) == ["read", "probe"]


def test_omits_deferred_tools_until_model_tools_loads_them() -> None:
    read, search, probe = tool("read"), tool("search", {"deferred": True}), tool("probe")
    registry = create_registry()
    registry.install(define_extension({"name": "tools", "tools": [read, search, probe]}))

    unloaded = resolve(None, registry.snapshot())
    assert names(unloaded.tools) == ["read", "probe"]
    assert names(unloaded.deferred) == ["search"]
    assert names(unloaded.callable) == ["read", "search", "probe"]

    # An array selects exactly its names, deferred ones too, in its order.
    loaded = resolve({"modelTools": ["search"]}, registry.snapshot())
    assert names(loaded.tools) == ["search"]
    assert loaded.deferred == []

    # An object appends the deferred tools in `add`, in add order; other names have no effect.
    appended = resolve(
        {"modelTools": {"add": ["search", "unknown", "read"]}}, registry.snapshot()
    )
    assert names(appended.tools) == ["read", "probe", "search"]
    assert appended.deferred == []

    # A remove wins over an add and still omits that deferred tool from the offered set.
    removed = resolve(
        {"modelTools": {"remove": ["search", "read"], "add": ["search"]}}, registry.snapshot()
    )
    assert names(removed.tools) == ["probe"]
    assert names(removed.deferred) == ["search"]


# ── Storing and reloading ───────────────────────────────────────────────────


async def test_stores_names_and_resolves_the_conversation_agent_from_them() -> None:
    read, bash = tool("read"), tool("bash")
    base = define_extension({"name": "tools", "tools": [read, bash]})
    extra = define_extension({"name": "extra", "tools": [tool("grep")]})
    registry = create_registry()
    registry.install(base)
    registry.install(extra)
    opened = await open_tasks(MemoryStorage(), [], {"registry": registry, "settings": {"extensions": [base]}})
    root = await opened["harness"].root(context)
    await root.configure({"extensions": {"add": [extra]}}, context)
    state = await opened["harness"].snapshot(AgentDoc, root.id, context)
    assert state is not None
    assert state["extensions"] == {"add": ["extra"]}
    assert names((await root.agent(context)).tools) == ["read", "bash", "grep"]
    await root.configure(
        {
            "model": {"provider": "p", "modelId": "m"},
            "thinking_level": "high",
            "instructions": "Be terse.",
            "cwd": "/work",
            "tools": [read],
            "model_tools": [read],
        },
        context,
    )
    state = await opened["harness"].snapshot(AgentDoc, root.id, context)
    assert state is not None
    assert state["tools"] == ["read"]
    assert state["modelTools"] == ["read"]
    assert state["model"] == {"provider": "p", "modelId": "m"}
    assert state["thinkingLevel"] == "high"
    assert state["cwd"] == "/work"
    agent = await root.agent(context)
    assert names(agent.tools) == ["read"]
    assert agent.model == {"provider": "p", "modelId": "m"}
    assert agent.thinking_level == "high"
    assert agent.cwd == "/work"
    assert rendered_sections(agent) == [("instructions", "Be terse.")]
    await root.configure(
        {"model": None, "cwd": None, "instructions": None, "extensions": None}, context
    )
    cleared = await root.agent(context)
    assert cleared.model is None
    assert cleared.cwd is None
    assert names(cleared.extensions) == ["tools"]
    assert rendered_sections(cleared) == []
    await root.configure({"bogus": "ignored"}, context)
    stored = await opened["harness"].snapshot(AgentDoc, root.id, context)
    assert stored is not None
    assert "bogus" not in stored
    await opened["harness"].close(context)


async def test_reloads_an_extension_in_place_without_reopening_the_session() -> None:
    seen: list[list[tuple[str, str | None]]] = []
    reached = deferred()
    release = deferred()

    async def first_phase(task: Any, runtime: Any, ctx: Any) -> None:
        seen.append(rendered_sections(await runtime.agent(ctx)))
        reached.resolve()
        await release.promise
        await runtime.commit(
            lambda: {"status": "running", "checkpoint": {"phase": "second"}}, ctx
        )

    async def second_phase(task: Any, runtime: Any, ctx: Any) -> None:
        seen.append(rendered_sections(await runtime.agent(ctx)))
        await runtime.commit(lambda: completed(None), ctx)

    Reload = define_task(
        {
            "name": "test.reload",
            "version": 1,
            "initial": lambda _input: {"phase": "first"},
            "phases": {"first": first_phase, "second": second_phase},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    registry = create_registry()
    registry.install(
        define_extension(
            {
                "name": "role",
                "sections": [section("role", lambda *_args: "v1")],
                "tools": [tool("v1")],
            }
        )
    )
    opened = await open_tasks(MemoryStorage(), [Reload], {"registry": registry})
    root = await opened["harness"].root(context)
    id = await start(root, Reload)
    opened["harness"].resume()
    await reached.promise
    registry.install(
        define_extension(
            {
                "name": "role",
                "sections": [section("role", lambda *_args: "v2")],
                "tools": [tool("v2")],
            }
        )
    )
    # The live conversation resolves again through the current registry, at its next phase.
    assert rendered_sections(await root.agent(context)) == [("role", "v2")]
    release.resolve()
    await opened["harness"].wait_for_task(id, context)
    assert seen == [[("role", "v1")], [("role", "v2")]]
    assert names((await root.agent(context)).tools) == ["v2"]
    await opened["harness"].close(context)


async def test_dispatch_hook_handlers_in_extension_order_and_contains_ordinary_throws() -> None:
    log: list[str] = []

    async def run(task: Any, runtime: Any, ctx: Any) -> None:
        await runtime.hooks.each("ping", lambda handler: handler())
        await runtime.commit(lambda: completed(None), ctx)

    Hooked = one_step("test.hooked", run)
    opened = await open_tasks(MemoryStorage(), [Hooked])

    def ping(label: str) -> Any:
        def handler() -> None:
            log.append(label)
            if label == "boom":
                raise RuntimeError("hook failed")

        return handler

    for name, label in (("one", "first"), ("two", "boom"), ("three", "last")):
        opened["registry"].install(
            define_extension({"name": name, "hooks": [hook(Hooked, {"ping": ping(label)})]})
        )
    id = await start(await opened["harness"].root(context), Hooked)
    opened["harness"].resume()
    await opened["harness"].wait_for_task(id, context)
    assert log == ["first", "boom", "last"]
    assert [str(error) for error in opened["reports"]] == ["hook failed"]
    await opened["harness"].close(context)

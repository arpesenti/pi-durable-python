"""Registry publication, agent resolution, and hook composition."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from pi_durable.harness.agent import agent_hooks, resolve_agent, resolve_settings
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


def tool(name: str) -> Any:
    return define_tool({"name": name, "description": f"{name} tool", "execute": lambda: None})


def test_resolves_tools_sections_wraps_and_hooks_in_extension_order() -> None:
    alpha, beta = tool("alpha"), tool("beta")
    greet = section("greet", lambda _input: "hello")
    reports: list[BaseException] = []
    registry = create_registry()
    registry.install(define_extension({"name": "base", "tools": [alpha, beta], "sections": [greet]}))
    task = define_task(
        {
            "name": "test.hooked",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": lambda task, runtime, ctx: None},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    registry.install(
        define_extension(
            {
                "name": "wraps",
                "wraps": [
                    wrap_tool(alpha, lambda item: replace(item, description="wrapped")),
                    wrap_section("greet", lambda item: replace(item, tag=False)),
                ],
                "hooks": [hook(task, {"ping": lambda: None})],
            }
        )
    )
    agent = resolve_agent(None, registry.snapshot(), resolve_settings(None), reports.append)
    assert [item.name for item in agent.tools] == ["alpha", "beta"]
    assert agent.tools[0].description == "wrapped"
    assert [(item.key, item.tag) for item in agent.sections] == [("greet", False)]
    assert agent.thinking_level == "off"
    assert agent_hooks(agent, "test.hooked")[0]["ping"] is not None
    assert reports == []


def test_installing_an_extension_with_the_same_name_replaces_it_in_place() -> None:
    registry = create_registry()
    first = define_extension({"name": "tools", "tools": [tool("alpha")]})
    registry.install(first)
    registry.install(define_extension({"name": "tools", "tools": [tool("beta")]}))
    agent = resolve_agent(None, registry.snapshot(), resolve_settings(None), lambda _error: None)
    assert [item.name for item in agent.tools] == ["beta"]
    registry.uninstall(first)
    agent = resolve_agent(None, registry.snapshot(), resolve_settings(None), lambda _error: None)
    assert agent.tools == []
    assert registry.snapshot().installed() == ()


def test_rejects_duplicate_task_names_and_reserved_section_keys() -> None:
    registry = create_registry()
    task = define_task(
        {
            "name": "test.duplicate",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": lambda task, runtime, ctx: None},
            "abort": lambda task, runtime, ctx: None,
        }
    )
    registry.install(define_extension({"name": "one", "tasks": [task]}))
    with pytest.raises(ValueError, match="already installed"):
        registry.install(define_extension({"name": "two", "tasks": [task]}))
    with pytest.raises(ValueError, match="reserved"):
        registry.install(
            define_extension({"name": "bad", "sections": [section("instructions", lambda _i: "")]})
        )


def test_resolved_agent_reads_the_snapshot_it_was_given() -> None:
    registry = create_registry()
    registry.install(define_extension({"name": "tools", "tools": [tool("alpha")]}))
    snapshot = registry.snapshot()
    registry.install(define_extension({"name": "tools", "tools": [tool("beta")]}))
    agent = resolve_agent(None, snapshot, resolve_settings(None), lambda _error: None)
    assert [item.name for item in agent.tools] == ["alpha"]

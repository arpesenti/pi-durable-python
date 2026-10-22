"""Translated upstream ``harness-prompt`` cases: system prompt and tool loadout preparation.

The prompt planner is exercised directly against committed conversations. Request-time
integration (generation's ``prepare`` phase) lands with #14; the ordering regression the live
``system-order-cache-e2e`` suite covers is translated in ``test_system_order_cache_e2e.py``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import Any, cast

import pytest
from harness_support import add_section, open_harness, tool, user
from session_support import context

from pi_durable.context import AbortController, with_abort_signal
from pi_durable.harness.agent import resolve_agent, resolve_settings
from pi_durable.harness.define import define_extension, define_tool, section, wrap_section
from pi_durable.harness.prompt import (
    current_tools,
    declarations_equal,
    plan_system_entries,
    render_sections,
    replay_sections,
    to_tool_declaration,
)
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import Agent, PromptInput, PromptSection, ToolRegistration
from pi_durable.ids import ConversationId, EntryId


class Reader:
    """A document reader with no committed documents."""

    async def snapshot(self, *_args: Any) -> None:
        return None

    async def snapshot_as_of(self, *_args: Any) -> None:
        return None


def prompt_input(agent: Agent, shown: dict[str, str] | None = None) -> PromptInput:
    return PromptInput(
        conversation_id=cast(ConversationId, 1),
        agent=agent,
        env=None,
        shown={} if shown is None else dict(shown),
        read=Reader(),
    )


def resolved_agent(registry: Any, state: dict[str, Any] | None = None) -> Agent:
    def fail(error: BaseException) -> None:
        raise error

    return resolve_agent(state, registry.snapshot(), resolve_settings(None), fail)


@asynccontextmanager
async def root() -> AsyncIterator[Any]:
    opened = await open_harness()
    harness = opened["harness"]
    try:
        yield await harness.root(context)
    finally:
        await harness.close(context)


def planned(draft: Any) -> dict[str, Any]:
    message = draft["model"][0]
    assert message["role"] == "system"
    assert message["content"] == ""
    assert message["timestamp"] == 7
    result: dict[str, Any] = {"sections": dict(message.get("sections") or {})}
    if "edits" in draft:
        assert all(edit["action"] == "omit" for edit in draft["edits"])
        result["omit"] = [edit["target"] for edit in draft["edits"]]
    return result


async def apply(conversation: Any, desired: dict[str, str]) -> list[dict[str, Any]]:
    """Plan against the current context, append the plan, and check that replay yields ``desired``."""

    drafts = plan_system_entries(
        await conversation.context(context), dict(desired), [], 7
    )

    async def append(tx: Any) -> None:
        for draft in drafts:
            await tx.append_entry(conversation.id, draft)

    await conversation.commit(append, context)
    replayed = replay_sections((await conversation.context(context))["messages"])
    assert list(replayed.items()) == list(desired.items())
    return [planned(draft) for draft in drafts]


async def last_system_id(conversation: Any) -> EntryId:
    page = await conversation.entries({}, 100, None, context)
    return cast(
        EntryId,
        next(entry for entry in page["items"] if entry["kind"] == "pi.system")["id"],
    )


async def marker(conversation: Any, head: EntryId | str) -> EntryId:
    async def append(tx: Any) -> EntryId:
        entry = await tx.append_entry(
            conversation.id,
            {"kind": "summary", "head": head, "model": [user("summary")]},
        )
        return cast(EntryId, entry["id"])

    return await conversation.commit(append, context)


async def _cwd(_input: PromptInput, _context: Any) -> str:
    return "/repo"


def _failing(*_args: Any) -> str:
    raise RuntimeError("render failed")


def _new_failing(*_args: Any) -> str:
    raise RuntimeError("also failed")


def _with_git(inner: PromptSection) -> Callable[..., Any]:
    async def render(value: Any, ctx: Any) -> str:
        rendered = inner.render(value, ctx)
        if hasattr(rendered, "__await__"):
            rendered = await rendered
        return f"{rendered} (git)"

    return render


def _cancelled(*_args: Any) -> str:
    raise RuntimeError("cancelled")


# ── Section rendering ──────────────────────────────────────────────────────


async def test_renders_sections_in_order_with_tags_omissions_wrappers_and_failures() -> None:
    registry = create_registry()
    add_section(registry, "preamble", lambda *_args: "You are helpful.", {"tag": False})
    add_section(registry, "cwd", _cwd)
    add_section(registry, "skipped", lambda *_args: None)
    add_section(registry, "failing", _failing)
    add_section(registry, "new-failing", _new_failing)
    registry.install(
        define_extension(
            {
                "name": "git",
                "wraps": [
                    wrap_section(
                        "cwd",
                        lambda inner: replace(inner, render=_with_git(inner)),
                    )
                ],
            }
        )
    )
    reports: list[BaseException] = []
    shown = {"failing": "<failing>\nold\n</failing>", "cwd": "stale"}
    agent = resolved_agent(registry)
    desired = await render_sections(
        agent.sections,
        prompt_input(agent, shown),
        shown,
        reports.append,
        context,
    )
    assert list(desired.items()) == [
        ("preamble", "You are helpful."),
        ("cwd", "<cwd>\n/repo (git)\n</cwd>"),
        ("failing", "<failing>\nold\n</failing>"),
    ]
    assert [str(error) for error in reports] == ["render failed", "also failed"]


async def test_propagates_section_errors_after_cancellation() -> None:
    controller = AbortController()
    controller.abort(RuntimeError("cancelled"))
    cancelled = with_abort_signal(controller.signal, context)
    failing = section("a", _cancelled)
    agent = resolved_agent(create_registry())
    with pytest.raises(RuntimeError, match="cancelled"):
        await render_sections(
            [failing], prompt_input(agent), {}, lambda _error: None, cancelled
        )


def test_replays_sections_in_place_deletes_on_null_and_appends_readditions() -> None:
    def message(sections: dict[str, str | None]) -> dict[str, Any]:
        return {"role": "system", "content": "", "sections": sections, "timestamp": 1}

    shown = replay_sections(
        [
            message({"a": "1", "b": "2", "c": "3"}),
            user("x"),
            message({"b": "20", "a": None}),
            message({"a": "10"}),
        ]
    )
    assert list(shown.items()) == [("b", "20"), ("c", "3"), ("a", "10")]


# ── Section planning ───────────────────────────────────────────────────────


async def test_emits_minimal_value_patches_removals_and_additions() -> None:
    async with root() as conversation:
        assert await apply(conversation, {"a": "1", "b": "2", "c": "3"}) == [
            {"sections": {"a": "1", "b": "2", "c": "3"}}
        ]
        assert await apply(conversation, {"a": "1", "b": "20", "c": "3", "d": "4"}) == [
            {"sections": {"b": "20", "d": "4"}}
        ]
        assert await apply(conversation, {"a": "1", "c": "3", "d": "4"}) == [
            {"sections": {"b": None}}
        ]
        assert await apply(conversation, {"a": "1", "c": "3", "d": "4"}) == []
        assert await apply(conversation, {}) == [
            {"sections": {"a": None, "c": None, "d": None}}
        ]


async def test_rewrites_order_only_changes_and_readditions_as_two_entries() -> None:
    async with root() as conversation:
        await apply(conversation, {"a": "1", "b": "2"})
        assert await apply(conversation, {"b": "2", "a": "1"}) == [
            {"sections": {"a": None, "b": None}},
            {"sections": {"b": "2", "a": "1"}},
        ]

    async with root() as readded:
        await apply(readded, {"a": "1", "b": "2", "c": "3"})
        assert await apply(readded, {"a": "1", "c": "3"}) == [
            {"sections": {"b": None}}
        ]
        # Patching would append `b` after `c`.
        assert await apply(readded, {"a": "1", "b": "2", "c": "3"}) == [
            {"sections": {"a": None, "c": None}},
            {"sections": {"a": "1", "b": "2", "c": "3"}},
        ]


async def test_rebaselines_after_a_head_marker_omitting_retained_deltas_on_both_sides() -> None:
    async with root() as conversation:
        await apply(conversation, {"a": "1", "b": "2"})

        async def append_user(tx: Any) -> None:
            await tx.append_entry(
                conversation.id, {"kind": "pi.user", "model": [user("hi")]}
            )

        await conversation.commit(append_user, context)
        await apply(conversation, {"a": "1", "b": "20"})
        delta = await last_system_id(conversation)
        # The head keeps the delta but cuts its baseline: replay alone would show only `b`.
        await marker(conversation, delta)
        assert await apply(conversation, {"a": "1", "b": "20"}) == [
            {"sections": {"a": "1", "b": "20"}, "omit": [delta]}
        ]
        baseline = await last_system_id(conversation)
        # A system entry follows the marker now, so later changes are ordinary patches.
        assert await apply(conversation, {"a": "1", "b": "21"}) == [
            {"sections": {"b": "21"}}
        ]
        after = await last_system_id(conversation)
        # A second marker keeps deltas from both sides of the first one.
        await marker(conversation, delta)
        assert await apply(conversation, {"a": "1", "b": "21"}) == [
            {"sections": {"a": "1", "b": "21"}, "omit": [delta, baseline, after]}
        ]


async def test_writes_a_complete_post_head_baseline_even_when_replay_already_matches() -> None:
    async with root() as conversation:
        await apply(conversation, {"a": "1"})
        baseline = await last_system_id(conversation)
        await marker(conversation, baseline)
        assert await apply(conversation, {"a": "1"}) == [
            {"sections": {"a": "1"}, "omit": [baseline]}
        ]
        await marker(conversation, "self")
        assert await apply(conversation, {}) == [{"sections": {}}]
        assert await apply(conversation, {}) == []


# ── Tool loadout planning ──────────────────────────────────────────────────


def test_compares_only_the_provider_facing_declaration() -> None:
    sampling = {"type": "json_schema", "strict": "prefer"}
    first = define_tool(
        {
            "name": "read",
            "description": "read a file",
            "parameters": {"type": "object", "properties": {}},
            "replay": "safe",
            "execution_mode": "parallel",
            "constrained_sampling": sampling,
        }
    )
    same = define_tool(
        {
            "name": "read",
            "description": "read a file",
            "parameters": {"type": "object", "properties": {}},
            "replay": "unsafe",
            "execution_mode": "sequential",
            "constrained_sampling": dict(sampling),
        }
    )
    assert declarations_equal(first, same)
    assert to_tool_declaration(first) == {
        "name": "read",
        "description": "read a file",
        "parameters": {"type": "object", "properties": {}},
        "constrainedSampling": sampling,
    }
    assert to_tool_declaration(tool("read", "read a file")) == {
        "name": "read",
        "description": "read a file",
        "parameters": {"type": "object", "properties": {}},
    }
    assert not declarations_equal(first, tool("read", "changed"))


async def apply_tools(
    conversation: Any,
    tools: list[ToolRegistration],
    sections: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Plan tools (and optionally sections), append the plan, and return each message's changes."""

    drafts = plan_system_entries(
        await conversation.context(context),
        dict(sections or {}),
        tools,
        7,
    )

    async def append(tx: Any) -> None:
        for draft in drafts:
            await tx.append_entry(conversation.id, draft)

    await conversation.commit(append, context)
    offered = current_tools((await conversation.context(context))["messages"])
    assert offered == [to_tool_declaration(tool) for tool in tools]
    changes: list[dict[str, Any]] = []
    for draft in drafts:
        message = cast(dict[str, Any], draft["model"][0])
        result: dict[str, Any] = {}
        if "toolsRemoved" in message:
            result["removed"] = [tool["name"] for tool in message["toolsRemoved"]]
        if "toolsAdded" in message:
            result["added"] = [tool["name"] for tool in message["toolsAdded"]]
        if "sections" in message:
            result["sections"] = dict(message["sections"])
        changes.append(result)
    return changes


async def test_adds_removes_replaces_changed_declarations_and_rewrites_order_when_needed() -> None:
    async with root() as conversation:
        a, b, c = tool("a"), tool("b"), tool("c")
        assert await apply_tools(conversation, [a, b]) == [{"added": ["a", "b"]}]
        assert await apply_tools(conversation, [a, b]) == []
        assert await apply_tools(conversation, [a, b, c]) == [{"added": ["c"]}]
        assert await apply_tools(conversation, [a, c]) == [{"removed": ["b"]}]
        # A changed declaration at the end is removed and re-added in place.
        c2 = tool("c", "changed")
        assert await apply_tools(conversation, [a, c2]) == [
            {"removed": ["c"], "added": ["c"]}
        ]
        # A changed declaration in the middle would move to the end, so the whole order is rewritten.
        a2 = tool("a", "changed")
        assert await apply_tools(conversation, [a2, c2]) == [
            {"removed": ["a", "c"], "added": ["a", "c"]}
        ]
        # Order-only change.
        assert await apply_tools(conversation, [c2, a2]) == [
            {"removed": ["a", "c"], "added": ["c", "a"]}
        ]
        assert await apply_tools(conversation, []) == [{"removed": ["c", "a"]}]


async def test_puts_tool_changes_on_the_last_section_entry_and_redeclares_tools_after_a_head_cut() -> None:
    async with root() as conversation:
        a, b = tool("a"), tool("b")
        assert await apply_tools(conversation, [a], {"x": "1", "y": "2"}) == [
            {"added": ["a"], "sections": {"x": "1", "y": "2"}}
        ]
        # Section order changes need two entries; the tool change rides on the second.
        assert await apply_tools(conversation, [a, b], {"y": "2", "x": "1"}) == [
            {"sections": {"x": None, "y": None}},
            {"added": ["b"], "sections": {"y": "2", "x": "1"}},
        ]
        await marker(conversation, "self")
        assert await apply_tools(conversation, [a, b], {"y": "2", "x": "1"}) == [
            {"added": ["a", "b"], "sections": {"y": "2", "x": "1"}}
        ]

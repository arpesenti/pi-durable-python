"""Translated upstream ``system-order-cache-e2e`` regression coverage.

The upstream suite measures Anthropic's prompt-cache hit rate across a tool change. That live
measurement needs Anthropic credentials, so it stays skipped; the ordering regression it protects is
translated here against committed transcripts: the baseline must lead the request even when the
user entry was written before it, and a later tool change must arrive positionally without moving
the baseline. The Anthropic adapter itself is covered with fakes in test_providers_anthropic.py.
"""

from __future__ import annotations

from typing import Any

import pytest
from harness_support import open_harness, tool, user
from session_support import context

from pi_durable.harness.prompt import current_tools, plan_system_entries
from pi_durable.harness.types import ToolRegistration


async def test_keeps_the_system_baseline_leading_across_a_tool_change() -> None:
    opened = await open_harness()
    harness = opened["harness"]
    try:
        conversation = await harness.root(context)
        first = tool("probe_first")
        second = tool("probe_second")

        async def append_user(text: str) -> None:
            async def change(tx: Any) -> None:
                await tx.append_entry(
                    conversation.id,
                    {"kind": "pi.user", "model": [user(text)]},
                )

            await conversation.commit(change, context)

        # The input is committed before generation prepares the baseline.
        await append_user("READY")

        async def append_system(desired: dict[str, str], tools: list[ToolRegistration]) -> None:
            drafts = plan_system_entries(
                await conversation.context(context), desired, tools, 7
            )
            assert drafts

            async def change(tx: Any) -> None:
                for draft in drafts:
                    await tx.append_entry(conversation.id, draft)

            await conversation.commit(change, context)

        await append_system({"preamble": "You are a probe."}, [first])
        view = await conversation.context(context)
        assert view["messages"][0]["role"] == "system"
        assert view["messages"][1]["role"] == "user"
        assert [tool["name"] for tool in current_tools(view["messages"])] == [
            "probe_first"
        ]

        # A second turn adds a tool. The baseline stays leading and the delta is positional.
        await append_user("READY AGAIN")
        await append_system({"preamble": "You are a probe."}, [first, second])
        after = await conversation.context(context)
        assert [message["role"] for message in after["messages"]] == [
            "system",
            "user",
            "user",
            "system",
        ]
        assert after["messages"][0]["sections"] == {"preamble": "You are a probe."}
        assert [tool["name"] for tool in current_tools(after["messages"])] == [
            "probe_first",
            "probe_second",
        ]
        assert [entry["kind"] for entry in after["entries"]].count("pi.system") == 2
    finally:
        await harness.close(context)


@pytest.mark.skip(
    reason="Measures Anthropic prompt-cache reads across a tool change; needs live credentials."
)
async def test_cache_read_share_across_a_tool_change() -> None:
    raise AssertionError("covered with the provider cache e2e")

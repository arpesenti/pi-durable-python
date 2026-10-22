"""Translated upstream usage cases: per-conversation ``pi.usage`` and the Session total."""

from __future__ import annotations

from typing import Any

from chat_support import (
    all_entries,
    assistant_message,
    chat_setup,
    open_chat,
    usage,
)
from session_support import context

from pi_durable import MemoryStorage, UsageDoc, UsageState
from pi_durable.harness.usage import add_usage, add_usage_state, record_usage


async def test_records_an_assistant_entry_under_its_provider_and_model() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("hi", usage=usage(input=10, output=5))])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    state = await harness.snapshot(UsageDoc, root.id, context)
    assert state == {
        "models": {"faux/faux-1": usage(input=10, output=5)},
        "tools": {},
    }
    assert [entry["kind"] for entry in await all_entries(root)] == ["pi.user", "pi.assistant"]
    await harness.close(context)


async def test_counts_failed_and_retried_attempts_and_sums_conversations() -> None:
    setup = chat_setup()
    setup.settings["retry"] = {"enabled": True, "max_retries": 1, "base_delay_ms": 1}
    setup.faux.set_responses(
        [
            assistant_message(
                (),
                stop_reason="error",
                error_message="503 Service Unavailable",
                usage=usage(input=3, output=1),
            ),
            assistant_message("ok", usage=usage(input=4, output=2)),
        ]
    )
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    other = await harness.create_conversation(
        {
            "ownership": {"kind": "ownerless"},
            "agent": {"model": {"provider": "faux", "modelId": "faux-1"}},
        },
        context,
    )
    harness.resume()
    await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    setup.faux.set_responses([assistant_message("child", usage=usage(input=7, output=3))])
    await (await other.submit({"type": "input", "content": "hi"}, context)).wait(context)
    root_state = await harness.snapshot(UsageDoc, root.id, context)
    assert root_state["models"]["faux/faux-1"] == usage(input=7, output=3)
    total = await harness.usage(context)
    assert total["models"]["faux/faux-1"] == usage(input=14, output=6)
    assert total["tools"] == {}
    await harness.close(context)


async def test_a_fork_starts_at_zero() -> None:
    setup = chat_setup()
    setup.faux.set_responses([assistant_message("hi", usage=usage(input=10, output=5))])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    settled = await (await root.submit({"type": "input", "content": "hi"}, context)).wait(context)
    child = await root.fork(settled["entry"], {"ownership": {"kind": "ownerless"}}, context)
    assert await harness.snapshot(UsageDoc, child.id, context) == {
        "models": {},
        "tools": {},
    }
    await harness.close(context)


async def test_adds_optional_counters_only_once_either_side_reports_them() -> None:
    total = usage(input=1)
    add_usage(total, {**usage(input=2), "cacheWrite1h": 3, "reasoning": 4})
    assert total["cacheWrite1h"] == 3
    assert total["reasoning"] == 4
    add_usage(total, usage(input=1))
    assert total["cacheWrite1h"] == 3
    assert total["reasoning"] == 4


async def test_add_usage_state_keeps_bucket_totals() -> None:
    sum_value: UsageState = {"models": {}, "tools": {}}
    add_usage_state(
        sum_value,
        {
            "models": {"faux/faux-1": usage(input=1)},
            "tools": {"toString": usage(output=2)},
        },
    )
    add_usage_state(
        sum_value,
        {
            "models": {"faux/faux-1": usage(input=2)},
            "tools": {"toString": usage(output=3)},
        },
    )
    assert sum_value["models"]["faux/faux-1"] == usage(input=3)
    assert sum_value["tools"]["toString"] == usage(output=5)


async def test_records_into_a_conversation_document_in_the_writer_commit() -> None:
    setup = chat_setup()
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]

    async def write(tx: Any) -> None:
        await record_usage(tx, root.id, "models", "faux/x", usage(input=2))

    await root.commit(write, context)
    assert (await harness.snapshot(UsageDoc, root.id, context))["models"]["faux/x"] == usage(
        input=2
    )
    await harness.close(context)

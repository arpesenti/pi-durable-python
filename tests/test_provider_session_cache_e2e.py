"""Translated upstream ``provider-session-cache-e2e`` cases.

The upstream suite performs a live Codex prompt-cache measurement and skips without credentials, so
that measurement stays skipped here. The durable half — one provider session ID per conversation,
reused on every request — runs against fake models; test_providers_openai.py checks the adapter half
by forwarding that identity as OpenAI's prompt-cache key.
"""

from __future__ import annotations

import os
import re
from typing import Any

import pytest
from chat_support import all_entries, assistant_message, chat_setup, open_chat, usage
from session_support import context

from pi_durable import AssistantEntry, MemoryStorage, ProviderDoc, UsageDoc

LIVE = bool(os.environ.get("OPENAI_CODEX_TOKEN"))


@pytest.mark.skipif(not LIVE, reason="live Codex prompt-cache measurement needs credentials")
async def test_reuses_the_codex_prompt_cache_across_durable_turns() -> None:
    raise AssertionError("live credentials are required")


async def test_forwards_one_session_id_for_every_turn_of_a_conversation() -> None:
    setup = chat_setup()
    forwarded: list[str] = []
    cache_read: list[int] = []

    def capture(_request: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
        forwarded.append(options.get("sessionId", ""))
        turn = len(forwarded)
        return assistant_message(
            f"answer {turn}",
            usage=usage(input=100, cache_read=0 if turn == 1 else 100),
        )

    setup.faux.set_responses([capture, capture, capture])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    for turn in range(3):
        settled = await (
            await root.submit({"type": "input", "content": f"turn {turn}"}, context)
        ).wait(context)
        assert settled["status"] == "done"
        answer = settled["answer"]
        message = await root.commit(
            lambda tx, entry=answer: tx.entry(AssistantEntry, entry), context
        )
        cache_read.append(message["model"][0]["usage"]["cacheRead"])
    session_id = (await harness.snapshot(ProviderDoc, root.id, context))["sessionId"]
    assert re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
        session_id,
    )
    assert forwarded == [session_id, session_id, session_id]
    assert cache_read == [0, 100, 100]
    assert [entry["kind"] for entry in await all_entries(root)].count("pi.assistant") == 3
    await harness.close(context)

async def test_every_created_and_forked_conversation_gets_its_own_identity() -> None:
    setup = chat_setup()
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    child = await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    entry = await root.commit(
        lambda tx: tx.append_entry(root.id, {"kind": "note", "data": {"text": "x"}}), context
    )
    forked = await root.fork(entry["id"], {"ownership": {"kind": "ownerless"}}, context)
    identities = [
        (await harness.snapshot(ProviderDoc, conversation.id, context))["sessionId"]
        for conversation in (root, child, forked)
    ]
    assert len(set(identities)) == 3
    for session_id in identities:
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            session_id,
        )
    assert await harness.snapshot(UsageDoc, forked.id, context) == {"models": {}, "tools": {}}
    await harness.close(context)

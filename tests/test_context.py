from pi_durable import MemoryStorage, create_session
from pi_durable.context import (
    assistant_message,
    read_context,
    system_message,
    text_content,
    tool_call,
    tool_result_message,
    user_message,
)


async def _conversation(session):
    return await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )


async def _append(session, conversation_id, kind, *messages, **extra):
    return await session.commit(
        lambda tx: tx.append_entry(
            conversation_id, {"kind": kind, "model": list(messages), **extra}
        )
    )


async def test_head_marker_resets_context_to_self():
    session = create_session(MemoryStorage())
    conversation = await _conversation(session)
    cid = conversation["id"]
    await _append(session, cid, "pi.user", user_message("old question"))
    await _append(session, cid, "pi.assistant", assistant_message([text_content("old answer")]))
    await _append(
        session,
        cid,
        "pi.reset",
        user_message("handoff"),
        head="self",
    )
    await _append(session, cid, "pi.user", user_message("new question"))

    view = await read_context(session, cid)
    assert [message["role"] for message in view["messages"]] == ["user", "user"]
    assert view["messages"][0]["content"] == "handoff"
    assert view["entries"][0]["kind"] == "pi.reset"
    await session.close()


async def test_newest_edit_wins_for_omit_and_replace():
    session = create_session(MemoryStorage())
    conversation = await _conversation(session)
    cid = conversation["id"]
    target = await _append(session, cid, "pi.user", user_message("original"))
    newer = await _append(
        session,
        cid,
        "app.edit",
        edits=[{"target": target["id"], "action": "replace", "messages": [user_message("replaced")]}],
    )
    await _append(
        session,
        cid,
        "app.edit",
        edits=[{"target": target["id"], "action": "omit"}],
    )
    await _append(
        session,
        cid,
        "app.edit",
        edits=[{"target": newer["id"], "action": "omit"}],
    )
    await _append(session, cid, "pi.user", user_message("tail"))

    view = await read_context(session, cid)
    assert [message["content"] for message in view["messages"]] == ["tail"]
    await session.close()


async def test_tool_results_move_after_their_assistant_in_call_order():
    session = create_session(MemoryStorage())
    conversation = await _conversation(session)
    cid = conversation["id"]
    await _append(
        session,
        cid,
        "pi.assistant",
        assistant_message(
            [
                tool_call("a", "first", {}),
                tool_call("b", "second", {}),
            ],
            stop_reason="toolUse",
        ),
    )
    # A steer is committed before the results, but results must lead it.
    await _append(session, cid, "pi.user", user_message("steer"))
    await _append(session, cid, "pi.tool-result", tool_result_message("b", "second", [text_content("B")]))
    await _append(session, cid, "pi.tool-result", tool_result_message("a", "first", [text_content("A")]))

    view = await read_context(session, cid)
    assert [message["role"] for message in view["messages"]] == [
        "assistant",
        "toolResult",
        "toolResult",
        "user",
    ]
    assert [message.get("toolCallId") for message in view["messages"][1:3]] == ["a", "b"]
    await session.close()


async def test_missing_tool_result_is_synthesized_and_orphans_are_dropped():
    session = create_session(MemoryStorage())
    conversation = await _conversation(session)
    cid = conversation["id"]
    await _append(session, cid, "pi.tool-result", tool_result_message("orphan", "x", [text_content("orphan")]))
    await _append(
        session,
        cid,
        "pi.assistant",
        assistant_message([tool_call("call", "run", {})], stop_reason="toolUse"),
    )
    await _append(session, cid, "pi.user", user_message("next"))

    view = await read_context(session, cid)
    assert [message["role"] for message in view["messages"]] == ["assistant", "toolResult", "user"]
    assert view["messages"][1]["isError"] is True
    assert view["messages"][1]["details"] == {"reason": "missing_result"}
    await session.close()


async def test_failed_assistant_messages_are_excluded_from_future_requests():
    session = create_session(MemoryStorage())
    conversation = await _conversation(session)
    cid = conversation["id"]
    for reason in ("aborted", "error", "deferred"):
        await _append(
            session,
            cid,
            "pi.assistant",
            assistant_message([text_content(reason)], stop_reason=reason),
        )
    await _append(session, cid, "pi.assistant", assistant_message([text_content("ok")], stop_reason="stop"))
    view = await read_context(session, cid)
    assert [message["content"][0]["text"] for message in view["messages"]] == ["ok"]
    await session.close()


async def test_baseline_system_message_leads_user_input():
    session = create_session(MemoryStorage())
    conversation = await _conversation(session)
    cid = conversation["id"]
    await _append(session, cid, "pi.user", user_message("question"))
    await _append(
        session,
        cid,
        "pi.system",
        system_message({"preamble": "You are helpful."}, timestamp=1),
    )
    view = await read_context(session, cid)
    assert [message["role"] for message in view["messages"]] == ["system", "user"]

    # A later non-baseline system message stays where it was committed.
    await _append(
        session,
        cid,
        "app.instructions",
        system_message({"extra": "later"}, timestamp=2),
    )
    view = await read_context(session, cid)
    assert [message["role"] for message in view["messages"]] == ["system", "user", "system"]
    await session.close()

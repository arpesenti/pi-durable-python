import pytest

from pi_durable import (
    MemoryStorage,
    ReadAfterWrite,
    SessionClosed,
    create_session,
    define_doc,
)


async def test_commit_is_atomic_across_entries_and_documents():
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def failing(tx):
        await tx.append_entry(conversation["id"], {"kind": "app.note", "data": "first"})
        await tx.append_entry(conversation["id"], {"kind": "app.note", "data": "second"})
        raise RuntimeError("rollback")

    with pytest.raises(RuntimeError):
        await session.commit(failing)

    page = await session.commit(lambda tx: tx.scan_entries({"conversationId": conversation["id"]}, 10))
    assert page["items"] == []
    await session.close()


async def test_fork_sees_history_through_cutoff_only():
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    first = await session.commit(
        lambda tx: tx.append_entry(conversation["id"], {"kind": "app.note", "data": "hello"})
    )
    await session.commit(
        lambda tx: tx.append_entry(conversation["id"], {"kind": "app.note", "data": "goodbye"})
    )

    branch = await session.commit(
        lambda tx: tx.fork_conversation(
            conversation["id"], first["id"], {"ownership": {"kind": "ownerless"}}
        )
    )
    page = await session.commit(
        lambda tx: tx.scan_entries(
            {"conversationId": branch["id"], "order": "ascending"}, 10
        )
    )
    assert [entry["data"] for entry in page["items"]] == ["hello"]

    # The fork does not see the parent's later entry.
    assert await session.commit(lambda tx: tx.entry(first["id"])) is not None
    latest = await session.commit(
        lambda tx: tx.scan_entries({"conversationId": branch["id"]}, 10)
    )
    assert [entry["data"] for entry in latest["items"]] == ["hello"]
    await session.close()


async def test_table_read_after_write_is_rejected_but_document_access_is_not():
    Notes = define_doc(
        kind="app.notes",
        version=1,
        scope="conversation",
        history="latest",
        fork="initial",
        initial=lambda: {"text": ""},
        checkpoint_when=lambda value, ops, info: True,
    )
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def callback(tx):
        await tx.append_entry(conversation["id"], {"kind": "app.note"})
        with pytest.raises(ReadAfterWrite):
            await tx.conversation(conversation["id"])
        (await tx.doc(Notes, conversation["id"]))["text"] = "still allowed"
        return "done"

    assert await session.commit(callback) == "done"
    assert await session.snapshot(Notes, conversation["id"]) == {"text": "still allowed"}
    await session.close()


async def test_scans_paginate_and_respect_order_and_bounds():
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    for index in range(5):
        await session.commit(
            lambda tx, index=index: tx.append_entry(
                conversation["id"], {"kind": "app.note", "data": index}
            )
        )

    first = await session.commit(
        lambda tx: tx.scan_entries({"conversationId": conversation["id"]}, 2)
    )
    assert [entry["data"] for entry in first["items"]] == [4, 3]
    second = await session.commit(
        lambda tx: tx.scan_entries({"conversationId": conversation["id"]}, 2, first["next"])
    )
    assert [entry["data"] for entry in second["items"]] == [2, 1]

    ascending = await session.commit(
        lambda tx: tx.scan_entries(
            {"conversationId": conversation["id"], "order": "ascending"}, 10
        )
    )
    assert [entry["data"] for entry in ascending["items"]] == [0, 1, 2, 3, 4]

    bounded = await session.commit(
        lambda tx: tx.scan_entries(
            {
                "conversationId": conversation["id"],
                "minEntryId": ascending["items"][1]["id"],
                "maxEntryId": ascending["items"][3]["id"],
                "order": "ascending",
            },
            10,
        )
    )
    assert [entry["data"] for entry in bounded["items"]] == [1, 2, 3]
    await session.close()


async def test_close_rejects_new_work_and_settles_closed():
    session = create_session(MemoryStorage())
    await session.close()
    with pytest.raises(SessionClosed):
        await session.commit(lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}))
    assert await session.closed == {"reason": "closed"}


async def test_owner_task_must_exist_for_owned_conversation():
    session = create_session(MemoryStorage())
    with pytest.raises(ValueError):
        await session.commit(
            lambda tx: tx.create_conversation({"ownership": {"kind": "task", "taskId": 999}})
        )
    await session.close()

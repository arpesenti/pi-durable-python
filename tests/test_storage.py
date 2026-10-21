import pytest

from pi_durable import StorageRequestError, create_session, define_doc

Notes = define_doc(
    kind="app.notes",
    version=1,
    scope="conversation",
    history="rewindable",
    fork="asOf",
    initial=lambda: {"text": ""},
    checkpoint_when=lambda value, ops, info: False,
)


async def test_records_and_document_history_survive_reopen(storage_factory):
    storage = await storage_factory.open()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]

    async def first(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note", "data": "one"})
        (await tx.doc(Notes, cid))["text"] = "first"
        return entry

    first_entry = await session.commit(first)

    async def second(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note", "data": "two"})
        (await tx.doc(Notes, cid))["text"] = "second"
        submission = await tx.create_submission(
            {"type": "input", "conversationId": cid, "status": "queued", "content": "hi"}
        )
        tx.place_submission(submission["id"], entry["id"])
        return entry, submission["id"]

    second_entry, submission_id = await session.commit(second)
    await session.close()

    if storage_factory.kind == "memory":
        return

    storage2 = await storage_factory.open()
    session2 = create_session(storage2)
    reopened = await session2.commit(lambda tx: tx.conversation(cid))
    assert reopened["id"] == cid
    entries = await session2.commit(
        lambda tx: tx.scan_entries({"conversationId": cid, "order": "ascending"}, 10)
    )
    assert [entry["data"] for entry in entries["items"]] == ["one", "two"]
    assert (await session2.snapshot(Notes, cid))["text"] == "second"
    assert (await session2.snapshot_as_of(Notes, cid, first_entry["id"]))["text"] == "first"
    assert (await session2.snapshot_as_of(Notes, cid, second_entry["id"]))["text"] == "second"
    submission = await session2.commit(lambda tx: tx.submission_by_request(cid, "missing"))
    assert submission is None
    record = await storage2.submission(submission_id)
    assert record["status"] == "placed"
    await session2.close()


async def test_fork_copies_survive_reopen(storage_factory):
    storage = await storage_factory.open()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]

    async def write(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note"})
        (await tx.doc(Notes, cid))["text"] = "parent"
        return entry

    entry = await session.commit(write)
    branch = await session.commit(
        lambda tx: tx.fork_conversation(cid, entry["id"], {"ownership": {"kind": "ownerless"}})
    )
    await session.close()

    if storage_factory.kind == "memory":
        return

    session2 = create_session(await storage_factory.open())
    assert (await session2.snapshot(Notes, branch["id"]))["text"] == "parent"
    entries = await session2.commit(
        lambda tx: tx.scan_entries({"conversationId": branch["id"]}, 10)
    )
    assert len(entries["items"]) == 1
    await session2.close()


async def test_mint_ids_strictly_increase_across_reopen(storage_factory):
    storage = await storage_factory.open()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    first = await session.commit(lambda tx: tx.append_entry(conversation["id"], {"kind": "a"}))
    await session.close()

    if storage_factory.kind == "memory":
        return
    session2 = create_session(await storage_factory.open())
    second = await session2.commit(lambda tx: tx.append_entry(conversation["id"], {"kind": "b"}))
    assert second["id"] > first["id"]
    await session2.close()


async def test_document_can_be_read_at_an_ancestor_commit_through_a_fork(storage_factory):
    storage = await storage_factory.open()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]

    async def write(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note"})
        (await tx.doc(Notes, cid))["text"] = "at fork"
        return entry

    entry = await session.commit(write)
    await session.commit(
        lambda tx: tx.fork_conversation(cid, entry["id"], {"ownership": {"kind": "ownerless"}})
    )
    # Reading an inherited entry through the parent still resolves the parent's own
    # historical incarnation.
    assert (await session.snapshot_as_of(Notes, cid, entry["id"]))["text"] == "at fork"
    await session.close()


async def test_historical_reads_of_latest_only_documents_are_rejected(storage_factory):
    Latest = define_doc(
        kind="app.latest",
        version=1,
        scope="conversation",
        history="latest",
        fork="initial",
        initial=lambda: {"n": 0},
        checkpoint_when=lambda value, ops, info: True,
    )
    storage = await storage_factory.open()
    session = create_session(storage)
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]

    async def write(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note"})
        (await tx.doc(Latest, cid))["n"] = 1
        return entry

    entry = await session.commit(write)
    await session.commit(lambda tx: tx.append_entry(cid, {"kind": "app.note"}))
    with pytest.raises(StorageRequestError):
        await session.snapshot_as_of(Latest, cid, entry["id"])
    await session.close()

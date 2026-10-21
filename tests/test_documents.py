import asyncio

import pytest

from pi_durable import MemoryStorage, create_session, define_doc, define_doc_family


def notes_definition():
    return define_doc(
        kind="app.notes",
        version=1,
        scope="conversation",
        history="rewindable",
        fork="asOf",
        initial=lambda: {"text": "", "items": []},
        checkpoint_when=lambda value, ops, info: False,
    )


async def test_draft_assignment_and_nested_mutation_are_tracked():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def write(tx):
        draft = await tx.doc(Notes, conversation["id"])
        draft["text"] = "hello"
        draft["items"].append("a")
        draft["items"].insert(0, "b")
        draft["items"][1] = "c"
        draft["items"].pop()
        return draft.ops

    ops = await session.commit(write)
    assert await session.snapshot(Notes, conversation["id"]) == {"text": "hello", "items": ["b"]}
    assert [op[0] for op in ops] == ["s", "p", "p", "s", "p"]
    await session.close()


async def test_snapshot_as_of_reads_the_commit_that_wrote_the_entry():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def first(tx):
        entry = await tx.append_entry(conversation["id"], {"kind": "app.note"})
        (await tx.doc(Notes, conversation["id"]))["text"] = "one"
        return entry

    first_entry = await session.commit(first)
    second_entry = await session.commit(
        lambda tx: tx.append_entry(conversation["id"], {"kind": "app.note"})
    )

    async def third(tx):
        entry = await tx.append_entry(conversation["id"], {"kind": "app.note"})
        (await tx.doc(Notes, conversation["id"]))["text"] = "three"
        return entry

    third_entry = await session.commit(third)

    assert (await session.snapshot_as_of(Notes, conversation["id"], first_entry["id"]))["text"] == "one"
    assert (await session.snapshot_as_of(Notes, conversation["id"], second_entry["id"]))["text"] == "one"
    assert (await session.snapshot_as_of(Notes, conversation["id"], third_entry["id"]))["text"] == "three"
    assert (await session.snapshot(Notes, conversation["id"]))["text"] == "three"
    await session.close()


async def test_fork_policies_as_of_current_and_initial():
    AsOf = define_doc(
        kind="app.asof",
        version=1,
        scope="conversation",
        history="rewindable",
        fork="asOf",
        initial=lambda: {"n": 0},
        checkpoint_when=lambda value, ops, info: True,
    )
    Current = define_doc(
        kind="app.current",
        version=1,
        scope="conversation",
        history="latest",
        fork="current",
        initial=lambda: {"n": 0},
        checkpoint_when=lambda value, ops, info: True,
    )
    Fresh = define_doc(
        kind="app.fresh",
        version=1,
        scope="conversation",
        history="latest",
        fork="initial",
        initial=lambda: {"n": 0},
        checkpoint_when=lambda value, ops, info: True,
    )
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def write(tx, value):
        entry = await tx.append_entry(conversation["id"], {"kind": "app.note"})
        for token in (AsOf, Current, Fresh):
            (await tx.doc(token, conversation["id"]))["n"] = value
        return entry

    first = await session.commit(lambda tx: write(tx, 1))
    await session.commit(lambda tx: write(tx, 2))
    branch = await session.commit(
        lambda tx: tx.fork_conversation(
            conversation["id"], first["id"], {"ownership": {"kind": "ownerless"}}
        )
    )
    assert (await session.snapshot(AsOf, branch["id"]))["n"] == 1
    assert (await session.snapshot(Current, branch["id"]))["n"] == 2
    # "initial" documents are not copied; reads never create them.
    assert await session.snapshot(Fresh, branch["id"]) is None

    async def create_fresh(tx):
        draft = await tx.doc(Fresh, branch["id"])
        assert draft["n"] == 0

    await session.commit(create_fresh)
    assert (await session.snapshot(Fresh, branch["id"]))["n"] == 0
    await session.close()


async def test_fork_copy_is_independent():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def write(tx):
        entry = await tx.append_entry(conversation["id"], {"kind": "app.note"})
        (await tx.doc(Notes, conversation["id"]))["text"] = "parent"
        return entry

    entry = await session.commit(write)
    branch = await session.commit(
        lambda tx: tx.fork_conversation(
            conversation["id"], entry["id"], {"ownership": {"kind": "ownerless"}}
        )
    )

    async def edit(tx):
        (await tx.doc(Notes, branch["id"]))["text"] = "branch"

    await session.commit(edit)
    assert (await session.snapshot(Notes, branch["id"]))["text"] == "branch"
    assert (await session.snapshot(Notes, conversation["id"]))["text"] == "parent"
    await session.close()


async def test_retirement_and_recreation_use_a_new_incarnation():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def first(tx):
        (await tx.doc(Notes, conversation["id"]))["text"] = "one"

    await session.commit(first)

    async def retire(tx):
        await tx.retire_doc(Notes, conversation["id"])

    await session.commit(retire)
    assert await session.snapshot(Notes, conversation["id"]) is None

    async def recreate(tx):
        (await tx.doc(Notes, conversation["id"]))["text"] = "two"

    await session.commit(recreate)
    assert (await session.snapshot(Notes, conversation["id"]))["text"] == "two"
    await session.close()


async def test_document_families_create_members_with_seeds_and_ignore_later_seeds():
    Scratch = define_doc_family(
        kind="app.scratch",
        version=1,
        scope="conversation",
        history="latest",
        fork="initial",
        initial=lambda seed: {"value": seed},
        checkpoint_when=lambda value, ops, info: True,
    )
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )

    async def write(tx):
        first = await tx.doc(Scratch, conversation["id"], "a", 1)
        again = await tx.doc(Scratch, conversation["id"], "a", 999)
        assert first is again
        first["value"] = 2

    await session.commit(write)
    assert await session.snapshot(Scratch, conversation["id"], "a") == {"value": 2}
    assert await session.snapshot(Scratch, conversation["id"], "b") is None
    await session.close()


async def test_migration_runs_in_memory_for_reads_and_as_a_base_on_write():
    Old = define_doc(
        kind="app.versioned",
        version=1,
        scope="conversation",
        history="latest",
        fork="initial",
        initial=lambda: {"value": 1},
        checkpoint_when=lambda value, ops, info: False,
    )
    New = define_doc(
        kind="app.versioned",
        version=2,
        scope="conversation",
        history="latest",
        fork="initial",
        initial=lambda: {"value": 1, "label": ""},
        migrate=lambda value, from_version: {**value, "label": "migrated"},
        checkpoint_when=lambda value, ops, info: False,
    )
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    await session.commit(lambda tx: tx.doc(Old, conversation["id"]))
    assert await session.snapshot(New, conversation["id"]) == {"value": 1, "label": "migrated"}
    await session.close()


async def test_watch_delivers_committed_frames_serially_and_ends_on_retirement():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    await session.commit(lambda tx: tx.doc(Notes, conversation["id"]))
    watch = await session.watch_doc(Notes, conversation["id"])
    assert watch.value == {"text": "", "items": []}
    received = []
    delivered = asyncio.Event()

    async def listener(value, ops):
        received.append(value["text"])
        if value["text"] == "second":
            delivered.set()

    watch.start(listener)
    for text in ("first", "second"):
        async def write(tx, text=text):
            (await tx.doc(Notes, conversation["id"]))["text"] = text

        await session.commit(write)
    await asyncio.wait_for(delivered.wait(), 2)
    assert received == ["first", "second"]
    await watch.stop()
    assert (await watch.closed).reason == "stopped"

    watch2 = await session.watch_doc(Notes, conversation["id"])
    ended = asyncio.Event()

    def on_end(value, ops):
        if value is None:
            ended.set()

    watch2.start(on_end)

    async def retire(tx):
        await tx.retire_doc(Notes, conversation["id"])

    await session.commit(retire)
    await asyncio.wait_for(ended.wait(), 2)
    await asyncio.wait_for(watch2.closed, 2)
    end = await watch2.closed
    assert end.reason == "retired"
    await session.close()


async def test_document_state_tracks_committed_revisions_and_closes():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    await session.commit(lambda tx: tx.doc(Notes, conversation["id"]))
    state = await session.document_state(Notes, conversation["id"])
    assert state.value == {"text": "", "items": []}
    values = []
    state.subscribe(lambda value, ops: values.append(value["text"]))
    async def write(tx):
        (await tx.doc(Notes, conversation["id"]))["text"] = "changed"
    await session.commit(write)
    await asyncio.sleep(0.01)
    assert values == ["changed"]
    assert state.value["text"] == "changed"
    state.close()
    assert state.closed
    await session.close()


async def test_retired_document_state_observes_retirement():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    await session.commit(lambda tx: tx.doc(Notes, conversation["id"]))
    state = await session.document_state(Notes, conversation["id"])
    values = []
    state.subscribe(lambda value, ops: values.append(value))
    async def retire(tx):
        await tx.retire_doc(Notes, conversation["id"])
    await session.commit(retire)
    await asyncio.sleep(0.01)
    assert values == [None]
    await session.close()


async def test_snapshot_as_of_requires_a_visible_entry():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    with pytest.raises(ValueError):
        await session.snapshot_as_of(Notes, conversation["id"], 999)
    await session.close()


async def test_family_snapshot_as_of_and_fork_copy():
    Scratch = define_doc_family(
        kind="app.scratch-history",
        version=1,
        scope="conversation",
        history="rewindable",
        fork="asOf",
        initial=lambda seed: {"value": seed},
        checkpoint_when=lambda value, ops, info: False,
    )
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]

    async def first(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note"})
        (await tx.doc(Scratch, cid, "a", 0))["value"] = 1
        return entry

    first_entry = await session.commit(first)

    async def second(tx):
        entry = await tx.append_entry(cid, {"kind": "app.note"})
        (await tx.doc(Scratch, cid, "a", 0))["value"] = 2
        return entry

    second_entry = await session.commit(second)
    assert await session.snapshot_as_of(Scratch, cid, "a", first_entry["id"]) == {"value": 1}
    assert await session.snapshot_as_of(Scratch, cid, "a", second_entry["id"]) == {"value": 2}

    branch = await session.commit(
        lambda tx: tx.fork_conversation(cid, first_entry["id"], {"ownership": {"kind": "ownerless"}})
    )
    assert await session.snapshot(Scratch, branch["id"], "a") == {"value": 1}

    async def edit(tx):
        (await tx.doc(Scratch, branch["id"], "a", 0))["value"] = 3

    await session.commit(edit)
    assert await session.snapshot(Scratch, branch["id"], "a") == {"value": 3}
    assert await session.snapshot(Scratch, cid, "a") == {"value": 2}
    await session.close()


async def test_retirement_then_recreation_in_one_transaction():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]

    async def write(tx, text):
        (await tx.doc(Notes, cid))["text"] = text

    await session.commit(lambda tx: write(tx, "old"))

    async def recreate(tx):
        await tx.retire_doc(Notes, cid)
        draft = await tx.doc(Notes, cid)
        draft["text"] = "new"

    await session.commit(recreate)
    assert await session.snapshot(Notes, cid) == {"text": "new", "items": []}
    await session.close()


async def test_watch_buffer_resets_to_a_full_value_after_one_hundred_pending_frames():
    Notes = notes_definition()
    session = create_session(MemoryStorage())
    conversation = await session.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
    )
    cid = conversation["id"]
    await session.commit(lambda tx: tx.doc(Notes, cid))
    watch = await session.watch_doc(Notes, cid)
    gate = asyncio.Event()
    calls = []

    async def listener(value, ops):
        calls.append((value["text"], ops))
        await gate.wait()

    watch.start(listener)
    for index in range(102):
        async def write(tx, index=index):
            (await tx.doc(Notes, cid))["text"] = f"v{index}"

        await session.commit(write)
    gate.set()
    for _ in range(100):
        if len(calls) >= 2:
            break
        await asyncio.sleep(0.01)
    assert calls[0][0] == "v0"
    assert calls[1][0] == "v101"
    assert calls[1][1] == [["r", {"text": "v101", "items": []}]]
    await watch.stop()
    await session.close()

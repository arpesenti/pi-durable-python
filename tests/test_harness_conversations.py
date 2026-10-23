"""Translated upstream ``harness-conversations`` cases for creation, forks, and handles."""

from __future__ import annotations

from typing import Any, cast

import pytest
from session_support import ControlledStorage, context

from pi_durable import (
    AgentDoc,
    Harness,
    InboxDoc,
    LiveDoc,
    ProviderDoc,
    ReadAfterWrite,
    UsageDoc,
)
from pi_durable.harness.registry import create_registry
from pi_durable.tasks import define_task

MODEL = {"provider": "faux", "modelId": "faux-1"}


async def open_conversations(
    storage: ControlledStorage | None = None,
    *,
    created: Any = None,
) -> dict[str, Any]:
    reports: list[BaseException] = []
    registry = create_registry()
    opened = storage or ControlledStorage()
    harness = await Harness.open(
        opened,
        {
            "models": {},
            "registry": registry,
            "on_report": reports.append,
            **({} if created is None else {"conversation_created": created}),
        },
        context,
    )
    return {"harness": harness, "registry": registry, "reports": reports, "storage": opened}


async def entries_of(conversation: Any) -> list[dict[str, Any]]:
    page = await conversation.entries({}, 100, None, context)
    return list(reversed(page["items"]))


async def agent_state(harness: Harness, conversation_id: int) -> dict[str, Any] | None:
    return await harness.snapshot(AgentDoc, conversation_id, context)


async def conversation_ids(harness: Harness) -> list[int]:
    async def scan(tx: Any) -> Any:
        return [record["id"] for record in (await tx.scan_conversations({}, 100))["items"]]

    values = await harness.commit(scan, context)
    return sorted(values)


def hold_task() -> Any:
    async def run(_task: Any, _runtime: Any, _context: Any) -> None:
        return None

    return define_task(
        {
            "name": "test.hold",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": lambda _task, _runtime, _context: None,
        }
    )


async def test_creates_the_root_once_and_ignores_later_options() -> None:
    opened = await open_conversations()
    harness = opened["harness"]
    storage = opened["storage"]
    inits: list[int] = []

    async def init(tx: Any, conversation_id: int) -> None:
        inits.append(conversation_id)
        await tx.append_entry(conversation_id, {"kind": "test.init"})

    baseline = len(storage.commits)
    root = await harness.root(context, {"agent": {"cwd": "/first"}, "init": init})
    assert len(storage.commits) == baseline + 1
    assert inits == [root.id]
    assert (await root.agent(context)).cwd == "/first"
    commits = len(storage.commits)
    again = await harness.root(context, {"agent": {"cwd": "/second"}, "init": init})
    assert again.id == root.id
    assert inits == [root.id]
    assert len(storage.commits) == commits
    assert (await again.agent(context)).cwd == "/first"
    assert [entry["kind"] for entry in await entries_of(again)] == ["test.init"]
    await harness.close(context)


async def test_commits_creation_hook_agent_and_init_in_one_conversation_commit() -> None:
    seen: list[dict[str, Any]] = []

    async def created(tx: Any, record: Any) -> None:
        seen.append(record)
        await tx.append_entry(record["id"], {"kind": "test.hook"})

    opened = await open_conversations(created=created)
    harness = opened["harness"]
    storage = opened["storage"]
    await harness.root(context)
    baseline = len(storage.commits)

    async def init(tx: Any, conversation_id: int) -> None:
        await tx.append_entry(conversation_id, {"kind": "test.init"})

    child = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}, "agent": {"cwd": "/child"}, "init": init},
        context,
    )
    assert len(storage.commits) == baseline + 1
    assert seen[-1]["id"] == child.id
    assert "parent" not in seen[-1] and "owner" not in seen[-1]
    assert [entry["kind"] for entry in await entries_of(child)] == ["test.hook", "test.init"]
    assert await agent_state(harness, child.id) == {"cwd": "/child"}
    for token in (LiveDoc, InboxDoc, UsageDoc, ProviderDoc):
        assert await harness.snapshot(token, child.id, context) is not None
    await harness.close(context)


async def test_runs_the_creation_hook_for_raw_transaction_paths() -> None:
    seen: list[dict[str, Any]] = []

    async def created(_tx: Any, record: Any) -> None:
        seen.append(record)

    opened = await open_conversations(created=created)
    harness = opened["harness"]
    root = await harness.root(context)
    assert [record["id"] for record in seen] == [root.id]

    raw = await root.commit(
        lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}}), context
    )
    assert seen[-1]["id"] == raw["id"]

    entry = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    forked = await root.commit(
        lambda tx: tx.fork_conversation(
            root.id, entry["id"], {"ownership": {"kind": "ownerless"}}
        ),
        context,
    )
    assert seen[-1]["id"] == forked["id"]
    assert seen[-1]["parent"] == {"conversationId": root.id, "at": entry["id"]}
    await harness.close(context)


async def test_fails_the_creating_commit_when_the_hook_or_init_throws() -> None:
    failing = {"hook": False}

    async def created(_tx: Any, _record: Any) -> None:
        if failing["hook"]:
            raise RuntimeError("hook failed")

    opened = await open_conversations(created=created)
    harness = opened["harness"]
    await harness.root(context)
    before = await conversation_ids(harness)
    failing["hook"] = True
    with pytest.raises(RuntimeError, match="hook failed"):
        await harness.create_conversation({"ownership": {"kind": "ownerless"}}, context)
    assert await conversation_ids(harness) == before

    failing["hook"] = False

    async def init(_tx: Any, _conversation_id: int) -> None:
        raise RuntimeError("init failed")

    with pytest.raises(RuntimeError, match="init failed"):
        await harness.create_conversation(
            {"ownership": {"kind": "ownerless"}, "init": init}, context
        )
    assert await conversation_ids(harness) == before
    await harness.close(context)


async def test_rejects_table_reads_inside_the_creation_hook_and_init() -> None:
    errors: list[BaseException] = []

    async def created(tx: Any, record: Any) -> None:
        try:
            await tx.conversation(record["id"])
        except ReadAfterWrite as error:
            errors.append(error)

    opened = await open_conversations(created=created)
    harness = opened["harness"]
    root = await harness.root(context)
    assert len(errors) == 1

    async def init(tx: Any, conversation_id: int) -> None:
        try:
            await tx.conversation(conversation_id)
        except ReadAfterWrite as error:
            errors.append(error)

    child = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}, "init": init}, context
    )
    # One hook read for the root, one for the child, and the child's init read.
    assert len(errors) == 3
    assert await conversation_ids(harness) == sorted([root.id, child.id])
    await harness.close(context)


async def test_requires_explicit_ownership_for_creation_and_forks() -> None:
    opened = await open_conversations()
    harness = opened["harness"]
    root = await harness.root(context)
    mark = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    before = await conversation_ids(harness)
    with pytest.raises(TypeError, match="explicit ownership"):
        await harness.create_conversation({}, context)
    with pytest.raises(TypeError, match="explicit ownership"):
        await root.fork(mark["id"], {}, context)
    assert await conversation_ids(harness) == before
    await harness.close(context)


async def test_copies_the_owner_agent_into_a_task_owned_conversation() -> None:
    opened = await open_conversations()
    harness = opened["harness"]
    root = await harness.root(context)
    await root.configure(
        {"model": MODEL, "instructions": "be brief", "cwd": "/work"}, context
    )
    Held = hold_task()

    async def create(tx: Any) -> Any:
        task_id = await tx.create_task(
            Held, None, {"ownership": {"kind": "conversation"}}
        )
        return await tx.create_conversation(
            {"ownership": {"kind": "task", "taskId": task_id}}
        )

    owned = await root.commit(create, context)
    assert await agent_state(harness, owned["id"]) == {
        "model": MODEL,
        "instructions": "be brief",
        "cwd": "/work",
    }
    handle = await harness.conversation(owned["id"], context)
    assert handle is not None
    mark = await handle.commit(lambda tx: tx.append_entry(handle.id, {"kind": "test.mark"}), context)
    await root.configure({"instructions": "changed"}, context)
    copied = await agent_state(harness, owned["id"])
    assert copied is not None and copied["instructions"] == "be brief"

    forked = await handle.fork(mark["id"], {"ownership": {"kind": "ownerless"}}, context)
    assert await agent_state(harness, forked.id) == {
        "model": MODEL,
        "instructions": "be brief",
        "cwd": "/work",
    }

    ownerless = await harness.create_conversation(
        {"ownership": {"kind": "ownerless"}}, context
    )
    assert await agent_state(harness, ownerless.id) == {}
    await harness.close(context)


async def test_forks_at_an_entry_with_the_agent_change_and_init_in_one_commit() -> None:
    opened = await open_conversations()
    harness = opened["harness"]
    storage = opened["storage"]
    root = await harness.root(context, {"agent": {"cwd": "/parent"}})
    mark = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    await root.configure({"cwd": "/later"}, context)
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    before = len(storage.commits)

    async def init(tx: Any, conversation_id: int) -> None:
        await tx.append_entry(conversation_id, {"kind": "test.init"})

    child = await root.fork(
        mark["id"],
        {"ownership": {"kind": "ownerless"}, "agent": {"instructions": "go"}, "init": init},
        context,
    )
    assert len(storage.commits) == before + 1
    assert [entry["kind"] for entry in await entries_of(child)] == ["test.mark", "test.init"]
    resolved = await child.agent(context)
    assert resolved.cwd == "/parent"
    assert resolved.instructions == "go"
    record = await harness.commit(lambda tx: tx.conversation(child.id), context)
    assert record is not None
    assert cast(Any, record)["parent"] == {"conversationId": root.id, "at": mark["id"]}
    await harness.close(context)


async def test_leaves_no_child_when_a_fork_init_throws() -> None:
    opened = await open_conversations()
    harness = opened["harness"]
    root = await harness.root(context)
    mark = await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    before = await conversation_ids(harness)

    async def init(_tx: Any, _conversation_id: int) -> None:
        raise RuntimeError("fork init failed")

    with pytest.raises(RuntimeError, match="fork init failed"):
        await root.fork(
            mark["id"], {"ownership": {"kind": "ownerless"}, "init": init}, context
        )
    assert await conversation_ids(harness) == before
    await harness.close(context)


async def test_handles_are_stateless_and_read_the_committed_conversation() -> None:
    opened = await open_conversations()
    harness = opened["harness"]
    root = await harness.root(context)
    first = await harness.conversation(root.id, context)
    second = await harness.conversation(root.id, context)
    assert first is not None and second is not None
    assert first is not second
    assert first.id == second.id == root.id
    assert await harness.conversation(99_999, context) is None

    await first.configure({"cwd": "/one"}, context)
    assert (await second.agent(context)).cwd == "/one"
    entry = await second.commit(
        lambda tx: tx.append_entry(second.id, {"kind": "test.mark"}), context
    )
    page = await first.entries({}, 10, None, context)
    assert [item["id"] for item in page["items"]] == [entry["id"]]
    assert (await first.context(context))["entries"][0]["id"] == entry["id"]

    await harness.close(context)
    with pytest.raises(RuntimeError, match="Harness is closed"):
        await first.context(context)


async def test_reopens_the_same_root_and_keeps_its_committed_transcript() -> None:
    storage = ControlledStorage()
    first = await open_conversations(storage)
    root = await first["harness"].root(context, {"agent": {"cwd": "/kept"}})
    await root.commit(lambda tx: tx.append_entry(root.id, {"kind": "test.mark"}), context)
    await first["harness"].close(context)

    reopened = await open_conversations(storage.reopen())
    same = await reopened["harness"].root(context, {"agent": {"cwd": "/ignored"}})
    assert same.id == root.id
    assert (await same.agent(context)).cwd == "/kept"
    assert [entry["kind"] for entry in await entries_of(same)] == ["test.mark"]
    await reopened["harness"].close(context)

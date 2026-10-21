"""Translated v1.3.0 ``session-definitions`` suite (runtime slice)."""

from __future__ import annotations

from typing import Any

import pytest
from session_support import context, create_conversation, open_test_session

from pi_durable import (
    CommonDocDefinition,
    ConversationDocFamilyToken,
    ConversationDocToken,
    DocDefinition,
    DocFamilyDefinition,
    DocFamilyToken,
    DocToken,
    RewindableConversationDocFamilyToken,
    RewindableConversationDocToken,
    SessionDocFamilyToken,
    SessionDocToken,
    TaskDocFamilyToken,
    TaskDocToken,
    define_doc,
    define_doc_family,
    define_task,
)
from pi_durable.ids import TaskId


def initial() -> dict[str, int]:
    return {"value": 0}


SessionDoc = define_doc(
    {"kind": "t.session", "version": 1, "scope": "session", "initial": initial}
)
LatestDoc = define_doc(
    {
        "kind": "t.latest",
        "version": 1,
        "scope": "conversation",
        "history": "latest",
        "fork": "current",
        "initial": initial,
    }
)
RewindableDoc = define_doc(
    {
        "kind": "t.rewindable",
        "version": 1,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "asOf",
        "initial": initial,
    }
)
TaskDoc = define_doc({"kind": "t.task", "version": 1, "scope": "task", "initial": initial})
SessionFamily = define_doc_family(
    {
        "kind": "t.session-family",
        "version": 1,
        "family": True,
        "scope": "session",
        "initial": lambda seed: {"value": seed},
    }
)
ConversationFamily = define_doc_family(
    {
        "kind": "t.conversation-family",
        "version": 1,
        "family": True,
        "scope": "conversation",
        "history": "rewindable",
        "fork": "initial",
        "initial": lambda seed: {"value": seed},
    }
)
TaskFamily = define_doc_family(
    {
        "kind": "t.task-family",
        "version": 1,
        "family": True,
        "scope": "task",
        "initial": lambda seed: {"value": seed},
    }
)

WorkTask = define_task(
    {
        "name": "test.definition-work",
        "version": 1,
        "initial": lambda _input: {"phase": "start"},
        "phases": {"start": lambda *_args: None},
        "abort": lambda *_args: None,
    }
)


def test_validates_persisted_version_semantics() -> None:
    with pytest.raises(TypeError, match="positive integer"):
        define_doc({"kind": "k", "version": 0, "scope": "session", "initial": initial})
    with pytest.raises(TypeError, match="positive integer"):
        define_doc({"kind": "k", "version": 1.5, "scope": "session", "initial": initial})
    assert define_doc(
        {"kind": "", "version": 1, "scope": "session", "initial": initial}
    ).definition.kind == ""
    assert SessionDoc.definition.kind == "t.session"


def test_scope_specific_token_names_are_the_erased_tokens() -> None:
    assert CommonDocDefinition is DocDefinition
    assert DocFamilyDefinition is DocDefinition
    assert SessionDocToken is DocToken
    assert ConversationDocToken is DocToken
    assert RewindableConversationDocToken is DocToken
    assert TaskDocToken is DocToken
    assert SessionDocFamilyToken is DocFamilyToken
    assert ConversationDocFamilyToken is DocFamilyToken
    assert RewindableConversationDocFamilyToken is DocFamilyToken
    assert TaskDocFamilyToken is DocFamilyToken


def test_exposes_family_and_scope_semantics() -> None:
    assert SessionDoc.definition.family is False
    assert SessionDoc.definition.scope == "session"
    assert LatestDoc.definition.history == "latest"
    assert RewindableDoc.definition.fork == "asOf"
    assert TaskDoc.definition.scope == "task"
    assert SessionFamily.definition.family is True
    assert ConversationFamily.definition.history == "rewindable"
    assert TaskFamily.definition.scope == "task"


async def test_uses_every_owner_key_and_seed_overload() -> None:
    _storage, session, _publications = open_test_session()
    conversation_id = await create_conversation(session)

    async def create(tx: Any) -> TaskId:
        task_id = await tx.create_task(
            WorkTask,
            {"path": "definitions"},
            {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
        )
        (await tx.doc(SessionDoc)).value = 1
        (await tx.doc(LatestDoc, conversation_id)).value = 2
        (await tx.doc(RewindableDoc, conversation_id)).value = 3
        (await tx.doc(TaskDoc, task_id)).value = 4
        (await tx.doc(SessionFamily, "k", 5)).value = 5
        (await tx.doc(ConversationFamily, conversation_id, "k", 6)).value = 6
        (await tx.doc(TaskFamily, task_id, "k", 7)).value = 7
        return task_id

    task_id = await session.commit(create, context)
    assert await session.snapshot(SessionDoc, context) == {"value": 1}
    assert await session.snapshot(LatestDoc, conversation_id, context) == {"value": 2}
    assert await session.snapshot(RewindableDoc, conversation_id, context) == {"value": 3}
    assert await session.snapshot(TaskDoc, task_id, context) == {"value": 4}
    assert await session.snapshot(SessionFamily, "k", context) == {"value": 5}
    assert await session.snapshot(ConversationFamily, conversation_id, "k", context) == {"value": 6}
    assert await session.snapshot(TaskFamily, task_id, "k", context) == {"value": 7}

    async def retire(tx: Any) -> None:
        await tx.retire_doc(SessionDoc)
        await tx.retire_doc(LatestDoc, conversation_id)
        await tx.retire_doc(RewindableDoc, conversation_id)
        await tx.retire_doc(TaskDoc, task_id)
        await tx.retire_doc(SessionFamily, "k")
        await tx.retire_doc(ConversationFamily, conversation_id, "k")
        await tx.retire_doc(TaskFamily, task_id, "k")

    await session.commit(retire, context)
    assert await session.snapshot(SessionDoc, context) is None
    assert await session.snapshot(LatestDoc, conversation_id, context) is None
    assert await session.snapshot(RewindableDoc, conversation_id, context) is None
    assert await session.snapshot(TaskDoc, task_id, context) is None
    assert await session.snapshot(SessionFamily, "k", context) is None
    assert await session.snapshot(ConversationFamily, conversation_id, "k", context) is None
    assert await session.snapshot(TaskFamily, task_id, "k", context) is None

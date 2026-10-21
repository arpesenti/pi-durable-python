import json

import pytest

from pi_durable import (
    AssistantEntry,
    CompactionEntry,
    ResetEntry,
    SystemEntry,
    ToolResultEntry,
    UserEntry,
    define_entry,
)
from pi_durable.entries import Entry
from pi_durable.ids import ConversationId, EntryId, TaskId
from pi_durable.json import FrozenDict, FrozenList
from pi_durable.types import entry_draft, entry_record

USER_MESSAGE = {"role": "user", "content": "hello", "timestamp": 1}


def test_define_entry_rejects_invalid_kinds() -> None:
    for kind in [""]:
        with pytest.raises(TypeError, match="non-empty string"):
            define_entry(kind)
    with pytest.raises(TypeError, match="non-empty string"):
        define_entry(1)  # type: ignore[arg-type]


def test_tokens_narrow_by_kind() -> None:
    record = entry_record(EntryId(2), ConversationId(1), {"kind": "pi.user", "model": [USER_MESSAGE]})
    assert UserEntry.is_(record) is True
    assert AssistantEntry.is_(record) is False
    assert AssistantEntry.is_(None) is False
    assert isinstance(UserEntry, Entry)
    assert UserEntry.kind == "pi.user"


def test_builtin_tokens_match_the_reference_kinds() -> None:
    assert {
        UserEntry.kind,
        AssistantEntry.kind,
        SystemEntry.kind,
        ToolResultEntry.kind,
        ResetEntry.kind,
        CompactionEntry.kind,
    } == {
        "pi.user",
        "pi.assistant",
        "pi.system",
        "pi.tool-result",
        "pi.reset",
        "pi.compaction",
    }


def test_entry_draft_accepts_every_reference_field() -> None:
    draft = entry_draft(
        {
            "kind": "app.note",
            "model": [USER_MESSAGE],
            "data": {"nested": [1, True, None]},
            "head": "self",
            "edits": [
                {"target": 3, "action": "omit"},
                {"target": 4, "action": "replace", "messages": [USER_MESSAGE]},
            ],
        }
    )
    assert draft["kind"] == "app.note"
    assert draft["model"] == [USER_MESSAGE]
    assert draft["data"] == {"nested": [1, True, None]}
    assert draft["head"] == "self"
    assert draft["edits"] == [
        {"target": 3, "action": "omit"},
        {"target": 4, "action": "replace", "messages": [USER_MESSAGE]},
    ]
    assert isinstance(draft, FrozenDict)
    assert isinstance(draft["model"], FrozenList)
    assert isinstance(draft["data"]["nested"], FrozenList)
    with pytest.raises(TypeError, match="immutable"):
        draft["kind"] = "changed"


def test_entry_draft_keeps_an_explicit_null_data_value() -> None:
    draft = entry_draft({"kind": "app.note", "data": None})
    assert "data" in draft
    assert draft["data"] is None


def test_entry_draft_copies_by_value() -> None:
    message = dict(USER_MESSAGE)
    data = {"items": [1]}
    draft = entry_draft({"kind": "app.note", "model": [message], "data": data})
    message["content"] = "changed"
    data["items"].append(2)
    assert draft["model"] == [USER_MESSAGE]
    assert draft["data"] == {"items": [1]}


@pytest.mark.parametrize(
    "draft",
    [
        {},
        {"kind": ""},
        {"kind": 1},
        {"kind": "app.note", "unknown": 1},
        {"kind": "app.note", "id": 1},
        {"kind": "app.note", "model": USER_MESSAGE},
        {"kind": "app.note", "model": [{"content": "no role"}]},
        {"kind": "app.note", "data": object()},
        {"kind": "app.note", "head": 0},
        {"kind": "app.note", "head": True},
        {"kind": "app.note", "head": "elsewhere"},
        {"kind": "app.note", "edits": {"target": 1, "action": "omit"}},
        {"kind": "app.note", "edits": [{"target": 1}]},
        {"kind": "app.note", "edits": [{"action": "omit"}]},
        {"kind": "app.note", "edits": [{"target": 1, "action": "omit", "messages": []}]},
        {"kind": "app.note", "edits": [{"target": 1, "action": "replace"}]},
        {"kind": "app.note", "edits": [{"target": 1, "action": "remove"}]},
        {"kind": "app.note", "edits": [{"target": 1, "action": "omit", "extra": 1}]},
    ],
)
def test_entry_draft_rejects_invalid_drafts(draft: dict[str, object]) -> None:
    with pytest.raises(TypeError):
        entry_draft(draft)


def test_entry_record_assigns_identity_and_resolves_self_head() -> None:
    record = entry_record(
        EntryId(7),
        ConversationId(1),
        {"kind": "pi.reset", "head": "self", "model": [USER_MESSAGE]},
        by_task_id=TaskId(9),
    )
    assert record["id"] == 7
    assert record["conversationId"] == 1
    assert record["kind"] == "pi.reset"
    assert record["head"] == 7
    assert record["byTaskId"] == 9
    assert record["model"] == [USER_MESSAGE]
    assert isinstance(record, FrozenDict)
    assert json.loads(json.dumps(record)) == {
        "id": 7,
        "conversationId": 1,
        "kind": "pi.reset",
        "model": [USER_MESSAGE],
        "head": 7,
        "byTaskId": 9,
    }


def test_entry_record_preserves_an_explicit_head() -> None:
    record = entry_record(EntryId(7), ConversationId(1), {"kind": "pi.compaction", "head": EntryId(3)})
    assert record["head"] == 3


def test_entry_record_omits_absent_optionals() -> None:
    record = entry_record(EntryId(7), ConversationId(1), {"kind": "app.note"})
    assert dict(record) == {"id": 7, "conversationId": 1, "kind": "app.note"}


def test_entry_record_is_immutable_at_every_depth() -> None:
    record = entry_record(
        EntryId(7),
        ConversationId(1),
        {"kind": "app.note", "model": [USER_MESSAGE], "data": {"items": [1]}},
    )
    with pytest.raises(TypeError, match="immutable"):
        record["kind"] = "changed"
    with pytest.raises(TypeError, match="immutable"):
        record["model"][0]["content"] = "changed"
    assert isinstance(record["data"], dict)
    assert isinstance(record["data"]["items"], list)
    with pytest.raises(TypeError, match="immutable"):
        record["data"]["items"].append(2)


def test_entry_record_rejects_invalid_identity() -> None:
    for invalid in [0, -1, True, 1.5]:
        with pytest.raises(TypeError):
            entry_record(invalid, ConversationId(1), {"kind": "app.note"})  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            entry_record(EntryId(1), invalid, {"kind": "app.note"})  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            entry_record(
                EntryId(1),
                ConversationId(1),
                {"kind": "app.note"},
                by_task_id=invalid,  # type: ignore[arg-type]
            )


def test_entry_record_rejects_identity_fields_in_a_draft() -> None:
    with pytest.raises(TypeError, match="unknown field"):
        entry_record(EntryId(1), ConversationId(1), {"kind": "app.note", "conversationId": 2})

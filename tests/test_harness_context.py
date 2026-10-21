"""Translated v1.3.0 ``harness-context`` suite: context derivation cases.

The Harness-level cases of that suite — kept ranges across a task's reads and their retention,
``TaskRuntime.context()``, and the freeze guarantees of a kept range — arrive with the Harness
(#12-#22) and are skipped here with their reasons.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import NamedTuple

import pytest
from harness_support import assistant, describe_message, system, tool_result, user
from session_support import context, create_conversation, open_test_session

from pi_durable.harness.types import ContextView
from pi_durable.ids import ConversationId, EntryId
from pi_durable.session.session import SessionImpl
from pi_durable.types import ConversationRecord, EntryDraft, EntryRecord, MessageRecord


class Setup(NamedTuple):
    session: SessionImpl
    root_id: ConversationId
    append: Callable[[EntryDraft], Awaitable[EntryRecord]]
    message: Callable[..., Awaitable[EntryRecord]]
    read: Callable[..., Awaitable[ContextView]]
    fork: Callable[[EntryId], Awaitable[ConversationRecord]]


async def setup() -> Setup:
    _storage, session, _publications = open_test_session()
    root_id = await create_conversation(session)

    async def append(draft: EntryDraft) -> EntryRecord:
        return await session.commit(lambda tx: tx.append_entry(root_id, draft), context)

    async def message(model: MessageRecord, kind: str = "message") -> EntryRecord:
        return await append({"kind": kind, "model": [model]})

    async def read(
        conversation_id: ConversationId | None = None,
        at: EntryId | None = None,
    ) -> ContextView:
        return await session.context(
            root_id if conversation_id is None else conversation_id, context, at
        )

    async def fork(at: EntryId) -> ConversationRecord:
        return await session.commit(
            lambda tx: tx.fork_conversation(root_id, at, {"ownership": {"kind": "ownerless"}}),
            context,
        )

    return Setup(session, root_id, append, message, read, fork)


def ids(entries: list[EntryRecord]) -> list[EntryId]:
    return [entry["id"] for entry in entries]


def rendered(view: ContextView) -> list[str]:
    return [describe_message(message) for message in view["messages"]]


async def test_returns_the_whole_transcript_without_a_head_and_excludes_model_less_entries() -> None:
    s = await setup()
    first = await s.message(user("hi"))
    note = await s.append({"kind": "note", "data": {"text": "display only"}})
    answer = await s.message(assistant("hello"))
    view = await s.read()
    assert view["head"] is None
    assert ids(view["entries"]) == [first["id"], note["id"], answer["id"]]
    assert rendered(view) == ["user:hi", "assistant:hello"]


async def test_excludes_aborted_error_and_deferred_assistant_messages_but_keeps_their_entries() -> None:
    s = await setup()
    await s.message(user("q"))
    aborted = await s.message(assistant("partial", {"stop_reason": "aborted"}))
    await s.message(assistant("failed", {"stop_reason": "error"}))
    await s.message(assistant("later", {"stop_reason": "deferred"}))
    await s.message(assistant("done", {"stop_reason": "length"}))
    view = await s.read()
    assert len(view["entries"]) == 5
    assert view["entries"][1]["id"] == aborted["id"]
    assert rendered(view) == ["user:q", "assistant:done"]


async def test_resolves_self_heads_and_uses_the_newest_head_marker() -> None:
    s = await setup()
    await s.message(user("old"))
    reset = await s.append({"kind": "reset", "head": "self", "model": [user("fresh start")]})
    assert reset["head"] == reset["id"]
    after = await s.message(assistant("after reset"))
    view = await s.read()
    assert view["head"] is not None and view["head"]["id"] == reset["id"]
    assert ids(view["entries"]) == [reset["id"], after["id"]]
    assert rendered(view) == ["user:fresh start", "assistant:after reset"]

    # A compaction summary heads an earlier kept entry; older head markers in range drop out.
    summary = await s.append({"kind": "summary", "head": after["id"], "model": [user("summary")]})
    tail = await s.message(user("next"))
    view = await s.read()
    assert view["head"] is not None and view["head"]["id"] == summary["id"]
    assert ids(view["entries"]) == [summary["id"], after["id"], tail["id"]]
    assert rendered(view) == ["user:summary", "assistant:after reset", "user:next"]


async def test_applies_the_newest_edit_per_target_within_the_active_range() -> None:
    s = await setup()
    first = await s.message(user("first"))
    second = await s.message(user("second"))
    await s.append(
        {
            "kind": "edit",
            "edits": [{"target": first["id"], "action": "replace", "messages": [user("first v2")]}],
        }
    )
    await s.append(
        {
            "kind": "edit",
            "edits": [{"target": first["id"], "action": "replace", "messages": [user("first v3")]}],
        }
    )
    await s.append(
        {"kind": "edit", "edits": [{"target": second["id"], "action": "omit"}]}
    )
    view = await s.read()
    assert len(view["entries"]) == 5
    assert rendered(view) == ["user:first v3"]

    # Edits before the active range no longer apply.
    reset = await s.append({"kind": "reset", "head": second["id"]})
    view = await s.read()
    assert view["head"] is not None and view["head"]["id"] == reset["id"]
    assert rendered(view) == []
    await s.append(
        {
            "kind": "edit",
            "edits": [{"target": second["id"], "action": "replace", "messages": [user("second v2")]}],
        }
    )
    view = await s.read()
    assert rendered(view) == ["user:second v2"]


async def test_keeps_positional_system_messages_and_orders_tool_results_by_call_order() -> None:
    s = await setup()
    await s.message(system({"preamble": "You help."}), "pi.system")
    await s.message(user("run tools"))
    await s.message(assistant("calling", {"calls": ["b", "a"]}))
    await s.message(tool_result("a"))
    await s.append({"kind": "pi.system", "model": [system({"cwd": "/repo"})]})
    await s.message(tool_result("b"))
    await s.message(tool_result("zz"))
    await s.message(assistant("done"))
    view = await s.read()
    assert rendered(view) == [
        "system:preamble",
        "user:run tools",
        "assistant:calling",
        "result:b:result b",
        "result:a:result a",
        "system:cwd",
        "assistant:done",
    ]


async def test_leads_with_a_system_message_that_only_user_messages_precede() -> None:
    s = await setup()
    await s.message(user("first"))
    await s.message(user("steered"))
    await s.message(system({"preamble": "You help."}), "pi.system")
    await s.message(assistant("answer"))
    await s.message(user("next"))
    await s.message(system({"cwd": "/repo"}), "pi.system")
    view = await s.read()
    assert rendered(view) == [
        "system:preamble",
        "user:first",
        "user:steered",
        "assistant:answer",
        "user:next",
        "system:cwd",
    ]
    # Stored order and contributions stay as committed.
    assert [
        describe_message(message)
        for contributions in view["contributions"]
        for message in contributions
    ][:3] == [
        "user:first",
        "user:steered",
        "system:preamble",
    ]

    # After a reset, the baseline written after the handoff leads too.
    await s.append({"kind": "reset", "head": "self", "model": [user("handoff")]})
    await s.message(system({"preamble": "Baseline."}), "pi.system")
    view = await s.read()
    assert rendered(view) == ["system:preamble", "user:handoff"]


async def test_leads_with_the_baseline_after_a_compaction_that_keeps_assistant_messages() -> None:
    s = await setup()
    await s.message(user("old"))
    kept = await s.message(user("kept"))
    await s.message(assistant("calling", {"calls": ["a"]}))
    await s.message(tool_result("a"))
    await s.message(assistant("answer"))
    await s.append({"kind": "summary", "head": kept["id"], "model": [user("summary")]})
    await s.message(user("next"))
    await s.message(system({"preamble": "Baseline."}), "pi.system")
    view = await s.read()
    assert rendered(view) == [
        "system:preamble",
        "user:summary",
        "user:kept",
        "assistant:calling",
        "result:a:result a",
        "assistant:answer",
        "user:next",
    ]

    # Later system messages stay in place.
    await s.message(assistant("done"))
    await s.message(system({"cwd": "/repo"}), "pi.system")
    view = await s.read()
    assert rendered(view)[:2] == ["system:preamble", "user:summary"]
    assert rendered(view)[-2:] == ["assistant:done", "system:cwd"]


async def test_leads_with_a_baseline_written_after_the_first_answers() -> None:
    s = await setup()
    await s.message(user("first"), "pi.user")
    await s.message(assistant("answer"), "pi.assistant")
    await s.message(user("next"), "pi.user")
    await s.message(system({"plan": "Plan."}), "pi.system")
    view = await s.read()
    assert rendered(view) == ["system:plan", "user:first", "assistant:answer", "user:next"]


async def test_keeps_system_messages_other_than_the_baseline_in_place() -> None:
    s = await setup()
    await s.message(user("first"))
    await s.message(assistant("answer"))
    # Another kind's system message is positional; the baseline after it is not the first system message.
    await s.message(system({"note": "From here on."}), "host.note")
    await s.message(system({"preamble": "Baseline."}), "pi.system")
    view = await s.read()
    assert rendered(view) == [
        "user:first",
        "assistant:answer",
        "system:note",
        "system:preamble",
    ]

    # After a compaction that keeps the other kind's system message, the new baseline stays behind it.
    kept = await s.message(user("kept"))
    await s.message(assistant("kept answer"))
    await s.message(system({"mode": "plan"}), "host.note")
    await s.append({"kind": "summary", "head": kept["id"], "model": [user("summary")]})
    await s.message(user("next"))
    await s.message(system({"preamble": "Baseline."}), "pi.system")
    view = await s.read()
    assert rendered(view) == [
        "user:summary",
        "user:kept",
        "assistant:kept answer",
        "system:mode",
        "user:next",
        "system:preamble",
    ]


async def test_does_not_lead_with_a_first_system_entry_written_after_another_system_message() -> None:
    s = await setup()
    host = await s.message(system({"legacy": "Use the legacy format."}), "host.system")
    await s.message(user("first"))
    await s.message(assistant("answer"))
    # Generation wrote this entry against the host's message: it only removes a section, so it is no baseline.
    await s.message(system({"legacy": None}), "pi.system")
    await s.append({"kind": "edit", "edits": [{"target": host["id"], "action": "omit"}]})
    view = await s.read()
    assert rendered(view) == ["user:first", "assistant:answer", "system:legacy"]

    # The same holds for a system message an edit put before it.
    other = await setup()
    first = await other.message(user("first"))
    await other.append(
        {
            "kind": "edit",
            "edits": [
                {
                    "target": first["id"],
                    "action": "replace",
                    "messages": [user("first"), system({"note": "Note."})],
                }
            ],
        }
    )
    await other.message(assistant("answer"))
    await other.message(system({"note": None}), "pi.system")
    await other.append(
        {
            "kind": "edit",
            "edits": [{"target": first["id"], "action": "replace", "messages": [user("first")]}],
        }
    )
    view = await other.read()
    assert rendered(view) == ["user:first", "assistant:answer", "system:note"]


async def test_does_not_lead_with_a_retained_edited_or_later_system_entry_in_place_of_the_baseline() -> None:
    s = await setup()
    kept = await s.message(user("kept"))
    await s.message(assistant("answer"))
    retained = await s.message(system({"cwd": "/repo"}), "pi.system")
    # Before generation writes the baseline after the head marker, a retained system entry stays in place.
    await s.append({"kind": "summary", "head": kept["id"], "model": [user("summary")]})
    await s.message(user("next"))
    view = await s.read()
    assert rendered(view) == [
        "user:summary",
        "user:kept",
        "assistant:answer",
        "system:cwd",
        "user:next",
    ]

    # The baseline omits the retained entry, as generation writes it, and leads.
    baseline = await s.append(
        {
            "kind": "pi.system",
            "model": [system({"preamble": "Baseline."})],
            "edits": [{"target": retained["id"], "action": "omit"}],
        }
    )
    await s.message(assistant("done"))
    await s.message(system({"preamble": None}), "pi.system")
    view = await s.read()
    assert rendered(view) == [
        "system:preamble",
        "user:summary",
        "user:kept",
        "assistant:answer",
        "user:next",
        "assistant:done",
        "system:preamble",
    ]

    # An omitted baseline leaves no message to lead; the delta after it is not a baseline and stays in place.
    await s.append({"kind": "edit", "edits": [{"target": baseline["id"], "action": "omit"}]})
    view = await s.read()
    assert rendered(view) == [
        "user:summary",
        "user:kept",
        "assistant:answer",
        "user:next",
        "assistant:done",
        "system:preamble",
    ]

    # A replacement of the baseline is not one either.
    await s.append(
        {
            "kind": "edit",
            "edits": [
                {
                    "target": baseline["id"],
                    "action": "replace",
                    "messages": [system({"other": "Other."})],
                }
            ],
        }
    )
    view = await s.read()
    assert rendered(view) == [
        "user:summary",
        "user:kept",
        "assistant:answer",
        "user:next",
        "system:other",
        "assistant:done",
        "system:preamble",
    ]


async def test_synthesizes_missing_tool_results_after_a_fork_and_drops_results_cut_from_their_call() -> None:
    s = await setup()
    await s.message(user("go"))
    call = await s.message(assistant("calling", {"calls": ["x", "y"]}))
    await s.message(tool_result("x"))
    second = await s.message(tool_result("y"))
    child = await s.fork(call["id"])
    child_view = await s.read(child["id"])
    assert rendered(child_view) == [
        "user:go",
        "assistant:calling",
        "result:x:error",
        "result:y:error",
    ]
    missing = child_view["messages"][2]
    assert missing["role"] == "toolResult"
    assert missing["toolName"] == "tool-x"
    assert missing["details"] == {"reason": "missing_result"}

    # A head between a call and its results leaves stray results that are not sent.
    await s.append({"kind": "reset", "head": second["id"]})
    parent_view = await s.read()
    assert rendered(parent_view) == []
    assert [entry["kind"] for entry in parent_view["entries"]] == ["reset", "message"]


async def test_reads_the_context_as_of_an_earlier_entry_as_a_fork_at_that_entry_starts() -> None:
    s = await setup()
    first = await s.message(user("first"))
    call = await s.message(assistant("calling", {"calls": ["x", "y"]}))
    result = await s.message(tool_result("x"))
    await s.message(tool_result("y"))
    edit = await s.append(
        {
            "kind": "edit",
            "edits": [{"target": first["id"], "action": "replace", "messages": [user("first v2")]}],
        }
    )
    reset = await s.append({"kind": "reset", "head": "self", "model": [user("fresh start")]})
    tail = await s.message(assistant("after reset"))

    for at in [first, call, result, edit, reset, tail]:
        fork = await s.fork(at["id"])
        assert await s.read(at=at["id"]) == await s.read(fork["id"])
    # Stepped back before the edit and the reset: neither applies, and a cut call gets synthesized results.
    cut_view = await s.read(at=call["id"])
    assert rendered(cut_view) == [
        "user:first",
        "assistant:calling",
        "result:x:error",
        "result:y:error",
    ]
    assert await s.read(at=tail["id"]) == await s.read()
    assert await s.read(at=None) == await s.read()

    other = await s.fork(first["id"])
    with pytest.raises(ValueError, match="is not visible"):
        await s.read(other["id"], at=tail["id"])


async def test_returns_the_empty_view_for_a_conversation_without_entries() -> None:
    s = await setup()
    assert await s.read() == {"head": None, "entries": [], "contributions": [], "messages": []}


@pytest.mark.skip(
    reason="Extends a task's context read and its scanned rows; "
    "TaskRuntime.context() arrives with #9-#10."
)
async def test_extends_a_tasks_context_read_with_only_newer_entries() -> None:
    raise AssertionError("covered with TaskRuntime.context()")


@pytest.mark.skip(
    reason="Keeps a context read across tasks and for the retention period; "
    "the Harness arrives in #12-#22."
)
async def test_keeps_a_context_read_across_tasks_and_for_the_retention_period_once_idle() -> None:
    raise AssertionError("covered with the Harness context retention")


@pytest.mark.skip(
    reason="Compares extended reads with whole reads from a task; "
    "TaskRuntime arrives with #9-#10."
)
async def test_derives_the_same_view_from_extended_reads_as_from_whole_reads() -> None:
    raise AssertionError("covered with the Harness kept-range extension")


@pytest.mark.skip(
    reason="Finds the baseline among entries an extended read adds; "
    "the kept range arrives with the Harness in #12-#22."
)
async def test_finds_the_baseline_among_the_entries_an_extended_read_adds() -> None:
    raise AssertionError("covered with the Harness kept-range extension")


@pytest.mark.skip(
    reason="Keeps returned views and kept ranges apart; the kept range arrives with the Harness."
)
async def test_keeps_returned_views_and_kept_ranges_apart() -> None:
    raise AssertionError("covered with the Harness kept-range cache")


@pytest.mark.skip(
    reason="Freezes kept entries deeply; the kept range arrives with the Harness."
)
async def test_freezes_kept_entries_deeply_even_when_storage_freezes_them_shallowly() -> None:
    raise AssertionError("covered with the Harness kept-range cache")


@pytest.mark.skip(
    reason="Drops a read that finishes after its task ended; "
    "TaskRuntime and context retention arrive with #9-#10."
)
async def test_does_not_keep_a_read_that_finishes_after_its_task_ended() -> None:
    raise AssertionError("covered with TaskRuntime.context()")


@pytest.mark.skip(
    reason="Drops an idle context read on a timer; the Harness context retention arrives in #12-#22."
)
async def test_drops_an_idle_context_read_on_a_timer_when_nothing_else_runs() -> None:
    raise AssertionError("covered with the Harness context retention")


@pytest.mark.skip(
    reason="Drops a context read once idle with zero retention; the Harness arrives in #12-#22."
)
async def test_drops_a_context_read_once_idle_with_zero_retention() -> None:
    raise AssertionError("covered with the Harness context retention")

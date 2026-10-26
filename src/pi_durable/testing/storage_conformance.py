"""Runner-independent storage conformance cases, translated from v1.3.0 ``testing``.

The suite covers atomic commits, detached values, cursor pagination and order, fork ancestry,
task and submission indexes, document history and copies, global ID ownership, and close
behavior. It exercises only the public :class:`~pi_durable.types.Storage` contract.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeAlias, cast

from ..context import BACKGROUND_CONTEXT
from ..ids import (
    ROOT_CONVERSATION_ID,
    ConversationId,
    EntryId,
    id_from_number,
)
from ..types import Cursor, Page, ScanOrder, Storage, StorageWrite
from .types import (
    StorageConformanceAssertions,
    StorageConformanceCase,
    StorageConformanceOptions,
)

context = BACKGROUND_CONTEXT

_MAX_SAFE_INTEGER = 9_007_199_254_740_991


async def _create_root(storage: Storage) -> ConversationId:
    await storage.commit([{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context)
    return ROOT_CONVERSATION_ID


def _pending_task(id: int, conversation_id: ConversationId, phase: str = "ready") -> dict[str, Any]:
    return {
        "id": id,
        "conversationId": conversation_id,
        "kind": "test.task",
        "version": 1,
        "input": {"value": id},
        "state": {"status": "pending", "checkpoint": {"phase": phase}},
        "background": False,
        "abortRequested": False,
    }


def _entry(
    id: int,
    conversation_id: ConversationId,
    kind: str = "message",
    **extra: Any,
) -> dict[str, Any]:
    return {"id": id, "conversationId": conversation_id, "kind": kind, **extra}


class _Rejects:
    def __init__(self, actual: object, assertions: StorageConformanceAssertions) -> None:
        self._actual = actual
        self._assertions = assertions

    async def to_throw(self, message_includes: str) -> None:
        await self._assertions.rejects(cast(Awaitable[object], self._actual), message_includes)


class _Resolves:
    def __init__(self, actual: object, assertions: StorageConformanceAssertions) -> None:
        self._actual = actual
        self._assertions = assertions

    async def to_be(self, expected: object) -> None:
        self._assertions.strict_equal(await cast(Awaitable[Any], self._actual), expected)


class _AssertionResult:
    def __init__(self, actual: object, assertions: StorageConformanceAssertions) -> None:
        self._actual = actual
        self._assertions = assertions

    def to_be(self, expected: object) -> None:
        self._assertions.strict_equal(self._actual, expected)

    def to_be_defined(self) -> None:
        self._assertions.ok(self._actual is not None, "Expected value to be defined")

    def to_be_greater_than(self, expected: float) -> None:
        self._assertions.greater_than(cast(float, self._actual), expected)

    def to_be_undefined(self) -> None:
        self._assertions.strict_equal(self._actual, None)

    def to_equal(self, expected: object) -> None:
        self._assertions.deep_equal(self._actual, expected)

    def to_have_length(self, expected: int) -> None:
        self._assertions.strict_equal(len(cast(Sequence[Any], self._actual)), expected)

    def to_match_object(self, expected: object) -> None:
        self._assertions.partial_deep_equal(self._actual, expected)

    @property
    def rejects(self) -> _Rejects:
        return _Rejects(self._actual, self._assertions)

    @property
    def resolves(self) -> _Resolves:
        return _Resolves(self._actual, self._assertions)


def _assertion_facade(
    assertions: StorageConformanceAssertions,
) -> Callable[[object], _AssertionResult]:
    return lambda actual: _AssertionResult(actual, assertions)


def _create_case(
    options: StorageConformanceOptions,
    name: str,
    test: Callable[[Storage], Awaitable[None]],
) -> StorageConformanceCase:
    async def run() -> None:
        await options["with_storage"](test)

    return StorageConformanceCase(name=name, run=run)


def create_storage_conformance(options: StorageConformanceOptions) -> list[StorageConformanceCase]:
    """Create runner-independent cases; the provider calls and awaits its callback once per case."""

    expect = _assertion_facade(options["assertions"])

    async def reserves_id_one(storage: Storage) -> None:
        expect(await storage.mint_id()).to_be(2)
        await expect(_create_root(storage)).resolves.to_be(ROOT_CONVERSATION_ID)
        expect(await storage.conversation(ROOT_CONVERSATION_ID, context)).to_equal(
            {"id": ROOT_CONVERSATION_ID}
        )
        await expect(
            storage.commit(
                [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
            )
        ).rejects.to_throw(f"ID {ROOT_CONVERSATION_ID} already belongs to conversation")

    async def commits_mixed_table_writes(storage: Storage) -> None:
        root_id = await _create_root(storage)
        entry_id = await storage.mint_id()
        task_id = await storage.mint_id()
        submission_id = await storage.mint_id()
        task = _pending_task(task_id, root_id)
        submission = {
            "id": submission_id,
            "conversationId": root_id,
            "requestId": "request-1",
            "type": "input",
            "status": "placed",
            "entry": entry_id,
        }
        initial_seq = await storage.commit(
            [
                {
                    "type": "entry",
                    "value": _entry(entry_id, root_id, "user", data={"text": "hello"}),
                },
                {"type": "task", "value": task},
                {"type": "submission", "value": submission},
            ],
            context,
        )

        expect(await storage.entry(entry_id, context)).to_equal(
            {
                "entry": _entry(entry_id, root_id, "user", data={"text": "hello"}),
                "commitSeq": initial_seq,
            }
        )
        expect(await storage.task(task_id, context)).to_equal(task)
        expect(await storage.submission(submission_id, context)).to_equal(submission)

        transient_entry_id = await storage.mint_id()
        running_task = {**task, "state": {"status": "running", "checkpoint": {"phase": "effect"}}}
        done_submission = {**submission, "status": "done", "answer": transient_entry_id}
        await expect(
            storage.commit(
                [
                    {"type": "task", "value": running_task},
                    {"type": "submission", "value": done_submission},
                    {"type": "entry", "value": _entry(transient_entry_id, root_id, "assistant")},
                    {"type": "conversation", "value": {"id": root_id}},
                ],
                context,
            )
        ).rejects.to_throw(f"ID {root_id} already belongs to conversation")

        expect(await storage.task(task_id, context)).to_equal(task)
        expect(await storage.submission(submission_id, context)).to_equal(submission)
        expect(await storage.entry(transient_entry_id, context)).to_be_undefined()
        after_rollback_seq = await storage.commit(
            [
                {
                    "type": "entry",
                    "value": _entry(await storage.mint_id(), root_id, "after-rollback"),
                }
            ],
            context,
        )
        expect(after_rollback_seq).to_be_greater_than(initial_seq)

    async def detaches_retained_writes(storage: Storage) -> None:
        root_id = await _create_root(storage)
        entry_id = await storage.mint_id()
        task_id = await storage.mint_id()
        submission_id = await storage.mint_id()
        entry_data: dict[str, Any] = {"nested": [1, 2]}
        checkpoint: dict[str, Any] = {"phase": "ready", "nested": {"count": 1}}
        detail: dict[str, Any] = {"codes": ["initial"]}
        stored_entry = _entry(entry_id, root_id, "note", data=entry_data)
        stored_task = {
            **_pending_task(task_id, root_id),
            "state": {"status": "pending", "checkpoint": checkpoint},
        }
        stored_submission = {
            "id": submission_id,
            "conversationId": root_id,
            "type": "input",
            "status": "unanswered",
            "reason": "failed",
            "detail": detail,
        }
        await storage.commit(
            [
                {"type": "entry", "value": stored_entry},
                {"type": "task", "value": stored_task},
                {"type": "submission", "value": stored_submission},
            ],
            context,
        )

        entry_data["nested"].append(3)
        checkpoint["nested"]["count"] = 2
        detail["codes"].append("mutated")
        read_entry = await storage.entry(entry_id, context)
        assert read_entry is not None
        expect(read_entry["entry"]["data"]).to_equal({"nested": [1, 2]})
        read_task = await storage.task(task_id, context)
        assert read_task is not None
        expect(read_task["state"]).to_equal(
            {"status": "pending", "checkpoint": {"phase": "ready", "nested": {"count": 1}}}
        )
        read_submission = await storage.submission(submission_id, context)
        assert read_submission is not None
        expect(read_submission["detail"]).to_equal({"codes": ["initial"]})

        read_entry["entry"]["data"]["nested"].append(9)
        if read_task["state"]["status"] != "terminal":
            read_task["state"]["checkpoint"]["nested"]["count"] = 9
        read_submission["detail"]["codes"].append("read mutation")

        again_entry = await storage.entry(entry_id, context)
        assert again_entry is not None
        expect(again_entry["entry"]["data"]).to_equal({"nested": [1, 2]})
        again_task = await storage.task(task_id, context)
        assert again_task is not None
        expect(again_task["state"]).to_equal(
            {"status": "pending", "checkpoint": {"phase": "ready", "nested": {"count": 1}}}
        )
        again_submission = await storage.submission(submission_id, context)
        assert again_submission is not None
        expect(again_submission["detail"]).to_equal({"codes": ["initial"]})

    async def detaches_prototype_like_keys(storage: Storage) -> None:
        # Python has no prototype chain, so the analogue is that these names stay ordinary,
        # lossless JSON keys and stored values are detached from the caller's objects.
        root_id = await _create_root(storage)
        entry_id = await storage.mint_id()
        data: dict[str, Any] = json.loads(
            '{"__proto__":{"polluted":false},"constructor":{"label":"stored"},"toString":"value"}'
        )
        await storage.commit(
            [{"type": "entry", "value": _entry(entry_id, root_id, "note", data=data)}], context
        )

        data["__proto__"]["polluted"] = True
        data["constructor"]["label"] = "mutated"
        first_read = await storage.entry(entry_id, context)
        assert first_read is not None
        first_data = first_read["entry"]["data"]
        expect(sorted(first_data)).to_equal(["__proto__", "constructor", "toString"])
        expect(first_data["__proto__"]).to_equal({"polluted": False})
        expect(first_data["constructor"]).to_equal({"label": "stored"})
        expect(first_data["toString"]).to_be("value")

        first_data["__proto__"]["polluted"] = True
        second_read = await storage.entry(entry_id, context)
        assert second_read is not None
        second_data = second_read["entry"]["data"]
        expect(second_data["__proto__"]).to_equal({"polluted": False})
        expect(second_data["constructor"]).to_equal({"label": "stored"})
        expect(second_data["toString"]).to_be("value")

    async def indexes_entries_out_of_order(storage: Storage) -> None:
        root_id = await _create_root(storage)
        await storage.commit(
            [
                {"type": "entry", "value": _entry(30, root_id)},
                {"type": "entry", "value": _entry(10, root_id)},
                {
                    "type": "entry",
                    "value": _entry(20, root_id, "marker", head=EntryId(10)),
                },
            ],
            context,
        )

        page = await storage.scan_entries({"conversationId": root_id}, 10, None, context)
        expect([item["id"] for item in page["items"]]).to_equal([30, 20, 10])
        marker = await storage.find_latest_head_marker(root_id, None, context)
        assert marker is not None
        expect(marker["id"]).to_be(20)

    async def continues_entry_cursor(storage: Storage) -> None:
        root_id = await _create_root(storage)
        oldest_id = await storage.mint_id()
        middle_id = await storage.mint_id()
        newest_id = await storage.mint_id()
        await storage.commit(
            [
                {"type": "entry", "value": _entry(oldest_id, root_id)},
                {"type": "entry", "value": _entry(middle_id, root_id)},
                {"type": "entry", "value": _entry(newest_id, root_id)},
            ],
            context,
        )

        first = await storage.scan_entries({"conversationId": root_id}, 2, None, context)
        expect([item["id"] for item in first["items"]]).to_equal([newest_id, middle_id])
        appended_id = await storage.mint_id()
        await storage.commit([{"type": "entry", "value": _entry(appended_id, root_id)}], context)
        second = await storage.scan_entries({"conversationId": root_id}, 2, first.get("next"), context)
        expect([item["id"] for item in second["items"]]).to_equal([oldest_id])
        expect(second.get("next")).to_be_undefined()

    async def paginates_conversations(storage: Storage) -> None:
        root_id = await _create_root(storage)
        second_id = await storage.mint_id()
        third_id = await storage.mint_id()
        await storage.commit(
            [
                {"type": "conversation", "value": {"id": third_id}},
                {"type": "conversation", "value": {"id": second_id}},
            ],
            context,
        )

        first = await storage.scan_conversations({}, 2, None, context)
        expect([item["id"] for item in first["items"]]).to_equal([root_id, second_id])
        expect(first.get("next")).to_be_defined()
        round_tripped: Cursor = json.loads(json.dumps(first["next"]))
        second = await storage.scan_conversations({}, 2, round_tripped, context)
        expect([item["id"] for item in second["items"]]).to_equal([third_id])
        expect(second.get("next")).to_be_undefined()

    async def filters_conversations_by_owner(storage: Storage) -> None:
        root_id = await _create_root(storage)
        other_owner_id = await storage.mint_id()
        first_task_id = await storage.mint_id()
        second_task_id = await storage.mint_id()
        first_id = await storage.mint_id()
        second_id = await storage.mint_id()
        third_id = await storage.mint_id()
        await storage.commit(
            [
                {"type": "conversation", "value": {"id": other_owner_id}},
                {
                    "type": "conversation",
                    "value": {
                        "id": first_id,
                        "owner": {"conversationId": root_id, "taskId": first_task_id},
                    },
                },
                {
                    "type": "conversation",
                    "value": {
                        "id": second_id,
                        "owner": {"conversationId": root_id, "taskId": second_task_id},
                    },
                },
                {
                    "type": "conversation",
                    "value": {
                        "id": third_id,
                        "owner": {"conversationId": other_owner_id, "taskId": first_task_id},
                    },
                },
            ],
            context,
        )

        first = await storage.scan_conversations(
            {"ownerConversationId": root_id}, 1, None, context
        )
        expect([item["id"] for item in first["items"]]).to_equal([first_id])
        expect(first.get("next")).to_be_defined()
        second = await storage.scan_conversations(
            {"ownerConversationId": root_id}, 1, first.get("next"), context
        )
        expect([item["id"] for item in second["items"]]).to_equal([second_id])
        expect(second.get("next")).to_be_undefined()
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_conversations({"ownerTaskId": first_task_id}, 10, None, context)
                )["items"]
            ]
        ).to_equal([first_id, third_id])
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_conversations(
                        {"ownerConversationId": root_id, "ownerTaskId": first_task_id},
                        10,
                        None,
                        context,
                    )
                )["items"]
            ]
        ).to_equal([first_id])

    async def scans_deep_fork_history(storage: Storage) -> None:
        root_id = await _create_root(storage)
        root_first = await storage.mint_id()
        root_fork_point = await storage.mint_id()
        root_excluded_same_commit = await storage.mint_id()
        root_entries_seq = await storage.commit(
            [
                {"type": "entry", "value": _entry(root_first, root_id)},
                {
                    "type": "entry",
                    "value": _entry(root_fork_point, root_id, "marker", head=root_first),
                },
                {"type": "entry", "value": _entry(root_excluded_same_commit, root_id)},
            ],
            context,
        )
        child_id = await storage.mint_id()
        await storage.commit(
            [
                {
                    "type": "conversation",
                    "value": {
                        "id": child_id,
                        "parent": {"conversationId": root_id, "at": root_fork_point},
                    },
                },
            ],
            context,
        )
        child_fork_point = await storage.mint_id()
        child_excluded = await storage.mint_id()
        await storage.commit(
            [
                {"type": "entry", "value": _entry(child_fork_point, child_id, "note")},
                {"type": "entry", "value": _entry(child_excluded, child_id)},
            ],
            context,
        )
        root_excluded_later = await storage.mint_id()
        await storage.commit(
            [{"type": "entry", "value": _entry(root_excluded_later, root_id)}], context
        )
        grandchild_id = await storage.mint_id()
        await storage.commit(
            [
                {
                    "type": "conversation",
                    "value": {
                        "id": grandchild_id,
                        "parent": {"conversationId": child_id, "at": child_fork_point},
                    },
                },
            ],
            context,
        )
        grandchild_head = await storage.mint_id()
        grandchild_tail = await storage.mint_id()
        grandchild_entries_seq = await storage.commit(
            [
                {
                    "type": "entry",
                    "value": _entry(grandchild_head, grandchild_id, "marker", head=grandchild_head),
                },
                {"type": "entry", "value": _entry(grandchild_tail, grandchild_id)},
            ],
            context,
        )
        child_excluded_later = await storage.mint_id()
        await storage.commit(
            [{"type": "entry", "value": _entry(child_excluded_later, child_id)}], context
        )

        first = await storage.scan_entries({"conversationId": grandchild_id}, 2, None, context)
        expect([item["id"] for item in first["items"]]).to_equal(
            [grandchild_tail, grandchild_head]
        )
        second = await storage.scan_entries(
            {"conversationId": grandchild_id}, 2, first.get("next"), context
        )
        expect([item["id"] for item in second["items"]]).to_equal(
            [child_fork_point, root_fork_point]
        )
        third = await storage.scan_entries(
            {"conversationId": grandchild_id}, 2, second.get("next"), context
        )
        expect([item["id"] for item in third["items"]]).to_equal([root_first])
        expect(third.get("next")).to_be_undefined()

        # Oldest first: the root's segment, then each fork's, each capped at the next fork point.
        ascending: dict[str, Any] = {"conversationId": grandchild_id, "order": "ascending"}
        first_up = await storage.scan_entries(ascending, 2, None, context)
        expect([item["id"] for item in first_up["items"]]).to_equal([root_first, root_fork_point])
        second_up = await storage.scan_entries(ascending, 2, first_up.get("next"), context)
        expect([item["id"] for item in second_up["items"]]).to_equal(
            [child_fork_point, grandchild_head]
        )
        third_up = await storage.scan_entries(
            {"conversationId": grandchild_id}, 2, second_up.get("next"), context
        )
        expect([item["id"] for item in third_up["items"]]).to_equal([grandchild_tail])
        expect(third_up.get("next")).to_be_undefined()
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_entries(
                        {
                            **ascending,
                            "minEntryId": root_fork_point,
                            "maxEntryId": child_fork_point,
                        },
                        10,
                        None,
                        context,
                    )
                )["items"]
            ]
        ).to_equal([root_fork_point, child_fork_point])
        await expect(
            storage.scan_entries(
                {"conversationId": grandchild_id, "order": "descending"},
                2,
                first_up.get("next"),
                context,
            )
        ).rejects.to_throw("cursor")

        current_marker = await storage.find_latest_head_marker(grandchild_id, None, context)
        assert current_marker is not None
        expect(current_marker["id"]).to_be(grandchild_head)
        expect(current_marker["head"]).to_be(grandchild_head)
        historical_marker = await storage.find_latest_head_marker(
            grandchild_id, child_fork_point, context
        )
        assert historical_marker is not None
        expect(historical_marker["id"]).to_be(root_fork_point)
        expect(historical_marker["head"]).to_be(root_first)
        expect(
            await storage.find_latest_head_marker(grandchild_id, root_first, context)
        ).to_be_undefined()

        active_first = await storage.scan_entries(
            {"conversationId": grandchild_id, "minEntryId": current_marker["head"]},
            1,
            None,
            context,
        )
        expect([item["id"] for item in active_first["items"]]).to_equal([grandchild_tail])
        expect(active_first.get("next")).to_be_defined()
        active_second = await storage.scan_entries(
            {"conversationId": grandchild_id, "minEntryId": current_marker["head"]},
            1,
            active_first.get("next"),
            context,
        )
        expect([item["id"] for item in active_second["items"]]).to_equal([grandchild_head])
        expect(active_second.get("next")).to_be_undefined()

        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_entries(
                        {
                            "conversationId": grandchild_id,
                            "minEntryId": historical_marker["head"],
                            "maxEntryId": child_fork_point,
                        },
                        10,
                        None,
                        context,
                    )
                )["items"]
            ]
        ).to_equal([child_fork_point, root_fork_point, root_first])

        expect(await storage.entry(root_first, context)).to_equal(
            {"entry": _entry(root_first, root_id), "commitSeq": root_entries_seq}
        )
        root_fork_lookup = await storage.entry(root_fork_point, context)
        assert root_fork_lookup is not None
        expect(root_fork_lookup["commitSeq"]).to_be(root_entries_seq)
        grandchild_head_lookup = await storage.entry(grandchild_head, context)
        assert grandchild_head_lookup is not None
        expect(grandchild_head_lookup["commitSeq"]).to_be(grandchild_entries_seq)
        grandchild_tail_lookup = await storage.entry(grandchild_tail, context)
        assert grandchild_tail_lookup is not None
        expect(grandchild_tail_lookup["commitSeq"]).to_be(grandchild_entries_seq)
        expect(
            await storage.entry(id_from_number(999_999), context)
        ).to_be_undefined()

        expect(await storage.entry(grandchild_id, root_first, context)).to_equal(
            {"entry": _entry(root_first, root_id), "commitSeq": root_entries_seq}
        )
        child_fork_lookup = await storage.entry(grandchild_id, child_fork_point, context)
        assert child_fork_lookup is not None
        expect(child_fork_lookup["entry"]["conversationId"]).to_be(child_id)
        tail_lookup = await storage.entry(grandchild_id, grandchild_tail, context)
        assert tail_lookup is not None
        expect(tail_lookup["commitSeq"]).to_be(grandchild_entries_seq)
        expect(
            await storage.entry(grandchild_id, root_excluded_same_commit, context)
        ).to_be_undefined()
        expect(
            await storage.entry(grandchild_id, root_excluded_later, context)
        ).to_be_undefined()
        expect(await storage.entry(grandchild_id, child_excluded, context)).to_be_undefined()
        expect(
            await storage.entry(grandchild_id, child_excluded_later, context)
        ).to_be_undefined()
        expect(await storage.entry(root_id, grandchild_head, context)).to_be_undefined()
        expect(await storage.entry(grandchild_id, id_from_number(999_999), context)).to_be_undefined()
        await expect(
            storage.entry(id_from_number(999_999), root_first, context)
        ).rejects.to_throw("Unknown conversation")
        await expect(
            storage.scan_entries(
                {"conversationId": id_from_number(999_999)}, 10, None, context
            )
        ).rejects.to_throw("Unknown conversation")

    async def scans_tables_in_either_order(storage: Storage) -> None:
        root_id = await _create_root(storage)
        conversation_ids: list[int] = [root_id]
        task_ids: list[int] = []
        submission_ids: list[int] = []
        for _ in range(3):
            conversation_id = await storage.mint_id()
            task_id = await storage.mint_id()
            submission_id = await storage.mint_id()
            await storage.commit(
                [
                    {"type": "conversation", "value": {"id": conversation_id}},
                    {"type": "task", "value": _pending_task(task_id, root_id)},
                    {
                        "type": "submission",
                        "value": {
                            "id": submission_id,
                            "conversationId": root_id,
                            "type": "input",
                            "status": "queued",
                        },
                    },
                ],
                context,
            )
            conversation_ids.append(conversation_id)
            task_ids.append(task_id)
            submission_ids.append(submission_id)

        Scan: TypeAlias = Callable[[ScanOrder | None, Cursor | None], Awaitable[Page[Any, Cursor]]]
        scans: list[tuple[Scan, list[int]]] = [
            (
                lambda order, cursor: storage.scan_conversations(
                    {"order": order} if order is not None else {}, 2, cursor, context
                ),
                conversation_ids,
            ),
            (
                lambda order, cursor: storage.scan_tasks(
                    {"order": order} if order is not None else {}, 2, cursor, context
                ),
                task_ids,
            ),
            (
                lambda order, cursor: storage.scan_submissions(
                    {"order": order} if order is not None else {}, 2, cursor, context
                ),
                submission_ids,
            ),
        ]

        async def all_ids(scan: Scan, order: ScanOrder | None) -> list[int]:
            found: list[int] = []
            page = await scan(order, None)
            found.extend(item["id"] for item in page["items"])
            while page.get("next") is not None:
                # A cursor carries its order; the query may omit it.
                page = await scan(None, json.loads(json.dumps(page["next"])))
                found.extend(item["id"] for item in page["items"])
            return found

        for scan, ids in scans:
            reversed_ids = list(reversed(ids))
            expect(await all_ids(scan, None)).to_equal(ids)
            expect(await all_ids(scan, "ascending")).to_equal(ids)
            expect(await all_ids(scan, "descending")).to_equal(reversed_ids)
            descending = await scan("descending", None)
            expect(
                [item["id"] for item in (await scan("descending", descending.get("next")))["items"]]
            ).to_equal(reversed_ids[2:4])
            await expect(scan("ascending", descending.get("next"))).rejects.to_throw("cursor")

    async def replaces_task_records(storage: Storage) -> None:
        root_id = await _create_root(storage)
        first_id = await storage.mint_id()
        second_id = await storage.mint_id()
        third_id = await storage.mint_id()
        first = {**_pending_task(first_id, root_id), "memos": {"winner": "first"}}
        second = {**_pending_task(second_id, root_id), "background": True}
        third = {**_pending_task(third_id, root_id), "abortRequested": True}
        await storage.commit(
            [
                {"type": "task", "value": first},
                {"type": "task", "value": second},
                {"type": "task", "value": third},
            ],
            context,
        )

        running = {
            **first,
            "state": {"status": "running", "checkpoint": {"phase": "effect", "attempt": 1}},
            "abortRequested": True,
        }
        await storage.commit([{"type": "task", "value": running}], context)
        expect(await storage.task(first_id, context)).to_equal(running)
        terminal = {
            "id": first_id,
            "conversationId": root_id,
            "kind": first["kind"],
            "version": first["version"],
            "input": first["input"],
            "state": {"status": "terminal", "outcome": {"status": "completed", "result": {"entryId": 99}}},
            "background": False,
            "abortRequested": True,
        }
        await storage.commit([{"type": "task", "value": terminal}], context)
        expect(await storage.task(first_id, context)).to_equal(terminal)

        pending_page = await storage.scan_tasks({"status": "pending"}, 1, None, context)
        expect([item["id"] for item in pending_page["items"]]).to_equal([second_id])
        expect(pending_page.get("next")).to_be_defined()
        second_page = await storage.scan_tasks(
            {"status": "pending"}, 1, pending_page.get("next"), context
        )
        expect([item["id"] for item in second_page["items"]]).to_equal([third_id])
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_tasks(
                        {"status": "terminal", "abortRequested": True}, 10, None, context
                    )
                )["items"]
            ]
        ).to_equal([first_id])
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_tasks({"background": True}, 10, None, context)
                )["items"]
            ]
        ).to_equal([second_id])

    async def stores_owners_and_scans_statuses(storage: Storage) -> None:
        root_id = await _create_root(storage)
        owner_id = await storage.mint_id()
        waiting_id = await storage.mint_id()
        completing_id = await storage.mint_id()
        owner = _pending_task(owner_id, root_id)
        waiting = {
            **_pending_task(waiting_id, root_id),
            "owner": owner_id,
            "state": {
                "status": "waiting",
                "checkpoint": {"phase": "next"},
                "on": [owner_id],
                "policy": "allSettled",
            },
            "memos": {"kept": True},
        }
        base = {key: value for key, value in _pending_task(completing_id, root_id).items() if key != "state"}
        completing = {
            **base,
            "owner": owner_id,
            "state": {"status": "completing", "outcome": {"status": "failed", "error": {"message": "held"}}},
        }
        await storage.commit(
            [
                {"type": "task", "value": owner},
                {"type": "task", "value": waiting},
                {"type": "task", "value": completing},
            ],
            context,
        )
        expect(await storage.task(waiting_id, context)).to_equal(waiting)
        expect(await storage.task(completing_id, context)).to_equal(completing)

        async def scan(status: str) -> list[Any]:
            page = await storage.scan_tasks({"status": status}, 10, None, context)
            return list(page["items"])

        expect(await scan("waiting")).to_equal([waiting])
        expect(await scan("completing")).to_equal([completing])
        expect([item["id"] for item in await scan("pending")]).to_equal([owner_id])
        terminal = {
            **completing,
            "state": {"status": "terminal", "outcome": completing["state"]["outcome"]},
        }
        await storage.commit([{"type": "task", "value": terminal}], context)
        expect(await scan("completing")).to_equal([])
        expect(await scan("terminal")).to_equal([terminal])

    async def stores_and_scans_sleeping_tasks_by_status(storage: Storage) -> None:
        root_id = await _create_root(storage)
        sleeping_id = await storage.mint_id()
        sleeping = {
            **_pending_task(sleeping_id, root_id, "next"),
            "state": {"status": "sleeping", "checkpoint": {"phase": "next"}, "until": 61_000},
            "memos": {"kept": True},
        }
        await storage.commit([{"type": "task", "value": sleeping}], context)
        expect(await storage.task(sleeping_id, context)).to_equal(sleeping)

        async def scan(status: str) -> list[Any]:
            page = await storage.scan_tasks({"status": status}, 10, None, context)
            return list(page["items"])

        expect(await scan("sleeping")).to_equal([sleeping])
        running = {**sleeping, "state": {"status": "running", "checkpoint": {"phase": "next"}}}
        await storage.commit([{"type": "task", "value": running}], context)
        expect(await scan("sleeping")).to_equal([])
        expect(await scan("running")).to_equal([running])

    async def indexes_request_ids(storage: Storage) -> None:
        root_id = await _create_root(storage)
        second_conversation_id = await storage.mint_id()
        await storage.commit(
            [{"type": "conversation", "value": {"id": second_conversation_id}}], context
        )
        first_id = await storage.mint_id()
        second_id = await storage.mint_id()
        other_conversation_id = await storage.mint_id()
        first = {
            "id": first_id,
            "conversationId": root_id,
            "requestId": "same",
            "type": "input",
            "status": "queued",
        }
        second = {
            "id": second_id,
            "conversationId": root_id,
            "requestId": "other",
            "type": "input",
            "status": "queued",
        }
        other_conversation = {
            "id": other_conversation_id,
            "conversationId": second_conversation_id,
            "requestId": "same",
            "type": "input",
            "status": "queued",
        }
        await storage.commit(
            [
                {"type": "submission", "value": first},
                {"type": "submission", "value": second},
                {"type": "submission", "value": other_conversation},
            ],
            context,
        )
        expect(await storage.submission_by_request(root_id, "same", context)).to_equal(first)
        expect(
            await storage.submission_by_request(second_conversation_id, "same", context)
        ).to_equal(other_conversation)

        placed_second = {**second, "status": "placed", "entry": await storage.mint_id()}
        await storage.commit([{"type": "submission", "value": placed_second}], context)
        expect(await storage.submission(second_id, context)).to_equal(placed_second)
        expect(await storage.submission_by_request(root_id, "other", context)).to_equal(
            placed_second
        )

        async def ids(query: dict[str, Any]) -> list[int]:
            found: list[int] = []
            cursor: Cursor | None = None
            while True:
                page = await storage.scan_submissions(query, 1, cursor, context)
                found.extend(item["id"] for item in page["items"])
                cursor = page.get("next")
                if cursor is None:
                    return found

        expect(await ids({})).to_equal([first_id, second_id, other_conversation_id])
        expect(await ids({"conversationId": root_id})).to_equal([first_id, second_id])
        # A status change moves the record between status scans.
        expect(await ids({"status": "queued"})).to_equal([first_id, other_conversation_id])
        expect(await ids({"status": "placed"})).to_equal([second_id])
        expect(
            await ids({"conversationId": second_conversation_id, "status": "queued"})
        ).to_equal([other_conversation_id])
        expect(
            await ids({"conversationId": second_conversation_id, "status": "placed"})
        ).to_equal([])
        expect(
            (await storage.scan_submissions({"status": "placed"}, 10, None, context))["items"]
        ).to_equal([placed_second])

    async def stores_passive_write_submissions(storage: Storage) -> None:
        root_id = await _create_root(storage)
        done_id = await storage.mint_id()
        failed_id = await storage.mint_id()
        queued_done = {
            "id": done_id,
            "conversationId": root_id,
            "requestId": "passive-done",
            "type": "write",
            "status": "queued",
        }
        queued_failed = {
            "id": failed_id,
            "conversationId": root_id,
            "requestId": "passive-failed",
            "type": "write",
            "status": "queued",
        }
        await storage.commit(
            [
                {"type": "submission", "value": queued_done},
                {"type": "submission", "value": queued_failed},
            ],
            context,
        )

        done = {**queued_done, "status": "done", "entry": await storage.mint_id()}
        unanswered = {
            **queued_failed,
            "status": "unanswered",
            "reason": "closed",
            "detail": {"retryable": False},
        }
        await storage.commit(
            [
                {"type": "submission", "value": done},
                {"type": "submission", "value": unanswered},
            ],
            context,
        )
        expect(await storage.submission(done_id, context)).to_equal(done)
        expect(await storage.submission_by_request(root_id, "passive-done", context)).to_equal(
            done
        )
        expect(await storage.submission(failed_id, context)).to_equal(unanswered)
        expect(
            await storage.submission_by_request(root_id, "passive-failed", context)
        ).to_equal(unanswered)

    async def reconstructs_rewindable_documents(storage: Storage) -> None:
        root_id = await _create_root(storage)
        first_id = await storage.mint_id()
        first_record = {
            "id": first_id,
            "kind": "conversation.notes",
            "scope": {"kind": "conversation", "conversationId": root_id},
            "history": "rewindable",
            "fork": "asOf",
        }
        initial: dict[str, Any] = {"items": ["a"], "nested": {"count": 1}}
        created_at = await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": first_record,
                    "content": {"kind": "base", "version": 1, "value": initial},
                }
            ],
            context,
        )
        appended = ["b"]
        ops = [
            ["p", ["items"], 1, 0, appended],
            ["s", ["nested", "count"], 2],
        ]
        changed_at = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": first_id,
                    "content": {"kind": "delta", "version": 1, "ops": ops},
                }
            ],
            context,
        )

        initial["items"].append("caller mutation")
        appended.append("caller mutation")
        expect(await storage.document(first_id, created_at, context)).to_match_object(
            {"version": 1, "value": {"items": ["a"], "nested": {"count": 1}}, "deltasSinceBase": 0}
        )
        changed = await storage.document(first_id, changed_at, context)
        assert changed is not None
        expect(changed["value"]).to_equal({"items": ["a", "b"], "nested": {"count": 2}})
        expect(changed["deltasSinceBase"]).to_be(1)
        changed["value"]["items"].append("read mutation")
        current = await storage.document(first_id, "current", context)
        assert current is not None
        expect(current["value"]).to_equal({"items": ["a", "b"], "nested": {"count": 2}})

        checkpoint_at = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": first_id,
                    "content": {
                        "kind": "base",
                        "version": 2,
                        "value": {"items": ["checkpoint"], "nested": {"count": 3}},
                    },
                }
            ],
            context,
        )
        replaced_at = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": first_id,
                    "content": {
                        "kind": "delta",
                        "version": 2,
                        "ops": [["r", {"items": ["replacement"], "nested": {"count": 4}}]],
                    },
                }
            ],
            context,
        )
        expect(await storage.document(first_id, changed_at, context)).to_match_object(
            {"version": 1, "value": {"items": ["a", "b"], "nested": {"count": 2}}}
        )
        expect(await storage.document(first_id, checkpoint_at, context)).to_match_object(
            {"version": 2, "value": {"items": ["checkpoint"], "nested": {"count": 3}}, "deltasSinceBase": 0}
        )
        expect(await storage.document(first_id, replaced_at, context)).to_match_object(
            {"value": {"items": ["replacement"], "nested": {"count": 4}}, "deltasSinceBase": 1}
        )
        current_after = await storage.document(first_id, "current", context)
        assert current_after is not None
        expect(current_after["deltasSinceBase"]).to_be(1)

        second_id = await storage.mint_id()
        retired_at = await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": {**first_record, "id": second_id},
                    "content": {"kind": "base", "version": 1, "value": {"items": ["new"]}},
                },
                {"type": "document.retire", "id": first_id},
                {
                    "type": "document.change",
                    "id": first_id,
                    "content": {"kind": "delta", "version": 2, "ops": [["s", ["retiring"], True]]},
                },
            ],
            context,
        )
        address = {"kind": first_record["kind"], "scope": first_record["scope"]}
        historical = await storage.find_document(address, changed_at, context)
        assert historical is not None
        expect(historical["id"]).to_be(first_id)
        expect(await storage.find_document(address, retired_at, context)).to_match_object(
            {"id": second_id, "createdAt": retired_at}
        )
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_documents(
                        {"scope": first_record["scope"], "at": changed_at}, 10, None, context
                    )
                )["items"]
            ]
        ).to_equal([first_id])
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_documents(
                        {"scope": first_record["scope"], "at": retired_at}, 10, None, context
                    )
                )["items"]
            ]
        ).to_equal([second_id])
        expect(await storage.document(first_id, retired_at, context)).to_be_undefined()
        second_current = await storage.document(second_id, "current", context)
        assert second_current is not None
        expect(second_current["value"]).to_equal({"items": ["new"]})

    async def streams_long_document_tails(storage: Storage) -> None:
        root_id = await _create_root(storage)
        id = await storage.mint_id()
        record = {
            "id": id,
            "kind": "conversation.long-tail",
            "scope": {"kind": "conversation", "conversationId": root_id},
            "history": "rewindable",
            "fork": "asOf",
        }
        initial = {
            "revision": 0,
            "rows": [{"value": value, "stable": f"row-{value}"} for value in range(512)],
        }
        created_at = await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": record,
                    "content": {"kind": "base", "version": 1, "value": initial},
                }
            ],
            context,
        )
        before_replacement = json.loads(json.dumps(initial))
        before_replacement_at = created_at
        for revision in range(1, 25):
            index = (revision * 17) % len(before_replacement["rows"])
            before_replacement["rows"][index]["value"] = -revision
            before_replacement["revision"] = revision
            before_replacement_at = await storage.commit(
                [
                    {
                        "type": "document.change",
                        "id": id,
                        "content": {
                            "kind": "delta",
                            "version": 1,
                            "ops": [
                                ["s", ["rows", index, "value"], -revision],
                                ["s", ["revision"], revision],
                            ],
                        },
                    }
                ],
                context,
            )

        replacement = {
            "revision": 100,
            "rows": [
                {"value": 10_000 + value, "stable": f"new-{value}"} for value in range(512)
            ],
        }
        replacement_snapshot = json.loads(json.dumps(replacement))
        replacement_at = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "delta", "version": 1, "ops": [["r", replacement]]},
                }
            ],
            context,
        )
        replacement["rows"][0]["value"] = -999

        current = json.loads(json.dumps(replacement_snapshot))
        for revision in range(101, 125):
            index = (revision * 19) % len(current["rows"])
            current["rows"][index]["value"] = -revision
            current["revision"] = revision
            await storage.commit(
                [
                    {
                        "type": "document.change",
                        "id": id,
                        "content": {
                            "kind": "delta",
                            "version": 1,
                            "ops": [
                                ["s", ["rows", index, "value"], -revision],
                                ["s", ["revision"], revision],
                            ],
                        },
                    }
                ],
                context,
            )

        created = await storage.document(id, created_at, context)
        assert created is not None
        expect(created["value"]).to_equal(initial)
        before = await storage.document(id, before_replacement_at, context)
        assert before is not None
        expect(before["value"]).to_equal(before_replacement)
        replaced = await storage.document(id, replacement_at, context)
        assert replaced is not None
        expect(replaced["value"]).to_equal(replacement_snapshot)
        read = await storage.document(id, "current", context)
        assert read is not None
        expect(read["value"]).to_equal(current)
        read["value"]["rows"][0]["value"] = -1_000
        current_again = await storage.document(id, "current", context)
        assert current_again is not None
        expect(current_again["value"]).to_equal(current)

    async def copies_document_bases(storage: Storage) -> None:
        root_id = await _create_root(storage)
        child_id = await storage.mint_id()
        second_child_id = await storage.mint_id()
        await storage.commit(
            [
                {"type": "conversation", "value": {"id": child_id}},
                {"type": "conversation", "value": {"id": second_child_id}},
            ],
            context,
        )
        source_id = await storage.mint_id()
        source_record = {
            "id": source_id,
            "kind": "copy.source",
            "scope": {"kind": "conversation", "conversationId": root_id},
            "history": "rewindable",
            "fork": "asOf",
        }
        created_at = await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": source_record,
                    "content": {
                        "kind": "base",
                        "version": 2,
                        "value": {"count": 1, "rows": [{"value": "base"}]},
                    },
                }
            ],
            context,
        )
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": source_id,
                    "content": {
                        "kind": "delta",
                        "version": 2,
                        "ops": [
                            ["s", ["count"], 2],
                            ["p", ["rows"], 1, 0, [{"value": "current"}]],
                        ],
                    },
                }
            ],
            context,
        )
        historical_copy_id = await storage.mint_id()
        current_copy_id = await storage.mint_id()
        retired_copy_id = await storage.mint_id()

        def child_record(id: int, conversation_id: int) -> dict[str, Any]:
            return {
                "id": id,
                "kind": source_record["kind"],
                "scope": {"kind": "conversation", "conversationId": conversation_id},
                "history": "rewindable",
                "fork": "asOf",
            }

        await storage.commit(
            [
                {
                    "type": "document.copy",
                    "record": child_record(historical_copy_id, child_id),
                    "source": {"id": source_id, "at": created_at},
                },
                {
                    "type": "document.copy",
                    "record": child_record(current_copy_id, second_child_id),
                    "source": {"id": source_id, "at": "current"},
                },
                {
                    "type": "document.copy",
                    "record": child_record(retired_copy_id, root_id),
                    "source": {"id": source_id, "at": "current"},
                },
                {"type": "document.retire", "id": retired_copy_id},
            ],
            context,
        )
        expect(await storage.document(historical_copy_id, "current", context)).to_match_object(
            {"version": 2, "value": {"count": 1, "rows": [{"value": "base"}]}}
        )
        expect(await storage.document(current_copy_id, "current", context)).to_match_object(
            {"version": 2, "value": {"count": 2, "rows": [{"value": "base"}, {"value": "current"}]}}
        )
        expect(await storage.document(retired_copy_id, "current", context)).to_be_undefined()

        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": source_id,
                    "content": {"kind": "base", "version": 2, "value": {"count": 99, "rows": []}},
                },
                {"type": "document.retire", "id": source_id},
            ],
            context,
        )
        surviving_copy = await storage.document(current_copy_id, "current", context)
        assert surviving_copy is not None
        expect(surviving_copy["value"]).to_equal(
            {"count": 2, "rows": [{"value": "base"}, {"value": "current"}]}
        )

        latest_source_id = await storage.mint_id()
        latest_copy_id = await storage.mint_id()
        latest_source = {
            "id": latest_source_id,
            "kind": "copy.latest",
            "scope": {"kind": "conversation", "conversationId": root_id},
            "history": "latest",
            "fork": "current",
        }
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": latest_source,
                    "content": {"kind": "base", "version": 4, "value": {"retained": "copy"}},
                }
            ],
            context,
        )
        await storage.commit(
            [
                {
                    "type": "document.copy",
                    "record": {
                        **latest_source,
                        "id": latest_copy_id,
                        "scope": {"kind": "conversation", "conversationId": child_id},
                    },
                    "source": {"id": latest_source_id, "at": "current"},
                }
            ],
            context,
        )
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": latest_source_id,
                    "content": {"kind": "base", "version": 4, "value": {"retained": "source-only"}},
                },
                {"type": "document.retire", "id": latest_source_id},
            ],
            context,
        )
        expect(await storage.document(latest_copy_id, "current", context)).to_match_object(
            {"version": 4, "value": {"retained": "copy"}}
        )

        conflict_id = await storage.mint_id()
        conflict_error: object | None = None
        try:
            await storage.commit(
                [
                    {
                        "type": "document.copy",
                        "record": child_record(conflict_id, child_id),
                        "source": {"id": current_copy_id, "at": "current"},
                    },
                    {"type": "document.retire", "id": current_copy_id},
                ],
                context,
            )
        except Exception as error:
            conflict_error = error
        # Rejected without effect: the copy and the retirement in the same batch are both absent.
        expect(isinstance(conflict_error, Exception)).to_be(True)
        expect(await storage.document(conflict_id, "current", context)).to_be_undefined()
        current_copy = await storage.document(current_copy_id, "current", context)
        assert current_copy is not None
        expect(current_copy["value"]).to_equal(
            {"count": 2, "rows": [{"value": "base"}, {"value": "current"}]}
        )

        mismatch_id = await storage.mint_id()
        mismatch_error: object | None = None
        try:
            await storage.commit(
                [
                    {
                        "type": "document.copy",
                        "record": {**child_record(mismatch_id, child_id), "kind": "copy.mismatch"},
                        "source": {"id": current_copy_id, "at": "current"},
                    }
                ],
                context,
            )
        except Exception as error:
            mismatch_error = error
        expect(isinstance(mismatch_error, Exception)).to_be(True)
        expect(await storage.document(mismatch_id, "current", context)).to_be_undefined()

    async def uses_bases_for_version_transitions(storage: Storage) -> None:
        await _create_root(storage)
        id = await storage.mint_id()
        record = {"id": id, "kind": "session.settings", "scope": {"kind": "session"}}
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": record,
                    "content": {"kind": "base", "version": 1, "value": {"count": 1}},
                }
            ],
            context,
        )
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 2]]},
                }
            ],
            context,
        )
        migrated_at = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "base", "version": 2, "value": {"count": 3}},
                }
            ],
            context,
        )
        expect(await storage.document(id, "current", context)).to_match_object(
            {"version": 2, "value": {"count": 3}}
        )
        await expect(storage.document(id, migrated_at, context)).rejects.to_throw(
            "does not retain historical content"
        )

        await expect(
            storage.commit(
                [
                    {
                        "type": "document.change",
                        "id": id,
                        "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 4]]},
                    }
                ],
                context,
            )
        ).rejects.to_throw("version transition requires a base")
        unchanged = await storage.document(id, "current", context)
        assert unchanged is not None
        expect(unchanged["value"]).to_equal({"count": 3})
        await storage.commit([{"type": "document.retire", "id": id}], context)
        expect(await storage.document(id, "current", context)).to_be_undefined()

    async def indexes_logical_addresses(storage: Storage) -> None:
        root_id = await _create_root(storage)
        first_id = await storage.mint_id()
        second_id = await storage.mint_id()
        conversation_id = await storage.mint_id()
        task_id = await storage.mint_id()
        task_singleton_id = await storage.mint_id()
        task_family_id = await storage.mint_id()
        task_other_kind_id = await storage.mint_id()
        created_at = await storage.commit(
            [
                {"type": "task", "value": _pending_task(task_id, root_id)},
                {
                    "type": "document.create",
                    "record": {
                        "id": first_id,
                        "kind": "cache",
                        "scope": {"kind": "session"},
                        "key": "__proto__",
                    },
                    "content": {"kind": "base", "version": 1, "value": {"owner": "first"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": second_id,
                        "kind": "cache",
                        "scope": {"kind": "session"},
                        "key": "constructor",
                    },
                    "content": {"kind": "base", "version": 1, "value": {"owner": "second"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": conversation_id,
                        "kind": "cache",
                        "scope": {"kind": "conversation", "conversationId": root_id},
                        "history": "latest",
                        "fork": "current",
                        "key": "__proto__",
                    },
                    "content": {"kind": "base", "version": 1, "value": {"owner": "conversation"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": task_singleton_id,
                        "kind": "task.cache",
                        "scope": {"kind": "task", "taskId": task_id},
                    },
                    "content": {"kind": "base", "version": 1, "value": {"owner": "singleton"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": task_family_id,
                        "kind": "task.cache",
                        "scope": {"kind": "task", "taskId": task_id},
                        "key": "member",
                    },
                    "content": {"kind": "base", "version": 1, "value": {"owner": "family"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": task_other_kind_id,
                        "kind": "task.other",
                        "scope": {"kind": "task", "taskId": task_id},
                    },
                    "content": {"kind": "base", "version": 1, "value": {"owner": "other"}},
                },
            ],
            context,
        )

        first_address = await storage.find_document(
            {"kind": "cache", "scope": {"kind": "session"}, "key": "__proto__"}, "current", context
        )
        assert first_address is not None
        expect(first_address["id"]).to_be(first_id)
        first_scan = await storage.scan_documents(
            {"scope": {"kind": "session"}, "at": "current"}, 1, None, context
        )
        expect(first_scan["items"]).to_have_length(1)
        second_scan = await storage.scan_documents(
            {"scope": {"kind": "session"}, "at": "current"}, 1, first_scan.get("next"), context
        )
        expect([item["id"] for item in [*first_scan["items"], *second_scan["items"]]]).to_equal(
            [first_id, second_id]
        )
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_documents(
                        {"scope": {"kind": "conversation", "conversationId": root_id}, "at": "current"},
                        10,
                        None,
                        context,
                    )
                )["items"]
            ]
        ).to_equal([conversation_id])
        singleton = await storage.find_document(
            {"kind": "task.cache", "scope": {"kind": "task", "taskId": task_id}}, "current", context
        )
        assert singleton is not None
        expect(singleton["id"]).to_be(task_singleton_id)
        family = await storage.find_document(
            {"kind": "task.cache", "scope": {"kind": "task", "taskId": task_id}, "key": "member"},
            "current",
            context,
        )
        assert family is not None
        expect(family["id"]).to_be(task_family_id)
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_documents(
                        {"scope": {"kind": "task", "taskId": task_id}, "at": "current", "kind": "task.cache"},
                        10,
                        None,
                        context,
                    )
                )["items"]
            ]
        ).to_equal([task_singleton_id, task_family_id])
        await expect(storage.document(task_singleton_id, created_at, context)).rejects.to_throw(
            "does not retain historical content"
        )

    async def keeps_lifecycle_failures_atomic(storage: Storage) -> None:
        root_id = await _create_root(storage)
        first_id = await storage.mint_id()
        second_id = await storage.mint_id()
        record = {"id": first_id, "kind": "singleton", "scope": {"kind": "session"}}
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": record,
                    "content": {"kind": "base", "version": 1, "value": {"value": 1}},
                }
            ],
            context,
        )
        await expect(
            storage.commit(
                [
                    {
                        "type": "document.create",
                        "record": {**record, "id": second_id},
                        "content": {"kind": "base", "version": 1, "value": {"value": 2}},
                    },
                    {
                        "type": "document.change",
                        "id": first_id,
                        "content": {"kind": "delta", "version": 1, "ops": []},
                    },
                ],
                context,
            )
        ).rejects.to_throw("already has a current incarnation")
        first_current = await storage.document(first_id, "current", context)
        assert first_current is not None
        expect(first_current["value"]).to_equal({"value": 1})
        expect(await storage.document(second_id, "current", context)).to_be_undefined()

        empty_id = await storage.mint_id()
        empty_at = await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": {
                        "id": empty_id,
                        "kind": record["kind"],
                        "key": "empty",
                        "scope": {"kind": "conversation", "conversationId": root_id},
                        "history": "rewindable",
                        "fork": "initial",
                    },
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
                {"type": "document.retire", "id": empty_id},
            ],
            context,
        )
        expect(await storage.document(empty_id, "current", context)).to_be_undefined()
        expect(await storage.document(empty_id, empty_at, context)).to_be_undefined()
        expect(
            await storage.find_document(
                {
                    "kind": record["kind"],
                    "scope": {"kind": "conversation", "conversationId": root_id},
                    "key": "empty",
                },
                empty_at,
                context,
            )
        ).to_be_undefined()

    async def rolls_back_document_failures(storage: Storage) -> None:
        root_id = await _create_root(storage)
        task_id = await storage.mint_id()
        submission_id = await storage.mint_id()
        document_id = await storage.mint_id()
        task = _pending_task(task_id, root_id)
        submission = {
            "id": submission_id,
            "conversationId": root_id,
            "requestId": "atomic",
            "type": "input",
            "status": "queued",
        }
        record = {"id": document_id, "kind": "atomic", "scope": {"kind": "session"}}
        baseline_seq = await storage.commit(
            [
                {"type": "task", "value": task},
                {"type": "submission", "value": submission},
                {
                    "type": "document.create",
                    "record": record,
                    "content": {"kind": "base", "version": 1, "value": {"count": 1}},
                },
            ],
            context,
        )

        entry_id = await storage.mint_id()
        conflicting_document_id = await storage.mint_id()
        await expect(
            storage.commit(
                [
                    {
                        "type": "task",
                        "value": {**task, "state": {"status": "running", "checkpoint": {"phase": "effect"}}},
                    },
                    {
                        "type": "submission",
                        "value": {**submission, "status": "unanswered", "reason": "failed"},
                    },
                    {"type": "entry", "value": _entry(entry_id, root_id, "transient")},
                    {
                        "type": "document.create",
                        "record": {**record, "id": conflicting_document_id},
                        "content": {"kind": "base", "version": 1, "value": {"count": 2}},
                    },
                ],
                context,
            )
        ).rejects.to_throw("already has a current incarnation")

        expect(await storage.task(task_id, context)).to_equal(task)
        expect((await storage.scan_tasks({"status": "pending"}, 10, None, context))["items"]).to_equal(
            [task]
        )
        expect(await storage.submission_by_request(root_id, "atomic", context)).to_equal(
            submission
        )
        expect(await storage.entry(entry_id, context)).to_be_undefined()
        expect(
            await storage.document(conflicting_document_id, "current", context)
        ).to_be_undefined()
        address = await storage.find_document(
            {"kind": record["kind"], "scope": record["scope"]}, "current", context
        )
        assert address is not None
        expect(address["id"]).to_be(document_id)
        after_rollback_seq = await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": document_id,
                    "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 3]]},
                }
            ],
            context,
        )
        expect(after_rollback_seq).to_be_greater_than(baseline_seq)

    async def keeps_string_identities_lossless(storage: Storage) -> None:
        root_id = await _create_root(storage)
        first = "\ud800"
        second = "\ud801"
        first_task_id = await storage.mint_id()
        second_task_id = await storage.mint_id()
        first_submission_id = await storage.mint_id()
        second_submission_id = await storage.mint_id()
        first_kind_document_id = await storage.mint_id()
        second_kind_document_id = await storage.mint_id()
        first_key_document_id = await storage.mint_id()
        second_key_document_id = await storage.mint_id()
        await storage.commit(
            [
                {"type": "task", "value": {**_pending_task(first_task_id, root_id), "kind": first}},
                {"type": "task", "value": {**_pending_task(second_task_id, root_id), "kind": second}},
                {
                    "type": "submission",
                    "value": {
                        "id": first_submission_id,
                        "conversationId": root_id,
                        "requestId": first,
                        "type": "input",
                        "status": "queued",
                    },
                },
                {
                    "type": "submission",
                    "value": {
                        "id": second_submission_id,
                        "conversationId": root_id,
                        "requestId": second,
                        "type": "input",
                        "status": "queued",
                    },
                },
                {
                    "type": "document.create",
                    "record": {"id": first_kind_document_id, "kind": first, "scope": {"kind": "session"}},
                    "content": {"kind": "base", "version": 1, "value": {"identity": "first kind"}},
                },
                {
                    "type": "document.create",
                    "record": {"id": second_kind_document_id, "kind": second, "scope": {"kind": "session"}},
                    "content": {"kind": "base", "version": 1, "value": {"identity": "second kind"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": first_key_document_id,
                        "kind": "family",
                        "key": first,
                        "scope": {"kind": "session"},
                    },
                    "content": {"kind": "base", "version": 1, "value": {"identity": "first key"}},
                },
                {
                    "type": "document.create",
                    "record": {
                        "id": second_key_document_id,
                        "kind": "family",
                        "key": second,
                        "scope": {"kind": "session"},
                    },
                    "content": {"kind": "base", "version": 1, "value": {"identity": "second key"}},
                },
            ],
            context,
        )

        expect(
            [item["id"] for item in (await storage.scan_tasks({"kind": first}, 10, None, context))["items"]]
        ).to_equal([first_task_id])
        expect(
            [item["id"] for item in (await storage.scan_tasks({"kind": second}, 10, None, context))["items"]]
        ).to_equal([second_task_id])
        first_task = await storage.task(first_task_id, context)
        assert first_task is not None
        expect(first_task["kind"]).to_be(first)
        second_task = await storage.task(second_task_id, context)
        assert second_task is not None
        expect(second_task["kind"]).to_be(second)
        first_request = await storage.submission_by_request(root_id, first, context)
        assert first_request is not None
        expect(first_request["requestId"]).to_be(first)
        expect(first_request["id"]).to_be(first_submission_id)
        second_request = await storage.submission_by_request(root_id, second, context)
        assert second_request is not None
        expect(second_request["id"]).to_be(second_submission_id)
        first_kind = await storage.find_document(
            {"kind": first, "scope": {"kind": "session"}}, "current", context
        )
        assert first_kind is not None
        expect(first_kind["id"]).to_be(first_kind_document_id)
        second_kind = await storage.find_document(
            {"kind": second, "scope": {"kind": "session"}}, "current", context
        )
        assert second_kind is not None
        expect(second_kind["id"]).to_be(second_kind_document_id)
        first_key = await storage.find_document(
            {"kind": "family", "key": first, "scope": {"kind": "session"}}, "current", context
        )
        assert first_key is not None
        expect(first_key["id"]).to_be(first_key_document_id)
        second_key = await storage.find_document(
            {"kind": "family", "key": second, "scope": {"kind": "session"}}, "current", context
        )
        assert second_key is not None
        expect(second_key["id"]).to_be(second_key_document_id)
        expect(
            [
                item["id"]
                for item in (
                    await storage.scan_documents(
                        {"scope": {"kind": "session"}, "at": "current", "kind": first}, 10, None, context
                    )
                )["items"]
            ]
        ).to_equal([first_kind_document_id])

    async def keeps_one_global_id_namespace(storage: Storage) -> None:
        root_id = await _create_root(storage)
        explicit_entry_id = 100
        await storage.commit(
            [{"type": "entry", "value": _entry(explicit_entry_id, root_id)}], context
        )
        expect(await storage.mint_id()).to_be(101)
        await expect(
            storage.commit(
                [
                    {
                        "type": "task",
                        "value": _pending_task(explicit_entry_id, root_id),
                    }
                ],
                context,
            )
        ).rejects.to_throw(f"ID {explicit_entry_id} already belongs to entry")

        await storage.commit(
            [{"type": "entry", "value": _entry(_MAX_SAFE_INTEGER, root_id, "last-id")}], context
        )
        await expect(storage.mint_id()).rejects.to_throw("ID space is exhausted")
        await expect(storage.mint_id()).rejects.to_throw("ID space is exhausted")

    async def rejects_every_operation_after_close(storage: Storage) -> None:
        await _create_root(storage)
        await storage.close(context)
        await expect(storage.conversation(ROOT_CONVERSATION_ID, context)).rejects.to_throw("closed")
        await expect(storage.commit(cast(Sequence[StorageWrite], []), context)).rejects.to_throw(
            "closed"
        )
        await expect(storage.mint_id()).rejects.to_throw("closed")

    return [
        _create_case(options, "reserves ID 1 for the immutable root conversation", reserves_id_one),
        _create_case(
            options,
            "commits mixed table writes atomically and rolls all of them back on failure",
            commits_mixed_table_writes,
        ),
        _create_case(
            options, "detaches retained writes and every returned record", detaches_retained_writes
        ),
        _create_case(
            options,
            "detaches prototype-like JSON keys without changing object prototypes",
            detaches_prototype_like_keys,
        ),
        _create_case(options, "indexes entries committed out of ID order", indexes_entries_out_of_order),
        _create_case(
            options,
            "continues an entry cursor below its last item after a newer commit",
            continues_entry_cursor,
        ),
        _create_case(
            options,
            "paginates conversations by opaque cursor in ascending ID order",
            paginates_conversations,
        ),
        _create_case(
            options, "filters and pages conversations by durable owner edges", filters_conversations_by_owner
        ),
        _create_case(
            options,
            "scans deep fork history newest-first through every ancestor cap",
            scans_deep_fork_history,
        ),
        _create_case(
            options,
            "scans tables in either ID order and continues a cursor in its order",
            scans_tables_in_either_order,
        ),
        _create_case(
            options, "replaces complete task records and pages filtered task scans", replaces_task_records
        ),
        _create_case(
            options,
            "stores owners and scans waiting and completing tasks by status",
            stores_owners_and_scans_statuses,
        ),
        _create_case(
            options,
            "stores and scans sleeping tasks by status",
            stores_and_scans_sleeping_tasks_by_status,
        ),
        _create_case(
            options,
            "indexes request IDs per conversation and replaces complete submission records",
            indexes_request_ids,
        ),
        _create_case(
            options,
            "stores passive write submissions without input-only lifecycle states",
            stores_passive_write_submissions,
        ),
        _create_case(
            options,
            "reconstructs rewindable documents and preserves half-open incarnations",
            reconstructs_rewindable_documents,
        ),
        _create_case(
            options, "streams long document tails across root replacement deltas", streams_long_document_tails
        ),
        _create_case(
            options,
            "copies stored document bases independently and rejects ambiguous sources",
            copies_document_bases,
        ),
        _create_case(
            options,
            "uses bases for version transitions and rejects historical reads of current-only documents",
            uses_bases_for_version_transitions,
        ),
        _create_case(
            options,
            "indexes logical addresses and exact-scope scans independently",
            indexes_logical_addresses,
        ),
        _create_case(
            options,
            "keeps document lifecycle failures atomic and gives create-plus-retire an empty lifetime",
            keeps_lifecycle_failures_atomic,
        ),
        _create_case(
            options,
            "rolls back record tables and secondary indexes when a document command fails",
            rolls_back_document_failures,
        ),
        _create_case(
            options, "keeps indexed string identities lossless", keeps_string_identities_lossless
        ),
        _create_case(
            options,
            "keeps one global record ID namespace and rejects exhausted ID minting",
            keeps_one_global_id_namespace,
        ),
        _create_case(options, "rejects every operation after close", rejects_every_operation_after_close),
    ]


__all__ = ["create_storage_conformance"]

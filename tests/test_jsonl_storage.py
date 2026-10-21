"""Translated v1.3.0 ``jsonl-storage`` suite.

Covers both conformance runs (fresh and across reopen) plus publication ordering, poisoning,
reclamation, torn-tail truncation, and corruption detection.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from collections.abc import Awaitable, Callable, Generator, Sequence
from pathlib import Path
from typing import Any

import pytest
from storage_support import expect

from pi_durable.context import BACKGROUND_CONTEXT, Context
from pi_durable.env import Err, FileError, Result, err
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.ids import ROOT_CONVERSATION_ID, Seq
from pi_durable.storage.jsonl import (
    JsonlCorruptionError,
    JsonlStorage,
    JsonlStorageOptions,
    JsonlStoragePoisonedError,
)
from pi_durable.testing import (
    StorageConformanceCase,
    StorageConformanceOptions,
    create_expect_assertions,
    create_storage_conformance,
)
from pi_durable.types import (
    Storage,
    StorageWrite,
)

context: Context = BACKGROUND_CONTEXT

_open_storages: list[JsonlStorage] = []
_temp_directories: list[Path] = []


@pytest.fixture(autouse=True)
async def _cleanup() -> Generator[None, None, None]:
    yield
    while _open_storages:
        await _open_storages.pop().close(context)
    while _temp_directories:
        shutil.rmtree(_temp_directories.pop(), ignore_errors=True)


def _temp_directory(prefix: str = "pi-durable-jsonl-") -> Path:
    directory = Path(tempfile.mkdtemp(prefix=prefix))
    _temp_directories.append(directory)
    return directory


async def _open_storage(
    directory: Path,
    fs: Any = None,
    options: JsonlStorageOptions | None = None,
) -> JsonlStorage:
    storage = await JsonlStorage.open(
        str(directory),
        PosixExecutionEnv(cwd=str(directory)) if fs is None else fs,
        context,
        options,
    )
    _open_storages.append(storage)
    return storage


async def _create_storage() -> JsonlStorage:
    return await _open_storage(_temp_directory())


class ReopeningStorage:
    """A storage that closes and reopens the JSONL directory after every commit."""

    def __init__(self, current: JsonlStorage, directory: str) -> None:
        self._current = current
        self._directory = directory
        self._closed = False

    async def commit(self, writes: Sequence[StorageWrite], commit_context: Context) -> Seq:
        if self._closed:
            return await self._current.commit(writes, commit_context)
        try:
            return await self._current.commit(writes, commit_context)
        finally:
            await self._current.close(context)
            self._current = await _open_storage(Path(self._directory), options=None)

    async def mint_id(self) -> int:
        return await self._current.mint_id()

    async def conversation(self, id: Any, read_context: Context) -> Any:
        return await self._current.conversation(id, read_context)

    async def scan_conversations(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_conversations(query, limit, cursor, read_context)

    async def entry(self, *args: Any) -> Any:
        return await self._current.entry(*args)

    async def find_latest_head_marker(
        self, conversation_id: Any, at: Any, read_context: Context
    ) -> Any:
        return await self._current.find_latest_head_marker(conversation_id, at, read_context)

    async def scan_entries(self, query: Any, limit: int, cursor: Any, read_context: Context) -> Any:
        return await self._current.scan_entries(query, limit, cursor, read_context)

    async def task(self, id: Any, read_context: Context) -> Any:
        return await self._current.task(id, read_context)

    async def scan_tasks(self, query: Any, limit: int, cursor: Any, read_context: Context) -> Any:
        return await self._current.scan_tasks(query, limit, cursor, read_context)

    async def submission(self, id: Any, read_context: Context) -> Any:
        return await self._current.submission(id, read_context)

    async def scan_submissions(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_submissions(query, limit, cursor, read_context)

    async def submission_by_request(
        self, conversation_id: Any, request_id: str, read_context: Context
    ) -> Any:
        return await self._current.submission_by_request(conversation_id, request_id, read_context)

    async def find_document(self, address: Any, at: Any, read_context: Context) -> Any:
        return await self._current.find_document(address, at, read_context)

    async def document(self, id: Any, at: Any, read_context: Context) -> Any:
        return await self._current.document(id, at, read_context)

    async def scan_documents(
        self, query: Any, limit: int, cursor: Any, read_context: Context
    ) -> Any:
        return await self._current.scan_documents(query, limit, cursor, read_context)

    async def close(self, close_context: Context) -> None:
        if self._closed:
            return
        self._closed = True
        await self._current.close(close_context)


_assertions = create_expect_assertions(expect)


async def _with_jsonl(use: Callable[[Storage], Awaitable[None]]) -> None:
    await use(await _create_storage())


async def _with_reopening_jsonl(use: Callable[[Storage], Awaitable[None]]) -> None:
    directory = _temp_directory("pi-durable-jsonl-conformance-")
    current = await _open_storage(directory)
    storage = ReopeningStorage(current, str(directory))
    try:
        await use(storage)
    finally:
        await storage.close(context)


CASES: list[StorageConformanceCase] = create_storage_conformance(
    StorageConformanceOptions(assertions=_assertions, with_storage=_with_jsonl)
)
REOPEN_CASES: list[StorageConformanceCase] = create_storage_conformance(
    StorageConformanceOptions(assertions=_assertions, with_storage=_with_reopening_jsonl)
)


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_storage_conformance(case: StorageConformanceCase) -> None:
    await case.run()


@pytest.mark.parametrize(
    "case",
    REOPEN_CASES,
    ids=[f"{case.name} [across reopen]" for case in REOPEN_CASES],
)
async def test_storage_conformance_across_reopen(case: StorageConformanceCase) -> None:
    await case.run()


def _pending_task(id: int, phase: str = "ready") -> dict[str, Any]:
    return {
        "id": id,
        "conversationId": ROOT_CONVERSATION_ID,
        "kind": "test.task",
        "version": 1,
        "input": None,
        "state": {"status": "pending", "checkpoint": {"phase": phase}},
        "background": False,
        "abortRequested": False,
    }


def _terminal_task(id: int) -> dict[str, Any]:
    return {
        "id": id,
        "conversationId": ROOT_CONVERSATION_ID,
        "kind": "test.task",
        "version": 1,
        "input": None,
        "state": {"status": "terminal", "outcome": {"status": "completed", "result": None}},
        "background": False,
        "abortRequested": False,
    }


async def _create_root(storage: Storage) -> None:
    await storage.commit(
        [{"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}}], context
    )


def _session_document(id: int, kind: str = "test.document") -> dict[str, Any]:
    return {"id": id, "kind": kind, "scope": {"kind": "session"}}


class Failure:
    def __init__(self, operation: str, call: int, mode: str) -> None:
        self.operation = operation
        self.call = call
        self.mode = mode


class InstrumentedEnv(PosixExecutionEnv):
    def __init__(self, *, cwd: str) -> None:
        super().__init__(cwd=cwd)
        self.operations: list[str] = []
        self._failure: Failure | None = None
        self._append_calls = 0
        self._flush_calls = 0
        self._write_calls = 0
        self._rename_calls = 0
        self._remove_calls = 0

    def fail(self, failure: Failure) -> None:
        self._failure = failure
        self._reset_observations()

    def clear(self) -> None:
        self._failure = None
        self._reset_observations()

    def _reset_observations(self) -> None:
        self._append_calls = 0
        self._flush_calls = 0
        self._write_calls = 0
        self._rename_calls = 0
        self._remove_calls = 0
        self.operations.clear()

    async def append_file(
        self, path: str, content: str | bytes, append_context: Context
    ) -> Result[None, FileError]:
        self._append_calls += 1
        self.operations.append(f"append:{os.path.basename(path)}")
        failure = self._failure
        if failure is None or failure.operation != "append" or failure.call != self._append_calls:
            return await super().append_file(path, content, append_context)
        if failure.mode == "before":
            return err(FileError("unknown", "injected append failure", path))
        if failure.mode == "short":
            data = content.encode() if isinstance(content, str) else content
            partial = data[: max(1, len(data) // 2)]
            written = await super().append_file(path, partial, append_context)
            if isinstance(written, Err):
                return written
            return err(FileError("unknown", "injected short append", path))
        written = await super().append_file(path, content, append_context)
        if isinstance(written, Err):
            return written
        return err(FileError("unknown", "injected post-append failure", path))

    async def flush_file(self, path: str, flush_context: Context) -> Result[None, FileError]:
        self._flush_calls += 1
        self.operations.append(f"flush:{os.path.basename(path)}")
        failure = self._failure
        if failure is None or failure.operation != "flush" or failure.call != self._flush_calls:
            return await super().flush_file(path, flush_context)
        if failure.mode == "after":
            flushed = await super().flush_file(path, flush_context)
            if isinstance(flushed, Err):
                return flushed
        return err(FileError("unknown", "injected flush failure", path))

    async def write_file(
        self, path: str, content: str | bytes, write_context: Context
    ) -> Result[None, FileError]:
        self._write_calls += 1
        self.operations.append(f"write:{os.path.basename(path)}")
        failure = self._failure
        if failure is None or failure.operation != "write" or failure.call != self._write_calls:
            return await super().write_file(path, content, write_context)
        if failure.mode == "before":
            return err(FileError("unknown", "injected write failure", path))
        if failure.mode == "short":
            data = content.encode() if isinstance(content, str) else content
            partial = data[: max(1, len(data) // 2)]
            written = await super().write_file(path, partial, write_context)
            if isinstance(written, Err):
                return written
            return err(FileError("unknown", "injected short write", path))
        written = await super().write_file(path, content, write_context)
        if isinstance(written, Err):
            return written
        return err(FileError("unknown", "injected post-write failure", path))

    async def rename_file(
        self, source_path: str, destination_path: str, rename_context: Context
    ) -> Result[None, FileError]:
        self._rename_calls += 1
        self.operations.append(
            f"rename:{os.path.basename(source_path)}->{os.path.basename(destination_path)}"
        )
        failure = self._failure
        if failure is None or failure.operation != "rename" or failure.call != self._rename_calls:
            return await super().rename_file(source_path, destination_path, rename_context)
        if failure.mode == "after":
            renamed = await super().rename_file(source_path, destination_path, rename_context)
            if isinstance(renamed, Err):
                return renamed
        return err(FileError("unknown", "injected rename failure", source_path))

    async def remove(
        self, path: str, options: dict[str, Any] | None, remove_context: Context
    ) -> Result[None, FileError]:
        self._remove_calls += 1
        self.operations.append(f"remove:{os.path.basename(path)}")
        failure = self._failure
        if failure is None or failure.operation != "remove" or failure.call != self._remove_calls:
            return await super().remove(path, options, remove_context)
        if failure.mode == "after":
            removed = await super().remove(path, options, remove_context)
            if isinstance(removed, Err):
                return removed
        return err(FileError("unknown", "injected remove failure", path))


def _read_lines(path: Path) -> list[str]:
    text = path.read_text()
    return [] if text == "" else text.rstrip().split("\n")


def _file_exists(path: Path) -> bool:
    return path.exists()


async def test_opens_through_the_posix_filesystem() -> None:
    directory = _temp_directory()
    storage = await JsonlStorage.open(
        str(directory), PosixExecutionEnv(cwd=str(directory)), context
    )
    _open_storages.append(storage)
    await _create_root(storage)
    assert await storage.conversation(ROOT_CONVERSATION_ID, context) == {
        "id": ROOT_CONVERSATION_ID
    }


async def test_publishes_every_commit_before_reclaiming_current_only_and_terminal_sidecars() -> (
    None
):
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    task_id = await storage.mint_id()
    document_id = await storage.mint_id()
    await storage.commit([{"type": "task", "value": _pending_task(task_id)}], context)
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(document_id),
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": document_id,
                "content": {"kind": "delta", "version": 1, "ops": []},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": document_id,
                "content": {"kind": "base", "version": 1, "value": {"count": 1}},
            }
        ],
        context,
    )
    assert len(_read_lines(directory / f"task-{task_id}.jsonl")) == 1
    assert len(_read_lines(directory / f"doc-{document_id}.jsonl")) == 1

    await storage.commit([{"type": "document.retire", "id": document_id}], context)
    await storage.commit([{"type": "task", "value": _terminal_task(task_id)}], context)

    assert len(_read_lines(directory / "main.jsonl")) == 7
    assert not _file_exists(directory / f"task-{task_id}.jsonl")
    assert not _file_exists(directory / f"doc-{document_id}.jsonl")
    marker_types = [
        json.loads(line)["type"] for line in _read_lines(directory / "main.jsonl")
    ]
    assert marker_types == ["commit"] * 7


async def test_orders_multiple_live_task_replacements_in_one_sidecar_by_ordinal() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    task_id = await storage.mint_id()
    await storage.commit(
        [
            {"type": "task", "value": _pending_task(task_id, "first")},
            {"type": "task", "value": _pending_task(task_id, "second")},
        ],
        context,
    )
    assert len(_read_lines(directory / f"task-{task_id}.jsonl")) == 2

    reopened = await _open_storage(directory)
    assert await reopened.task(task_id, context) == _pending_task(task_id, "second")


async def test_removes_a_complete_plus_torn_unconfirmed_multi_record_append() -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env)
    await _create_root(storage)
    task_id = await storage.mint_id()
    env.fail(Failure("append", 1, "short"))
    with pytest.raises(JsonlStoragePoisonedError, match="poisoned"):
        await storage.commit(
            [
                {"type": "task", "value": _pending_task(task_id, "first")},
                {"type": "task", "value": _pending_task(task_id, "second-" + "x" * 512)},
            ],
            context,
        )
    partial = (directory / f"task-{task_id}.jsonl").read_bytes()
    assert partial[-1] != 0x0A
    assert len([byte for byte in partial if byte == 0x0A]) == 1

    reopened = await _open_storage(directory)
    assert await reopened.task(task_id, context) is None
    assert (directory / f"task-{task_id}.jsonl").stat().st_size == 0


async def test_serializes_the_candidate_before_io_and_leaves_preparation_failures_usable() -> (
    None
):
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env)
    await _create_root(storage)
    env.clear()
    id = await storage.mint_id()
    with pytest.raises(TypeError):
        await storage.commit(
            [
                {
                    "type": "entry",
                    "value": {
                        "id": id,
                        "conversationId": ROOT_CONVERSATION_ID,
                        "kind": "bad",
                        "data": object(),
                    },
                }
            ],
            context,
        )
    assert env.operations == []
    assert await storage.entry(id, context) is None
    assert (
        await storage.commit(
            [
                {
                    "type": "entry",
                    "value": {"id": id, "conversationId": ROOT_CONVERSATION_ID, "kind": "good"},
                }
            ],
            context,
        )
        == 2
    )


APPEND_FAILURES = [
    Failure("append", 1, "before"),
    Failure("append", 1, "after"),
    Failure("append", 1, "short"),
    Failure("append", 2, "before"),
    Failure("append", 2, "after"),
    Failure("append", 3, "before"),
    Failure("append", 3, "short"),
    Failure("append", 3, "after"),
]


@pytest.mark.parametrize(
    "failure",
    APPEND_FAILURES,
    ids=[f"{f.mode} failure at append {f.call}" for f in APPEND_FAILURES],
)
async def test_poisons_after_an_append_failure_and_recovers_only_confirmed_state(
    failure: Failure,
) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env)
    await _create_root(storage)
    first_id = await storage.mint_id()
    second_id = await storage.mint_id()
    env.fail(failure)
    with pytest.raises(JsonlStoragePoisonedError, match="poisoned"):
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": _session_document(first_id, "first"),
                    "content": {"kind": "base", "version": 1, "value": {"text": "α"}},
                },
                {
                    "type": "document.create",
                    "record": _session_document(second_id, "second"),
                    "content": {"kind": "base", "version": 1, "value": {"text": "β"}},
                },
            ],
            context,
        )
    with pytest.raises(JsonlStoragePoisonedError, match="poisoned"):
        await storage.document(first_id, "current", context)

    reopened = await _open_storage(directory)
    marker_survived = failure.call == 3 and failure.mode == "after"
    first = await reopened.document(first_id, "current", context)
    second = await reopened.document(second_id, "current", context)
    if marker_survived:
        assert first is not None and first["value"] == {"text": "α"}
        assert second is not None and second["value"] == {"text": "β"}
    else:
        assert first is None
        assert second is None


FLUSH_FAILURES = [
    Failure("flush", 1, "before"),
    Failure("flush", 1, "after"),
    Failure("flush", 2, "before"),
    Failure("flush", 2, "after"),
]


@pytest.mark.parametrize(
    "failure",
    FLUSH_FAILURES,
    ids=[f"{f.mode} failure at flush {f.call}" for f in FLUSH_FAILURES],
)
async def test_poisons_after_a_flush_failure_and_never_writes_a_marker(failure: Failure) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env, {"fsync": True})
    await _create_root(storage)
    first_id = await storage.mint_id()
    second_id = await storage.mint_id()
    env.fail(failure)
    with pytest.raises(JsonlStoragePoisonedError, match="poisoned"):
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": _session_document(first_id, "flush.first"),
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
                {
                    "type": "document.create",
                    "record": _session_document(second_id, "flush.second"),
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
            ],
            context,
        )
    reopened = await _open_storage(directory)
    assert await reopened.document(first_id, "current", context) is None
    assert await reopened.document(second_id, "current", context) is None


async def test_orders_publication_flushes_and_flushes_main_only_to_authorize_reclamation() -> (
    None
):
    for fsync in (False, True):
        directory = _temp_directory()
        env = InstrumentedEnv(cwd=str(directory))
        storage = await _open_storage(directory, env, {"fsync": fsync})
        await _create_root(storage)
        first_id = await storage.mint_id()
        second_id = await storage.mint_id()
        env.clear()
        await storage.commit(
            [
                {
                    "type": "document.create",
                    "record": _session_document(second_id, "second"),
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
                {
                    "type": "document.create",
                    "record": _session_document(first_id, "first"),
                    "content": {"kind": "base", "version": 1, "value": {}},
                },
            ],
            context,
        )
        expected = [
            f"append:doc-{second_id}.jsonl",
            f"append:doc-{first_id}.jsonl",
            *(
                [f"flush:doc-{second_id}.jsonl", f"flush:doc-{first_id}.jsonl"]
                if fsync
                else []
            ),
            "append:main.jsonl",
        ]
        assert env.operations == expected

        env.clear()
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": first_id,
                    "content": {"kind": "base", "version": 1, "value": {"checkpoint": True}},
                }
            ],
            context,
        )
        assert env.operations == [
            f"append:doc-{first_id}.jsonl",
            *([f"flush:doc-{first_id}.jsonl"] if fsync else []),
            "append:main.jsonl",
            *(["flush:main.jsonl"] if fsync else []),
            f"write:doc-{first_id}.jsonl.reclaim",
            *([f"flush:doc-{first_id}.jsonl.reclaim"] if fsync else []),
            f"rename:doc-{first_id}.jsonl.reclaim->doc-{first_id}.jsonl",
        ]

        env.clear()
        await storage.commit(
            [
                {
                    "type": "entry",
                    "value": {
                        "id": await storage.mint_id(),
                        "conversationId": ROOT_CONVERSATION_ID,
                        "kind": "main-only",
                    },
                }
            ],
            context,
        )
        assert env.operations == ["append:main.jsonl"]

        task_id = await storage.mint_id()
        await storage.commit([{"type": "task", "value": _pending_task(task_id)}], context)
        env.clear()
        await storage.commit([{"type": "task", "value": _terminal_task(task_id)}], context)
        assert env.operations == [
            "append:main.jsonl",
            *(["flush:main.jsonl"] if fsync else []),
            f"remove:task-{task_id}.jsonl",
        ]


RECLAIM_FAILURES = [
    Failure("write", 1, "before"),
    Failure("write", 1, "short"),
    Failure("write", 1, "after"),
    Failure("rename", 1, "before"),
    Failure("rename", 1, "after"),
]


@pytest.mark.parametrize(
    "failure",
    RECLAIM_FAILURES,
    ids=[f"{f.operation} {f.mode}" for f in RECLAIM_FAILURES],
)
async def test_recovers_a_committed_base_across_a_reclaim_failure(failure: Failure) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env)
    await _create_root(storage)
    id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(id),
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 1]]},
            }
        ],
        context,
    )

    env.fail(failure)
    assert (
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "base", "version": 1, "value": {"count": 2}},
                }
            ],
            context,
        )
        == 4
    )
    current = await storage.document(id, "current", context)
    assert current is not None and current["value"] == {"count": 2}
    await storage.close(context)

    reopened = await _open_storage(directory)
    recovered = await reopened.document(id, "current", context)
    assert recovered is not None and recovered["value"] == {"count": 2}
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 1
    assert [name for name in os.listdir(directory) if name.endswith(".reclaim")] == []


@pytest.mark.parametrize("mode", ["before", "after"])
async def test_recovers_document_retirement_reclamation_across_a_remove_failure(
    mode: str,
) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env)
    await _create_root(storage)
    task_id = await storage.mint_id()
    id = await storage.mint_id()
    await storage.commit(
        [
            {"type": "task", "value": _pending_task(task_id)},
            {
                "type": "document.create",
                "record": {"id": id, "kind": "task.document", "scope": {"kind": "task", "taskId": task_id}},
                "content": {"kind": "base", "version": 1, "value": {"count": 1}},
            },
        ],
        context,
    )
    env.fail(Failure("remove", 1, mode))
    assert await storage.commit([{"type": "document.retire", "id": id}], context) == 3
    assert await storage.document(id, "current", context) is None
    await storage.close(context)

    reopened = await _open_storage(directory)
    assert await reopened.document(id, "current", context) is None
    assert await reopened.task(task_id, context) == _pending_task(task_id)
    assert not _file_exists(directory / f"doc-{id}.jsonl")
    assert [name for name in os.listdir(directory) if name.endswith(".reclaim")] == []


@pytest.mark.parametrize("mode", ["before", "after"])
async def test_defers_reclamation_after_an_authorizing_main_flush_failure(mode: str) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env, {"fsync": True})
    await _create_root(storage)
    id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(id),
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    env.fail(Failure("flush", 2, mode))
    assert (
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "base", "version": 1, "value": {"count": 2}},
                }
            ],
            context,
        )
        == 3
    )
    assert env.operations == [
        f"append:doc-{id}.jsonl",
        f"flush:doc-{id}.jsonl",
        "append:main.jsonl",
        "flush:main.jsonl",
    ]
    current = await storage.document(id, "current", context)
    assert current is not None and current["value"] == {"count": 2}
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 2
    await storage.close(context)

    recovery_env = InstrumentedEnv(cwd=str(directory))
    recovery_env.fail(Failure("flush", 1, mode))
    deferred = await _open_storage(directory, recovery_env, {"fsync": True})
    deferred_value = await deferred.document(id, "current", context)
    assert deferred_value is not None and deferred_value["value"] == {"count": 2}
    assert recovery_env.operations == ["flush:main.jsonl"]
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 2
    await deferred.close(context)

    reclaimed = await _open_storage(directory, None, {"fsync": True})
    reclaimed_value = await reclaimed.document(id, "current", context)
    assert reclaimed_value is not None and reclaimed_value["value"] == {"count": 2}
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 1


@pytest.mark.parametrize("mode", ["before", "after"])
async def test_keeps_a_committed_base_usable_after_a_reclaim_temp_flush_failure(
    mode: str,
) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env, {"fsync": True})
    await _create_root(storage)
    id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(id),
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    env.fail(Failure("flush", 3, mode))
    assert (
        await storage.commit(
            [
                {
                    "type": "document.change",
                    "id": id,
                    "content": {"kind": "base", "version": 1, "value": {"count": 2}},
                }
            ],
            context,
        )
        == 3
    )
    env.clear()
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 3]]},
            }
        ],
        context,
    )
    await storage.close(context)

    reopened = await _open_storage(directory, None, {"fsync": True})
    value = await reopened.document(id, "current", context)
    assert value is not None and value["value"] == {"count": 3}
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 2


@pytest.mark.parametrize("mode", ["before", "after"])
async def test_recovers_terminal_task_reclamation_across_a_remove_failure(mode: str) -> None:
    directory = _temp_directory()
    env = InstrumentedEnv(cwd=str(directory))
    storage = await _open_storage(directory, env)
    await _create_root(storage)
    id = await storage.mint_id()
    await storage.commit([{"type": "task", "value": _pending_task(id)}], context)
    env.fail(Failure("remove", 1, mode))
    assert await storage.commit([{"type": "task", "value": _terminal_task(id)}], context) == 3
    assert await storage.task(id, context) == _terminal_task(id)
    await storage.close(context)

    reopened = await _open_storage(directory)
    assert await reopened.task(id, context) == _terminal_task(id)
    assert not _file_exists(directory / f"task-{id}.jsonl")
    assert [name for name in os.listdir(directory) if name.endswith(".reclaim")] == []


async def test_appends_later_deltas_to_the_replacement_sidecar_after_a_current_only_base() -> (
    None
):
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(id),
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "base", "version": 1, "value": {"count": 10}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 11]]},
            }
        ],
        context,
    )
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 2
    reopened = await _open_storage(directory)
    value = await reopened.document(id, "current", context)
    assert value is not None and value["value"] == {"count": 11}


async def test_never_reclaims_rewindable_history_including_after_a_base_and_retirement() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    id = await storage.mint_id()
    record = {
        "id": id,
        "kind": "rewindable",
        "scope": {"kind": "conversation", "conversationId": ROOT_CONVERSATION_ID},
        "history": "rewindable",
        "fork": "asOf",
    }
    created_at = await storage.commit(
        [
            {
                "type": "document.create",
                "record": record,
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    changed_at = await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 1]]},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": id,
                "content": {"kind": "base", "version": 1, "value": {"count": 2}},
            }
        ],
        context,
    )
    await storage.commit([{"type": "document.retire", "id": id}], context)

    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 3
    reopened = await _open_storage(directory)
    created = await reopened.document(id, created_at, context)
    assert created is not None and created["value"] == {"count": 0}
    changed = await reopened.document(id, changed_at, context)
    assert changed is not None and changed["value"] == {"count": 1}
    assert await reopened.document(id, "current", context) is None
    assert len(_read_lines(directory / f"doc-{id}.jsonl")) == 3


async def test_reclaims_retired_task_session_and_latest_conversation_document_sidecars() -> (
    None
):
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    task_id = await storage.mint_id()
    session_id = await storage.mint_id()
    latest_id = await storage.mint_id()
    task_document_id = await storage.mint_id()
    created_at = await storage.commit(
        [
            {"type": "task", "value": _pending_task(task_id)},
            {
                "type": "document.create",
                "record": _session_document(session_id, "session"),
                "content": {"kind": "base", "version": 1, "value": {}},
            },
            {
                "type": "document.create",
                "record": {
                    "id": latest_id,
                    "kind": "latest",
                    "scope": {"kind": "conversation", "conversationId": ROOT_CONVERSATION_ID},
                    "history": "latest",
                    "fork": "current",
                },
                "content": {"kind": "base", "version": 1, "value": {}},
            },
            {
                "type": "document.create",
                "record": {
                    "id": task_document_id,
                    "kind": "task",
                    "scope": {"kind": "task", "taskId": task_id},
                },
                "content": {"kind": "base", "version": 1, "value": {}},
            },
        ],
        context,
    )
    retired_at = await storage.commit(
        [
            {"type": "document.retire", "id": session_id},
            {"type": "document.retire", "id": latest_id},
            {"type": "document.retire", "id": task_document_id},
            {"type": "task", "value": _terminal_task(task_id)},
        ],
        context,
    )
    for name in [
        f"doc-{session_id}.jsonl",
        f"doc-{latest_id}.jsonl",
        f"doc-{task_document_id}.jsonl",
        f"task-{task_id}.jsonl",
    ]:
        assert not _file_exists(directory / name)

    reopened = await _open_storage(directory)
    assert await reopened.task(task_id, context) == _terminal_task(task_id)
    assert await reopened.document(session_id, "current", context) is None
    assert await reopened.document(latest_id, "current", context) is None
    assert await reopened.document(task_document_id, "current", context) is None
    historical = await reopened.find_document(
        {"kind": "session", "scope": {"kind": "session"}}, created_at, context
    )
    assert historical is not None
    assert historical["id"] == session_id
    assert historical["createdAt"] == created_at
    assert historical["retiredAt"] == retired_at
    assert (
        await reopened.find_document(
            {"kind": "session", "scope": {"kind": "session"}}, retired_at, context
        )
        is None
    )


async def test_truncates_torn_utf8_tails_at_exact_offsets_and_reuses_the_sequence() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    document_id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(document_id),
                "content": {"kind": "base", "version": 1, "value": {"text": "kept"}},
            }
        ],
        context,
    )
    sidecar_path = directory / f"doc-{document_id}.jsonl"
    main_path = directory / "main.jsonl"
    sidecar_size = sidecar_path.stat().st_size
    main_size = main_path.stat().st_size
    torn = '{"text":"€'.encode()
    with sidecar_path.open("ab") as handle:
        handle.write(torn[:-1])
    with main_path.open("ab") as handle:
        handle.write(torn[:-1])

    reopened = await _open_storage(directory)
    assert sidecar_path.stat().st_size == sidecar_size
    assert main_path.stat().st_size == main_size
    assert (
        await reopened.commit(
            [
                {
                    "type": "document.change",
                    "id": document_id,
                    "content": {"kind": "delta", "version": 1, "ops": []},
                }
            ],
            context,
        )
        == 3
    )


async def test_removes_complete_unconfirmed_tails_without_resurrecting_terminal_tasks() -> (
    None
):
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    task_id = await storage.mint_id()
    await storage.commit([{"type": "task", "value": _pending_task(task_id)}], context)
    await storage.commit([{"type": "task", "value": _terminal_task(task_id)}], context)
    sidecar_path = directory / f"task-{task_id}.jsonl"
    assert not _file_exists(sidecar_path)
    with sidecar_path.open("a") as handle:
        handle.write(
            json.dumps(
                {
                    "format": 1,
                    "type": "record",
                    "seq": 4,
                    "ordinal": 0,
                    "payload": {"type": "task", "value": _pending_task(task_id, "stale")},
                }
            )
            + "\n"
        )

    reopened = await _open_storage(directory)
    assert not _file_exists(sidecar_path)
    assert await reopened.task(task_id, context) == _terminal_task(task_id)
    assert await reopened.commit([], context) == 4


async def test_fails_open_when_confirmed_sidecar_data_is_missing() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    document_id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(document_id),
                "content": {"kind": "base", "version": 1, "value": {}},
            }
        ],
        context,
    )
    (directory / f"doc-{document_id}.jsonl").write_text("")
    with pytest.raises(JsonlCorruptionError, match="Missing confirmed sidecar record"):
        await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)


async def test_rejects_a_confirmed_record_after_an_unconfirmed_sidecar_record() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    document_id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(document_id),
                "content": {"kind": "base", "version": 1, "value": {"count": 0}},
            }
        ],
        context,
    )
    await storage.commit(
        [
            {
                "type": "document.change",
                "id": document_id,
                "content": {"kind": "delta", "version": 1, "ops": [["s", ["count"], 1]]},
            }
        ],
        context,
    )
    path = directory / f"doc-{document_id}.jsonl"
    first, second = _read_lines(path)
    unconfirmed = json.dumps(
        {
            "format": 1,
            "type": "record",
            "seq": 2,
            "ordinal": 999,
            "payload": {
                "type": "document",
                "id": document_id,
                "content": {"kind": "delta", "version": 1, "ops": []},
            },
        }
    )
    path.write_text(f"{first}\n{unconfirmed}\n{second}\n")
    with pytest.raises(JsonlCorruptionError, match="Confirmed record follows an unconfirmed tail"):
        await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)


async def test_rejects_non_increasing_main_commit_sequences() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    path = directory / "main.jsonl"
    with path.open("a") as handle:
        handle.write(path.read_text())
    with pytest.raises(JsonlCorruptionError, match="Commit sequence does not strictly increase"):
        await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)


async def test_rejects_structurally_invalid_confirmed_document_content() -> None:
    directory = _temp_directory()
    storage = await _open_storage(directory)
    await _create_root(storage)
    document_id = await storage.mint_id()
    await storage.commit(
        [
            {
                "type": "document.create",
                "record": _session_document(document_id),
                "content": {"kind": "base", "version": 1, "value": {}},
            }
        ],
        context,
    )
    path = directory / f"doc-{document_id}.jsonl"
    record = json.loads(path.read_text().strip())
    del record["payload"]["content"]["value"]
    path.write_text(json.dumps(record) + "\n")

    with pytest.raises(JsonlCorruptionError, match="Invalid document content"):
        await JsonlStorage.open(str(directory), PosixExecutionEnv(cwd=str(directory)), context)


async def test_rejects_malformed_complete_main_and_sidecar_lines() -> None:
    main_directory = _temp_directory("pi-durable-jsonl-main-")
    main_storage = await _open_storage(main_directory)
    await _create_root(main_storage)
    with (main_directory / "main.jsonl").open("a") as handle:
        handle.write("{bad}\n")
    with pytest.raises(JsonlCorruptionError, match="Malformed complete main.jsonl"):
        await JsonlStorage.open(
            str(main_directory), PosixExecutionEnv(cwd=str(main_directory)), context
        )

    sidecar_directory = _temp_directory("pi-durable-jsonl-sidecar-")
    sidecar_storage = await _open_storage(sidecar_directory)
    await _create_root(sidecar_storage)
    (sidecar_directory / "doc-99.jsonl").write_text("{bad}\n")
    with pytest.raises(JsonlCorruptionError, match="Malformed complete doc-99.jsonl"):
        await JsonlStorage.open(
            str(sidecar_directory), PosixExecutionEnv(cwd=str(sidecar_directory)), context
        )

"""Append-only journal storage shared by the JSONL and SQLite backends.

Both backends persist whole atomic commits and rebuild the in-memory reference state on
open. Commit order is preserved, so commit sequences (and therefore document history)
are identical after a reopen.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..json import copy_json
from .memory import MemoryStorage


class JournalStorage:
    """Base class for durable backends: persist a commit, then apply it to the mirror."""

    def __init__(self) -> None:
        self._mirror = MemoryStorage()

    async def commit(self, writes: Sequence[Mapping[str, Any]]) -> int:
        seq = await self._mirror.commit(writes)
        self._persist(seq, copy_json(list(writes)))
        return seq

    async def mint_id(self) -> int:
        return await self._mirror.mint_id()

    async def conversation(self, conversation_id: int):
        return await self._mirror.conversation(conversation_id)

    async def scan_conversations(self, query, limit, cursor=None):
        return await self._mirror.scan_conversations(query, limit, cursor)

    async def entry(self, conversation_id: int, entry_id: int):
        return await self._mirror.entry(conversation_id, entry_id)

    async def entry_by_id(self, entry_id: int):
        return await self._mirror.entry_by_id(entry_id)

    async def find_latest_head_marker(self, conversation_id: int, at_or_before: int | None = None):
        return await self._mirror.find_latest_head_marker(conversation_id, at_or_before)

    async def scan_entries(self, query, limit, cursor=None):
        return await self._mirror.scan_entries(query, limit, cursor)

    async def task(self, task_id: int):
        return await self._mirror.task(task_id)

    async def scan_tasks(self, query, limit, cursor=None):
        return await self._mirror.scan_tasks(query, limit, cursor)

    async def submission(self, submission_id: int):
        return await self._mirror.submission(submission_id)

    async def scan_submissions(self, query, limit, cursor=None):
        return await self._mirror.scan_submissions(query, limit, cursor)

    async def submission_by_request(self, conversation_id: int, request_id: str):
        return await self._mirror.submission_by_request(conversation_id, request_id)

    async def find_document(self, address, at):
        return await self._mirror.find_document(address, at)

    async def document(self, document_id: int, at):
        return await self._mirror.document(document_id, at)

    async def scan_documents(self, query, limit, cursor=None):
        return await self._mirror.scan_documents(query, limit, cursor)

    async def close(self) -> None:
        await self._mirror.close()
        self._close_journal()

    # ── Backend hooks ──────────────────────────────────────────────────────

    def _persist(self, seq: int, writes: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    def _close_journal(self) -> None:
        pass

    async def _replay(self, commits: list[tuple[int, list[dict[str, Any]]]]) -> None:
        for expected, writes in commits:
            actual = await self._mirror.commit(writes)
            if actual != expected:
                raise RuntimeError(
                    f"Journal commit sequence {expected} replayed as {actual}; the journal is not contiguous"
                )


class JsonlStorage(JournalStorage):
    """Journal one Storage commit per JSON line under ``directory/journal.jsonl``."""

    def __init__(self, directory: str | os.PathLike[str]) -> None:
        super().__init__()
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "journal.jsonl"
        commits: list[tuple[int, list[dict[str, Any]]]] = []
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise RuntimeError(f"Corrupt journal line {line_number} in {self.path}") from error
                    commits.append((int(entry["seq"]), entry["writes"]))
        self._queue = commits
        self._handle = self.path.open("a", encoding="utf-8")
        self._pending_replay = True

    async def ready(self) -> JsonlStorage:
        if self._pending_replay:
            self._pending_replay = False
            await self._replay(self._queue)
            self._queue = []
        return self

    def _persist(self, seq: int, writes: list[dict[str, Any]]) -> None:
        line = json.dumps({"seq": seq, "writes": writes}, separators=(",", ":"), allow_nan=False)
        self._handle.write(line + "\n")
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def _close_journal(self) -> None:
        if not self._handle.closed:
            self._handle.close()


class SqliteStorage(JournalStorage):
    """Durable journal in SQLite. One row per atomic commit in a WAL database."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        super().__init__()
        self._connection = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            "CREATE TABLE IF NOT EXISTS commits (seq INTEGER PRIMARY KEY, writes TEXT NOT NULL)"
        )
        rows = self._connection.execute("SELECT seq, writes FROM commits ORDER BY seq").fetchall()
        self._queue = [(int(seq), json.loads(writes)) for seq, writes in rows]
        self._pending_replay = True

    async def ready(self) -> SqliteStorage:
        if self._pending_replay:
            self._pending_replay = False
            await self._replay(self._queue)
            self._queue = []
        return self

    def _persist(self, seq: int, writes: list[dict[str, Any]]) -> None:
        payload = json.dumps(writes, separators=(",", ":"), allow_nan=False)
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            self._connection.execute("INSERT INTO commits (seq, writes) VALUES (?, ?)", (seq, payload))
            self._connection.execute("COMMIT")
        except BaseException:
            self._connection.execute("ROLLBACK")
            raise

    def _close_journal(self) -> None:
        self._connection.close()


__all__ = ["JournalStorage", "JsonlStorage", "SqliteStorage"]

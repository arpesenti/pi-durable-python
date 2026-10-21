"""Translated v1.3.0 ``sqlite-migrations`` suite.

Covers repeatable schema application, the newer-than-supported guard, atomic bootstrap and
migration rollback, and preservation of stored records across a failed migration retry.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
from collections.abc import Generator
from pathlib import Path

import pytest

from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.ids import ROOT_CONVERSATION_ID, EntryId
from pi_durable.storage.sqlite import (
    CURRENT_SQLITE_SCHEMA_VERSION,
    SQLITE_MIGRATIONS,
    SqliteMigration,
    apply_sqlite_migrations,
)
from pi_durable.storage.sqlite.posix import (
    PosixSqliteDatabase,
    open_posix_sqlite_database,
    open_posix_sqlite_storage,
)

context = BACKGROUND_CONTEXT

_directories: list[Path] = []


@pytest.fixture(autouse=True)
def _cleanup() -> Generator[None, None, None]:
    yield
    while _directories:
        shutil.rmtree(_directories.pop(), ignore_errors=True)


def _database_path() -> str:
    directory = Path(tempfile.mkdtemp(prefix="pi-durable-migrations-"))
    _directories.append(directory)
    return str(directory / "storage.sqlite")


async def _open_database() -> PosixSqliteDatabase:
    return await open_posix_sqlite_database(_database_path())


async def test_creates_the_current_schema_and_can_be_applied_repeatedly() -> None:
    database = await _open_database()
    try:
        await apply_sqlite_migrations(database)
        await apply_sqlite_migrations(database)
        assert await database.get("SELECT version FROM durable_schema WHERE singleton = 1") == {
            "version": CURRENT_SQLITE_SCHEMA_VERSION
        }
        assert await database.get(
            "SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1"
        ) == {"next_id": "2", "next_seq": 1}
    finally:
        await database.close()


async def test_rejects_a_database_newer_than_the_portable_core() -> None:
    path = _database_path()
    database = await open_posix_sqlite_database(path)
    await apply_sqlite_migrations(database)
    await database.run(
        "UPDATE durable_schema SET version = ? WHERE singleton = 1",
        CURRENT_SQLITE_SCHEMA_VERSION + 1,
    )
    await database.close()

    with pytest.raises(RuntimeError, match="is newer than supported version"):
        await open_posix_sqlite_storage(path)


async def test_rolls_initial_bootstrap_and_every_pending_migration_back_together() -> None:
    database = await _open_database()
    try:
        failed = [
            SqliteMigration(
                version=1,
                statements=(
                    "CREATE TABLE migration_first (value TEXT) STRICT",
                    "INSERT INTO migration_first (value) VALUES ('retained')",
                ),
            ),
            SqliteMigration(
                version=2,
                statements=(
                    "CREATE TABLE migration_second (value TEXT) STRICT",
                    "THIS IS NOT SQL",
                ),
            ),
        ]
        with pytest.raises(sqlite3.OperationalError):
            await apply_sqlite_migrations(database, failed)
        assert await database.get(
            "SELECT count(*) AS count FROM sqlite_schema "
            "WHERE name IN ('durable_schema', 'migration_first', 'migration_second')"
        ) == {"count": 0}

        await apply_sqlite_migrations(
            database,
            [
                failed[0],
                SqliteMigration(
                    version=2,
                    statements=("CREATE TABLE migration_second (value TEXT) STRICT",),
                ),
            ],
        )
        assert await database.get("SELECT version FROM durable_schema WHERE singleton = 1") == {
            "version": 2
        }
        assert await database.get("SELECT value FROM migration_first") == {"value": "retained"}
    finally:
        await database.close()


async def test_rolls_a_failed_migration_back_and_preserves_stored_data_for_a_successful_retry() -> None:
    path = _database_path()
    storage = await open_posix_sqlite_storage(path)
    await storage.commit(
        [
            {"type": "conversation", "value": {"id": ROOT_CONVERSATION_ID}},
            {
                "type": "entry",
                "value": {
                    "id": EntryId(2),
                    "conversationId": ROOT_CONVERSATION_ID,
                    "kind": "retained",
                    "data": {"retained": True},
                },
            },
        ],
        context,
    )
    await storage.close(context)

    database = await open_posix_sqlite_database(path)
    try:
        next_version = CURRENT_SQLITE_SCHEMA_VERSION + 1
        failed_migrations = [
            *SQLITE_MIGRATIONS,
            SqliteMigration(
                version=next_version,
                statements=(
                    "CREATE TABLE migration_probe (value TEXT) STRICT",
                    "THIS IS NOT SQL",
                ),
            ),
        ]
        with pytest.raises(sqlite3.OperationalError):
            await apply_sqlite_migrations(database, failed_migrations)
        assert await database.get("SELECT version FROM durable_schema WHERE singleton = 1") == {
            "version": CURRENT_SQLITE_SCHEMA_VERSION
        }
        assert await database.get(
            "SELECT count(*) AS count FROM sqlite_schema "
            "WHERE type = 'table' AND name = 'migration_probe'"
        ) == {"count": 0}

        successful_migrations = [
            *SQLITE_MIGRATIONS,
            SqliteMigration(
                version=next_version,
                statements=("CREATE TABLE migration_probe (value TEXT) STRICT",),
            ),
        ]
        await apply_sqlite_migrations(database, successful_migrations)
        assert await database.get("SELECT version FROM durable_schema WHERE singleton = 1") == {
            "version": next_version
        }
        stored = await database.get("SELECT record, commit_seq FROM entries WHERE id = 2")
        assert stored is not None
        assert json.loads(stored["record"]) == {
            "id": 2,
            "conversationId": ROOT_CONVERSATION_ID,
            "kind": "retained",
            "data": {"retained": True},
        }
        assert stored["commit_seq"] == 1
        assert await database.get(
            "SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1"
        ) == {"next_id": "3", "next_seq": 2}
    finally:
        await database.close()

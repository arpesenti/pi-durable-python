"""Ordered SQLite schema history and atomic application, translated from v1.3.0 ``sqlite/migrations.ts``."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .database import SqliteDatabase, SqliteExecutor
from .schema import DEFAULT_SQLITE_SCHEMA, SqliteSchema


@dataclass(frozen=True, slots=True)
class SqliteMigration:
    """One versioned, order-sensitive group of schema statements."""

    version: int
    statements: Sequence[str]


def create_sqlite_migrations(schema: SqliteSchema) -> tuple[SqliteMigration, ...]:
    """Build immutable, ordered schema history for a set of SQLite identifiers."""

    tables = schema.tables
    indexes = schema.indexes
    # next_id is TEXT because JavaScript's SQLite driver rejects INTEGER results outside its safe
    # integer range. The Python port keeps the column shape for persisted-record parity.
    initial_schema = (
        f"""CREATE TABLE {tables['durableMetadata']} (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            next_id TEXT NOT NULL,
            next_seq INTEGER NOT NULL
        ) STRICT""",
        f"INSERT INTO {tables['durableMetadata']} (singleton, next_id, next_seq) VALUES (1, '2', 1)",
        f"""CREATE TABLE {tables['recordIds']} (
            id INTEGER PRIMARY KEY,
            record_type TEXT NOT NULL
                CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document'))
        ) STRICT""",
        f"""CREATE TABLE {tables['conversations']} (
            id INTEGER PRIMARY KEY,
            owner_conversation_id INTEGER,
            owner_task_id INTEGER,
            record TEXT NOT NULL CHECK (json_valid(record))
        ) STRICT""",
        f"CREATE INDEX {indexes['conversationsByOwnerConversation']} "
        f"ON {tables['conversations']} (owner_conversation_id, id)",
        f"CREATE INDEX {indexes['conversationsByOwnerTask']} "
        f"ON {tables['conversations']} (owner_task_id, id)",
        f"""CREATE TABLE {tables['entries']} (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            head INTEGER,
            commit_seq INTEGER NOT NULL,
            record TEXT NOT NULL CHECK (json_valid(record))
        ) STRICT""",
        f"CREATE INDEX {indexes['entriesByConversation']} "
        f"ON {tables['entries']} (conversation_id, id DESC)",
        f"CREATE INDEX {indexes['entryHeadsByConversation']} "
        f"ON {tables['entries']} (conversation_id, id DESC) WHERE head IS NOT NULL",
        f"""CREATE TABLE {tables['tasks']} (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
            abort_requested INTEGER NOT NULL CHECK (abort_requested IN (0, 1)),
            background INTEGER NOT NULL CHECK (background IN (0, 1)),
            record TEXT NOT NULL CHECK (json_valid(record))
        ) STRICT""",
        f"CREATE INDEX {indexes['tasksByStatus']} ON {tables['tasks']} (status, id)",
        f"CREATE INDEX {indexes['tasksByConversation']} ON {tables['tasks']} (conversation_id, id)",
        f"CREATE INDEX {indexes['tasksByKind']} ON {tables['tasks']} (kind, id)",
        f"CREATE INDEX {indexes['tasksByAbortRequested']} "
        f"ON {tables['tasks']} (abort_requested, id)",
        f"CREATE INDEX {indexes['tasksByBackground']} ON {tables['tasks']} (background, id)",
        f"""CREATE TABLE {tables['submissions']} (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            request_id TEXT,
            status TEXT NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
            record TEXT NOT NULL CHECK (json_valid(record))
        ) STRICT""",
        f"CREATE INDEX {indexes['submissionsByRequest']} "
        f"ON {tables['submissions']} (conversation_id, request_id)",
        f"CREATE INDEX {indexes['submissionsByConversation']} "
        f"ON {tables['submissions']} (conversation_id, id)",
        f"CREATE INDEX {indexes['submissionsByStatus']} ON {tables['submissions']} (status, id)",
        f"""CREATE TABLE {tables['documents']} (
            id INTEGER PRIMARY KEY,
            kind TEXT NOT NULL,
            family INTEGER NOT NULL CHECK (family IN (0, 1)),
            key_value TEXT NOT NULL,
            scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
            owner_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            retired_at INTEGER,
            record TEXT NOT NULL CHECK (json_valid(record))
        ) STRICT""",
        f"""CREATE INDEX {indexes['documentsByAddress']} ON {tables['documents']}
            (kind, scope_kind, owner_id, family, key_value, created_at DESC, retired_at)""",
        f"CREATE INDEX {indexes['documentsByScope']} "
        f"ON {tables['documents']} (scope_kind, owner_id, id)",
        f"CREATE INDEX {indexes['documentsByScopeKind']} "
        f"ON {tables['documents']} (scope_kind, owner_id, kind, id)",
        f"""CREATE TABLE {tables['documentRevisions']} (
            document_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
            version INTEGER NOT NULL,
            content TEXT NOT NULL CHECK (json_valid(content)),
            PRIMARY KEY (document_id, seq)
        ) STRICT""",
        f"CREATE INDEX {indexes['documentRevisionsByKind']} "
        f"ON {tables['documentRevisions']} (document_id, kind, seq DESC)",
    )
    # Version 2 adds the `sleeping` task status. SQLite cannot change a CHECK constraint in place,
    # so the table is rebuilt.
    rebuilt = f"{tables['tasks']}_v2"
    sleeping_status = (
        f"""CREATE TABLE {rebuilt} (
            id INTEGER PRIMARY KEY,
            conversation_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK (status IN ('pending', 'running', 'waiting', 'sleeping', 'completing', 'terminal')),
            abort_requested INTEGER NOT NULL CHECK (abort_requested IN (0, 1)),
            background INTEGER NOT NULL CHECK (background IN (0, 1)),
            record TEXT NOT NULL CHECK (json_valid(record))
        ) STRICT""",
        f"""INSERT INTO {rebuilt} (id, conversation_id, kind, status, abort_requested, background, record)
            SELECT id, conversation_id, kind, status, abort_requested, background, record
            FROM {tables['tasks']}""",
        f"DROP TABLE {tables['tasks']}",
        f"ALTER TABLE {rebuilt} RENAME TO {tables['tasks']}",
        f"CREATE INDEX {indexes['tasksByStatus']} ON {tables['tasks']} (status, id)",
        f"CREATE INDEX {indexes['tasksByConversation']} ON {tables['tasks']} (conversation_id, id)",
        f"CREATE INDEX {indexes['tasksByKind']} ON {tables['tasks']} (kind, id)",
        f"CREATE INDEX {indexes['tasksByAbortRequested']} "
        f"ON {tables['tasks']} (abort_requested, id)",
        f"CREATE INDEX {indexes['tasksByBackground']} ON {tables['tasks']} (background, id)",
    )
    return (
        SqliteMigration(version=1, statements=initial_schema),
        SqliteMigration(version=2, statements=sleeping_status),
    )


SQLITE_MIGRATIONS = create_sqlite_migrations(DEFAULT_SQLITE_SCHEMA)
CURRENT_SQLITE_SCHEMA_VERSION = SQLITE_MIGRATIONS[-1].version if SQLITE_MIGRATIONS else 0


async def apply_sqlite_migrations(
    database: SqliteDatabase,
    migrations: Sequence[SqliteMigration] | None = None,
) -> None:
    """Apply all pending schema migrations atomically."""

    await apply_sqlite_migrations_for_schema(
        database,
        DEFAULT_SQLITE_SCHEMA,
        SQLITE_MIGRATIONS if migrations is None else migrations,
    )


async def apply_sqlite_migrations_for_schema(
    database: SqliteDatabase,
    schema: SqliteSchema,
    migrations: Sequence[SqliteMigration] | None = None,
) -> None:
    """Apply migrations rendered for a particular schema."""

    if migrations is None:
        migrations = create_sqlite_migrations(schema)
    for index, migration in enumerate(migrations):
        if migration.version != index + 1:
            raise RuntimeError("Durable SQLite migrations must have contiguous versions starting at 1")

    durable_schema = schema.tables["durableSchema"]

    async def migrate(transaction: SqliteExecutor) -> None:
        await transaction.exec(
            f"""CREATE TABLE IF NOT EXISTS {durable_schema} (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                version INTEGER NOT NULL CHECK (version >= 0)
            ) STRICT"""
        )
        await transaction.run(
            f"INSERT OR IGNORE INTO {durable_schema} (singleton, version) VALUES (1, 0)"
        )
        row = await transaction.get(
            f"SELECT version FROM {durable_schema} WHERE singleton = 1"
        )
        if row is None:
            raise RuntimeError("Durable SQLite schema metadata is missing")
        current_version = migrations[-1].version if migrations else 0
        if row["version"] > current_version:
            raise RuntimeError(
                f"Durable SQLite schema version {row['version']} is newer than supported "
                f"version {current_version}"
            )
        for migration in migrations:
            if migration.version <= row["version"]:
                continue
            for statement in migration.statements:
                await transaction.exec(statement)
            await transaction.run(
                f"UPDATE {durable_schema} SET version = ? WHERE singleton = 1", migration.version
            )

    await database.transaction(migrate)


__all__ = [
    "CURRENT_SQLITE_SCHEMA_VERSION",
    "SQLITE_MIGRATIONS",
    "SqliteMigration",
    "apply_sqlite_migrations",
    "apply_sqlite_migrations_for_schema",
    "create_sqlite_migrations",
]

"""Portable SQLite storage backend and migrations.

The host adapter lives in :mod:`pi_durable.storage.sqlite.posix`; this subpath stays free of it,
so importing the portable backend never selects a driver.
"""

from .database import SqliteDatabase, SqliteExecutor, SqliteValue
from .migrations import (
    CURRENT_SQLITE_SCHEMA_VERSION,
    SQLITE_MIGRATIONS,
    SqliteMigration,
    apply_sqlite_migrations,
)
from .storage import SqliteStorage, SqliteStorageOptions

__all__ = [
    "CURRENT_SQLITE_SCHEMA_VERSION",
    "SQLITE_MIGRATIONS",
    "SqliteDatabase",
    "SqliteExecutor",
    "SqliteMigration",
    "SqliteStorage",
    "SqliteStorageOptions",
    "SqliteValue",
    "apply_sqlite_migrations",
]

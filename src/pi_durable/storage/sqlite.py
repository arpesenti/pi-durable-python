"""Open a SQLite-backed Storage."""

from __future__ import annotations

import os

from .journal import SqliteStorage


async def open_sqlite_storage(path: str | os.PathLike[str]) -> SqliteStorage:
    return await SqliteStorage(path).ready()


__all__ = ["SqliteStorage", "open_sqlite_storage"]

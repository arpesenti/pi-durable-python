"""Open a JSONL-backed Storage."""

from __future__ import annotations

import os

from .journal import JsonlStorage


async def open_jsonl_storage(directory: str | os.PathLike[str]) -> JsonlStorage:
    return await JsonlStorage(directory).ready()


__all__ = ["JsonlStorage", "open_jsonl_storage"]

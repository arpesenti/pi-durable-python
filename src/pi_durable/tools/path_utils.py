"""Tool path normalization and confinement to the execution environment's working directory."""

from __future__ import annotations

import os
import re
from collections.abc import Sequence

from ..context import Context
from ..env import ExecutionEnv, FileError, get_or_throw

UNICODE_SPACES = re.compile("[\u00a0\u2000-\u200a\u202f\u205f\u3000]")
NARROW_NO_BREAK_SPACE = "\u202f"
_AM_PM = re.compile(" (AM|PM)\\.", re.IGNORECASE)


def normalize_tool_path(path: str) -> str:
    """Normalize Unicode spaces and drop a leading ``@`` a model may add."""

    normalized = UNICODE_SPACES.sub(" ", path)
    return normalized[1:] if normalized.startswith("@") else normalized


def _within(root: str, path: str) -> bool:
    try:
        return os.path.commonpath([root, path]) == root
    except ValueError:
        return False


async def canonical_path(env: ExecutionEnv, path: str, context: Context) -> str:
    """The canonical path, or the canonical parent joined with the name when the file is missing."""

    result = await env.canonical_path(path, context)
    if result.ok:
        return result.value
    if result.error.code == "not_supported":
        return path
    if result.error.code != "not_found":
        raise result.error
    parent = get_or_throw(await env.join_path([path, ".."], context))
    if parent == path or not path.startswith(parent):
        return path
    separator = 0 if parent.endswith(("/", "\\")) else 1
    name = path[len(parent) + separator :]
    return get_or_throw(await env.join_path([await canonical_path(env, parent, context), name], context))


async def ensure_within_cwd(env: ExecutionEnv, resolved: str, original: str, context: Context) -> str:
    """Reject a resolved path that leaves the environment's working directory."""

    root = get_or_throw(await env.absolute_path(".", context))
    if not _within(os.path.abspath(root), os.path.abspath(resolved)):
        raise ValueError(f"Path {original} is outside the working directory")
    try:
        canonical_root = await canonical_path(env, root, context)
        canonical_target = await canonical_path(env, resolved, context)
    except FileError as error:
        if error.code in ("not_supported", "not_found"):
            # A filesystem without canonical paths falls back to the lexical check above.
            return resolved
        raise
    if not _within(os.path.abspath(canonical_root), os.path.abspath(canonical_target)):
        raise ValueError(f"Path {original} is outside the working directory")
    return resolved


async def resolve_tool_path(env: ExecutionEnv, path: str, context: Context) -> str:
    """Resolve the path against the environment, refusing to escape its working directory."""

    resolved = get_or_throw(await env.absolute_path(normalize_tool_path(path), context))
    return await ensure_within_cwd(env, resolved, path, context)


async def resolve_read_tool_path(env: ExecutionEnv, path: str, context: Context) -> str:
    """Resolve a ``read`` path, trying the Unicode variants a filesystem may use."""

    resolved = await resolve_tool_path(env, path, context)
    variants: Sequence[str] = (
        resolved,
        _AM_PM.sub(lambda match: f"{NARROW_NO_BREAK_SPACE}{match.group(1)}.", resolved),
        # NFD, for filesystems that store decomposed names.
        _normalize_nfd(resolved),
        resolved.replace("'", "\u2019"),
        _normalize_nfd(resolved.replace("'", "\u2019")),
    )
    seen: set[str] = set()
    for variant in variants:
        if variant in seen:
            continue
        seen.add(variant)
        if get_or_throw(await env.exists(variant, context)):
            return variant
    return resolved


def _normalize_nfd(text: str) -> str:
    import unicodedata

    return unicodedata.normalize("NFD", text)


__all__ = [
    "NARROW_NO_BREAK_SPACE",
    "UNICODE_SPACES",
    "canonical_path",
    "ensure_within_cwd",
    "normalize_tool_path",
    "resolve_read_tool_path",
    "resolve_tool_path",
]

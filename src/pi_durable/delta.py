"""Chord-compatible JSON operation batches.

Durable documents are materialized by replaying ordered operation batches. The tuple vocabulary
is the one Chord Delta publishes:

``["r", value]``
    Replace the complete value.
``["s", path, value]``
    Set an object property or array element.
``["d", path]``
    Delete an object property or remove an array element.
``["a", path, text]``
    Append to a string.
``["t", path, count]``
    Remove ``count`` UTF-16 code units from a string's front.
``["p", path, index, remove, items]``
    Splice an array.
``["m", path, permutation]``
    Reorder an array in place: ``new[i] = old[permutation[i]]``.

The applier is intentionally independent of the document tracker. Memory storage uses it to
materialize persisted deltas; Session drafts emit the same tuples from their own overlay.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, TypeAlias, cast

from .json import JsonValue, copy_json

Op: TypeAlias = list[Any]
"""One decoded operation tuple, represented as its JSON array."""

RESERVED_SEGMENTS = frozenset({"__proto__", "constructor", "prototype"})

MAX_DELTA_OPERATIONS = 4_096
"""Beyond this many operations a batch folds into one root replacement, as Chord does."""

OVERLAP_SCAN_LIMIT = 65_536
"""Characters Chord's bounded overlap search examines; beyond it a string is written whole."""


class UnsafePathError(Exception):
    """An operation addresses an unsafe or malformed path segment."""

    def __init__(self, segment: object) -> None:
        super().__init__(f"unsafe path segment: {segment}")
        self.segment = segment
        self.name = "UnsafePathError"


class PathError(Exception):
    """An operation cannot resolve to a value of the shape it expects."""

    def __init__(self, path: object) -> None:
        super().__init__(f"unresolvable path: {path}")
        self.path = path
        self.name = "PathError"


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def overlap_strings(before: str, after: str, limit: int = OVERLAP_SCAN_LIMIT) -> int:
    """Longest suffix of ``before`` that is a prefix of ``after``, within ``limit`` characters.

    Knuth–Morris–Pratt, so a large tail window costs one pass; Chord's delta writer uses the same
    bounded search and falls back to writing the whole string when it finds no shared part.
    """

    if not before or not after or limit <= 0:
        return 0
    prefix = after[:limit]
    combined = prefix + "\x00" + before
    table = [0] * len(combined)
    length = 0
    for index in range(1, len(combined)):
        while length > 0 and combined[index] != combined[length]:
            length = table[length - 1]
        if combined[index] == combined[length]:
            length += 1
        table[index] = length
    return min(table[-1], len(prefix), len(before))


def assert_safe_path(path: object) -> None:
    """Reject segments that could reach the prototype chain or are not JSON path segments."""

    if not isinstance(path, list):
        raise TypeError("path is not an array")
    for segment in path:
        if isinstance(segment, str):
            if segment in RESERVED_SEGMENTS:
                raise UnsafePathError(segment)
        elif not _is_int(segment) or segment < 0:
            raise UnsafePathError(segment)


def _assert_path_arg(value: object, *, non_empty: bool = False) -> None:
    if not isinstance(value, list):
        raise TypeError("path is not an array")
    if non_empty and not value:
        raise TypeError("path is empty")
    assert_safe_path(value)


def _assert_permutation(value: object) -> None:
    if not isinstance(value, list):
        raise TypeError("m permutation is not an array")
    seen = [False] * len(value)
    for index in value:
        if not _is_int(index) or index < 0 or index >= len(value) or seen[index]:
            raise TypeError("m permutation is not a bijection")
        seen[index] = True


def assert_valid_op(op: object) -> None:
    """Validate one decoded operation tuple."""

    if not isinstance(op, list) or not op:
        raise TypeError("op is not a tuple")
    verb = op[0]
    if verb == "r":
        if len(op) != 2:
            raise TypeError("r arity")
    elif verb == "s":
        if len(op) != 3:
            raise TypeError("s arity")
        _assert_path_arg(op[1], non_empty=True)
    elif verb == "d":
        if len(op) != 2:
            raise TypeError("d arity")
        _assert_path_arg(op[1], non_empty=True)
    elif verb == "a":
        if len(op) != 3 or not isinstance(op[2], str):
            raise TypeError("a shape")
        _assert_path_arg(op[1], non_empty=True)
    elif verb == "t":
        if len(op) != 3 or not _is_int(op[2]) or op[2] < 0:
            raise TypeError("t shape")
        _assert_path_arg(op[1], non_empty=True)
    elif verb == "p":
        if len(op) != 5:
            raise TypeError("p arity")
        _assert_path_arg(op[1])
        if not _is_int(op[2]) or op[2] < 0:
            raise TypeError("p index")
        if not _is_int(op[3]) or op[3] < 0:
            raise TypeError("p remove")
        if not isinstance(op[4], list):
            raise TypeError("p items")
    elif verb == "m":
        if len(op) != 3:
            raise TypeError("m arity")
        _assert_path_arg(op[1])
        _assert_permutation(op[2])
    else:
        raise TypeError(f"unknown op verb: {verb}")


def _resolve(root: JsonValue, path: Sequence[Any]) -> Any:
    value: Any = root
    for segment in path:
        if isinstance(segment, str):
            if not isinstance(value, dict):
                raise PathError(list(path))
            value = value[segment]
        else:
            if not isinstance(value, list) or segment >= len(value):
                raise PathError(list(path))
            value = value[segment]
    return value


def _assert_index_in_range(parent: list[Any], index: int) -> None:
    if index > len(parent):
        raise UnsafePathError(index)


def apply(target: JsonValue, ops: Iterable[Op]) -> JsonValue:
    """Apply decoded operations to a mutable value and return the possibly replaced root.

    Payloads carried by an operation are copied into the result, so the result never aliases the
    caller's operations and later operations can safely address what an earlier one inserted.
    """

    root: Any = target
    for op in ops:
        assert_valid_op(op)
        if op[0] == "r":
            root = copy_json(op[1])
            continue
        path = op[1]
        assert_safe_path(path)
        if op[0] == "p":
            parent = root if not path else _resolve(root, path)
            if not isinstance(parent, list):
                raise PathError(path)
            index, remove, items = op[2], op[3], op[4]
            parent[index:index + remove] = cast(list[Any], copy_json(items))
            continue
        if op[0] == "m":
            parent = root if not path else _resolve(root, path)
            if not isinstance(parent, list) or len(parent) != len(op[2]):
                raise PathError(path)
            previous = parent[:]
            for index, source in enumerate(op[2]):
                parent[index] = previous[source]
            continue

        parent = _resolve(root, path[:-1])
        key = path[-1]
        if isinstance(parent, list):
            if not _is_int(key):
                raise UnsafePathError(key)
            _assert_index_in_range(parent, key)
        if not isinstance(parent, (dict, list)):
            raise PathError(path)
        if op[0] == "s":
            parent[key] = copy_json(op[2])
        elif op[0] == "d":
            if isinstance(parent, list):
                if key >= len(parent):
                    raise PathError(path)
                del parent[key]
            else:
                parent.pop(key, None)
        elif op[0] == "a":
            current = parent[key] if isinstance(parent, list) or key in parent else None
            if not isinstance(current, str):
                raise PathError(path)
            parent[key] = current + op[2]
        elif op[0] == "t":
            current = parent[key] if isinstance(parent, list) or key in parent else None
            if not isinstance(current, str):
                raise PathError(path)
            parent[key] = current[op[2]:]
    return root


def apply_immutable(target: JsonValue, ops: Sequence[Op]) -> JsonValue:
    """Apply one batch without mutating ``target`` or its containers."""

    return apply(copy_json(target), ops)


def apply_immutable_batches(target: JsonValue, batches: Iterable[Sequence[Op]]) -> JsonValue:
    """Apply ordered batches without exposing or mutating an intermediate revision."""

    root = copy_json(target)
    for ops in batches:
        root = apply(root, ops)
    return root


__all__ = [
    "MAX_DELTA_OPERATIONS",
    "Op",
    "PathError",
    "RESERVED_SEGMENTS",
    "UnsafePathError",
    "apply",
    "apply_immutable",
    "apply_immutable_batches",
    "assert_safe_path",
    "assert_valid_op",
]

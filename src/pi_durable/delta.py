"""Chord-compatible JSON operation tuples and immutable revision tracking.

Operations are plain JSON arrays so they can cross storage, SQLite and JSONL unchanged:
``["r", value]``, ``["s", path, value]``, ``["d", path]``, ``["p", path, start, delete_count, items]``,
``["a", path, text]`` and ``["t", path, length]``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from .json import JsonError, copy_json, ensure_json

Path = Sequence[str | int]
Op = list[Any]


def _child(container: Any, segment: str | int, path: str) -> Any:
    if isinstance(segment, str):
        if not isinstance(container, dict):
            raise JsonError(f"{path} is not an object")
        if segment not in container:
            raise KeyError("/".join(map(str, path.split("."))))
        return container[segment]
    if not isinstance(container, list):
        raise JsonError(f"{path} is not an array")
    if not isinstance(segment, int) or isinstance(segment, bool):
        raise JsonError(f"{path} index must be an integer")
    if segment < 0 or segment >= len(container):
        raise IndexError(f"{path} index {segment} out of range")
    return container[segment]


def value_at(value: Any, path: Path) -> Any:
    current = value
    for index, segment in enumerate(path):
        current = _child(current, segment, f"$.{'.'.join(map(str, path[:index]))}")
    return current


def _parent_at(value: Any, path: Path) -> tuple[Any, str | int]:
    if not path:
        raise JsonError("the root has no parent")
    return value_at(value, path[:-1]), path[-1]


def _set_mutable(root: Any, path: Path, value: Any) -> None:
    if not path:
        raise JsonError("'s' cannot target the root")
    parent, segment = _parent_at(root, path)
    if isinstance(segment, str):
        if not isinstance(parent, dict):
            raise JsonError("set target is not an object")
        parent[segment] = value
        return
    if not isinstance(parent, list):
        raise JsonError("set target is not an array")
    if isinstance(segment, int) and 0 <= segment < len(parent):
        parent[segment] = value
        return
    raise IndexError(f"array index {segment} out of range")


def _delete_mutable(root: Any, path: Path) -> None:
    if not path:
        raise JsonError("'d' cannot target the root")
    parent, segment = _parent_at(root, path)
    if isinstance(segment, str):
        if not isinstance(parent, dict) or segment not in parent:
            raise KeyError(segment)
        del parent[segment]
        return
    if not isinstance(parent, list) or not isinstance(segment, int):
        raise JsonError("delete target is not an array index")
    del parent[segment]


def apply_op(value: Any, op: Op) -> Any:
    """Apply one operation immutably (the input is not mutated)."""
    kind = op[0] if op else None
    if kind == "r":
        if len(op) != 2:
            raise JsonError("malformed 'r' operation")
        return copy_json(op[1])
    result = copy_json(value)
    if kind == "s":
        _, path, new_value = op
        ensure_json(new_value)
        _set_mutable(result, list(path), copy_json(new_value))
        return result
    if kind == "d":
        _, path = op
        _delete_mutable(result, list(path))
        return result
    if kind == "p":
        _, path, start, delete_count, items = op
        target = value_at(result, list(path))
        if not isinstance(target, list):
            raise JsonError("'p' target is not an array")
        start = int(start)
        delete_count = int(delete_count)
        if start < 0 or start > len(target) or delete_count < 0 or start + delete_count > len(target):
            raise IndexError("splice range out of bounds")
        target[start : start + delete_count] = [copy_json(item) for item in items]
        return result
    if kind == "a":
        _, path, text = op
        target = value_at(result, list(path))
        if not isinstance(target, str) or not isinstance(text, str):
            raise JsonError("'a' appends to a string")
        _set_mutable(result, list(path), target + text)
        return result
    if kind == "t":
        _, path, length = op
        target = value_at(result, list(path))
        if not isinstance(target, str) or not isinstance(length, int) or length < 0:
            raise JsonError("'t' truncates a string")
        _set_mutable(result, list(path), target[:length])
        return result
    if kind == "m":
        # Reorder an array in place: new[i] = old[permutation[i]].
        _, path, permutation = op
        target = value_at(result, list(path))
        if not isinstance(target, list):
            raise JsonError("'m' target is not an array")
        if sorted(permutation) != list(range(len(target))):
            raise JsonError("'m' permutation is not a permutation")
        _set_mutable(result, list(path), [target[index] for index in permutation])
        return result
    raise JsonError(f"unknown operation {kind!r}")


def apply_ops(value: Any, ops: Iterable[Op]) -> Any:
    result = copy_json(value)
    for op in ops:
        result = apply_op(result, op)
    return result


def is_replace(op: Op) -> bool:
    return bool(op) and op[0] == "r"


def is_base(ops: Sequence[Op]) -> bool:
    return bool(ops) and ops[0][0] == "r"


@dataclass
class Prepared:
    """An immutable revision ready to be adopted after storage commits."""

    value: Any
    ops: list[Op]
    aborted: bool = False

    def abort(self) -> None:
        """Release a revision that storage did not adopt."""
        self.aborted = True


@dataclass
class Change:
    """One open draft change: its base value, its current value, and the emitted ops."""

    base: Any
    value: Any
    ops: list[Op] = field(default_factory=list)
    revoked: bool = False

    def _assert_open(self) -> None:
        if self.revoked:
            raise RuntimeError("Document draft is no longer usable (its transaction settled)")

    def set(self, path: Path, new_value: Any) -> None:
        self._assert_open()
        ensure_json(new_value)
        path = list(path)
        copied = copy_json(new_value)
        _set_mutable(self.value, path, copied)
        self.ops.append(["s", path, copied])

    def delete(self, path: Path) -> None:
        self._assert_open()
        path = list(path)
        _delete_mutable(self.value, path)
        self.ops.append(["d", path])

    def splice(self, path: Path, start: int, delete_count: int, items: Sequence[Any]) -> None:
        self._assert_open()
        path = list(path)
        copied = [copy_json(item) for item in items]
        target = value_at(self.value, path)
        if not isinstance(target, list):
            raise JsonError("splice target is not an array")
        if start < 0 or start > len(target) or delete_count < 0 or start + delete_count > len(target):
            raise IndexError("splice range out of bounds")
        target[start : start + delete_count] = copied
        self.ops.append(["p", path, start, delete_count, copied])

    def prepare(self) -> Prepared:
        """Seal the change and return the exact revision for adoption."""
        self._assert_open()
        self.revoked = True
        return Prepared(value=self.value, ops=list(self.ops))

    def abort(self) -> None:
        self.revoked = True


class Tracker:
    """Mutable in-memory mirror of one committed document incarnation."""

    def __init__(self, value: Any) -> None:
        self.value = copy_json(value)

    def begin_change(self) -> Change:
        return Change(base=self.value, value=copy_json(self.value))

    def adopt(self, prepared: Prepared) -> None:
        self.value = prepared.value


def track(value: Any) -> Tracker:
    ensure_json(value)
    return Tracker(value)


__all__ = [
    "Change",
    "Op",
    "Path",
    "Prepared",
    "Tracker",
    "apply_op",
    "apply_ops",
    "is_base",
    "is_replace",
    "track",
    "value_at",
]

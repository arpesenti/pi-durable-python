"""Mutable document drafts backed by Chord-compatible operation batches.

A draft is a transaction-scoped overlay over one committed document value. Reads return
transaction-scoped views; writes copy their placed value into the draft immediately and record
a replayable Chord operation batch that reproduces the final value. Unchanged subtrees keep
sharing the committed revision, so an empty batch publishes and writes nothing while a structural
no-op such as ``shift()`` followed by ``unshift(first)`` is still a durable change.

Drafts revoke when the transaction settles. Mutations and property/index reads check that seal.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar, cast

from ..delta import MAX_DELTA_OPERATIONS, Op
from ..json import JsonObject, JsonValue, copy_json

T = TypeVar("T")

_MISSING = object()


# ── JSON equality and operation emission ────────────────────────────────────


def _is_container(value: object) -> bool:
    return isinstance(value, (dict, list))


def _same_value(left: object, right: object) -> bool:
    """JSON equality with JavaScript's strict scalar identity."""

    if left is right:
        return True
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return all(_same_value(left[key], right[key]) for key in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            _same_value(a, b) for a, b in zip(left, right, strict=True)
        )
    return type(left) is type(right) and left == right


def _overlap(before: str, after: str) -> int:
    """Longest suffix of ``before`` that is a prefix of ``after``."""

    limit = min(len(before), len(after))
    for length in range(limit, 0, -1):
        if before.endswith(after[:length]):
            return length
    return 0


def _emit_changed(ops: list[Op], path: list[Any], before: object, after: JsonValue) -> None:
    """Emit one operation that turns ``before`` into ``after`` at ``path``."""

    if before is not _MISSING and _same_value(before, after):
        return
    if isinstance(before, str) and isinstance(after, str):
        if after.startswith(before):
            ops.append(["a", path, after[len(before) :]])
            return
        shared = _overlap(before, after)
        if shared > 0:
            ops.append(["t", path, len(before) - shared])
            if len(after) > shared:
                ops.append(["a", path, after[shared:]])
            return
    ops.append(["s", path, after])


# ── Overlay nodes ───────────────────────────────────────────────────────────


@dataclass(slots=True)
class _Slot:
    """One array position: a retained base element or a detached insertion."""

    base_index: int | None
    value: JsonValue
    inserted: bool = False
    override: bool = False
    node: _ObjectNode | _ArrayNode | None = None


class _ObjectNode:
    """Overlay over one JSON object."""

    __slots__ = (
        "_materialized",
        "base",
        "children",
        "deletes",
        "draft",
        "readded",
        "writes",
    )

    base: dict[str, Any]
    children: dict[str, _ObjectNode | _ArrayNode]
    deletes: dict[str, None]
    draft: DocumentDraft
    readded: dict[str, None]
    writes: dict[str, Any]
    _materialized: Any

    def __init__(self, draft: DocumentDraft, base: dict[str, Any]) -> None:
        self.draft = draft
        self.base = base
        self.writes = {}
        self.deletes = {}
        self.readded = {}
        self.children = {}
        self._materialized = _MISSING

    # Reads

    def has(self, key: str) -> bool:
        if key in self.deletes:
            return False
        return key in self.writes or key in self.base

    def get(self, key: str) -> object:
        if key in self.deletes:
            return _MISSING
        if key in self.writes:
            return self.writes[key]
        return self.base.get(key, _MISSING)

    def keys(self) -> list[str]:
        keys = [key for key in self.base if key not in self.deletes and key not in self.readded]
        for key in self.writes:
            if key not in keys:
                keys.append(key)
        return keys

    def wrap(self, key: str) -> Any:
        value = self.get(key)
        if not _is_container(value):
            return value
        child = self.children.get(key)
        if child is None or child.base is not value:
            child = self.draft._node_for(value)
            self.children[key] = child
        return self.draft._view(child)

    # Writes

    def set(self, key: str, value: Any) -> None:
        placed = self.draft._place(value)
        deleted = key in self.deletes
        current = self.get(key)
        if not deleted and current is not _MISSING:
            if not _is_container(current) and not _is_container(placed) and _same_value(current, placed):
                return
        if deleted:
            self.deletes.pop(key, None)
            if key in self.base:
                self.readded[key] = None
        self.writes[key] = placed
        self.children.pop(key, None)
        self._materialized = _MISSING

    def delete(self, key: str) -> None:
        if not self.has(key):
            return
        self.writes.pop(key, None)
        self.children.pop(key, None)
        self.readded.pop(key, None)
        if key in self.base:
            self.deletes[key] = None
        self._materialized = _MISSING

    # Materialization and emission

    def snapshot(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key in self.keys():
            if key in self.writes:
                value = self.writes[key]
                child = self.children.get(key)
                if child is not None:
                    value = child.snapshot()
            else:
                value = self.base[key]
                child = self.children.get(key)
                if child is not None:
                    value = child.snapshot()
            result[key] = value
        return result

    def materialize(self) -> dict[str, Any]:
        if self._materialized is not _MISSING:
            return cast(dict[str, Any], self._materialized)

        changed = bool(self.writes or self.deletes)
        result: dict[str, Any] = {}
        for key in self.keys():
            current = self.base.get(key)
            child = self.children.get(key)
            if key in self.writes:
                value = self.writes[key]
            else:
                value = current
            if child is not None:
                value = child.materialize()
            result[key] = value
            if value is not current:
                changed = True
        if not changed and list(result) == list(self.base):
            self._materialized = self.base
            return self.base
        self._materialized = result
        return result

    def emit(self, path: list[Any], ops: list[Op]) -> None:
        for key in self.readded:
            if key in self.base:
                ops.append(["d", [*path, key]])

        for key in self.writes:
            before = _MISSING if key in self.readded else self.base.get(key, _MISSING)
            after = self.writes[key]
            child = self.children.get(key)
            if child is not None:
                after = child.materialize()
            _emit_changed(ops, [*path, key], before, after)

        for key in self.deletes:
            if key in self.base and key not in self.writes:
                ops.append(["d", [*path, key]])

        for key, child in self.children.items():
            if key in self.writes or key in self.deletes:
                continue
            child.emit([*path, key], ops)


class _ArrayNode:
    """Overlay over one JSON array, retaining base positions and detached insertions."""

    __slots__ = ("_materialized", "base", "draft", "reordered", "slots", "structural")

    base: list[Any]
    draft: DocumentDraft
    reordered: bool
    slots: list[_Slot]
    structural: bool
    _materialized: Any

    def __init__(self, draft: DocumentDraft, base: list[Any]) -> None:
        self.draft = draft
        self.base = base
        self.slots = [_Slot(index, value) for index, value in enumerate(base)]
        self.structural = False
        self.reordered = False
        self._materialized = _MISSING

    # Reads

    def length(self) -> int:
        return len(self.slots)

    def value_at(self, index: int) -> Any:
        slot = self.slots[index]
        if slot.node is not None:
            return slot.node.snapshot()
        return slot.value

    def wrap_at(self, index: int) -> Any:
        slot = self.slots[index]
        value = slot.value
        if slot.node is None and _is_container(value):
            slot.node = self.draft._node_for(value)
        if slot.node is None:
            return value
        return self.draft._view(slot.node)

    def values(self) -> list[Any]:
        return [slot.value if slot.node is None else slot.node.snapshot() for slot in self.slots]

    def snapshot(self) -> list[Any]:
        return self.values()

    # Structural edits

    def _insert_slots(self, values: Iterable[Any]) -> list[_Slot]:
        slots: list[_Slot] = []
        for value in values:
            slots.append(_Slot(None, self.draft._place(value), inserted=True))
        return slots

    def splice(self, index: int, remove: int, values: Iterable[Any]) -> list[Any]:
        if index < 0:
            index = max(0, len(self.slots) + index)
        index = min(index, len(self.slots))
        removed = [self.value_at(position) for position in range(index, min(index + remove, len(self.slots)))]
        self.slots[index : index + remove] = self._insert_slots(values)
        self.structural = True
        self._materialized = _MISSING
        return removed

    def set(self, index: int, value: Any) -> None:
        placed = self.draft._place(value)
        slot = self.slots[index]
        if not slot.inserted:
            slot.override = True
        slot.value = placed
        slot.node = None
        self._materialized = _MISSING

    def delete(self, index: int) -> None:
        self.splice(index, 1, ())

    def reverse(self) -> None:
        self.slots.reverse()
        self.reordered = True
        self._materialized = _MISSING

    def sort(self, key: Any = None, reverse: bool = False) -> None:
        if key is None:
            self.slots.sort(key=lambda slot: self._sort_value(slot), reverse=reverse)
        else:
            self.slots.sort(key=lambda slot: key(self._sort_value(slot)), reverse=reverse)
        self.reordered = True
        self._materialized = _MISSING

    def _sort_value(self, slot: _Slot) -> Any:
        return slot.value if slot.node is None else slot.node.snapshot()

    # Materialization and emission

    def materialize(self) -> list[Any]:
        if self._materialized is not _MISSING:
            return cast(list[Any], self._materialized)
        if not self.structural and not self.reordered:
            unchanged = True
            result: list[Any] = []
            for position, slot in enumerate(self.slots):
                value = slot.value
                if slot.node is not None:
                    value = slot.node.materialize()
                if slot.override or value is not self.base[position]:
                    unchanged = False
                result.append(value)
            if unchanged:
                self._materialized = self.base
                return self.base
            self._materialized = result
            return result

        result = []
        for slot in self.slots:
            if slot.node is not None:
                result.append(slot.node.materialize())
            else:
                result.append(slot.value)
        self._materialized = result
        return result

    def emit(self, path: list[Any], ops: list[Op]) -> None:
        if self.structural:
            self._emit_structural(path, ops)
            return
        if self.reordered:
            permutation = [cast(int, slot.base_index) for slot in self.slots]
            if permutation != list(range(len(self.base))):
                ops.append(["m", path, permutation])
            self._emit_slot_changes(path, ops)
            return
        self._emit_slot_changes(path, ops)

    def _emit_structural(self, path: list[Any], ops: list[Op]) -> None:
        retained = [slot.base_index for slot in self.slots if not slot.inserted]
        retained_set = set(retained)

        removals: list[tuple[int, int]] = []
        end = len(self.base)
        while end > 0:
            if end - 1 in retained_set:
                end -= 1
                continue
            start = end - 1
            while start > 0 and start - 1 not in retained_set:
                start -= 1
            removals.append((start, end - start))
            end = start
        for start, remove in removals:
            ops.append(["p", path, start, remove, []])

        retained_values = [cast(int, index) for index in retained]
        if retained_values != sorted(retained_values):
            positions = {value: index for index, value in enumerate(sorted(retained_values))}
            ops.append(["m", path, [positions[value] for value in retained_values]])

        index = 0
        while index < len(self.slots):
            if not self.slots[index].inserted:
                index += 1
                continue
            start = index
            items: list[Any] = []
            while index < len(self.slots) and self.slots[index].inserted:
                slot = self.slots[index]
                items.append(slot.value if slot.node is None else slot.node.materialize())
                index += 1
            ops.append(["p", path, start, 0, items])

        self._emit_slot_changes(path, ops)

    def _emit_slot_changes(self, path: list[Any], ops: list[Op]) -> None:
        for index, slot in enumerate(self.slots):
            if slot.inserted:
                continue
            after: Any = slot.node.materialize() if slot.node is not None else slot.value
            if slot.override:
                base_index = cast(int, slot.base_index)
                _emit_changed(ops, [*path, index], self.base[base_index], after)
            elif slot.node is not None:
                slot.node.emit([*path, index], ops)


@dataclass(frozen=True, slots=True)
class PreparedDraft:
    """A sealed draft: the immutable next revision and its detached op batch."""

    value: JsonObject
    ops: list[Op] = field(default_factory=list)


# ── Public draft views ──────────────────────────────────────────────────────


class DraftObject:
    """A mutable object member of a document draft."""

    __slots__ = ("_draft", "_node")

    _draft: DocumentDraft
    _node: Any

    def __init__(self, draft: DocumentDraft, node: _ObjectNode) -> None:
        object.__setattr__(self, "_draft", draft)
        object.__setattr__(self, "_node", node)

    def _check(self) -> None:
        self._draft._check_open()

    def __getattr__(self, name: str) -> Any:
        self._check()
        if name.startswith("_"):
            raise AttributeError(name)
        value = self._node.get(name)
        if value is _MISSING:
            raise AttributeError(name)
        return self._node.wrap(name)

    def __setattr__(self, name: str, value: Any) -> None:
        self._check()
        if name in _INTERNAL:
            object.__setattr__(self, name, value)
            return
        self._node.set(name, value)

    def __delattr__(self, name: str) -> None:
        self._check()
        self._node.delete(name)

    def __getitem__(self, key: str) -> Any:
        self._check()
        value = self._node.get(key)
        if value is _MISSING:
            raise KeyError(key)
        return self._node.wrap(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._check()
        self._node.set(key, value)

    def __delitem__(self, key: str) -> None:
        self._check()
        self._node.delete(key)

    def __contains__(self, key: object) -> bool:
        self._check()
        return isinstance(key, str) and self._node.has(key)

    def __iter__(self) -> Iterator[str]:
        self._check()
        return iter(self._node.keys())

    def __len__(self) -> int:
        self._check()
        return len(self._node.keys())

    def get(self, key: str, default: Any = None) -> Any:
        self._check()
        value = self._node.get(key)
        if value is _MISSING:
            return default
        return self._node.wrap(key)

    def keys(self) -> Iterable[str]:
        self._check()
        return self._node.keys()

    def values(self) -> Iterable[Any]:
        self._check()
        return (self._node.wrap(key) for key in self._node.keys())

    def items(self) -> Iterable[tuple[str, Any]]:
        self._check()
        return ((key, self._node.wrap(key)) for key in self._node.keys())

    def pop(self, key: str, default: Any = None) -> Any:
        self._check()
        value = self._node.get(key)
        if value is _MISSING:
            return default
        wrapped = self._node.wrap(key)
        self._node.delete(key)
        return wrapped

    def update(self, values: Mapping[str, Any]) -> None:
        self._check()
        for key, value in values.items():
            self._node.set(key, value)

    def snapshot(self) -> dict[str, Any]:
        """A detached plain JSON object of this draft member."""

        self._check()
        return self._node.snapshot()

    def __repr__(self) -> str:
        return f"DraftObject({self._node.snapshot()!r})"


class DraftList:
    """A mutable array member of a document draft."""

    __slots__ = ("_draft", "_node")

    _draft: DocumentDraft
    _node: Any

    def __init__(self, draft: DocumentDraft, node: _ArrayNode) -> None:
        object.__setattr__(self, "_draft", draft)
        object.__setattr__(self, "_node", node)

    def _check(self) -> None:
        self._draft._check_open()

    def __len__(self) -> int:
        self._check()
        return self._node.length()

    def __iter__(self) -> Iterator[Any]:
        self._check()
        for index in range(self._node.length()):
            yield self._node.wrap_at(index)

    def __getitem__(self, index: int | slice) -> Any:
        self._check()
        if isinstance(index, slice):
            return [self._node.wrap_at(i) for i in range(*index.indices(self._node.length()))]
        return self._node.wrap_at(index)

    def __setitem__(self, index: int | slice, value: Any) -> None:
        self._check()
        if isinstance(index, slice):
            if not isinstance(value, Iterable):
                raise TypeError("can only assign an iterable to a list slice")
            start, stop, step = index.indices(self._node.length())
            if step == 1:
                self._node.splice(start, stop - start, value)
            else:
                targets = list(range(start, stop, step))
                values = list(value)
                if len(targets) != len(values):
                    raise ValueError(
                        f"attempt to assign sequence of size {len(values)} "
                        f"to extended slice of size {len(targets)}"
                    )
                for target, item in zip(targets, values, strict=True):
                    self._node.set(target, item)
        else:
            self._node.set(index, value)

    def __delitem__(self, index: int | slice) -> None:
        self._check()
        if isinstance(index, slice):
            start, stop, step = index.indices(self._node.length())
            if step == 1:
                self._node.splice(start, stop - start, ())
            else:
                for target in reversed(list(range(start, stop, step))):
                    self._node.delete(target)
        else:
            self._node.delete(index)

    def append(self, value: Any) -> None:
        self._check()
        self._node.splice(self._node.length(), 0, (value,))

    def push(self, *values: Any) -> None:
        self._check()
        self._node.splice(self._node.length(), 0, values)

    def extend(self, values: Iterable[Any]) -> None:
        self._check()
        self._node.splice(self._node.length(), 0, values)

    def insert(self, index: int, value: Any) -> None:
        self._check()
        self._node.splice(index, 0, (value,))

    def pop(self, index: int = -1) -> Any:
        self._check()
        return self._node.splice(index, 1, ())[0]

    def shift(self) -> Any:
        self._check()
        return self._node.splice(0, 1, ())[0]

    def unshift(self, *values: Any) -> None:
        self._check()
        self._node.splice(0, 0, values)

    def remove(self, value: Any) -> None:
        self._check()
        target = self._draft._plain(value)
        for index, current in enumerate(self._node.values()):
            if _same_value(current, target):
                self._node.splice(index, 1, ())
                return
        raise ValueError(f"{target!r} is not in list")

    def clear(self) -> None:
        self._check()
        self._node.splice(0, self._node.length(), ())

    def reverse(self) -> None:
        self._check()
        self._node.reverse()

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        self._check()
        self._node.sort(key=key, reverse=reverse)

    def index(self, value: Any, start: int = 0, stop: int | None = None) -> int:
        self._check()
        target = self._draft._plain(value)
        values = self._node.values()
        end = len(values) if stop is None else stop
        for index in range(start, min(end, len(values))):
            if _same_value(values[index], target):
                return index
        raise ValueError(f"{target!r} is not in list")

    def count(self, value: Any) -> int:
        self._check()
        target = self._draft._plain(value)
        return sum(1 for current in self._node.values() if _same_value(current, target))

    def snapshot(self) -> list[Any]:
        """A detached plain JSON array of this draft member."""

        self._check()
        return self._node.values()

    def __contains__(self, value: object) -> bool:
        self._check()
        target = self._draft._plain(value)
        return any(_same_value(current, target) for current in self._node.values())

    def __eq__(self, other: object) -> bool:
        return self._node.values() == (other._node.values() if isinstance(other, DraftList) else other)

    def __repr__(self) -> str:
        return f"DraftList({self._node.values()!r})"


_INTERNAL = frozenset(
    {
        "_check",
        "_draft",
        "_node",
        "_plain",
        "_place",
        "_sealed",
    }
)


class DocumentDraft(DraftObject):
    """Root draft for one document acquisition in a transaction."""

    __slots__ = ("_sealed",)

    def __init__(self, value: JsonObject) -> None:
        if not isinstance(value, dict):
            raise TypeError("Document draft roots must be JSON objects")
        object.__setattr__(self, "_draft", self)
        object.__setattr__(self, "_node", _ObjectNode(self, value))
        object.__setattr__(self, "_sealed", False)

    def _check_open(self) -> None:
        if self._sealed:
            raise RuntimeError("Transaction has settled")

    def _check(self) -> None:
        self._check_open()

    def _view(self, node: _ObjectNode | _ArrayNode) -> Any:
        if isinstance(node, _ObjectNode):
            return DraftObject(self, node)
        return DraftList(self, node)

    def _node_for(self, value: Any) -> _ObjectNode | _ArrayNode:
        if isinstance(value, dict):
            return _ObjectNode(self, value)
        return _ArrayNode(self, value)

    def _plain(self, value: Any) -> Any:
        if isinstance(value, DocumentDraft):
            return value.snapshot()
        if isinstance(value, DraftObject):
            return value._node.snapshot()
        if isinstance(value, DraftList):
            return value._node.values()
        return value

    def _place(self, value: Any) -> JsonValue:
        return copy_json(self._plain(value))

    def snapshot(self) -> dict[str, Any]:
        return self._node.snapshot()

    def _seal(self) -> None:
        object.__setattr__(self, "_sealed", True)

    def _prepare(self) -> PreparedDraft:
        """Seal the draft and return its detached value plus Chord-compatible operations."""

        self._check_open()
        self._seal()
        value = cast(JsonObject, self._node.materialize())
        ops: list[Op] = []
        self._node.emit([], ops)
        # Chord folds a batch larger than its operation budget into one root replacement.
        if len(ops) > MAX_DELTA_OPERATIONS:
            ops = [["r", value]]
        return PreparedDraft(value=value, ops=ops)


__all__ = ["DocumentDraft", "DraftList", "DraftObject", "PreparedDraft"]

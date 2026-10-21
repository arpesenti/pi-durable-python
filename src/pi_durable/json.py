"""Strict JSON values and detached copies.

Durable state is JSON: records are copied into strict JSON at every ownership boundary, and a
value that cannot be represented without losing information is rejected. Objects are plain
``dict`` values, arrays are plain ``list`` values, and numbers are finite. ``bool`` is accepted
before ``int`` because it is an ``int`` subclass in Python, but it remains a JSON boolean.

``FrozenDict`` and ``FrozenList`` are the immutable values produced at record-construction
boundaries. They remain JSON-encodable (they subclass ``dict`` and ``list``) and ``copy_json``
turns them back into ordinary mutable JSON trees.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, TypeAlias, TypeGuard

JsonValue: TypeAlias = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
"""A strict JSON value. ``Any`` is intentionally not part of the recursive alias."""

JsonObject: TypeAlias = dict[str, JsonValue]
"""The JSON object shape used as the root of every durable document."""


class FrozenDict(dict[str, Any]):
    """A JSON object that rejects mutation.

    It subclasses ``dict`` so the standard library's JSON encoder and storage layers can consume
    it without adapters. Deep freeze helpers produce frozen children as well, so a record cannot
    be changed through a retained reference.
    """

    __slots__ = ()

    def _immutable(self, *args: object, **kwargs: object) -> None:
        raise TypeError("Frozen JSON values are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __ior__ = _immutable  # type: ignore[assignment]
    clear = _immutable
    pop = _immutable
    popitem = _immutable  # type: ignore[assignment]
    setdefault = _immutable
    update = _immutable


class FrozenList(list[Any]):
    """A JSON array that rejects mutation while remaining a ``list``."""

    __slots__ = ()

    def _immutable(self, *args: object, **kwargs: object) -> None:
        raise TypeError("Frozen JSON values are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __iadd__ = _immutable  # type: ignore[assignment]
    __imul__ = _immutable  # type: ignore[assignment]
    append = _immutable
    clear = _immutable
    extend = _immutable
    insert = _immutable
    pop = _immutable
    remove = _immutable
    reverse = _immutable
    sort = _immutable


def _is_object(value: object) -> TypeGuard[Mapping[str, Any]]:
    return type(value) is dict or isinstance(value, FrozenDict)


def _is_array(value: object) -> TypeGuard[list[Any]]:
    return type(value) is list or isinstance(value, FrozenList)


def is_json_value(value: object) -> TypeGuard[JsonValue]:
    """Return whether ``value`` is strict JSON with plain, dense containers and no cycles."""

    return _check(value, set())


def _check(value: object, ancestors: set[int]) -> bool:
    if value is None or isinstance(value, bool) or isinstance(value, str):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if _is_array(value):
        marker = id(value)
        if marker in ancestors:
            return False
        ancestors.add(marker)
        try:
            return all(_check(item, ancestors) for item in value)
        finally:
            ancestors.remove(marker)
    if _is_object(value):
        marker = id(value)
        if marker in ancestors:
            return False
        ancestors.add(marker)
        try:
            for key, item in value.items():
                if not isinstance(key, str) or not _check(item, ancestors):
                    return False
            return True
        finally:
            ancestors.remove(marker)
    return False


def copy_json(value: object) -> JsonValue:
    """Return a detached strict-JSON copy of ``value``.

    No container or scalar object is shared with the input. Aliases in the input become separate
    copies in the output, and a cycle or any non-JSON value raises ``TypeError`` with a message
    mentioning strict JSON.
    """

    return _copy(value, set())


def _copy(value: object, ancestors: set[int]) -> JsonValue:
    if value is None or isinstance(value, bool) or isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        raise TypeError("Value contains a non-finite number and is not strict JSON")
    if _is_array(value):
        marker = id(value)
        if marker in ancestors:
            raise TypeError("Value contains cycles and is not strict JSON")
        ancestors.add(marker)
        try:
            return [_copy(item, ancestors) for item in value]
        finally:
            ancestors.remove(marker)
    if _is_object(value):
        marker = id(value)
        if marker in ancestors:
            raise TypeError("Value contains cycles and is not strict JSON")
        ancestors.add(marker)
        try:
            copied: dict[str, JsonValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("Value must contain strict JSON string keys")
                copied[str(key)] = _copy(item, ancestors)
            return copied
        finally:
            ancestors.remove(marker)
    raise TypeError(f"Value contains a non-JSON {type(value).__name__}; expected strict JSON")


def freeze_json(value: object) -> JsonValue:
    """Validate ``value`` as strict JSON and return an immutable, alias-free tree."""

    return _freeze(copy_json(value))


def _freeze(value: JsonValue) -> JsonValue:
    if isinstance(value, list):
        return FrozenList(_freeze(item) for item in value)
    if isinstance(value, dict):
        return FrozenDict({key: _freeze(item) for key, item in value.items()})
    return value




__all__ = [
    "FrozenDict",
    "FrozenList",
    "JsonObject",
    "JsonValue",
    "copy_json",
    "freeze_json",
    "is_json_value",
]

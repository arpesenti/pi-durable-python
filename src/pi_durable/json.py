"""Strict JSON values, copying, and small helpers shared by every layer."""

from __future__ import annotations

import math
from collections.abc import Mapping, MutableMapping, MutableSequence
from typing import Any

JsonValue = Any
JsonObject = dict[str, Any]


class JsonError(TypeError):
    """A value is not strict JSON."""


def _reject_constant(token: str) -> None:
    raise JsonError(f"{token} is not valid JSON")


def ensure_json(value: Any, *, path: str = "$") -> None:
    """Validate that ``value`` is strict JSON (objects have string keys, no NaN/Infinity)."""
    if value is None or isinstance(value, (bool, str)):
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise JsonError(f"{path} is not a finite number")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            ensure_json(item, path=f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise JsonError(f"{path} has a non-string key {key!r}")
            ensure_json(item, path=f"{path}.{key}")
        return
    raise JsonError(f"{path} is not JSON: {type(value).__name__}")


def copy_json(value: Any) -> Any:
    """Return a detached strict-JSON copy. Tuples become lists."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise JsonError("non-finite number is not JSON")
        return value
    if isinstance(value, (list, tuple)):
        return [copy_json(item) for item in value]
    if isinstance(value, Mapping):
        result: JsonObject = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise JsonError(f"non-string key {key!r} is not JSON")
            result[key] = copy_json(item)
        return result
    raise JsonError(f"{type(value).__name__} is not JSON")


def json_equal(left: Any, right: Any) -> bool:
    """Structural equality that treats tuples and lists alike."""
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(json_equal(a, b) for a, b in zip(left, right))
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return left.keys() == right.keys() and all(json_equal(left[k], right[k]) for k in left)
    if isinstance(left, bool) != isinstance(right, bool):
        return False
    return left == right


def omit_none(values: Mapping[str, Any]) -> JsonObject:
    """Build a JSON object, dropping ``None`` values (the persisted optional-field convention)."""
    return {key: copy_json(value) for key, value in values.items() if value is not None}


def error_message(error: BaseException | object) -> str:
    """A safe message for any thrown value."""
    if isinstance(error, BaseException):
        return str(error) or type(error).__name__
    try:
        return str(error)
    except Exception:  # noqa: BLE001 - must not fail while formatting a failure
        return repr(error)


def deep_freeze(value: Any) -> Any:
    """Recursively freeze a JSON value for sharing with listeners.

    Python has no Object.freeze; the contract is enforced by convention: values handed to
    listeners are detached copies, and storage returns detached copies on every read.
    """
    return value


def as_dict(value: Any, *, what: str) -> MutableMapping[str, Any]:
    if not isinstance(value, dict):
        raise JsonError(f"{what} must be a JSON object")
    return value


def as_list(value: Any, *, what: str) -> MutableSequence[Any]:
    if not isinstance(value, list):
        raise JsonError(f"{what} must be a JSON array")
    return value


__all__ = [
    "JsonError",
    "JsonObject",
    "JsonValue",
    "as_dict",
    "as_list",
    "copy_json",
    "deep_freeze",
    "ensure_json",
    "error_message",
    "json_equal",
    "omit_none",
]

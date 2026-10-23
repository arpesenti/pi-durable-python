"""Small readers shared by the coding tools."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


def field(value: Any, name: str, default: Any = None) -> Any:
    """A field of a mapping or object, as the wire and Python shapes vary."""

    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def field_either(value: Any, snake: str, camel: str, default: Any = None) -> Any:
    """A snake_case field, falling back to the upstream camelCase spelling."""

    found = field(value, snake)
    return field(value, camel, default) if found is None else found


def js_number_string(value: Any) -> str:
    """``String(number)`` for the numbers tool arguments may hold."""

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value.is_integer() and abs(value) < 1e21:
            return str(int(value))
        return repr(value)
    return str(value)


__all__ = ["field", "field_either", "js_number_string"]

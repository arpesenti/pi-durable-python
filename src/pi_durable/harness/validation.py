"""Tool-argument repair, coercion, and validation.

Port of the subset of pi-ai 1.3.0 ``utils/validation.ts`` the tool task needs. Upstream validates
against a TypeBox schema; the Python port validates against the JSON Schema the tool declares,
coercing primitives the way pi-ai does before the first validation, and removing optional
properties a provider sent as ``null``.
"""

from __future__ import annotations

import copy
import json
import math
from collections.abc import Mapping
from typing import Any

from ..json import copy_json

_JSON_TYPES = {"number", "integer", "boolean", "string", "null", "array", "object"}

_MESSAGES = {
    "number": "Expected number",
    "integer": "Expected integer",
    "boolean": "Expected boolean",
    "string": "Expected string",
    "null": "Expected null",
    "array": "Expected array",
    "object": "Expected object",
}


class ToolArgumentError(ValueError):
    """Invalid arguments for a tool call, with the message the model reads."""


def validate_tool_arguments(tool: Any, call: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a call's arguments against the tool's schema, returning the coerced arguments.

    Raises :class:`ToolArgumentError` with the formatted message pi-ai throws.
    """

    name = str(call.get("name") or getattr(tool, "name", ""))
    if call.get("argumentsError") is not None:
        raise ToolArgumentError(f'Invalid arguments for tool "{name}": {call["argumentsError"]}')
    schema = getattr(tool, "parameters", None) or {}
    original = call.get("arguments")
    args = copy.deepcopy(copy_json(original if original is not None else {}))
    if isinstance(args, dict):
        _normalize_optional_nulls(args, schema)
        args = _coerce(args, schema)
    if isinstance(args, dict) and _errors(args, schema) == []:
        return args
    errors = _errors(args, schema) or [("root", "Expected object")]
    detail = "\n".join(f"  - {path}: {message}" for path, message in errors)
    raise ToolArgumentError(
        f'Validation failed for tool "{name}":\n{detail}\n\n'
        f"Received arguments:\n{json.dumps(original, indent=2)}"
    )


# ── Validation ──────────────────────────────────────────────────────────────


def _path_of(instance_path: str) -> str:
    """Format a JSON Pointer as a dotted path; the root is ``root``."""

    path = instance_path.lstrip("/").replace("/", ".")
    return path or "root"


def _errors(value: Any, schema: Any, instance_path: str = "") -> list[tuple[str, str]]:
    if not isinstance(schema, Mapping):
        return []
    found: list[tuple[str, str]] = []
    if isinstance(schema.get("allOf"), list):
        for nested in schema["allOf"]:
            found.extend(_errors(value, nested, instance_path))
    for keyword in ("anyOf", "oneOf"):
        alternatives = schema.get(keyword)
        if not isinstance(alternatives, list):
            continue
        passed = sum(1 for nested in alternatives if not _errors(value, nested, instance_path))
        wanted = 1 if keyword == "oneOf" else 1
        if passed < wanted:
            found.append((_path_of(instance_path), "Expected union value"))
    if "const" in schema and value != schema["const"]:
        found.append((_path_of(instance_path), f"Expected {json.dumps(schema['const'])}"))
    if isinstance(schema.get("enum"), list) and value not in schema["enum"]:
        options = ", ".join(json.dumps(item) for item in schema["enum"])
        found.append((_path_of(instance_path), f"Expected one of {options}"))
    types = _schema_types(schema)
    if types and not any(_matches(value, item) for item in types):
        expected = " | ".join(types) if len(types) > 1 else types[0]
        found.append((_path_of(instance_path), _MESSAGES.get(types[0], f"Expected {expected}")))
        return found
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        found.extend(_number_errors(value, schema, instance_path))
    if isinstance(value, str):
        found.extend(_string_errors(value, schema, instance_path))
    if isinstance(value, list):
        found.extend(_array_errors(value, schema, instance_path))
    if isinstance(value, dict):
        found.extend(_object_errors(value, schema, instance_path))
    return found


def _schema_types(schema: Mapping[str, Any]) -> list[str]:
    declared = schema.get("type")
    if isinstance(declared, str):
        return [declared] if declared in _JSON_TYPES else []
    if isinstance(declared, list):
        return [item for item in declared if isinstance(item, str) and item in _JSON_TYPES]
    return []


def _matches(value: Any, json_type: str) -> bool:
    if json_type == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if json_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if json_type == "boolean":
        return isinstance(value, bool)
    if json_type == "string":
        return isinstance(value, str)
    if json_type == "null":
        return value is None
    if json_type == "array":
        return isinstance(value, list)
    if json_type == "object":
        return isinstance(value, dict)
    return False


def _number_errors(value: float, schema: Mapping[str, Any], path: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    minimum = schema.get("minimum")
    if isinstance(minimum, (int, float)) and value < minimum:
        found.append((_path_of(path), f"Expected number to be greater or equal to {minimum}"))
    maximum = schema.get("maximum")
    if isinstance(maximum, (int, float)) and value > maximum:
        found.append((_path_of(path), f"Expected number to be less or equal to {maximum}"))
    exclusive_minimum = schema.get("exclusiveMinimum")
    if isinstance(exclusive_minimum, (int, float)) and value <= exclusive_minimum:
        found.append((_path_of(path), f"Expected number to be greater than {exclusive_minimum}"))
    exclusive_maximum = schema.get("exclusiveMaximum")
    if isinstance(exclusive_maximum, (int, float)) and value >= exclusive_maximum:
        found.append((_path_of(path), f"Expected number to be less than {exclusive_maximum}"))
    multiple = schema.get("multipleOf")
    if isinstance(multiple, (int, float)) and multiple > 0:
        quotient = value / multiple
        if not math.isclose(quotient, round(quotient), abs_tol=1e-9):
            found.append((_path_of(path), f"Expected number to be a multiple of {multiple}"))
    return found


def _string_errors(value: str, schema: Mapping[str, Any], path: str) -> list[tuple[str, str]]:
    import re

    found: list[tuple[str, str]] = []
    min_length = schema.get("minLength")
    if isinstance(min_length, int) and len(value) < min_length:
        found.append((_path_of(path), f"Expected string to have a minimum length of {min_length}"))
    max_length = schema.get("maxLength")
    if isinstance(max_length, int) and len(value) > max_length:
        found.append((_path_of(path), f"Expected string to have a maximum length of {max_length}"))
    pattern = schema.get("pattern")
    if isinstance(pattern, str):
        try:
            matched = re.search(pattern, value) is not None
        except re.error:
            matched = True
        if not matched:
            found.append((_path_of(path), f"Expected string to match '{pattern}'"))
    return found


def _array_errors(value: list[Any], schema: Mapping[str, Any], path: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    min_items = schema.get("minItems")
    if isinstance(min_items, int) and len(value) < min_items:
        found.append((_path_of(path), f"Expected array to have at least {min_items} items"))
    max_items = schema.get("maxItems")
    if isinstance(max_items, int) and len(value) > max_items:
        found.append((_path_of(path), f"Expected array to have at most {max_items} items"))
    if schema.get("uniqueItems") is True:
        seen: list[Any] = []
        for item in value:
            if item in seen:
                found.append((_path_of(path), "Expected array items to be unique"))
                break
            seen.append(item)
    items = schema.get("items")
    if isinstance(items, list):
        for index, item_schema in enumerate(items):
            if index < len(value):
                found.extend(_errors(value[index], item_schema, f"{path}/{index}"))
    elif isinstance(items, Mapping):
        for index, item in enumerate(value):
            found.extend(_errors(item, items, f"{path}/{index}"))
    return found


def _object_errors(value: dict[str, Any], schema: Mapping[str, Any], path: str) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    properties = schema.get("properties")
    properties = properties if isinstance(properties, Mapping) else {}
    required = schema.get("required")
    for name in required if isinstance(required, list) else []:
        if name not in value:
            found.append((_path_of(f"{path}/{name}"), "Expected required property"))
    for name, item in value.items():
        item_path = f"{path}/{name}"
        if name in properties:
            found.extend(_errors(item, properties[name], item_path))
            continue
        additional = schema.get("additionalProperties")
        if additional is False:
            found.append((_path_of(item_path), "Expected not to have additional properties"))
        elif isinstance(additional, Mapping):
            found.extend(_errors(item, additional, item_path))
    min_properties = schema.get("minProperties")
    if isinstance(min_properties, int) and len(value) < min_properties:
        found.append((_path_of(path), f"Expected object to have at least {min_properties} properties"))
    max_properties = schema.get("maxProperties")
    if isinstance(max_properties, int) and len(value) > max_properties:
        found.append((_path_of(path), f"Expected object to have at most {max_properties} properties"))
    return found


# ── Coercion ────────────────────────────────────────────────────────────────


def _normalize_optional_nulls(value: Any, schema: Any) -> None:
    """Remove optional properties a provider sent as ``null``, as pi-ai does before validation."""

    if not isinstance(schema, Mapping):
        return
    if isinstance(value, list):
        items = schema.get("items")
        if isinstance(items, list):
            for index, item in enumerate(value):
                if index < len(items):
                    _normalize_optional_nulls(item, items[index])
        elif isinstance(items, Mapping):
            for item in value:
                _normalize_optional_nulls(item, items)
        return
    if not isinstance(value, dict):
        return
    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return
    required = set(schema.get("required") or [])
    for name, property_schema in properties.items():
        if name not in value:
            continue
        if (
            value[name] is None
            and name not in required
            and isinstance(property_schema, Mapping)
            and not isinstance(property_schema.get("$ref"), str)
            and _errors(None, property_schema) != []
        ):
            del value[name]
        else:
            _normalize_optional_nulls(value[name], property_schema)


def _coerce(value: Any, schema: Any) -> Any:
    if not isinstance(schema, Mapping):
        return value
    if isinstance(schema.get("allOf"), list):
        for nested in schema["allOf"]:
            value = _coerce(value, nested)
    for keyword in ("anyOf", "oneOf"):
        alternatives = schema.get(keyword)
        if isinstance(alternatives, list):
            value = _coerce_union(value, alternatives)
    types = _schema_types(schema)
    matches_union_member = len(types) > 1 and any(_matches(value, item) for item in types)
    if types and not matches_union_member:
        for json_type in types:
            candidate = _coerce_primitive(value, json_type)
            if candidate is not value:
                value = candidate
                break
    if "object" in types and isinstance(value, dict):
        _coerce_object(value, schema)
    if "array" in types and isinstance(value, list):
        _coerce_array(value, schema)
    return value


def _coerce_union(value: Any, alternatives: list[Any]) -> Any:
    if any(not _errors(value, schema) for schema in alternatives):
        return value
    for schema in alternatives:
        candidate = copy.deepcopy(value)
        coerced = _coerce(candidate, schema)
        if not _errors(coerced, schema):
            return coerced
    return value


def _coerce_object(value: dict[str, Any], schema: Mapping[str, Any]) -> None:
    properties = schema.get("properties")
    defined = set(properties or {}) if isinstance(properties, Mapping) else set()
    if isinstance(properties, Mapping):
        for name, property_schema in properties.items():
            if name in value:
                value[name] = _coerce(value[name], property_schema)
    additional = schema.get("additionalProperties")
    if isinstance(additional, Mapping):
        for name, item in value.items():
            if name not in defined:
                value[name] = _coerce(item, additional)


def _coerce_array(value: list[Any], schema: Mapping[str, Any]) -> None:
    items = schema.get("items")
    if isinstance(items, list):
        for index, item_schema in enumerate(items):
            if index < len(value):
                value[index] = _coerce(value[index], item_schema)
        return
    if isinstance(items, Mapping):
        for index, item in enumerate(value):
            value[index] = _coerce(item, items)


def _coerce_primitive(value: Any, json_type: str) -> Any:
    if json_type == "number":
        if value is None:
            return 0
        if isinstance(value, str) and value.strip() != "":
            try:
                parsed = float(value)
            except ValueError:
                return value
            if math.isfinite(parsed):
                return int(parsed) if parsed.is_integer() else parsed
        if isinstance(value, bool) and type(value) is bool:
            return 1 if value else 0
        return value
    if json_type == "integer":
        if value is None:
            return 0
        if isinstance(value, str) and value.strip() != "":
            try:
                parsed = float(value)
            except ValueError:
                return value
            if math.isfinite(parsed) and parsed.is_integer():
                return int(parsed)
        if isinstance(value, bool) and type(value) is bool:
            return 1 if value else 0
        return value
    if json_type == "boolean":
        if value is None:
            return False
        if value == "true":
            return True
        if value == "false":
            return False
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            if value == 1:
                return True
            if value == 0:
                return False
        return value
    if json_type == "string":
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return _js_string(value)
        return value
    if json_type == "null":
        if value == "" or (isinstance(value, int) and not isinstance(value, bool) and value == 0):
            return None
        if value is False:
            return None
        return value
    return value


def _js_string(value: int | float) -> str:
    """JavaScript ``String(number)``: integral floats lose their fraction, ``1e21`` is exponential."""

    if isinstance(value, int):
        return str(value)
    if value.is_integer() and abs(value) < 1e21:
        return str(int(value))
    return repr(value)


__all__ = ["ToolArgumentError", "validate_tool_arguments"]

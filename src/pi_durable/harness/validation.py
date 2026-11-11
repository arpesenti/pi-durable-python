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
from collections import OrderedDict
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


class ToolSchemaError(ValueError):
    """A tool's JSON Schema is refused where it is compiled, because a ``$ref`` names nothing.

    Upstream compiles a tool's schemas with ``@earendil-works/pi-schema``'s ``compile()``, whose
    resolver throws for a reference that names nothing (``packages/schema/src/resolver.ts``). Both
    of its call sites pass ``unresolvedRefs: "reject"``, which instead resolves the dangling place
    to ``false`` so no value passes there; the words for it are ``unresolved reference <ref>``
    (``packages/schema/src/explain.ts:108``). The port refuses the schema where it is compiled,
    which is where its own validator is built, because ``jsonschema`` would otherwise discover the
    dangling reference lazily while a value is checked. The message is upstream's own words.
    """


# ── Object schemas are closed unless they are opened ────────────────────────

#: The keywords that make a schema the builder's ``obj()`` shape: it names the properties a value
#: may carry, so an unmentioned property is one the tool never declared. Upstream's ``obj()`` always
#: writes ``properties`` (``packages/schema/src/builders.ts:194-198``) and ``map()`` always writes
#: ``additionalProperties`` (``builders.ts:137``), so neither builder ever leaves an object naming
#: neither; a raw schema that does constrains no property name, and stays open.
_OBJECT_SHAPE_KEYWORDS = ("properties", "required")


def is_closed_by_default(schema: Any) -> bool:
    """Whether ``schema`` is an object schema the port compiles closed.

    Upstream's ``obj()`` builder writes ``additionalProperties: false`` unless it is passed
    ``{open: true}`` -- ``...(open === true ? {} : { additionalProperties: false })``,
    ``@earendil-works/pi-schema`` ``packages/schema/src/builders.ts:197``. With no builder here, a
    plain mapping says the same thing by naming its properties and leaving ``additionalProperties``
    unwritten; ``additionalProperties: true`` is the port's spelling of ``{open: true}``.
    """

    if not isinstance(schema, Mapping) or "additionalProperties" in schema:
        return False
    return any(keyword in schema for keyword in _OBJECT_SHAPE_KEYWORDS)


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
        additional = schema.get("additionalProperties", False if is_closed_by_default(schema) else True)
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


_VALIDATOR_CACHE_SIZE = 64

_STRUCTURED_VALIDATORS: OrderedDict[int, tuple[Any, Any]] = OrderedDict()
"""Compiled validators keyed by the identity of the schema object they validate, least recently used
first, so a long-lived process with many short-lived tools does not accumulate them."""


#: The base URI references in a schema without an ``$id`` resolve against. The same one upstream's
#: validator uses, so a relative reference means the same thing in both.
DEFAULT_BASE = "https://json-schema.invalid/root.json"


def structured_output_path(pointer: str) -> str:
    """A JSON Pointer as the dotted path a diagnostic names: ``/edits/0/oldText`` -> ``edits.0.oldText``.

    Upstream's rule, transcribed from ``formatValidationPath`` in pi-ai 1.5.0
    ``src/utils/validation.ts:21``, which both of its validators use: the empty pointer is ``root``;
    otherwise drop the leading ``/``, split on ``/``, unescape ``~1`` as ``/`` and ``~0`` as ``~``,
    and join with ``.``.
    """

    if not pointer:
        return "root"
    segments = [
        segment.replace("~1", "/").replace("~0", "~") for segment in pointer.removeprefix("/").split("/")
    ]
    return ".".join(segments)


def invalid_structured_output_message(name: str, pointer: str, message: str | None) -> str:
    """Upstream's ``invalid_structured_output`` wording for a value that breaks its schema.

    Verbatim from ``src/harness/tool.ts:777-801`` ``structuredOutputError``, including the ``invalid``
    fallback when the validator gives the first error no message. Only the validator behind
    ``message`` is the port's own (``jsonschema``); see ``PARITY.md`` Allowed deviations.
    """

    return (
        f"Tool {name} returned structuredOutput that does not match its schema: "
        f"{structured_output_path(pointer)}: {message or 'invalid'}"
    )


def _each_schema(node: Any) -> Any:
    """Every schema ``node`` holds, itself first, walking only the keywords that hold subschemas.

    A ``$ref`` or ``additionalProperties`` inside ``enum``, ``const`` or an unknown keyword names
    nothing and closes nothing, so those values are not walked -- the discipline upstream's
    resolver keeps for the same reason.
    """

    if isinstance(node, list):
        for item in node:
            yield from _each_schema(item)
        return
    if not isinstance(node, Mapping):
        return
    yield node
    for key, value in list(node.items()):
        if key in _SCHEMA_KEYWORDS:
            yield from _each_schema(value)
        elif key in _SCHEMA_MAP_KEYWORDS and isinstance(value, Mapping):
            for item in value.values():
                yield from _each_schema(item)


_ESCAPE_TABLE = (("~", "~0"), ("/", "~1"))


def _escape_pointer(segment: str) -> str:
    for character, escape in _ESCAPE_TABLE:
        segment = segment.replace(character, escape)
    return segment


_REF_KEYWORDS = ("$ref", "$dynamicRef")

#: Keywords whose value is a schema, or an array of schemas.
_SCHEMA_KEYWORDS = frozenset(
    {
        "additionalItems",
        "additionalProperties",
        "allOf",
        "anyOf",
        "contains",
        "else",
        "if",
        "items",
        "not",
        "oneOf",
        "prefixItems",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)

#: Keywords whose value is an object naming schemas.
_SCHEMA_MAP_KEYWORDS = frozenset(
    {
        "$defs",
        "definitions",
        "dependencies",
        "dependentSchemas",
        "patternProperties",
        "properties",
    }
)


def _closed_copy(schema: Any) -> Any:
    """A copy of the schema with every closed-by-default object told so.

    The data spelling of upstream's builder default: ``obj()`` writes ``additionalProperties:
    false``, and omits the keyword for ``{open: true}`` (``builders.ts:197``). Here the authored
    mapping omits it to say closed and writes ``true`` to say opened, so closing is the only
    addition and an opened object is left exactly as authored.
    """

    closed = copy_json(schema)
    for node in _each_schema(closed):
        if isinstance(node, dict) and is_closed_by_default(node):
            node["additionalProperties"] = False
    return closed


def without_open_marker(schema: Any) -> Any:
    """A copy of ``schema`` with the port's ``{open: true}`` spelling dropped: what a host is told.

    ``additionalProperties: true`` says a tool author opened an object, which is an authoring option
    upstream keeps in ``obj()`` and never emits. It is not part of the wire contract either, so a
    declaration hands back the schema as it would have been written without it -- and an object
    carrying a real subschema for the keyword keeps it.
    """

    emitted = copy_json(schema)
    for node in _each_schema(emitted):
        if isinstance(node, dict) and node.get("additionalProperties") is True:
            del node["additionalProperties"]
    return emitted


def _refuse_unresolved_refs(schema: Any) -> None:
    """Resolve every reference now, and refuse the schema where one names nothing.

    ``jsonschema`` resolves lazily, deep inside ``iter_errors``, so a tool whose schema dangles
    would fail on a value rather than where it is compiled. Upstream's resolver throws for a
    reference that names nothing; the port throws :class:`ToolSchemaError` at the moment its own
    validator is built, in the words upstream's explainer uses for the broken reference.
    """

    try:
        from referencing import Registry
        from referencing.exceptions import Unresolvable
        from referencing.jsonschema import DRAFT202012
    except ImportError as error:  # pragma: no cover - arrives with the same extra as jsonschema
        raise ImportError(
            "JSON Schema validation requires the 'schema' extra: install pi-durable-python[schema]"
        ) from error
    base = schema.get("$id") if isinstance(schema, Mapping) else None
    base = base if isinstance(base, str) else DEFAULT_BASE
    resolver = Registry().with_resource(base, DRAFT202012.create_resource(schema)).resolver(base)
    for node in _each_schema(schema):
        for keyword in _REF_KEYWORDS:
            reference = node.get(keyword)
            if not isinstance(reference, str):
                continue
            try:
                resolver.lookup(reference)
            except Unresolvable as error:
                raise ToolSchemaError(f"unresolved reference {reference}") from error


def structured_output_validator(schema: Any) -> Any:
    """Compile a tool's ``structuredOutputSchema``: objects closed, references resolved.

    Needs the optional ``schema`` extra, imported only here so the core package has no runtime
    dependency. Raises :class:`ToolSchemaError` when a reference names nothing.
    """

    cached = _STRUCTURED_VALIDATORS.get(id(schema))
    if cached is not None and cached[0] is schema:
        _STRUCTURED_VALIDATORS.move_to_end(id(schema))
        return cached[1]
    try:
        from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
    except ImportError as error:
        # The core package keeps no runtime dependency; only a schema tool needs the extra.
        raise ImportError(
            "JSON Schema validation requires the 'schema' extra: "
            "install pi-durable-python[schema]"
        ) from error
    closed = _closed_copy(schema)
    _refuse_unresolved_refs(closed)
    validator = Draft202012Validator(closed)
    _STRUCTURED_VALIDATORS[id(schema)] = (schema, validator)
    _STRUCTURED_VALIDATORS.move_to_end(id(schema))
    while len(_STRUCTURED_VALIDATORS) > _VALIDATOR_CACHE_SIZE:
        _STRUCTURED_VALIDATORS.popitem(last=False)
    return validator


def structured_output_error(tool: Any, result: Mapping[str, Any]) -> str | None:
    """Why a result's ``structuredOutput`` breaks the tool's contract, or ``None`` when it keeps it.

    A tool that declares ``structured_output_schema`` must return a matching value, except on an
    error result, where it may omit it; a tool without one must not return one. The three messages
    are upstream's, read from ``src/harness/tool.ts:777-801`` ``structuredOutputError``; the first
    error the validator reports -- its words and the pointer it names -- is what the substituted
    validator produces (``PARITY.md`` Allowed deviations), formatted by upstream's own path rule.
    """

    name = str(getattr(tool, "name", ""))
    schema = getattr(tool, "structured_output_schema", None)
    present = "structuredOutput" in result
    value = result.get("structuredOutput")
    if schema is None:
        if not present:
            return None
        return f"Tool {name} returned structuredOutput but declares no structuredOutputSchema"
    if not present:
        if result.get("isError") is True:
            return None
        return f"Tool {name} returned no structuredOutput"
    validator = structured_output_validator(schema)
    first = next(iter(validator.iter_errors(value)), None)
    if first is None:
        return None
    pointer = "".join(f"/{_escape_pointer(str(item))}" for item in first.absolute_path)
    return invalid_structured_output_message(name, pointer, first.message)


__all__ = [
    "ToolArgumentError",
    "ToolSchemaError",
    "invalid_structured_output_message",
    "structured_output_error",
    "structured_output_path",
    "structured_output_validator",
    "validate_tool_arguments",
    "without_open_marker",
]

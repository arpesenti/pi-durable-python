"""The ``invalid_structured_output`` wording, asserted against upstream's strings.

Upstream builds the message in ``src/harness/tool.ts`` ``structuredOutputError`` (the cached v1.5.0
source, lines 777-801) and validates with ``@earendil-works/pi-schema``'s
``compile(schema, { formats, unresolvedRefs: "reject" })``. The three templates asserted here are
copied from that function verbatim, and ``{"count": "three"}`` against an object with a required
``count: num()`` is the case upstream's own ``test/harness-structured-output.test.ts`` asserts as
``"does not match its schema: count:"``.

What the port substitutes is the validator behind the trailing ``${path}: ${message}``:
``jsonschema``'s ``Draft202012Validator`` instead of pi-schema's ``Validator``, so the first error's
own words and the path it names are the substitute's -- upstream's explainer reports that value as
``count: expected number, got "three"`` (``packages/schema/src/explain.ts:75``) and an undeclared
property as ``extra: unknown field`` at the property's own path (``explain.ts:201``). Everything
else is upstream's and is asserted here as it is written there: the wrapper, the
JSON-pointer-to-dotted-path rule with its ``~1``/``~0`` unescaping, the ``root`` path for an empty
pointer, the ``invalid`` fallback for an error with no message, and the refusal of a schema whose
``$ref`` names nothing. Recorded in ``PARITY.md`` Allowed deviations.
"""

from __future__ import annotations

from typing import Any

import pytest

from pi_durable.harness.define import define_tool
from pi_durable.harness.validation import (
    ToolSchemaError,
    invalid_structured_output_message,
    structured_output_error,
    structured_output_validator,
)

# ── Upstream's wording, read from src/harness/tool.ts structuredOutputError ─

NO_SCHEMA = "Tool {name} returned structuredOutput but declares no structuredOutputSchema"
NO_VALUE = "Tool {name} returned no structuredOutput"
MISMATCH = "Tool {name} returned structuredOutput that does not match its schema: {path}: {message}"

COUNT = {"type": "object", "properties": {"count": {"type": "number"}}, "required": ["count"]}


def tool(name: str, structured_output_schema: Any = None) -> Any:
    return define_tool(
        {
            "name": name,
            "description": name,
            "parameters": {"type": "object", "properties": {}},
            "execute": lambda *_args: {},
            "structuredOutputSchema": structured_output_schema,
        }
    )


def broken(name: str, schema: Any, value: Any) -> str:
    """The mismatch message for ``value``, asserted to carry upstream's wrapper."""

    message = structured_output_error(tool(name, schema), {"structuredOutput": value})
    assert message is not None
    return message


# ── The three messages a broken result produces ─────────────────────────────


def test_a_value_with_no_declared_schema_gets_upstreams_message() -> None:
    message = structured_output_error(tool("plain"), {"structuredOutput": {"count": 3}})
    assert message == NO_SCHEMA.format(name="plain")


def test_a_schema_tool_with_no_value_gets_upstreams_message() -> None:
    message = structured_output_error(tool("counter", COUNT), {"output": []})
    assert message == NO_VALUE.format(name="counter")


def test_a_value_that_breaks_its_schema_gets_upstreams_wrapper() -> None:
    """Upstream's own case: ``{count: "three"}`` against ``obj({count: num()})``.

    Upstream words that first error ``count: expected number, got "three"``
    (``packages/schema/src/explain.ts:75``); the dotted path agrees, and only the words after it are
    the substituted validator's own.
    """

    message = broken("counter", COUNT, {"count": "three"})
    prefix = MISMATCH.format(name="counter", path="count", message="")
    assert message.startswith(prefix)
    # The wrapper and the dotted path are upstream's; the tail is what the substituted validator
    # says about the value, which is the one part the port cannot quote (PARITY.md).
    assert message[len(prefix) :] == "'three' is not of type 'number'"


def test_names_the_missing_value_only_for_a_success_and_keeps_an_error_results_value() -> None:
    counter = tool("counter", COUNT)
    assert structured_output_error(counter, {"output": [], "isError": True}) is None
    kept = {"structuredOutput": {"count": 0}, "isError": True}
    assert structured_output_error(counter, kept) is None
    assert structured_output_error(counter, {"output": [], "isError": False}) is not None


def test_says_root_when_the_value_itself_breaks_the_schema() -> None:
    message = broken("counter", COUNT, "three")
    assert message.startswith("Tool counter returned structuredOutput that does not match its schema: root: ")


def test_writes_a_nested_pointer_as_a_dotted_path_through_its_escapes() -> None:
    """``/edits/0/a~1b~0c`` becomes ``edits.0.a/b~c``: ``~1`` is ``/`` and ``~0`` is ``~``."""

    item = {"type": "object", "properties": {"a/b~c": {"type": "number"}}, "required": ["a/b~c"]}
    nested = {
        "type": "object",
        "properties": {"edits": {"type": "array", "items": item}},
        "required": ["edits"],
    }
    message = broken("editor", nested, {"edits": [{"a/b~c": "nope"}]})
    assert message.startswith(
        "Tool editor returned structuredOutput that does not match its schema: edits.0.a/b~c: "
    )


def test_falls_back_to_invalid_when_the_validator_has_no_message() -> None:
    assert invalid_structured_output_message("counter", "", None) == (
        "Tool counter returned structuredOutput that does not match its schema: root: invalid"
    )
    assert invalid_structured_output_message("counter", "/edits/0/oldText", "expected a string") == (
        "Tool counter returned structuredOutput that does not match its schema: edits.0.oldText:"
        " expected a string"
    )


# ── A schema is refused where it is compiled ────────────────────────────────


def test_refuses_an_unresolvable_reference_when_the_schema_is_compiled() -> None:
    """The refusal reads as upstream words the broken reference (``packages/schema/src/explain.ts:108``)."""

    dangling = {"type": "object", "properties": {"count": {"$ref": "#/definitions/Count"}}}
    with pytest.raises(ToolSchemaError, match="^unresolved reference #/definitions/Count$"):
        structured_output_validator(dangling)


def test_refuses_it_before_any_value_is_checked() -> None:
    """``jsonschema`` resolves lazily inside ``iter_errors``; the port must not wait for a value."""

    dangling = {"type": "object", "properties": {"count": {"$ref": "#/definitions/Count"}}}
    with pytest.raises(ToolSchemaError):
        structured_output_error(tool("counter", dangling), {"structuredOutput": {"count": 1}})


def test_compiles_a_schema_whose_reference_names_a_definition() -> None:
    resolved = {
        "type": "object",
        "properties": {"count": {"$ref": "#/definitions/Count"}},
        "required": ["count"],
        "definitions": {"Count": {"type": "number"}},
    }
    assert structured_output_validator(resolved) is not None
    assert structured_output_error(tool("counter", resolved), {"structuredOutput": {"count": 1}}) is None


# ── Object schemas are closed unless the tool opens them ────────────────────


def test_refuses_a_property_the_closed_schema_does_not_declare() -> None:
    """A host's object that never opened itself is closed, here through the structured-output path."""

    message = broken("counter", COUNT, {"count": 1, "extra": 2})

    # Upstream names the offending property at its own path -- ``extra: unknown field``,
    # ``packages/schema/src/explain.ts:201``. ``jsonschema`` reports the ``additionalProperties``
    # failure at the object it guards instead, so this diagnostic opens at ``root``: the path and
    # the words of the first error are both the substituted validator's (PARITY.md).
    assert message.startswith(
        "Tool counter returned structuredOutput that does not match its schema: root: "
    )
    assert "extra" in message


def test_accepts_any_property_a_tool_opened_its_schema_to() -> None:
    opened = {"type": "object", "properties": {"count": {"type": "number"}}, "additionalProperties": True}
    value = {"structuredOutput": {"count": 1, "extra": 2}}
    assert structured_output_error(tool("counter", opened), value) is None


def test_validates_an_extra_property_against_the_schema_it_opens_the_object_to() -> None:
    mapped = {
        "type": "object",
        "properties": {"count": {"type": "number"}},
        "additionalProperties": {"type": "string"},
    }
    ok = {"structuredOutput": {"count": 1, "extra": "x"}}
    assert structured_output_error(tool("counter", mapped), ok) is None
    message = broken("counter", mapped, {"count": 1, "extra": 2})
    prefix = "Tool counter returned structuredOutput that does not match its schema: extra: "
    assert message.startswith(prefix)

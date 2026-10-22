"""Tool-argument repair, coercion, and validation (pi-ai ``validateToolArguments``).

The tool task validates a call's arguments against the resolved implementation's schema before and
after ``beforeTool``. These cases pin the coercion, optional-null removal, and message format the
tool suite relies on.
"""

from __future__ import annotations

from typing import Any

import pytest

from pi_durable.harness.define import define_tool
from pi_durable.harness.validation import ToolArgumentError, validate_tool_arguments

ECHO = {
    "type": "object",
    "properties": {"text": {"type": "string"}},
}


def tool(parameters: Any, name: str = "echo") -> Any:
    return define_tool({"name": name, "parameters": parameters, "execute": lambda *_args: {}})


def test_coerces_primitives_the_way_pi_ai_does() -> None:
    assert validate_tool_arguments(tool(ECHO), {"name": "echo", "arguments": {"text": 1}}) == {
        "text": "1"
    }
    assert validate_tool_arguments(tool(ECHO), {"name": "echo", "arguments": {"text": True}}) == {
        "text": "true"
    }
    number = {"type": "object", "properties": {"n": {"type": "number"}}}
    assert validate_tool_arguments(tool(number), {"name": "echo", "arguments": {"n": "2.5"}}) == {
        "n": 2.5
    }
    integer = {"type": "object", "properties": {"n": {"type": "integer"}}}
    assert validate_tool_arguments(tool(integer), {"name": "echo", "arguments": {"n": "2"}}) == {
        "n": 2
    }
    boolean = {"type": "object", "properties": {"yes": {"type": "boolean"}}}
    assert validate_tool_arguments(tool(boolean), {"name": "echo", "arguments": {"yes": "true"}}) == {
        "yes": True
    }


def test_removes_optional_properties_a_provider_sent_as_null() -> None:
    assert validate_tool_arguments(tool(ECHO), {"name": "echo", "arguments": {"text": None}}) == {}


def test_keeps_a_required_property_sent_as_null_and_coerces_it() -> None:
    required = {**ECHO, "required": ["text"]}
    assert validate_tool_arguments(tool(required), {"name": "echo", "arguments": {"text": None}}) == {
        "text": ""
    }


def test_reports_a_type_mismatch_with_the_path_and_the_received_arguments() -> None:
    with pytest.raises(ToolArgumentError) as raised:
        validate_tool_arguments(
            tool(ECHO), {"name": "echo", "arguments": {"text": {"not": "a string"}}}
        )
    message = str(raised.value)
    assert message.startswith('Validation failed for tool "echo":')
    assert "  - text: Expected string" in message
    assert 'Received arguments:\n{\n  "text": {\n    "not": "a string"\n  }\n}' in message


def test_reports_a_missing_required_property_with_its_path() -> None:
    required = {**ECHO, "required": ["text"]}
    with pytest.raises(ToolArgumentError) as raised:
        validate_tool_arguments(tool(required), {"name": "echo", "arguments": {}})
    assert "  - text: Expected required property" in str(raised.value)


def test_validates_nested_objects_and_arrays_with_coercion() -> None:
    schema = {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {"n": {"type": "number"}},
                    "required": ["n"],
                },
            }
        },
    }
    assert validate_tool_arguments(
        tool(schema), {"name": "echo", "arguments": {"items": [{"n": "1"}, {"n": 2}]}}
    ) == {"items": [{"n": 1}, {"n": 2}]}
    with pytest.raises(ToolArgumentError) as raised:
        validate_tool_arguments(
            tool(schema), {"name": "echo", "arguments": {"items": [{"n": 1}, {}]}}
        )
    assert "  - items.1.n: Expected required property" in str(raised.value)


def test_rejects_unknown_properties_when_the_schema_forbids_them() -> None:
    closed = {**ECHO, "additionalProperties": False}
    with pytest.raises(ToolArgumentError) as raised:
        validate_tool_arguments(tool(closed), {"name": "echo", "arguments": {"other": 1}})
    assert "  - other: Expected not to have additional properties" in str(raised.value)


def test_accepts_a_union_member_and_an_enum() -> None:
    union = {
        "type": "object",
        "properties": {"value": {"anyOf": [{"type": "string"}, {"type": "number"}]}},
    }
    assert validate_tool_arguments(tool(union), {"name": "echo", "arguments": {"value": 2}}) == {
        "value": 2
    }
    enum = {"type": "object", "properties": {"mode": {"enum": ["fast", "slow"]}}}
    assert validate_tool_arguments(tool(enum), {"name": "echo", "arguments": {"mode": "fast"}}) == {
        "mode": "fast"
    }
    with pytest.raises(ToolArgumentError) as raised:
        validate_tool_arguments(tool(enum), {"name": "echo", "arguments": {"mode": "other"}})
    assert '  - mode: Expected one of "fast", "slow"' in str(raised.value)

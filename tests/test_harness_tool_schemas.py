"""Built-in tool schemas as upstream emits them, closed by default, opened where upstream opened.

Upstream authors every built-in schema with ``@earendil-works/pi-schema``'s builders (the cached
v1.5.0 source): ``obj(properties, options)`` emits ``{type: "object", properties, required?}`` and
writes ``additionalProperties: false`` only when it was *not* passed ``{open: true}`` --
``...(open === true ? {} : { additionalProperties: false })``, ``packages/schema/src/builders.ts:197``.
Every built-in passes ``{open: true}`` (``src/tools/bash.ts:10``), so none of them carries the keyword.
Each expected object below is what those builders emit for the call in ``src/tools/bash.ts``,
``src/tools/read.ts``, ``src/tools/write.ts``, ``src/tools/edit.ts`` and
``src/tool-search/tool-search.ts``, transcribed from them.

The port has no builders, so an authored object that leaves ``additionalProperties`` unwritten is
compiled closed -- ``obj()``'s default -- and ``additionalProperties: true`` is the port's data
spelling of ``{open: true}``. It is an authoring option rather than part of the interface a model
sees, so a declaration hands the schema back without it, which is why ``to_tool_declaration`` is
what the parameter schemas are compared through. Recorded in ``PARITY.md`` Allowed deviations.
"""

from __future__ import annotations

import json
from typing import Any, cast

from pi_durable.harness.define import define_tool
from pi_durable.harness.prompt import declarations_equal, plan_system_entries, to_tool_declaration
from pi_durable.harness.tool import prepare as repair_arguments
from pi_durable.harness.tool import validate
from pi_durable.harness.validation import (
    ToolArgumentError,
    validate_tool_arguments,
    without_open_marker,
)
from pi_durable.tool_search import TOOL_SEARCH_SCHEMA, create_tool_search_tool
from pi_durable.tools import create_bash_tool, create_edit_tool, create_read_tool, create_write_tool
from pi_durable.tools.bash import BASH_SCHEMA, SHELL_STRUCTURED_OUTPUT_SCHEMA
from pi_durable.tools.edit import EDIT_SCHEMA, REPLACE_EDIT_SCHEMA
from pi_durable.tools.read import (
    READ_IMAGE_BLOCK_SCHEMA,
    READ_SCHEMA,
    READ_STRUCTURED_OUTPUT_SCHEMA,
)
from pi_durable.tools.write import WRITE_SCHEMA

# ── What upstream's builders emit ───────────────────────────────────────────

BASH_PARAMETERS = {
    "type": "object",
    "properties": {
        "command": {"type": "string", "description": "Bash command to execute"},
        "timeout": {
            "type": "number",
            "description": "Timeout in seconds (optional, no default timeout)",
        },
    },
    "required": ["command"],
}

SHELL_STRUCTURED_OUTPUT = {
    "type": "object",
    "properties": {
        "output": {
            "type": "string",
            "description": "Combined stdout and stderr: the retained tail, as the model sees it",
        },
        "truncated": {"type": "boolean", "description": "Whether earlier output was dropped"},
        "fullOutputPath": {
            "type": "string",
            "description": "File with the complete output, when it was spilled",
        },
        "exitCode": {"type": "number"},
    },
    "required": ["output", "truncated", "exitCode"],
}

READ_PARAMETERS = {
    "type": "object",
    "properties": {
        "path": {"type": "string", "description": "Path to the file to read (relative or absolute)"},
        "offset": {"type": "number", "description": "Line number to start reading from (1-indexed)"},
        "limit": {"type": "number", "description": "Maximum number of lines to read"},
    },
    "required": ["path"],
}

READ_IMAGE_BLOCK = {
    "type": "object",
    "properties": {
        "type": {"const": "image"},
        "data": {"type": "string"},
        "mimeType": {"type": "string"},
    },
    "required": ["type", "data", "mimeType"],
}

READ_STRUCTURED_OUTPUT = {"anyOf": [{"type": "string"}, READ_IMAGE_BLOCK]}

WRITE_PARAMETERS = {
    "type": "object",
    "properties": {
        "path": {"type": "string", "description": "Path to the file to write (relative or absolute)"},
        "content": {"type": "string", "description": "Content to write to the file"},
    },
    "required": ["path", "content"],
}

REPLACE_EDIT = {
    "type": "object",
    "properties": {
        "oldText": {
            "type": "string",
            "description": (
                "Exact text for one targeted replacement. It must be unique in the original file "
                "and must not overlap with any other edits[].oldText in the same call."
            ),
        },
        "newText": {"type": "string", "description": "Replacement text for this targeted edit."},
    },
    "required": ["oldText", "newText"],
}

EDIT_PARAMETERS = {
    "type": "object",
    "properties": {
        "path": {"type": "string", "description": "Path to the file to edit (relative or absolute)"},
        "edits": {
            "type": "array",
            "items": REPLACE_EDIT,
            "description": (
                "One or more targeted replacements. Each edit is matched against the original "
                "file, not incrementally. Do not include overlapping or nested edits. If two "
                "changes touch the same block or nearby lines, merge them into one edit instead."
            ),
        },
    },
    "required": ["path", "edits"],
}

TOOL_SEARCH_PARAMETERS = {
    "type": "object",
    "properties": {
        "query": {"type": "string", "description": "Search query for deferred tools."},
        "limit": {
            "type": "number",
            "description": "Maximum number of tools to return. Defaults to 8.",
        },
    },
    "required": ["query"],
}


def host_tool(name: str, parameters: Any) -> Any:
    """A tool a host declares, with ``parameters`` as any JSON Schema object."""

    return define_tool(
        {
            "name": name,
            "description": name,
            "parameters": parameters,
            "execute": lambda args, *_rest: {"output": []},
        }
    )


def called(registration: Any, arguments: dict[str, Any]) -> dict[str, Any]:
    """The arguments the harness would hand ``registration``, validated against its schema."""

    return validate_tool_arguments(
        registration, {"name": registration.name, "arguments": arguments}
    )


def emitted_parameters(registration: Any) -> Any:
    """The parameters a host is told about: the authored schema minus the open spelling."""

    return to_tool_declaration(registration)["parameters"]


# ── The built-in schemas, as upstream emits them ────────────────────────────


def test_bash_declares_the_schema_upstream_builds() -> None:
    emitted = emitted_parameters(create_bash_tool())
    assert emitted == BASH_PARAMETERS
    # Key order too: `obj()` writes type, properties, required, and nothing for an open object.
    assert json.dumps(emitted) == json.dumps(BASH_PARAMETERS)
    # The authored schema is that object plus the port's spelling of `{open: true}`.
    assert BASH_SCHEMA == {**BASH_PARAMETERS, "additionalProperties": True}


def test_bash_reports_the_structured_output_upstream_declares() -> None:
    assert without_open_marker(SHELL_STRUCTURED_OUTPUT_SCHEMA) == SHELL_STRUCTURED_OUTPUT
    assert SHELL_STRUCTURED_OUTPUT_SCHEMA == {**SHELL_STRUCTURED_OUTPUT, "additionalProperties": True}


def test_read_declares_the_schemas_upstream_builds() -> None:
    registration = create_read_tool()
    assert emitted_parameters(registration) == READ_PARAMETERS
    # `union([str(), obj({type: lit("image"), data: str(), mimeType: str()}, {open: true})])`.
    assert without_open_marker(READ_STRUCTURED_OUTPUT_SCHEMA) == READ_STRUCTURED_OUTPUT
    # The image branch is the block itself, opened as upstream's `obj(..., {open: true})` is.
    assert READ_STRUCTURED_OUTPUT_SCHEMA["anyOf"][1] is READ_IMAGE_BLOCK_SCHEMA
    assert READ_SCHEMA == {**READ_PARAMETERS, "additionalProperties": True}
    assert registration.structured_output_schema is READ_STRUCTURED_OUTPUT_SCHEMA


def test_write_declares_the_schema_upstream_builds() -> None:
    assert emitted_parameters(create_write_tool()) == WRITE_PARAMETERS
    assert WRITE_SCHEMA == {**WRITE_PARAMETERS, "additionalProperties": True}


def test_edit_declares_the_schema_upstream_builds() -> None:
    assert emitted_parameters(create_edit_tool()) == EDIT_PARAMETERS
    assert without_open_marker(EDIT_SCHEMA) == EDIT_PARAMETERS
    # The array of edits holds the very schema an entry is authored by, opened as upstream's is.
    assert EDIT_SCHEMA["properties"]["edits"]["items"] is REPLACE_EDIT_SCHEMA
    assert without_open_marker(REPLACE_EDIT_SCHEMA) == REPLACE_EDIT


def test_tool_search_declares_the_schema_upstream_builds() -> None:
    assert emitted_parameters(create_tool_search_tool()) == TOOL_SEARCH_PARAMETERS
    assert TOOL_SEARCH_SCHEMA == {**TOOL_SEARCH_PARAMETERS, "additionalProperties": True}


# ── Closed unless the tool opened itself ────────────────────────────────────


def test_the_builtins_accept_an_argument_their_schema_does_not_name() -> None:
    """Upstream's own words for why (src/tools/bash.ts:10): "Open objects, as the tools accepted
    extra arguments when their schemas were TypeBox's." Every built-in passes `{open: true}`, so an
    argument its schema does not name stays accepted; a call carrying one is what this checks.
    """

    for registration, arguments in (
        (create_bash_tool(), {"command": "true"}),
        (create_read_tool(), {"path": "f"}),
        (create_write_tool(), {"path": "f", "content": ""}),
        (create_edit_tool(), {"path": "f", "edits": [{"oldText": "a", "newText": "b"}]}),
        (create_tool_search_tool(), {"query": "search"}),
    ):
        accepted = called(registration, {**arguments, "unexpected": 1})
        assert accepted == {**arguments, "unexpected": 1}


def test_a_host_tools_closed_object_refuses_an_argument_it_does_not_name() -> None:
    """The same call against an object that never opened itself is rejected, as ``obj()`` closes it."""

    closed = {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}
    host = host_tool("host", closed)
    assert called(host, {"path": "f"}) == {"path": "f"}
    try:
        called(host, {"path": "f", "unexpected": 1})
    except ToolArgumentError as error:
        assert "unexpected" in str(error)
        assert "Expected not to have additional properties" in str(error)
    else:
        raise AssertionError("a closed object schema accepted an undeclared argument")


def test_an_object_naming_no_properties_stays_open_as_a_record() -> None:
    """``{"type": "object"}`` names no property to close, so it takes what is sent.

    Neither builder upstream has produces it: ``obj()`` always writes ``properties``, even empty
    (``packages/schema/src/builders.ts:194-198``), and ``map()`` always writes
    ``additionalProperties`` (``builders.ts:137``).
    """

    assert called(host_tool("host", {"type": "object"}), {"anything": 1}) == {"anything": 1}


def test_an_opened_object_still_validates_the_property_it_opens_itself_to() -> None:
    """A schema under ``additionalProperties`` is a subschema, not the port's open spelling."""

    mapped = {
        "type": "object",
        "properties": {"count": {"type": "number"}},
        "required": ["count"],
        "additionalProperties": {"type": "number"},
    }
    host = host_tool("host", mapped)
    assert called(host, {"count": 1, "other": 2}) == {"count": 1, "other": 2}
    try:
        called(host, {"count": 1, "other": "two"})
    except ToolArgumentError as error:
        assert "other" in str(error)
    else:
        raise AssertionError("an extra property escaped the schema it was opened to")


# ── The translated tool-argument suite ──────────────────────────────────────
#
# Upstream's ``test/tool-args-typebox.test.ts`` declares one tool whose ``parameters`` is the
# plain-JSON shape a TypeBox-built schema has -- ``{path: TString, offset: TOptional<TInteger>,
# tags: TArray<TString>}`` with ``required: ["path", "tags"]`` -- and checks that ``execute`` and
# ``prepareArguments`` are handed the value that schema types. The types are what mypy checks here;
# what this run can check is that both of those seams receive the arguments the schema describes.

TYPEBOX_SHAPED_PARAMETERS = {
    "type": "object",
    "properties": {
        "path": {"~kind": "String", "type": "string"},
        "offset": {"~kind": "Integer", "type": "integer", "~optional": True},
        "tags": {"~kind": "Array", "type": "array", "items": {"~kind": "String", "type": "string"}},
    },
    "required": ["path", "tags"],
}

ARGS = {"path": "f", "offset": 2, "tags": ["a", "b"]}


def test_a_tools_arguments_are_the_value_its_schema_types_in_both_seams() -> None:
    seen: dict[str, Any] = {}

    def prepare_arguments(args: Any) -> Any:
        seen["prepared"] = args
        return args

    def execute(args: Any, *_rest: Any) -> dict[str, Any]:
        seen["executed"] = args
        return {"output": []}

    registration = define_tool(
        {
            "name": "read",
            "description": "Reads a file",
            "parameters": TYPEBOX_SHAPED_PARAMETERS,
            "prepareArguments": prepare_arguments,
            "execute": execute,
        }
    )
    call = {"name": "read", "arguments": ARGS}
    repaired = repair_arguments(registration, call["arguments"])
    assert repaired == ARGS
    validated = validate(registration, call, repaired)
    assert validated == ARGS
    # The harness hands execute what validate returned, which is the value the schema types.
    run = registration.execute
    assert run is not None
    assert run(validated, None, None) == {"output": []}
    assert seen == {"prepared": ARGS, "executed": ARGS}


def test_dropping_the_open_spelling_keeps_a_schema_the_object_was_opened_to() -> None:
    opened = {"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": True}
    assert without_open_marker(opened) == {"type": "object", "properties": {"a": {"type": "string"}}}
    mapped = {
        "type": "object",
        "properties": {"a": {"type": "string"}},
        "additionalProperties": {"type": "number"},
    }
    assert without_open_marker(mapped) == mapped


def test_the_open_spelling_never_reaches_a_stored_system_entry() -> None:
    """The marker is an authoring option, so nothing a host stores or replays may carry it.

    ``plan_system_entries`` writes the ``toolsAdded`` of the ``pi.system`` entry the harness persists,
    and the next request replays that entry through ``current_tools`` and compares it against the live
    registration with ``declarations_equal``. A marker left there would be round-tripped to the host
    and would churn the plan against every registration that opened itself.
    """

    view: Any = {"head": {"id": 1}, "entries": [], "contributions": [], "messages": []}
    registrations = [
        create_bash_tool(),
        create_read_tool(),
        create_write_tool(),
        create_edit_tool(),
        create_tool_search_tool(),
    ]
    drafts = plan_system_entries(view, {}, registrations, 7)
    assert drafts
    added = [
        declaration
        for draft in drafts
        for declaration in cast(dict[str, Any], draft["model"][0]).get("toolsAdded") or ()
    ]
    assert [declaration["name"] for declaration in added] == [each.name for each in registrations]
    for declaration, registration in zip(added, registrations, strict=True):
        assert "additionalProperties" not in json.dumps(declaration)
        assert declaration["parameters"] == emitted_parameters(registration)
        # The stored declaration replays as the interface of the live tool, so the plan holds still.
        assert declarations_equal(declaration, registration)

"""The ``edit`` coding tool."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, TypedDict, cast

from ..context import Context
from ..env import FileError
from ..harness.define import define_tool
from ..harness.types import ToolExecutionApi, ToolExecutionResult, ToolRegistration
from ..json import JsonValue
from .edit_diff import (
    apply_edits_to_normalized_content,
    detect_line_ending,
    generate_diff_string,
    generate_unified_patch,
    normalize_to_lf,
    restore_line_endings,
    strip_bom,
)
from .env import require_env
from .file_mutation_queue import with_file_mutation_queue
from .path_utils import resolve_tool_path


class ReplaceEdit(TypedDict):
    oldText: str
    newText: str


class EditToolInput(TypedDict):
    """Arguments of one ``edit`` call."""

    path: str
    edits: list[ReplaceEdit]


class EditToolDetails(TypedDict, total=False):
    """The two diff renderings, and where the first change lands in the new file."""

    diff: str
    patch: str
    firstChangedLine: int


REPLACE_EDIT_SCHEMA: dict[str, Any] = {
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

EDIT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "description": "Path to the file to edit (relative or absolute)",
        },
        "edits": {
            "type": "array",
            "items": REPLACE_EDIT_SCHEMA,
            "description": (
                "One or more targeted replacements. Each edit is matched against the original "
                "file, not incrementally. Do not include overlapping or nested edits. If two "
                "changes touch the same block or nearby lines, merge them into one edit instead."
            ),
        },
    },
    "required": ["path", "edits"],
}


def _is_single_edit(value: Any) -> bool:
    if not isinstance(value, Mapping) or isinstance(value, (list, tuple)):
        return False
    return isinstance(value.get("oldText"), str) and isinstance(value.get("newText"), str)


def prepare_edit_arguments(input: Any) -> Any:
    """Repair shapes models commonly send, without changing the call's arguments in place."""

    if not isinstance(input, Mapping) or isinstance(input, (list, tuple)):
        return input
    args: dict[str, Any] = dict(input)
    edits = args.get("edits")
    if isinstance(edits, str):
        try:
            parsed = json.loads(edits)
        except (ValueError, TypeError):
            parsed = None
        if isinstance(parsed, list):
            args["edits"] = parsed
        elif _is_single_edit(parsed):
            args["edits"] = [parsed]
    elif _is_single_edit(edits):
        args["edits"] = [edits]

    old_text = args.get("oldText")
    new_text = args.get("newText")
    if not isinstance(old_text, str) or not isinstance(new_text, str):
        return args
    merged = list(args["edits"]) if isinstance(args.get("edits"), list) else []
    merged.append({"oldText": old_text, "newText": new_text})
    rest = {key: value for key, value in args.items() if key not in ("oldText", "newText")}
    rest["edits"] = merged
    return rest


def _validate_edit_input(input: Mapping[str, Any]) -> tuple[str, list[dict[str, str]]]:
    edits = input.get("edits")
    if not isinstance(edits, list) or len(edits) == 0:
        raise ValueError("Edit tool input is invalid. edits must contain at least one replacement.")
    return str(input["path"]), [dict(edit) for edit in edits]


def _edit_access_error(path: str, error: FileError) -> RuntimeError:
    message = f"Could not edit file: {path}. Error code: {error.code}."
    raised = RuntimeError(message)
    raised.__cause__ = error
    return raised


def create_edit_tool() -> ToolRegistration:
    """Edit a single file using exact text replacement."""

    async def execute(
        args: Mapping[str, Any], api: ToolExecutionApi, context: Context
    ) -> ToolExecutionResult:
        path, edits = _validate_edit_input(args)
        env = require_env(api)
        absolute_path = await resolve_tool_path(env, path, context)

        async def mutate() -> ToolExecutionResult:
            if context.abort_signal is not None and context.abort_signal.aborted:
                raise RuntimeError("Operation aborted")
            info = await env.file_info(absolute_path, context)
            if not info.ok:
                raise _edit_access_error(path, info.error)
            if info.value.get("kind") not in ("file", "symlink"):
                raise RuntimeError(f"Could not edit file: {path}. Path is not a file.")

            read_result = await env.read_text_file(absolute_path, context)
            if not read_result.ok:
                raise _edit_access_error(path, read_result.error)
            if context.abort_signal is not None and context.abort_signal.aborted:
                raise RuntimeError("Operation aborted")

            bom, content = strip_bom(read_result.value)
            original_ending = detect_line_ending(content)
            normalized_content = normalize_to_lf(content)
            applied = apply_edits_to_normalized_content(normalized_content, edits, path)
            if context.abort_signal is not None and context.abort_signal.aborted:
                raise RuntimeError("Operation aborted")

            final_content = bom + restore_line_endings(applied.new_content, original_ending)
            write_result = await env.write_file(absolute_path, final_content, context)
            if not write_result.ok:
                raise _edit_access_error(path, write_result.error)
            if context.abort_signal is not None and context.abort_signal.aborted:
                raise RuntimeError("Operation aborted")

            diff = generate_diff_string(applied.base_content, applied.new_content)
            details: EditToolDetails = {
                "diff": diff.diff,
                "patch": generate_unified_patch(path, applied.base_content, applied.new_content),
            }
            if diff.first_changed_line is not None:
                details["firstChangedLine"] = diff.first_changed_line
            return {
                "output": [
                    {"type": "text", "text": f"Successfully replaced {len(edits)} block(s) in {path}."}
                ],
                "details": cast(JsonValue, details),
            }

        return await with_file_mutation_queue(env, absolute_path, mutate, context)

    return define_tool(
        {
            "name": "edit",
            "description": (
                "Edit a single file using exact text replacement. Every edits[].oldText must "
                "match a unique, non-overlapping region of the original file. If two changes "
                "affect the same block or nearby lines, merge them into one edit instead of "
                "emitting overlapping edits. Do not include large unchanged regions just to "
                "connect distant changes."
            ),
            "parameters": EDIT_SCHEMA,
            "prepare_arguments": prepare_edit_arguments,
            "execute": execute,
        }
    )


__all__ = [
    "EDIT_SCHEMA",
    "REPLACE_EDIT_SCHEMA",
    "EditToolDetails",
    "EditToolInput",
    "ReplaceEdit",
    "create_edit_tool",
    "prepare_edit_arguments",
]

"""The ``tool_search`` tool, a copy of the coding agent's.

The model searches the deferred tools it is not offered yet, and the matches are loaded with
``control.addTools``, so the next request offers them. Loading writes the conversation's
``modelTools`` (its ``add``), so loaded tools stay offered on that branch, also after a compaction.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any, TypedDict, cast

from ..context import Context
from ..harness.define import define_extension, define_tool
from ..harness.types import Extension, ToolExecutionApi, ToolExecutionResult, ToolRegistration
from ..json import JsonValue
from ..search import SearchDocument, rank_documents

TOOL_SEARCH_TOOL_NAME = "tool_search"
DEFAULT_TOOL_SEARCH_LIMIT = 8

#: Fixed, so it does not change while deferred tools are registered, for example as MCP servers
#: connect.
TOOL_SEARCH_DESCRIPTION = (
    "# Tool discovery\n\n"
    "Searches over deferred tool metadata with BM25 and exposes matching tools for the next model "
    "call.\n\n"
    "Some of the tools, such as tools of MCP servers, may not have been provided to you upfront, "
    f"and you should use this tool (`{TOOL_SEARCH_TOOL_NAME}`) to search for the required tools. "
    f"For MCP tool discovery, always use `{TOOL_SEARCH_TOOL_NAME}`."
)

TOOL_SEARCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "query": {
            "type": "string",
            "description": "Search query for deferred tools.",
        },
        "limit": {
            "type": "number",
            "description": (f"Maximum number of tools to return. Defaults to {DEFAULT_TOOL_SEARCH_LIMIT}."),
        },
    },
    "required": ["query"],
}


class ToolSearchToolDetails(TypedDict):
    """``tool_search`` result details: the tools it loaded."""

    loaded: list[str]


def _is_object(value: Any) -> bool:
    return isinstance(value, Mapping)


def _schema_text(schema: Any, parts: list[str]) -> None:
    """Schema descriptions and property names, recursively."""

    if not _is_object(schema):
        return
    description = schema.get("description")
    if isinstance(description, str):
        parts.append(description)
    properties = schema.get("properties")
    if _is_object(properties):
        for name, property_schema in properties.items():
            parts.append(name)
            _schema_text(property_schema, parts)
    _schema_text(schema.get("items"), parts)
    for key in ("anyOf", "oneOf", "allOf"):
        variants = schema.get(key)
        if isinstance(variants, (list, tuple)):
            for variant in variants:
                _schema_text(variant, parts)


def _search_text(tool: ToolRegistration) -> str:
    """A tool's name, the name with ``_`` as spaces, its description, and its parameters' text."""

    parts = [tool.name, tool.name.replace("_", " "), tool.description]
    _schema_text(tool.parameters, parts)
    return " ".join(part for part in parts if part.strip() != "")


def _positive_integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and (not math.isfinite(value) or not value.is_integer()):
        return None
    return int(value) if value > 0 else None


def _first_line(description: str) -> str:
    return description.strip().split("\n", 1)[0].rstrip("\r")


async def _execute(args: Mapping[str, Any], api: ToolExecutionApi, context: Context) -> ToolExecutionResult:
    query = args.get("query")
    if not isinstance(query, str) or query.strip() == "":
        raise ValueError("query must not be empty")
    raw_limit = args.get("limit")
    maximum = DEFAULT_TOOL_SEARCH_LIMIT if raw_limit is None else _positive_integer(raw_limit)
    if maximum is None:
        raise ValueError("limit must be a positive integer")
    candidates = list((await api.agent(context)).deferred)
    documents: list[SearchDocument] = [{"name": tool.name, "text": _search_text(tool)} for tool in candidates]
    loaded = rank_documents(query, documents, maximum)
    if not loaded:
        text = "No matching tools found."
    else:
        by_name = {tool.name: tool for tool in candidates}
        lines = [f"- {name}: {_first_line(by_name[name].description)}" for name in loaded]
        text = (
            f"Loaded {len(loaded)} tool{'' if len(loaded) == 1 else 's'}. "
            "They are available from your next call:\n" + "\n".join(lines)
        )
    details: ToolSearchToolDetails = {"loaded": loaded}
    result: ToolExecutionResult = {
        "output": [{"type": "text", "text": text}],
        "details": cast(JsonValue, details),
    }
    if loaded:
        result["control"] = {"addTools": loaded}
    return result


def create_tool_search_tool() -> ToolRegistration:
    """The ``tool_search`` tool: rank ``Agent.deferred`` with BM25 and load the matches."""

    return define_tool(
        {
            "name": TOOL_SEARCH_TOOL_NAME,
            "description": TOOL_SEARCH_DESCRIPTION,
            "parameters": TOOL_SEARCH_SCHEMA,
            # Searching changes what the model is offered; scripts have their own `searchTools()`.
            "callers": ["model"],
            "replay": "safe",
            "execute": _execute,
        }
    )


def create_tool_search_extension() -> Extension:
    """The ``tool_search`` extension: select it where the model should find deferred tools."""

    return define_extension({"name": "tool-search", "tools": [create_tool_search_tool()]})


__all__ = [
    "DEFAULT_TOOL_SEARCH_LIMIT",
    "TOOL_SEARCH_TOOL_NAME",
    "ToolSearchToolDetails",
    "create_tool_search_extension",
    "create_tool_search_tool",
]

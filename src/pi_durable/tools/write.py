"""The ``write`` coding tool."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypedDict

from ..context import Context
from ..env import get_or_throw
from ..harness.define import define_tool
from ..harness.types import ToolExecutionApi, ToolExecutionResult, ToolRegistration
from .env import require_env
from .file_mutation_queue import with_file_mutation_queue
from .path_utils import resolve_tool_path


class WriteToolInput(TypedDict):
    """Arguments of one ``write`` call."""

    path: str
    content: str


WRITE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "description": "Path to the file to write (relative or absolute)",
        },
        "content": {"type": "string", "description": "Content to write to the file"},
    },
    "required": ["path", "content"],
}


def create_write_tool() -> ToolRegistration:
    """Write content to a file, creating it and its parent directories when needed."""

    async def execute(
        args: Mapping[str, Any], api: ToolExecutionApi, context: Context
    ) -> ToolExecutionResult:
        path = str(args["path"])
        content = str(args["content"])
        env = require_env(api)
        absolute_path = await resolve_tool_path(env, path, context)

        async def mutate() -> ToolExecutionResult:
            if context.abort_signal is not None and context.abort_signal.aborted:
                raise RuntimeError("Operation aborted")
            get_or_throw(await env.write_file(absolute_path, content, context))
            if context.abort_signal is not None and context.abort_signal.aborted:
                raise RuntimeError("Operation aborted")
            return {"output": [{"type": "text", "text": f"Successfully wrote to {path}"}]}

        return await with_file_mutation_queue(env, absolute_path, mutate, context)

    return define_tool(
        {
            "name": "write",
            "description": (
                "Write content to a file. Creates the file if it doesn't exist, overwrites if it "
                "does. Automatically creates parent directories."
            ),
            "parameters": WRITE_SCHEMA,
            "execute": execute,
        }
    )


__all__ = ["WRITE_SCHEMA", "WriteToolInput", "create_write_tool"]

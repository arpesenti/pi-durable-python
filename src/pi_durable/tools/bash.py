"""The ``bash`` coding tool.

Port of the v1.3.0 ``src/tools/bash.ts`` without the excluded PowerShell tool. The command runs
through the call's execution environment, streams into the Harness's retained output window, spills
oversized output, and reports a nonzero exit as an error result.
"""

from __future__ import annotations

import inspect
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any, TypedDict, cast

from ..context import Context
from ..env import ExecutionError, Result, ShellExecOptions, ShellExecResult
from ..harness.define import define_tool
from ..harness.types import ToolDiagnostic, ToolExecutionApi, ToolExecutionResult, ToolRegistration
from ..truncate import DEFAULT_MAX_BYTES, DEFAULT_MAX_LINES
from ._values import field, js_number_string
from .env import require_env

MAX_TIMEOUT_SECONDS = 2_147_483_647 / 1000


class BashToolInput(TypedDict):
    """Arguments of one ``bash`` call."""

    command: str
    timeout: float


@dataclass(slots=True)
class BashExecution:
    """A command about to run, which ``prepare`` may change."""

    command: str = ""
    cwd: str = ""
    env: dict[str, str] = dataclass_field(default_factory=dict)
    inherit_env: bool = True


BashPrepare = Callable[[BashExecution, ToolExecutionApi, Context], Awaitable[None] | None]


class BashToolOptions(TypedDict, total=False):
    """Options of :func:`create_bash_tool`."""

    command_prefix: str
    prepare: BashPrepare


BASH_SCHEMA: dict[str, Any] = {
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

SHELL_STRUCTURED_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "output": {
            "type": "string",
            "description": "Combined stdout and stderr: the retained tail, as the model sees it",
        },
        "truncated": {
            "type": "boolean",
            "description": "Whether earlier output was dropped",
        },
        "fullOutputPath": {
            "type": "string",
            "description": "File with the complete output, when it was spilled",
        },
        "exitCode": {"type": "number"},
    },
    "required": ["output", "truncated", "exitCode"],
}


def _validate_timeout(timeout: float | None) -> None:
    if timeout is None:
        return
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("Invalid timeout: must be a finite number of seconds")
    if timeout > MAX_TIMEOUT_SECONDS:
        raise ValueError(f"Invalid timeout: maximum is {js_number_string(MAX_TIMEOUT_SECONDS)} seconds")


async def _prepare_execution(
    command: str,
    options: Mapping[str, Any] | None,
    api: ToolExecutionApi,
    context: Context,
) -> BashExecution:
    values = dict(options or {})
    prefix = values.get("command_prefix", values.get("commandPrefix"))
    execution = BashExecution(
        command=f"{prefix}\n{command}" if prefix else command,
        cwd=require_env(api).cwd,
        env={},
        inherit_env=True,
    )
    prepare = values.get("prepare")
    if prepare is not None:
        prepared = prepare(execution, api, context)
        if inspect.isawaitable(prepared):
            await prepared
    return execution


async def _run_command(
    commands: Sequence[str | Sequence[str]],
    execution: BashExecution,
    timeout: float | None,
    api: ToolExecutionApi,
    context: Context,
) -> ToolExecutionResult:
    env = require_env(api)
    result: Result[ShellExecResult, ExecutionError] | None = None
    for command in commands:
        options: dict[str, Any] = {
            "cwd": execution.cwd,
            "env": dict(execution.env),
            "inherit_env": execution.inherit_env,
            "on_output": lambda text, _context, info: api.output(text, field(info, "skipped")),
            "spill": {"after_bytes": DEFAULT_MAX_BYTES, "after_lines": DEFAULT_MAX_LINES},
        }
        if timeout is not None:
            options["timeout"] = timeout
        # An environment may then omit output outside the retained tail and report the omission.
        output_window = getattr(api, "output_window", None)
        if output_window is not None:
            options["window"] = output_window
        result = await env.exec(command, cast(ShellExecOptions, options), context)
        # A program that could not start produced no output; the next form may.
        if result.ok or result.error.code != "spawn_error":
            break
    if result is None:
        raise RuntimeError("No command to run")
    spill_path = (
        field(result.value, "spillPath", field(result.value, "spill_path"))
        if result.ok
        else field(result.error, "spill_path", field(result.error, "spillPath"))
    )
    if spill_path is not None:
        api.diagnostic(
            ToolDiagnostic(severity="info", code="full_output", message=f"Full output: {spill_path}")
        )
    if not result.ok:
        error = result.error
        if error.code == "aborted" and context.abort_signal is not None and context.abort_signal.aborted:
            raise error
        if error.code == "timeout":
            raise RuntimeError(f"Command timed out after {js_number_string(timeout)} seconds")
        if error.code == "aborted":
            raise RuntimeError("Command aborted")
        raise error
    exit_code = result.value.get("exitCode")
    retained = api.retained_output()
    structured: dict[str, Any] = {
        "output": field(retained, "text", ""),
        "truncated": bool(field(retained, "truncated", False)),
        "exitCode": exit_code,
    }
    if spill_path is not None:
        structured["fullOutputPath"] = spill_path
    if exit_code == 0:
        return {"structuredOutput": structured}
    return {
        "structuredOutput": structured,
        "isError": True,
        "diagnostics": [
            ToolDiagnostic(
                severity="error",
                code="exit_code",
                message=f"Command exited with code {js_number_string(exit_code)}",
            )
        ],
    }


def create_bash_tool(
    options: BashToolOptions | Mapping[str, Any] | None = None,
    *,
    command_prefix: str | None = None,
    prepare: BashPrepare | None = None,
) -> ToolRegistration:
    """Run a command through the environment's shell.

    Its output streams to ``api.output()``, where the Harness keeps the tail within the default
    limits; the result content is that retained output. Output beyond the limits is spilled to a
    file whose path is reported as a diagnostic. A nonzero exit is an error result with the exit
    code; a timeout throws.
    """

    values: dict[str, Any] = dict(options or {})
    if command_prefix is not None:
        values["command_prefix"] = command_prefix
    if prepare is not None:
        values["prepare"] = prepare

    async def execute(
        args: Mapping[str, Any], api: ToolExecutionApi, context: Context
    ) -> ToolExecutionResult:
        timeout = args.get("timeout")
        _validate_timeout(timeout)
        execution = await _prepare_execution(str(args["command"]), values, api, context)
        return await _run_command([execution.command], execution, timeout, api, context)

    return define_tool(
        {
            "name": "bash",
            "description": (
                "Execute a bash command in the current working directory. Returns combined stdout "
                "and stderr. Output is truncated to last "
                f"{DEFAULT_MAX_LINES} lines or {DEFAULT_MAX_BYTES // 1024}KB (whichever is hit "
                "first). If truncated, full output is saved to a temp file. Optionally provide a "
                "timeout in seconds."
            ),
            "parameters": BASH_SCHEMA,
            "structured_output_schema": SHELL_STRUCTURED_OUTPUT_SCHEMA,
            "output_limits": {"retain": "tail"},
            "execute": execute,
        }
    )


__all__ = [
    "BASH_SCHEMA",
    "MAX_TIMEOUT_SECONDS",
    "SHELL_STRUCTURED_OUTPUT_SCHEMA",
    "BashExecution",
    "BashPrepare",
    "BashToolInput",
    "BashToolOptions",
    "create_bash_tool",
]

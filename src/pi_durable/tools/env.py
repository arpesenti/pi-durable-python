"""Execution-environment access shared by the coding tools."""

from __future__ import annotations

from typing import Any

from ..env import ExecutionEnv


def require_env(api: Any) -> ExecutionEnv:
    """The call's execution environment; a tool without one fails with an ordinary error."""

    env = getattr(api, "env", None)
    if env is None:
        raise RuntimeError("No execution environment is configured")
    return env


__all__ = ["require_env"]

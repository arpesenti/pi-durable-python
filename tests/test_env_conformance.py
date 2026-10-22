"""Translated v1.3.0 ``env-node-conformance`` cases against ``PosixExecutionEnv``.

The runner-independent suite is driven through the same public seam the upstream Node backend
uses; polling watches are the portable backend's native mode.
"""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest
from storage_support import expect

from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.env import ExecutionEnv
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.testing import create_env_conformance, create_expect_assertions


async def _with_env(use: Callable[[ExecutionEnv], Awaitable[None]]) -> None:
    cwd = tempfile.mkdtemp(prefix="pi-durable-env-conformance-")
    env = PosixExecutionEnv(cwd=cwd, watch={"poll_interval_ms": 50})
    try:
        await use(env)
    finally:
        await env.cleanup(BACKGROUND_CONTEXT)
        shutil.rmtree(cwd, ignore_errors=True)


CASES = create_env_conformance(
    {
        "assertions": create_expect_assertions(expect),
        "with_env": _with_env,
        "shell": ["sh", "-c"],
        "list_files": True,
    }
)


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
async def test_posix_execution_env_conformance(case) -> None:
    await case.run()


class _RecordingRunner:
    def expect(self, actual: object, message: str | None = None):
        return expect(actual, message)

    def __init__(self) -> None:
        self.suite_name = ""
        self.cases: list[tuple[str, object, int | None]] = []

    def describe(self, name: str, suite) -> None:
        self.suite_name = name
        suite()

    def it(self, name: str, test, timeout_ms: int | None = None) -> None:
        self.cases.append((name, test, timeout_ms))


def test_register_env_conformance_registers_every_case() -> None:
    from pi_durable.testing import register_env_conformance

    runner = _RecordingRunner()
    register_env_conformance(
        cast(Any, runner), "PosixExecutionEnv conformance", _with_env, {"list_files": True}
    )

    assert runner.suite_name == "PosixExecutionEnv conformance"
    assert len(runner.cases) == len(CASES)
    assert [name for name, _, _ in runner.cases] == [case.name for case in CASES]
    assert all(callable(test) for _, test, _ in runner.cases)

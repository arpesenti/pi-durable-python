"""Register the runner-independent conformance cases with a test runner."""

from __future__ import annotations

from typing import Any, cast

from .assertions import create_expect_assertions
from .env_conformance import create_env_conformance
from .storage_conformance import create_storage_conformance
from .types import (
    EnvConformanceOptions,
    EnvConformanceProvider,
    StorageConformanceProvider,
    StorageConformanceRunner,
)


def register_storage_conformance(
    runner: StorageConformanceRunner,
    name: str,
    with_storage: StorageConformanceProvider,
) -> None:
    """Register the runner-independent cases with a Vitest/Jest-compatible test runner."""

    cases = create_storage_conformance(
        {
            "assertions": create_expect_assertions(runner.expect),
            "with_storage": with_storage,
        }
    )

    def suite() -> None:
        for test_case in cases:
            runner.it(test_case.name, test_case.run)

    runner.describe(name, suite)


def register_env_conformance(
    runner: StorageConformanceRunner,
    name: str,
    with_env: EnvConformanceProvider,
    options: dict[str, Any] | None = None,
) -> None:
    """Register the runner-independent environment cases with a compatible test runner."""

    input_options = cast(
        EnvConformanceOptions,
        {
            "assertions": create_expect_assertions(runner.expect),
            "with_env": with_env,
            **(options or {}),
        },
    )
    cases = create_env_conformance(input_options)

    def suite() -> None:
        for test_case in cases:
            runner.it(test_case.name, test_case.run, test_case.timeout_ms)

    runner.describe(name, suite)


__all__ = ["register_env_conformance", "register_storage_conformance"]

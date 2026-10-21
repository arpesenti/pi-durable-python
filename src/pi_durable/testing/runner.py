"""Register the runner-independent conformance cases with a test runner."""

from __future__ import annotations

from .assertions import create_expect_assertions
from .storage_conformance import create_storage_conformance
from .types import StorageConformanceProvider, StorageConformanceRunner


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


__all__ = ["register_storage_conformance"]

"""Conformance and test helpers for storage backends.

The suite is runner independent: :func:`create_storage_conformance` returns cases and
:func:`register_storage_conformance` registers them with a Vitest/Jest-compatible runner.
Benchmark helpers are deliberately excluded (see ``PARITY.md``).
"""

from .assertions import create_expect_assertions
from .runner import register_storage_conformance
from .storage_conformance import create_storage_conformance
from .types import (
    ExpectLike,
    StorageConformanceAssertions,
    StorageConformanceCase,
    StorageConformanceOptions,
    StorageConformanceProvider,
    StorageConformanceRunner,
)

__all__ = [
    "ExpectLike",
    "StorageConformanceAssertions",
    "StorageConformanceCase",
    "StorageConformanceOptions",
    "StorageConformanceProvider",
    "StorageConformanceRunner",
    "create_expect_assertions",
    "create_storage_conformance",
    "register_storage_conformance",
]

"""Public types and protocols of the runner-independent conformance suites."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, NotRequired, Protocol, TypedDict

from ..env import ExecutionEnv
from ..types import Storage

ExpectLike = Callable[[Any, str | None], Any]
"""A Vitest/Jest-style ``expect`` callable: it returns a matcher for the actual value."""


class StorageConformanceAssertions(Protocol):
    """The assertion vocabulary the storage conformance cases use."""

    def ok(self, value: object, message: str | None = None) -> None: ...

    def strict_equal(self, actual: object, expected: object) -> None: ...

    def deep_equal(self, actual: object, expected: object) -> None: ...

    def partial_deep_equal(self, actual: object, expected: object) -> None: ...

    def greater_than(self, actual: float, expected: float) -> None: ...

    async def rejects(self, operation: Awaitable[object], message_includes: str) -> None: ...


StorageConformanceProvider = Callable[[Callable[[Storage], Awaitable[None]]], Awaitable[None]]
"""Runs one case against a fresh storage, calling and awaiting ``use`` exactly once."""


class StorageConformanceOptions(TypedDict):
    assertions: StorageConformanceAssertions
    with_storage: StorageConformanceProvider


@dataclass(frozen=True, slots=True)
class StorageConformanceCase:
    """One runner-independent conformance case."""

    name: str
    run: Callable[[], Awaitable[None]]


class StorageConformanceRunner(Protocol):
    """The subset of a Vitest/Jest-compatible runner the registration helper needs."""

    expect: ExpectLike

    def describe(self, name: str, suite: Callable[[], object]) -> object: ...

    def it(
        self,
        name: str,
        test: Callable[[], Awaitable[None]],
        timeout_ms: int | None = None,
    ) -> object: ...


EnvConformanceAssertions = StorageConformanceAssertions
"""The assertion vocabulary the environment conformance cases use."""

EnvConformanceProvider = Callable[[Callable[[ExecutionEnv], Awaitable[None]]], Awaitable[None]]
"""Calls ``use`` once with an environment whose ``cwd`` is a fresh, empty, writable directory."""


class EnvConformanceOptions(TypedDict):
    assertions: EnvConformanceAssertions
    with_env: EnvConformanceProvider
    shell: NotRequired[Sequence[str]]
    symlinks: NotRequired[bool]
    list_files: NotRequired[bool]


@dataclass(frozen=True, slots=True)
class EnvConformanceCase:
    """One runner-independent environment conformance case."""

    name: str
    run: Callable[[], Awaitable[None]]
    timeout_ms: int | None = None


__all__ = [
    "EnvConformanceAssertions",
    "EnvConformanceCase",
    "EnvConformanceOptions",
    "EnvConformanceProvider",
    "ExpectLike",
    "StorageConformanceAssertions",
    "StorageConformanceCase",
    "StorageConformanceOptions",
    "StorageConformanceProvider",
    "StorageConformanceRunner",
]

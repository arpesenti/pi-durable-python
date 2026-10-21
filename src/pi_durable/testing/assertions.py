"""Adapter from a Vitest/Jest-style ``expect`` to the conformance assertion vocabulary.

The adapter imports neither runner: it calls the snake_case matcher operations of a compatible
``expect`` callable, so translated suites can supply pytest-backed assertions without changing
the conformance cases.
"""

from __future__ import annotations

from collections.abc import Awaitable

from .types import ExpectLike, StorageConformanceAssertions


class _Assertions:
    def __init__(self, expect: ExpectLike) -> None:
        self._expect = expect

    def ok(self, value: object, message: str | None = None) -> None:
        self._expect(value, message).to_be_truthy()

    def strict_equal(self, actual: object, expected: object) -> None:
        self._expect(actual, None).to_be(expected)

    def deep_equal(self, actual: object, expected: object) -> None:
        self._expect(actual, None).to_equal(expected)

    def partial_deep_equal(self, actual: object, expected: object) -> None:
        self._expect(actual, None).to_match_object(expected)

    def greater_than(self, actual: float, expected: float) -> None:
        self._expect(actual, None).to_be_greater_than(expected)

    async def rejects(self, operation: Awaitable[object], message_includes: str) -> None:
        await self._expect(operation, None).rejects.to_throw(message_includes)


def create_expect_assertions(expect: ExpectLike) -> StorageConformanceAssertions:
    """Adapt a runner's ``expect`` to :class:`StorageConformanceAssertions` without importing it."""

    return _Assertions(expect)


__all__ = ["create_expect_assertions"]

"""A pytest-backed ``expect`` for the runner-independent storage conformance cases.

``create_storage_conformance`` is runner independent: it takes a small assertion object. The
upstream tests supply Vitest's ``expect`` through ``createExpectAssertions``; here the same
adapter is fed a matcher built on plain assertions, so the translated suites exercise the
public adapter instead of bypassing it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any


def _partial_match(actual: object, expected: object) -> bool:
    if isinstance(expected, Mapping):
        return isinstance(actual, Mapping) and all(
            key in actual and _partial_match(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, Sequence) and not isinstance(expected, (str, bytes)):
        return (
            isinstance(actual, Sequence)
            and not isinstance(actual, (str, bytes))
            and len(actual) == len(expected)
            and all(_partial_match(item, want) for item, want in zip(actual, expected, strict=True))
        )
    return actual == expected


class _Rejects:
    def __init__(self, operation: Awaitable[Any]) -> None:
        self._operation = operation

    async def to_throw(self, message_includes: str) -> None:
        try:
            await self._operation
        except Exception as error:
            assert message_includes in str(error), f"expected {message_includes!r} in {error!r}"
        else:
            raise AssertionError("expected the operation to throw")


class _Resolves:
    def __init__(self, operation: Awaitable[Any]) -> None:
        self._operation = operation

    async def to_be(self, expected: object) -> None:
        assert await self._operation == expected


class Matcher:
    def __init__(self, actual: Any, message: str | None) -> None:
        self.actual = actual
        self.message = message

    def _fail(self, expectation: str) -> AssertionError:
        prefix = "" if self.message is None else f"{self.message}: "
        return AssertionError(f"{prefix}{self.actual!r} {expectation}")

    def to_be(self, expected: object) -> None:
        if self.actual != expected:
            raise self._fail(f"is not {expected!r}")

    def to_equal(self, expected: object) -> None:
        if self.actual != expected:
            raise self._fail(f"does not equal {expected!r}")

    def to_match_object(self, expected: object) -> None:
        if not _partial_match(self.actual, expected):
            raise self._fail(f"does not match {expected!r}")

    def to_be_greater_than(self, expected: float) -> None:
        if not self.actual > expected:
            raise self._fail(f"is not greater than {expected!r}")

    def to_be_truthy(self) -> None:
        if not self.actual:
            raise self._fail("is not truthy")

    @property
    def rejects(self) -> _Rejects:
        return _Rejects(self.actual)

    @property
    def resolves(self) -> _Resolves:
        return _Resolves(self.actual)


def expect(actual: Any, message: str | None = None) -> Matcher:
    return Matcher(actual, message)

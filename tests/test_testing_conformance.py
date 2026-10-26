"""Translated v1.3.0 ``testing`` assertions, runner, and the conformance case list.

The runner-independent cases are asserted through the public adapter, exactly as the upstream
``createExpectAssertions`` adapter is. The runner test drives ``register_storage_conformance``
with a recording runner instead of Vitest's global ``describe``/``it``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Generator
from typing import Any

from storage_support import expect

from pi_durable import MemoryStorage
from pi_durable.testing import (
    StorageConformanceCase,
    StorageConformanceOptions,
    create_expect_assertions,
    create_storage_conformance,
    register_storage_conformance,
)


class _RecordingMatcher:
    def __init__(self, actual: Any, calls: list[tuple[Any, ...]]) -> None:
        self._actual = actual
        self._calls = calls

    def _record(self, method: str, *args: Any) -> None:
        self._calls.append((method, self._actual, *args))

    def to_be(self, expected: object) -> None:
        self._record("to_be", expected)

    def to_equal(self, expected: object) -> None:
        self._record("to_equal", expected)

    def to_match_object(self, expected: object) -> None:
        self._record("to_match_object", expected)

    def to_be_greater_than(self, expected: float) -> None:
        self._record("to_be_greater_than", expected)

    def to_be_truthy(self) -> None:
        self._record("to_be_truthy")

    @property
    def rejects(self) -> _RecordingRejects:
        return _RecordingRejects(self._actual, self._calls)


class _RecordingRejects:
    def __init__(self, actual: Any, calls: list[tuple[Any, ...]]) -> None:
        self._actual = actual
        self._calls = calls

    async def to_throw(self, expected: str) -> None:
        self._calls.append(("rejects.to_throw", self._actual, expected))


class _Operation:
    """An awaitable the recording matcher never awaits."""

    def __await__(self) -> Generator[Any, None, None]:
        async def run() -> None:
            raise RuntimeError("boom")

        return run().__await__()


async def test_create_expect_assertions_maps_every_assertion_to_the_runner() -> None:
    calls: list[tuple[Any, ...]] = []
    operation = _Operation()

    def recording_expect(actual: Any, message: str | None = None) -> _RecordingMatcher:
        calls.append(("expect", actual, message))
        return _RecordingMatcher(actual, calls)

    assertions = create_expect_assertions(recording_expect)

    assertions.ok("value", "a message")
    assertions.strict_equal(1, 2)
    assertions.deep_equal({"a": 1}, {"a": 2})
    assertions.partial_deep_equal({"a": 1}, {"a": 1})
    assertions.greater_than(3, 2)
    await assertions.rejects(operation, "boom")

    assert calls == [
        ("expect", "value", "a message"),
        ("to_be_truthy", "value"),
        ("expect", 1, None),
        ("to_be", 1, 2),
        ("expect", {"a": 1}, None),
        ("to_equal", {"a": 1}, {"a": 2}),
        ("expect", {"a": 1}, None),
        ("to_match_object", {"a": 1}, {"a": 1}),
        ("expect", 3, None),
        ("to_be_greater_than", 3, 2),
        ("expect", operation, None),
        ("rejects.to_throw", operation, "boom"),
    ]


class _RecordingRunner:
    def __init__(self) -> None:
        self.described: list[str] = []
        self.registered: list[tuple[str, Callable[[], Awaitable[None]], int | None]] = []
        self.expect = expect

    def describe(self, name: str, suite: Callable[[], object]) -> None:
        self.described.append(name)
        suite()

    def it(
        self,
        name: str,
        test: Callable[[], Awaitable[None]],
        timeout_ms: int | None = None,
    ) -> None:
        self.registered.append((name, test, timeout_ms))


async def test_register_storage_conformance_registers_one_case_per_suite() -> None:
    recording = _RecordingRunner()
    uses: list[MemoryStorage] = []

    async def with_storage(use: Callable[[Any], Awaitable[None]]) -> None:
        storage = MemoryStorage()
        uses.append(storage)
        await use(storage)

    register_storage_conformance(recording, "MemoryStorage", with_storage)

    assert recording.described == ["MemoryStorage"]
    cases = create_storage_conformance(
        StorageConformanceOptions(assertions=create_expect_assertions(expect), with_storage=with_storage)
    )
    assert [name for name, _, _ in recording.registered] == [case.name for case in cases]
    assert len(recording.registered) == 25
    assert all(timeout is None for _, _, timeout in recording.registered)

    first = dict((name, test) for name, test, _ in recording.registered)[
        "reserves ID 1 for the immutable root conversation"
    ]
    await first()
    assert len(uses) == 1


async def test_conformance_cases_run_the_provider_exactly_once() -> None:
    calls: list[int] = []

    async def with_storage(use: Callable[[Any], Awaitable[None]]) -> None:
        calls.append(1)
        await use(MemoryStorage())

    cases = create_storage_conformance(
        StorageConformanceOptions(assertions=create_expect_assertions(expect), with_storage=with_storage)
    )
    assert all(isinstance(case, StorageConformanceCase) for case in cases)
    assert len({case.name for case in cases}) == len(cases)
    assert calls == []

    await next(case for case in cases if case.name == "rejects every operation after close").run()
    assert calls == [1]

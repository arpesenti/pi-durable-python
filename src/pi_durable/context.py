"""Invocation-scoped values and cancellation.

The v1.3.0 kernel passes a Chord ``Context`` explicitly through every operation. This port keeps
the explicit parameter but reduces the surface to what durable needs: an optional abort signal
and an immutable chain of typed values. Aborted work observes cancellation through
:meth:`AbortSignal.wait`, :func:`await_with_context`, or by checking ``context.abort_signal``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class AbortError(Exception):
    """The operation was cancelled through its context.

    ``reason`` is the value passed to :meth:`AbortController.abort`; it is ``None`` when the
    cancellation did not supply one.
    """

    def __init__(self, reason: object | None = None) -> None:
        super().__init__("The operation was aborted")
        self.name = "AbortError"
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ContextKey(Generic[T]):
    """Typed identity for one value carried by a :class:`Context`."""

    token: object
    description: str = ""


def create_context_key(description: str) -> ContextKey[T]:
    """Create a key whose identity, not description, selects a context value."""

    return ContextKey(token=object(), description=description)


class AbortSignal:
    """An observable cancellation state shared by a context and its derived contexts."""

    __slots__ = ("_aborted", "_cleanup", "_listeners", "_reason")

    def __init__(self) -> None:
        self._aborted = False
        self._reason: object | None = None
        self._listeners: list[Callable[[], None]] = []
        self._cleanup: list[Callable[[], None]] = []

    @property
    def aborted(self) -> bool:
        return self._aborted

    @property
    def reason(self) -> object | None:
        return self._reason

    def add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Register ``listener`` and return a function that removes it.

        A listener runs synchronously on the aborting caller. It is not invoked when the signal
        is already aborted; such callers should check :attr:`aborted` or await :meth:`wait`.
        """

        if not self._aborted:
            self._listeners.append(listener)
        return lambda: self._remove(listener)

    def _remove(self, listener: Callable[[], None]) -> None:
        try:
            self._listeners.remove(listener)
        except ValueError:
            pass

    def _abort(self, reason: object | None) -> None:
        if self._aborted:
            return
        self._aborted = True
        self._reason = reason
        listeners = self._listeners
        self._listeners = []
        cleanup = self._cleanup
        self._cleanup = []
        for listener in listeners:
            listener()
        for remove in cleanup:
            remove()

    async def wait(self) -> None:
        """Resolve when the signal aborts; return immediately when it already has."""

        if self._aborted:
            return
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[None] = loop.create_future()

        def wake() -> None:
            if not waiter.done():
                waiter.set_result(None)

        remove = self.add_listener(wake)
        if self._aborted:
            wake()
        try:
            await waiter
        finally:
            remove()

    def raise_if_aborted(self) -> None:
        """Raise :class:`AbortError` when the signal has aborted."""

        if self._aborted:
            reason = self._reason
            raise AbortError(reason)

    @staticmethod
    def any(signals: Sequence[AbortSignal]) -> AbortSignal:
        """Return a signal aborted when any input signal aborts, with the first reason."""

        combined = AbortSignal()
        for signal in signals:
            if signal.aborted:
                combined._abort(signal.reason)
                break
        if combined.aborted:
            return combined

        def abort() -> None:
            for signal in signals:
                if signal.aborted:
                    combined._abort(signal.reason)
                    return

        combined._cleanup = [signal.add_listener(abort) for signal in signals]
        return combined


class AbortController:
    """Owns one independently cancellable :class:`AbortSignal`."""

    __slots__ = ("signal",)

    def __init__(self) -> None:
        self.signal = AbortSignal()

    def abort(self, reason: object | None = None) -> None:
        """Abort once; later calls are no-ops and keep the first reason."""

        self.signal._abort(reason)


class Context:
    """Immutable invocation-scoped values and cancellation state.

    Values are looked up by key identity. The built-in abort signal is itself stored as a context
    value so derived contexts compose without a mutable field.
    """

    __slots__ = ("_name",)

    _ABORT_SIGNAL_KEY: ContextKey[AbortSignal | None] = create_context_key("[context abort signal]")

    def __init__(self, name: str = "[Context]") -> None:
        self._name = name

    @property
    def abort_signal(self) -> AbortSignal | None:
        return self.value(Context._ABORT_SIGNAL_KEY)

    def value(self, key: ContextKey[T]) -> T | None:
        return None

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return f"Context({self._name!r})"


class _EmptyContext(Context):
    __slots__ = ()


class _ContextValue(Context):
    __slots__ = ("_parent", "_key", "_value")

    def __init__(self, parent: Context, key: ContextKey[Any], value: Any) -> None:
        self._parent = parent
        self._key = key
        self._value = value
        self._name = f"{parent}.WithValue({key.description or 'anonymous'})"

    def value(self, key: ContextKey[T]) -> T | None:
        if key.token is self._key.token:
            return self._value
        return self._parent.value(key)


BACKGROUND_CONTEXT: Context = _EmptyContext("[Context BACKGROUND_CONTEXT]")
TODO_CONTEXT: Context = _EmptyContext("[Context TODO_CONTEXT]")


def with_context_value(key: ContextKey[T], value: T, parent: Context) -> Context:
    """Derive a context containing one additional or replaced value."""

    return _ContextValue(parent, key, value)


def with_abort_signal(signal: AbortSignal, context: Context) -> Context:
    """Derive a context cancelled by either the parent signal or ``signal``."""

    parent = context.abort_signal
    combined = signal if parent is None else AbortSignal.any((parent, signal))
    return with_context_value(Context._ABORT_SIGNAL_KEY, combined, context)


def without_abort_signal(context: Context) -> Context:
    """Derive a context retaining all values except caller cancellation.

    Intended for mandatory cleanup only.
    """

    return with_context_value(Context._ABORT_SIGNAL_KEY, None, context)


@dataclass(frozen=True, slots=True)
class CancelScope:
    """An independently cancellable child context and its cancel function."""

    context: Context
    _controller: AbortController

    def cancel(self, reason: object | None = None) -> None:
        """Abort the child context once; later calls keep the first reason."""

        self._controller.abort(reason)


def with_cancel(context: Context) -> CancelScope:
    """Derive an independently cancellable child context."""

    controller = AbortController()
    return CancelScope(context=with_abort_signal(controller.signal, context), _controller=controller)


async def await_with_context(awaitable: Awaitable[T], context: Context) -> T:
    """Await ``awaitable`` until it settles or ``context`` is cancelled.

    Cancellation rejects only this waiter with :class:`AbortError`; the underlying awaitable is
    left to finish.
    """

    signal = context.abort_signal
    if signal is None:
        return await awaitable
    if signal.aborted:
        _close_coroutine(awaitable)
        raise AbortError(signal.reason)

    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[None] = loop.create_future()

    def abort() -> None:
        if not waiter.done():
            waiter.set_exception(AbortError(signal.reason))

    remove = signal.add_listener(abort)
    if signal.aborted:
        abort()
    task: asyncio.Future[T] = asyncio.ensure_future(awaitable)
    try:
        await asyncio.wait((task, waiter), return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            return task.result()
        task.add_done_callback(_discard_result)
        waiter.result()
        raise AssertionError("unreachable")
    finally:
        remove()
        if not waiter.done():
            waiter.cancel()


def _discard_result(task: asyncio.Future[Any]) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:
        pass


def _close_coroutine(awaitable: Awaitable[Any]) -> None:
    close = getattr(awaitable, "close", None)
    if close is not None:
        close()


__all__ = [
    "BACKGROUND_CONTEXT",
    "TODO_CONTEXT",
    "AbortController",
    "AbortError",
    "AbortSignal",
    "CancelScope",
    "Context",
    "ContextKey",
    "await_with_context",
    "create_context_key",
    "with_abort_signal",
    "with_cancel",
    "with_context_value",
    "without_abort_signal",
]

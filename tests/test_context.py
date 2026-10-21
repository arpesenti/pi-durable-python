import asyncio

import pytest

from pi_durable.context import (
    BACKGROUND_CONTEXT,
    TODO_CONTEXT,
    AbortController,
    AbortError,
    AbortSignal,
    ContextKey,
    await_with_context,
    create_context_key,
    with_abort_signal,
    with_cancel,
    with_context_value,
    without_abort_signal,
)


def test_context_values_are_looked_up_by_identity_and_fall_through() -> None:
    request: ContextKey[str] = create_context_key("request")
    unrelated: ContextKey[str] = create_context_key("request")
    child = with_context_value(request, "value", BACKGROUND_CONTEXT)
    grandchild = with_context_value(unrelated, "other", child)

    assert child.value(request) == "value"
    assert child.value(unrelated) is None
    assert grandchild.value(request) == "value"
    assert grandchild.value(unrelated) == "other"
    assert BACKGROUND_CONTEXT.value(request) is None
    assert str(child) == "[Context BACKGROUND_CONTEXT].WithValue(request)"
    assert "anonymous" in str(with_context_value(create_context_key(""), 1, BACKGROUND_CONTEXT))


def test_derived_contexts_do_not_mutate_their_parent() -> None:
    key: ContextKey[int] = create_context_key("number")
    child = with_context_value(key, 1, BACKGROUND_CONTEXT)
    replacement = with_context_value(key, 2, child)
    assert child.value(key) == 1
    assert replacement.value(key) == 2


def test_named_contexts_have_distinct_identities() -> None:
    assert BACKGROUND_CONTEXT is not TODO_CONTEXT
    assert str(BACKGROUND_CONTEXT) == "[Context BACKGROUND_CONTEXT]"
    assert str(TODO_CONTEXT) == "[Context TODO_CONTEXT]"


def test_context_on_a_controller_exposes_cancellation_state() -> None:
    controller = AbortController()
    context = with_abort_signal(controller.signal, BACKGROUND_CONTEXT)
    assert context.abort_signal is controller.signal
    assert context.abort_signal.aborted is False

    controller.abort("stop")
    assert context.abort_signal.aborted is True
    assert context.abort_signal.reason == "stop"
    with pytest.raises(AbortError) as raised:
        context.abort_signal.raise_if_aborted()
    assert raised.value.reason == "stop"


def test_with_cancel_derives_an_independent_child() -> None:
    parent = AbortController()
    parent_context = with_abort_signal(parent.signal, BACKGROUND_CONTEXT)
    scope = with_cancel(parent_context)

    scope.cancel("child")
    assert scope.context.abort_signal is not None
    assert scope.context.abort_signal.aborted is True
    assert scope.context.abort_signal.reason == "child"
    assert parent_context.abort_signal is not None
    assert parent_context.abort_signal.aborted is False

    other = with_cancel(BACKGROUND_CONTEXT)
    other.cancel()
    assert other.context.abort_signal is not None
    assert other.context.abort_signal.aborted is True
    assert other.context.abort_signal.reason is None


def test_parent_abort_reaches_a_cancellable_child() -> None:
    scope = with_cancel(BACKGROUND_CONTEXT)
    parent = AbortController()
    context = with_abort_signal(parent.signal, scope.context)
    parent.abort("parent")
    assert context.abort_signal is not None
    assert context.abort_signal.aborted is True
    assert context.abort_signal.reason == "parent"


def test_combined_signals_report_the_first_abort() -> None:
    first = AbortController()
    second = AbortController()
    combined = AbortSignal.any([first.signal, second.signal])
    second.abort("second")
    assert combined.aborted is True
    assert combined.reason == "second"
    first.abort("first")
    assert combined.reason == "second"


def test_abort_signal_any_honors_already_aborted_inputs() -> None:
    aborted = AbortController()
    aborted.abort("early")
    combined = AbortSignal.any([aborted.signal, AbortController().signal])
    assert combined.aborted is True
    assert combined.reason == "early"


def test_without_abort_signal_retains_values_but_drops_cancellation() -> None:
    key: ContextKey[str] = create_context_key("request")
    controller = AbortController()
    context = with_context_value(key, "value", with_abort_signal(controller.signal, BACKGROUND_CONTEXT))
    clean = without_abort_signal(context)
    assert clean.value(key) == "value"
    assert clean.abort_signal is None


async def test_async_work_observes_abort() -> None:
    scope = with_cancel(BACKGROUND_CONTEXT)
    observed = asyncio.Event()

    async def watch() -> None:
        assert scope.context.abort_signal is not None
        await scope.context.abort_signal.wait()
        observed.set()

    task = asyncio.create_task(watch())
    await asyncio.sleep(0)
    assert observed.is_set() is False
    scope.cancel("stop")
    await asyncio.wait_for(observed.wait(), timeout=1)
    await task


async def test_await_with_context_returns_the_underlying_result() -> None:
    async def work() -> str:
        return "done"

    assert await await_with_context(work(), BACKGROUND_CONTEXT) == "done"


async def test_await_with_context_rejects_on_cancellation() -> None:
    scope = with_cancel(BACKGROUND_CONTEXT)
    started = asyncio.Event()
    finished = asyncio.Event()

    async def work() -> str:
        started.set()
        await asyncio.sleep(0.05)
        finished.set()
        return "done"

    task = asyncio.create_task(await_with_context(work(), scope.context))
    await started.wait()
    scope.cancel("cancelled")
    with pytest.raises(AbortError) as raised:
        await task
    assert raised.value.reason == "cancelled"
    await asyncio.wait_for(finished.wait(), timeout=1)


async def test_await_with_context_rejects_immediately_when_already_aborted() -> None:
    scope = with_cancel(BACKGROUND_CONTEXT)
    scope.cancel("done")

    async def work() -> str:
        return "unreachable"

    with pytest.raises(AbortError):
        await await_with_context(work(), scope.context)

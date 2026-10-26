"""Host timers: every timer of the Session and Harness is armed through ``set_timer``.

Port of v1.4.0 ``src/timers.ts``. A foreground timer paces or resumes work, so it must fire: one
the host throws on or declines is armed with :func:`native_timer`, and the first such failure is
reported. A background timer only frees memory, so a host may decline it by returning ``None``;
the memory is then freed at later commits.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from .harness.types import SetTimer

MAX_TIMER_DELAY_MS = 2_147_483_647
"""Longest delay a loop timer accepts; a longer one is waited for in several steps."""


def native_timer(
    callback: Callable[[], None], delay_ms: float, *, background: bool = False
) -> Callable[[], None] | None:
    """The default ``HarnessOptions.set_timer``.

    A loop timer cannot keep a Python process alive once its loop ends, so both kinds are armed
    the same way. A delay beyond what the loop accepts is waited out in steps.
    """

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return None
    cancelled = False
    handle: asyncio.TimerHandle | None = None

    def arm(remaining: float) -> None:
        nonlocal handle

        def fire() -> None:
            if cancelled:
                return
            if remaining > MAX_TIMER_DELAY_MS:
                arm(remaining - MAX_TIMER_DELAY_MS)
            else:
                callback()

        handle = loop.call_later(min(remaining, MAX_TIMER_DELAY_MS) / 1000.0, fire)

    arm(max(0.0, float(delay_ms)))

    def disarm() -> None:
        nonlocal cancelled
        cancelled = True
        if handle is not None:
            handle.cancel()

    return disarm


class Timers:
    """Timers the Session and Harness arm through the host's ``set_timer``."""

    __slots__ = ("_arm", "_report", "_reported")

    def __init__(self, set_timer: SetTimer | None, report: Callable[[BaseException], None]) -> None:
        self._arm: SetTimer = set_timer if set_timer is not None else native_timer
        self._reported = False
        self._report = report

    def foreground(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None]:
        """A timer that paces or resumes work; falls back to the default when the host declines it."""

        try:
            disarm = self._arm(callback, delay_ms, background=False)
            if disarm is not None:
                return disarm
            self._fall_back(
                RuntimeError("HarnessOptions.set_timer declined a timer that paces work; using the default")
            )
        except Exception as error:
            self._fall_back(error)
        return native_timer(callback, delay_ms, background=False) or (lambda: None)

    def background(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None] | None:
        """A timer that only frees memory; ``None`` when the host declines it."""

        try:
            return self._arm(callback, delay_ms, background=True)
        except Exception as error:
            self._report(error)
            return None

    def _fall_back(self, error: BaseException) -> None:
        if self._reported:
            return
        self._reported = True
        self._report(error)


def host_timers(
    set_timer: SetTimer | None, report: Callable[[BaseException], None]
) -> Timers:
    """Wrap a host ``set_timer``; see :class:`Timers`."""

    return Timers(set_timer, report)


__all__ = ["MAX_TIMER_DELAY_MS", "Timers", "host_timers", "native_timer"]

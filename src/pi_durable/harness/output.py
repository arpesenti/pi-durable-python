"""Bounded output streams and adaptive progress commits.

Port of Chord 1.3.0 ``output.ts``, the module the upstream tool task uses to bound a call's
running output and to pace its progress commits. ``OutputBuffer`` keeps a head or tail window of a
stream at a cost proportional to each chunk, and ``Progress`` paces commits of a changing value by
their written size.
"""

from __future__ import annotations

import asyncio
import codecs
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from ..delta import overlap_strings

#: ``truncate.ts`` defaults shared by tools: whichever limit is hit first wins.
DEFAULT_MAX_LINES = 2000
DEFAULT_MAX_BYTES = 50 * 1024

#: Each progress commit also buys a pause proportional to what it wrote.
PROGRESS_BYTES_PER_SECOND = 100 * 1024

_INVALID_OUTPUT = re.compile("[\x00-\x08\x0b-\x1f\ufff9-\ufffb]")
_SURROGATE = re.compile("[\ud800-\udfff]")


@dataclass(frozen=True, slots=True)
class OutputLimits:
    """Retention limits of one output stream."""

    max_bytes: int = DEFAULT_MAX_BYTES
    max_lines: int = DEFAULT_MAX_LINES
    retain: str = "head"


@dataclass(frozen=True, slots=True)
class BoundedOutput:
    """Retained output and what the limits dropped."""

    text: str
    dropped_bytes: int
    dropped_lines: int


@dataclass(frozen=True, slots=True)
class OutputSlice:
    """An exact slice of the input within the limits, and what it left out."""

    text: str
    bytes: int
    dropped_bytes: int
    dropped_lines: int


def sanitize_output(text: str) -> str:
    """Remove control characters that break display and transcripts; tabs and newlines stay."""

    return _INVALID_OUTPUT.sub("", text)


def utf8_byte_length(text: str) -> int:
    """UTF-8 byte length of ``text``; a lone surrogate counts as U+FFFD, three bytes."""

    if text.isascii():
        return len(text)
    return len(_encode(text))


def _encode(text: str) -> bytes:
    if _SURROGATE.search(text) is None:
        return text.encode("utf-8")
    return _SURROGATE.sub("\ufffd", text).encode("utf-8")


def _decode(data: bytes) -> str:
    return data.decode("utf-8", "replace")


def bound_output(text: str, limits: OutputLimits) -> OutputSlice:
    """Bound ``text`` to whole lines within the limits: the first lines for ``head``, the last for ``tail``.

    The result is an exact slice, trailing newline included. A single line longer than ``max_bytes``
    is cut at the byte limit on a character boundary.
    """

    data = _encode(text)
    start, end = _head_range(data, limits) if limits.retain == "head" else _tail_range(data, limits)
    kept = data[start:end]
    return OutputSlice(
        text=text if len(kept) == len(data) else _decode(kept),
        bytes=len(kept),
        dropped_bytes=len(data) - len(kept),
        dropped_lines=_line_count(data) - _line_count(kept),
    )


def _head_range(data: bytes, limits: OutputLimits) -> tuple[int, int]:
    if limits.max_lines == 0 or limits.max_bytes == 0:
        return 0, 0
    end = len(data)
    lines = 0
    index = data.find(b"\n")
    while index != -1:
        lines += 1
        if lines == limits.max_lines:
            end = index + 1
            break
        index = data.find(b"\n", index + 1)
    if end > limits.max_bytes:
        newline = data.rfind(b"\n", 0, limits.max_bytes)
        end = _character_end(data, limits.max_bytes) if newline == -1 else newline + 1
    return 0, end


def _tail_range(data: bytes, limits: OutputLimits) -> tuple[int, int]:
    if limits.max_lines == 0 or limits.max_bytes == 0:
        return len(data), len(data)
    # A trailing newline ends the last line rather than starting another.
    last = len(data) - 2 if data.endswith(b"\n") else len(data) - 1
    start = 0
    lines = 1
    index = -1 if last < 0 else data.rfind(b"\n", 0, last + 1)
    while index != -1:
        if lines == limits.max_lines:
            start = index + 1
            break
        lines += 1
        index = -1 if index == 0 else data.rfind(b"\n", 0, index)
    if len(data) - start > limits.max_bytes:
        first = len(data) - limits.max_bytes
        newline = data.find(b"\n", max(0, first - 1))
        # The first line starting inside the byte window, or a cut of the last line when it alone
        # is too long.
        start = newline + 1 if newline != -1 and newline + 1 < len(data) else _character_start(data, first)
    return start, len(data)


def _character_end(data: bytes, index: int) -> int:
    """The last character boundary at or before ``index``."""

    end = index
    while end > 0 and (data[end] & 0xC0) == 0x80:
        end -= 1
    return end


def _character_start(data: bytes, index: int) -> int:
    """The first character boundary at or after ``index``."""

    start = index
    while start < len(data) and (data[start] & 0xC0) == 0x80:
        start += 1
    return start


def _line_count(data: bytes) -> int:
    if not data:
        return 0
    newlines = data.count(b"\n")
    return newlines + (0 if data.endswith(b"\n") else 1)


class OutputBuffer:
    """Bounded running output of one stream, such as a tool call's.

    Accepting a chunk costs time proportional to the chunk: head retention stops storing once the
    window is full, and tail retention drops stored text the window no longer needs when it
    snapshots. Counts of the whole stream are kept so the dropped totals stay exact.
    """

    def __init__(self, limits: OutputLimits) -> None:
        self._limits = limits
        self._decoder = codecs.getincrementaldecoder("utf-8")("replace")
        self._started = False
        # Stored chunks: for head the start of the stream, for tail a suffix that still contains the
        # next window.
        self._chunks: list[tuple[str, int, int]] = []
        self._stored_bytes = 0
        self._stored_newlines = 0
        self._full = False
        self._total_bytes = 0
        self._total_newlines = 0
        self._ends_with_newline = True

    @property
    def stored_bytes(self) -> int:
        """Bytes currently held; bounded by the limits plus one chunk."""

        return self._stored_bytes

    def push(self, chunk: str | bytes, skipped: Mapping[str, Any] | None = None) -> bool:
        """Accept a chunk; return whether anything was accepted.

        ``skipped`` is output omitted right before the chunk, which must be more than the tail
        window by at least one byte or line; only tail retention accepts it.
        """

        # Bytes of an incomplete character from an earlier byte chunk come first.
        pending = self._decoder.decode(b"") if isinstance(chunk, str) or skipped is not None else ""
        text = chunk if isinstance(chunk, str) else self._decoder.decode(chunk, False)
        first = not self._started and pending == "" and skipped is None
        if pending != "" or text != "" or skipped is not None:
            self._started = True
        if first and isinstance(chunk, bytes) and text.startswith("\ufeff"):
            text = text[1:]
        if skipped is None:
            return self._accept(pending + text)
        if self._limits.retain != "tail":
            raise ValueError("Skipped output requires tail retention")
        self._accept(pending)
        self._skip(_skip_of(skipped))
        self._accept(text)
        return True

    def _skip(self, skipped: OutputSkip) -> None:
        if skipped.byte_count == 0:
            return
        self._total_bytes += skipped.byte_count
        self._total_newlines += skipped.newlines
        self._ends_with_newline = skipped.ends_with_newline
        self._chunks = []
        self._stored_bytes = 0
        self._stored_newlines = 0

    def end(self) -> None:
        """Flush an incomplete trailing character as a replacement character; call when the stream ends."""

        self._accept(self._decoder.decode(b"", True))

    def _accept(self, text: str) -> bool:
        if text == "":
            return False
        bytes_ = utf8_byte_length(text)
        newlines = text.count("\n")
        self._total_bytes += bytes_
        self._total_newlines += newlines
        self._ends_with_newline = text.endswith("\n")
        if self._full:
            return True
        self._chunks.append((text, bytes_, newlines))
        self._stored_bytes += bytes_
        self._stored_newlines += newlines
        if self._limits.retain == "head":
            # Nothing past a full window is ever needed.
            self._full = (
                self._stored_bytes > self._limits.max_bytes
                or self._stored_newlines >= self._limits.max_lines
            )
            return True
        # Drop leading chunks while the rest still holds more than a window: more than max_bytes
        # bytes or max_lines newlines, plus one, so the window's line start can still be found.
        while len(self._chunks) > 1:
            first_bytes, first_newlines = self._chunks[0][1], self._chunks[0][2]
            bytes_after = self._stored_bytes - first_bytes
            newlines_after = self._stored_newlines - first_newlines
            if bytes_after <= self._limits.max_bytes + 1 and newlines_after <= self._limits.max_lines + 1:
                break
            self._chunks.pop(0)
            self._stored_bytes = bytes_after
            self._stored_newlines = newlines_after
        return True

    def snapshot(self) -> BoundedOutput:
        """Retained, sanitized output and what the limits dropped from the whole stream."""

        stored = self._chunks[0][0] if len(self._chunks) == 1 else "".join(chunk[0] for chunk in self._chunks)
        kept = bound_output(stored, self._limits)
        stored_lines = _lines(self._stored_newlines, stored == "" or stored.endswith("\n"))
        kept_lines = stored_lines - kept.dropped_lines
        # Tail windows never reach back before this one, but finding a later window's first line needs
        # what precedes it: keep the shortest suffix longer than the window by a byte or a line.
        if self._limits.retain == "tail" or len(self._chunks) > 1:
            text = _tail_margin(stored, self._limits) if self._limits.retain == "tail" else stored
            bytes_ = utf8_byte_length(text) if self._limits.retain == "tail" else self._stored_bytes
            self._chunks = [] if text == "" else [(text, bytes_, text.count("\n"))]
            self._stored_bytes = bytes_
            self._stored_newlines = self._chunks[0][2] if self._chunks else 0
        return BoundedOutput(
            text=sanitize_output(kept.text),
            dropped_bytes=self._total_bytes - kept.bytes,
            dropped_lines=_lines(self._total_newlines, self._ends_with_newline) - kept_lines,
        )


@dataclass(frozen=True, slots=True)
class OutputSkip:
    """Output a producer omitted before a chunk, measured on the decoded text."""

    byte_count: int
    newlines: int
    ends_with_newline: bool


def _skip_of(skipped: Mapping[str, Any]) -> OutputSkip:
    bytes_ = skipped.get("bytes", skipped.get("byteCount", 0))
    newlines = skipped.get("newlines", 0)
    ends = skipped.get("ends_with_newline", skipped.get("endsWithNewline", False))
    return OutputSkip(int(bytes_ or 0), int(newlines or 0), bool(ends))


def _tail_margin(text: str, limits: OutputLimits) -> str:
    """The shortest suffix of ``text`` with more than ``max_bytes`` bytes or ``max_lines`` newlines."""

    data = _encode(text)
    byte_start = (
        _character_end(data, len(data) - limits.max_bytes - 1) if len(data) > limits.max_bytes else 0
    )
    line_start = 0
    newlines = 0
    index = data.rfind(b"\n")
    while index != -1:
        newlines += 1
        if newlines > limits.max_lines:
            line_start = index
            break
        if index == 0:
            break
        index = data.rfind(b"\n", 0, index)
    start = max(byte_start, line_start)
    return text if start == 0 else _decode(data[start:])


def _lines(newlines: int, terminated: bool) -> int:
    return newlines + (0 if terminated else 1)


def overlap(left: str, right: str, limit: int) -> int:
    """Length of the longest suffix of ``left`` that is a prefix of ``right``, bounded by ``limit``."""

    return overlap_strings(left, right, limit)


def _now_ms() -> float:
    return time.monotonic() * 1000.0


class ProgressClock(Protocol):
    """Where progress pacing reads its time and arms its timer.

    A ``ToolExecutionApi`` progress commit runs on the Harness clock through the host's
    ``set_timer``; without one, :class:`Progress` uses a monotonic clock and an asyncio timer.
    """

    def now(self) -> float: ...

    def set_timer(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None]: ...


class _LoopClock:
    """The default clock: a monotonic millisecond clock and an asyncio timer."""

    def now(self) -> float:
        return _now_ms()

    def set_timer(self, callback: Callable[[], None], delay_ms: float) -> Callable[[], None]:
        handle = asyncio.get_running_loop().call_later(max(0.0, delay_ms) / 1000.0, callback)
        return handle.cancel


class Progress:
    """Adaptive progress commits: the first change after an idle period commits at once; each commit
    then delays the next by at least ``min_interval_ms`` and by its written size at 100 KiB/s.

    At most one commit is in flight; changes made meanwhile coalesce into the next one.
    """

    def __init__(
        self,
        write: Callable[[], Any],
        on_error: Callable[[BaseException], None],
        min_interval_ms: int,
        clock: ProgressClock | None = None,
    ) -> None:
        self._write = write
        self._on_error = on_error
        self._min_interval_ms = max(0, min_interval_ms)
        self._clock: ProgressClock = clock if clock is not None else _LoopClock()
        self._waiters: list[asyncio.Future[None]] = []
        self._timer: Callable[[], None] | None = None
        self._in_flight: asyncio.Future[None] | None = None
        self._next_at = 0.0
        self._dirty = False
        self._stopped = False

    def mark(self) -> None:
        """Schedule a commit."""

        self._dirty = True
        self._schedule()

    def mark_and_wait(self) -> asyncio.Future[None]:
        """Schedule a commit; the future settles with the commit that includes this change."""

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        self.mark()
        return waiter

    async def stop(self) -> list[asyncio.Future[None]]:
        """Stop committing and wait for the commit in flight; returns the waiters the final commit settles."""

        self._stopped = True
        if self._timer is not None:
            self._timer()
            self._timer = None
        if self._in_flight is not None:
            await self._in_flight
        pending = self._waiters
        self._waiters = []
        return pending

    def _schedule(self) -> None:
        if self._stopped or self._timer is not None or self._in_flight is not None:
            return
        wait = self._next_at - self._clock.now()
        if wait <= 0:
            self._flush()
        else:
            self._timer = self._clock.set_timer(self._fire, wait)

    def _fire(self) -> None:
        self._timer = None
        self._flush()

    def _flush(self) -> None:
        if self._stopped or not self._dirty:
            return
        self._dirty = False
        waiters = self._waiters
        self._waiters = []
        started = self._clock.now()
        # Called synchronously, so a writer captures what changed before the commit is awaited.
        try:
            produced = self._write()
        except BaseException as error:
            self._fail(error, started, waiters)
            return

        async def complete() -> None:
            try:
                written = await produced if hasattr(produced, "__await__") else produced
            except BaseException as error:
                self._fail(error, started, waiters)
                return
            bytes_ = int(written or 0)
            self._next_at = started + max(
                self._min_interval_ms, bytes_ * 1000.0 / PROGRESS_BYTES_PER_SECOND
            )
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_result(None)

        self._in_flight = asyncio.ensure_future(complete())
        self._in_flight.add_done_callback(lambda _: None)

        def finished(_: Any) -> None:
            self._in_flight = None
            if self._dirty:
                self._schedule()

        self._in_flight.add_done_callback(finished)

    def _fail(self, error: BaseException, started: float, waiters: list[asyncio.Future[None]]) -> None:
        self._next_at = started + self._min_interval_ms
        for waiter in waiters:
            if not waiter.done():
                waiter.set_exception(error)
                waiter.exception()
        self._on_error(error)


__all__ = [
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_LINES",
    "PROGRESS_BYTES_PER_SECOND",
    "BoundedOutput",
    "OutputBuffer",
    "OutputLimits",
    "OutputSkip",
    "OutputSlice",
    "Progress",
    "ProgressClock",
    "bound_output",
    "overlap",
    "sanitize_output",
    "utf8_byte_length",
]

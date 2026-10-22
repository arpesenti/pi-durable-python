"""Bounded output and adaptive progress commits (the port's Chord ``output.ts``).

These helpers bound a tool call's running output and pace its ``pi.live`` commits, so the suite
checks the exact windows, dropped totals, sanitizing, and throttle behavior the tool suite observes
through a running tool.
"""

from __future__ import annotations

import asyncio
from typing import Any

from session_support import flush
from task_support import eventually

from pi_durable.harness.output import (
    DEFAULT_MAX_BYTES,
    OutputBuffer,
    OutputLimits,
    Progress,
    bound_output,
    overlap,
    sanitize_output,
    utf8_byte_length,
)


def test_bounds_head_output_to_whole_lines_and_reports_what_it_dropped() -> None:
    head = OutputLimits(max_bytes=DEFAULT_MAX_BYTES, max_lines=2, retain="head")
    bounded = bound_output("a\nb\nc\n", head)
    assert bounded.text == "a\nb\n"
    assert (bounded.dropped_lines, bounded.dropped_bytes) == (1, 2)
    # A single line longer than the byte limit is cut at the byte limit on a character boundary.
    cut = bound_output("abcdefghij", OutputLimits(max_bytes=4))
    assert (cut.text, cut.dropped_lines, cut.dropped_bytes) == ("abcd", 0, 6)
    accented = bound_output("ééé", OutputLimits(max_bytes=3))
    assert (accented.text, accented.dropped_bytes) == ("é", 4)


def test_bounds_tail_output_and_treats_a_trailing_newline_as_ending_its_line() -> None:
    tail = OutputLimits(max_bytes=DEFAULT_MAX_BYTES, max_lines=2, retain="tail")
    bounded = bound_output("a\nb\nc\n", tail)
    assert bounded.text == "b\nc\n"
    assert (bounded.dropped_lines, bounded.dropped_bytes) == (1, 2)
    assert bound_output("a\nb\n", OutputLimits(max_lines=1, retain="tail")).text == "b\n"


def test_a_head_buffer_stops_storing_at_its_window_and_counts_the_whole_stream() -> None:
    buffer = OutputBuffer(OutputLimits(max_lines=2))
    assert buffer.push("1\n2\n3\n4\n") is True
    first = buffer.snapshot()
    assert (first.text, first.dropped_lines, first.dropped_bytes) == ("1\n2\n", 2, 4)
    buffer.push("5\n")
    second = buffer.snapshot()
    assert (second.text, second.dropped_lines, second.dropped_bytes) == ("1\n2\n", 3, 6)


def test_a_tail_buffer_keeps_the_last_window() -> None:
    buffer = OutputBuffer(OutputLimits(max_lines=2, retain="tail"))
    buffer.push("1\n2\n3\n4\n")
    snapshot = buffer.snapshot()
    assert (snapshot.text, snapshot.dropped_lines, snapshot.dropped_bytes) == ("3\n4\n", 2, 4)
    buffer.push("5\n")
    following = buffer.snapshot()
    assert (following.text, following.dropped_lines, following.dropped_bytes) == ("4\n5\n", 3, 6)


def test_a_skipped_chunk_counts_as_if_every_byte_had_arrived() -> None:
    buffer = OutputBuffer(OutputLimits(max_lines=1, retain="tail"))
    buffer.push("dropped\n")
    buffer.push("x\ny\n", {"bytes": 8, "newlines": 1, "ends_with_newline": True})
    snapshot = buffer.snapshot()
    assert snapshot.text == "y\n"
    assert (snapshot.dropped_lines, snapshot.dropped_bytes) == (3, 18)


def test_sanitizes_control_characters_but_keeps_tabs_and_newlines() -> None:
    assert sanitize_output("a\x07b\r\n\tc") == "ab\n\tc"
    buffer = OutputBuffer(OutputLimits())
    buffer.push("a\x07b\r\n")
    assert buffer.snapshot().text == "ab\n"


def test_counts_utf8_bytes_with_lone_surrogates_as_replacement_characters() -> None:
    assert utf8_byte_length("abc") == 3
    assert utf8_byte_length("é") == 2
    assert utf8_byte_length("\ud800") == 3


def test_finds_the_bounded_overlap_of_two_windows() -> None:
    assert overlap("abc", "cdef", 65_536) == 1
    assert overlap("abc", "xyz", 65_536) == 0
    assert overlap("aaaa", "aaab", 65_536) == 3
    assert overlap("abcdef", "def", 65_536) == 3
    assert overlap("abcdef", "def", 2) == 0


def test_commits_the_first_change_at_once_and_coalesces_the_rest() -> None:
    commits: list[int] = []

    async def main() -> None:
        async def write() -> int:
            commits.append(len(commits))
            await flush()
            return 1

        progress = Progress(write, lambda error: None, 100)
        progress.mark()
        await eventually(lambda: len(commits) == 1)
        # Two changes inside the throttle window coalesce into one commit after it.
        progress.mark()
        progress.mark()
        await eventually(lambda: len(commits) == 2)
        pending = await progress.stop()
        assert pending == []

    asyncio.run(main())


def test_a_details_waiter_settles_with_the_commit_that_includes_it() -> None:
    async def main() -> None:
        writes: list[int] = []

        async def write() -> int:
            writes.append(len(writes))
            return 1

        progress = Progress(write, lambda error: None, 1)
        waiter = progress.mark_and_wait()
        await waiter
        assert writes == [0]
        await progress.stop()

    asyncio.run(main())


def test_stop_waits_for_an_in_flight_commit_and_returns_the_waiters_it_did_not_write() -> None:
    async def main() -> None:
        release: Any = asyncio.Event()

        async def write() -> int:
            await release.wait()
            return 1

        progress = Progress(write, lambda error: None, 1)
        first = progress.mark_and_wait()
        progress.mark()
        second = progress.mark_and_wait()
        await flush()
        release.set()
        pending = await progress.stop()
        assert first.done()
        assert second in pending
        second.set_result(None)

    asyncio.run(main())

"""Find where selected lines of a file are, in bounded memory.

Port of the v1.3.0 ``env/line-scan`` helper. A caller feeds a file's bytes in order; the finished
scan gives byte offsets and decoded sizes for the selected line range.
"""

from __future__ import annotations

import codecs
import math
from typing import Any

from ..truncate import utf8_byte_length
from .decode import range_decoder, starts_with_bom

_NEWLINE = 0x0A
_MAX_SAFE_INTEGER = 9_007_199_254_740_991


class LineScan(dict[str, Any]):
    """Where lines of a file are, as :meth:`BinaryReader.scan_lines` found them."""


class LineScanner:
    """Compute a :class:`LineScan` from a file's bytes fed in order.

    ``start_line`` and ``end_line`` must be non-negative integers with ``end_line > start_line``;
    an absent ``end_line`` means to the end of the file.
    """

    __slots__ = (
        "_bom",
        "_end",
        "_end_line",
        "_first_line",
        "_first_line_bytes",
        "_first_line_end",
        "_head",
        "_last_line_start",
        "_line_start",
        "_newlines",
        "_position",
        "_selected_bytes",
        "_selection",
        "_start",
        "_start_line",
    )

    def __init__(self, start_line: int, end_line: int | None = None) -> None:
        if (
            isinstance(start_line, bool)
            or not isinstance(start_line, int)
            or start_line < 0
            or start_line > _MAX_SAFE_INTEGER
        ):
            raise ValueError("Invalid line range")
        if end_line is not None and (
            isinstance(end_line, bool)
            or not isinstance(end_line, int)
            or end_line <= start_line
            or end_line > _MAX_SAFE_INTEGER
        ):
            raise ValueError("Invalid line range")
        self._start_line = start_line
        self._end_line = math.inf if end_line is None else end_line
        self._position = 0
        self._newlines = 0
        self._line_start = 0
        self._start: int | None = None
        self._end: int | None = None
        self._first_line_end: int | None = None
        self._last_line_start: int | None = None
        self._selected_bytes = 0
        self._first_line_bytes = 0
        self._selection: codecs.IncrementalDecoder | None = None
        self._first_line: codecs.IncrementalDecoder | None = None
        self._head: list[int] | None = []
        self._bom = False
        if start_line == 0:
            self._begin(0)

    def push(self, chunk: bytes) -> None:
        """Feed the next bytes of the file."""

        if self._head is not None:
            take = min(3 - len(self._head), len(chunk))
            self._head.extend(chunk[:take])
            if len(self._head) < 3:
                return
            self._release_head()
            chunk = chunk[take:]
        self._process(chunk)

    def _release_head(self) -> None:
        head = bytes(self._head or ())
        self._head = None
        self._bom = starts_with_bom(head)
        self._process(head)

    def _process(self, chunk: bytes) -> None:
        base = self._position
        consumed = 0
        index = chunk.find(b"\n")
        while index != -1:
            # The newline ends line ``self._newlines``; it belongs to the selection between lines only.
            self._feed(chunk, base, consumed, index)
            line = self._newlines
            position = base + index
            if line == self._start_line:
                self._end_first_line(position)
            if line == self._end_line - 1:
                self._end_selection(position)
            self._feed(chunk, base, index, index + 1)
            consumed = index + 1
            self._newlines += 1
            self._line_start = position + 1
            if self._newlines == self._start_line:
                self._begin(self._line_start)
            if self._newlines == self._end_line - 1:
                self._last_line_start = self._line_start
            index = chunk.find(b"\n", consumed)
        self._feed(chunk, base, consumed, len(chunk))
        self._position += len(chunk)

    def finish(self) -> LineScan:
        """Finish the scan; no more bytes may follow."""

        if self._head is not None:
            self._release_head()
        size = self._position
        if self._start is None:
            return LineScan(
                newlines=self._newlines,
                start=size,
                end=size,
                firstLineEnd=size,
                lastLineStart=size,
                selectedBytes=0,
                firstLineBytes=0,
            )
        if self._first_line_end is None:
            self._end_first_line(size)
        if self._end is None:
            self._end_selection(size)
        return LineScan(
            newlines=self._newlines,
            start=self._start,
            end=self._end if self._end is not None else size,
            firstLineEnd=self._first_line_end if self._first_line_end is not None else size,
            # A selection that reaches past the last line ends with the last line.
            lastLineStart=self._last_line_start if self._last_line_start is not None else self._line_start,
            selectedBytes=self._selected_bytes,
            firstLineBytes=self._first_line_bytes,
        )

    def _begin(self, start: int) -> None:
        self._start = start
        if self._start_line == self._end_line - 1:
            self._last_line_start = start
        self._selection = range_decoder()
        self._first_line = range_decoder()

    def _end_first_line(self, position: int) -> None:
        self._first_line_end = position
        if self._first_line is not None:
            self._first_line_bytes += utf8_byte_length(self._first_line.decode(b"", True))
        self._first_line = None

    def _end_selection(self, position: int) -> None:
        self._end = position
        if self._selection is not None:
            self._selected_bytes += utf8_byte_length(self._selection.decode(b"", True))
        self._selection = None

    def _feed(self, chunk: bytes, base: int, start: int, end: int) -> None:
        """Feed ``chunk[start:end]``, which starts at file offset ``base``, to open decoders."""

        # Decoding the whole file drops a leading byte-order mark.
        if self._bom and base + start < 3:
            start = min(end, 3 - base)
        if end <= start:
            return
        data = chunk[start:end]
        if self._selection is not None:
            self._selected_bytes += utf8_byte_length(self._selection.decode(data, False))
        if self._first_line is not None:
            self._first_line_bytes += utf8_byte_length(self._first_line.decode(data, False))


__all__ = ["LineScanner"]

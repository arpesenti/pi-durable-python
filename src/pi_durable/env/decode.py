"""Streaming UTF-8 decoding that matches decoding a whole byte range at once.

Python's incremental ``utf-8`` decoder keeps a byte-order mark, so the port mirrors the upstream
WHATWG ``TextDecoder`` configuration: the range decoder never strips a mark, and
:class:`StreamDecoder` drops one only when it starts the stream.
"""

from __future__ import annotations

import codecs
from typing import Final

_BOM: Final = b"\xef\xbb\xbf"


def range_decoder() -> codecs.IncrementalDecoder:
    """A decoder for one byte range; the caller strips a leading mark when it starts the file."""

    return codecs.getincrementaldecoder("utf-8")("replace")


def starts_with_bom(first_bytes: bytes) -> bool:
    """Whether decoding the whole input drops its first three bytes as a byte-order mark."""

    return first_bytes[:3] == _BOM


class StreamDecoder:
    """Decode one stream chunk by chunk exactly like decoding all of it at once."""

    __slots__ = ("_decoder", "_started")

    def __init__(self) -> None:
        self._decoder = range_decoder()
        self._started = False

    def decode(self, data: bytes | None = None) -> str:
        """Text for ``data``, holding back an incomplete character; no ``data`` flushes the stream."""

        text = self._decoder.decode(b"", True) if data is None else self._decoder.decode(data, False)
        if self._started or text == "":
            return text
        self._started = True
        # U+FEFF encodes only as EF BB BF, so a leading U+FEFF is exactly a mark at the stream start.
        return text[1:] if text.startswith("\ufeff") else text


__all__ = ["StreamDecoder", "range_decoder", "starts_with_bom"]

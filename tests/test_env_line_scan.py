"""Translated v1.3.0 ``env-line-scan`` cases."""

from __future__ import annotations

import pytest

from pi_durable.env import LineScanner, StreamDecoder


def _random(seed: int):
    state = seed & 0xFFFFFFFF

    def next_value() -> float:
        nonlocal state
        state = (state + 0x6D2B79F5) & 0xFFFFFFFF
        value = state
        value = ((value ^ (value >> 15)) * (value | 1)) & 0xFFFFFFFF
        value ^= (value + ((value ^ (value >> 7)) * (value | 61))) & 0xFFFFFFFF
        return ((value ^ (value >> 14)) & 0xFFFFFFFF) / 4294967296

    return next_value


PIECES = [
    bytes([0x0A]),
    bytes([0x0A]),
    bytes([0x61]),
    bytes([0x62, 0x63]),
    bytes([0xEF, 0xBB, 0xBF]),
    bytes([0xC3, 0xA9]),
    bytes([0xE2, 0x82, 0xAC]),
    bytes([0xF0, 0x9F, 0x98, 0x80]),
    bytes([0xE2, 0x82]),
    bytes([0xFF]),
    bytes([0x80]),
    bytes([0xF0, 0x9F]),
    bytes([0x0D, 0x0A]),
]


def _random_file(next_value) -> bytes:
    result = bytearray()
    pieces = int(next_value() * 60)
    for _ in range(pieces):
        result.extend(PIECES[int(next_value() * len(PIECES))])
    return bytes(result)


def _utf8(text: str) -> int:
    return len(text.encode("utf-8"))


def _decoded(data: bytes) -> str:
    text = data.decode("utf-8", "replace")
    return text[1:] if text.startswith("\ufeff") else text


def test_stream_decoder_keeps_a_bom_that_does_not_start_the_stream() -> None:
    data = bytes([0xE2, 0x82, 0xEF, 0xBB, 0xBF, 0x61])
    decoder = StreamDecoder()
    text = "".join(decoder.decode(bytes([byte])) for byte in data) + decoder.decode()
    assert text == data.decode("utf-8", "replace")
    assert text == "\ufffd\ufeffa"


def test_stream_decoder_decodes_any_chunking_like_decoding_the_whole_stream() -> None:
    for seed in range(1, 5001):
        next_value = _random(seed)
        data = _random_file(next_value)
        decoder = StreamDecoder()
        text = ""
        offset = 0
        while offset < len(data):
            size = 1 + int(next_value() * 7)
            text += decoder.decode(data[offset : offset + size])
            offset += size
        assert text + decoder.decode() == _decoded(data), f"seed {seed}"


def _lines(data: bytes) -> list[str]:
    return _decoded(data).split("\n")


def test_line_scanner_agrees_with_decoding_and_splitting_the_whole_file() -> None:
    for seed in range(1, 5001):
        next_value = _random(seed)
        data = _random_file(next_value)
        lines = _lines(data)
        start_line = int(next_value() * (len(lines) + 2))
        end_line = None if next_value() < 0.3 else start_line + 1 + int(next_value() * (len(lines) + 1))
        scanner = LineScanner(start_line, end_line)
        offset = 0
        while offset < len(data):
            size = 1 + int(next_value() * 7)
            scanner.push(data[offset : offset + size])
            offset += size
        scan = scanner.finish()
        message = f"seed {seed}"
        assert scan["newlines"] == len(lines) - 1, message
        selected = lines[start_line:end_line]

        def decode_range(start: int, end: int, data: bytes = data) -> str:
            payload = data[start:end]
            if start == 0 and payload.startswith(b"\xef\xbb\xbf"):
                payload = payload[3:]
            return payload.decode("utf-8", "replace")

        assert decode_range(scan["start"], scan["end"]) == "\n".join(selected), message
        assert scan["selectedBytes"] == _utf8("\n".join(selected)), message
        if start_line < len(lines):
            assert decode_range(scan["start"], scan["firstLineEnd"]) == lines[start_line], message
            assert scan["firstLineBytes"] == _utf8(lines[start_line]), message
            last_line = min(end_line if end_line is not None else len(lines), len(lines)) - 1
            if last_line + 1 < len(lines):
                last_end = data.index(b"\n", scan["lastLineStart"])
            else:
                last_end = len(data)
            assert decode_range(scan["lastLineStart"], last_end) == lines[last_line], message
        else:
            assert scan["start"] == len(data), message
            assert scan["end"] == len(data), message
            assert scan["selectedBytes"] == 0, message


def test_line_scanner_rejects_empty_or_invalid_ranges() -> None:
    with pytest.raises(ValueError):
        LineScanner(2, 2)
    with pytest.raises(ValueError):
        LineScanner(-1)
    with pytest.raises(ValueError):
        LineScanner(1.5)  # type: ignore[arg-type]

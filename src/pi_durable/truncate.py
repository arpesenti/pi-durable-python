"""Shared truncation utilities for tool outputs.

Port of the v1.3.0 ``truncate.ts``: two independent limits, whichever is hit first wins, and no
partial lines. Tool output streams are bounded by :class:`~pi_durable.harness.output.OutputBuffer`
instead.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_MAX_LINES = 2000
DEFAULT_MAX_BYTES = 50 * 1024


def _well_formed(text: str) -> str:
    """Combine surrogate pairs and replace lone surrogates, like a WHATWG ``TextEncoder``."""

    if not any(0xD800 <= ord(character) <= 0xDFFF for character in text):
        return text
    characters: list[str] = []
    index = 0
    while index < len(text):
        code = ord(text[index])
        if 0xD800 <= code <= 0xDBFF and index + 1 < len(text):
            low = ord(text[index + 1])
            if 0xDC00 <= low <= 0xDFFF:
                characters.append(chr(0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)))
                index += 2
                continue
        if 0xD800 <= code <= 0xDFFF:
            characters.append("\ufffd")
        else:
            characters.append(text[index])
        index += 1
    return "".join(characters)


def utf8_byte_length(text: str) -> int:
    """UTF-8 byte length of ``text``; a lone surrogate counts as three bytes."""

    return len(_well_formed(text).encode("utf-8"))


@dataclass(frozen=True, slots=True)
class TruncationOptions:
    max_lines: int = DEFAULT_MAX_LINES
    max_bytes: int = DEFAULT_MAX_BYTES


@dataclass(frozen=True, slots=True)
class TruncationResult:
    content: str
    truncated: bool
    truncated_by: str | None
    total_lines: int
    total_bytes: int
    output_lines: int
    output_bytes: int
    last_line_partial: bool
    first_line_exceeds_limit: bool
    max_lines: int
    max_bytes: int


def _split_lines_for_counting(content: str) -> list[str]:
    if content == "":
        return []
    lines = content.split("\n")
    if content.endswith("\n"):
        lines.pop()
    return lines


def format_size(bytes_: int) -> str:
    """Format bytes as a human-readable size."""

    if bytes_ < 1024:
        return f"{bytes_}B"
    if bytes_ < 1024 * 1024:
        return f"{bytes_ / 1024:.1f}KB"
    return f"{bytes_ / (1024 * 1024):.1f}MB"


def truncate_head(content: str, options: TruncationOptions | None = None) -> TruncationResult:
    """Keep the first complete lines that fit both limits."""

    return truncate_head_of(
        content,
        (len(_split_lines_for_counting(content)), utf8_byte_length(content)),
        options,
    )


def truncate_head_of(
    prefix: str,
    totals: tuple[int, int],
    options: TruncationOptions | None = None,
) -> TruncationResult:
    """``truncate_head`` of a text known by a prefix and its totals.

    ``prefix`` must be the whole text, or longer than ``max_bytes + 1`` UTF-8 bytes, or hold at
    least ``max_lines`` newlines; then the result equals ``truncate_head`` of the whole text.
    """

    limits = options or TruncationOptions()
    max_lines = limits.max_lines
    max_bytes = limits.max_bytes
    total_lines, total_bytes = totals
    lines = _split_lines_for_counting(prefix)

    if total_lines <= max_lines and total_bytes <= max_bytes:
        return TruncationResult(
            content=prefix,
            truncated=False,
            truncated_by=None,
            total_lines=total_lines,
            total_bytes=total_bytes,
            output_lines=total_lines,
            output_bytes=total_bytes,
            last_line_partial=False,
            first_line_exceeds_limit=False,
            max_lines=max_lines,
            max_bytes=max_bytes,
        )

    first_line_bytes = utf8_byte_length(lines[0])
    if first_line_bytes > max_bytes:
        return TruncationResult(
            content="",
            truncated=True,
            truncated_by="bytes",
            total_lines=total_lines,
            total_bytes=total_bytes,
            output_lines=0,
            output_bytes=0,
            last_line_partial=False,
            first_line_exceeds_limit=True,
            max_lines=max_lines,
            max_bytes=max_bytes,
        )

    output_lines: list[str] = []
    output_bytes_count = 0
    truncated_by: str = "lines"
    for index, line in enumerate(lines):
        if index >= max_lines:
            break
        line_bytes = utf8_byte_length(line) + (1 if index > 0 else 0)
        if output_bytes_count + line_bytes > max_bytes:
            truncated_by = "bytes"
            break
        output_lines.append(line)
        output_bytes_count += line_bytes

    # Without a byte break, only omitted lines prove the line limit was reached.
    if truncated_by != "bytes":
        truncated_by = "lines" if len(output_lines) < total_lines else "bytes"

    output_content = "\n".join(output_lines)
    return TruncationResult(
        content=output_content,
        truncated=True,
        truncated_by=truncated_by,
        total_lines=total_lines,
        total_bytes=total_bytes,
        output_lines=len(output_lines),
        output_bytes=utf8_byte_length(output_content),
        last_line_partial=False,
        first_line_exceeds_limit=False,
        max_lines=max_lines,
        max_bytes=max_bytes,
    )


__all__ = [
    "DEFAULT_MAX_BYTES",
    "DEFAULT_MAX_LINES",
    "TruncationOptions",
    "TruncationResult",
    "format_size",
    "truncate_head",
    "truncate_head_of",
    "utf8_byte_length",
]

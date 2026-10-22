"""Translated v1.3.0 ``env-truncate`` cases."""

from __future__ import annotations

from pi_durable.truncate import (
    TruncationOptions,
    format_size,
    truncate_head,
    utf8_byte_length,
)


def _byte_length(content: str) -> int:
    return utf8_byte_length(content)


def test_reports_utf8_byte_counts_in_truncation_results() -> None:
    content = "aé🙂\nb"
    result = truncate_head(content, None)
    assert result.truncated is False
    assert result.total_bytes == _byte_length(content)
    assert result.output_bytes == _byte_length(content)
    assert result.total_bytes == 9


def test_counts_utf8_bytes_and_truncates_without_partial_characters() -> None:
    for text, expected in [
        ("", 0),
        ("ascii", 5),
        ("é", 2),
        ("中", 3),
        ("🙂", 4),
        ("\ud83d", 3),
        ("\ude42", 3),
        ("a\ud83d\ude42b", 6),
        ("\u07ff\u0800\uffff", 8),
    ]:
        assert _byte_length(text) == expected

    result = truncate_head("aé🙂\nb", None)
    assert result.content == "aé🙂\nb"


def test_does_not_count_a_trailing_newline_as_an_extra_line() -> None:
    content = "\n".join(["line", "line", "line"]) + "\n"

    head = truncate_head(content, TruncationOptions(max_lines=3, max_bytes=100))
    assert head.truncated is False
    assert head.total_lines == 3
    assert head.output_lines == 3


def test_truncates_head_by_line_limits() -> None:

    content = "one\ntwo\nthree\nfour"
    result = truncate_head(content, TruncationOptions(max_bytes=100, max_lines=2))
    assert result.content == "one\ntwo"
    assert result.truncated is True
    assert result.truncated_by == "lines"
    assert result.total_lines == 4
    assert result.output_lines == 2


def test_reports_bytes_when_only_a_trailing_newline_exceeds_limits_at_the_line_cap() -> None:

    result = truncate_head("hello\nworld\n", TruncationOptions(max_bytes=11, max_lines=2))
    assert result.content == "hello\nworld"
    assert result.truncated is True
    assert result.truncated_by == "bytes"
    assert result.total_lines == 2
    assert result.output_lines == 2


def test_truncates_head_on_utf8_byte_limits_without_partial_lines() -> None:

    result = truncate_head("éé\nabc", TruncationOptions(max_bytes=4, max_lines=10))
    assert result.content == "éé"
    assert result.truncated is True
    assert result.truncated_by == "bytes"
    assert result.output_bytes == 4
    assert result.first_line_exceeds_limit is False


def test_reports_head_truncation_when_the_first_line_exceeds_the_byte_limit() -> None:

    result = truncate_head("éé\nabc", TruncationOptions(max_bytes=3, max_lines=10))
    assert result.content == ""
    assert result.truncated is True
    assert result.truncated_by == "bytes"
    assert result.first_line_exceeds_limit is True


def test_formats_sizes() -> None:
    assert format_size(1023) == "1023B"
    assert format_size(1536) == "1.5KB"
    assert format_size(3 * 1024 * 1024) == "3.0MB"


def test_truncate_head_of_matches_truncate_head_for_a_full_prefix() -> None:
    from pi_durable.truncate import truncate_head_of

    content = "one\ntwo\nthree\nfour"
    options = TruncationOptions(max_bytes=100, max_lines=2)
    assert truncate_head_of(content, (4, utf8_byte_length(content)), options) == truncate_head(
        content, options
    )

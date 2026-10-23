"""Translated upstream ``tools-read-differential`` cases.

The reference read decodes the whole file, splits it into lines, and truncates the selection. The
tool must return exactly the same result while reading only a bounded part of the file.
"""

from __future__ import annotations

import math
import struct
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.tools.image import detect_supported_image_mime_type
from pi_durable.tools.image_processor import to_base64
from pi_durable.tools.read import create_read_tool
from pi_durable.truncate import DEFAULT_MAX_BYTES, TruncationResult, format_size, truncate_head


def truthy_number(value: float | None) -> bool:
    """JavaScript truthiness for the numbers an offset or limit may hold."""

    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and not (isinstance(value, float) and math.isnan(value))
        and value != 0
    )


def slice_index(value: float) -> float:
    """``Number`` to a slice index: NaN is 0, other values truncate toward zero."""

    if isinstance(value, float) and math.isnan(value):
        return 0
    if isinstance(value, float) and math.isinf(value):
        return value
    return math.trunc(value)


def js_number_string(value: float | None) -> str:
    """``String(number)`` for the messages the reference builds."""

    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value.is_integer() and abs(value) < 1e21:
            return str(int(value))
        return repr(value)
    return str(value)


def character_end(data: bytes, index: int) -> int:
    """The last character boundary at or before ``index``."""

    end = min(index, len(data))
    while 0 < end < len(data) and (data[end] & 0xC0) == 0x80:
        end -= 1
    return end


def truncation_details(truncation: TruncationResult) -> dict[str, Any]:
    return {
        "truncated": truncation.truncated,
        "truncatedBy": truncation.truncated_by,
        "totalLines": truncation.total_lines,
        "totalBytes": truncation.total_bytes,
        "outputLines": truncation.output_lines,
        "outputBytes": truncation.output_bytes,
        "lastLinePartial": truncation.last_line_partial,
        "firstLineExceedsLimit": truncation.first_line_exceeds_limit,
        "maxLines": truncation.max_lines,
        "maxBytes": truncation.max_bytes,
    }


def reference_read(
    bytes_: bytes,
    path: str,
    offset: float | None,
    limit: float | None,
) -> dict[str, Any]:
    """The ``read`` tool before bounded reads: decode the whole file and truncate the selection."""

    mime_type = detect_supported_image_mime_type(bytes_)
    if mime_type is not None:
        return {
            "output": [{"type": "image", "data": to_base64(bytes_), "mimeType": mime_type}],
            "diagnostics": [
                {
                    "severity": "info",
                    "code": "image",
                    "message": f"Read image file [{mime_type}].",
                }
            ],
        }
    text = bytes_.decode("utf-8", "replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    all_lines = text.split("\n")
    total_file_lines = len(all_lines)
    start_line: float = max(0, offset - 1) if truthy_number(offset) else 0  # type: ignore[operator]
    start_line_display = start_line + 1
    if start_line >= total_file_lines:
        raise RuntimeError(
            f"Offset {js_number_string(offset)} is beyond end of file ({total_file_lines} lines total)"
        )
    user_limited_lines: float | None = None
    slice_start = slice_index(start_line)
    if limit is not None:
        end_line = min(start_line + limit, total_file_lines)
        user_limited_lines = end_line - start_line
        relative_end = slice_index(end_line)
        if relative_end < 0:
            slice_end = max(total_file_lines + relative_end, 0)
        else:
            slice_end = relative_end
        selected_content = "\n".join(all_lines[int(slice_start) : int(slice_end)])
    else:
        selected_content = "\n".join(all_lines[int(slice_start) :])
    truncation = truncate_head(selected_content)
    diagnostics: list[dict[str, Any]] = []
    output_text = truncation.content
    details: dict[str, Any] | None = None
    if truncation.first_line_exceeds_limit:
        integral = float(start_line).is_integer()
        index = int(start_line)
        line_bytes = all_lines[index].encode("utf-8") if integral and 0 <= index < total_file_lines else b""
        end = character_end(line_bytes, DEFAULT_MAX_BYTES)
        output_text = line_bytes[:end].decode("utf-8", "replace")
        diagnostics.append(
            {
                "severity": "warn",
                "code": "truncated",
                "message": (
                    f"Line {js_number_string(start_line_display)} is "
                    f"{format_size(len(line_bytes))}, exceeds the "
                    f"{format_size(DEFAULT_MAX_BYTES)} limit; showing its first "
                    f"{format_size(end)}. Use bash: sed -n "
                    f"'{js_number_string(start_line_display)}p' {path} | tail -c +{end + 1}"
                ),
            }
        )
        details = {
            "truncation": {
                **truncation_details(truncation),
                "outputBytes": end,
                "outputLines": 1,
            }
        }
    elif truncation.truncated:
        end_line_display = start_line_display + truncation.output_lines - 1
        next_offset = end_line_display + 1
        limit_text = (
            "" if truncation.truncated_by == "lines" else f" ({format_size(DEFAULT_MAX_BYTES)} limit)"
        )
        diagnostics.append(
            {
                "severity": "info",
                "code": "truncated",
                "message": (
                    f"Showing lines {js_number_string(start_line_display)}-"
                    f"{js_number_string(end_line_display)} of {total_file_lines}{limit_text}. "
                    f"Use offset={js_number_string(next_offset)} to continue."
                ),
            }
        )
        details = {"truncation": truncation_details(truncation)}
    elif user_limited_lines is not None and start_line + user_limited_lines < total_file_lines:
        remaining = total_file_lines - (start_line + user_limited_lines)
        next_offset = start_line + user_limited_lines + 1
        diagnostics.append(
            {
                "severity": "info",
                "message": (
                    f"{js_number_string(remaining)} more lines in file. Use offset="
                    f"{js_number_string(next_offset)} to continue."
                ),
            }
        )
    result: dict[str, Any] = {
        "output": [] if output_text == "" else [{"type": "text", "text": output_text}],
        "diagnostics": diagnostics,
    }
    if details is not None:
        result["details"] = details
    return result


def random(seed: int) -> Any:
    state = seed & 0xFFFFFFFF

    def imul(left: int, right: int) -> int:
        return ((left & 0xFFFFFFFF) * (right & 0xFFFFFFFF)) & 0xFFFFFFFF

    def next_value() -> float:
        nonlocal state
        state = (state + 0x6D2B79F5) & 0xFFFFFFFF
        value = state
        value = imul(value ^ (value >> 15), value | 1)
        value = (value ^ ((value + imul(value ^ (value >> 7), value | 61)) & 0xFFFFFFFF)) & 0xFFFFFFFF
        return ((value ^ (value >> 14)) & 0xFFFFFFFF) / 4294967296

    return next_value


PIECES = (
    b"\n",
    b"\n",
    b"a",
    b"bcde",
    b"\xef\xbb\xbf",
    b"\xc3\xa9",
    b"\xe2\x82\xac",
    b"\xf0\x9f\x98\x80",
    b"\xe2\x82",
    b"\xff",
    b"\r\n",
)

OFFSETS: tuple[float | None, ...] = (
    None,
    0,
    1,
    2,
    3,
    2.5,
    -4,
    50,
    2001,
    3000,
    1e20,
    float("nan"),
)
LIMITS: tuple[float | None, ...] = (
    None,
    0,
    1,
    2,
    -3,
    -1e20,
    1.5,
    7,
    2000,
    2500,
    1e20,
    float("nan"),
)


def random_file(next_value: Any) -> bytes:
    """Mostly small files, some over the line limit, some over the byte limit, one huge line."""

    kind = next_value()
    if kind < 0.1:
        return b"\xef\xbb\xbf" + b"x\n" * 2100
    if kind < 0.2:
        line = bytearray(b"a" * (DEFAULT_MAX_BYTES + 200 + math.floor(next_value() * 400)))
        line[DEFAULT_MAX_BYTES - 1 + math.floor(next_value() * 3)] = 0xC3
        return bytes(line) + b"\nb"
    if kind < 0.3:
        return (b"0123456789" * 30 + b"\n") * 200
    pieces = bytearray()
    count = math.floor(next_value() * 80)
    for _ in range(count):
        pieces.extend(PIECES[math.floor(next_value() * len(PIECES))])
    return bytes(pieces)


class Api:
    output_window: None = None

    def __init__(self, env: PosixExecutionEnv) -> None:
        self.env = env
        self.models: Any = None

    def output(self, chunk: str | bytes, skipped: Any = None) -> None:
        del chunk, skipped

    def diagnostic(self, diagnostic: Mapping[str, Any]) -> None:
        del diagnostic

    def retained_output(self) -> Mapping[str, Any]:
        return {"text": "", "truncated": False}

    async def details(self, value: Any, context: Any) -> None:
        del value, context

    async def agent(self, context: Any) -> Any:
        del context
        return {}


async def outcome(run: Any) -> Any:
    try:
        result = run()
        if hasattr(result, "__await__"):
            result = await result
        return result
    except Exception as error:  # noqa: BLE001 - the reference compares thrown messages too
        return {"error": str(error)}


async def test_returns_exactly_what_reading_the_whole_file_returned(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))
    tool = create_read_tool()
    execute = tool.execute
    assert execute is not None
    api = Api(env)
    for seed in range(1, 401):
        next_value = random(seed)
        file = random_file(next_value)
        (tmp_path / "f.txt").write_bytes(file)
        for _ in range(4):
            offset = OFFSETS[math.floor(next_value() * len(OFFSETS))]
            limit = LIMITS[math.floor(next_value() * len(LIMITS))]
            args: dict[str, Any] = {"path": "f.txt"}
            if offset is not None:
                args["offset"] = offset
            if limit is not None:
                args["limit"] = limit
            actual = await outcome(lambda args=args: execute(args, api, BACKGROUND_CONTEXT))
            expected = await outcome(
                lambda file=file, offset=offset, limit=limit: reference_read(file, "f.txt", offset, limit)
            )
            assert actual == expected, f"seed {seed} offset {offset} limit {limit}"


async def test_detects_animated_pngs_whose_actl_chunk_lies_far_beyond_the_header(
    tmp_path: Path,
) -> None:
    def chunk(chunk_type: str, length: int) -> bytes:
        return struct.pack(">I", length) + chunk_type.encode("ascii") + b"\x00" * (length + 4)

    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk("IHDR", 13)
        + chunk("iCCP", 200_000)
        + chunk("acTL", 8)
        + chunk("IDAT", 10)
    )
    (tmp_path / "a.png").write_bytes(png)
    env = PosixExecutionEnv(cwd=str(tmp_path))
    tool = create_read_tool()
    execute = tool.execute
    assert execute is not None
    api = Api(env)
    actual = await outcome(lambda: execute({"path": "a.png"}, api, BACKGROUND_CONTEXT))
    expected = await outcome(lambda: reference_read(png, "a.png", None, None))
    assert actual == expected

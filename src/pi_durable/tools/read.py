"""The ``read`` coding tool: bounded text reads and image blocks.

Port of the v1.3.0 ``src/tools/read.ts``. Text is read through ``openBinaryReader`` and
``scanLines``, so a read costs one scan of the file plus the shown head; images are detected by
content and returned as one image block.
"""

from __future__ import annotations

import inspect
import math
from collections.abc import Mapping
from typing import Any, TypedDict, cast

from ..context import Context
from ..env import (
    BinaryReader,
    FileInfo,
    get_or_throw,
    range_decoder,
    starts_with_bom,
)
from ..harness.define import define_tool
from ..harness.types import ToolDiagnostic, ToolExecutionApi, ToolExecutionResult, ToolRegistration
from ..json import JsonValue
from ..truncate import (
    DEFAULT_MAX_BYTES,
    DEFAULT_MAX_LINES,
    TruncationResult,
    format_size,
    truncate_head_of,
    utf8_byte_length,
)
from ._values import field, field_either, js_number_string
from .env import require_env
from .image import (
    ReaderByteSource,
    detect_supported_image_mime_type_of,
    image_dimensions,
)
from .image_processor import (
    DEFAULT_IMAGE_LIMITS,
    INLINE_IMAGE_TYPES,
    ImageLimits,
    ImageProcessor,
    base64_length,
    to_base64,
)
from .path_utils import resolve_read_tool_path

READ_CHUNK = 64 * 1024


class ReadToolInput(TypedDict, total=False):
    """Arguments of one ``read`` call."""

    path: str
    offset: int | float
    limit: int | float


class ReadToolDetails(TypedDict, total=False):
    """How the shown text was cut; the text itself is the result content."""

    truncation: dict[str, Any]


class ReadToolOptions(TypedDict, total=False):
    """Options of :func:`create_read_tool`."""

    images: ImageProcessor


READ_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "path": {
            "type": "string",
            "description": "Path to the file to read (relative or absolute)",
        },
        "offset": {
            "type": "number",
            "description": "Line number to start reading from (1-indexed)",
        },
        "limit": {
            "type": "number",
            "description": "Maximum number of lines to read",
        },
    },
    "required": ["path"],
}


def _is_safe_integer(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return False
    return abs(value) <= 9_007_199_254_740_991


def _slice_index(value: float) -> float:
    if isinstance(value, float) and math.isnan(value):
        return 0
    if isinstance(value, float) and math.isinf(value):
        return value
    return math.trunc(value)


def _truthy_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and not (isinstance(value, float) and math.isnan(value))
        and value != 0
    )


def _is_integral(value: float) -> bool:
    return not isinstance(value, bool) and float(value).is_integer()


def _character_end(data: bytes, index: int) -> int:
    """The last character boundary at or before ``index``."""

    end = min(index, len(data))
    while 0 < end < len(data) and (data[end] & 0xC0) == 0x80:
        end -= 1
    return end


def _truncation_details(truncation: TruncationResult) -> dict[str, Any]:
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


async def _read_head(
    reader: BinaryReader,
    start: int,
    end: int,
    skip_bom: bool,
    context: Context,
) -> str:
    """The decoded start of ``[start, end)``, enough for :func:`truncate_head_of`."""

    decoder = range_decoder()
    text = ""
    newlines = 0
    position = 3 if skip_bom and start == 0 else start
    while position < end:
        data = get_or_throw(await reader.read(position, min(READ_CHUNK, end - position), context))
        if len(data) == 0:
            break
        position += len(data)
        decoded = decoder.decode(data, False)
        text += decoded
        newlines += decoded.count("\n")
        if newlines >= DEFAULT_MAX_LINES or utf8_byte_length(text) > DEFAULT_MAX_BYTES + 1:
            return text
    return text + decoder.decode(b"", True)


async def _read_text(
    reader: BinaryReader,
    path: str,
    offset: int | float | None,
    limit: int | float | None,
    context: Context,
) -> ToolExecutionResult:
    start_line: float = max(0, offset - 1) if _truthy_number(offset) else 0  # type: ignore[operator]
    start_line_display = start_line + 1
    # Lines are selected like ``allLines.slice(startLine, endLine)``, which truncates fractional indices.
    slice_start = _slice_index(start_line)
    # A start beyond any file is scanned from 0 only to count lines; the offset check below then fails.
    scan_start = int(slice_start) if _is_safe_integer(slice_start) else 0
    requested_end: float | None = (
        None if limit is None else max(scan_start + 1, _slice_index(start_line + limit))
    )
    scan_end = int(requested_end) if requested_end is not None and _is_safe_integer(requested_end) else None

    async def scan_of(end_line: int | None) -> dict[str, Any]:
        options: dict[str, Any] = {"start_line": scan_start}
        if end_line is not None:
            options["end_line"] = end_line
        return get_or_throw(await reader.scan_lines(options, context))

    scan = await scan_of(scan_end)
    total_file_lines = scan["newlines"] + 1
    if start_line >= total_file_lines:
        raise RuntimeError(
            f"Offset {js_number_string(offset)} is beyond end of file ({total_file_lines} lines total)"
        )

    user_limited_lines: float | None = None
    selected_line_count: float = total_file_lines - slice_start
    if limit is not None:
        end_line = min(start_line + limit, total_file_lines)
        user_limited_lines = end_line - start_line
        # ``slice`` counts a negative end from the end of the lines; scan again for it.
        relative_end = _slice_index(end_line)
        if relative_end < 0:
            slice_end = max(total_file_lines + relative_end, 0)
        else:
            slice_end = relative_end
        selected_line_count = max(0, slice_end - slice_start)
        if selected_line_count > 0 and relative_end < 0:
            scan = await scan_of(int(slice_end))

    empty = selected_line_count == 0
    # Counted like ``truncate_head``: a trailing newline adds no line, and empty text has none.
    ends_with_newline = (
        not empty and scan["lastLineStart"] == scan["end"] and scan["lastLineStart"] > scan["start"]
    )
    total_lines = (
        0
        if empty or scan["selectedBytes"] == 0
        else int(selected_line_count) - (1 if ends_with_newline else 0)
    )
    total_bytes = 0 if empty else scan["selectedBytes"]
    first_bytes = get_or_throw(await reader.read(0, 3, context))
    head = (
        ""
        if empty
        else await _read_head(
            reader,
            scan["start"],
            scan["end"],
            starts_with_bom(first_bytes),
            context,
        )
    )

    truncation = truncate_head_of(head, (total_lines, total_bytes))
    diagnostics: list[ToolDiagnostic] = []
    output_text = truncation.content
    details: ReadToolDetails | None = None
    if truncation.first_line_exceeds_limit:
        # Show the start of the line, cut at the byte limit on a character boundary.
        integral = _is_integral(start_line)
        line_bytes = head.split("\n")[0].encode("utf-8") if integral else b""
        line_size = scan["firstLineBytes"] if integral else 0
        end = _character_end(line_bytes, DEFAULT_MAX_BYTES)
        output_text = line_bytes[:end].decode("utf-8", "replace")
        diagnostics.append(
            ToolDiagnostic(
                severity="warn",
                code="truncated",
                message=(
                    f"Line {js_number_string(start_line_display)} is {format_size(line_size)}, "
                    f"exceeds the {format_size(DEFAULT_MAX_BYTES)} limit; showing its first "
                    f"{format_size(end)}. Use bash: sed -n "
                    f"'{js_number_string(start_line_display)}p' {path} | tail -c +{end + 1}"
                ),
            )
        )
        details = {
            "truncation": {
                **_truncation_details(truncation),
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
            ToolDiagnostic(
                severity="info",
                code="truncated",
                message=(
                    f"Showing lines {js_number_string(start_line_display)}-"
                    f"{js_number_string(end_line_display)} of {total_file_lines}{limit_text}. "
                    f"Use offset={js_number_string(next_offset)} to continue."
                ),
            )
        )
        details = {"truncation": _truncation_details(truncation)}
    elif user_limited_lines is not None and start_line + user_limited_lines < total_file_lines:
        remaining = total_file_lines - (start_line + user_limited_lines)
        next_offset = start_line + user_limited_lines + 1
        diagnostics.append(
            ToolDiagnostic(
                severity="info",
                message=(
                    f"{js_number_string(remaining)} more lines in file. Use offset="
                    f"{js_number_string(next_offset)} to continue."
                ),
            )
        )

    output: list[Any] = [] if output_text == "" else [{"type": "text", "text": output_text}]
    result: ToolExecutionResult = {"output": output, "diagnostics": diagnostics}
    if details is not None:
        result["details"] = cast(JsonValue, details)
    return result


class _ImageModel(TypedDict):
    vision: bool
    limits: ImageLimits


async def _image_model(api: ToolExecutionApi, context: Context) -> _ImageModel:
    """The conversation model's image input, overridden by its ``inputLimits.images.resize``."""

    agent = await api.agent(context)
    ref = field(agent, "model")
    model: Any = None
    if ref is not None:
        provider = field(ref, "provider")
        model_id = field_either(ref, "model_id", "modelId")
        models = getattr(api, "models", None)
        get_model = getattr(models, "get_model", None)
        if callable(get_model) and provider is not None and model_id is not None:
            model = get_model(provider, model_id)
    input_types = field(model, "input")
    limits_source = field_either(model, "input_limits", "inputLimits")
    resize = field(field(limits_source, "images"), "resize")
    limits: ImageLimits = {
        "max_width": field_either(resize, "max_width", "maxWidth", DEFAULT_IMAGE_LIMITS["max_width"]),
        "max_height": field_either(resize, "max_height", "maxHeight", DEFAULT_IMAGE_LIMITS["max_height"]),
        "max_bytes": field_either(resize, "max_bytes", "maxBytes", DEFAULT_IMAGE_LIMITS["max_bytes"]),
    }
    return {
        "vision": True if input_types is None else "image" in input_types,
        "limits": limits,
    }


def _image_error(message: str) -> ToolExecutionResult:
    return {
        "output": [],
        "isError": True,
        "diagnostics": [ToolDiagnostic(severity="error", code="unsupported_image", message=message)],
    }


async def _read_image(
    reader: BinaryReader,
    info: FileInfo,
    path: str,
    mime_type: str,
    processor: ImageProcessor | None,
    image_model: _ImageModel,
    context: Context,
) -> ToolExecutionResult:
    """One image block, prepared by ``processor`` or passed through within the limits."""

    notes: list[str] = []
    data: str
    output_mime: str
    if processor is not None:
        bytes_ = get_or_throw(await reader.read(0, info["size"], context))
        raw_prepared: Any = processor.prepare(bytes_, mime_type, image_model["limits"])
        prepared = await raw_prepared if inspect.isawaitable(raw_prepared) else raw_prepared
        if prepared is None:
            return _image_error(f"{path} is an image ({mime_type}) that cannot be prepared for the model")
        data = field(prepared, "data", "")
        output_mime = field_either(prepared, "mime_type", "mimeType", mime_type)
        converted_from = field_either(prepared, "converted_from", "convertedFrom")
        if converted_from is not None:
            notes.append(f"Converted from {converted_from} to {output_mime}.")
        resized = field(prepared, "resized")
        if resized is not None:
            source = field(resized, "from")
            target = field(resized, "to")
            from_width = field(source, "width")
            from_height = field(source, "height")
            to_width = field(target, "width")
            to_height = field(target, "height")
            if None not in (from_width, from_height, to_width, to_height) and to_width:
                scale = f"{from_width / to_width:.2f}"
                notes.append(
                    f"Resized from {from_width}x{from_height} to {to_width}x{to_height}. "
                    f"Multiply coordinates by {scale} to map them to the original."
                )
    else:
        if mime_type not in INLINE_IMAGE_TYPES:
            return _image_error(
                f"{path} is an image ({mime_type}) that needs converting, and no image processor "
                "is configured"
            )
        unshrinkable = "and no image processor is configured to shrink it"
        limits = image_model["limits"]
        if base64_length(info["size"]) > limits["max_bytes"]:
            return _image_error(
                f"{path} is an image ({mime_type}) of {format_size(info['size'])}, too large to "
                f"send, {unshrinkable}"
            )
        bytes_ = get_or_throw(await reader.read(0, info["size"], context))
        size = image_dimensions(bytes_, mime_type)
        if size is None:
            return _image_error(f"{path} is an image ({mime_type}) whose size cannot be read")
        if size["width"] > limits["max_width"] or size["height"] > limits["max_height"]:
            return _image_error(
                f"{path} is an image ({mime_type}) of {size['width']}x{size['height']}, larger "
                f"than {limits['max_width']}x{limits['max_height']}, {unshrinkable}"
            )
        data = to_base64(bytes_)
        output_mime = mime_type

    if not image_model["vision"]:
        notes.append("The current model does not support images; it sees a placeholder instead.")
    message = " ".join([f"Read image file [{output_mime}].", *notes])
    return {
        "output": [{"type": "image", "data": data, "mimeType": output_mime}],
        "diagnostics": [ToolDiagnostic(severity="info", code="image", message=message)],
    }


def create_read_tool(
    options: ReadToolOptions | Mapping[str, Any] | None = None,
    *,
    images: ImageProcessor | None = None,
) -> ToolRegistration:
    """Read text files and images.

    Truncation, continuation, and image remarks are diagnostics; the content is only file text, or
    one image, which programs get as its ``ImageContent``.
    """

    values: dict[str, Any] = dict(options or {})
    if images is not None:
        values["images"] = images
    processor = values.get("images")
    formats = "jpg, png, gif, webp" if processor is None else "jpg, png, gif, webp, bmp"

    async def execute(
        args: Mapping[str, Any], api: ToolExecutionApi, context: Context
    ) -> ToolExecutionResult:
        path = str(args["path"])
        offset = args.get("offset")
        limit = args.get("limit")
        env = require_env(api)
        absolute_path = await resolve_read_tool_path(env, path, context)
        reader = get_or_throw(await env.open_binary_reader(absolute_path, None, context))
        try:
            # A concurrent writer can change the file between the scan and the reads. Appending (a
            # growing log) leaves the scanned text as it was; an image must be whole. Otherwise the
            # file is read again once.
            attempt = 0
            while True:
                before = get_or_throw(await reader.info(context))
                mime_type = await detect_supported_image_mime_type_of(
                    ReaderByteSource(reader, before["size"], context)
                )
                if mime_type is None:
                    result = await _read_text(reader, path, offset, limit, context)
                else:
                    result = await _read_image(
                        reader,
                        before,
                        path,
                        mime_type,
                        processor,
                        await _image_model(api, context),
                        context,
                    )
                after = get_or_throw(await reader.info(context))
                unchanged = after["size"] == before["size"] and after["mtimeMs"] == before["mtimeMs"]
                if unchanged or (mime_type is None and after["size"] > before["size"]):
                    return result
                if attempt == 1:
                    raise RuntimeError(f"{path} changed while it was read")
                attempt += 1
        finally:
            await reader.close(context)

    return define_tool(
        {
            "name": "read",
            "description": (
                "Read the contents of a file. Supports text files and images "
                f"({formats}). Images are sent as attachments. For text files, output is "
                f"truncated to {DEFAULT_MAX_LINES} lines or {DEFAULT_MAX_BYTES // 1024}KB "
                "(whichever is hit first). Use offset/limit for large files. When you need the "
                "full file, continue with offset until complete."
            ),
            "parameters": READ_SCHEMA,
            "execute": execute,
        }
    )


__all__ = [
    "READ_CHUNK",
    "READ_SCHEMA",
    "ReadToolDetails",
    "ReadToolInput",
    "ReadToolOptions",
    "create_read_tool",
]

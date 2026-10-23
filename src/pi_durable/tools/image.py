"""Image type and header detection for the ``read`` tool.

Port of the v1.3.0 ``src/tools/image.ts``. Only headers and, for PNG, the chunk headers up to the
first ``acTL`` or ``IDAT`` are read; image pixels are never decoded here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..context import Context
from ..env import BinaryReader, get_or_throw

PNG_SIGNATURE = (0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A)
#: Bytes every check except the APNG chunk walk needs: BMP reads up to offset 29.
HEADER_BYTES = 32
BLOCK_BYTES = 64 * 1024


class ByteSource(Protocol):
    """Positional reads of a file of ``size`` bytes."""

    size: int

    async def read(self, offset: int, length: int) -> bytes: ...


async def detect_supported_image_mime_type_of(source: ByteSource) -> str | None:
    """``detect_supported_image_mime_type`` of a whole file, reading only its header."""

    header = await source.read(0, HEADER_BYTES)
    if not _starts_with(header, PNG_SIGNATURE):
        return detect_supported_image_mime_type(header)
    if _is_png(header) and not await _is_animated_png_of(source):
        return "image/png"
    return None


async def _is_animated_png_of(source: ByteSource) -> bool:
    block = b""
    block_start = 0

    async def bytes_at(offset: int, length: int) -> bytes:
        nonlocal block, block_start
        if offset < block_start or offset + length > block_start + len(block):
            block_start = offset
            block = await source.read(offset, BLOCK_BYTES)
        return block[offset - block_start : offset - block_start + length]

    offset = len(PNG_SIGNATURE)
    while offset + 8 <= source.size:
        chunk_header = await bytes_at(offset, 8)
        chunk_length = _read_uint32_be(chunk_header, 0)
        if _starts_with_ascii(chunk_header, 4, "acTL"):
            return True
        if _starts_with_ascii(chunk_header, 4, "IDAT"):
            return False
        next_offset = offset + 8 + chunk_length + 4
        if next_offset <= offset or next_offset > source.size:
            return False
        offset = next_offset
    return False


def detect_supported_image_mime_type(buffer: bytes) -> str | None:
    """The MIME type of a supported image, or ``None``."""

    if _starts_with(buffer, (0xFF, 0xD8, 0xFF)):
        return None if _byte(buffer, 3) == 0xF7 else "image/jpeg"
    if _starts_with(buffer, PNG_SIGNATURE):
        return "image/png" if _is_png(buffer) and not _is_animated_png(buffer) else None
    if _starts_with_ascii(buffer, 0, "GIF87a") or _starts_with_ascii(buffer, 0, "GIF89a"):
        return "image/gif"
    if _starts_with_ascii(buffer, 0, "RIFF") and _starts_with_ascii(buffer, 8, "WEBP"):
        return "image/webp"
    if _starts_with_ascii(buffer, 0, "BM") and _is_bmp(buffer):
        return "image/bmp"
    return None


def _is_png(buffer: bytes) -> bool:
    return (
        len(buffer) >= 16
        and _read_uint32_be(buffer, len(PNG_SIGNATURE)) == 13
        and _starts_with_ascii(buffer, 12, "IHDR")
    )


def _is_animated_png(buffer: bytes) -> bool:
    offset = len(PNG_SIGNATURE)
    while offset + 8 <= len(buffer):
        chunk_length = _read_uint32_be(buffer, offset)
        chunk_type_offset = offset + 4
        if _starts_with_ascii(buffer, chunk_type_offset, "acTL"):
            return True
        if _starts_with_ascii(buffer, chunk_type_offset, "IDAT"):
            return False
        next_offset = offset + 8 + chunk_length + 4
        if next_offset <= offset or next_offset > len(buffer):
            return False
        offset = next_offset
    return False


def image_dimensions(bytes_: bytes, mime_type: str) -> dict[str, int] | None:
    """The width and height an image declares in its header, without decoding it."""

    length = len(bytes_)
    if mime_type == "image/png":
        if length < 24:
            return None
        return {"width": _read_uint32_be(bytes_, 16), "height": _read_uint32_be(bytes_, 20)}
    if mime_type == "image/gif":
        if length < 10:
            return None
        return {"width": _read_uint16_le(bytes_, 6), "height": _read_uint16_le(bytes_, 8)}
    if mime_type == "image/webp":
        if length < 30:
            return None
        if _starts_with_ascii(bytes_, 12, "VP8 "):
            return {
                "width": _read_uint16_le(bytes_, 26) & 0x3FFF,
                "height": _read_uint16_le(bytes_, 28) & 0x3FFF,
            }
        if _starts_with_ascii(bytes_, 12, "VP8L"):
            bits = _read_uint32_le(bytes_, 21)
            return {
                "width": (bits & 0x3FFF) + 1,
                "height": ((bits >> 14) & 0x3FFF) + 1,
            }
        if _starts_with_ascii(bytes_, 12, "VP8X"):

            def uint24(offset: int) -> int:
                return _read_uint16_le(bytes_, offset) + (_byte(bytes_, offset + 2) << 16)

            return {"width": uint24(24) + 1, "height": uint24(27) + 1}
        return None
    if mime_type == "image/jpeg":
        offset = 2
        while offset + 9 <= length:
            if _byte(bytes_, offset) != 0xFF:
                return None
            marker = _byte(bytes_, offset + 1)
            if marker == 0xFF:
                offset += 1
                continue
            # SOF0 to SOF15, except DHT (C4), JPG (C8), and DAC (CC), which share the range.
            if 0xC0 <= marker <= 0xCF and marker not in (0xC4, 0xC8, 0xCC):
                return {
                    "width": _read_uint16_be(bytes_, offset + 7),
                    "height": _read_uint16_be(bytes_, offset + 5),
                }
            offset += 2 + _read_uint16_be(bytes_, offset + 2)
        return None
    return None


def _is_bmp(buffer: bytes) -> bool:
    if len(buffer) < 26:
        return False
    declared_file_size = _read_uint32_le(buffer, 2)
    pixel_data_offset = _read_uint32_le(buffer, 10)
    dib_header_size = _read_uint32_le(buffer, 14)
    if declared_file_size != 0 and declared_file_size < 26:
        return False
    if pixel_data_offset < 14 + dib_header_size:
        return False
    if declared_file_size != 0 and pixel_data_offset >= declared_file_size:
        return False

    if dib_header_size == 12:
        color_planes = _read_uint16_le(buffer, 22)
        bits_per_pixel = _read_uint16_le(buffer, 24)
    elif 40 <= dib_header_size <= 124:
        if len(buffer) < 30:
            return False
        color_planes = _read_uint16_le(buffer, 26)
        bits_per_pixel = _read_uint16_le(buffer, 28)
    else:
        return False
    return color_planes == 1 and bits_per_pixel in (1, 4, 8, 16, 24, 32)


def _byte(buffer: bytes, offset: int) -> int:
    return buffer[offset] if offset < len(buffer) else 0


def _read_uint16_le(buffer: bytes, offset: int) -> int:
    return _byte(buffer, offset) + (_byte(buffer, offset + 1) << 8)


def _read_uint16_be(buffer: bytes, offset: int) -> int:
    return (_byte(buffer, offset) << 8) + _byte(buffer, offset + 1)


def _read_uint32_be(buffer: bytes, offset: int) -> int:
    return (
        _byte(buffer, offset) * 0x1000000
        + (_byte(buffer, offset + 1) << 16)
        + (_byte(buffer, offset + 2) << 8)
        + _byte(buffer, offset + 3)
    )


def _read_uint32_le(buffer: bytes, offset: int) -> int:
    return (
        _byte(buffer, offset)
        + (_byte(buffer, offset + 1) << 8)
        + (_byte(buffer, offset + 2) << 16)
        + _byte(buffer, offset + 3) * 0x1000000
    )


def _starts_with(buffer: bytes, values: tuple[int, ...]) -> bool:
    if len(buffer) < len(values):
        return False
    return all(buffer[index] == value for index, value in enumerate(values))


def _starts_with_ascii(buffer: bytes, offset: int, text: str) -> bool:
    if len(buffer) < offset + len(text):
        return False
    return all(buffer[offset + index] == ord(character) for index, character in enumerate(text))


@dataclass(slots=True)
class ReaderByteSource:
    """A :class:`ByteSource` over an environment's ``BinaryReader``."""

    reader: BinaryReader
    size: int
    context: Context

    async def read(self, offset: int, length: int) -> bytes:
        return get_or_throw(await self.reader.read(offset, length, self.context))


__all__ = [
    "BLOCK_BYTES",
    "HEADER_BYTES",
    "PNG_SIGNATURE",
    "ByteSource",
    "ReaderByteSource",
    "detect_supported_image_mime_type",
    "detect_supported_image_mime_type_of",
    "image_dimensions",
]

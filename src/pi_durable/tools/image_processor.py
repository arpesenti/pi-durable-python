"""Image preparation protocol for the ``read`` tool.

Port of the v1.3.0 ``src/tools/image-processor.ts``. There is no Python image implementation; an
application supplies an :class:`ImageProcessor` when it wants ``read`` to resize or convert images.
"""

from __future__ import annotations

import base64
from typing import Any, Protocol, TypedDict


class ImageLimits(TypedDict):
    """Limits an image the model sees must keep."""

    max_width: int
    """Pixels. Default 2000."""

    max_height: int
    """Pixels. Default 2000."""

    max_bytes: int
    """Bytes of base64. Default 4.5 MiB, below Anthropic's 5 MB."""


DEFAULT_IMAGE_LIMITS: ImageLimits = {
    "max_width": 2000,
    "max_height": 2000,
    "max_bytes": int(4.5 * 1024 * 1024),
}


class PreparedImage(TypedDict, total=False):
    """An image the model can take: a supported format, within the limits, as base64."""

    data: str
    mime_type: str
    """The image's format, such as ``image/png``."""

    resized: dict[str, Any]
    """Set when the image was resized: its size before and after, for the model to map coordinates back."""

    converted_from: str
    """The original format, set when it differs from ``mime_type``."""


class ImageProcessor(Protocol):
    """Decodes, orients, resizes, and re-encodes images so the model can take them."""

    async def prepare(self, data: bytes, mime_type: str, limits: ImageLimits) -> PreparedImage | None: ...


#: Formats every provider takes inline; others, such as BMP, need converting.
INLINE_IMAGE_TYPES: frozenset[str] = frozenset({"image/png", "image/jpeg", "image/gif", "image/webp"})


def to_base64(data: bytes) -> str:
    """Standard base64 of ``data``."""

    return base64.b64encode(data).decode("ascii")


def base64_length(byte_length: int) -> int:
    """The length of the base64 of ``byte_length`` bytes."""

    return ((byte_length + 2) // 3) * 4


__all__ = [
    "DEFAULT_IMAGE_LIMITS",
    "INLINE_IMAGE_TYPES",
    "ImageLimits",
    "ImageProcessor",
    "PreparedImage",
    "base64_length",
    "to_base64",
]

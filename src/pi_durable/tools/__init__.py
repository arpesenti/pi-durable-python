"""Built-in coding tools.

Port of the v1.3.0 ``src/tools/index.ts``. ``read``, ``write``, ``edit``, and ``bash`` use only
``api.env``; nothing installs them automatically. PowerShell is excluded, and there is no image
implementation: ``read`` optionally takes an :class:`~pi_durable.tools.ImageProcessor`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..harness.define import define_extension
from ..harness.types import Extension
from .bash import (
    BashExecution,
    BashPrepare,
    BashToolInput,
    BashToolOptions,
    create_bash_tool,
)
from .edit import EditToolDetails, EditToolInput, create_edit_tool
from .image_processor import (
    DEFAULT_IMAGE_LIMITS,
    ImageLimits,
    ImageProcessor,
    PreparedImage,
)
from .read import ReadToolDetails, ReadToolInput, ReadToolOptions, create_read_tool
from .write import WriteToolInput, create_write_tool


def create_coding_tools(
    options: ReadToolOptions | Mapping[str, Any] | None = None,
    *,
    images: ImageProcessor | None = None,
) -> Extension:
    """The ``coding-tools`` extension: ``read``, ``write``, ``edit``, and ``bash``.

    ``images`` prepares the images ``read`` returns.
    """

    return define_extension(
        {
            "name": "coding-tools",
            "tools": [
                create_read_tool(options, images=images),
                create_write_tool(),
                create_edit_tool(),
                create_bash_tool(),
            ],
        }
    )


#: :func:`create_coding_tools` without an image processor.
CodingTools = create_coding_tools()


__all__ = [
    "BashExecution",
    "BashPrepare",
    "BashToolInput",
    "BashToolOptions",
    "CodingTools",
    "DEFAULT_IMAGE_LIMITS",
    "EditToolDetails",
    "EditToolInput",
    "ImageLimits",
    "ImageProcessor",
    "PreparedImage",
    "ReadToolDetails",
    "ReadToolInput",
    "ReadToolOptions",
    "WriteToolInput",
    "create_bash_tool",
    "create_coding_tools",
    "create_edit_tool",
    "create_read_tool",
    "create_write_tool",
]

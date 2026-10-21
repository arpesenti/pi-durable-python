"""Portable execution-environment capability.

This module publishes the ``FileSystem`` capability and its result, error, and metadata shapes.
Shell execution and the full POSIX backend arrive with the ``env`` ticket; the JSONL backend
depends only on this filesystem capability.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import (
    Any,
    ClassVar,
    Generic,
    Literal,
    Protocol,
    TypeAlias,
    TypeVar,
    runtime_checkable,
)

from ..context import Context

TValue = TypeVar("TValue")
TError = TypeVar("TError")


@dataclass(frozen=True, slots=True)
class Ok(Generic[TValue]):
    """A successful result."""

    value: TValue
    ok: ClassVar[Literal[True]] = True


@dataclass(frozen=True, slots=True)
class Err(Generic[TError]):
    """A failed result carrying the expected error."""

    error: TError
    ok: ClassVar[Literal[False]] = False


Result: TypeAlias = Ok[TValue] | Err[TError]
"""The outcome of a fallible operation; expected failures are returned instead of thrown."""


def ok(value: TValue) -> Result[TValue, TError]:
    return Ok(value)


def err(error: TError) -> Result[TValue, TError]:
    return Err(error)


def get_or_throw(result: Result[TValue, Any]) -> TValue:
    if isinstance(result, Err):
        error = result.error
        if isinstance(error, BaseException):
            raise error
        raise RuntimeError(to_error(error))
    return result.value


def get_or_undefined(result: Result[TValue, Any]) -> TValue | None:
    return None if isinstance(result, Err) else result.value


def to_error(error: object) -> Exception:
    """Normalize any thrown value to an exception without throwing while formatting it."""

    if isinstance(error, Exception):
        return error
    if isinstance(error, str):
        return RuntimeError(error)
    try:
        return RuntimeError(json.dumps(error))
    except Exception:
        return RuntimeError(str(error))


FileKind: TypeAlias = Literal["file", "directory", "symlink"]

FileErrorCode: TypeAlias = Literal[
    "aborted",
    "not_found",
    "permission_denied",
    "not_directory",
    "is_directory",
    "invalid",
    "not_supported",
    "unknown",
]


class FileError(Exception):
    """A filesystem operation failed. ``code`` classifies the expected failure."""

    def __init__(
        self,
        code: FileErrorCode,
        message: str,
        path: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.name = "FileError"
        self.code = code
        self.path = path
        if cause is not None:
            self.__cause__ = cause


class FileInfo(dict[str, Any]):
    """Metadata of one file, directory, or symbolic link.

    A plain ``dict`` subclass exposing the upstream fields: ``name``, ``path``, ``kind``,
    ``size``, and ``mtimeMs``.
    """


class TextLine(dict[str, Any]):
    """One line of text and whether it ended with a terminator."""


class TextLineReader(Protocol):
    async def read_line(self, context: Context) -> Result[TextLine | None, FileError]: ...

    async def close(self, context: Context) -> None: ...


class LineScan(dict[str, Any]):
    """Where lines of a file are, as :meth:`BinaryReader.scan_lines` found them."""


class BinaryReader(Protocol):
    """Positional reads from one opened regular file; all calls see the same file if renamed."""

    async def info(self, context: Context) -> Result[FileInfo, FileError]: ...

    async def read(self, offset: int, length: int, context: Context) -> Result[bytes, FileError]: ...

    async def scan_lines(
        self, options: dict[str, Any], context: Context
    ) -> Result[LineScan, FileError]: ...

    async def close(self, context: Context) -> None: ...


class WatchTarget(dict[str, Any]):
    """A file or directory to watch."""


class WatchPaths(dict[str, Any]):
    paths: list[str]


class WatchOverflow(dict[str, Any]):
    overflow: Literal[True]


class WatchError(dict[str, Any]):
    error: FileError


WatchChange: TypeAlias = WatchPaths | WatchOverflow | WatchError


class FileWatcher(Protocol):
    mode: Literal["native", "polling"]

    async def close(self, context: Context) -> None: ...


class DirPage(dict[str, Any]):
    entries: list[FileInfo]
    done: bool


class DirReader(Protocol):
    async def next(self, max_entries: int, context: Context) -> Result[DirPage, FileError]: ...

    async def close(self, context: Context) -> None: ...


class ListFilesOptions(dict[str, Any]):
    """Options for the optional ``FileSystem.list_files``."""


class FileListEntry(dict[str, Any]):
    """One entry of a :class:`FileListing`."""


class FileListing(dict[str, Any]):
    """Files, directories, and symbolic links below a directory."""


@runtime_checkable
class FileSystem(Protocol):
    """Portable filesystem capability. Operations return failures rather than throwing."""

    id: str
    cwd: str

    async def absolute_path(self, path: str, context: Context) -> Result[str, FileError]: ...

    async def join_path(self, parts: Sequence[str], context: Context) -> Result[str, FileError]: ...

    async def read_text_file(self, path: str, context: Context) -> Result[str, FileError]: ...

    async def open_text_line_reader(
        self, path: str, context: Context
    ) -> Result[TextLineReader, FileError]: ...

    async def read_text_lines(
        self,
        path: str,
        options: dict[str, Any] | None,
        context: Context,
    ) -> Result[list[str], FileError]: ...

    async def read_binary_file(self, path: str, context: Context) -> Result[bytes, FileError]: ...

    async def open_binary_reader(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[BinaryReader, FileError]: ...

    async def write_file(
        self, path: str, content: str | bytes, context: Context
    ) -> Result[None, FileError]: ...

    async def append_file(
        self, path: str, content: str | bytes, context: Context
    ) -> Result[None, FileError]: ...

    async def truncate_file(
        self, path: str, size: int, context: Context
    ) -> Result[None, FileError]: ...

    async def flush_file(self, path: str, context: Context) -> Result[None, FileError]: ...

    async def rename_file(
        self, source_path: str, destination_path: str, context: Context
    ) -> Result[None, FileError]: ...

    async def file_info(self, path: str, context: Context) -> Result[FileInfo, FileError]: ...

    async def list_dir(self, path: str, context: Context) -> Result[list[FileInfo], FileError]: ...

    async def list_files(
        self,
        path: str,
        options: ListFilesOptions | None,
        context: Context,
    ) -> Result[FileListing, FileError]: ...

    async def open_dir_reader(self, path: str, context: Context) -> Result[DirReader, FileError]: ...

    async def watch(
        self,
        targets: Sequence[WatchTarget],
        on_change: Callable[[WatchChange], None],
        context: Context,
    ) -> Result[FileWatcher, FileError]: ...

    async def canonical_path(self, path: str, context: Context) -> Result[str, FileError]: ...

    async def exists(self, path: str, context: Context) -> Result[bool, FileError]: ...

    async def create_dir(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[None, FileError]: ...

    async def remove(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[None, FileError]: ...

    async def create_temp_dir(
        self, prefix: str | None, context: Context
    ) -> Result[str, FileError]: ...

    async def create_temp_file(
        self, options: dict[str, Any] | None, context: Context
    ) -> Result[str, FileError]: ...

    async def cleanup(self, context: Context) -> None: ...


__all__ = [
    "BinaryReader",
    "DirPage",
    "DirReader",
    "Err",
    "FileError",
    "FileErrorCode",
    "FileInfo",
    "FileKind",
    "FileListEntry",
    "FileListing",
    "FileSystem",
    "FileWatcher",
    "LineScan",
    "ListFilesOptions",
    "Ok",
    "Result",
    "TextLine",
    "TextLineReader",
    "WatchChange",
    "WatchError",
    "WatchOverflow",
    "WatchPaths",
    "WatchTarget",
    "err",
    "get_or_throw",
    "get_or_undefined",
    "ok",
    "to_error",
]

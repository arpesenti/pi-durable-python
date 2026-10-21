"""POSIX filesystem backend.

The filesystem capability is complete enough for the JSONL backend and its failure-injection
tests. Shell execution, line and binary readers, directory listings, and file watching land with
the ``env`` ticket; the corresponding methods report ``not_supported`` until then.
"""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname

from ..context import AbortError, Context
from . import (
    BinaryReader,
    DirReader,
    Err,
    FileError,
    FileErrorCode,
    FileInfo,
    FileListing,
    FileWatcher,
    ListFilesOptions,
    Ok,
    Result,
    TextLineReader,
    WatchChange,
    WatchTarget,
    err,
    ok,
    to_error,
)

_MAX_SAFE_INTEGER = 9_007_199_254_740_991


def resolve_path(cwd: str, path: str) -> str:
    """Resolve ``~``, ``file://`` URLs, and relative paths the way the Node backend does."""

    normalized = path
    if normalized == "~":
        normalized = os.path.expanduser("~")
    elif normalized.startswith("~/"):
        normalized = os.path.join(os.path.expanduser("~"), normalized[2:])
    elif normalized.startswith("file://"):
        try:
            normalized = url2pathname(urlparse(normalized).path)
        except ValueError:
            pass
    if os.path.isabs(normalized):
        return os.path.abspath(normalized)
    return os.path.abspath(os.path.join(cwd, normalized))


def _file_kind(stats: os.stat_result) -> str | None:
    if stat.S_ISREG(stats.st_mode):
        return "file"
    if stat.S_ISDIR(stats.st_mode):
        return "directory"
    if stat.S_ISLNK(stats.st_mode):
        return "symlink"
    return None


def _file_info(path: str, stats: os.stat_result) -> Result[FileInfo, FileError]:
    kind = _file_kind(stats)
    if kind is None:
        return err(FileError("invalid", "Unsupported file type", path))
    return ok(
        FileInfo(
            name=os.path.basename(path),
            path=path,
            kind=kind,
            size=stats.st_size,
            mtimeMs=stats.st_mtime * 1000,
        )
    )


def _error_code(error: BaseException) -> FileErrorCode:
    if isinstance(error, FileError):
        return error.code
    if isinstance(error, AbortError):
        return "aborted"
    if isinstance(error, FileNotFoundError):
        return "not_found"
    if isinstance(error, PermissionError):
        return "permission_denied"
    if isinstance(error, NotADirectoryError):
        return "not_directory"
    if isinstance(error, IsADirectoryError):
        return "is_directory"
    if isinstance(error, (ValueError, OverflowError)):
        return "invalid"
    return "unknown"


def to_file_error(error: object, fallback_path: str | None = None) -> FileError:
    if isinstance(error, FileError):
        return error
    cause = to_error(error)
    path = getattr(error, "filename", None)
    if not isinstance(path, str):
        path = fallback_path
    return FileError(_error_code(cause), str(cause), path, cause)


def _abort_result(context: Context, path: str | None = None) -> Result[Any, FileError] | None:
    signal = context.abort_signal
    if signal is not None and signal.aborted:
        return err(FileError("aborted", "aborted", path))
    return None


def _encode(content: str | bytes) -> bytes:
    return content.encode() if isinstance(content, str) else content


class PosixExecutionEnv:
    """The POSIX filesystem capability for the local machine."""

    id = "posix"

    def __init__(self, *, cwd: str | None = None) -> None:
        self.cwd = os.getcwd() if cwd is None else cwd

    async def absolute_path(self, path: str, context: Context) -> Result[str, FileError]:
        return ok(resolve_path(self.cwd, path))

    async def join_path(self, parts: Sequence[str], context: Context) -> Result[str, FileError]:
        return ok(os.path.normpath(os.path.join(*parts)))

    async def read_text_file(self, path: str, context: Context) -> Result[str, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            return ok(Path(resolved).read_bytes().decode("utf-8", "replace"))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def read_binary_file(self, path: str, context: Context) -> Result[bytes, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            return ok(Path(resolved).read_bytes())
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def write_file(
        self, path: str, content: str | bytes, context: Context
    ) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            os.makedirs(os.path.dirname(resolved) or self.cwd, exist_ok=True)
            after_mkdir = _abort_result(context, resolved)
            if after_mkdir is not None:
                return after_mkdir
            Path(resolved).write_bytes(_encode(content))
            return ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def append_file(
        self, path: str, content: str | bytes, context: Context
    ) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            os.makedirs(os.path.dirname(resolved) or self.cwd, exist_ok=True)
            after_mkdir = _abort_result(context, resolved)
            if after_mkdir is not None:
                return after_mkdir
            with open(resolved, "ab") as handle:
                handle.write(_encode(content))
            return _abort_result(context, resolved) or ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def truncate_file(
        self, path: str, size: int, context: Context
    ) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        if isinstance(size, bool) or not isinstance(size, int) or size < 0 or size > _MAX_SAFE_INTEGER:
            return err(FileError("invalid", "File size must be a non-negative safe integer", resolved))
        try:
            with open(resolved, "r+b") as handle:
                handle.truncate(size)
            return _abort_result(context, resolved) or ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def flush_file(self, path: str, context: Context) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            with open(resolved, "r+b") as handle:
                os.fsync(handle.fileno())
            return _abort_result(context, resolved) or ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def rename_file(
        self, source_path: str, destination_path: str, context: Context
    ) -> Result[None, FileError]:
        source = resolve_path(self.cwd, source_path)
        destination = resolve_path(self.cwd, destination_path)
        aborted = _abort_result(context, destination)
        if aborted is not None:
            return aborted
        try:
            os.replace(source, destination)
            return ok(None)
        except OSError as error:
            return err(to_file_error(error, source))

    async def file_info(self, path: str, context: Context) -> Result[FileInfo, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            return _file_info(resolved, os.lstat(resolved))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def list_dir(self, path: str, context: Context) -> Result[list[FileInfo], FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        infos: list[FileInfo] = []
        try:
            with os.scandir(resolved) as entries:
                for entry in entries:
                    loop_abort = _abort_result(context, resolved)
                    if loop_abort is not None:
                        return loop_abort
                    info = _file_info(entry.path, os.lstat(entry.path))
                    if isinstance(info, Ok):
                        infos.append(info.value)
            return ok(infos)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def canonical_path(self, path: str, context: Context) -> Result[str, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            return ok(os.path.realpath(resolved))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def exists(self, path: str, context: Context) -> Result[bool, FileError]:
        result = await self.file_info(path, context)
        if isinstance(result, Ok):
            return ok(True)
        if result.error.code == "not_found":
            return ok(False)
        return err(result.error)

    async def create_dir(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        recursive = True if options is None else options.get("recursive", True)
        try:
            if recursive:
                os.makedirs(resolved, exist_ok=True)
            else:
                os.mkdir(resolved)
            return ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def remove(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        recursive = bool(options and options.get("recursive"))
        force = bool(options and options.get("force"))
        try:
            if recursive and os.path.isdir(resolved) and not os.path.islink(resolved):
                shutil.rmtree(resolved, ignore_errors=False)
            else:
                os.remove(resolved)
            return ok(None)
        except FileNotFoundError as error:
            return ok(None) if force else err(to_file_error(error, resolved))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def create_temp_dir(
        self, prefix: str | None, context: Context
    ) -> Result[str, FileError]:
        aborted = _abort_result(context)
        if aborted is not None:
            return aborted
        try:
            return ok(tempfile.mkdtemp(prefix=prefix or "tmp-"))
        except OSError as error:
            return err(to_file_error(error))

    async def create_temp_file(
        self, options: dict[str, Any] | None, context: Context
    ) -> Result[str, FileError]:
        directory = await self.create_temp_dir("tmp-", context)
        if isinstance(directory, Err):
            return directory
        prefix = "" if options is None else options.get("prefix", "")
        suffix = "" if options is None else options.get("suffix", "")
        path = os.path.join(directory.value, f"{prefix}{uuid.uuid4()}{suffix}")
        try:
            Path(path).write_bytes(b"")
            return ok(path)
        except OSError as error:
            return err(to_file_error(error, path))

    async def cleanup(self, context: Context) -> None:
        return None

    # ── Capabilities that land with the env ticket ──────────────────────────

    async def open_text_line_reader(
        self, path: str, context: Context
    ) -> Result[TextLineReader, FileError]:
        return err(FileError("not_supported", "Line readers are not implemented yet", path))

    async def read_text_lines(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[list[str], FileError]:
        return err(FileError("not_supported", "Line readers are not implemented yet", path))

    async def open_binary_reader(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[BinaryReader, FileError]:
        return err(FileError("not_supported", "Binary readers are not implemented yet", path))

    async def open_dir_reader(self, path: str, context: Context) -> Result[DirReader, FileError]:
        return err(FileError("not_supported", "Directory readers are not implemented yet", path))

    async def list_files(
        self, path: str, options: ListFilesOptions | None, context: Context
    ) -> Result[FileListing, FileError]:
        return err(FileError("not_supported", "File listings are not implemented yet", path))

    async def watch(
        self,
        targets: Sequence[WatchTarget],
        on_change: Callable[[WatchChange], None],
        context: Context,
    ) -> Result[FileWatcher, FileError]:
        del on_change
        path = targets[0].get("path") if targets else None
        return err(FileError("not_supported", "Watching is not implemented yet", path))


__all__ = ["PosixExecutionEnv", "resolve_path", "to_file_error"]

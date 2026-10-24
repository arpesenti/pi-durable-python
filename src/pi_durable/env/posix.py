"""POSIX execution environment.

A local filesystem and shell implementation of the portable :class:`~pi_durable.env.ExecutionEnv`
protocol. Filesystem operations return failures instead of throwing; shell execution streams
decoded output, bounds it with a timeout, and can spill the complete raw output to a temporary
file. Watches poll snapshots, which sees creations, replacements, and renamed ancestors without
depending on a platform event stream.
"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import math
import os
import re
import shutil
import signal
import stat
import tempfile
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse
from urllib.request import url2pathname

from ..context import AbortError, Context
from . import (
    BinaryReader,
    DirPage,
    DirReader,
    Err,
    ExecutionEnv,
    ExecutionError,
    FileError,
    FileErrorCode,
    FileInfo,
    FileListing,
    FileWatcher,
    LineScan,
    ListFilesOptions,
    Ok,
    Result,
    ShellExecOptions,
    ShellExecResult,
    ShellOutputInfo,
    TextLine,
    TextLineReader,
    WatchChange,
    WatchError,
    WatchPaths,
    WatchTarget,
    err,
    ok,
    to_error,
)
from .decode import StreamDecoder
from .line_scan import LineScanner

_MAX_SAFE_INTEGER = 9_007_199_254_740_991
_MAX_TIMEOUT_MS = 2_147_483_647
_EXIT_STDIO_GRACE_SECONDS = 0.1
_BINARY_READ_CHUNK = 1024 * 1024
_LINE_READ_CHUNK = 64 * 1024
_DEFAULT_POLL_INTERVAL_MS = 2000
_DEFAULT_MAX_DIRECTORIES = 10_000
_IGNORE_HASH_MAX_BYTES = 256 * 1024
_IGNORE_HASH_RECENT_SECONDS = 5
_DEFAULT_LIST_MAX_ENTRIES = 50_000
_DEFAULT_LIST_MAX_BYTES = 4 * 1024 * 1024
_MAX_LIST_MAX_BYTES = 15_900_000
_DEFAULT_LIST_TIMEOUT_MS = 10_000


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
    if getattr(error, "errno", None) in (errno.ELOOP, errno.EMLINK):
        return "invalid"
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


def _closed(what: str, path: str) -> Result[Any, FileError]:
    return err(FileError("invalid", f"{what} is closed", path))


class _TextLineReader:
    def __init__(self, path: str, file: Any) -> None:
        self._path = path
        self._file = file
        self._decoder = StreamDecoder()
        self._buffer = ""
        self._offset = 0
        self._ended = False
        self._closed = False

    async def read_line(self, context: Context) -> Result[TextLine | None, FileError]:
        aborted = _abort_result(context, self._path)
        if aborted is not None:
            return aborted
        if self._closed:
            return _closed("Text line reader", self._path)
        try:
            while True:
                newline = self._buffer.find("\n")
                if newline != -1:
                    text = self._buffer[:newline]
                    self._buffer = self._buffer[newline + 1 :]
                    return ok(TextLine(text=text, terminated=True))
                if self._ended:
                    if self._buffer == "":
                        return ok(None)
                    text = self._buffer
                    self._buffer = ""
                    return ok(TextLine(text=text, terminated=False))

                # Explicit positions allow an aborted read to be retried without skipping bytes.
                chunk = await asyncio.to_thread(os.pread, self._file.fileno(), _LINE_READ_CHUNK, self._offset)
                after_read = _abort_result(context, self._path)
                if after_read is not None:
                    return after_read
                self._offset += len(chunk)
                if not chunk:
                    self._buffer += self._decoder.decode()
                    self._ended = True
                else:
                    self._buffer += self._decoder.decode(chunk)
        except OSError as error:
            return err(to_file_error(error, self._path))

    async def close(self, context: Context) -> None:
        del context
        if self._closed:
            return
        self._closed = True
        self._buffer = ""
        try:
            self._file.close()
        except OSError:
            pass


class _BinaryReader:
    def __init__(self, path: str, fd: int, info: FileInfo) -> None:
        self._path = path
        self._fd = fd
        self._info = info
        self._closed = False

    async def info(self, context: Context) -> Result[FileInfo, FileError]:
        aborted = _abort_result(context, self._path)
        if aborted is not None:
            return aborted
        if self._closed:
            return _closed("Binary reader", self._path)
        try:
            return _file_info(self._path, os.fstat(self._fd))
        except OSError as error:
            return err(to_file_error(error, self._path))

    async def read(self, offset: int, length: int, context: Context) -> Result[bytes, FileError]:
        aborted = _abort_result(context, self._path)
        if aborted is not None:
            return aborted
        if self._closed:
            return _closed("Binary reader", self._path)
        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or isinstance(length, bool)
            or not isinstance(length, int)
            or offset < 0
            or length < 0
            or offset > _MAX_SAFE_INTEGER
            or length > _MAX_SAFE_INTEGER
        ):
            return err(
                FileError(
                    "invalid",
                    "Offset and length must be non-negative safe integers",
                    self._path,
                )
            )
        chunks: list[bytes] = []
        total = 0
        try:
            while total < length:
                chunk = await asyncio.to_thread(
                    os.pread, self._fd, min(length - total, _BINARY_READ_CHUNK), offset + total
                )
                after_read = _abort_result(context, self._path)
                if after_read is not None:
                    return after_read
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
        except OSError as error:
            return err(to_file_error(error, self._path))
        return ok(b"".join(chunks))

    async def scan_lines(self, options: dict[str, Any], context: Context) -> Result[LineScan, FileError]:
        aborted = _abort_result(context, self._path)
        if aborted is not None:
            return aborted
        if self._closed:
            return _closed("Binary reader", self._path)
        try:
            start_line = options.get("start_line", options.get("startLine"))
            end_line = options.get("end_line", options.get("endLine"))
            scanner = LineScanner(cast(int, start_line), cast(int | None, end_line))
        except (KeyError, TypeError, ValueError):
            return err(FileError("invalid", "Invalid line range", self._path))
        try:
            position = 0
            while True:
                chunk = await asyncio.to_thread(os.pread, self._fd, _LINE_READ_CHUNK, position)
                after_read = _abort_result(context, self._path)
                if after_read is not None:
                    return after_read
                if not chunk:
                    return ok(scanner.finish())
                scanner.push(chunk)
                position += len(chunk)
        except OSError as error:
            return err(to_file_error(error, self._path))

    async def close(self, context: Context) -> None:
        del context
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self._fd)
        except OSError:
            pass


class _DirReader:
    def __init__(self, path: str, iterator: Any) -> None:
        self._path = path
        self._iterator = iterator
        self._done = False
        self._closed = False

    async def next(self, max_entries: int, context: Context) -> Result[DirPage, FileError]:
        aborted = _abort_result(context, self._path)
        if aborted is not None:
            return aborted
        if self._closed:
            return _closed("Directory reader", self._path)
        if isinstance(max_entries, bool) or not isinstance(max_entries, int) or max_entries <= 0:
            return err(FileError("invalid", "maxEntries must be a positive safe integer", self._path))
        entries: list[FileInfo] = []
        try:
            while not self._done and len(entries) < max_entries:
                entry = next(self._iterator, None)
                if entry is None:
                    self._done = True
                    break
                loop_abort = _abort_result(context, self._path)
                if loop_abort is not None:
                    return loop_abort
                entry_path = os.path.join(self._path, entry.name)
                try:
                    info = _file_info(entry_path, os.lstat(entry_path))
                except FileNotFoundError:
                    # Removed between enumeration and lstat: not part of the listing any more.
                    continue
                except OSError as error:
                    return err(to_file_error(error, entry_path))
                if isinstance(info, Ok):
                    entries.append(info.value)
            return ok(DirPage(entries=entries, done=self._done))
        except OSError as error:
            return err(to_file_error(error, self._path))

    async def close(self, context: Context) -> None:
        del context
        if self._closed:
            return
        self._closed = True
        try:
            self._iterator.close()
        except OSError:
            pass


@dataclass(frozen=True, slots=True)
class _WatchTarget:
    path: str
    recursive: bool
    hidden: bool
    names: frozenset[str]


@dataclass(frozen=True, slots=True)
class _Entry:
    kind: str
    dev: int
    ino: int
    size: int
    mtime_ns: int
    digest: str | None = None


class _BudgetExceeded(Exception):
    pass


def _entry_of(stats: os.stat_result, *, identity_only: bool = False, digest: str | None = None) -> _Entry:
    kind = _file_kind(stats) or "other"
    if identity_only:
        return _Entry(kind, stats.st_dev, stats.st_ino, 0, 0, None)
    return _Entry(
        kind,
        stats.st_dev,
        stats.st_ino,
        stats.st_size if kind == "file" else 0,
        stats.st_mtime_ns if kind == "file" else 0,
        digest,
    )


def _ancestors(path: str) -> list[str]:
    result: list[str] = []
    current = os.path.dirname(path)
    while True:
        result.append(current)
        parent = os.path.dirname(current)
        if parent == current:
            return result
        current = parent


def _within(path: str, ancestor: str) -> bool:
    return path == ancestor or path.startswith(ancestor + os.sep)


def _excluded(target: _WatchTarget, name: str) -> bool:
    return (target.hidden and name.startswith(".")) or name in target.names


class PosixWatchOptions(dict[str, Any]):
    """Options for :class:`PosixExecutionEnv` watches.

    ``poll_interval_ms`` sets the snapshot interval and ``max_directories`` bounds the watched
    tree. ``mode`` is accepted for compatibility; the portable backend always polls.
    """


class PosixFileWatcher:
    """Polling watcher: snapshots the watched tree and reports differences.

    Polling is the portable choice: it sees creations, replacements, and renamed ancestors without
    depending on a platform's event stream, and mode ``polling`` is one of the documented modes.
    """

    mode: Literal["native", "polling"] = "polling"

    def __init__(
        self,
        targets: Sequence[_WatchTarget],
        on_change: Callable[[WatchChange], None],
        *,
        poll_interval_ms: int = _DEFAULT_POLL_INTERVAL_MS,
        max_directories: int = _DEFAULT_MAX_DIRECTORIES,
    ) -> None:
        self._targets = list(targets)
        self._on_change = on_change
        self._poll_interval = max(poll_interval_ms, 1) / 1000
        self._max_directories = max_directories
        self._snapshot: dict[str, _Entry] = {}
        self._task: asyncio.Task[None] | None = None
        self._closed = False

    @classmethod
    async def open(
        cls,
        targets: Sequence[_WatchTarget],
        on_change: Callable[[WatchChange], None],
        *,
        poll_interval_ms: int = _DEFAULT_POLL_INTERVAL_MS,
        max_directories: int = _DEFAULT_MAX_DIRECTORIES,
    ) -> PosixFileWatcher:
        watcher = cls(
            targets,
            on_change,
            poll_interval_ms=poll_interval_ms,
            max_directories=max_directories,
        )
        try:
            watcher._snapshot = await watcher._scan()
        except _BudgetExceeded as error:
            raise FileError("invalid", str(error)) from None
        watcher._task = asyncio.create_task(watcher._run())
        return watcher

    def _deliver(self, change: WatchChange) -> None:
        if self._closed:
            return
        try:
            self._on_change(change)
        except Exception:
            # A throwing callback must not stop watching.
            pass

    async def _run(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._poll_interval)
                if self._closed:
                    return
                try:
                    snapshot = await self._scan()
                except _BudgetExceeded as error:
                    self._deliver(WatchError(error=FileError("invalid", str(error))))
                    return
                except (FileError, OSError) as error:
                    self._deliver(WatchError(error=to_file_error(error)))
                    return
                changed = self._diff(self._snapshot, snapshot)
                self._snapshot = snapshot
                if changed:
                    self._deliver(WatchPaths(paths=sorted(changed)))
        except asyncio.CancelledError:
            return

    def _diff(self, previous: dict[str, _Entry], next_snapshot: dict[str, _Entry]) -> set[str]:
        changed: set[str] = set()
        for path, entry in next_snapshot.items():
            before = previous.get(path)
            if before is None or before != entry:
                changed.add(self._reported(path))
        for path in previous:
            if path not in next_snapshot:
                changed.add(self._reported(path))
        return changed

    def _reported(self, path: str) -> str:
        if any(_within(path, target.path) for target in self._targets):
            return path
        for target in self._targets:
            if _within(target.path, path):
                return target.path
        return path

    async def _scan(self) -> dict[str, _Entry]:
        snapshot: dict[str, _Entry] = {}
        counted: set[str] = set()
        traversed: set[tuple[int, str]] = set()

        def count(directory: str) -> None:
            counted.add(directory)
            if len(counted) > self._max_directories:
                raise _BudgetExceeded(f"Watched paths exceed {self._max_directories} directories")

        async def record(path: str, stats: os.stat_result) -> None:
            digest: str | None = None
            if (
                stat.S_ISREG(stats.st_mode)
                and stats.st_size <= _IGNORE_HASH_MAX_BYTES
                and time.time() - stats.st_mtime < _IGNORE_HASH_RECENT_SECONDS
            ):
                try:
                    digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
                except OSError:
                    digest = None
            snapshot[path] = _entry_of(stats, digest=digest)

        async def scan_directory(index: int, target: _WatchTarget, directory: str) -> None:
            key = (index, directory)
            if key in traversed:
                return
            traversed.add(key)
            try:
                names = sorted(os.listdir(directory), key=os.fsencode)
            except FileNotFoundError:
                return
            except (PermissionError, NotADirectoryError):
                # The watched directory itself must be readable; below it, unreadable directories are skipped.
                if directory == target.path:
                    raise
                return
            for name in names:
                if _excluded(target, name):
                    continue
                path = os.path.join(directory, name)
                if path in snapshot:
                    kind = snapshot[path].kind
                else:
                    try:
                        stats = os.lstat(path)
                    except OSError:
                        continue
                    await record(path, stats)
                    kind = snapshot[path].kind
                if target.recursive and kind == "directory":
                    count(path)
                    await scan_directory(index, target, path)

        for index, target in enumerate(self._targets):
            for ancestor in _ancestors(target.path):
                if ancestor in snapshot:
                    continue
                try:
                    stats = os.lstat(ancestor)
                except OSError:
                    continue
                snapshot[ancestor] = _entry_of(stats, identity_only=True)
            try:
                stats = os.stat(target.path)
            except PermissionError:
                raise
            except OSError:
                continue
            await record(target.path, stats)
            if stat.S_ISDIR(stats.st_mode):
                count(target.path)
                await scan_directory(index, target, target.path)
        return snapshot

    async def close(self, context: Context) -> None:
        del context
        if self._closed:
            return
        self._closed = True
        task = self._task
        self._task = None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


def _gitignore_matches(relative: str, pattern: str) -> bool:
    """Match one gitignore pattern; ``*`` and ``?`` do not cross ``/`` and ``**`` does."""

    parts: list[str] = ["^"]
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                parts.append(".*")
                index += 2
                continue
            parts.append("[^/]*")
        elif character == "?":
            parts.append("[^/]")
        elif character == "[":
            end = pattern.find("]", index + 1)
            if end == -1:
                parts.append(re.escape(character))
            else:
                body = pattern[index + 1 : end]
                if body.startswith("!"):
                    body = "^" + body[1:]
                parts.append("[" + body + "]")
                index = end
        else:
            parts.append(re.escape(character))
        index += 1
    parts.append("$")
    return re.match("".join(parts), relative) is not None


@dataclass(frozen=True, slots=True)
class _IgnoreRule:
    base: str
    pattern: str
    negated: bool
    directory_only: bool
    anchored: bool

    def matches(self, path: str, kind: str) -> bool:
        if self.directory_only and kind != "directory":
            return False
        relative = os.path.relpath(path, self.base)
        if relative == os.pardir or relative.startswith(os.pardir + os.sep):
            return False
        relative = relative.replace(os.sep, "/")
        if self.anchored or "/" in self.pattern:
            return _gitignore_matches(relative, self.pattern)
        return any(_gitignore_matches(part, self.pattern) for part in relative.split("/"))


def _read_ignore_file(path: str, base: str) -> list[_IgnoreRule]:
    rules: list[_IgnoreRule] = []
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return rules
    for line in lines:
        line = line.rstrip("\r")
        if line == "" or line.startswith("#"):
            continue
        negated = line.startswith("!")
        if negated:
            line = line[1:]
        if line == "":
            continue
        directory_only = line.endswith("/")
        if directory_only:
            line = line[:-1]
        anchored = line.startswith("/")
        if anchored:
            line = line[1:]
        if line == "":
            continue
        rules.append(_IgnoreRule(base, line, negated, directory_only, anchored))
    return rules


def _ignored(path: str, kind: str, rules: Sequence[_IgnoreRule]) -> bool:
    ignored = False
    for rule in rules:
        if rule.matches(path, kind):
            ignored = not rule.negated
    return ignored


def _find_git_root(path: str) -> str | None:
    current = os.path.abspath(path)
    while True:
        if os.path.exists(os.path.join(current, ".git")):
            return current
        parent = os.path.dirname(current)
        if parent == current:
            return None
        current = parent


def _ignore_rules_for(root: str) -> list[_IgnoreRule]:
    git_root = _find_git_root(root)
    if git_root is None:
        return []
    rules = _read_ignore_file(os.path.join(git_root, ".git", "info", "exclude"), git_root)
    relative = os.path.relpath(root, git_root)
    components = [] if relative == os.curdir else relative.split(os.sep)
    current = git_root
    rules.extend(_read_ignore_file(os.path.join(current, ".gitignore"), current))
    for component in components:
        current = os.path.join(current, component)
        rules.extend(_read_ignore_file(os.path.join(current, ".gitignore"), current))
    return rules


def _list_option(options: dict[str, Any] | None, name: str, default: Any) -> Any:
    if options is None:
        return default
    return options.get(name, default)


def _listing_int(value: object, *, maximum: int | None = None) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    if maximum is not None and value > maximum:
        return None
    return value


class _SpillWriter:
    """Write the complete raw output once either threshold is crossed."""

    def __init__(self, env: PosixExecutionEnv, context: Context, options: dict[str, Any]) -> None:
        self._env = env
        self._context = context
        self._after_bytes = int(options.get("after_bytes", 0))
        self._after_lines = int(options.get("after_lines", 0))
        self._prefix: list[bytes] = []
        self._seen_bytes = 0
        self._seen_newlines = 0
        self._started = False
        self._finished = False
        self._error: ExecutionError | None = None
        self._path: str | None = None
        self._file: Any = None
        self._lock = asyncio.Lock()

    @property
    def path(self) -> str | None:
        return self._path

    @property
    def error(self) -> ExecutionError | None:
        return self._error

    @property
    def active(self) -> bool:
        return self._started and not self._finished

    async def feed(self, chunk: bytes) -> None:
        if not chunk or self._finished or self._error is not None:
            return
        async with self._lock:
            if self._started:
                await self._write(chunk)
                return
            self._seen_bytes += len(chunk)
            self._seen_newlines += chunk.count(b"\n")
            lines = self._seen_newlines + (0 if chunk.endswith(b"\n") else 1)
            if self._seen_bytes <= self._after_bytes and lines <= self._after_lines:
                self._prefix.append(chunk)
                return
            prefix = self._prefix
            self._prefix = []
            await self._start()
            for part in prefix:
                await self._write(part)
            await self._write(chunk)

    async def _start(self) -> None:
        self._started = True
        created = await self._env.create_temp_file({"prefix": "pi-output-", "suffix": ".log"}, self._context)
        if isinstance(created, Err):
            self._error = ExecutionError(
                "unknown",
                f"Failed to preserve complete shell output: {created.error}",
                created.error,
            )
            return
        self._path = created.value
        try:
            self._file = open(created.value, "ab")
        except OSError as error:
            self._error = ExecutionError(
                "unknown",
                f"Failed to preserve complete shell output: {error}",
                error,
            )

    async def _write(self, chunk: bytes) -> None:
        if self._error is not None or self._file is None:
            return
        try:
            self._file.write(chunk)
        except OSError as error:
            self._error = ExecutionError(
                "unknown",
                f"Failed to preserve complete shell output: {error}",
                error,
            )

    async def finish(self) -> None:
        async with self._lock:
            if self._finished:
                return
            self._finished = True
            if self._file is not None:
                try:
                    self._file.close()
                except OSError as error:
                    if self._error is None:
                        self._error = ExecutionError(
                            "unknown",
                            f"Failed to preserve complete shell output: {error}",
                            error,
                        )


def _resolve_timeout(timeout: object) -> Result[int | None, ExecutionError]:
    if timeout is None:
        return ok(None)
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        return err(ExecutionError("timeout", "Invalid timeout: must be a finite number of seconds"))
    timeout_ms = timeout * 1000
    if timeout_ms > _MAX_TIMEOUT_MS:
        return err(
            ExecutionError(
                "timeout",
                f"Invalid timeout: maximum is {_MAX_TIMEOUT_MS / 1000} seconds",
            )
        )
    return ok(int(timeout_ms))


def _shell_config(shell_path: str | None) -> Result[list[str], ExecutionError]:
    if shell_path is not None:
        if os.path.exists(shell_path):
            return ok([shell_path, "-c"])
        return err(ExecutionError("shell_unavailable", f"Custom shell path not found: {shell_path}"))
    if os.path.exists("/bin/bash"):
        return ok(["/bin/bash", "-c"])
    found = shutil.which("bash")
    if found:
        return ok([found, "-c"])
    return ok(["sh", "-c"])


def _shell_env(
    base: dict[str, str] | None,
    extra: dict[str, str] | None,
    inherit: bool,
) -> dict[str, str]:
    if not inherit:
        environment: dict[str, str] = {}
    else:
        environment = dict(os.environ)
        if base:
            environment.update({key: value for key, value in base.items() if value is not None})
    if extra:
        environment.update({key: value for key, value in extra.items() if value is not None})
    return environment


def _kill_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, AttributeError):
        try:
            process.kill()
        except ProcessLookupError:
            pass


class PosixExecutionEnv(ExecutionEnv):
    """The POSIX filesystem and shell for the local machine."""

    id = "posix:local"

    def __init__(
        self,
        *,
        cwd: str | None = None,
        shell_path: str | None = None,
        shell_env: dict[str, str] | None = None,
        watch: PosixWatchOptions | dict[str, Any] | None = None,
    ) -> None:
        self.cwd = os.getcwd() if cwd is None else cwd
        self.shell_path = shell_path
        self.shell_env = shell_env
        watch_options: dict[str, Any] = watch or {}
        self._poll_interval_ms = int(watch_options.get("poll_interval_ms", _DEFAULT_POLL_INTERVAL_MS))
        self._max_directories = int(watch_options.get("max_directories", _DEFAULT_MAX_DIRECTORIES))
        self._children: set[asyncio.subprocess.Process] = set()
        self._cleanup_generation = 0

    async def absolute_path(self, path: str, context: Context) -> Result[str, FileError]:
        del context
        return ok(resolve_path(self.cwd, path))

    async def join_path(self, parts: Sequence[str], context: Context) -> Result[str, FileError]:
        del context
        return ok(os.path.normpath(os.path.join(*parts)))

    async def read_text_file(self, path: str, context: Context) -> Result[str, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            data = await asyncio.to_thread(Path(resolved).read_bytes)
            return ok(data.decode("utf-8", "replace"))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def write_file(self, path: str, content: str | bytes, context: Context) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            os.makedirs(os.path.dirname(resolved) or self.cwd, exist_ok=True)
            after_mkdir = _abort_result(context, resolved)
            if after_mkdir is not None:
                return after_mkdir
            await asyncio.to_thread(Path(resolved).write_bytes, _encode(content))
            return ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def append_file(self, path: str, content: str | bytes, context: Context) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            os.makedirs(os.path.dirname(resolved) or self.cwd, exist_ok=True)
            after_mkdir = _abort_result(context, resolved)
            if after_mkdir is not None:
                return after_mkdir

            def append() -> None:
                with open(resolved, "ab") as handle:
                    handle.write(_encode(content))

            await asyncio.to_thread(append)
            return _abort_result(context, resolved) or ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def truncate_file(self, path: str, size: int, context: Context) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        if isinstance(size, bool) or not isinstance(size, int) or size < 0 or size > _MAX_SAFE_INTEGER:
            return err(FileError("invalid", "File size must be a non-negative safe integer", resolved))
        try:
            fd = os.open(resolved, os.O_RDWR)
            try:
                os.ftruncate(fd, size)
            finally:
                os.close(fd)
            return _abort_result(context, resolved) or ok(None)
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def flush_file(self, path: str, context: Context) -> Result[None, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            fd = os.open(resolved, os.O_RDWR)
            try:
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    return err(FileError("is_directory", "Is a directory", resolved))
                os.fsync(fd)
            finally:
                os.close(fd)
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
            names = await asyncio.to_thread(os.listdir, resolved)
            for name in names:
                loop_abort = _abort_result(context, resolved)
                if loop_abort is not None:
                    return loop_abort
                entry_path = os.path.join(resolved, name)
                try:
                    info = _file_info(entry_path, os.lstat(entry_path))
                except FileNotFoundError:
                    continue
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

    async def create_temp_dir(self, prefix: str | None, context: Context) -> Result[str, FileError]:
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

    async def open_text_line_reader(self, path: str, context: Context) -> Result[TextLineReader, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            file = await asyncio.to_thread(open, resolved, "rb")
            after_open = _abort_result(context, resolved)
            if after_open is not None:
                file.close()
                return after_open
            return ok(_TextLineReader(resolved, file))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def read_text_lines(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[list[str], FileError]:
        max_lines = None if options is None else options.get("max_lines")
        if max_lines is not None and max_lines <= 0:
            return ok([])
        opened = await self.open_text_line_reader(path, context)
        if isinstance(opened, Err):
            return opened
        lines: list[str] = []
        try:
            while max_lines is None or len(lines) < max_lines:
                line = await opened.value.read_line(context)
                if isinstance(line, Err):
                    return line
                if line.value is None:
                    break
                lines.append(line.value["text"])
            return ok(lines)
        finally:
            await opened.value.close(context)

    async def read_binary_file(self, path: str, context: Context) -> Result[bytes, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            return ok(await asyncio.to_thread(Path(resolved).read_bytes))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def open_binary_reader(
        self, path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[BinaryReader, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        no_follow = bool(options and options.get("no_follow"))
        flags = os.O_RDONLY
        if hasattr(os, "O_NONBLOCK"):
            flags |= os.O_NONBLOCK
        if no_follow and hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd: int | None = None
        try:
            if no_follow and not hasattr(os, "O_NOFOLLOW") and os.path.islink(resolved):
                return err(FileError("invalid", "Refusing to follow a symbolic link", resolved))
            fd = os.open(resolved, flags)
            stats = os.fstat(fd)
            if stat.S_ISDIR(stats.st_mode):
                os.close(fd)
                return err(
                    FileError(
                        "is_directory",
                        "EISDIR: illegal operation on a directory, read",
                        resolved,
                    )
                )
            if not stat.S_ISREG(stats.st_mode):
                os.close(fd)
                return err(FileError("invalid", "Not a regular file", resolved))
            after_open = _abort_result(context, resolved)
            if after_open is not None:
                os.close(fd)
                return after_open
            info = _file_info(resolved, stats)
            if isinstance(info, Err):
                os.close(fd)
                return info
            return ok(_BinaryReader(resolved, fd, info.value))
        except OSError as error:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if no_follow and getattr(error, "errno", None) in (errno.ELOOP, errno.EMLINK):
                return err(FileError("invalid", "Refusing to follow a symbolic link", resolved, error))
            return err(to_file_error(error, resolved))

    async def open_dir_reader(self, path: str, context: Context) -> Result[DirReader, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        try:
            iterator = await asyncio.to_thread(os.scandir, resolved)
            after_open = _abort_result(context, resolved)
            if after_open is not None:
                iterator.close()
                return after_open
            return ok(_DirReader(resolved, iterator))
        except OSError as error:
            return err(to_file_error(error, resolved))

    async def list_files(
        self, path: str, options: ListFilesOptions | None, context: Context
    ) -> Result[FileListing, FileError]:
        resolved = resolve_path(self.cwd, path)
        aborted = _abort_result(context, resolved)
        if aborted is not None:
            return aborted
        opts: dict[str, Any] = {} if options is None else dict(options)
        max_entries = _listing_int(_list_option(opts, "max_entries", _DEFAULT_LIST_MAX_ENTRIES))
        max_bytes = _listing_int(
            _list_option(opts, "max_bytes", _DEFAULT_LIST_MAX_BYTES),
            maximum=_MAX_LIST_MAX_BYTES,
        )
        timeout_ms = _listing_int(_list_option(opts, "timeout_ms", _DEFAULT_LIST_TIMEOUT_MS))
        if max_entries is None:
            return err(FileError("invalid", "maxEntries must be a non-negative safe integer", resolved))
        if max_bytes is None:
            return err(FileError("invalid", "maxBytes must be a bounded non-negative safe integer", resolved))
        if timeout_ms is None:
            return err(FileError("invalid", "timeoutMs must be a non-negative safe integer", resolved))
        hidden = bool(opts.get("hidden", True))
        use_gitignore = bool(opts.get("gitignore", True))
        exclude = set(opts.get("exclude", ()))
        try:
            stats = await asyncio.to_thread(os.stat, resolved)
        except OSError as error:
            return err(to_file_error(error, resolved))
        if not stat.S_ISDIR(stats.st_mode):
            return err(FileError("not_directory", "ENOTDIR: not a directory", resolved))

        entries: list[dict[str, Any]] = []
        used_bytes = 0
        deadline = time.monotonic() + timeout_ms / 1000
        truncated = False
        root_rules = _ignore_rules_for(resolved) if use_gitignore else []

        class _StopWalk(Exception):
            pass

        def take(entry_path: str, kind: str) -> None:
            nonlocal used_bytes, truncated
            relative = os.path.relpath(entry_path, resolved).replace(os.sep, "/")
            cost = len(relative.encode()) + 2
            if len(entries) >= max_entries or used_bytes + cost > max_bytes or time.monotonic() > deadline:
                truncated = True
                raise _StopWalk
            entries.append({"path": relative, "kind": kind})
            used_bytes += cost

        async def walk(directory: str, rules: list[_IgnoreRule]) -> None:
            if _abort_result(context, directory) is not None:
                raise AbortError()
            try:
                names = await asyncio.to_thread(os.listdir, directory)
            except FileNotFoundError:
                return
            except PermissionError:
                if directory == resolved:
                    raise
                return
            for name in sorted(names, key=os.fsencode):
                if name == ".git":
                    continue
                if not hidden and name.startswith("."):
                    continue
                if name in exclude:
                    continue
                entry_path = os.path.join(directory, name)
                try:
                    entry_stats = os.lstat(entry_path)
                except OSError:
                    continue
                kind = _file_kind(entry_stats)
                if kind is None:
                    continue
                if use_gitignore and _ignored(entry_path, kind, rules):
                    continue
                take(entry_path, kind)
                if kind == "directory" and not stat.S_ISLNK(entry_stats.st_mode):
                    child_rules = rules
                    if use_gitignore:
                        child_rules = rules + _read_ignore_file(
                            os.path.join(entry_path, ".gitignore"), entry_path
                        )
                    await walk(entry_path, child_rules)

        try:
            await walk(resolved, root_rules)
        except _StopWalk:
            pass
        except AbortError:
            return err(FileError("aborted", "aborted", resolved))
        except OSError as error:
            return err(to_file_error(error, resolved))
        return ok(FileListing(entries=entries, truncated=truncated))

    async def watch(
        self,
        targets: Sequence[WatchTarget],
        on_change: Callable[[WatchChange], None],
        context: Context,
    ) -> Result[FileWatcher, FileError]:
        aborted = _abort_result(context)
        if aborted is not None:
            return aborted
        resolved_targets = [
            _WatchTarget(
                resolve_path(self.cwd, target["path"]),
                target.get("recursive") is True,
                bool((target.get("exclude") or {}).get("hidden")),
                frozenset((target.get("exclude") or {}).get("names") or ()),
            )
            for target in targets
        ]
        try:
            watcher = await PosixFileWatcher.open(
                resolved_targets,
                on_change,
                poll_interval_ms=self._poll_interval_ms,
                max_directories=self._max_directories,
            )
            after_open = _abort_result(context)
            if after_open is not None:
                await watcher.close(context)
                return after_open
            return ok(watcher)
        except FileError as error:
            return err(error)
        except OSError as error:
            return err(to_file_error(error))

    async def exec(
        self,
        command: str | Sequence[str],
        options: ShellExecOptions | None,
        context: Context,
    ) -> Result[ShellExecResult, ExecutionError]:
        opts: dict[str, Any] = {} if options is None else dict(options)
        signal_state = context.abort_signal
        if signal_state is not None and signal_state.aborted:
            return err(ExecutionError("aborted", "aborted"))
        timeout = _resolve_timeout(opts.get("timeout"))
        if isinstance(timeout, Err):
            return timeout

        cwd = resolve_path(self.cwd, opts["cwd"]) if opts.get("cwd") else self.cwd
        if isinstance(command, str):
            config = _shell_config(self.shell_path)
            if isinstance(config, Err):
                return config
            argv = [*config.value, command]
        else:
            argv = list(command)
            if not argv:
                return err(ExecutionError("spawn_error", "Empty argv: no program to run"))
        if not os.path.isdir(cwd):
            return err(
                ExecutionError(
                    "spawn_error",
                    f"Working directory does not exist: {cwd}\nCannot execute bash commands.",
                )
            )

        environment = _shell_env(
            self.shell_env,
            opts.get("env"),
            bool(opts.get("inherit_env", True)),
        )
        # cleanup() cannot see a process that is still spawning, so remember the generation and
        # kill the process once it exists if cleanup ran in between.
        cleanup_generation = self._cleanup_generation
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=cwd,
                env=environment,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except (OSError, ValueError) as error:
            return err(ExecutionError("spawn_error", str(error), error))

        self._children.add(process)
        if cleanup_generation != self._cleanup_generation:
            _kill_process(process)
        loop = asyncio.get_running_loop()
        timed_out = False
        callback_error: ExecutionError | None = None
        stream_error: ExecutionError | None = None
        settled = False
        spill = _SpillWriter(self, context, opts["spill"]) if opts.get("spill") else None
        on_output = opts.get("on_output")
        timeout_handle: asyncio.TimerHandle | None = None
        remove_abort: Callable[[], None] | None = None

        def emit(text: str, stream: str) -> None:
            nonlocal callback_error
            if settled or text == "" or on_output is None or callback_error is not None:
                return
            try:
                on_output(text, context, ShellOutputInfo(stream=cast(Any, stream)))
            except Exception as error:
                cause = to_error(error)
                callback_error = ExecutionError("callback_error", str(cause), cause)
                _kill_process(process)

        async def pump(stream: asyncio.StreamReader, decoder: StreamDecoder, name: str) -> None:
            while True:
                chunk = await stream.read(65536)
                if not chunk:
                    break
                text = decoder.decode(chunk)
                emit(text, name)
                if spill is not None:
                    await spill.feed(chunk)
            emit(decoder.decode(), name)

        stdout_task = asyncio.create_task(pump(cast(Any, process.stdout), StreamDecoder(), "stdout"))
        stderr_task = asyncio.create_task(pump(cast(Any, process.stderr), StreamDecoder(), "stderr"))

        def on_timeout() -> None:
            nonlocal timed_out
            timed_out = True
            _kill_process(process)

        if timeout.value is not None:
            timeout_handle = loop.call_later(timeout.value / 1000, on_timeout)
        if signal_state is not None:
            remove_abort = signal_state.add_listener(lambda: _kill_process(process))
            if signal_state.aborted:
                _kill_process(process)

        try:
            await process.wait()
            grace = 5.0 if spill is not None else _EXIT_STDIO_GRACE_SECONDS
            try:
                await asyncio.wait_for(asyncio.gather(stdout_task, stderr_task), timeout=grace)
            except TimeoutError:
                for task in (stdout_task, stderr_task):
                    task.cancel()
                await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            if spill is not None:
                await spill.finish()
            settled = True
        except asyncio.CancelledError:
            _kill_process(process)
            for task in (stdout_task, stderr_task):
                task.cancel()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            if spill is not None:
                await spill.finish()
            raise
        finally:
            settled = True
            if timeout_handle is not None:
                timeout_handle.cancel()
            if remove_abort is not None:
                remove_abort()
            self._children.discard(process)

        if callback_error is not None:
            return err(callback_error)
        interrupted: ExecutionError | None = None
        if timed_out:
            interrupted = ExecutionError("timeout", f"timeout:{opts.get('timeout')}")
        elif signal_state is not None and signal_state.aborted:
            interrupted = ExecutionError("aborted", "aborted")
        if interrupted is not None:
            if spill is not None and spill.path is not None:
                interrupted.spill_path = spill.path
            return err(interrupted)
        if spill is not None and spill.error is not None:
            return err(spill.error)
        if stream_error is not None:
            return err(stream_error)
        return_code = process.returncode
        exit_code = 1
        if return_code is not None:
            exit_code = return_code if return_code >= 0 else 128 + abs(return_code)
        result = ShellExecResult(exitCode=exit_code)
        if spill is not None and spill.path is not None:
            result["spillPath"] = spill.path
        return ok(result)

    async def cleanup(self, context: Context) -> None:
        del context
        # Bumping the generation also terminates a child whose spawn is still in flight.
        self._cleanup_generation += 1
        for process in list(self._children):
            _kill_process(process)
        self._children.clear()


__all__ = [
    "PosixExecutionEnv",
    "PosixFileWatcher",
    "PosixWatchOptions",
    "resolve_path",
    "to_file_error",
]

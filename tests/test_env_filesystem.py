"""The portable ``FileSystem`` capability and the POSIX backend JSONL depends on.

These cases pin the filesystem result shapes and the operations the JSONL backend and its
failure-injection tests use. The translated upstream conformance, shell, watcher, and reader
suites live in ``test_env_conformance.py`` and ``test_env_posix.py``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TypeVar

import pytest

from pi_durable.context import BACKGROUND_CONTEXT, AbortController, with_abort_signal
from pi_durable.env import (
    FileError,
    FileInfo,
    Result,
    err,
    get_or_throw,
    get_or_undefined,
    ok,
    to_error,
)
from pi_durable.env.posix import PosixExecutionEnv

T = TypeVar("T")


def _unwrap(result: Result[T, FileError]) -> T:
    return get_or_throw(result)


def test_result_helpers_round_trip() -> None:
    success: Result[int, FileError] = ok(3)
    failure: Result[int, FileError] = err(FileError("not_found", "missing", "/tmp/x"))

    assert get_or_throw(success) == 3
    assert get_or_undefined(success) == 3
    assert get_or_undefined(failure) is None
    with pytest.raises(FileError, match="missing"):
        get_or_throw(failure)


def test_file_error_carries_the_documented_fields() -> None:
    cause = RuntimeError("cause")
    error = FileError("permission_denied", "denied", "/tmp/x", cause)

    assert error.name == "FileError"
    assert error.code == "permission_denied"
    assert error.path == "/tmp/x"
    assert error.__cause__ is cause
    assert str(error) == "denied"


def test_to_error_normalizes_thrown_values() -> None:
    existing = RuntimeError("existing")
    assert to_error(existing) is existing
    assert str(to_error("message")) == "message"
    assert str(to_error(7)) == "7"


async def test_resolves_absolute_paths_against_the_environment_cwd(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))

    assert _unwrap(await env.absolute_path("a/b", BACKGROUND_CONTEXT)) == str(tmp_path / "a" / "b")
    assert _unwrap(await env.absolute_path("/absolute", BACKGROUND_CONTEXT)) == "/absolute"
    joined = _unwrap(await env.join_path([str(tmp_path), "dir", "file.jsonl"], BACKGROUND_CONTEXT))
    assert joined == str(tmp_path / "dir" / "file.jsonl")


async def test_writes_appends_and_reads_text_and_binary(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))

    await env.create_dir(str(tmp_path / "nested"), None, BACKGROUND_CONTEXT)
    assert (await env.write_file(str(tmp_path / "nested" / "a.txt"), "héllo", BACKGROUND_CONTEXT)).ok
    assert (
        await env.append_file(str(tmp_path / "nested" / "a.txt"), "\nsecond", BACKGROUND_CONTEXT)
    ).ok

    assert _unwrap(await env.read_text_file(str(tmp_path / "nested" / "a.txt"), BACKGROUND_CONTEXT)) == (
        "héllo\nsecond"
    )
    binary = _unwrap(
        await env.read_binary_file(str(tmp_path / "nested" / "a.txt"), BACKGROUND_CONTEXT)
    )
    assert binary == "héllo\nsecond".encode()

    missing = await env.read_binary_file(str(tmp_path / "missing"), BACKGROUND_CONTEXT)
    assert not missing.ok and missing.error.code == "not_found"
    assert missing.error.path == str(tmp_path / "missing")


async def test_write_creates_parent_directories(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))
    target = tmp_path / "deep" / "nested" / "file.txt"

    assert (await env.write_file(str(target), "value", BACKGROUND_CONTEXT)).ok
    assert target.read_text() == "value"


async def test_truncates_and_flushes_at_exact_byte_offsets(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))
    path = tmp_path / "data.bin"
    path.write_bytes(b"abcdef\n")

    assert (await env.truncate_file(str(path), 3, BACKGROUND_CONTEXT)).ok
    assert path.read_bytes() == b"abc"
    assert (await env.flush_file(str(path), BACKGROUND_CONTEXT)).ok

    invalid = await env.truncate_file(str(path), -1, BACKGROUND_CONTEXT)
    assert not invalid.ok and invalid.error.code == "invalid"
    missing = await env.flush_file(str(tmp_path / "missing"), BACKGROUND_CONTEXT)
    assert not missing.ok and missing.error.code == "not_found"


async def test_renames_removes_and_reports_kinds(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))
    source = tmp_path / "source.jsonl"
    target = tmp_path / "target.jsonl"
    source.write_text("line\n")

    assert (await env.rename_file(str(source), str(target), BACKGROUND_CONTEXT)).ok
    info = _unwrap(await env.file_info(str(target), BACKGROUND_CONTEXT))
    assert isinstance(info, dict)
    assert info["kind"] == "file"
    assert info["name"] == "target.jsonl"
    assert info["size"] == 5

    assert _unwrap(await env.exists(str(target), BACKGROUND_CONTEXT)) is True
    assert (await env.remove(str(target), {"force": True}, BACKGROUND_CONTEXT)).ok
    assert _unwrap(await env.exists(str(target), BACKGROUND_CONTEXT)) is False
    assert (await env.remove(str(target), {"force": True}, BACKGROUND_CONTEXT)).ok
    missing = await env.remove(str(target), None, BACKGROUND_CONTEXT)
    assert not missing.ok and missing.error.code == "not_found"


async def test_lists_directories_with_kinds_and_keeps_unsupported_entries_out(
    tmp_path: Path,
) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))
    (tmp_path / "file.txt").write_text("abc")
    (tmp_path / "dir").mkdir()
    (tmp_path / "link").symlink_to(tmp_path / "file.txt")

    listing = _unwrap(await env.list_dir(str(tmp_path), BACKGROUND_CONTEXT))
    assert isinstance(listing, list)
    by_name: dict[str, FileInfo] = {info["name"]: info for info in listing}
    assert by_name["file.txt"]["kind"] == "file"
    assert by_name["file.txt"]["size"] == 3
    assert by_name["dir"]["kind"] == "directory"
    assert by_name["link"]["kind"] == "symlink"

    assert _unwrap(await env.canonical_path(str(tmp_path / "link"), BACKGROUND_CONTEXT)) == os.path.realpath(
        tmp_path / "link"
    )
    missing = await env.list_dir(str(tmp_path / "missing"), BACKGROUND_CONTEXT)
    assert not missing.ok and missing.error.code == "not_found"


async def test_creates_and_cleans_temporary_locations(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))

    directory = _unwrap(await env.create_temp_dir("pi-test-", BACKGROUND_CONTEXT))
    assert isinstance(directory, str) and Path(directory).is_dir()
    file_path = _unwrap(
        await env.create_temp_file({"prefix": "log-", "suffix": ".txt"}, BACKGROUND_CONTEXT)
    )
    assert isinstance(file_path, str) and Path(file_path).read_text() == ""
    await env.cleanup(BACKGROUND_CONTEXT)


async def test_an_aborted_context_reports_aborted_instead_of_reading(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path))
    (tmp_path / "file.txt").write_text("value")
    controller = AbortController()
    controller.abort()

    result = await env.read_binary_file(
        str(tmp_path / "file.txt"),
        with_abort_signal(controller.signal, BACKGROUND_CONTEXT),
    )
    assert not result.ok
    assert result.error.code == "aborted"

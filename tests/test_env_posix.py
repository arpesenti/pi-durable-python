"""Translated v1.3.0 ``env-node``, ``env-node-spill``, and reader cases.

Node-specific Windows and stream-internals cases are translated to the portable surface; the
Node backend itself is replaced by ``PosixExecutionEnv``.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, TypeVar

import pytest

from pi_durable.context import (
    BACKGROUND_CONTEXT,
    AbortController,
    Context,
    with_abort_signal,
)
from pi_durable.env import (
    Err,
    FileError,
    Ok,
    Result,
    ShellExecOptions,
    ShellOutputInfo,
    WatchChange,
    WatchTarget,
    get_or_throw,
)
from pi_durable.env.posix import PosixExecutionEnv

T = TypeVar("T")


def _unwrap(result: Result[T, Any]) -> T:
    return get_or_throw(result)


def _aborted_context() -> Context:
    controller = AbortController()
    controller.abort()
    return with_abort_signal(controller.signal, BACKGROUND_CONTEXT)


async def _collect(
    env: PosixExecutionEnv,
    command: str | list[str],
    options: ShellExecOptions | None = None,
    context: Context = BACKGROUND_CONTEXT,
) -> tuple[Result[Any, Any], str]:
    output = ""

    def on_output(text: str, _context: Context, _info: ShellOutputInfo) -> None:
        nonlocal output
        output += text

    merged: ShellExecOptions = ShellExecOptions({} if options is None else options)
    merged["on_output"] = on_output
    return await env.exec(command, merged, context), output


async def _wait_for(condition: Callable[[], Awaitable[bool]], timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not await condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.01)


@pytest.fixture()
def env(tmp_path: Path):
    return PosixExecutionEnv(cwd=str(tmp_path))


async def test_reads_writes_lists_and_removes_files_and_directories(
    env: PosixExecutionEnv, tmp_path: Path
) -> None:
    assert _unwrap(await env.absolute_path("nested/child", BACKGROUND_CONTEXT)) == str(
        tmp_path / "nested" / "child"
    )
    assert _unwrap(await env.join_path([str(tmp_path), "nested", "child"], BACKGROUND_CONTEXT)) == str(
        tmp_path / "nested" / "child"
    )
    _unwrap(await env.create_dir("nested/child", None, BACKGROUND_CONTEXT))
    _unwrap(await env.write_file("nested/child/file.txt", "hel", BACKGROUND_CONTEXT))
    _unwrap(await env.append_file("nested/child/file.txt", "lo", BACKGROUND_CONTEXT))
    assert _unwrap(await env.read_text_file("nested/child/file.txt", BACKGROUND_CONTEXT)) == "hello"
    assert _unwrap(
        await env.read_text_lines("nested/child/file.txt", {"max_lines": 1}, BACKGROUND_CONTEXT)
    ) == ["hello"]
    assert _unwrap(await env.read_binary_file("nested/child/file.txt", BACKGROUND_CONTEXT)) == b"hello"

    entries = _unwrap(await env.list_dir("nested/child", BACKGROUND_CONTEXT))
    assert len(entries) == 1
    assert entries[0]["name"] == "file.txt"
    assert entries[0]["path"] == str(tmp_path / "nested" / "child" / "file.txt")
    assert entries[0]["kind"] == "file"
    assert entries[0]["size"] == 5
    assert isinstance(entries[0]["mtimeMs"], float)

    assert _unwrap(await env.exists("nested/child/file.txt", BACKGROUND_CONTEXT)) is True
    _unwrap(await env.remove("nested/child/file.txt", None, BACKGROUND_CONTEXT))
    assert _unwrap(await env.exists("nested/child/file.txt", BACKGROUND_CONTEXT)) is False


async def test_expands_home_relative_paths_and_file_urls(env: PosixExecutionEnv, tmp_path: Path) -> None:
    assert _unwrap(await env.absolute_path("~/pi-node-env-test", BACKGROUND_CONTEXT)) == os.path.join(
        os.path.expanduser("~"), "pi-node-env-test"
    )
    file_path = tmp_path / "file with spaces.txt"
    assert _unwrap(await env.absolute_path(file_path.as_uri(), BACKGROUND_CONTEXT)) == str(file_path)


async def test_file_info_reports_files_directories_and_symlinks(
    env: PosixExecutionEnv, tmp_path: Path
) -> None:
    _unwrap(await env.create_dir("dir", {"recursive": True}, BACKGROUND_CONTEXT))
    _unwrap(await env.write_file("dir/file.txt", "hello", BACKGROUND_CONTEXT))
    (tmp_path / "file-link").symlink_to(tmp_path / "dir/file.txt")
    (tmp_path / "dir-link").symlink_to(tmp_path / "dir")

    assert _unwrap(await env.file_info("dir", BACKGROUND_CONTEXT))["kind"] == "directory"
    assert _unwrap(await env.file_info("dir/file.txt", BACKGROUND_CONTEXT))["size"] == 5
    assert _unwrap(await env.file_info("file-link", BACKGROUND_CONTEXT))["kind"] == "symlink"
    assert _unwrap(await env.file_info("dir-link", BACKGROUND_CONTEXT))["kind"] == "symlink"
    assert _unwrap(await env.canonical_path("file-link", BACKGROUND_CONTEXT)) == os.path.realpath(
        tmp_path / "dir/file.txt"
    )


async def test_lists_symlinks_as_symlinks(env: PosixExecutionEnv, tmp_path: Path) -> None:
    _unwrap(await env.write_file("target.txt", "hello", BACKGROUND_CONTEXT))
    (tmp_path / "link.txt").symlink_to(tmp_path / "target.txt")
    entries = _unwrap(await env.list_dir(".", BACKGROUND_CONTEXT))
    assert sorted((entry["name"], entry["kind"]) for entry in entries) == [
        ("link.txt", "symlink"),
        ("target.txt", "file"),
    ]


async def test_stops_text_lines_at_the_requested_limit(env: PosixExecutionEnv) -> None:
    _unwrap(await env.write_file("file.txt", "one\ntwo\nthree", BACKGROUND_CONTEXT))
    assert _unwrap(await env.read_text_lines("file.txt", {"max_lines": 1}, BACKGROUND_CONTEXT)) == ["one"]
    assert _unwrap(await env.read_text_lines("file.txt", {"max_lines": 0}, BACKGROUND_CONTEXT)) == []


async def test_missing_paths_are_file_errors_and_exists_is_false(
    env: PosixExecutionEnv, tmp_path: Path
) -> None:
    info = await env.file_info("missing.txt", BACKGROUND_CONTEXT)
    assert isinstance(info, Err)
    assert isinstance(info.error, FileError)
    assert info.error.name == "FileError"
    assert info.error.code == "not_found"
    assert info.error.path == str(tmp_path / "missing.txt")
    assert _unwrap(await env.exists("missing.txt", BACKGROUND_CONTEXT)) is False


async def test_listing_a_non_directory_is_not_directory(env: PosixExecutionEnv) -> None:
    _unwrap(await env.write_file("file.txt", "hello", BACKGROUND_CONTEXT))
    result = await env.list_dir("file.txt", BACKGROUND_CONTEXT)
    assert isinstance(result, Err)
    assert result.error.code == "not_directory"


async def test_append_creates_parent_directories(env: PosixExecutionEnv) -> None:
    _unwrap(await env.append_file("new/nested/file.txt", "a", BACKGROUND_CONTEXT))
    _unwrap(await env.append_file("new/nested/file.txt", "b", BACKGROUND_CONTEXT))
    assert _unwrap(await env.read_text_file("new/nested/file.txt", BACKGROUND_CONTEXT)) == "ab"


async def test_rename_replaces_the_destination(env: PosixExecutionEnv) -> None:
    _unwrap(await env.write_file("source.txt", "new", BACKGROUND_CONTEXT))
    _unwrap(await env.write_file("destination.txt", "old", BACKGROUND_CONTEXT))
    _unwrap(await env.rename_file("source.txt", "destination.txt", BACKGROUND_CONTEXT))
    assert _unwrap(await env.exists("source.txt", BACKGROUND_CONTEXT)) is False
    assert _unwrap(await env.read_text_file("destination.txt", BACKGROUND_CONTEXT)) == "new"


async def test_rename_reports_the_source_path_when_missing(env: PosixExecutionEnv, tmp_path: Path) -> None:
    _unwrap(await env.write_file("destination.txt", "unchanged", BACKGROUND_CONTEXT))
    result = await env.rename_file("missing-source.txt", "destination.txt", BACKGROUND_CONTEXT)
    assert isinstance(result, Err)
    assert result.error.code == "not_found"
    assert result.error.path == str(tmp_path / "missing-source.txt")
    assert _unwrap(await env.read_text_file("destination.txt", BACKGROUND_CONTEXT)) == "unchanged"


async def test_creates_temporary_directories_and_files(env: PosixExecutionEnv) -> None:
    directory = _unwrap(await env.create_temp_dir("node-env-test-", BACKGROUND_CONTEXT))
    try:
        assert Path(directory).is_dir()
        file_path = _unwrap(
            await env.create_temp_file({"prefix": "prefix-", "suffix": ".txt"}, BACKGROUND_CONTEXT)
        )
        assert Path(file_path).is_file()
        assert file_path.endswith(".txt")
        assert _unwrap(await env.read_text_file(file_path, BACKGROUND_CONTEXT)) == ""
    finally:
        shutil.rmtree(directory, ignore_errors=True)


async def test_create_and_remove_options(env: PosixExecutionEnv) -> None:
    create_result = await env.create_dir("missing/child", {"recursive": False}, BACKGROUND_CONTEXT)
    assert isinstance(create_result, Err)
    assert create_result.error.code == "not_found"

    _unwrap(await env.write_file("dir/child/file.txt", "hello", BACKGROUND_CONTEXT))
    remove_directory = await env.remove("dir", {"recursive": False}, BACKGROUND_CONTEXT)
    assert isinstance(remove_directory, Err)
    _unwrap(await env.remove("dir", {"recursive": True}, BACKGROUND_CONTEXT))
    assert _unwrap(await env.exists("dir", BACKGROUND_CONTEXT)) is False

    remove_missing = await env.remove("missing", {"force": False}, BACKGROUND_CONTEXT)
    assert isinstance(remove_missing, Err)
    _unwrap(await env.remove("missing", {"force": True}, BACKGROUND_CONTEXT))


async def test_pre_aborted_operations_have_no_side_effects(env: PosixExecutionEnv) -> None:
    _unwrap(await env.write_file("file.txt", "hello", BACKGROUND_CONTEXT))
    context = _aborted_context()
    results = await asyncio.gather(
        env.read_text_file("file.txt", context),
        env.read_text_lines("file.txt", None, context),
        env.read_binary_file("file.txt", context),
        env.open_text_line_reader("file.txt", context),
        env.open_binary_reader("file.txt", None, context),
        env.open_dir_reader(".", context),
        env.list_files(".", None, context),
        env.write_file("other.txt", "hello", context),
        env.append_file("file.txt", " world", context),
        env.truncate_file("file.txt", 1, context),
        env.flush_file("file.txt", context),
        env.rename_file("file.txt", "renamed.txt", context),
        env.file_info("file.txt", context),
        env.list_dir(".", context),
        env.canonical_path("file.txt", context),
        env.exists("file.txt", context),
        env.create_dir("dir", None, context),
        env.remove("file.txt", None, context),
        env.create_temp_dir(None, context),
        env.create_temp_file(None, context),
        env.exec("printf spawned", None, context),
        env.watch([WatchTarget({"path": "file.txt"})], lambda change: None, context),
    )
    for result in results:
        assert isinstance(result, Err)
        assert result.error.code == "aborted"
    assert _unwrap(await env.read_text_file("file.txt", BACKGROUND_CONTEXT)) == "hello"
    assert sorted(entry["name"] for entry in _unwrap(await env.list_dir(".", BACKGROUND_CONTEXT))) == [
        "file.txt"
    ]


async def test_truncates_and_extends_files_to_exact_byte_sizes(env: PosixExecutionEnv) -> None:
    _unwrap(await env.write_file("file.bin", bytes([0x61, 0xC3, 0xA9, 0x0A, 0x62, 0x0A]), BACKGROUND_CONTEXT))
    _unwrap(await env.truncate_file("file.bin", 2, BACKGROUND_CONTEXT))
    assert _unwrap(await env.read_binary_file("file.bin", BACKGROUND_CONTEXT)) == bytes([0x61, 0xC3])
    _unwrap(await env.truncate_file("file.bin", 4, BACKGROUND_CONTEXT))
    assert _unwrap(await env.read_binary_file("file.bin", BACKGROUND_CONTEXT)) == bytes([0x61, 0xC3, 0, 0])
    _unwrap(await env.truncate_file("file.bin", 0, BACKGROUND_CONTEXT))
    assert _unwrap(await env.file_info("file.bin", BACKGROUND_CONTEXT))["size"] == 0


async def test_rejects_invalid_truncation_sizes_and_never_creates_missing_files(
    env: PosixExecutionEnv, tmp_path: Path
) -> None:
    _unwrap(await env.write_file("file.txt", "hello", BACKGROUND_CONTEXT))
    for size in [-1, 1.5, float("nan"), float("inf"), 9_007_199_254_740_992]:
        result = await env.truncate_file("file.txt", size, BACKGROUND_CONTEXT)  # type: ignore[arg-type]
        assert isinstance(result, Err)
        assert result.error.code == "invalid"
        assert result.error.path == str(tmp_path / "file.txt")
    assert _unwrap(await env.read_text_file("file.txt", BACKGROUND_CONTEXT)) == "hello"
    missing = await env.truncate_file("missing.txt", 0, BACKGROUND_CONTEXT)
    assert isinstance(missing, Err)
    assert missing.error.code == "not_found"
    assert _unwrap(await env.exists("missing.txt", BACKGROUND_CONTEXT)) is False


async def test_flush_existing_files_and_report_missing_or_directories(
    env: PosixExecutionEnv, tmp_path: Path
) -> None:
    _unwrap(await env.write_file("file.txt", "durable", BACKGROUND_CONTEXT))
    _unwrap(await env.flush_file("file.txt", BACKGROUND_CONTEXT))
    assert _unwrap(await env.read_text_file("file.txt", BACKGROUND_CONTEXT)) == "durable"
    missing = await env.flush_file("missing.txt", BACKGROUND_CONTEXT)
    assert isinstance(missing, Err)
    assert missing.error.code == "not_found"
    _unwrap(await env.create_dir("dir", None, BACKGROUND_CONTEXT))
    directory = await env.flush_file("dir", BACKGROUND_CONTEXT)
    assert isinstance(directory, Err)
    assert directory.error.code == "is_directory"


async def test_cleanup_is_best_effort(env: PosixExecutionEnv) -> None:
    await env.cleanup(BACKGROUND_CONTEXT)


async def test_text_line_reader_reports_termination_and_keeps_carriage_returns(
    env: PosixExecutionEnv,
) -> None:
    _unwrap(await env.write_file("lines.txt", "one\r\n\ntwo\npartial", BACKGROUND_CONTEXT))
    reader = _unwrap(await env.open_text_line_reader("lines.txt", BACKGROUND_CONTEXT))
    try:
        lines = []
        while True:
            line = _unwrap(await reader.read_line(BACKGROUND_CONTEXT))
            if line is None:
                break
            lines.append(line)
        assert lines == [
            {"text": "one\r", "terminated": True},
            {"text": "", "terminated": True},
            {"text": "two", "terminated": True},
            {"text": "partial", "terminated": False},
        ]
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) is None
    finally:
        await reader.close(BACKGROUND_CONTEXT)


async def test_text_line_reader_handles_empty_and_lone_newline(env: PosixExecutionEnv) -> None:
    _unwrap(await env.write_file("empty.txt", "", BACKGROUND_CONTEXT))
    _unwrap(await env.write_file("newline.txt", "\n", BACKGROUND_CONTEXT))
    empty = _unwrap(await env.open_text_line_reader("empty.txt", BACKGROUND_CONTEXT))
    assert _unwrap(await empty.read_line(BACKGROUND_CONTEXT)) is None
    await empty.close(BACKGROUND_CONTEXT)
    newline = _unwrap(await env.open_text_line_reader("newline.txt", BACKGROUND_CONTEXT))
    assert _unwrap(await newline.read_line(BACKGROUND_CONTEXT)) == {"text": "", "terminated": True}
    assert _unwrap(await newline.read_line(BACKGROUND_CONTEXT)) is None
    await newline.close(BACKGROUND_CONTEXT)


async def test_text_line_reader_decodes_characters_split_across_chunks(env: PosixExecutionEnv) -> None:
    first = "a" * (64 * 1024 - 2) + "😀tail"
    second = "é" * 100_000
    _unwrap(await env.write_file("large.txt", f"{first}\n{second}", BACKGROUND_CONTEXT))
    reader = _unwrap(await env.open_text_line_reader("large.txt", BACKGROUND_CONTEXT))
    try:
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) == {"text": first, "terminated": True}
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) == {"text": second, "terminated": False}
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) is None
    finally:
        await reader.close(BACKGROUND_CONTEXT)


async def test_text_line_reader_rejects_a_cancelled_pending_read_without_consuming_bytes(
    env: PosixExecutionEnv,
) -> None:
    _unwrap(await env.write_file("lines.txt", "one\ntwo\n", BACKGROUND_CONTEXT))
    reader = _unwrap(await env.open_text_line_reader("lines.txt", BACKGROUND_CONTEXT))
    try:
        controller = AbortController()
        pending = reader.read_line(with_abort_signal(controller.signal, BACKGROUND_CONTEXT))
        controller.abort()
        result = await pending
        assert isinstance(result, Err)
        assert result.error.code == "aborted"
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) == {"text": "one", "terminated": True}
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) == {"text": "two", "terminated": True}
        assert _unwrap(await reader.read_line(BACKGROUND_CONTEXT)) is None
    finally:
        await reader.close(BACKGROUND_CONTEXT)


async def test_text_line_reader_rejects_reads_after_close_and_closes_idempotently(
    env: PosixExecutionEnv,
) -> None:
    _unwrap(await env.write_file("lines.txt", "one\n", BACKGROUND_CONTEXT))
    reader = _unwrap(await env.open_text_line_reader("lines.txt", BACKGROUND_CONTEXT))
    await reader.close(BACKGROUND_CONTEXT)
    await reader.close(BACKGROUND_CONTEXT)
    result = await reader.read_line(BACKGROUND_CONTEXT)
    assert isinstance(result, Err)
    assert result.error.code == "invalid"


async def test_open_text_line_reader_reports_missing_files(env: PosixExecutionEnv, tmp_path: Path) -> None:
    result = await env.open_text_line_reader("missing.txt", BACKGROUND_CONTEXT)
    assert isinstance(result, Err)
    assert result.error.code == "not_found"
    assert result.error.path == str(tmp_path / "missing.txt")


async def test_large_binary_reads_span_internal_chunks(env: PosixExecutionEnv) -> None:
    data = bytes((index * 31) % 251 for index in range(int(2.5 * 1024 * 1024)))
    _unwrap(await env.write_file("big.bin", data, BACKGROUND_CONTEXT))
    reader = _unwrap(await env.open_binary_reader("big.bin", None, BACKGROUND_CONTEXT))
    try:
        assert _unwrap(await reader.read(0, len(data) + 10, BACKGROUND_CONTEXT)) == data
        middle = _unwrap(await reader.read(1024 * 1024 - 3, 7, BACKGROUND_CONTEXT))
        assert middle == data[1024 * 1024 - 3 : 1024 * 1024 + 4]
    finally:
        await reader.close(BACKGROUND_CONTEXT)


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFOs are POSIX-only")
async def test_binary_reader_refuses_a_fifo_without_waiting_for_a_writer(
    env: PosixExecutionEnv, tmp_path: Path
) -> None:
    os.mkfifo(tmp_path / "pipe")
    result = await env.open_binary_reader("pipe", None, BACKGROUND_CONTEXT)
    assert isinstance(result, Err)
    assert result.error.code == "invalid"


async def test_shell_executes_in_cwd_with_env_overrides(env: PosixExecutionEnv) -> None:
    result, output = await _collect(
        env,
        'printf \'%s\' "$PI_ENV_TEST" > cwd-marker.txt; printf \'%s:%s\' "$PWD" "$PI_ENV_TEST"',
        ShellExecOptions(env={"PI_ENV_TEST": "ok"}),
    )
    assert _unwrap(result)["exitCode"] == 0
    assert Path(env.cwd, "cwd-marker.txt").read_text() == "ok"
    assert output == f"{os.path.realpath(env.cwd)}:ok"


async def test_shell_environment_overrides(env: PosixExecutionEnv) -> None:
    configured = PosixExecutionEnv(
        cwd=env.cwd,
        shell_env={
            "PI_SESSION_FILE": "/stale/parent.jsonl",
            "PI_CODING_AGENT": "true",
            "PI_ENV_PRESERVED_TEST": "preserved",
        },
    )
    _, output = await _collect(
        configured,
        'printf \'%s:%s|%s|%s\' "${PI_SESSION_FILE+x}" "${PI_SESSION_FILE-}" '
        '"$PI_CODING_AGENT" "$PI_ENV_PRESERVED_TEST"',
        None,
    )
    assert output == "x:/stale/parent.jsonl|true|preserved"
    _, output = await _collect(
        configured,
        'printf \'%s:%s|%s|%s\' "${PI_SESSION_FILE+x}" "${PI_SESSION_FILE-}" '
        '"$PI_CODING_AGENT" "$PI_ENV_PRESERVED_TEST"',
        ShellExecOptions(env={"PI_SESSION_FILE": ""}),
    )
    assert output == "x:|true|preserved"
    _, output = await _collect(
        configured,
        'printf \'%s:%s|%s|%s\' "${PI_SESSION_FILE+x}" "${PI_SESSION_FILE-}" '
        '"$PI_CODING_AGENT" "$PI_ENV_PRESERVED_TEST"',
        ShellExecOptions(env={"PI_SESSION_FILE": "/sessions/current.jsonl"}),
    )
    assert output == "x:/sessions/current.jsonl|true|preserved"


async def test_shell_can_replace_the_inherited_environment(env: PosixExecutionEnv) -> None:
    os.environ["PI_ENV_INHERITED_TEST"] = "host"
    try:
        configured = PosixExecutionEnv(cwd=env.cwd, shell_env={"PI_ENV_CONFIGURED_TEST": "configured"})
        _, output = await _collect(
            configured,
            'printf \'%s:%s:%s\' "${PI_ENV_INHERITED_TEST-}" "${PI_ENV_CONFIGURED_TEST-}" '
            '"${PI_ENV_EXPLICIT_TEST-}"',
            ShellExecOptions(inherit_env=False, env={"PI_ENV_EXPLICIT_TEST": "explicit"}),
        )
        assert output == "::explicit"
    finally:
        del os.environ["PI_ENV_INHERITED_TEST"]


async def test_shell_streams_stdout_and_stderr(env: PosixExecutionEnv) -> None:
    result, output = await _collect(env, "printf out; printf err >&2", None)
    assert _unwrap(result) == {"exitCode": 0}
    assert "out" in output
    assert "err" in output


async def test_shell_decodes_utf8_split_across_raw_chunks(env: PosixExecutionEnv) -> None:
    script = (
        "import sys,time\n"
        "b='😀'.encode()\n"
        "sys.stdout.buffer.write(b[:2]); sys.stdout.flush()\n"
        "time.sleep(0.05)\n"
        "sys.stdout.buffer.write(b[2:]); sys.stdout.flush()\n"
    )
    _, output = await _collect(env, f"{sys.executable} -c {_quote(script)}", None)
    assert output == "😀"


def _quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


async def test_shell_reports_missing_working_directory(env: PosixExecutionEnv, tmp_path: Path) -> None:
    missing = PosixExecutionEnv(cwd=str(tmp_path / "missing"))
    result, _ = await _collect(missing, "printf ok", None)
    assert isinstance(result, Err)
    assert result.error.code == "spawn_error"
    assert "Working directory does not exist" in str(result.error)


async def test_shell_returns_non_zero_exit_codes(env: PosixExecutionEnv) -> None:
    result, _ = await _collect(env, "exit 7", None)
    assert _unwrap(result) == {"exitCode": 7}


async def test_shell_maps_signal_killed_processes(env: PosixExecutionEnv) -> None:
    result, _ = await _collect(env, "kill -9 $$", None)
    assert _unwrap(result)["exitCode"] == 128 + 9


async def test_shell_timeout_and_invalid_timeouts(env: PosixExecutionEnv) -> None:
    result, _ = await _collect(env, "sleep 5", ShellExecOptions(timeout=0.01))
    assert isinstance(result, Err)
    assert result.error.code == "timeout"
    for timeout in [0, -1, float("nan"), float("inf"), 2_147_484]:
        result, _ = await _collect(env, "touch spawned", ShellExecOptions(timeout=timeout))
        assert isinstance(result, Err)
        assert result.error.code == "timeout"
        assert "Invalid timeout" in str(result.error)
    assert _unwrap(await env.exists("spawned", BACKGROUND_CONTEXT)) is False


async def test_shell_returns_callback_errors(env: PosixExecutionEnv) -> None:
    def on_output(text: str, _context: Context, _info: ShellOutputInfo) -> None:
        raise RuntimeError("callback failed")

    result = await env.exec("printf out", ShellExecOptions(on_output=on_output), BACKGROUND_CONTEXT)
    assert isinstance(result, Err)
    assert result.error.code == "callback_error"
    assert str(result.error) == "callback failed"


async def test_shell_unavailable_and_spawn_errors(env: PosixExecutionEnv, tmp_path: Path) -> None:
    missing = PosixExecutionEnv(cwd=env.cwd, shell_path=str(tmp_path / "missing-shell"))
    result, _ = await _collect(missing, "printf ok", None)
    assert isinstance(result, Err)
    assert result.error.code == "shell_unavailable"

    shell_path = tmp_path / "not-executable-shell"
    shell_path.write_text("not executable")
    bad = PosixExecutionEnv(cwd=env.cwd, shell_path=str(shell_path))
    result, _ = await _collect(bad, "printf ok", None)
    assert isinstance(result, Err)
    assert result.error.code == "spawn_error"


async def test_shell_abort(env: PosixExecutionEnv) -> None:
    pre = await env.exec("touch spawned", None, _aborted_context())
    assert isinstance(pre, Err)
    assert pre.error.code == "aborted"
    assert _unwrap(await env.exists("spawned", BACKGROUND_CONTEXT)) is False

    controller = AbortController()
    running = env.exec("sleep 5", None, with_abort_signal(controller.signal, BACKGROUND_CONTEXT))
    controller.abort()
    result = await running
    assert isinstance(result, Err)
    assert result.error.code == "aborted"


async def test_cleanup_terminates_active_shell_processes(env: PosixExecutionEnv) -> None:
    execution = asyncio.create_task(env.exec("touch started; sleep 60", None, BACKGROUND_CONTEXT))

    async def started() -> bool:
        return _unwrap(await env.exists("started", BACKGROUND_CONTEXT))

    try:
        await _wait_for(started)
    finally:
        await env.cleanup(BACKGROUND_CONTEXT)
    result = await asyncio.wait_for(execution, 3)
    assert isinstance(result, Ok)


async def test_shell_does_not_spill_before_thresholds(env: PosixExecutionEnv) -> None:
    result, _ = await _collect(
        env,
        "printf short",
        ShellExecOptions(spill={"after_bytes": 100, "after_lines": 10}),
    )
    assert "spillPath" not in _unwrap(result)


async def test_shell_preserves_raw_spill_bytes(env: PosixExecutionEnv) -> None:
    expected = bytes([0x66, 0x80, 0x00, 0x6F])
    script = f"import sys; sys.stdout.buffer.write(bytes({list(expected)!r}))"
    result, _ = await _collect(
        env,
        f"{sys.executable} -c {_quote(script)}",
        ShellExecOptions(spill={"after_bytes": 1, "after_lines": 10}),
    )
    spill_path = _unwrap(result)["spillPath"]
    try:
        assert _unwrap(await env.read_binary_file(spill_path, BACKGROUND_CONTEXT)) == expected
    finally:
        shutil.rmtree(os.path.dirname(spill_path), ignore_errors=True)


async def test_shell_reports_spill_of_a_timed_out_command(env: PosixExecutionEnv) -> None:
    result, _ = await _collect(
        env,
        "printf 12345678901234567890; sleep 5",
        ShellExecOptions(timeout=0.3, spill={"after_bytes": 10, "after_lines": 10}),
    )
    assert isinstance(result, Err)
    assert result.error.code == "timeout"
    assert result.error.spill_path is not None
    try:
        assert (
            _unwrap(await env.read_text_file(result.error.spill_path, BACKGROUND_CONTEXT))
            == "12345678901234567890"
        )
    finally:
        shutil.rmtree(os.path.dirname(result.error.spill_path), ignore_errors=True)


class _FailingSpillEnv(PosixExecutionEnv):
    async def create_temp_file(self, options: dict[str, Any] | None, context: Context):
        if options is not None and options.get("prefix") == "pi-output-":
            return Err(FileError("not_found", "missing", os.path.join(self.cwd, "missing", "spill.log")))
        return await super().create_temp_file(options, context)


async def test_shell_fails_rather_than_losing_a_requested_spill(env: PosixExecutionEnv) -> None:
    failing = _FailingSpillEnv(cwd=env.cwd)
    result, _ = await _collect(
        failing,
        "printf 12345678901234567890",
        ShellExecOptions(spill={"after_bytes": 10, "after_lines": 10}),
    )
    assert isinstance(result, Err)
    assert result.error.code == "unknown"
    assert "Failed to preserve complete shell output" in str(result.error)


async def test_shell_preserves_complete_large_output_in_the_spill(env: PosixExecutionEnv) -> None:
    size = 500_000
    script = f"print('x' * {size}, end='')"
    result, _ = await _collect(
        env,
        f"{sys.executable} -c {_quote(script)}",
        ShellExecOptions(spill={"after_bytes": 10, "after_lines": 10}),
    )
    spill_path = _unwrap(result)["spillPath"]
    try:
        assert len(_unwrap(await env.read_text_file(spill_path, BACKGROUND_CONTEXT))) == size
    finally:
        shutil.rmtree(os.path.dirname(spill_path), ignore_errors=True)


async def test_shell_streams_every_line_and_spills_them(env: PosixExecutionEnv) -> None:
    result, output = await _collect(
        env,
        "i=1; while [ $i -le 15000 ]; do echo line-$i; i=$((i+1)); done",
        ShellExecOptions(spill={"after_bytes": 1024 * 1024, "after_lines": 100}),
    )
    assert output == "".join(f"line-{index}\n" for index in range(1, 15001))
    spill_path = _unwrap(result)["spillPath"]
    try:
        spilled = _unwrap(await env.read_text_lines(spill_path, None, BACKGROUND_CONTEXT))
        assert len(spilled) == 15000
        assert spilled[-1] == "line-15000"
    finally:
        shutil.rmtree(os.path.dirname(spill_path), ignore_errors=True)


async def test_watch_refuses_and_errors_on_a_directory_budget(tmp_path: Path) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path), watch={"max_directories": 3, "poll_interval_ms": 50})
    _unwrap(await env.create_dir("tree/a/b", None, BACKGROUND_CONTEXT))
    _unwrap(await env.create_dir("tree/c", None, BACKGROUND_CONTEXT))
    refused = await env.watch(
        [WatchTarget({"path": "tree", "recursive": True})], lambda change: None, BACKGROUND_CONTEXT
    )
    assert isinstance(refused, Err)
    assert refused.error.code == "invalid"

    _unwrap(await env.remove("tree/c", {"recursive": True}, BACKGROUND_CONTEXT))
    changes: list[WatchChange] = []
    watcher = _unwrap(
        await env.watch(
            [WatchTarget({"path": "tree", "recursive": True})], changes.append, BACKGROUND_CONTEXT
        )
    )
    try:
        _unwrap(await env.create_dir("tree/d", None, BACKGROUND_CONTEXT))
        deadline = asyncio.get_running_loop().time() + 3
        while (
            not any("error" in change for change in changes) and asyncio.get_running_loop().time() < deadline
        ):
            await asyncio.sleep(0.02)
        assert changes
        assert changes[-1]["error"].code == "invalid"
    finally:
        await watcher.close(BACKGROUND_CONTEXT)


async def test_shell_keeps_reading_descendant_output_while_a_spill_is_pending(
    env: PosixExecutionEnv,
) -> None:
    command = "printf '%020d' 0 | tr 0 a; (sleep 0.2; printf '%01000d' 0 | tr 0 b) &"
    result, output = await _collect(
        env,
        command,
        ShellExecOptions(spill={"after_bytes": 10, "after_lines": 10}),
    )
    assert output == "a" * 20 + "b" * 1000
    spill_path = _unwrap(result)["spillPath"]
    try:
        assert _unwrap(await env.read_text_file(spill_path, BACKGROUND_CONTEXT)) == "a" * 20 + "b" * 1000
    finally:
        shutil.rmtree(os.path.dirname(spill_path), ignore_errors=True)

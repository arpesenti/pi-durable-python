"""Runner-independent ``ExecutionEnv`` conformance cases.

Translated from v1.3.0 ``testing/env-conformance``. A provider supplies a fresh, empty, writable
directory for every case, so the suite exercises only the public environment contract.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from ..context import (
    BACKGROUND_CONTEXT,
    AbortController,
    Context,
    with_abort_signal,
)
from ..env import (
    ExecutionEnv,
    FileInfo,
    ListFilesOptions,
    Result,
    ShellExecOptions,
    ShellOutputInfo,
    WatchChange,
    WatchTarget,
    get_or_throw,
)
from ..truncate import utf8_byte_length
from .types import (
    EnvConformanceAssertions,
    EnvConformanceCase,
    EnvConformanceOptions,
)

context = BACKGROUND_CONTEXT


def _aborted_context() -> Context:
    controller = AbortController()
    controller.abort()
    return with_abort_signal(controller.signal, BACKGROUND_CONTEXT)


def _error_code(result: Result[Any, Any]) -> str | None:
    return None if result.ok else result.error.code


async def _read_all(env: ExecutionEnv, path: str, max_entries: int) -> tuple[list[list[FileInfo]], bool]:
    reader = get_or_throw(await env.open_dir_reader(path, context))
    pages: list[list[FileInfo]] = []
    try:
        for _ in range(1000):
            next_page = get_or_throw(await reader.next(max_entries, context))
            pages.append(next_page["entries"])
            if next_page["done"]:
                return pages, True
        return pages, False
    finally:
        await reader.close(context)


def _watch_targets(*entries: dict[str, Any]) -> list[WatchTarget]:
    return [WatchTarget(entry) for entry in entries]


def _list_options(entry: dict[str, Any] | None) -> ListFilesOptions | None:
    return None if entry is None else ListFilesOptions(entry)


def _shell_options(entry: dict[str, Any]) -> ShellExecOptions:
    return ShellExecOptions(entry)


async def _listed(
    env: ExecutionEnv, path: str, options: dict[str, Any] | None = None
) -> tuple[list[str], bool]:
    listing = get_or_throw(await env.list_files(path, _list_options(options), context))
    letters = {"file": "f", "directory": "d", "symlink": "l"}
    return [f"{letters[entry['kind']]} {entry['path']}" for entry in listing["entries"]], listing["truncated"]


def _covers(change: WatchChange, path: str) -> bool:
    if "overflow" in change:
        return True
    if "paths" not in change:
        return False
    return any(
        path == reported or path.startswith(reported + "/") or path.startswith(reported + "\\")
        for reported in change["paths"]
    )


async def _watching(
    env: ExecutionEnv,
    targets: Sequence[WatchTarget],
    run: Callable[
        [
            list[WatchChange],
            Callable[[str, Callable[[], Awaitable[None]]], Awaitable[None]],
            Callable[[str], Awaitable[str]],
        ],
        Awaitable[None],
    ],
) -> None:
    changes: list[WatchChange] = []
    watcher = get_or_throw(await env.watch(targets, changes.append, context))

    async def absolute(path: str) -> str:
        return get_or_throw(await env.absolute_path(path, context))

    async def expect_change(path: str, change: Callable[[], Awaitable[None]]) -> None:
        target = await absolute(path)
        start = len(changes)
        await change()
        deadline = asyncio.get_running_loop().time() + 3
        while not any(_covers(entry, target) for entry in changes[start:]):
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"No change reported {target}; got {changes[start:]!r}")
            await asyncio.sleep(0.02)

    try:
        await run(changes, expect_change, absolute)
    finally:
        await watcher.close(context)


async def _exec_collect(
    env: ExecutionEnv,
    command: str | Sequence[str],
    cwd: str | None = None,
) -> tuple[Result[Any, Any], str, str]:
    output = {"stdout": "", "stderr": ""}

    def on_output(text: str, _context: Context, info: ShellOutputInfo) -> None:
        output[info.stream] += text

    options: dict[str, Any] = {"on_output": on_output}
    if cwd is not None:
        options["cwd"] = cwd
    result = await env.exec(command, _shell_options(options), context)
    return result, output["stdout"], output["stderr"]


def create_env_conformance(options: EnvConformanceOptions) -> list[EnvConformanceCase]:
    """Create runner-independent cases for an :class:`ExecutionEnv`."""

    assert_ = options["assertions"]
    shell = options.get("shell", ["sh", "-c"])
    symlinks = options.get("symlinks", True)
    list_files = options.get("list_files", False)

    def create_case(
        name: str, test: Callable[[ExecutionEnv], Awaitable[None]], timeout_ms: int | None = None
    ) -> EnvConformanceCase:
        def run() -> Awaitable[None]:
            return options["with_env"](test)

        return EnvConformanceCase(name, run, timeout_ms)

    def _watch_case(name: str, test: Callable[[ExecutionEnv], Awaitable[None]]) -> EnvConformanceCase:
        return create_case(name, test, timeout_ms=30_000)

    cases: list[EnvConformanceCase] = [
        create_case(
            "binary reader reads byte ranges of the opened file",
            lambda env: _binary_reader_ranges(env, assert_),
        ),
        create_case(
            "binary reader scans lines like decoding the whole file",
            lambda env: _binary_reader_scans(env, assert_),
        ),
        create_case(
            "binary reader keeps reading the file it opened after a rename",
            lambda env: _binary_reader_after_rename(env, assert_),
        ),
        create_case(
            "binary reader refuses directories, missing files and aborted opens",
            lambda env: _binary_reader_refusals(env, assert_),
        ),
        create_case(
            "directory reader pages every entry exactly once",
            lambda env: _directory_reader_pages(env, assert_),
        ),
        create_case(
            "directory reader reports the end and refuses use after close",
            lambda env: _directory_reader_end(env, assert_),
        ),
        create_case(
            "directory reader refuses missing paths and files",
            lambda env: _directory_reader_refusals(env, assert_),
        ),
        create_case(
            "directory reader skips entries removed during enumeration",
            lambda env: _directory_reader_removed(env, assert_),
        ),
        _watch_case(
            "watch reports a missing file's creation, changes, replacement and removal",
            lambda env: _watch_file_lifecycle(env, assert_),
        ),
        _watch_case(
            "watch reports a missing target whose ancestors are created",
            lambda env: _watch_missing_ancestors(env, assert_),
        ),
        _watch_case(
            "watch follows directories created together with their contents",
            lambda env: _watch_new_directory_tree(env, assert_),
        ),
        _watch_case(
            "watch keeps watching a path whose parent is renamed and recreated",
            lambda env: _watch_renamed_parent(env, assert_),
        ),
        _watch_case(
            "watch skips excluded entries and reports a rename out of them",
            lambda env: _watch_excluded(env, assert_),
        ),
        _watch_case(
            "watch keeps recursive coverage where a non-recursive target overlaps",
            lambda env: _watch_overlapping(env, assert_),
        ),
        _watch_case(
            "watch follows a directory replaced at the same path",
            lambda env: _watch_replaced_directory(env, assert_),
        ),
        _watch_case(
            "watch stops reporting once closed",
            lambda env: _watch_closed(env, assert_),
        ),
        create_case(
            "argv exec passes arguments to the program without shell parsing",
            lambda env: _exec_argv(env, assert_, shell),
        ),
        create_case(
            "exec reports the stream of every chunk in both forms",
            lambda env: _exec_streams(env, assert_, shell),
        ),
        create_case(
            "argv exec honors cwd and exit codes",
            lambda env: _exec_cwd(env, assert_, shell),
        ),
        create_case(
            "argv exec reports missing programs and empty argv as spawn errors",
            lambda env: _exec_spawn_errors(env, assert_),
        ),
        create_case(
            "windowed exec keeps the exact tail and counts what it skips",
            lambda env: _exec_window(env, assert_, shell),
        ),
        create_case(
            "argv exec distinguishes timeout from abort",
            lambda env: _exec_timeout(env, assert_, shell),
        ),
    ]

    if list_files:
        cases.extend(
            [
                create_case(
                    "listFiles lists files and directories depth first in name order",
                    lambda env: _list_files_order(env, assert_),
                ),
                create_case(
                    "listFiles skips what git ignores in work trees, .git, and excluded names",
                    lambda env: _list_files_gitignore(env, assert_),
                ),
                create_case(
                    "listFiles stops at its bounds and reports truncated",
                    lambda env: _list_files_bounds(env, assert_),
                ),
                create_case(
                    "listFiles fails on missing directories, files, and aborts",
                    lambda env: _list_files_errors(env, assert_),
                ),
            ]
        )

    if symlinks:
        cases.append(
            create_case(
                "binary reader follows symlinks unless noFollow refuses the final one",
                lambda env: _binary_reader_symlinks(env, assert_, shell),
            )
        )
        if list_files:
            cases.append(
                create_case(
                    "listFiles lists symbolic links without following them",
                    lambda env: _list_files_symlinks(env, assert_, shell),
                )
            )
        cases.append(
            _watch_case(
                "watch reports changes to the file a watched symbolic link points to",
                lambda env: _watch_symlink_target(env, assert_, shell),
            )
        )

    return cases


async def _binary_reader_ranges(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.write_file("data.txt", "hello world", context))
    reader = get_or_throw(await env.open_binary_reader("data.txt", None, context))
    info = get_or_throw(await reader.info(context))
    assert_.partial_deep_equal(info, {"name": "data.txt", "kind": "file", "size": 11})
    assert_.strict_equal(get_or_throw(await reader.read(0, 5, context)).decode(), "hello")
    assert_.strict_equal(get_or_throw(await reader.read(6, 100, context)).decode(), "world")
    assert_.strict_equal(len(get_or_throw(await reader.read(11, 4, context))), 0)
    assert_.strict_equal(len(get_or_throw(await reader.read(50, 1, context))), 0)
    assert_.strict_equal(len(get_or_throw(await reader.read(3, 0, context))), 0)
    assert_.strict_equal(_error_code(await reader.read(-1, 1, context)), "invalid")
    assert_.strict_equal(_error_code(await reader.read(0, 1.5, context)), "invalid")  # type: ignore[arg-type]
    assert_.strict_equal(_error_code(await reader.read(0, 1, _aborted_context())), "aborted")
    await reader.close(context)
    await reader.close(context)
    assert_.strict_equal(_error_code(await reader.read(0, 1, context)), "invalid")
    assert_.strict_equal(_error_code(await reader.info(context)), "invalid")


async def _binary_reader_scans(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    data = bytes(
        [
            0xEF,
            0xBB,
            0xBF,
            0x61,
            0x0A,
            0xE2,
            0x82,
            0x0A,
            0x0A,
            0xEF,
            0xBB,
            0xBF,
            0x62,
            0x0A,
            0xC3,
            0xA9,
        ]
    )
    get_or_throw(await env.write_file("lines.txt", data, context))
    decoded = data.decode("utf-8", "replace")
    lines = (decoded[1:] if decoded.startswith("\ufeff") else decoded).split("\n")
    reader = get_or_throw(await env.open_binary_reader("lines.txt", None, context))
    try:
        for start_line, end_line in [
            (0, None),
            (0, 1),
            (1, 3),
            (2, 3),
            (3, None),
            (4, 9),
        ]:
            selection: dict[str, Any] = {"start_line": start_line}
            if end_line is not None:
                selection["end_line"] = end_line
            scan = get_or_throw(await reader.scan_lines(selection, context))
            selected = lines[start_line:end_line]

            def decode_range(start: int, end: int, ignore_bom: bool) -> str:
                payload = data[start:end]
                if not ignore_bom and payload.startswith(b"\xef\xbb\xbf"):
                    payload = payload[3:]
                return payload.decode("utf-8", "replace")

            assert_.strict_equal(scan["newlines"], len(lines) - 1)
            assert_.strict_equal(
                decode_range(scan["start"], scan["end"], scan["start"] > 0),
                "\n".join(selected),
            )
            assert_.strict_equal(scan["selectedBytes"], utf8_byte_length("\n".join(selected)))
            assert_.strict_equal(
                decode_range(scan["start"], scan["firstLineEnd"], scan["start"] > 0), lines[start_line]
            )
            assert_.strict_equal(scan["firstLineBytes"], utf8_byte_length(lines[start_line]))
        assert_.partial_deep_equal(
            get_or_throw(await reader.scan_lines({"start_line": 9}, context)),
            {"start": len(data), "end": len(data), "selectedBytes": 0},
        )
        assert_.strict_equal(
            _error_code(await reader.scan_lines({"start_line": 2, "end_line": 2}, context)),
            "invalid",
        )
    finally:
        await reader.close(context)


async def _binary_reader_after_rename(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.write_file("a.txt", "one", context))
    reader = get_or_throw(await env.open_binary_reader("a.txt", None, context))
    try:
        get_or_throw(await env.rename_file("a.txt", "b.txt", context))
        get_or_throw(await env.write_file("a.txt", "two", context))
        assert_.strict_equal(get_or_throw(await reader.read(0, 10, context)).decode(), "one")
    finally:
        await reader.close(context)


async def _binary_reader_refusals(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.create_dir("dir", None, context))
    get_or_throw(await env.write_file("file.txt", "x", context))
    assert_.strict_equal(_error_code(await env.open_binary_reader("dir", None, context)), "is_directory")
    assert_.strict_equal(_error_code(await env.open_binary_reader("missing.txt", None, context)), "not_found")
    assert_.strict_equal(
        _error_code(await env.open_binary_reader("file.txt", None, _aborted_context())),
        "aborted",
    )


async def _directory_reader_pages(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    names = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"]
    for name in names:
        get_or_throw(await env.write_file(name, name, context))
    get_or_throw(await env.create_dir("sub", None, context))
    pages, done = await _read_all(env, ".", 2)
    assert_.ok(done, "directory reader reached the end")
    for page in pages:
        assert_.ok(len(page) <= 2, "page within maxEntries")
    entries = [entry for page in pages for entry in page]
    assert_.deep_equal(sorted(entry["name"] for entry in entries), sorted([*names, "sub"]))
    sub = next(entry for entry in entries if entry["name"] == "sub")
    assert_.strict_equal(sub["kind"], "directory")
    assert_.partial_deep_equal(
        next(entry for entry in entries if entry["name"] == "a.txt"),
        {"kind": "file", "size": 5},
    )


async def _directory_reader_end(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.create_dir("empty", None, context))
    reader = get_or_throw(await env.open_dir_reader("empty", context))
    assert_.deep_equal(get_or_throw(await reader.next(10, context)), {"entries": [], "done": True})
    assert_.deep_equal(get_or_throw(await reader.next(10, context)), {"entries": [], "done": True})
    assert_.strict_equal(_error_code(await reader.next(0, context)), "invalid")
    assert_.strict_equal(_error_code(await reader.next(1, _aborted_context())), "aborted")
    await reader.close(context)
    await reader.close(context)
    assert_.strict_equal(_error_code(await reader.next(1, context)), "invalid")


async def _directory_reader_refusals(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.write_file("file.txt", "x", context))
    assert_.strict_equal(_error_code(await env.open_dir_reader("missing", context)), "not_found")
    file_result = await env.open_dir_reader("file.txt", context)
    assert_.strict_equal(_error_code(file_result), "not_directory")
    assert_.strict_equal(_error_code(await env.open_dir_reader(".", _aborted_context())), "aborted")


async def _directory_reader_removed(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.create_dir("dir", None, context))
    for name in ["x", "y", "z"]:
        get_or_throw(await env.write_file(f"dir/{name}", name, context))
    reader = get_or_throw(await env.open_dir_reader("dir", context))
    try:
        for name in ["x", "y", "z"]:
            get_or_throw(await env.remove(f"dir/{name}", None, context))
        entries: list[FileInfo] = []
        for _ in range(10):
            next_page = get_or_throw(await reader.next(10, context))
            entries.extend(next_page["entries"])
            if next_page["done"]:
                break
        assert_.deep_equal(entries, [])
    finally:
        await reader.close(context)


async def _watch_file_lifecycle(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    del assert_

    async def run(changes, expect_change, absolute):
        del changes, absolute
        await expect_change("AGENTS.md", lambda: env.write_file("AGENTS.md", "one", context))
        await expect_change("AGENTS.md", lambda: env.write_file("AGENTS.md", "two!", context))
        await expect_change(
            "AGENTS.md",
            lambda: _replace_file(env),
        )
        await expect_change("AGENTS.md", lambda: env.write_file("AGENTS.md", "four", context))
        await expect_change("AGENTS.md", lambda: env.remove("AGENTS.md", None, context))

    await _watching(env, _watch_targets({"path": "AGENTS.md"}), run)


async def _replace_file(env: ExecutionEnv) -> None:
    get_or_throw(await env.write_file("AGENTS.md.tmp", "three", context))
    get_or_throw(await env.rename_file("AGENTS.md.tmp", "AGENTS.md", context))


async def _watch_missing_ancestors(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    del assert_

    async def run(changes, expect_change, absolute):
        del changes, absolute
        await expect_change("a/b/c/AGENTS.md", lambda: env.write_file("a/b/c/AGENTS.md", "x", context))

    await _watching(env, _watch_targets({"path": "a/b/c/AGENTS.md"}), run)


async def _watch_new_directory_tree(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    del assert_
    get_or_throw(await env.create_dir("skills", None, context))

    async def run(changes, expect_change, absolute):
        del changes, absolute
        await expect_change(
            "skills/a/b/SKILL.md",
            lambda: env.write_file("skills/a/b/SKILL.md", "one", context),
        )
        await expect_change(
            "skills/a/b/SKILL.md",
            lambda: env.write_file("skills/a/b/SKILL.md", "two!", context),
        )
        await expect_change(
            "skills/a/b/c/SKILL.md",
            lambda: env.write_file("skills/a/b/c/SKILL.md", "deeper", context),
        )

    await _watching(env, _watch_targets({"path": "skills", "recursive": True}), run)


async def _watch_renamed_parent(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    del assert_
    get_or_throw(await env.write_file("proj/.pi/skills/x.md", "x", context))

    async def run(changes, expect_change, absolute):
        del changes, absolute
        await expect_change("proj/.pi/skills", lambda: env.rename_file("proj/.pi", "proj/old", context))
        await expect_change(
            "proj/.pi/skills/y.md",
            lambda: env.write_file("proj/.pi/skills/y.md", "y", context),
        )
        await expect_change(
            "proj/.pi/skills/y.md",
            lambda: env.write_file("proj/.pi/skills/y.md", "yy", context),
        )

    await _watching(env, _watch_targets({"path": "proj/.pi/skills", "recursive": True}), run)


async def _watch_excluded(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.create_dir("skills", None, context))

    async def run(changes, expect_change, absolute):
        get_or_throw(await env.write_file("skills/node_modules/dep/SKILL.md", "dep", context))
        get_or_throw(await env.write_file("skills/.SKILL.md.tmp", "draft", context))
        await expect_change(
            "skills/SKILL.md",
            lambda: env.rename_file("skills/.SKILL.md.tmp", "skills/SKILL.md", context),
        )
        hidden = [await absolute("skills/node_modules"), await absolute("skills/.SKILL.md.tmp")]
        for change in changes:
            if "paths" not in change:
                continue
            for path in change["paths"]:
                assert_.ok(
                    not any(path == excluded or path.startswith(excluded) for excluded in hidden),
                    f"excluded {path}",
                )

    await _watching(
        env,
        _watch_targets(
            {
                "path": "skills",
                "recursive": True,
                "exclude": {"hidden": True, "names": ["node_modules"]},
            }
        ),
        run,
    )


async def _watch_overlapping(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    del assert_
    get_or_throw(await env.write_file("skills/a/one.md", "one", context))

    async def run(changes, expect_change, absolute):
        del changes, absolute
        await expect_change("skills/a/two.md", lambda: env.write_file("skills/a/two.md", "two", context))

    await _watching(env, _watch_targets({"path": "skills"}, {"path": "skills", "recursive": True}), run)


async def _watch_replaced_directory(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    del assert_
    get_or_throw(await env.write_file("skills/a/x.md", "x", context))

    async def run(changes, expect_change, absolute):
        del changes, absolute

        async def replace() -> None:
            get_or_throw(await env.rename_file("skills/a", "skills-old", context))
            get_or_throw(await env.create_dir("skills/a", None, context))

        await expect_change("skills/a", replace)
        await expect_change("skills/a/y.md", lambda: env.write_file("skills/a/y.md", "y", context))
        await expect_change("skills/a/y.md", lambda: env.write_file("skills/a/y.md", "yy", context))

    await _watching(env, _watch_targets({"path": "skills", "recursive": True}), run)


async def _watch_closed(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    changes: list[WatchChange] = []
    watcher = get_or_throw(await env.watch(_watch_targets({"path": "file.txt"}), changes.append, context))
    assert_.ok(watcher.mode in ("native", "polling"), "watcher reports its mode")
    await watcher.close(context)
    await watcher.close(context)
    get_or_throw(await env.write_file("file.txt", "x", context))
    await asyncio.sleep(0.3)
    assert_.deep_equal(changes, [])


async def _exec_argv(env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]) -> None:
    hostile = "it's $(touch pwned) `touch pwned` *; touch pwned"
    result, stdout, _ = await _exec_collect(
        env,
        [*shell, 'printf "%s|%s" "$1" "$2"', "argv0", hostile, "a b"],
    )
    assert_.strict_equal(get_or_throw(result)["exitCode"], 0)
    assert_.strict_equal(stdout, f"{hostile}|a b")
    assert_.strict_equal(get_or_throw(await env.exists("pwned", context)), False)


async def _exec_streams(env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]) -> None:
    script = "printf out; printf err >&2; printf more"
    argv_result, argv_stdout, argv_stderr = await _exec_collect(env, [*shell, script])
    assert_.strict_equal(get_or_throw(argv_result)["exitCode"], 0)
    assert_.strict_equal(argv_stdout, "outmore")
    assert_.strict_equal(argv_stderr, "err")
    string_result, string_stdout, string_stderr = await _exec_collect(env, script)
    assert_.strict_equal(get_or_throw(string_result)["exitCode"], 0)
    assert_.strict_equal(string_stdout, "outmore")
    assert_.strict_equal(string_stderr, "err")


async def _exec_cwd(env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]) -> None:
    get_or_throw(await env.create_dir("sub", None, context))
    made, _, _ = await _exec_collect(env, [*shell, "printf x > made.txt; exit 3"], "sub")
    assert_.strict_equal(get_or_throw(made)["exitCode"], 3)
    assert_.strict_equal(get_or_throw(await env.read_text_file("sub/made.txt", context)), "x")


async def _exec_spawn_errors(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    assert_.strict_equal(
        _error_code(await env.exec(["pi-durable-conformance-missing-program"], None, context)),
        "spawn_error",
    )
    assert_.strict_equal(_error_code(await env.exec([], None, context)), "spawn_error")


async def _exec_window(env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]) -> None:
    line_count = 2000
    window = {"max_bytes": 200, "max_lines": 5, "min_interval_ms": 0, "bytes_per_second": 1_000_000_000}
    total_bytes = 0
    total_newlines = 0
    tail = ""

    def on_output(text: str, _context: Context, info: ShellOutputInfo) -> None:
        nonlocal total_bytes, total_newlines, tail
        if info.skipped is not None:
            total_bytes += info.skipped.bytes
            total_newlines += info.skipped.newlines
            after = len(text.encode())
            after_newlines = text.count("\n")
            assert_.ok(
                after > window["max_bytes"] or after_newlines > window["max_lines"],
                "a skip is followed by more than the window",
            )
            tail = ""
        total_bytes += len(text.encode())
        total_newlines += text.count("\n")
        tail += text

    result = await env.exec(
        [*shell, f"i=0; while [ $i -lt {line_count} ]; do echo line-$i; i=$((i+1)); done"],
        _shell_options({"window": window, "on_output": on_output}),
        context,
    )
    assert_.strict_equal(get_or_throw(result)["exitCode"], 0)
    expected = [f"line-{index}\n" for index in range(line_count)]
    assert_.strict_equal(total_bytes, len("".join(expected)))
    assert_.strict_equal(total_newlines, line_count)
    assert_.ok(
        tail.endswith("".join(expected[-window["max_lines"] :])),
        "the delivered output ends with the tail",
    )


async def _exec_timeout(env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]) -> None:
    timed_out = await env.exec([*shell, "sleep 2"], _shell_options({"timeout": 0.1}), context)
    assert_.strict_equal(_error_code(timed_out), "timeout")
    controller = AbortController()
    running = env.exec([*shell, "sleep 2"], None, with_abort_signal(controller.signal, context))
    await asyncio.sleep(0.1)
    controller.abort()
    assert_.strict_equal(_error_code(await running), "aborted")


async def _list_files_order(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    for path in ["b.txt", "a/z.txt", "a/b/c.txt", ".hidden/x", "A.txt"]:
        get_or_throw(await env.write_file(path, "", context))
    get_or_throw(await env.create_dir("empty", None, context))
    entries, truncated = await _listed(env, ".")
    assert_.deep_equal(
        (entries, truncated),
        (
            [
                "d .hidden",
                "f .hidden/x",
                "f A.txt",
                "d a",
                "d a/b",
                "f a/b/c.txt",
                "f a/z.txt",
                "f b.txt",
                "d empty",
            ],
            False,
        ),
    )
    assert_.deep_equal(
        (await _listed(env, ".", {"hidden": False}))[0],
        ["f A.txt", "d a", "d a/b", "f a/b/c.txt", "f a/z.txt", "f b.txt", "d empty"],
    )
    expected = (["d b", "f b/c.txt", "f z.txt"], False)
    assert_.deep_equal(await _listed(env, "a"), expected)
    assert_.deep_equal(await _listed(env, get_or_throw(await env.absolute_path("a", context))), expected)
    assert_.deep_equal(await _listed(env, "empty"), ([], False))


async def _list_files_gitignore(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    for path in ["keep.txt", "out/x.js", "sub/keep.txt", "sub/drop.log", "node_modules/m/i.js"]:
        get_or_throw(await env.write_file(path, "", context))
    get_or_throw(await env.write_file(".gitignore", "out/\n*.log\n", context))
    get_or_throw(await env.write_file("sub/.gitignore", "!keep.txt\n", context))
    assert_.ok("f out/x.js" in (await _listed(env, "."))[0])
    get_or_throw(await env.write_file(".git/HEAD", "ref: refs/heads/main\n", context))
    get_or_throw(await env.write_file(".git/info/exclude", "keep.txt\n", context))
    assert_.deep_equal(
        (await _listed(env, ".", {"exclude": ["node_modules"]}))[0],
        ["f .gitignore", "d sub", "f sub/.gitignore", "f sub/keep.txt"],
    )
    assert_.deep_equal((await _listed(env, "sub"))[0], ["f .gitignore", "f keep.txt"])
    all_entries = (await _listed(env, ".", {"gitignore": False}))[0]
    assert_.ok(
        "f out/x.js" in all_entries and "f node_modules/m/i.js" in all_entries and "f keep.txt" in all_entries
    )
    assert_.ok(not any(".git/" in entry or entry == "d .git" for entry in all_entries))


async def _list_files_bounds(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    for path in ["a", "b", "c"]:
        get_or_throw(await env.write_file(path, "", context))
    assert_.deep_equal(await _listed(env, ".", {"max_entries": 2}), (["f a", "f b"], True))
    assert_.deep_equal(await _listed(env, ".", {"max_entries": 3}), (["f a", "f b", "f c"], False))
    assert_.deep_equal(await _listed(env, ".", {"max_bytes": 6}), (["f a", "f b"], True))
    assert_.deep_equal(await _listed(env, ".", {"max_entries": 0}), ([], True))
    assert_.strict_equal(
        _error_code(await env.list_files(".", _list_options({"max_entries": -1}), context)),
        "invalid",
    )
    assert_.strict_equal(
        _error_code(await env.list_files(".", _list_options({"timeout_ms": 1.5}), context)),
        "invalid",
    )


async def _list_files_errors(env: ExecutionEnv, assert_: EnvConformanceAssertions) -> None:
    get_or_throw(await env.write_file("file.txt", "", context))
    assert_.strict_equal(_error_code(await env.list_files("missing", None, context)), "not_found")
    assert_.strict_equal(_error_code(await env.list_files("file.txt", None, context)), "not_directory")
    assert_.strict_equal(_error_code(await env.list_files(".", None, _aborted_context())), "aborted")


async def _binary_reader_symlinks(
    env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]
) -> None:
    get_or_throw(await env.write_file("target.txt", "target", context))
    get_or_throw(await env.create_dir("sub", None, context))
    get_or_throw(await env.write_file("sub/inner.txt", "inner", context))
    linked = await env.exec([*shell, "ln -s target.txt link.txt && ln -s sub dirlink"], None, context)
    assert_.strict_equal(get_or_throw(linked)["exitCode"], 0)

    followed = get_or_throw(await env.open_binary_reader("link.txt", None, context))
    assert_.strict_equal(get_or_throw(await followed.read(0, 10, context)).decode(), "target")
    await followed.close(context)
    assert_.strict_equal(
        _error_code(await env.open_binary_reader("link.txt", {"no_follow": True}, context)),
        "invalid",
    )
    inner = get_or_throw(await env.open_binary_reader("dirlink/inner.txt", {"no_follow": True}, context))
    assert_.strict_equal(get_or_throw(await inner.read(0, 10, context)).decode(), "inner")
    await inner.close(context)


async def _list_files_symlinks(
    env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]
) -> None:
    get_or_throw(await env.write_file("dir/inner.txt", "", context))
    linked = await env.exec(
        [*shell, "ln -s dir link && ln -s missing dangling && ln -s .. dir/loop"], None, context
    )
    assert_.strict_equal(get_or_throw(linked)["exitCode"], 0)
    assert_.deep_equal(
        (await _listed(env, "."))[0],
        ["l dangling", "d dir", "f dir/inner.txt", "l dir/loop", "l link"],
    )
    assert_.deep_equal((await _listed(env, "link"))[0], ["f inner.txt", "l loop"])


async def _watch_symlink_target(
    env: ExecutionEnv, assert_: EnvConformanceAssertions, shell: Sequence[str]
) -> None:
    get_or_throw(await env.write_file("data/real.md", "one", context))
    get_or_throw(await env.create_dir("config", None, context))
    linked = await env.exec([*shell, "ln -s ../data/real.md config/AGENTS.md"], None, context)
    assert_.strict_equal(get_or_throw(linked)["exitCode"], 0)

    async def run(changes, expect_change, absolute):
        del changes, absolute
        await expect_change("config/AGENTS.md", lambda: env.write_file("data/real.md", "two!", context))

    await _watching(env, _watch_targets({"path": "config/AGENTS.md"}), run)


__all__ = ["create_env_conformance"]

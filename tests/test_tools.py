"""Translated upstream ``tools`` cases for the coding tools.

The PowerShell suite is excluded with the PowerShell tool. Image cases that need a real decoder are
replaced by a test-double :class:`ImageProcessor`; the no-processor cases match upstream.
"""

from __future__ import annotations

import asyncio
import base64
import os
import struct
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import pytest

from pi_durable.context import (
    BACKGROUND_CONTEXT,
    AbortController,
    Context,
    with_abort_signal,
)
from pi_durable.env import (
    Err,
    ExecutionError,
    FileError,
    Ok,
    Result,
    ShellExecOptions,
    ShellExecResult,
    ShellOutputInfo,
    ShellOutputSkip,
    ShellOutputWindow,
    get_or_throw,
)
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.tools import (
    CodingTools,
    create_bash_tool,
    create_coding_tools,
    create_edit_tool,
    create_read_tool,
    create_write_tool,
)
from pi_durable.tools.file_mutation_queue import with_file_mutation_queue
from pi_durable.tools.image import detect_supported_image_mime_type
from pi_durable.truncate import DEFAULT_MAX_LINES

# ── Support ────────────────────────────────────────────────────────────────


class FakeApi:
    """A minimal execution API: the environment, collected output and diagnostics, nothing durable."""

    def __init__(self, env: Any) -> None:
        self.task_id = 1
        self.conversation_id = 1
        self.call_id = "call"
        self.env = env
        self.output_window: Mapping[str, Any] | None = None
        self.output_parts: list[tuple[str, Any]] = []
        self.diagnostics: list[dict[str, Any]] = []
        self.models: Any = None
        self.details_updates: list[Any] = []

    def output(self, chunk: str | bytes, skipped: Any = None) -> None:
        self.output_parts.append((chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk, skipped))

    def retained_output(self) -> Mapping[str, Any]:
        return {
            "text": "".join(chunk for chunk, _ in self.output_parts),
            "truncated": False,
        }

    def diagnostic(self, diagnostic: Mapping[str, Any]) -> None:
        self.diagnostics.append(dict(diagnostic))

    async def details(self, value: Any, context: Context) -> None:
        self.details_updates.append(value)

    async def agent(self, context: Context) -> Any:
        return {}


def fake_api(env: Any) -> tuple[FakeApi, Any]:
    api = FakeApi(env)
    return api, cast(Any, api)


async def run(
    tool: Any,
    args: Mapping[str, Any],
    env: Any,
    context: Context = BACKGROUND_CONTEXT,
) -> dict[str, Any]:
    api, _ = fake_api(env)
    result = tool.execute(args, api, context)
    if hasattr(result, "__await__"):
        result = await result
    values = dict(result or {})
    values["streamed"] = [chunk for chunk, _ in api.output_parts]
    values["reported"] = api.diagnostics
    return values


async def run_failing(
    tool: Any, args: Mapping[str, Any], env: Any
) -> tuple[BaseException, list[str], list[dict[str, Any]]]:
    api, _ = fake_api(env)
    try:
        result = tool.execute(args, api, BACKGROUND_CONTEXT)
        if hasattr(result, "__await__"):
            await result
    except BaseException as error:  # noqa: BLE001 - the test asserts the thrown value
        return error, [chunk for chunk, _ in api.output_parts], api.diagnostics
    raise AssertionError("Expected the tool to throw")


def text_output(result: Mapping[str, Any]) -> str:
    return "\n".join(
        cast(str, part["text"])
        for part in cast(Sequence[Mapping[str, Any]], result.get("output") or ())
        if part.get("type") == "text"
    )


def diagnostic_text(result: Mapping[str, Any]) -> str:
    return "\n".join(
        cast(str, diagnostic.get("message"))
        for diagnostic in cast(Sequence[Mapping[str, Any]], result.get("diagnostics") or ())
    )


def create_env(path: Path) -> PosixExecutionEnv:
    return PosixExecutionEnv(cwd=str(path))


class SlowReadEnvironment(PosixExecutionEnv):
    async def read_text_file(self, path: str, context: Context) -> Result[str, FileError]:
        await asyncio.sleep(0.02)
        return await super().read_text_file(path, context)


class BlockingWriteEnvironment(PosixExecutionEnv):
    def __init__(self, *, cwd: str, env_id: str | None = None) -> None:
        super().__init__(cwd=cwd)
        if env_id is not None:
            self.id = env_id
        self.first_write_started = asyncio.Event()
        self.finish_first_write = asyncio.Event()
        self.second_write_started = False

    async def write_file(self, path: str, content: str | bytes, context: Context) -> Result[None, FileError]:
        if content == "first\n":
            self.first_write_started.set()
            await self.finish_first_write.wait()
        elif content == "second\n":
            self.second_write_started = True
        return await super().write_file(path, content, context)


class BlockingEditEnvironment(PosixExecutionEnv):
    def __init__(self, *, cwd: str) -> None:
        super().__init__(cwd=cwd)
        self.first_edit_write_started = asyncio.Event()
        self.finish_first_edit_write = asyncio.Event()
        self.first_edit_write_settled = False
        self.second_edit_write_started = False

    async def write_file(self, path: str, content: str | bytes, context: Context) -> Result[None, FileError]:
        if content == "ALPHA\nbeta\n":
            self.first_edit_write_started.set()
            await self.finish_first_edit_write.wait()
            # The write itself must settle even though the call was aborted.
            result = await super().write_file(path, content, BACKGROUND_CONTEXT)
            self.first_edit_write_settled = True
            return result
        if content in ("ALPHA\nBETA\n", "alpha\nBETA\n"):
            self.second_edit_write_started = True
        return await super().write_file(path, content, context)


TRUNCATED_OUTPUT_LINES = DEFAULT_MAX_LINES + 1


class TimeoutOutputEnvironment(PosixExecutionEnv):
    async def exec(
        self,
        command: str | Sequence[str],
        options: ShellExecOptions | None,
        context: Context,
    ) -> Result[ShellExecResult, ExecutionError]:
        del command
        output = "".join(f"line-{index + 1}\n" for index in range(TRUNCATED_OUTPUT_LINES))
        spill_path = get_or_throw(
            await self.create_temp_file({"prefix": "timeout-", "suffix": ".log"}, context)
        )
        get_or_throw(await self.write_file(spill_path, output, context))
        values = dict(options or {})
        on_output = values.get("on_output")
        if on_output is not None:
            on_output(output, context, ShellOutputInfo(stream="stdout"))
        error = ExecutionError("timeout", f"timeout:{values.get('timeout')}")
        error.spill_path = spill_path
        return Err(error)


# ── Cases ──────────────────────────────────────────────────────────────────


async def test_fail_with_an_ordinary_error_when_no_environment_is_configured() -> None:
    with pytest.raises(RuntimeError, match="No execution environment"):
        await run(create_read_tool(), {"path": "x"}, None)


@pytest.mark.parametrize("signature", ["GIF87a", "GIF89a"])
def test_detects_the_complete_gif_signature(signature: str) -> None:
    assert detect_supported_image_mime_type(signature.encode("ascii")) == "image/gif"


# ── read ───────────────────────────────────────────────────────────────────


async def test_reads_text_with_offsets_and_limits_and_reports_continuation(
    tmp_path: Path,
) -> None:
    env = create_env(tmp_path)
    get_or_throw(
        await env.write_file(
            "test.txt",
            "\n".join(f"Line {index + 1}" for index in range(100)),
            BACKGROUND_CONTEXT,
        )
    )
    result = await run(create_read_tool(), {"path": "test.txt", "offset": 41, "limit": 20}, env)
    output = text_output(result)
    assert "Line 40" not in output
    assert "Line 41" in output
    assert "Line 60" in output
    assert "Line 61" not in output
    assert "more lines" not in output
    assert diagnostic_text(result) == "40 more lines in file. Use offset=61 to continue."


async def test_truncates_large_text_by_line_count(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(
        await env.write_file(
            "large.txt",
            "\n".join(f"Line {index + 1}" for index in range(2500)),
            BACKGROUND_CONTEXT,
        )
    )
    result = await run(create_read_tool(), {"path": "large.txt"}, env)
    assert diagnostic_text(result) == ("Showing lines 1-2000 of 2500. Use offset=2001 to continue.")
    assert cast(Sequence[Mapping[str, Any]], result["diagnostics"])[0]["code"] == "truncated"
    truncation = cast(Mapping[str, Any], result["details"])["truncation"]
    assert {key: truncation[key] for key in ("truncated", "truncatedBy", "totalLines", "outputLines")} == {
        "truncated": True,
        "truncatedBy": "lines",
        "totalLines": 2500,
        "outputLines": 2000,
    }


async def test_does_not_count_a_trailing_newline_as_an_extra_line_at_the_limit(
    tmp_path: Path,
) -> None:
    env = create_env(tmp_path)
    get_or_throw(
        await env.write_file("exact.txt", "\n".join("x" for _ in range(2000)) + "\n", BACKGROUND_CONTEXT)
    )
    result = await run(create_read_tool(), {"path": "exact.txt"}, env)
    assert result.get("details") is None
    assert result["diagnostics"] == []


async def test_shows_the_start_of_a_line_longer_than_the_byte_limit(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("long.txt", "é" * 40_000 + "\nnext\n", BACKGROUND_CONTEXT))
    result = await run(create_read_tool(), {"path": "long.txt"}, env)
    assert text_output(result) == "é" * 25_600
    assert diagnostic_text(result) == (
        "Line 1 is 78.1KB, exceeds the 50.0KB limit; showing its first 50.0KB. "
        "Use bash: sed -n '1p' long.txt | tail -c +51201"
    )
    truncation = cast(Mapping[str, Any], result["details"])["truncation"]
    assert {
        key: truncation[key] for key in ("truncated", "firstLineExceedsLimit", "outputBytes", "outputLines")
    } == {
        "truncated": True,
        "firstLineExceedsLimit": True,
        "outputBytes": 51_200,
        "outputLines": 1,
    }
    assert "content" not in truncation


async def test_rejects_offsets_beyond_the_file(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("short.txt", "one\ntwo\nthree", BACKGROUND_CONTEXT))
    with pytest.raises(RuntimeError, match=r"Offset 100 is beyond end of file \(3 lines total\)"):
        await run(create_read_tool(), {"path": "short.txt", "offset": 100}, env)


async def test_reads_a_log_that_grows_while_it_is_read(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("app.log", "one\ntwo\n", BACKGROUND_CONTEXT))
    log_path = tmp_path / "app.log"
    original_open = env.open_binary_reader

    async def open_binary_reader(
        path: str, options: dict[str, Any] | None, context: Context
    ) -> Result[Any, FileError]:
        opened = await original_open(path, options, context)
        if not opened.ok:
            return opened
        reader = opened.value
        original_read = reader.read

        async def read(position: int, length: int, read_context: Context) -> Result[bytes, FileError]:
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write("more\n")
            return await original_read(position, length, read_context)

        reader.read = read  # type: ignore[method-assign, assignment]
        return opened

    env.open_binary_reader = open_binary_reader  # type: ignore[method-assign]
    result = await run(create_read_tool(), {"path": "app.log", "limit": 2}, env)
    assert text_output(result) == "one\ntwo"


async def test_returns_images_by_content_as_one_image_block(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGD4DwABBAEAX+XDSwAAAABJRU5ErkJggg=="
    get_or_throw(await env.write_file("image.txt", base64.b64decode(data), BACKGROUND_CONTEXT))
    result = await run(create_read_tool(), {"path": "image.txt"}, env)
    assert result["output"] == [{"type": "image", "data": data, "mimeType": "image/png"}]
    assert result.get("isError") is None
    assert diagnostic_text(result) == "Read image file [image/png]."


def _png_header(width: int, height: int) -> bytes:
    return (
        b"\x89PNG\r\n\x1a\n"
        + struct.pack(">I", 13)
        + b"IHDR"
        + struct.pack(">II", width, height)
        + b"\x08\x02\x00\x00\x00"
    )


def _bmp(width: int = 4, height: int = 4) -> bytes:
    row = ((width * 3 + 3) // 4) * 4
    size = 54 + row * height
    header = bytearray(size)
    header[0:2] = b"BM"
    struct.pack_into("<I", header, 2, size)
    struct.pack_into("<I", header, 10, 54)
    struct.pack_into("<I", header, 14, 40)
    struct.pack_into("<i", header, 18, width)
    struct.pack_into("<i", header, 22, height)
    struct.pack_into("<H", header, 26, 1)
    struct.pack_into("<H", header, 28, 24)
    return bytes(header)


async def test_refuses_images_outside_the_limits_or_needing_conversion(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("wide.png", _png_header(2001, 2), BACKGROUND_CONTEXT))
    wide = await run(create_read_tool(), {"path": "wide.png"}, env)
    assert wide["output"] == [] and wide["isError"] is True
    assert diagnostic_text(wide) == (
        "wide.png is an image (image/png) of 2001x2, larger than 2000x2000, "
        "and no image processor is configured to shrink it"
    )

    get_or_throw(await env.write_file("a.bmp", _bmp(), BACKGROUND_CONTEXT))
    bmp = await run(create_read_tool(), {"path": "a.bmp"}, env)
    assert bmp["output"] == [] and bmp["isError"] is True
    assert diagnostic_text(bmp) == (
        "a.bmp is an image (image/bmp) that needs converting, and no image processor is configured"
    )


async def test_uses_an_image_processor_and_the_models_image_limits(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    png = _png_header(8, 6)
    get_or_throw(await env.write_file("a.png", png, BACKGROUND_CONTEXT))

    class Processor:
        def __init__(self) -> None:
            self.calls: list[tuple[int, str, Mapping[str, Any]]] = []

        async def prepare(self, data: bytes, mime_type: str, limits: Mapping[str, Any]) -> dict[str, Any]:
            self.calls.append((len(data), mime_type, limits))
            return {
                "data": "cHJlcGFyZWQ=",
                "mime_type": mime_type,
                "resized": {"from": {"width": 8, "height": 6}, "to": {"width": 4, "height": 3}},
            }

    processor = Processor()
    tool = create_read_tool({"images": processor})
    assert "bmp" in tool.description
    result = await run(tool, {"path": "a.png"}, env)
    assert result["output"] == [{"type": "image", "data": "cHJlcGFyZWQ=", "mimeType": "image/png"}]
    assert diagnostic_text(result) == (
        "Read image file [image/png]. Resized from 8x6 to 4x3. Multiply coordinates by 2.00 "
        "to map them to the original."
    )
    assert processor.calls == [
        (
            len(png),
            "image/png",
            {"max_width": 2000, "max_height": 2000, "max_bytes": 4_718_592},
        )
    ]


class _Models:
    def __init__(self, model: Any) -> None:
        self._model = model

    def get_model(self, provider: str, model_id: str) -> Any:
        del provider, model_id
        return self._model


class _ImageApi(FakeApi):
    def __init__(self, env: Any, model: Any) -> None:
        super().__init__(env)
        self.models = _Models(model)

    async def agent(self, context: Context) -> Any:
        del context
        return {"model": {"provider": "test", "modelId": "test"}}


async def test_uses_the_models_image_input_and_limits(tmp_path: Path) -> None:
    from pi_durable.providers import Model

    env = create_env(tmp_path)
    get_or_throw(await env.write_file("wide.png", _png_header(8, 6), BACKGROUND_CONTEXT))
    model = Model(
        provider="test",
        model_id="test",
        input_limits={"images": {"resize": {"max_width": 4, "max_height": 4}}},
    )
    api = _ImageApi(env, model)
    tool = create_read_tool()
    assert tool.execute is not None
    refused = await tool.execute({"path": "wide.png"}, api, BACKGROUND_CONTEXT)
    assert refused["output"] == [] and refused["isError"] is True
    assert diagnostic_text(refused) == (
        "wide.png is an image (image/png) of 8x6, larger than 4x4, "
        "and no image processor is configured to shrink it"
    )

    get_or_throw(await env.write_file("small.png", _png_header(4, 4), BACKGROUND_CONTEXT))
    text_only = _ImageApi(env, Model(provider="test", model_id="test", input=("text",)))
    shown = await text_only_agent_read(tool, text_only)
    assert shown["output"][0]["type"] == "image"
    assert diagnostic_text(shown) == (
        "Read image file [image/png]. The current model does not support images; it sees a "
        "placeholder instead."
    )


async def text_only_agent_read(tool: Any, api: FakeApi) -> dict[str, Any]:
    result = tool.execute({"path": "small.png"}, api, BACKGROUND_CONTEXT)
    if hasattr(result, "__await__"):
        result = await result
    return dict(result)


# ── write ──────────────────────────────────────────────────────────────────


async def test_writes_files_and_creates_parent_directories(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    result = await run(create_write_tool(), {"path": "nested/dir/file.txt", "content": "hello"}, env)
    assert text_output(result) == "Successfully wrote to nested/dir/file.txt"
    assert get_or_throw(await env.read_text_file("nested/dir/file.txt", BACKGROUND_CONTEXT)) == "hello"


async def test_keeps_the_mutation_queue_locked_until_an_aborted_write_settles(
    tmp_path: Path,
) -> None:
    env = BlockingWriteEnvironment(cwd=str(tmp_path))
    tool = create_write_tool()
    controller = AbortController()
    first = asyncio.create_task(
        run(
            tool,
            {"path": "file.txt", "content": "first\n"},
            env,
            with_abort_signal(controller.signal, BACKGROUND_CONTEXT),
        )
    )
    await env.first_write_started.wait()
    controller.abort()
    second = asyncio.create_task(run(tool, {"path": "file.txt", "content": "second\n"}, env))
    await asyncio.sleep(0.02)
    assert env.second_write_started is False
    env.finish_first_write.set()
    with pytest.raises((RuntimeError, FileError)):
        await first
    await second
    assert get_or_throw(await env.read_text_file("file.txt", BACKGROUND_CONTEXT)) == "second\n"


async def test_a_cancelled_waiter_does_not_deadlock_later_mutations(tmp_path: Path) -> None:
    env = BlockingWriteEnvironment(cwd=str(tmp_path))
    tool = create_write_tool()
    first = asyncio.create_task(run(tool, {"path": "file.txt", "content": "first\n"}, env))
    await env.first_write_started.wait()
    waiter = asyncio.create_task(run(tool, {"path": "file.txt", "content": "second\n"}, env))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    third = asyncio.create_task(run(tool, {"path": "file.txt", "content": "second\n"}, env))
    await asyncio.sleep(0.02)
    assert env.second_write_started is False
    env.finish_first_write.set()
    await first
    await third
    assert env.second_write_started is True
    assert get_or_throw(await env.read_text_file("file.txt", BACKGROUND_CONTEXT)) == "second\n"


# ── edit ───────────────────────────────────────────────────────────────────


async def test_applies_disjoint_edits_and_returns_both_diff_formats(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    original = "alpha\nbeta\ngamma\ndelta\n"
    get_or_throw(await env.write_file("edit.txt", original, BACKGROUND_CONTEXT))
    result = await run(
        create_edit_tool(),
        {
            "path": "edit.txt",
            "edits": [
                {"oldText": "alpha\n", "newText": "ALPHA\n"},
                {"oldText": "gamma\n", "newText": "GAMMA\n"},
            ],
        },
        env,
    )
    details = cast(Mapping[str, Any], result["details"])
    assert text_output(result) == "Successfully replaced 2 block(s) in edit.txt."
    assert "ALPHA" in details["diff"]
    assert "GAMMA" in details["diff"]
    assert details["patch"] == (
        "--- edit.txt\n+++ edit.txt\n@@ -1,4 +1,4 @@\n-alpha\n+ALPHA\n beta\n-gamma\n+GAMMA\n delta\n"
    )
    assert get_or_throw(await env.read_text_file("edit.txt", BACKGROUND_CONTEXT)) == (
        "ALPHA\nbeta\nGAMMA\ndelta\n"
    )


def test_repairs_edit_argument_shapes_without_mutating_them() -> None:
    prepare = create_edit_tool().prepare_arguments
    assert prepare is not None
    edit = {"oldText": "a", "newText": "b"}
    encoded = '[{"oldText": "a", "newText": "b"}]'
    as_string = {"path": "f", "edits": encoded}
    assert prepare(as_string) == {"path": "f", "edits": [edit]}
    assert as_string["edits"] == encoded
    assert prepare({"path": "f", "edits": '{"oldText": "a", "newText": "b"}'}) == {
        "path": "f",
        "edits": [edit],
    }
    assert prepare({"path": "f", "edits": edit}) == {"path": "f", "edits": [edit]}
    assert prepare({"path": "f", "edits": [edit], "oldText": "c", "newText": "d"}) == {
        "path": "f",
        "edits": [edit, {"oldText": "c", "newText": "d"}],
    }
    assert prepare({"path": "f", "edits": "not json"}) == {"path": "f", "edits": "not json"}


async def test_matches_all_edits_against_the_original_and_rejects_overlaps(
    tmp_path: Path,
) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("edit.txt", "one\ntwo\nthree\n", BACKGROUND_CONTEXT))
    with pytest.raises(ValueError, match="overlap"):
        await run(
            create_edit_tool(),
            {
                "path": "edit.txt",
                "edits": [
                    {"oldText": "one\ntwo\n", "newText": "ONE\nTWO\n"},
                    {"oldText": "two\nthree\n", "newText": "TWO\nTHREE\n"},
                ],
            },
            env,
        )
    assert get_or_throw(await env.read_text_file("edit.txt", BACKGROUND_CONTEXT)) == ("one\ntwo\nthree\n")


async def test_rejects_missing_and_duplicate_target_text(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("edit.txt", "foo foo foo", BACKGROUND_CONTEXT))
    tool = create_edit_tool()
    with pytest.raises(ValueError, match="Could not find the exact text"):
        await run(tool, {"path": "edit.txt", "edits": [{"oldText": "bar", "newText": "baz"}]}, env)
    with pytest.raises(ValueError, match="Found 3 occurrences"):
        await run(tool, {"path": "edit.txt", "edits": [{"oldText": "foo", "newText": "bar"}]}, env)


async def test_keeps_the_mutation_queue_locked_until_an_aborted_edit_write_settles(
    tmp_path: Path,
) -> None:
    env = BlockingEditEnvironment(cwd=str(tmp_path))
    get_or_throw(await env.write_file("file.txt", "alpha\nbeta\n", BACKGROUND_CONTEXT))
    tool = create_edit_tool()
    controller = AbortController()
    first = asyncio.create_task(
        run(
            tool,
            {"path": "file.txt", "edits": [{"oldText": "alpha", "newText": "ALPHA"}]},
            env,
            with_abort_signal(controller.signal, BACKGROUND_CONTEXT),
        )
    )
    await env.first_edit_write_started.wait()
    controller.abort()
    second = asyncio.create_task(
        run(
            tool,
            {"path": "file.txt", "edits": [{"oldText": "beta", "newText": "BETA"}]},
            env,
        )
    )
    await asyncio.sleep(0.02)
    assert env.second_edit_write_started is False
    env.finish_first_edit_write.set()
    with pytest.raises(RuntimeError, match="Operation aborted"):
        await first
    await second
    assert env.first_edit_write_settled is True
    assert get_or_throw(await env.read_text_file("file.txt", BACKGROUND_CONTEXT)) == ("ALPHA\nBETA\n")


async def test_serializes_concurrent_edits_through_canonical_and_symlink_paths(
    tmp_path: Path,
) -> None:
    env = SlowReadEnvironment(cwd=str(tmp_path))
    get_or_throw(await env.write_file("target.txt", "alpha\nbeta\ngamma\n", BACKGROUND_CONTEXT))
    os.symlink("target.txt", str(tmp_path / "link.txt"))
    tool = create_edit_tool()
    await asyncio.gather(
        run(tool, {"path": "target.txt", "edits": [{"oldText": "alpha", "newText": "ALPHA"}]}, env),
        run(tool, {"path": "link.txt", "edits": [{"oldText": "beta", "newText": "BETA"}]}, env),
    )
    assert get_or_throw(await env.read_text_file("target.txt", BACKGROUND_CONTEXT)) == (
        "ALPHA\nBETA\ngamma\n"
    )


async def test_serializes_edits_of_one_file_across_environment_objects(tmp_path: Path) -> None:
    first = SlowReadEnvironment(cwd=str(tmp_path))
    second = SlowReadEnvironment(cwd=str(tmp_path))
    get_or_throw(await first.write_file("file.txt", "alpha\nbeta\n", BACKGROUND_CONTEXT))
    tool = create_edit_tool()
    await asyncio.gather(
        run(tool, {"path": "file.txt", "edits": [{"oldText": "alpha", "newText": "ALPHA"}]}, first),
        run(tool, {"path": "file.txt", "edits": [{"oldText": "beta", "newText": "BETA"}]}, second),
    )
    assert get_or_throw(await first.read_text_file("file.txt", BACKGROUND_CONTEXT)) == ("ALPHA\nBETA\n")


async def test_serializes_a_new_file_created_through_a_symlinked_directory(
    tmp_path: Path,
) -> None:
    env = BlockingWriteEnvironment(cwd=str(tmp_path))
    (tmp_path / "real").mkdir()
    os.symlink(str(tmp_path / "real"), str(tmp_path / "link"))
    tool = create_write_tool()
    first = asyncio.create_task(run(tool, {"path": "link/new.txt", "content": "first\n"}, env))
    await env.first_write_started.wait()
    second = asyncio.create_task(run(tool, {"path": "real/new.txt", "content": "second\n"}, env))
    await asyncio.sleep(0.02)
    assert env.second_write_started is False
    env.finish_first_write.set()
    await asyncio.gather(first, second)
    assert get_or_throw(await env.read_text_file("real/new.txt", BACKGROUND_CONTEXT)) == "second\n"


async def test_keys_a_missing_file_whose_name_contains_a_backslash_like_the_created_file(
    tmp_path: Path,
) -> None:
    env = create_env(tmp_path)
    created = asyncio.Event()
    release = asyncio.Event()

    async def first() -> None:
        async def mutate() -> None:
            get_or_throw(await env.write_file("a\\b.txt", "first\n", BACKGROUND_CONTEXT))
            created.set()
            await release.wait()

        await with_file_mutation_queue(env, "a\\b.txt", mutate, BACKGROUND_CONTEXT)

    task = asyncio.create_task(first())
    await created.wait()
    entered = False

    async def second() -> None:
        nonlocal entered

        async def mutate() -> None:
            nonlocal entered
            entered = True

        await with_file_mutation_queue(env, "a\\b.txt", mutate, BACKGROUND_CONTEXT)

    second_task = asyncio.create_task(second())
    await asyncio.sleep(0.02)
    assert entered is False
    release.set()
    await asyncio.gather(task, second_task)
    assert entered is True


async def test_does_not_serialize_the_same_path_on_different_file_systems(
    tmp_path: Path,
) -> None:
    local = BlockingWriteEnvironment(cwd=str(tmp_path))
    other = BlockingWriteEnvironment(cwd=str(tmp_path), env_id="other")
    tool = create_write_tool()
    blocked = asyncio.create_task(run(tool, {"path": "file.txt", "content": "first\n"}, local))
    await local.first_write_started.wait()
    await run(tool, {"path": "file.txt", "content": "second\n"}, other)
    assert other.second_write_started is True
    local.finish_first_write.set()
    await blocked


async def test_edits_regular_files_through_symlinks(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("target.txt", "before\n", BACKGROUND_CONTEXT))
    os.symlink("target.txt", str(tmp_path / "link.txt"))
    await run(
        create_edit_tool(),
        {"path": "link.txt", "edits": [{"oldText": "before", "newText": "after"}]},
        env,
    )
    assert get_or_throw(await env.read_text_file("target.txt", BACKGROUND_CONTEXT)) == "after\n"


async def test_preserves_bom_and_crlf_line_endings(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("edit.txt", "\ufeffone\r\ntwo\r\n", BACKGROUND_CONTEXT))
    await run(
        create_edit_tool(),
        {"path": "edit.txt", "edits": [{"oldText": "two", "newText": "TWO"}]},
        env,
    )
    assert get_or_throw(await env.read_text_file("edit.txt", BACKGROUND_CONTEXT)) == ("\ufeffone\r\nTWO\r\n")


async def test_reports_a_patch_for_a_file_without_a_trailing_newline(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    get_or_throw(await env.write_file("edit.txt", "a\nb", BACKGROUND_CONTEXT))
    result = await run(
        create_edit_tool(),
        {"path": "edit.txt", "edits": [{"oldText": "b", "newText": "B"}]},
        env,
    )
    patch = cast(Mapping[str, Any], result["details"])["patch"]
    assert patch.count("\\ No newline at end of file") == 2
    assert get_or_throw(await env.read_text_file("edit.txt", BACKGROUND_CONTEXT)) == "a\nB"


async def test_refuses_paths_that_escape_the_working_directory(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    for tool, args in (
        (create_read_tool(), {"path": "../outside.txt"}),
        (create_write_tool(), {"path": "../outside.txt", "content": "x"}),
        (
            create_edit_tool(),
            {"path": "../outside.txt", "edits": [{"oldText": "secret", "newText": "x"}]},
        ),
    ):
        with pytest.raises(ValueError, match="outside the working directory"):
            await run(tool, args, env)
    with pytest.raises(ValueError, match="outside the working directory"):
        await run(create_read_tool(), {"path": str(outside)}, env)
    assert outside.read_text(encoding="utf-8") == "secret"


# ── bash ───────────────────────────────────────────────────────────────────


async def test_passes_the_retained_window_and_forwards_what_it_skipped(
    tmp_path: Path,
) -> None:
    window = cast(
        ShellOutputWindow,
        {
            "max_bytes": 4,
            "max_lines": 1,
            "min_interval_ms": 100,
            "bytes_per_second": 1024,
        },
    )
    skipped = ShellOutputSkip(bytes=6, newlines=2, ends_with_newline=True)
    received: list[ShellOutputWindow | None] = []

    class SkippingEnvironment(PosixExecutionEnv):
        async def exec(
            self,
            command: str | Sequence[str],
            options: ShellExecOptions | None,
            context: Context,
        ) -> Result[ShellExecResult, ExecutionError]:
            del command
            values = dict(options or {})
            received.append(cast(Any, values.get("window")))
            on_output = values.get("on_output")
            if on_output is not None:
                on_output(
                    "tail\n",
                    context,
                    ShellOutputInfo(stream="stdout", skipped=skipped),
                )
            return Ok(cast(ShellExecResult, {"exitCode": 0}))

    api = FakeApi(SkippingEnvironment(cwd=str(tmp_path)))
    api.output_window = window
    tool = create_bash_tool()
    assert tool.execute is not None
    await tool.execute({"command": "anything"}, api, BACKGROUND_CONTEXT)
    assert received == [window]
    assert api.output_parts == [("tail\n", skipped)]


async def test_streams_combined_output_and_repeats_it_with_the_exit_code(
    tmp_path: Path,
) -> None:
    result = await run(create_bash_tool(), {"command": "printf out; printf err >&2"}, create_env(tmp_path))
    assert "out" in "".join(result["streamed"])
    assert "err" in "".join(result["streamed"])
    assert result.get("output") is None
    assert result["structuredOutput"] == {
        "output": "".join(result["streamed"]),
        "truncated": False,
        "exitCode": 0,
    }


async def test_answers_a_nonzero_exit_and_throws_on_timeout(tmp_path: Path) -> None:
    env = create_env(tmp_path)
    tool = create_bash_tool()
    failed = await run(tool, {"command": "printf failed; exit 7"}, env)
    assert failed["isError"] is True
    assert failed["structuredOutput"] == {
        "output": "failed",
        "truncated": False,
        "exitCode": 7,
    }
    assert failed["diagnostics"] == [
        {
            "severity": "error",
            "code": "exit_code",
            "message": "Command exited with code 7",
        }
    ]
    assert "".join(failed["streamed"]) == "failed"
    error, _, _ = await run_failing(tool, {"command": "sleep 2", "timeout": 0.01}, env)
    assert str(error) == "Command timed out after 0.01 seconds"


async def test_reports_the_spill_of_a_command_that_times_out(tmp_path: Path) -> None:
    env = TimeoutOutputEnvironment(cwd=str(tmp_path))
    error, _, reported = await run_failing(
        create_bash_tool(), {"command": "emit-output-then-time-out", "timeout": 0.05}, env
    )
    assert str(error) == "Command timed out after 0.05 seconds"
    message = cast(str, reported[0]["message"])
    assert message.startswith("Full output: ")
    full_output = get_or_throw(
        await env.read_text_file(message.removeprefix("Full output: "), BACKGROUND_CONTEXT)
    )
    assert "line-1\nline-2" in full_output
    assert f"line-{DEFAULT_MAX_LINES}\nline-{TRUNCATED_OUTPUT_LINES}" in full_output


async def test_prepares_command_cwd_and_an_explicit_environment_with_the_calls_api(
    tmp_path: Path,
) -> None:
    env = PosixExecutionEnv(cwd=str(tmp_path), shell_env={"PI_BASH_PREPARE_INHERITED": "inherited"})
    get_or_throw(await env.create_dir("workspace", None, BACKGROUND_CONTEXT))
    workspace = str(tmp_path / "workspace")
    controller = AbortController()
    received: dict[str, Any] = {}

    async def prepare(execution: Any, api: Any, call_context: Context) -> None:
        received["env"] = api.env
        received["signal"] = call_context.abort_signal
        execution.cwd = workspace
        execution.env = {"PI_BASH_PREPARE_EXPLICIT": "explicit"}
        execution.inherit_env = False
        execution.command += (
            "\n: > prepared-cwd\nprintf '%s:%s:%s' \"$prefix\" "
            '"${PI_BASH_PREPARE_INHERITED-}" "$PI_BASH_PREPARE_EXPLICIT"'
        )
        execution.command += "\nprintf ':%s' \"$PWD\""

    tool = create_bash_tool(command_prefix="prefix=ready", prepare=prepare)
    result = await run(
        tool,
        {"command": ":"},
        env,
        with_abort_signal(controller.signal, BACKGROUND_CONTEXT),
    )
    assert received["env"] is env
    assert received["signal"] is controller.signal
    canonical_workspace = get_or_throw(await env.canonical_path(workspace, BACKGROUND_CONTEXT))
    assert "".join(result["streamed"]) == f"ready::explicit:{canonical_workspace}"
    assert get_or_throw(await env.exists(f"{workspace}/prepared-cwd", BACKGROUND_CONTEXT)) is True


async def test_supports_command_prefixes(tmp_path: Path) -> None:
    result = await run(
        create_bash_tool({"command_prefix": "value=hello"}),
        {"command": "printf $value"},
        create_env(tmp_path),
    )
    assert "".join(result["streamed"]) == "hello"


async def test_streams_every_byte_and_spills_complete_output_beyond_the_limits(
    tmp_path: Path,
) -> None:
    env = create_env(tmp_path)
    result = await run(
        create_bash_tool(),
        {"command": "i=1; while [ $i -le 3000 ]; do echo line-$i; i=$((i + 1)); done"},
        env,
    )
    expected = "".join(f"line-{index + 1}\n" for index in range(3000))
    assert "".join(result["streamed"]) == expected
    message = cast(str, result["reported"][0]["message"])
    assert message.startswith("Full output: ")
    assert (
        get_or_throw(await env.read_text_file(message.removeprefix("Full output: "), BACKGROUND_CONTEXT))
        == expected
    )


async def test_does_not_spill_output_within_the_limits(tmp_path: Path) -> None:
    result = await run(create_bash_tool(), {"command": "printf small"}, create_env(tmp_path))
    assert result["reported"] == []


# ── public surface ─────────────────────────────────────────────────────────


def test_coding_tools_extension_has_all_four_and_no_powershell() -> None:
    names = [tool.name for tool in CodingTools.tools]
    assert names == ["read", "write", "edit", "bash"]
    assert "powershell" not in names
    assert [tool.name for tool in create_coding_tools().tools] == names


def test_tools_package_exports_the_ported_surface() -> None:
    import pi_durable.tools as tools

    for name in (
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
    ):
        assert hasattr(tools, name), name
    assert not hasattr(tools, "create_power_shell_tool")
    assert not hasattr(tools, "PowerShellToolInput")

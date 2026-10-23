"""Translated upstream ``harness-structured-output`` cases.

A tool's ``structuredOutput`` is the value programs get from ``executeTool()``, separate from the
``output`` the model reads. A declared schema is validated after the ``afterTool`` chain; a
schema-less tool's value is its bounded output. A model-issued call never stores the value.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from chat_support import chat_setup, open_chat
from session_support import context
from tool_support import DONE, add_hooks, add_tool, all_entries, calls, codes
from tool_support import tool as make_tool

from pi_durable import MemoryStorage, NestedCallDoc, ToolResultEntry, ToolTask
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.tools import create_bash_tool

COUNT = {
    "type": "object",
    "properties": {"count": {"type": "number"}},
    "required": ["count"],
}


def call(name: str) -> dict[str, Any]:
    return calls([name, {}, "c1"])


def returning(name: str, result: Mapping[str, Any], **extra: Any) -> Any:
    return make_tool(name, lambda _args, _api, _context: dict(result), **extra)


async def nested(setup: Any, name: str) -> dict[str, Any]:
    """Run ``name`` as a nested call of a ``probe`` tool and return what the probe received."""

    received: dict[str, Any] = {}

    async def probe(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.update(await api.execute_tool(name, {}, call_context))
        return {}

    add_tool(setup.registry, make_tool("probe", probe))
    setup.faux.set_responses([call("probe"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await harness.close(context)
    return received


async def direct(setup: Any, name: str) -> dict[str, Any]:
    """Run ``name`` as a model-issued call and return its transcript message."""

    setup.faux.set_responses([call(name), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    entry = next(entry for entry in await all_entries(root) if ToolResultEntry.is_(entry))
    await harness.close(context)
    return cast(dict[str, Any], entry["model"][0])


async def test_gives_callers_a_schema_less_tools_output_shape() -> None:
    text_item = {"type": "text", "text": "one"}
    image = {"type": "image", "data": "AAAA", "mimeType": "image/png"}
    cases: list[tuple[list[dict[str, Any]], Any]] = [
        ([text_item], "one"),
        ([image], image),
        ([], ""),
        ([text_item, image], [text_item, image]),
        ([text_item, text_item], [text_item, text_item]),
    ]
    for output, value in cases:
        setup = chat_setup()
        add_tool(setup.registry, returning("plain", {"output": output}))
        received = await nested(setup, "plain")
        assert received["structuredOutput"] == value
        assert received["isError"] is False
        assert received["diagnostics"] == []


async def test_gives_callers_the_bounded_output_without_the_rendered_diagnostics() -> None:
    setup = chat_setup()
    add_tool(
        setup.registry,
        returning(
            "long",
            {
                "output": [{"type": "text", "text": "a\nb\nc"}],
                "diagnostics": [{"severity": "info", "message": "note"}],
            },
            output_limits={"max_lines": 1},
        ),
    )
    received = await nested(setup, "long")
    assert received["structuredOutput"] == "a\n"
    assert codes(received) == [None, "truncated"]


async def test_stores_exactly_what_callers_get_without_the_models_output_and_keeps_a_stored_null() -> None:
    setup = chat_setup()
    add_tool(setup.registry, returning("plain", {"output": [{"type": "text", "text": "a"}]}))
    add_tool(
        setup.registry,
        returning("nothing", {"structuredOutput": None}, structured_output_schema={"type": "null"}),
    )
    received: list[dict[str, Any]] = []

    async def probe(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.append(await api.execute_tool("plain", {}, call_context))
        received.append(await api.execute_tool("nothing", {}, call_context))
        return {}

    add_tool(setup.registry, make_tool("probe", probe))
    setup.faux.set_responses([call("probe"), DONE])
    stored: list[Any] = []
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]

    def capture(publication: Any, _listener_context: Any) -> None:
        for change in publication["changes"]:
            if change["type"] != "document" or change["record"]["kind"] != "pi.tool.nested-call":
                continue
            value = cast(dict[str, Any] | None, change["value"])
            if value is not None and "result" in value and change["ops"]:
                stored.append(value["result"])

    harness.subscribe_commits(capture)
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await harness.wait_for_idle(context)
    await harness.close(context)
    assert received[0]["structuredOutput"] == "a"
    assert received[0]["isError"] is False
    assert received[1]["structuredOutput"] is None
    assert received[1]["isError"] is False
    assert "output" not in received[0]
    assert stored == received


async def test_gives_callers_a_schema_tools_validated_output_which_the_transcript_does_not_store() -> None:
    result = {"output": [{"type": "text", "text": "3 things"}], "structuredOutput": {"count": 3}}
    setup = chat_setup()
    add_tool(setup.registry, returning("counter", result, structured_output_schema=COUNT))
    received = await nested(setup, "counter")
    assert received["structuredOutput"] == {"count": 3}
    assert received["isError"] is False

    model_setup = chat_setup()
    add_tool(model_setup.registry, returning("counter", result, structured_output_schema=COUNT))
    stored = await direct(model_setup, "counter")
    assert stored["isError"] is False
    assert stored["content"] == [{"type": "text", "text": "3 things"}]
    assert "structuredOutput" not in stored


async def test_turns_broken_structured_output_into_an_error_for_callers_and_a_report_for_the_model() -> None:
    cases: list[tuple[str, dict[str, Any], bool, str]] = [
        ("wrong", {"structuredOutput": {"count": "three"}}, True, "does not match its schema: count:"),
        ("missing", {"output": []}, True, "returned no structuredOutput"),
        ("undeclared", {"structuredOutput": {"count": 3}}, False, "declares no structuredOutputSchema"),
    ]
    for name, result, schema, message in cases:
        setup = chat_setup()
        extra = {"structured_output_schema": COUNT} if schema else {}
        add_tool(setup.registry, returning(name, result, **extra))
        received = await nested(setup, name)
        assert received["isError"] is True
        # The broken value is dropped; a schema-less tool's error result still carries its output, here none.
        if schema:
            assert "structuredOutput" not in received
        else:
            assert received["structuredOutput"] == ""
        assert codes(received) == ["invalid_structured_output"]
        assert message in received["diagnostics"][0]["message"]

        model_setup = chat_setup()
        add_tool(model_setup.registry, returning(name, result, **extra))
        # The model never sees structured output: its call stands, and the host gets a report.
        stored = await direct(model_setup, name)
        assert stored["isError"] is False
        assert message not in str(stored["content"])
        assert message in "\n".join(str(error) for error in model_setup.reports)


async def test_gives_callers_the_output_of_an_error_result_of_a_tool_without_a_schema() -> None:
    setup = chat_setup()
    add_tool(
        setup.registry,
        returning("bad", {"output": [{"type": "text", "text": "bad"}], "isError": True}),
    )
    received = await nested(setup, "bad")
    assert received["isError"] is True
    assert received["structuredOutput"] == "bad"


async def test_gives_callers_no_structured_output_for_an_error_result_the_harness_wrote() -> None:
    setup = chat_setup()
    received = await nested(setup, "missing")
    assert received["isError"] is True
    assert received["diagnostics"][0]["code"] == "tool_unavailable"
    assert "structuredOutput" not in received


async def test_gives_callers_a_schema_tools_error_text_as_a_diagnostic_never_as_structured_output() -> None:
    setup = chat_setup()
    add_tool(
        setup.registry,
        returning(
            "fails",
            {"output": [{"type": "text", "text": "nope"}], "isError": True},
            structured_output_schema=COUNT,
        ),
    )
    received = await nested(setup, "fails")
    assert received["isError"] is True
    assert received["diagnostics"] == [{"severity": "error", "code": "tool_error", "message": "nope"}]
    assert "structuredOutput" not in received

    # The model's call keeps the text in its output, without the extra diagnostic.
    model_setup = chat_setup()
    add_tool(
        model_setup.registry,
        returning(
            "fails",
            {"output": [{"type": "text", "text": "nope"}], "isError": True},
            structured_output_schema=COUNT,
        ),
    )
    stored = await direct(model_setup, "fails")
    assert stored["content"] == [{"type": "text", "text": "nope"}]

    # A thrown error's message is in its tool_error diagnostic, not in the output.
    thrown = chat_setup()

    def throw(_args: dict[str, Any], _api: Any, _context: Any) -> dict[str, Any]:
        raise Exception("permission denied")

    add_tool(thrown.registry, make_tool("throws", throw, structured_output_schema=COUNT))
    thrown_result = await nested(thrown, "throws")
    assert thrown_result["isError"] is True
    assert thrown_result["diagnostics"] == [
        {"severity": "error", "code": "tool_error", "message": "permission denied"}
    ]
    assert "structuredOutput" not in thrown_result

    # An error result whose own diagnostics say what went wrong gets no second one from its output.
    explained = chat_setup()
    add_tool(
        explained.registry,
        returning(
            "fails",
            {
                "output": [{"type": "text", "text": "partial log"}],
                "isError": True,
                "diagnostics": [{"severity": "error", "code": "quota", "message": "quota exceeded"}],
            },
            structured_output_schema=COUNT,
        ),
    )
    assert (await nested(explained, "fails"))["diagnostics"] == [
        {"severity": "error", "code": "quota", "message": "quota exceeded"}
    ]

    # An error result that has structured output keeps it.
    kept = chat_setup()
    add_tool(
        kept.registry,
        returning(
            "fails",
            {
                "output": [{"type": "text", "text": "nope"}],
                "structuredOutput": {"count": 0},
                "isError": True,
            },
            structured_output_schema=COUNT,
        ),
    )
    kept_result = await nested(kept, "fails")
    assert kept_result["isError"] is True
    assert kept_result["structuredOutput"] == {"count": 0}


async def test_keeps_structured_output_when_after_tool_replaces_the_output() -> None:
    setup = chat_setup()
    add_tool(setup.registry, returning("plain", {"output": [{"type": "text", "text": "secret"}]}))
    add_tool(
        setup.registry,
        returning("counter", {"structuredOutput": {"count": 1}}, structured_output_schema=COUNT),
    )
    add_tool(
        setup.registry,
        returning("other", {"structuredOutput": {"count": 1}}, structured_output_schema=COUNT),
    )

    def after_tool(hook_call: dict[str, Any], result: dict[str, Any], _api: Any, _context: Any) -> Any:
        if hook_call["name"] == "other":
            return {**result, "structuredOutput": {"count": "broken"}}
        if hook_call["name"] == "probe":
            return result
        return {**result, "output": [{"type": "text", "text": "rewritten"}]}

    add_hooks(setup.registry, ToolTask, {"afterTool": after_tool})
    seen: dict[str, dict[str, Any]] = {}

    async def probe(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        seen["plain"] = await api.execute_tool("plain", {}, call_context)
        seen["counter"] = await api.execute_tool("counter", {}, call_context)
        seen["other"] = await api.execute_tool("other", {}, call_context)
        return {}

    add_tool(setup.registry, make_tool("probe", probe))
    setup.faux.set_responses([call("probe"), DONE])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await harness.close(context)
    # A schema-less tool's value is its output, so redacting the output redacts what programs get.
    assert seen["plain"]["structuredOutput"] == "rewritten"
    assert seen["counter"]["structuredOutput"] == {"count": 1}
    assert seen["other"]["isError"] is True
    assert codes(seen["other"]) == ["invalid_structured_output"]


async def test_gives_callers_of_bash_its_retained_output_and_exit_code_also_for_a_nonzero_exit(
    tmp_path: Path,
) -> None:
    setup = chat_setup()
    add_tool(setup.registry, create_bash_tool())
    received: list[dict[str, Any]] = []

    async def probe(_args: dict[str, Any], api: Any, call_context: Any) -> dict[str, Any]:
        received.append(await api.execute_tool("bash", {"command": "printf ok"}, call_context))
        received.append(await api.execute_tool("bash", {"command": "printf bad; exit 4"}, call_context))
        return {}

    add_tool(setup.registry, make_tool("probe", probe))
    setup.faux.set_responses([call("probe"), DONE])
    opened = await open_chat(
        MemoryStorage(), setup, env=lambda _target, _context: PosixExecutionEnv(cwd=str(tmp_path))
    )
    harness, root = opened["harness"], opened["root"]
    harness.resume()
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    await harness.close(context)
    assert received[0]["isError"] is False
    assert received[0]["structuredOutput"] == {"output": "ok", "truncated": False, "exitCode": 0}
    assert received[1]["isError"] is True
    assert received[1]["structuredOutput"] == {"output": "bad", "truncated": False, "exitCode": 4}
    assert codes(received[1]) == ["exit_code"]


def test_does_not_import_jsonschema_until_a_tool_declares_a_schema() -> None:
    """The core package has no runtime dependency; the extra is needed only to validate output."""

    import os
    import subprocess
    import sys

    source = str(Path(__file__).resolve().parents[1] / "src")
    check = subprocess.run(
        [sys.executable, "-c", "import sys, pi_durable; print('jsonschema' in sys.modules)"],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": source},
        check=True,
    )
    assert check.stdout.strip() == "False"
    assert NestedCallDoc.definition.kind == "pi.tool.nested-call"


def test_needs_the_schema_extra_only_for_a_tool_that_declares_a_schema(monkeypatch: Any) -> None:
    import sys

    import pytest

    from pi_durable.harness.validation import structured_output_error

    schema = {
        "type": "object",
        "properties": {"count": {"type": "number"}},
        "required": ["count"],
    }
    declaring = make_tool("declaring", lambda _args, _api, _context: {}, structured_output_schema=schema)
    monkeypatch.setitem(sys.modules, "jsonschema", None)
    with pytest.raises(ImportError, match="pi-durable-python\\[schema\\]"):
        structured_output_error(declaring, {"structuredOutput": {"count": 1}})
    # A tool without a schema never needs the extra.
    plain = make_tool("plain", lambda _args, _api, _context: {})
    assert structured_output_error(plain, {"output": []}) is None

"""Translated v1.4.0 ``harness-host-inputs`` suite: ``models`` and ``settings`` as functions.

``HarnessOptions.models`` and ``HarnessOptions.settings`` accept a value or a zero-argument
function resolved at each use, so a host can replace its model configuration or settings without
reopening the Harness. Generation and compaction call the function at each request; ``api.models``
of tools and hooks reads the current value at each access.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from chat_support import FauxModels, all_entries, assistant_message, faux_tool_call, text_of
from session_support import context

from pi_durable import Harness, MemoryStorage, define_extension
from pi_durable.harness.define import define_tool
from pi_durable.harness.registry import create_registry
from pi_durable.harness.types import Registry


def answering(text: str) -> FauxModels:
    """Models whose only provider answers every request with ``text``."""

    models = FauxModels()
    models.set_responses([assistant_message(text) for _ in range(10)])
    return models


def answering_callable() -> FauxModels:
    """Models whose response is a callback, so a test can capture each request's options."""

    return FauxModels()


async def open_harness(
    options: Mapping[str, Any], registry: Registry | None = None
) -> dict[str, Any]:
    harness = await Harness.open(
        MemoryStorage(), {**options, "registry": registry or create_registry()}, context
    )
    root = await harness.root(
        context, {"agent": {"model": {"provider": "faux", "modelId": "faux-1"}}}
    )
    return {"harness": harness, "root": root}


async def turn(root: Any, content: str) -> str | None:
    settled = await (await root.submit({"type": "input", "content": content}, context)).wait(context)
    assert settled["status"] == "done"
    entries = await all_entries(root)
    model = entries[-1].get("model") or ()
    return text_of(model[0]) if model else None


async def test_generation_uses_the_current_return_value_of_a_models_function_at_each_request() -> None:
    current = [answering("first")]
    opened = await open_harness({"models": lambda: current[0]})
    assert await turn(opened["root"], "one") == "first"
    current[0] = answering("second")
    assert await turn(opened["root"], "two") == "second"
    await opened["harness"].close(context)


async def test_still_accepts_a_plain_models_object() -> None:
    opened = await open_harness({"models": answering("plain")})
    assert await turn(opened["root"], "one") == "plain"
    await opened["harness"].close(context)


async def test_tools_read_the_current_models_at_each_access_of_api_models() -> None:
    first = answering_callable()
    second = answering_callable()
    first.set_responses(
        [
            assistant_message(
                [faux_tool_call("probe", {}, id="call-1")], stop_reason="toolUse"
            ),
            assistant_message("done"),
        ]
    )
    # The request after the tool round reads the host's current value, so the swapped models answer it.
    second.set_responses([assistant_message("second answer")])
    current = [first]
    seen: list[Any] = []

    async def probe(_args: Any, api: Any, _context: Any) -> dict[str, Any]:
        seen.append(api.models)
        current[0] = second
        seen.append(api.models)
        return {"output": []}

    registry = create_registry()
    registry.install(
        define_extension(
            {
                "name": "probe",
                "tools": [
                    define_tool(
                        {
                            "name": "probe",
                            "description": "Records api.models before and after the host swaps it",
                            "parameters": {"type": "object", "properties": {}},
                            "execute": probe,
                        }
                    )
                ],
            }
        )
    )
    opened = await open_harness({"models": lambda: current[0]}, registry)
    assert await turn(opened["root"], "go") == "second answer"
    # The property reads the host's current value at each access.
    assert seen == [first, second]
    await opened["harness"].close(context)


async def test_generation_reads_the_current_return_value_of_a_settings_function() -> None:
    timeouts: list[Any] = []
    models = answering_callable()

    def reply(_request: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
        timeouts.append(options.get("timeoutMs"))
        return assistant_message("ok")

    models.set_responses([reply, reply])
    settings = [{"stream": {"timeout_ms": 1_000}}]
    opened = await open_harness({"models": models, "settings": lambda: settings[0]})
    await turn(opened["root"], "one")
    settings[0] = {"stream": {"timeout_ms": 2_000}}
    await turn(opened["root"], "two")
    assert timeouts == [1_000, 2_000]
    await opened["harness"].close(context)

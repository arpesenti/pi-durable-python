"""Translated upstream ``tool-search`` cases plus the BM25 ranking they rest on.

The six deferred-loading cases are a direct translation of ``test/tool-search.test.ts``: the offered
and deferred sets, load order, ``modelTools`` edits by name, extension reload, forks and reset
baselines, and an unrelated query. The tokenizer and ranker have their own unit cases because the
upstream suite reaches them only through the tool: camel-case boundaries, stop words, naive plural
stems, BM25 length normalization, ties in document order, and the limit.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from chat_support import all_entries, open_chat
from session_support import context
from tool_support import DONE, add_tool, calls, chat_setup, results

from pi_durable import AgentDoc, MemoryStorage
from pi_durable.harness.define import define_extension, define_tool
from pi_durable.harness.types import ToolRegistration
from pi_durable.search import SearchDocument, rank_documents, tokenize
from pi_durable.tool_search import (
    DEFAULT_TOOL_SEARCH_LIMIT,
    TOOL_SEARCH_TOOL_NAME,
    create_tool_search_extension,
    create_tool_search_tool,
)

DESCRIPTION = (
    "# Tool discovery\n\n"
    "Searches over deferred tool metadata with BM25 and exposes matching tools for the next model "
    "call.\n\n"
    "Some of the tools, such as tools of MCP servers, may not have been provided to you upfront, and "
    "you should use this tool (`tool_search`) to search for the required tools. For MCP tool "
    "discovery, always use `tool_search`."
)


def make_tool(name: str, description: str, **extra: Any) -> ToolRegistration:
    return define_tool(
        {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": {"number": {"type": "number", "description": "Issue number"}},
            },
            "execute": lambda *_args: {"output": [{"type": "text", "text": f"{name} ran"}]},
            **extra,
        }
    )


def setup() -> Any:
    """``alpha``, the deferred GitHub tools, ``omega``, then ``tool_search`` last."""

    chat = chat_setup()
    add_tool(chat.registry, make_tool("alpha", "Alpha tool"))
    add_tool(
        chat.registry,
        make_tool("github_get_issue", "Get a GitHub issue by number", deferred=True),
    )
    add_tool(
        chat.registry,
        make_tool("github_list_repos", "List the repositories of a user", deferred=True),
    )
    add_tool(chat.registry, make_tool("omega", "Omega tool"))
    chat.registry.install(create_tool_search_extension())
    return chat


async def offered(root: Any) -> list[str]:
    return [each.name for each in (await root.agent(context)).tools]


def system_messages(entries: Sequence[Any]) -> list[dict[str, Any]]:
    """The model messages of the ``pi.system`` entries, oldest first."""

    return [cast(dict[str, Any], entry["model"][0]) for entry in entries if entry["kind"] == "pi.system"]


def tools_added(message: dict[str, Any]) -> list[str]:
    return [tool["name"] for tool in message.get("toolsAdded") or ()]


def tool_result_texts(entries: Sequence[Any]) -> list[str]:
    return [
        "".join(str(item.get("text") or "") for item in result.get("content") or ())
        for result in results(entries)
    ]


def test_exposes_the_coding_agents_tool_search_definition() -> None:
    assert TOOL_SEARCH_TOOL_NAME == "tool_search"
    assert DEFAULT_TOOL_SEARCH_LIMIT == 8
    registration = create_tool_search_tool()
    assert registration.name == TOOL_SEARCH_TOOL_NAME
    assert registration.description == DESCRIPTION
    assert registration.parameters == {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search query for deferred tools.",
            },
            "limit": {
                "type": "number",
                "description": "Maximum number of tools to return. Defaults to 8.",
            },
        },
        "required": ["query"],
    }
    assert registration.callers == ["model"]
    assert registration.replay == "safe"
    extension = create_tool_search_extension()
    assert extension.name == "tool-search"
    assert [each.name for each in extension.tools] == ["tool_search"]


def test_tokenizes_camel_case_acronyms_stop_words_and_naive_stems() -> None:
    assert tokenize("GetGitHubIssue") == ["get", "git", "hub", "issue"]
    assert tokenize("HTTPRequest") == ["http", "request"]
    assert tokenize("The issue OF searches") == ["issue", "search"]
    assert tokenize("boxes wishes classes addresses issues") == [
        "box",
        "wish",
        "class",
        "address",
        "issue",
    ]
    assert tokenize("the and of") == []
    assert tokenize("") == []


def test_ranks_by_bm25_keeps_ties_in_document_order_and_honors_the_limit() -> None:
    documents: list[SearchDocument] = [
        {"name": "list", "text": "List the repositories of a user"},
        {"name": "get", "text": "Get a GitHub issue by number"},
        {"name": "long", "text": "alpha beta gamma delta"},
        {"name": "short", "text": "alpha"},
        {"name": "first", "text": "shared term"},
        {"name": "second", "text": "shared term"},
    ]
    assert rank_documents("github issue", documents, 8) == ["get"]
    assert rank_documents("repositories", documents, 8) == ["list"]
    assert rank_documents("issues", documents, 8) == ["get"]
    # A shorter document wins length normalization.
    assert rank_documents("alpha", documents, 8) == ["short", "long"]
    # Equal scores keep document order, before and after the limit.
    assert rank_documents("shared", documents, 8) == ["first", "second"]
    assert rank_documents("shared", documents, 1) == ["first"]
    assert rank_documents("weather forecast", documents, 8) == []
    assert rank_documents("", documents, 8) == []
    assert rank_documents("alpha", documents, 0) == []


async def test_offers_a_deferred_tool_only_once_tool_search_loads_it() -> None:
    chat = setup()
    chat.faux.set_responses(
        [
            calls(["tool_search", {"query": "github issue", "limit": 1}, "c1"]),
            calls(["github_get_issue", {"number": 42}, "c2"]),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    assert await offered(root) == ["alpha", "omega", "tool_search"]
    agent = await root.agent(context)
    assert [each.name for each in agent.deferred] == [
        "github_get_issue",
        "github_list_repos",
    ]
    # Deferred tools stay callable by other tools, such as code mode scripts.
    assert "github_get_issue" in [each.name for each in agent.callable]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    entries = await all_entries(root)
    search, issue = tool_result_texts(entries)
    assert search == (
        "Loaded 1 tool. They are available from your next call:\n"
        "- github_get_issue: Get a GitHub issue by number"
    )
    assert results(entries)[0]["details"] == {"loaded": ["github_get_issue"]}
    assert issue == "github_get_issue ran"
    state = await harness.snapshot(AgentDoc, root.id, context)
    assert state is not None
    assert state["modelTools"] == {"add": ["github_get_issue"]}
    assert await offered(root) == [
        "alpha",
        "omega",
        "tool_search",
        "github_get_issue",
    ]
    # A later search sees only the tools not loaded yet.
    assert [each.name for each in (await root.agent(context)).deferred] == ["github_list_repos"]
    # The load appended the declaration: no tool was removed and declared again.
    baseline, load = system_messages(entries)
    assert tools_added(baseline) == ["alpha", "omega", "tool_search"]
    assert tools_added(load) == ["github_get_issue"]
    assert "toolsRemoved" not in load
    await harness.close(context)


async def test_appends_each_loaded_tool_in_load_order_not_registry_order() -> None:
    chat = setup()
    chat.faux.set_responses(
        [
            calls(["tool_search", {"query": "repositories", "limit": 1}, "c1"]),
            calls(["tool_search", {"query": "issue", "limit": 1}, "c2"]),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert await offered(root) == [
        "alpha",
        "omega",
        "tool_search",
        "github_list_repos",
        "github_get_issue",
    ]
    loads = system_messages(await all_entries(root))[1:]
    assert [tools_added(message) for message in loads] == [
        ["github_list_repos"],
        ["github_get_issue"],
    ]
    assert all("toolsRemoved" not in message for message in loads)
    await harness.close(context)


async def test_records_loads_by_name_and_a_load_lifts_a_remove() -> None:
    chat = setup()

    def grow(*_args: Any) -> dict[str, Any]:
        return {
            "output": [],
            "control": {"addTools": ["github_get_issue", "alpha", "ghost"]},
        }

    add_tool(
        chat.registry,
        make_tool(
            "grow",
            "grow",
            execute=grow,
            parameters={"type": "object", "properties": {}},
        ),
    )
    chat.faux.set_responses([calls(["grow", {}, "c1"]), DONE, calls(["grow", {}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    state = await harness.snapshot(AgentDoc, root.id, context)
    assert state is not None
    # Unknown and non-deferred names are recorded too; they simply do not load anything.
    assert state["modelTools"] == {"add": ["github_get_issue", "alpha", "ghost"]}
    assert await offered(root) == [
        "alpha",
        "omega",
        "tool_search",
        "grow",
        "github_get_issue",
    ]
    # A name in both remove and add is not offered.
    [get_issue] = [each for each in (await root.agent(context)).callable if each.name == "github_get_issue"]
    await root.configure({"modelTools": {"remove": [get_issue], "add": [get_issue]}}, context)
    assert "github_get_issue" not in await offered(root)
    # Loading it again drops it from remove.
    await (await root.submit({"type": "input", "content": "again"}, context)).wait(context)
    state = await harness.snapshot(AgentDoc, root.id, context)
    assert state is not None
    assert state["modelTools"] == {"add": ["github_get_issue", "alpha", "ghost"]}
    assert "github_get_issue" in await offered(root)
    await harness.close(context)


async def test_keeps_a_loaded_tool_when_its_extension_is_gone_and_appends_to_an_array() -> None:
    chat = setup()
    issue = make_tool("github_get_issue", "Get a GitHub issue by number", deferred=True)
    chat.faux.set_responses(
        [
            calls(["tool_search", {"query": "github issue", "limit": 1}, "c1"]),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    # The server goes away: its extension is installed again without the tool.
    chat.registry.install(define_extension({"name": "tool:github_get_issue", "tools": []}))
    assert "github_get_issue" not in await offered(root)
    chat.registry.install(define_extension({"name": "tool:github_get_issue", "tools": [issue]}))
    assert "github_get_issue" in await offered(root)
    # With an explicit array, a load appends to it as before.
    agent = await root.agent(context)
    alpha = next(each for each in agent.tools if each.name == "alpha")
    search = next(each for each in agent.tools if each.name == "tool_search")
    await root.configure({"modelTools": [alpha, search]}, context)
    chat.faux.set_responses(
        [
            calls(["tool_search", {"query": "repositories", "limit": 1}, "c1"]),
            DONE,
        ]
    )
    await (await root.submit({"type": "input", "content": "more"}, context)).wait(context)
    state = await harness.snapshot(AgentDoc, root.id, context)
    assert state is not None
    assert state["modelTools"] == [
        "alpha",
        "tool_search",
        "github_list_repos",
    ]
    await harness.close(context)


async def test_keeps_loaded_tools_per_branch_and_in_the_baseline_after_a_reset() -> None:
    chat = setup()
    chat.faux.set_responses(
        [
            calls(["tool_search", {"query": "github issue", "limit": 1}, "c1"]),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    entries = await all_entries(root)
    before = await root.fork(entries[0]["id"], {"ownership": {"kind": "ownerless"}}, context)
    after = await root.fork(entries[-1]["id"], {"ownership": {"kind": "ownerless"}}, context)
    assert "github_get_issue" not in await offered(before)
    assert "github_get_issue" in await offered(after)
    await root.reset(None, context)
    chat.faux.set_responses([DONE])
    await (await root.submit({"type": "input", "content": "fresh"}, context)).wait(context)
    baseline = system_messages(await all_entries(root))[-1]
    assert tools_added(baseline) == [
        "alpha",
        "omega",
        "tool_search",
        "github_get_issue",
    ]
    await harness.close(context)


async def test_finds_nothing_for_an_unrelated_query_and_loads_nothing() -> None:
    chat = setup()
    chat.faux.set_responses([calls(["tool_search", {"query": "weather forecast"}, "c1"]), DONE])
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    assert tool_result_texts(await all_entries(root)) == ["No matching tools found."]
    state = await harness.snapshot(AgentDoc, root.id, context)
    assert state is None or state.get("modelTools") is None
    await harness.close(context)


async def test_rejects_an_empty_query_and_a_non_positive_integer_limit() -> None:
    chat = setup()
    chat.faux.set_responses(
        [
            calls(
                ["tool_search", {"query": "  "}, "c1"],
                ["tool_search", {"query": "github", "limit": 1.5}, "c2"],
                ["tool_search", {"query": "github", "limit": 0}, "c3"],
            ),
            DONE,
        ]
    )
    opened = await open_chat(MemoryStorage(), chat)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    empty, fraction, zero = results(await all_entries(root))
    assert [result["isError"] for result in (empty, fraction, zero)] == [True, True, True]
    texts = tool_result_texts(await all_entries(root))
    assert "query must not be empty" in texts[0]
    assert "limit must be a positive integer" in texts[1]
    assert "limit must be a positive integer" in texts[2]
    state = await harness.snapshot(AgentDoc, root.id, context)
    assert state is None or state.get("modelTools") is None
    await harness.close(context)

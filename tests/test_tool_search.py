"""Translated upstream ``tool-search`` cases plus the BM25 ranking they rest on.

The six deferred-loading cases are a direct translation of ``test/tool-search.test.ts``: the offered
and deferred sets, load order, ``modelTools`` edits by name, extension reload, forks and reset
baselines, and an unrelated query. The tokenizer cases pin what the upstream suite reaches only
through the tool: camel-case boundaries, stop words, naive plural stems. The ranker cases translate
``test/search.test.ts``'s ``rankDocuments`` cases one for one — every non-Unicode assertion of them
— over the same fixtures and the same expected orders: term relevance and the limit, the namespace
field, a name match above mentions in descriptions, the whole-local-name bonus, exact-name-first,
and coverage above score. The eight Unicode cases left untranslated — the ``rankDocuments``
``it.each`` "matches Unicode queries against %s" of ``test/search.test.ts``, one case per row:
``:65`` 搜索仓库问题, ``:66`` 文書を検索, ``:67`` 문서 검색, ``:68`` ค้นหาเอกสาร, ``:69`` Поиск документов,
``:70`` البحث عن الملفات, ``:71`` फ़ाइल खोजें, ``:72`` Rechercher un café — wait on
the word segmenter the port does not have, and the two port cases besides the translated ones pin
what no upstream case isolates: length normalization with ties in document order, and exact-name-first
against a competitor that outscores the named tool.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from chat_support import all_entries, open_chat
from session_support import context
from tool_support import DONE, add_tool, calls, chat_setup, results

from pi_durable import AgentDoc, MemoryStorage
from pi_durable.harness.define import define_extension, define_tool
from pi_durable.harness.prompt import to_tool_declaration
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


def search_document(name: str, text: str, namespace_text: str | None = None) -> SearchDocument:
    """Upstream ``test/search.test.ts``'s ``doc()``: the namespace text only when the case has one."""

    document: SearchDocument = {"name": name, "text": text}
    if namespace_text is not None:
        document["namespaceText"] = namespace_text
    return document


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
    parameters: dict[str, Any] = {
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
    # The authored schema carries the port's spelling of upstream's `{open: true}`; see PARITY.md.
    assert registration.parameters == {**parameters, "additionalProperties": True}
    # What a host is told drops it: all upstream's `obj(..., {open: true})` ever emitted.
    assert to_tool_declaration(registration)["parameters"] == parameters
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


def test_searches_the_namespace_text() -> None:
    """Upstream ``test/search.test.ts:83``: a term of the namespace text alone still matches."""

    namespaced = [search_document("mcp__x__run", "Run it.", "mcp__x Kubernetes cluster tools")]
    assert rank_documents("kubernetes", namespaced, 8) == ["mcp__x__run"]


#: Upstream ``test/search.test.ts:105``: the two CI tools of the name-match case.
CI_DOCUMENTS = [
    search_document("mcp__ci__status", "Deploy status: last deploy, deploy log, deploy errors."),
    search_document(
        "mcp__ci__deploy_release",
        "Start a release of the current branch to an environment, run the migrations, switch traffic"
        " over, and wait until the new version is live and healthy.",
    ),
]


def test_ranks_a_name_match_above_mentions_in_descriptions() -> None:
    """Upstream ``test/search.test.ts:105``: one mention in the name beats three in the text."""

    assert rank_documents("deploy", CI_DOCUMENTS, 1) == ["mcp__ci__deploy_release"]


#: Upstream ``test/search.test.ts:117``: the namespace text every ``mcp__peekaboo__`` tool shares.
PEEKABOO_NAMESPACE = "mcp__peekaboo Peekaboo controls macOS apps by app name."

#: Upstream ``test/search.test.ts:117``: the three ``mcp__peekaboo__`` tools share that namespace
#: text, and two unrelated tools compete with them.
PEEKABOO_DOCUMENTS = [
    search_document(
        "mcp__peekaboo__see",
        "Capture the app window of the peekaboo app and describe the app.",
        PEEKABOO_NAMESPACE,
    ),
    search_document("mcp__peekaboo__menu", "Click a menu item of an app.", PEEKABOO_NAMESPACE),
    search_document(
        "mcp__peekaboo__app",
        "Launch, quit, relaunch, focus, hide, or switch applications. Targets an application by"
        " name, bundle identifier, or process ID, and can wait until it is ready.",
        PEEKABOO_NAMESPACE,
    ),
    search_document("mcp__app__queue_read", "Read a queued message of this thread."),
    search_document(
        "mcp__dev-app__thread_read",
        "Read durable state and a paginated timeline from any thread. The default view returns user"
        " messages, assistant messages and plans.",
    ),
]


def test_ranks_a_tool_first_when_the_query_contains_its_whole_name() -> None:
    """Upstream ``test/search.test.ts:117``: the whole local name in the query is a bonus.

    The first two queries are the ``__``-suffix bonus at work: dropping upstream's
    ``score += fullNameBonus`` line from its own ``src/search.ts`` answers
    ``mcp__peekaboo__see`` for both, because the shared namespace text and the mentions in the
    descriptions decide without it.
    """

    assert rank_documents("peekaboo app", PEEKABOO_DOCUMENTS, 1) == ["mcp__peekaboo__app"]
    assert rank_documents("open the peekaboo app tool", PEEKABOO_DOCUMENTS, 1) == ["mcp__peekaboo__app"]
    assert rank_documents("read thread_read state", PEEKABOO_DOCUMENTS, 1) == ["mcp__dev-app__thread_read"]
    # Without the whole name, mentions in descriptions decide.
    assert rank_documents("describe window", PEEKABOO_DOCUMENTS, 1) == ["mcp__peekaboo__see"]


#: Upstream ``test/search.test.ts:89``: the two competing tools of the exact-name case.
NAMED_DOCUMENTS = [
    search_document("mcp__app__queue_read", "Read a queued message of this thread."),
    search_document(
        "mcp__dev-app__thread_read",
        "Read durable state and a paginated timeline from any thread. The default view returns user"
        " messages, assistant messages and plans. Continue with afterPosition until hasMore is false.",
    ),
]


def test_ranks_a_tool_first_when_the_query_is_its_name() -> None:
    """Upstream ``test/search.test.ts:89``: a query that names a tool reaches it first."""

    for query in ["thread_read", "dev_app__thread_read", "mcp__dev-app__thread_read", "Thread_Read"]:
        assert rank_documents(query, NAMED_DOCUMENTS, 1) == ["mcp__dev-app__thread_read"]
    # Partial names still rank by BM25.
    assert rank_documents("read", NAMED_DOCUMENTS, 1) == ["mcp__app__queue_read"]


#: Two tools whose names both carry ``thread`` and ``read``, written for #57: the competing one
#: outscores the named one, so only exact-name-first orders them the way upstream does.
EXACT_NAME_DOCUMENTS = [
    search_document("mcp__app__thread_read", "Fetch one item from the queue."),
    search_document(
        "mcp__thread__read_log",
        "Read a thread. Read the thread again and again.",
        "Thread and read tools",
    ),
]


def test_ranks_an_exact_name_query_ahead_of_the_document_that_outscores_it() -> None:
    """A query that is exactly a tool's name ranks it first, before coverage and score (#57).

    Upstream ``test/search.test.ts:89`` cannot see this rule: its two documents already land in the
    right order on the name weight and the whole-name bonus alone. Here ``mcp__thread__read_log``
    outscores the named tool, as the ``read`` query shows on its own, so the order below comes from
    exact-name-first and from nothing else. Dropping upstream's ``Number(right.exact) -
    Number(left.exact)`` key from the sort of its own ``src/search.ts`` answers the ``thread_read``
    query with ``mcp__thread__read_log`` first, while dropping its whole-name bonus leaves the
    answer unchanged.
    """

    assert rank_documents("thread_read", EXACT_NAME_DOCUMENTS, 2) == [
        "mcp__app__thread_read",
        "mcp__thread__read_log",
    ]
    assert rank_documents("mcp__app__thread_read", EXACT_NAME_DOCUMENTS, 2) == [
        "mcp__app__thread_read",
        "mcp__thread__read_log",
    ]
    # Not an exact name: the document that outscores it on mentions and namespace wins instead.
    assert rank_documents("read", EXACT_NAME_DOCUMENTS, 2) == [
        "mcp__thread__read_log",
        "mcp__app__thread_read",
    ]


#: Upstream ``test/search.test.ts:50``: the four tools the term-relevance and empty-query cases
#: share.
RANKING_DOCUMENTS = [
    search_document("mcp__github__list_issues", "List issues in a repository. state open or closed"),
    search_document("mcp__github__create_pull_request", "Open a pull request."),
    search_document("mcp__linear__search_issues", "Search Linear issues by text."),
    search_document("mcp__docs__search", "Search the documentation."),
]


def test_ranks_by_term_relevance_and_respects_the_limit() -> None:
    """Upstream ``test/search.test.ts:57``: BM25 relevance, then the limit."""

    assert rank_documents("issue", RANKING_DOCUMENTS, 8) == [
        "mcp__linear__search_issues",
        "mcp__github__list_issues",
    ]
    assert rank_documents("pull requests", RANKING_DOCUMENTS, 8)[0] == "mcp__github__create_pull_request"
    assert len(rank_documents("search", RANKING_DOCUMENTS, 1)) == 1
    assert rank_documents("closed", RANKING_DOCUMENTS, 8) == ["mcp__github__list_issues"]


def test_returns_nothing_for_unknown_or_empty_queries() -> None:
    """Upstream ``test/search.test.ts:77``: no term matches, or the query has no terms at all.

    The third query is the one upstream's Unicode segmenter answers with no terms; here the port's
    ASCII tokenizer does, so it pins the empty-query guard and nothing more.
    """

    assert rank_documents("kubernetes", RANKING_DOCUMENTS, 8) == []
    assert rank_documents("the", RANKING_DOCUMENTS, 8) == []
    assert rank_documents("\u672a\u77e5", RANKING_DOCUMENTS, 8) == []


#: Upstream ``test/search.test.ts:141``: the four ``mcp__peekaboo__`` tools of the coverage case.
PEEKABOO_BY_NAME_DOCUMENTS = [
    search_document(
        "mcp__peekaboo__app", "Launch, quit, focus, or switch applications by name or bundle identifier."
    ),
    search_document("mcp__peekaboo__see", "Capture the app window and describe the UI elements of the app."),
    search_document("mcp__peekaboo__window", "Move, resize, or close a window of an app."),
    search_document("mcp__peekaboo__image", "Take a screenshot of the screen, an app, or a window."),
]


def test_ranks_a_tool_matching_more_query_terms_first_whatever_the_scores() -> None:
    """Upstream ``test/search.test.ts:141``: coverage of the query's terms outranks any score.

    ``window`` and ``app`` are whole local names and take the bonus, yet each covers one of the two
    terms of the first two queries, so the tool covering both is the answer. An exact name still
    ranks first.
    """

    assert rank_documents("screenshot window", PEEKABOO_BY_NAME_DOCUMENTS, 1) == ["mcp__peekaboo__image"]
    assert rank_documents("describe app", PEEKABOO_BY_NAME_DOCUMENTS, 1) == ["mcp__peekaboo__see"]
    assert rank_documents("window", PEEKABOO_BY_NAME_DOCUMENTS, 1) == ["mcp__peekaboo__window"]


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

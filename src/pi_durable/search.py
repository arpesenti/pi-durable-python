"""BM25 tool search for the ``tool_search`` tool.

Port of the v1.5.0 ``src/search.ts``, the ranking the coding agent uses and durable keeps its own
copy of. Terms are lowercased, split at camel-case boundaries and non-alphanumerics, stripped of
stop words, and stemmed with the same naive singular rule. Okapi BM25 (``k1`` 1.2, ``b`` 0.75)
runs over the document's ``text``, with its ``name`` and its ``namespaceText`` as fields of their
own: a term of the name counts more than any number of mentions in the text, a tool whose whole
local name (after the last ``__``) is in the query takes a bonus worth one more match of every
query term, and a query that is exactly a tool's name ranks that tool first, before coverage. Only
``text`` is length-normalized, so a long name or long namespace instructions do not lower a tool's
score; equal matches keep document order.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from typing import NotRequired, TypedDict

K1 = 1.2
B = 0.75

#: A query term in the name counts this many times its IDF. The text part of a term is at most
#: ``K1 + 1`` (2.2) times its IDF and about 1 for a single mention, so a name match beats any
#: number of mentions in the text.
NAME_WEIGHT = 3.0

#: A query term of the namespace text counts this many times its IDF. Every tool of the namespace
#: shares it, so it helps find the namespace but not the tool in it.
NAMESPACE_WEIGHT = 0.5

STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "that",
        "the",
        "this",
        "to",
        "with",
    }
)

_CAMEL_BOUNDARY = re.compile(r"([a-z0-9])([A-Z])")
_ACRONYM_BOUNDARY = re.compile(r"([A-Z]+)([A-Z][a-z])")
_PLURAL_ES = re.compile(r"(ches|shes|sses|xes|zes)$")


class SearchDocument(TypedDict):
    """One searchable tool: its name, its text, and the text of its namespace.

    ``text`` is the description and the parameter names and descriptions, never the name, which the
    ranker reads from ``name``. ``namespaceText`` is the name, description and instructions of the
    tool's namespace.
    """

    name: str
    text: str
    namespaceText: NotRequired[str]


def _stem(term: str) -> str:
    """Naive singular form, so ``issues`` matches ``issue`` and ``searches`` matches ``search``."""

    if len(term) > 4 and term.endswith("ies"):
        return f"{term[:-3]}y"
    if len(term) > 4 and _PLURAL_ES.search(term):
        return term[:-2]
    if len(term) > 3 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def tokenize(text: str) -> list[str]:
    """Lowercase terms, split at camelCase boundaries and non-alphanumerics, without stop words."""

    separated = _CAMEL_BOUNDARY.sub(r"\1 \2", text)
    separated = _ACRONYM_BOUNDARY.sub(r"\1 \2", separated)
    return [
        _stem(term) for term in re.split(r"[^a-z0-9]+", separated.lower()) if term and term not in STOP_WORDS
    ]


def rank_documents(query: str, documents: Sequence[SearchDocument], limit: int) -> list[str]:
    """Okapi BM25 names of the best matches, best first; ties keep document order.

    A tool matching more of the query's terms, in any field, comes before one matching fewer,
    whatever their scores; within equal coverage the score decides, and within an equal score the
    document order does.
    """

    query_terms = list(dict.fromkeys(tokenize(query)))
    if not query_terms or not documents or limit <= 0:
        return []
    query_term_set = set(query_terms)
    namespace_terms = [set(tokenize(document.get("namespaceText", ""))) for document in documents]
    name_terms = [set(tokenize(document["name"])) for document in documents]
    # The terms of the name after its last ``__``, the part models say: ``thread`` and ``read`` for
    # ``mcp__app__thread_read``.
    local_name_terms = [
        list(dict.fromkeys(tokenize(document["name"].split("__")[-1]))) for document in documents
    ]
    term_counts = [_counts(tokenize(document["text"])) for document in documents]
    lengths = [sum(counts.values()) for counts in term_counts]
    total_length = sum(lengths)
    average_length = total_length / len(documents) if total_length else 1.0
    idf: dict[str, float] = {}
    for term in query_terms:
        frequency = sum(
            1
            for index, counts in enumerate(term_counts)
            if term in counts or term in namespace_terms[index] or term in name_terms[index]
        )
        idf[term] = math.log(1 + (len(documents) - frequency + 0.5) / (frequency + 0.5))
    # Worth one more match of every query term, to a tool whose whole local name is in the query.
    full_name_bonus = sum(idf.values())
    exact_query = _normalize_name(query)
    matches: list[tuple[str, bool, int, float]] = []
    for index, document in enumerate(documents):
        score = 0.0
        covered = 0
        norm = K1 * (1 - B + (B * lengths[index]) / average_length)
        for term in query_terms:
            count = term_counts[index].get(term)
            weight = (
                (NAME_WEIGHT if term in name_terms[index] else 0.0)
                + (NAMESPACE_WEIGHT if term in namespace_terms[index] else 0.0)
                + ((count * (K1 + 1)) / (count + norm) if count else 0.0)
            )
            if weight > 0:
                covered += 1
                score += idf[term] * weight
        if score == 0:
            continue
        local = local_name_terms[index]
        if local and all(term in query_term_set for term in local):
            score += full_name_bonus
        exact = _is_exact_name_query(exact_query, document["name"])
        matches.append((document["name"], exact, covered, score))
    # A tool naming the query whole comes first, before coverage; then the tool matching more of the
    # query's terms, whatever the scores; then the score. Equal in all three keeps document order.
    matches.sort(key=lambda match: (not match[1], -match[2], -match[3]))
    return [name for name, _exact, _covered, _score in matches[:limit]]


def _normalize_name(name: str) -> str:
    """Lowercase with characters invalid in script identifiers as ``_``, so ``dev-radius`` matches
    ``dev_radius``."""

    return re.sub(r"[^a-z0-9_$]", "_", name.strip().lower())


def _is_exact_name_query(query: str, name: str) -> bool:
    """Whether the whole query is the tool's name: its full name or a suffix after a ``__``
    separator, so ``thread_read`` and ``app__thread_read`` both name ``mcp__app__thread_read``."""

    normalized = _normalize_name(name)
    return normalized == query or normalized.endswith(f"__{query}")


def _counts(terms: Sequence[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for term in terms:
        counts[term] = counts.get(term, 0) + 1
    return counts


__all__ = ["SearchDocument", "rank_documents", "tokenize"]

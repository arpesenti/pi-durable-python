"""BM25 tool search for the ``tool_search`` tool.

Port of the v1.3.0 ``src/search.ts``, the ranking the coding agent uses and durable keeps its own
copy of. Terms are lowercased, split at camel-case boundaries and non-alphanumerics, stripped of
stop words, and stemmed with the same naive singular rule. Okapi BM25 (``k1`` 1.2, ``b`` 0.75)
orders documents by score; equal scores keep document order.
"""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from typing import TypedDict

K1 = 1.2
B = 0.75

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
    """One searchable tool: its name and all text the ranker reads."""

    name: str
    text: str


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
    """Okapi BM25 names of the best matches, best first; ties keep document order."""

    query_terms = list(dict.fromkeys(tokenize(query)))
    if not query_terms or not documents or limit <= 0:
        return []
    term_counts = [_counts(tokenize(document["text"])) for document in documents]
    lengths = [sum(counts.values()) for counts in term_counts]
    total_length = sum(lengths)
    average_length = total_length / len(documents) if total_length else 1.0
    idf: dict[str, float] = {}
    for term in query_terms:
        frequency = sum(1 for counts in term_counts if term in counts)
        idf[term] = math.log(1 + (len(documents) - frequency + 0.5) / (frequency + 0.5))
    matches: list[tuple[str, float]] = []
    for index, document in enumerate(documents):
        score = 0.0
        norm = K1 * (1 - B + (B * lengths[index]) / average_length)
        for term in query_terms:
            count = term_counts[index].get(term)
            if not count:
                continue
            score += idf[term] * ((count * (K1 + 1)) / (count + norm))
        if score > 0:
            matches.append((document["name"], score))
    matches.sort(key=lambda match: -match[1])
    return [name for name, _score in matches[:limit]]


def _counts(terms: Sequence[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for term in terms:
        counts[term] = counts.get(term, 0) + 1
    return counts


__all__ = ["SearchDocument", "rank_documents", "tokenize"]

"""Mutable document drafts backed by strict-JSON copies.

A draft is an ordinary mutable tree with attribute access for object members and list helpers for
arrays. Every value placed into a draft is copied by value; at settlement the whole tree is copied
again and either adopted as the new immutable revision or discarded.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, TypeVar, cast

from ..json import JsonObject, copy_json

T = TypeVar("T")

_INTERNAL = frozenset(
    {
        "_check",
        "_data",
        "_draft",
        "_root",
        "_sealed",
        "_unwrap",
    }
)


class _DraftBase:
    __slots__ = ()

    _data: Any
    _draft: DocumentDraft

    def _check(self) -> None:
        self._draft._check_open()


class DraftObject(_DraftBase):
    """A mutable object member of a document draft."""

    __slots__ = ("_data", "_draft")

    def __init__(self, draft: DocumentDraft, data: dict[str, Any]) -> None:
        object.__setattr__(self, "_draft", draft)
        object.__setattr__(self, "_data", data)

    def _check(self) -> None:
        self._draft._check_open()

    def __getattr__(self, name: str) -> Any:
        self._check()
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            value = self._data[name]
        except KeyError:
            raise AttributeError(name) from None
        return self._draft._wrap(value)

    def __setattr__(self, name: str, value: Any) -> None:
        self._check()
        if name in _INTERNAL:
            object.__setattr__(self, name, value)
            return
        self._data[name] = copy_json(self._draft._plain(value))

    def __delattr__(self, name: str) -> None:
        self._check()
        self._data.pop(name, None)

    def __getitem__(self, key: str) -> Any:
        self._check()
        return self._draft._wrap(self._data[key])

    def __setitem__(self, key: str, value: Any) -> None:
        self._check()
        self._data[key] = copy_json(self._draft._plain(value))

    def __delitem__(self, key: str) -> None:
        self._check()
        del self._data[key]

    def __contains__(self, key: object) -> bool:
        self._check()
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        self._check()
        return iter(self._data)

    def __len__(self) -> int:
        self._check()
        return len(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        self._check()
        if key not in self._data:
            return default
        return self._draft._wrap(self._data[key])

    def keys(self) -> Iterable[str]:
        self._check()
        return self._data.keys()

    def values(self) -> Iterable[Any]:
        self._check()
        return (self._draft._wrap(value) for value in self._data.values())

    def items(self) -> Iterable[tuple[str, Any]]:
        self._check()
        return ((key, self._draft._wrap(value)) for key, value in self._data.items())

    def pop(self, key: str, default: Any = None) -> Any:
        self._check()
        return self._draft._wrap(self._data.pop(key, default))

    def update(self, values: Mapping[str, Any]) -> None:
        self._check()
        for key, value in values.items():
            self._data[key] = copy_json(self._draft._plain(value))

    def __repr__(self) -> str:
        return f"DraftObject({self._data!r})"


class DraftList(_DraftBase):
    """A mutable array member of a document draft."""

    __slots__ = ("_data", "_draft")

    def __init__(self, draft: DocumentDraft, data: list[Any]) -> None:
        object.__setattr__(self, "_draft", draft)
        object.__setattr__(self, "_data", data)

    def _check(self) -> None:
        self._draft._check_open()

    def _place(self, value: Any) -> Any:
        return copy_json(self._draft._plain(value))

    def __len__(self) -> int:
        self._check()
        return len(self._data)

    def __iter__(self) -> Iterator[Any]:
        self._check()
        return (self._draft._wrap(value) for value in list(self._data))

    def __getitem__(self, index: int | slice) -> Any:
        self._check()
        value = self._data[index]
        if isinstance(index, slice):
            return [self._draft._wrap(item) for item in value]
        return self._draft._wrap(value)

    def __setitem__(self, index: int | slice, value: Any) -> None:
        self._check()
        if isinstance(index, slice):
            assert isinstance(value, Iterable)
            self._data[index] = [self._place(item) for item in value]
        else:
            self._data[index] = self._place(value)

    def __delitem__(self, index: int | slice) -> None:
        self._check()
        del self._data[index]

    def append(self, value: Any) -> None:
        self._check()
        self._data.append(self._place(value))

    push = append

    def extend(self, values: Iterable[Any]) -> None:
        self._check()
        self._data.extend(self._place(value) for value in values)

    def insert(self, index: int, value: Any) -> None:
        self._check()
        self._data.insert(index, self._place(value))

    def pop(self, index: int = -1) -> Any:
        self._check()
        return self._draft._wrap(self._data.pop(index))

    def shift(self) -> Any:
        self._check()
        return self._draft._wrap(self._data.pop(0))

    def unshift(self, *values: Any) -> None:
        self._check()
        for offset, value in enumerate(values):
            self._data.insert(offset, self._place(value))

    def remove(self, value: Any) -> None:
        self._check()
        self._data.remove(self._draft._plain(value))

    def clear(self) -> None:
        self._check()
        self._data.clear()

    def reverse(self) -> None:
        self._check()
        self._data.reverse()

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        self._check()
        self._data.sort(key=key, reverse=reverse)

    def index(self, value: Any, start: int = 0, stop: int | None = None) -> int:
        self._check()
        if stop is None:
            return self._data.index(self._draft._plain(value), start)
        return self._data.index(self._draft._plain(value), start, stop)

    def count(self, value: Any) -> int:
        self._check()
        return self._data.count(self._draft._plain(value))

    def __contains__(self, value: object) -> bool:
        self._check()
        return self._draft._plain(value) in self._data

    def __eq__(self, other: object) -> bool:
        return self._data == (other._data if isinstance(other, DraftList) else other)

    def __repr__(self) -> str:
        return f"DraftList({self._data!r})"


class DocumentDraft:
    """Root draft for one document acquisition in a transaction."""

    __slots__ = ("_data", "_draft", "_sealed")

    def __init__(self, value: JsonObject) -> None:
        object.__setattr__(self, "_data", copy_json(value))
        object.__setattr__(self, "_draft", self)
        object.__setattr__(self, "_sealed", False)

    def _check_open(self) -> None:
        if self._sealed:
            raise RuntimeError("Transaction has settled")

    def _wrap(self, value: Any) -> Any:
        if isinstance(value, dict):
            return DraftObject(self, value)
        if isinstance(value, list):
            return DraftList(self, value)
        return value

    def _plain(self, value: Any) -> Any:
        if isinstance(value, DocumentDraft):
            return value._data
        if isinstance(value, (DraftObject, DraftList)):
            return value._data
        return value

    def __getattr__(self, name: str) -> Any:
        self._check_open()
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            value = self._data[name]
        except KeyError:
            raise AttributeError(name) from None
        return self._wrap(value)

    def __setattr__(self, name: str, value: Any) -> None:
        self._check_open()
        if name in _INTERNAL:
            object.__setattr__(self, name, value)
            return
        self._data[name] = copy_json(self._plain(value))

    def __delattr__(self, name: str) -> None:
        self._check_open()
        self._data.pop(name, None)

    def __getitem__(self, key: str) -> Any:
        self._check_open()
        return self._wrap(self._data[key])

    def __setitem__(self, key: str, value: Any) -> None:
        self._check_open()
        self._data[key] = copy_json(self._plain(value))

    def __delitem__(self, key: str) -> None:
        self._check_open()
        del self._data[key]

    def __contains__(self, key: object) -> bool:
        self._check_open()
        return key in self._data

    def __iter__(self) -> Iterator[str]:
        self._check_open()
        return iter(self._data)

    def __len__(self) -> int:
        self._check_open()
        return len(self._data)

    def __repr__(self) -> str:
        return f"DocumentDraft({self._data!r})"

    def _seal(self) -> None:
        object.__setattr__(self, "_sealed", True)

    def _candidate(self) -> JsonObject:
        """Return a detached strict-JSON candidate and seal the draft."""

        self._check_open()
        self._sealed = True
        return cast(JsonObject, copy_json(self._data))


__all__ = ["DocumentDraft", "DraftList", "DraftObject"]

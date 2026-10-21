"""Revocable, path-tracking drafts over a document's JSON value.

A draft is a live view of one transaction-local change. Assigning into it records a Chord
operation; the committed value becomes visible only after storage admits the commit.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, MutableMapping, MutableSequence
from typing import Any, overload

from .delta import Change, Op
from .json import JsonError, copy_json, ensure_json


def _unwrap(value: Any) -> Any:
    if isinstance(value, _DraftView):
        return value.to_json()
    return value


def _wrap(change: Change, path: tuple[str | int, ...]) -> Any:
    value = _value(change, path)
    if isinstance(value, dict):
        return _DraftMapping(change, path)
    if isinstance(value, list):
        return _DraftList(change, path)
    return value


def _value(change: Change, path: tuple[str | int, ...]) -> Any:
    current = change.value
    for segment in path:
        current = current[segment]
    return current


class _DraftView:
    """Shared behavior of one nested draft view."""

    __slots__ = ("_change", "_path")

    def __init__(self, change: Change, path: tuple[str | int, ...]) -> None:
        self._change = change
        self._path = path

    @property
    def ops(self) -> list[Op]:
        return [copy_json(op) for op in self._change.ops]

    def to_json(self) -> Any:
        return copy_json(_value(self._change, self._path))

    def __eq__(self, other: object) -> bool:
        from .json import json_equal

        return json_equal(self.to_json(), _unwrap(other))

    def __repr__(self) -> str:
        return f"Draft({self.to_json()!r})"


class _DraftMapping(_DraftView, MutableMapping):
    def __getitem__(self, key: str) -> Any:
        if not isinstance(key, str):
            raise TypeError("document object keys are strings")
        return _wrap(self._change, self._path + (key,))

    def __setitem__(self, key: str, value: Any) -> None:
        if not isinstance(key, str):
            raise TypeError("document object keys are strings")
        value = _unwrap(value)
        ensure_json(value)
        self._change.set(self._path + (key,), value)

    def __delitem__(self, key: str) -> None:
        self._change.delete(self._path + (key,))

    def __iter__(self) -> Iterator[str]:
        return iter(_value(self._change, self._path))

    def __len__(self) -> int:
        return len(_value(self._change, self._path))

    def __contains__(self, key: object) -> bool:
        return key in _value(self._change, self._path)


class _DraftList(_DraftView, MutableSequence):
    def _list(self) -> list[Any]:
        value = _value(self._change, self._path)
        if not isinstance(value, list):
            raise JsonError("draft view is no longer an array")
        return value

    @overload
    def __getitem__(self, index: int) -> Any: ...

    @overload
    def __getitem__(self, index: slice) -> list[Any]: ...

    def __getitem__(self, index: int | slice) -> Any:
        if isinstance(index, slice):
            return copy_json(self._list()[index])
        return _wrap(self._change, self._path + (index,))

    @overload
    def __setitem__(self, index: int, value: Any) -> None: ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[Any]) -> None: ...

    def __setitem__(self, index: int | slice, value: Any) -> None:
        if isinstance(index, slice):
            items = [_unwrap(item) for item in value]
            for item in items:
                ensure_json(item)
            start, stop, step = index.indices(len(self._list()))
            if step != 1:
                raise ValueError("extended slice assignment is not supported by document drafts")
            copied = copy_json(items)
            self._change.splice(self._path, start, stop - start, copied)
            return
        value = _unwrap(value)
        ensure_json(value)
        if index < 0:
            index += len(self._list())
        self._change.set(self._path + (index,), value)

    def __delitem__(self, index: int | slice) -> None:
        length = len(self._list())
        if isinstance(index, slice):
            start, stop, step = index.indices(length)
            if step != 1:
                raise ValueError("extended slice deletion is not supported by document drafts")
            self._change.splice(self._path, start, stop - start, [])
            return
        if index < 0:
            index += length
        if index < 0 or index >= length:
            raise IndexError("draft list index out of range")
        self._change.splice(self._path, index, 1, [])

    def __len__(self) -> int:
        return len(self._list())

    def __iter__(self) -> Iterator[Any]:
        for index in range(len(self._list())):
            yield _wrap(self._change, self._path + (index,))

    def insert(self, index: int, value: Any) -> None:
        value = _unwrap(value)
        ensure_json(value)
        length = len(self._list())
        if index < 0:
            index = max(0, length + index)
        index = min(index, length)
        self._change.splice(self._path, index, 0, [value])

    def append(self, value: Any) -> None:
        value = _unwrap(value)
        ensure_json(value)
        self._change.splice(self._path, len(self._list()), 0, [value])

    def extend(self, values: Iterable[Any]) -> None:
        items = [_unwrap(item) for item in values]
        for item in items:
            ensure_json(item)
        self._change.splice(self._path, len(self._list()), 0, items)

    def pop(self, index: int = -1) -> Any:
        length = len(self._list())
        if index < 0:
            index += length
        if index < 0 or index >= length:
            raise IndexError("draft list pop index out of range")
        value = copy_json(self._list()[index])
        self._change.splice(self._path, index, 1, [])
        return value

    def remove(self, value: Any) -> None:
        target = _unwrap(value)
        for index, item in enumerate(self._list()):
            if item == target:
                self._change.splice(self._path, index, 1, [])
                return
        raise ValueError(f"{value!r} is not in the draft list")

    def clear(self) -> None:
        self._change.splice(self._path, 0, len(self._list()), [])


class DocumentDraft(_DraftMapping):
    """Root draft returned by ``Tx.doc()``."""

    def __init__(self, change: Change) -> None:
        super().__init__(change, ())

    @property
    def changed(self) -> bool:
        return bool(self._change.ops)


def begin_draft(change: Change) -> DocumentDraft:
    return DocumentDraft(change)


__all__ = ["DocumentDraft", "begin_draft"]

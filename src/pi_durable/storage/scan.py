"""Cursor and order helpers shared by storage backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeGuard

from ..errors import StorageRequestError
from ..types import Cursor, ScanOrder


@dataclass(frozen=True, slots=True)
class ScanStart:
    """Where a built-in storage scan starts: its order and the last ID a previous page returned."""

    order: ScanOrder
    after: int | None


def _is_safe_int(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and abs(value) <= 9_007_199_254_740_991


def scan_start(
    requested: ScanOrder | None,
    cursor: Cursor | None,
    fallback: ScanOrder,
) -> ScanStart:
    """Resolve a scan's order and position, rejecting malformed and conflicting cursors."""

    if requested is not None and requested not in ("ascending", "descending"):
        raise StorageRequestError(f"Invalid scan order: {requested}")
    if cursor is None:
        return ScanStart(order=requested or fallback, after=None)
    after = cursor.get("after")
    if not _is_safe_int(after):
        raise StorageRequestError("Invalid storage cursor")
    stored = cursor.get("order")
    if stored is not None and stored not in ("ascending", "descending"):
        raise StorageRequestError("Invalid storage cursor")
    order = valid_order(stored) or fallback
    if requested is not None and requested != order:
        raise StorageRequestError(f"The cursor continues a {order} scan; the query asks for {requested}")
    return ScanStart(order=order, after=after)


def valid_order(value: Any) -> ScanOrder | None:
    return value if value in ("ascending", "descending") else None


def next_cursor(id: int, order: ScanOrder) -> dict[str, Any]:
    """Return the continuation of a scan whose last returned item has ``id``."""

    return {"after": id, "order": order}


__all__ = ["ScanStart", "next_cursor", "scan_start"]

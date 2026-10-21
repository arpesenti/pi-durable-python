"""Typed document definitions, logical addresses, and definition-free materialization."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from .delta import Op, Tracker
from .json import JsonObject, copy_json

Scope = Literal["session", "conversation", "task"]
History = Literal["latest", "rewindable"]
ForkPolicy = Literal["asOf", "current", "initial"]


class DocumentDefinitionError(TypeError):
    pass


@dataclass(frozen=True)
class CheckpointInfo:
    """Stored replay state supplied to a document's checkpoint predicate."""

    deltas_since_base: int


CheckpointWhen = Callable[[Any, Sequence[Op], CheckpointInfo], bool]
Initializer = Callable[..., JsonObject]
Migrator = Callable[[Mapping[str, Any], int], JsonObject]


@dataclass(frozen=True)
class DocDefinition:
    """A singleton or keyed-family document definition.

    ``scope`` directly determines ownership and lifetime. Conversation definitions must
    declare ``history`` and ``fork``; every definition supplies ``checkpoint_when``.
    """

    kind: str
    version: int
    scope: Scope
    initial: Initializer
    checkpoint_when: CheckpointWhen
    history: History | None = None
    fork: ForkPolicy | None = None
    migrate: Migrator | None = None
    family: bool = False

    def validate(self) -> None:
        if not isinstance(self.kind, str) or not self.kind:
            raise DocumentDefinitionError("Document kind must be a non-empty string")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise DocumentDefinitionError(f"Document {self.kind} version must be a positive integer")
        if not callable(self.initial) or not callable(self.checkpoint_when):
            raise DocumentDefinitionError(f"Document {self.kind} requires initial() and checkpoint_when()")
        if self.scope == "conversation":
            if self.history not in ("latest", "rewindable"):
                raise DocumentDefinitionError(f"Conversation document {self.kind} requires history")
            allowed = {"latest": ("current", "initial"), "rewindable": ("asOf", "current", "initial")}
            if self.fork not in allowed[self.history]:
                raise DocumentDefinitionError(
                    f"Conversation document {self.kind} cannot use fork={self.fork!r} with history={self.history!r}"
                )
        elif self.history is not None or self.fork is not None:
            raise DocumentDefinitionError(f"Document {self.kind} has {self.scope} scope and cannot declare history/fork")
        if self.family and self.scope == "session":
            # Session families are allowed; the constraint is only that initial takes a seed.
            pass


@dataclass(frozen=True)
class DocToken:
    """Typed token passed explicitly to document access."""

    definition: DocDefinition

    @property
    def kind(self) -> str:
        return self.definition.kind

    @property
    def family(self) -> bool:
        return self.definition.family


def define_doc(
    *,
    kind: str,
    version: int,
    scope: Scope,
    initial: Initializer,
    checkpoint_when: CheckpointWhen,
    history: History | None = None,
    fork: ForkPolicy | None = None,
    migrate: Migrator | None = None,
) -> DocToken:
    """Define a singleton document."""
    definition = DocDefinition(
        kind=kind,
        version=version,
        scope=scope,
        initial=initial,
        checkpoint_when=checkpoint_when,
        history=history,
        fork=fork,
        migrate=migrate,
        family=False,
    )
    definition.validate()
    return DocToken(definition)


def define_doc_family(
    *,
    kind: str,
    version: int,
    scope: Scope,
    initial: Initializer,
    checkpoint_when: CheckpointWhen,
    history: History | None = None,
    fork: ForkPolicy | None = None,
    migrate: Migrator | None = None,
) -> DocToken:
    """Define a keyed document family; ``initial(seed)`` runs only when a member is absent."""
    definition = DocDefinition(
        kind=kind,
        version=version,
        scope=scope,
        initial=initial,
        checkpoint_when=checkpoint_when,
        history=history,
        fork=fork,
        migrate=migrate,
        family=True,
    )
    definition.validate()
    return DocToken(definition)


@dataclass(frozen=True)
class ResolvedAddress:
    address: JsonObject
    id: str
    next_argument: int


def address_id(address: Mapping[str, Any]) -> str:
    """Stable string identity of one logical address."""
    scope = address["scope"]
    owner = None
    if scope["kind"] == "conversation":
        owner = scope["conversationId"]
    elif scope["kind"] == "task":
        owner = scope["taskId"]
    return json.dumps([address["kind"], scope["kind"], owner, address.get("key")], separators=(",", ":"))


def resolve_address(definition: DocDefinition, args: Sequence[Any]) -> ResolvedAddress:
    """Resolve variadic public arguments: owner ID (unless session), family key, seed."""
    index = 0
    scope: JsonObject
    if definition.scope == "session":
        scope = {"kind": "session"}
    elif definition.scope == "conversation":
        if index >= len(args):
            raise TypeError(f"Document {definition.kind} requires a conversation ID")
        scope = {"kind": "conversation", "conversationId": _owner_id(args[index], "conversation", definition)}
        index += 1
    else:
        if index >= len(args):
            raise TypeError(f"Document {definition.kind} requires a task ID")
        scope = {"kind": "task", "taskId": _owner_id(args[index], "task", definition)}
        index += 1
    key = None
    if definition.family:
        if index >= len(args):
            raise TypeError(f"Document family {definition.kind} requires a key")
        key = args[index]
        if not isinstance(key, str):
            raise TypeError(f"Document family {definition.kind} key must be a string")
        index += 1
    address: JsonObject = {"kind": definition.kind, "scope": scope}
    if key is not None:
        address["key"] = key
    return ResolvedAddress(address=address, id=address_id(address), next_argument=index)


def _owner_id(value: Any, what: str, definition: DocDefinition) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"Document {definition.kind} requires a {what} ID")
    return value


def document_create(definition: DocDefinition, address: Mapping[str, Any], document_id: int) -> JsonObject:
    """Build the storage creation record for a new incarnation."""
    record: JsonObject = {"id": document_id, "kind": address["kind"], "scope": copy_json(address["scope"])}
    if "key" in address:
        record["key"] = address["key"]
    if definition.scope == "conversation":
        record["history"] = definition.history
        record["fork"] = definition.fork
    return record


def check_record_scope(definition: DocDefinition, record: Mapping[str, Any]) -> None:
    """Reject typed access whose token disagrees with persisted lifetime semantics."""
    scope = record["scope"]["kind"]
    if scope != definition.scope:
        raise TypeError(
            f"Document {record.get('id')} ({record.get('kind')}) does not match the supplied definition scope"
        )
    if scope == "conversation" and (
        record.get("history") != definition.history or record.get("fork") != definition.fork
    ):
        raise TypeError(
            f"Document {record.get('id')} ({record.get('kind')}) does not match the supplied definition semantics"
        )


def check_record_version(definition: DocDefinition, record: Mapping[str, Any], version: int) -> None:
    if version > definition.version:
        raise ValueError(
            f"Document {record.get('id')} ({record.get('kind')}) has newer version {version} than {definition.version}"
        )
    if version < definition.version and definition.migrate is None:
        raise ValueError(
            f"Document {record.get('id')} ({record.get('kind')}) requires migration from version {version}"
        )


def materialize_document(definition: DocDefinition, stored: Mapping[str, Any]) -> JsonObject:
    return materialize_document_value(definition, stored["record"], stored["version"], stored["value"])


def materialize_document_value(
    definition: DocDefinition, record: Mapping[str, Any], version: int, value: Mapping[str, Any]
) -> JsonObject:
    check_record_scope(definition, record)
    check_record_version(definition, record, version)
    if version == definition.version:
        return copy_json(value)
    assert definition.migrate is not None
    migrated = definition.migrate(copy_json(value), version)
    if not isinstance(migrated, dict):
        raise DocumentDefinitionError(f"Document {definition.kind} migrate() must return a JSON object")
    return copy_json(migrated)


def is_alive_at(record: Mapping[str, Any], at: int | str) -> bool:
    """Membership is ``createdAt <= at < retiredAt``; ``current`` means unretired."""
    if at == "current":
        return record.get("retiredAt") is None
    return record["createdAt"] <= at and (
        record.get("retiredAt") is None or at < record["retiredAt"]
    )


def is_current_only(record: Mapping[str, Any]) -> bool:
    return record["scope"]["kind"] != "conversation" or record.get("history") == "latest"


__all__ = [
    "CheckpointInfo",
    "DocDefinition",
    "DocToken",
    "DocumentDefinitionError",
    "ResolvedAddress",
    "address_id",
    "check_record_scope",
    "check_record_version",
    "define_doc",
    "define_doc_family",
    "document_create",
    "is_alive_at",
    "is_current_only",
    "materialize_document",
    "materialize_document_value",
    "resolve_address",
]


@dataclass
class LoadedDocument:
    """One committed incarnation owned by the Session tracker cache."""

    address_id: str
    record: JsonObject
    stored_version: int
    value_version: int
    deltas_since_base: int
    tracker: Tracker


__all__ += ["LoadedDocument"]

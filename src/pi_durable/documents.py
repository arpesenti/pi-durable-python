"""Document definitions, typed tokens, and materialization.

A document definition is a token-safe value object. Persisted records keep the v1.3.0 camelCase
shape; Python callers configure definitions with snake_case names and may also pass a mapping.
"""

from __future__ import annotations

import functools
import inspect
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias, cast

from .delta import Op
from .ids import (
    ConversationId,
    DocumentId,
    TaskId,
)
from .json import JsonObject, copy_json
from .types import CheckpointInfo, DocumentAddress, DocumentCreate, DocumentRecord, StoredDocument

DocScope: TypeAlias = Literal["session", "conversation", "task"]

#: Positional arities of the definition callbacks without the optional receiver.
_SINGLETON_INITIAL_ARITY = 0
_FAMILY_INITIAL_ARITY = 1
_MIGRATE_ARITY = 2
_CHECKPOINT_ARITY = 3


@dataclass(frozen=True, slots=True)
class DocDefinition:
    """Erased document definition used by the Session after token resolution."""

    kind: str
    version: int
    scope: DocScope
    initial: Callable[..., JsonObject]
    family: bool = False
    history: Literal["latest", "rewindable"] | None = None
    fork: Literal["asOf", "current", "initial"] | None = None
    migrate: Callable[[JsonObject, int], JsonObject] | None = None
    checkpoint_when: Callable[[JsonObject, list[Op], CheckpointInfo], bool] | None = None

    def __getitem__(self, key: str) -> Any:
        if key == "checkpointWhen":
            return self.checkpoint_when
        return getattr(self, key)


@dataclass(frozen=True, slots=True)
class DocToken:
    """Typed singleton document token passed explicitly to Session access."""

    definition: DocDefinition


@dataclass(frozen=True, slots=True)
class DocFamilyToken:
    """Typed keyed document-family token passed explicitly to Session access."""

    definition: DocDefinition


AnyDocToken: TypeAlias = DocToken | DocFamilyToken

# Scope-specific token names from the v1.3.0 public surface. Python resolves the scope at runtime
# from the definition, so these are the same erased token classes.
SessionDocToken: TypeAlias = DocToken
ConversationDocToken: TypeAlias = DocToken
RewindableConversationDocToken: TypeAlias = DocToken
TaskDocToken: TypeAlias = DocToken
SessionDocFamilyToken: TypeAlias = DocFamilyToken
ConversationDocFamilyToken: TypeAlias = DocFamilyToken
RewindableConversationDocFamilyToken: TypeAlias = DocFamilyToken
TaskDocFamilyToken: TypeAlias = DocFamilyToken

#: Erased definition shapes used by callers that do not care about family/singleton tokens.
CommonDocDefinition: TypeAlias = DocDefinition
DocFamilyDefinition: TypeAlias = DocDefinition


def _definition_values(definition: Mapping[str, Any] | None, kwargs: Mapping[str, Any]) -> dict[str, Any]:
    values = dict(definition or {})
    values.update(kwargs)
    aliases = {
        "checkpointWhen": "checkpoint_when",
        "initial": "initial",
    }
    for source, target in aliases.items():
        if source in values and target not in values:
            values[target] = values.pop(source)
    return values


def _bind_receiver(
    callback: Callable[..., Any] | None,
    definition: DocDefinition,
    arity: int,
) -> Callable[..., Any] | None:
    """Bind a definition callback that declares the definition as its receiver.

    v1.3.0 invokes `initial`, `migrate`, and `checkpointWhen` as methods, so `this` is the
    definition. Python callables are plain functions; one whose first parameter is named `self`
    or `definition` and that declares exactly one extra positional parameter receives the
    definition bound to it.
    """

    if callback is None:
        return None
    try:
        parameters = list(inspect.signature(callback).parameters.values())
    except (TypeError, ValueError):
        return callback
    if any(parameter.kind is inspect.Parameter.VAR_POSITIONAL for parameter in parameters):
        return callback
    positional = [
        parameter
        for parameter in parameters
        if parameter.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) != arity + 1:
        return callback
    if positional[0].name not in ("self", "definition"):
        return callback
    return functools.partial(callback, definition)


def _build_definition(values: Mapping[str, Any], *, family: bool) -> DocDefinition:
    kind = values.get("kind")
    if not isinstance(kind, str):
        raise TypeError("Document kind must be a string")
    version = values.get("version")
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise TypeError(f"Document {kind} version must be a positive integer")
    scope = values.get("scope")
    if scope not in ("session", "conversation", "task"):
        raise TypeError(f"Document {kind} scope must be session, conversation, or task")
    initial = values.get("initial")
    if not callable(initial):
        raise TypeError(f"Document {kind} requires an initial() function")
    history = values.get("history")
    fork = values.get("fork")
    if scope == "conversation":
        if history not in ("latest", "rewindable"):
            raise TypeError(f"Conversation document {kind} requires history 'latest' or 'rewindable'")
        if fork not in ("asOf", "current", "initial"):
            raise TypeError(f"Conversation document {kind} requires a fork policy")
    elif history is not None or fork is not None:
        raise TypeError(f"Document {kind} history and fork apply only to conversation scope")
    migrate = values.get("migrate")
    if migrate is not None and not callable(migrate):
        raise TypeError(f"Document {kind} migrate must be callable")
    checkpoint_when = values.get("checkpoint_when")
    if checkpoint_when is not None and not callable(checkpoint_when):
        raise TypeError(f"Document {kind} checkpoint_when must be callable")
    definition = DocDefinition(
        kind=kind,
        version=version,
        scope=scope,
        initial=initial,
        family=family,
        history=history,
        fork=fork,
        migrate=migrate,
        checkpoint_when=checkpoint_when,
    )
    # Bind definition callbacks that declare the definition as their receiver, after constructing
    # the final value so the receiver is the token's own definition.
    object.__setattr__(
        definition,
        "initial",
        _bind_receiver(
            initial, definition, _FAMILY_INITIAL_ARITY if family else _SINGLETON_INITIAL_ARITY
        ),
    )
    object.__setattr__(
        definition, "migrate", _bind_receiver(migrate, definition, _MIGRATE_ARITY)
    )
    object.__setattr__(
        definition,
        "checkpoint_when",
        _bind_receiver(checkpoint_when, definition, _CHECKPOINT_ARITY),
    )
    return definition


def define_doc(
    definition: Mapping[str, Any] | None = None,
    /,
    **kwargs: Any,
) -> DocToken:
    """Define a Session-, conversation-, or task-scoped singleton document."""

    return DocToken(_build_definition(_definition_values(definition, kwargs), family=False))


def define_doc_family(
    definition: Mapping[str, Any] | None = None,
    /,
    **kwargs: Any,
) -> DocFamilyToken:
    """Define a keyed Session-, conversation-, or task-scoped document family."""

    values = _definition_values(definition, kwargs)
    values.pop("family", None)
    return DocFamilyToken(_build_definition(values, family=True))


@dataclass(frozen=True, slots=True)
class ResolvedAddress:
    """Logical address plus its stable cache identity and consumed argument count."""

    address: DocumentAddress
    id: str
    next_argument: int


def _owner_id(value: object, definition: DocDefinition, scope: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise TypeError(f"Document {definition.kind} requires a {scope} ID")
    return value


def resolve_address(definition: DocDefinition, args: tuple[Any, ...] | list[Any]) -> ResolvedAddress:
    """Resolve an overloaded argument list and return the index after owner and family key."""

    index = 0
    if definition.scope == "session":
        scope: dict[str, Any] = {"kind": "session"}
    elif definition.scope == "conversation":
        scope = {
            "kind": "conversation",
            "conversationId": cast(ConversationId, _owner_id(args[index], definition, "conversation")),
        }
        index += 1
    else:
        scope = {"kind": "task", "taskId": cast(TaskId, _owner_id(args[index], definition, "task"))}
        index += 1
    key: str | None = None
    if definition.family:
        if index >= len(args) or not isinstance(args[index], str):
            raise TypeError(f"Document family {definition.kind} requires a string key")
        key = args[index]
        index += 1
    address: DocumentAddress = {"kind": definition.kind, "scope": cast(Any, scope)}
    if key is not None:
        address["key"] = key
    return ResolvedAddress(address=address, id=address_id(address), next_argument=index)


def address_id(address: DocumentAddress) -> str:
    """Return the stable string identity of one logical address."""

    scope = address["scope"]
    owner = None if scope["kind"] == "session" else scope.get("conversationId", scope.get("taskId"))
    return json.dumps(
        [address["kind"], scope["kind"], owner, address.get("key")],
        separators=(",", ":"),
    )


def document_create(
    definition: DocDefinition, address: DocumentAddress, document_id: DocumentId
) -> DocumentCreate:
    """Build the storage create record for a new incarnation at an address."""

    record: dict[str, Any] = {
        "id": document_id,
        "kind": address["kind"],
        "scope": copy_json(address["scope"]),
    }
    if "key" in address:
        record["key"] = address["key"]
    if address["scope"]["kind"] == "conversation":
        record["history"] = definition.history
        record["fork"] = definition.fork
    return cast(DocumentCreate, record)


def check_record_scope(definition: DocDefinition, record: DocumentCreate | DocumentRecord) -> None:
    """Reject typed access whose token disagrees with the persisted scope and semantics."""

    scope = record["scope"]
    if scope["kind"] != definition.scope:
        raise TypeError(
            f"Document {record['id']} ({record['kind']}) does not match the supplied definition semantics"
        )
    if scope["kind"] == "conversation":
        if record.get("history") != definition.history or record.get("fork") != definition.fork:
            raise TypeError(
                f"Document {record['id']} ({record['kind']}) does not match the supplied definition semantics"
            )


def check_record_version(
    definition: DocDefinition,
    record: DocumentCreate | DocumentRecord,
    version: int,
) -> None:
    """Reject typed access to a stored version the supplied definition cannot use."""

    if version > definition.version:
        raise ValueError(
            f"Document {record['id']} ({record['kind']}) has newer version {version} "
            f"than {definition.version}"
        )
    if version < definition.version and definition.migrate is None:
        raise ValueError(
            f"Document {record['id']} ({record['kind']}) requires migration from version {version}"
        )


def materialize_document_value(
    definition: DocDefinition,
    record: DocumentCreate | DocumentRecord,
    version: int,
    value: JsonObject,
) -> JsonObject:
    """Validate and materialize one detached value for typed access."""

    check_record_scope(definition, record)
    check_record_version(definition, record, version)
    if version == definition.version:
        return value
    assert definition.migrate is not None
    return cast(JsonObject, copy_json(definition.migrate(cast(JsonObject, copy_json(value)), version)))


def materialize_document(definition: DocDefinition, stored: StoredDocument) -> JsonObject:
    """Validate and materialize one detached stored document."""

    return materialize_document_value(definition, stored["record"], stored["version"], stored["value"])


__all__ = [
    "AnyDocToken",
    "CommonDocDefinition",
    "ConversationDocFamilyToken",
    "ConversationDocToken",
    "DocDefinition",
    "DocFamilyDefinition",
    "DocFamilyToken",
    "DocScope",
    "DocToken",
    "ResolvedAddress",
    "RewindableConversationDocFamilyToken",
    "RewindableConversationDocToken",
    "SessionDocFamilyToken",
    "SessionDocToken",
    "TaskDocFamilyToken",
    "TaskDocToken",
    "address_id",
    "check_record_scope",
    "check_record_version",
    "define_doc",
    "define_doc_family",
    "document_create",
    "materialize_document",
    "materialize_document_value",
    "resolve_address",
]

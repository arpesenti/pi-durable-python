"""Errors of the durable kernel and harness."""

from __future__ import annotations

from typing import Any


class ReadAfterWrite(RuntimeError):
    """A transaction read a table after its first table write. Read every required row before writing."""

    def __init__(self, method: str) -> None:
        super().__init__(f"Tx.{method}() cannot read tables after the first table write")
        self.name = "ReadAfterWrite"


class StorageRequestError(RuntimeError):
    """A storage read rejected an invalid request with no durable effect."""


class SessionFailed(RuntimeError):
    """The Session failed after a storage error; close and reopen it."""

    def __init__(self, cause: Any) -> None:
        super().__init__("Session failed after a storage error; close and reopen it")
        self.cause = cause


class ConversationBusy(RuntimeError):
    """A submission reached a busy conversation and was not admitted."""

    def __init__(self, conversation_id: int) -> None:
        super().__init__(f"Conversation {conversation_id} is busy")
        self.conversation_id = conversation_id


class SessionClosed(RuntimeError):
    """A handle was used after its Session began closing."""


class InvocationEnded(RuntimeError):
    """A task or tool invocation was used after it ended."""


class TaskFaulted(RuntimeError):
    """A task phase threw or made no durable progress."""


class DocumentAbsent(LookupError):
    """A document address is absent where the operation required one."""


__all__ = [
    "ConversationBusy",
    "DocumentAbsent",
    "InvocationEnded",
    "ReadAfterWrite",
    "SessionClosed",
    "SessionFailed",
    "StorageRequestError",
    "TaskFaulted",
]

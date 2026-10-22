"""Public errors of the durable kernel and harness."""

from __future__ import annotations

from typing import Any

from .ids import ConversationId


def error_message(error: object) -> str:
    """Return a message for any thrown value without throwing while formatting it."""

    if isinstance(error, BaseException):
        return str(error)
    try:
        return str(error)
    except Exception:
        return object.__repr__(error)


def is_failure(error: BaseException) -> bool:
    """Whether a thrown value is a failure of the work, rather than its teardown.

    Section 10.1 ends the Session on what the work throws. Cancellation and the interpreter's
    teardown signals (``CancelledError``, ``GeneratorExit``, ``SystemExit``, ``KeyboardInterrupt``)
    derive from ``BaseException`` alone: they unwind the work that is running and must keep
    unwinding, so they never end the Session and never become a task's fault.
    """

    return isinstance(error, Exception)


class _NamedError(Exception):
    """An error whose ``name`` matches its class, mirroring JavaScript ``Error.name``."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.name = type(self).__name__


class ReadAfterWrite(_NamedError):
    """A transaction read a table after its first table write.

    Read every required row before writing.
    """

    def __init__(self, method: str) -> None:
        super().__init__(f"Tx.{method}() cannot read tables after the first table write")


class StorageRequestError(_NamedError):
    """A Storage read rejected an invalid request, such as an unknown conversation or a foreign cursor.

    It has no durable effect. Unlike any other error a Storage throws, it fails only that read,
    not the Session; from ``commit()`` it is fatal like any other error.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class SessionFailed(_NamedError):
    """The Session failed, so nothing runs or commits any more.

    It fails when a Storage method throws, a commit cannot be adopted, or the Harness's own
    bookkeeping throws. ``cause`` is that first error. Reopen the Session; reopening recovers from
    what was committed.
    """

    def __init__(self, cause: object) -> None:
        super().__init__("Session failed after a storage error; close and reopen it")
        self.cause: Any = cause
        if isinstance(cause, BaseException):
            self.__cause__ = cause


class ConversationBusy(_NamedError):
    """A submission reached a busy conversation and was not admitted."""

    def __init__(self, conversation_id: ConversationId) -> None:
        super().__init__(f"Conversation {conversation_id} is busy")
        self.conversation_id = conversation_id


__all__ = [
    "ConversationBusy",
    "ReadAfterWrite",
    "SessionFailed",
    "StorageRequestError",
    "error_message",
]

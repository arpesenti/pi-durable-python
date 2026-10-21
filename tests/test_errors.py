import pytest

from pi_durable.errors import (
    ConversationBusy,
    ReadAfterWrite,
    SessionFailed,
    StorageRequestError,
    error_message,
)
from pi_durable.ids import ROOT_CONVERSATION_ID


def test_read_after_write_names_the_method() -> None:
    error = ReadAfterWrite("entry")
    assert str(error) == "Tx.entry() cannot read tables after the first table write"
    assert error.name == "ReadAfterWrite"


def test_storage_request_error_is_a_plain_message() -> None:
    error = StorageRequestError("unknown conversation")
    assert str(error) == "unknown conversation"
    assert error.name == "StorageRequestError"


def test_session_failed_keeps_and_chains_its_cause() -> None:
    cause = RuntimeError("disk failed")
    error = SessionFailed(cause)
    assert str(error) == "Session failed after a storage error; close and reopen it"
    assert error.cause is cause
    assert error.__cause__ is cause
    assert error.name == "SessionFailed"


def test_session_failed_accepts_non_exception_causes() -> None:
    error = SessionFailed("failure")
    assert error.cause == "failure"
    assert error.__cause__ is None


def test_conversation_busy_carries_the_conversation() -> None:
    error = ConversationBusy(ROOT_CONVERSATION_ID)
    assert str(error) == "Conversation 1 is busy"
    assert error.conversation_id == ROOT_CONVERSATION_ID
    assert error.name == "ConversationBusy"


def test_error_message_formats_values_without_raising() -> None:
    class BadString:
        def __str__(self) -> str:
            raise RuntimeError("cannot format")

    assert error_message(RuntimeError("boom")) == "boom"
    assert error_message("boom") == "boom"
    assert "BadString" in error_message(BadString())


def test_errors_are_exceptions() -> None:
    for error in [
        ReadAfterWrite("entry"),
        StorageRequestError("bad"),
        SessionFailed("cause"),
        ConversationBusy(ROOT_CONVERSATION_ID),
    ]:
        assert isinstance(error, Exception)
        with pytest.raises(type(error)):
            raise error

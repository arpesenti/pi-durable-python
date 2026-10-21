"""Python port of ``@earendil-works/pi-durable`` v1.3.0.

The public surface is being rebuilt against the vendored 1.3.0 specification. This value layer
provides strict JSON copies, branded numeric IDs, public errors, entry tokens, the minimal
Context/abort signal, and the Storage protocol; Session and Harness arrive in later tickets.
"""

from .entries import (
    AssistantEntry,
    CompactionEntry,
    Entry,
    ResetEntry,
    SystemEntry,
    ToolResultEntry,
    UserEntry,
    define_entry,
)
from .errors import ConversationBusy, ReadAfterWrite, SessionFailed, StorageRequestError

__version__ = "1.3.0"

__all__ = [
    "AssistantEntry",
    "CompactionEntry",
    "ConversationBusy",
    "Entry",
    "ReadAfterWrite",
    "ResetEntry",
    "SessionFailed",
    "StorageRequestError",
    "SystemEntry",
    "ToolResultEntry",
    "UserEntry",
    "__version__",
    "define_entry",
]

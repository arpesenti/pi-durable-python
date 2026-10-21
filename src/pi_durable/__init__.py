"""Python port of ``@earendil-works/pi-durable`` v1.3.0.

The public surface is rebuilt against the vendored 1.3.0 specification. This layer provides strict
JSON copies, branded numeric IDs, public errors, entry tokens, the minimal Context/abort signal,
the Storage protocol, the Session kernel, document definitions, task definitions, and the
in-memory reference backend. The Harness arrives in later tickets.
"""

from .documents import (
    CommonDocDefinition,
    DocDefinition,
    DocFamilyDefinition,
    DocFamilyToken,
    DocToken,
    define_doc,
    define_doc_family,
)
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
from .session.session import DocumentState, SessionImpl, create_session
from .storage.memory import MemoryStorage, PreparedMemoryCommit
from .tasks import Task, TaskDefinition, define_task
from .types import Session, Tx

__version__ = "1.3.0"

__all__ = [
    "AssistantEntry",
    "CompactionEntry",
    "CommonDocDefinition",
    "ConversationBusy",
    "DocDefinition",
    "DocFamilyDefinition",
    "DocFamilyToken",
    "DocToken",
    "DocumentState",
    "Entry",
    "MemoryStorage",
    "PreparedMemoryCommit",
    "ReadAfterWrite",
    "ResetEntry",
    "Session",
    "SessionFailed",
    "SessionImpl",
    "StorageRequestError",
    "SystemEntry",
    "Task",
    "TaskDefinition",
    "Tx",
    "ToolResultEntry",
    "UserEntry",
    "__version__",
    "create_session",
    "define_doc",
    "define_doc_family",
    "define_entry",
    "define_task",
]

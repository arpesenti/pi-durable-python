"""Python port of ``@earendil-works/pi-durable`` v1.3.0.

The public surface is rebuilt against the vendored 1.3.0 specification. This layer provides strict
JSON copies, branded numeric IDs, public errors, entry tokens, the minimal Context/abort signal,
the Storage protocol, the Session kernel, document definitions, task definitions, context
derivation, and the in-memory reference backend. The full Harness arrives in later tickets.
"""

from .documents import (
    CommonDocDefinition,
    ConversationDocFamilyToken,
    ConversationDocToken,
    DocDefinition,
    DocFamilyDefinition,
    DocFamilyToken,
    DocToken,
    RewindableConversationDocFamilyToken,
    RewindableConversationDocToken,
    SessionDocFamilyToken,
    SessionDocToken,
    TaskDocFamilyToken,
    TaskDocToken,
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
from .harness.types import ContextView
from .session.session import DocumentState, SessionImpl, create_session
from .storage.memory import MemoryStorage, PreparedMemoryCommit
from .tasks import Task, TaskDefinition, define_task
from .types import Session, Tx

__version__ = "1.3.0"

__all__ = [
    "AssistantEntry",
    "CompactionEntry",
    "CommonDocDefinition",
    "ContextView",
    "ConversationBusy",
    "ConversationDocFamilyToken",
    "ConversationDocToken",
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
    "RewindableConversationDocFamilyToken",
    "RewindableConversationDocToken",
    "Session",
    "SessionDocFamilyToken",
    "SessionDocToken",
    "SessionFailed",
    "SessionImpl",
    "StorageRequestError",
    "SystemEntry",
    "Task",
    "TaskDefinition",
    "TaskDocFamilyToken",
    "TaskDocToken",
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

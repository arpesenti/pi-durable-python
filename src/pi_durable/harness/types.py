"""Public Harness types.

The Harness layer grows in later tickets. This module holds the value shapes the Session produces
for it and the scheduler's registry, agent, settings, and task-runtime contracts.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Never, NotRequired, Protocol, TypeAlias, TypedDict, TypeVar

from ..context import AbortSignal, Context
from ..documents import (
    AnyDocToken,
    CommonDocDefinition,
    ConversationDocFamilyToken,
    ConversationDocToken,
    DocFamilyToken,
    DocToken,
    RewindableConversationDocFamilyToken,
    RewindableConversationDocToken,
    SessionDocFamilyToken,
    SessionDocToken,
    TaskDocFamilyToken,
    TaskDocToken,
)
from ..ids import ConversationId, EntryId, SubmissionId, TaskId
from ..json import JsonObject, JsonValue
from ..types import (
    ConversationOwnership,
    Cursor,
    DocumentReader,
    EntryDraft,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    InputDoneRecord,
    InputUnansweredRecord,
    MessageRecord,
    NextTaskState,
    Page,
    SubmissionRecord,
    TaskOutcome,
    TaskRecord,
    TaskState,
    WatchHandle,
    WriteDoneRecord,
    WriteUnansweredRecord,
)

if TYPE_CHECKING:
    from ..session.transaction import Transaction


class ContextView(TypedDict):
    """One conversation's committed transcript and the model context derived from it.

    ``head`` is the newest visible head marker, if any. ``entries`` is the raw active transcript,
    ``contributions`` holds each of their model messages after edits and excluded stop reasons, and
    ``messages`` is the provider request context: tool results ordered after their call, missing
    ones synthesized, and the baseline system message first.
    """

    head: EntryWithHead | None
    entries: list[EntryRecord]
    contributions: list[list[MessageRecord]]
    messages: list[MessageRecord]


# ── Registry and extensions ─────────────────────────────────────────────────

ThinkingLevel = Literal["off", "minimal", "low", "medium", "high", "xhigh", "max"]


ToolExecutionMode = Literal["parallel", "sequential"]


class ToolControl(TypedDict, total=False):
    """What a tool result asks the tool round to do."""

    addTools: list[str]
    terminate: Literal[True]
    handoff: str


class ToolDiagnostic(TypedDict, total=False):
    """Remark about a call for the model and the UI; never part of the tool's data."""

    severity: Literal["info", "warn", "error"]
    message: str
    code: str


class ToolExecutionResult(TypedDict, total=False):
    """Three channels: ``output`` for the model, ``structuredOutput`` for programs, ``details`` for UIs."""

    output: list[JsonValue]
    structuredOutput: JsonValue
    isError: bool
    details: JsonValue
    diagnostics: list[ToolDiagnostic]
    usage: dict[str, JsonValue]
    control: ToolControl


class NestedToolExecutionResult(TypedDict, total=False):
    """A nested call's result, for programs: no output for the model, and ``control`` does not apply."""

    taskId: TaskId
    structuredOutput: JsonValue
    isError: bool
    details: JsonValue
    diagnostics: list[ToolDiagnostic]
    usage: dict[str, JsonValue]
    durationMs: int


class ToolHookCall(TypedDict, total=False):
    """A tool call as hooks see it; a nested call adds ``parent``."""

    type: Literal["toolCall"]
    id: str
    name: str
    arguments: dict[str, JsonValue]
    parent: dict[str, Any]


class ToolHooks(TypedDict, total=False):
    """Hooks of the built-in tool task."""

    beforeTool: Callable[..., Any]
    afterTool: Callable[..., Any]


class ToolExecutionApi(Protocol):
    """What a running tool may use: bounded output, durable commits, and Harness reads."""

    task_id: TaskId
    conversation_id: ConversationId
    call_id: str
    registry: RegistrySnapshot
    models: Any
    env: Any
    output_window: Mapping[str, Any] | None

    def agent(self, context: Context) -> Awaitable[Agent]: ...

    def output(self, chunk: str | bytes, skipped: Mapping[str, Any] | None = None) -> None: ...

    def retained_output(self) -> Mapping[str, Any]: ...

    def diagnostic(self, diagnostic: Mapping[str, Any]) -> None: ...

    def details(self, value: JsonValue, context: Context) -> Awaitable[None]: ...

    def commit(self, change: Callable[..., Any], context: Context) -> Awaitable[Any]: ...

    def memo(self, name: str, *args: Any) -> Awaitable[JsonValue | None]: ...

    def create_task(
        self, task: Any, input: JsonValue, options: Mapping[str, Any], context: Context
    ) -> Awaitable[TaskId]: ...

    def get_task(self, id: TaskId, context: Context) -> Awaitable[TaskRecord | None]: ...

    def wait_for_task(self, id: TaskId, context: Context) -> Awaitable[SettledTask]: ...

    def conversation(self, id: ConversationId, context: Context) -> Awaitable[Any]: ...

    def snapshot(self, token: Any, *args: Any) -> Awaitable[JsonObject | None]: ...

    def snapshot_as_of(self, token: Any, *args: Any) -> Awaitable[JsonObject | None]: ...

    def watch_doc(self, token: Any, *args: Any) -> Awaitable[Any]: ...

    def execute_tool(
        self, name: str, args: Mapping[str, Any], context: Context, options: Mapping[str, Any] | None = None
    ) -> Awaitable[NestedToolExecutionResult]: ...


@dataclass(frozen=True, slots=True)
class ToolRegistration:
    """Executable tool registered in a registry, with the fields resolution and planning read."""

    name: str
    description: str = ""
    parameters: Any = None
    execute: Callable[..., Any] | None = None
    replay: Literal["safe", "unsafe"] | None = None
    execution_mode: ToolExecutionMode | None = None
    structured_output_schema: Any = None
    callers: Sequence[Literal["model", "tools"]] | None = None
    deferred: bool = False
    output_limits: Mapping[str, Any] | None = None
    constrained_sampling: Any = None
    prepare_arguments: Callable[..., Any] | None = None


@dataclass(frozen=True, slots=True)
class PromptSection:
    """One system prompt section."""

    key: str
    render: Callable[..., Any]
    tag: bool | None = None


HooksOf: TypeAlias = Any
"""Hook handler map a task definition declares; erased to ``Any`` in Python's runtime typing."""


@dataclass(frozen=True, slots=True)
class HookRegistration:
    """Hook handlers matching tasks by name."""

    task: str
    handlers: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Wrap:
    """A pure wrapper of one tool or section name."""

    tool: str | None = None
    section: str | None = None
    wrap: Callable[..., Any] | None = None


@dataclass(frozen=True, slots=True)
class Extension:
    """Named bundle of code installed in a registry and selected by conversations."""

    name: str
    tools: Sequence[ToolRegistration] = ()
    sections: Sequence[PromptSection] = ()
    hooks: Sequence[HookRegistration] = ()
    wraps: Sequence[Wrap] = ()
    tasks: Sequence[Any] = ()


class RegistrySnapshot(Protocol):
    """Immutable view of one published registry state."""

    def installed(self) -> Sequence[Extension]: ...

    def extension(self, name: str) -> Extension | None: ...

    def tools(self) -> Sequence[Mapping[str, Any]]: ...

    def sections(self) -> Sequence[Mapping[str, Any]]: ...

    def tasks(self) -> Sequence[Any]: ...

    def task(self, name: str) -> Any | None: ...


class RegistryReader(Protocol):
    """Read side of a registry consumed by a Harness."""

    def snapshot(self) -> RegistrySnapshot: ...

    def subscribe(self, listener: Callable[[], Any]) -> Callable[[], None]: ...


class Registry(RegistryReader, Protocol):
    """Application-owned registry of extensions."""

    def install(self, extension: Extension) -> None: ...

    def uninstall(self, extension: Extension) -> None: ...


# ── Agent and settings ──────────────────────────────────────────────────────


class ModelRef(TypedDict):
    provider: str
    modelId: str


class AgentState(TypedDict, total=False):
    """Stored choices of one conversation; names, not objects."""

    model: ModelRef
    thinkingLevel: ThinkingLevel
    extensions: list[str] | dict[str, list[str]]
    tools: list[str] | dict[str, list[str]]
    modelTools: list[str] | dict[str, list[str]]
    instructions: str
    cwd: str


class AgentChange(TypedDict, total=False):
    """A change to ``pi.agent``: a given field replaces the stored one, ``None`` clears it."""

    model: ModelRef | None
    thinkingLevel: ThinkingLevel | None
    extensions: Any
    tools: Any
    modelTools: Any
    instructions: str | None
    cwd: str | None


@dataclass(slots=True)
class Agent:
    """A conversation's agent resolved against a registry snapshot and the settings."""

    model: ModelRef | None = None
    thinking_level: ThinkingLevel = "off"
    extensions: Sequence[Extension] = ()
    tools: Sequence[ToolRegistration] = ()
    callable: Sequence[ToolRegistration] = ()
    deferred: Sequence[ToolRegistration] = ()
    sections: Sequence[PromptSection] = ()
    instructions: str | None = None
    cwd: str | None = None


@dataclass(frozen=True, slots=True)
class PromptInput:
    """Input to system prompt section rendering for one request preparation."""

    conversation_id: ConversationId
    agent: Agent
    env: Any
    shown: Mapping[str, str]
    read: DocumentReader


class ConversationStreamOptions(TypedDict, total=False):
    """Curated pi-ai request options; absent fields use the provider defaults."""

    transport: str
    timeout_ms: int
    max_retries: int
    max_retry_delay_ms: int
    headers: dict[str, str]
    metadata: JsonObject
    cache_retention: str
    deferred: bool | dict[str, str]


class ConversationRetryPolicy(TypedDict, total=False):
    """Durable generation attempt retries."""

    enabled: bool
    max_retries: int
    base_delay_ms: int
    max_agent_delay_ms: int


class CompactionPolicy(TypedDict, total=False):
    """Automatic compaction thresholds; manual compaction ignores ``enabled``."""

    enabled: bool
    reserve_tokens: int
    keep_recent_tokens: int
    background_tokens: int


CompactionReason = Literal["manual", "threshold", "overflow"]
"""What started a compaction task."""


class CompactionResult(TypedDict, total=False):
    """A blocking summary's ``entryId``, or a conversation-owned summary's ``submissionId``."""

    entryId: EntryId
    submissionId: SubmissionId


class ProgressPolicy(TypedDict, total=False):
    """How often running progress is committed."""

    partial_interval_ms: int
    output_interval_ms: int


class GenerationHooks(TypedDict, total=False):
    """Hooks of the built-in generation task."""

    beforeRequest: Callable[..., Any]
    afterResponse: Callable[..., Any]
    onYield: Callable[..., Any]
    afterTools: Callable[..., Any]


class CompactionHooks(TypedDict, total=False):
    """Hooks of the built-in compaction task."""

    beforeCompact: Callable[..., Any]


class EnvTarget(TypedDict):
    """What ``HarnessOptions.env`` builds an environment for."""

    conversation_id: ConversationId
    cwd: NotRequired[str]
    read: DocumentReader


class HookApi(Protocol):
    """What a hook may use: committed reads, the task ID, and the task's memos."""

    task_id: TaskId
    conversation_id: ConversationId
    models: Any

    async def snapshot(self, token: AnyDocToken, *args: Any) -> JsonObject | None: ...

    async def snapshot_as_of(self, token: AnyDocToken, *args: Any) -> JsonObject | None: ...

    async def memo(self, name: str, *args: Any) -> JsonValue | None: ...


HookResult = Any
"""A hook's value, absent when it declines, or an awaitable of it."""

ConversationInit = Callable[..., Any]
"""Runs inside the creating commit after the creation hook and the agent change."""


class ConversationCreateOptions(TypedDict):
    """Options of ``createConversation`` and ``fork``."""

    ownership: ConversationOwnership
    agent: NotRequired[AgentChange]
    init: NotRequired[ConversationInit]


class HarnessSettings(TypedDict, total=False):
    """Harness-wide run policy, read at every resolution and never copied."""

    extensions: Sequence[Extension]
    stream: ConversationStreamOptions
    retry: ConversationRetryPolicy
    compaction: CompactionPolicy
    progress: ProgressPolicy
    tool_execution: Literal["parallel", "sequential"]
    steering_mode: QueueMode
    follow_up_mode: QueueMode
    context_retention_ms: int
    document_retention_ms: int | float


@dataclass(slots=True)
class Settings:
    """Resolved Harness settings: every field over its built-in default."""

    extensions: Sequence[Extension] | None = None
    stream: dict[str, Any] = field(default_factory=dict)
    retry: dict[str, Any] = field(default_factory=dict)
    compaction: dict[str, Any] = field(default_factory=dict)
    progress: dict[str, Any] = field(default_factory=dict)
    tool_execution: Literal["parallel", "sequential"] = "parallel"
    steering_mode: Literal["all", "one-at-a-time"] = "one-at-a-time"
    follow_up_mode: Literal["all", "one-at-a-time"] = "one-at-a-time"
    context_retention_ms: int = 600_000
    document_retention_ms: int | float = 600_000


# ── Task runtime and inspection ─────────────────────────────────────────────


class SettledTask(TypedDict):
    """A task record that has reached a terminal state."""

    id: TaskId
    conversationId: ConversationId
    kind: str
    version: int
    input: JsonValue
    owner: NotRequired[TaskId]
    background: bool
    abortRequested: bool
    abortReason: NotRequired[Literal["restart"]]
    abandonOnRestart: NotRequired[Literal[True]]
    startedAt: NotRequired[int]
    endedAt: NotRequired[int]
    state: TaskState
    memos: NotRequired[Never]


AnyTask = Any
"""Erased executable task definition stored in the registry."""


class HookRunner(Protocol):
    """Runs every selected extension's handler for one hook name, in extension order."""

    async def each(self, name: str, invoke: Callable[[Any], Awaitable[None] | None]) -> None: ...


class TaskRuntime(Protocol):
    """What a phase handler may do: committed reads, gated commits, waits, and the Harness clock."""

    task_id: TaskId
    conversation_id: ConversationId
    signal: AbortSignal | None
    registry: RegistrySnapshot
    settings: Settings
    models: Any
    hooks: HookRunner

    async def agent(self, context: Context) -> Agent: ...

    async def env(self, context: Context) -> Any: ...

    async def commit(
        self,
        change: Callable[..., NextTaskState | None | Awaitable[NextTaskState | None]],
        context: Context,
    ) -> None: ...

    async def memo(self, name: str, *args: Any) -> JsonValue | None: ...

    async def conversation(self, id: ConversationId, context: Context) -> Any: ...

    async def get_task(self, id: TaskId, context: Context) -> TaskRecord | None: ...

    async def wait_for_task(self, id: TaskId, context: Context) -> SettledTask: ...

    async def abort_owned(self, id: TaskId, context: Context) -> None: ...

    async def owned_tasks(self, context: Context) -> list[TaskRecord]: ...

    async def outcomes(self, ids: Sequence[TaskId], context: Context) -> list[TaskOutcome]: ...

    async def entry(self, *args: Any) -> EntryRecord | None: ...

    async def context(self, conversation_id: ConversationId, context: Context, *args: Any) -> ContextView: ...

    def now(self) -> int: ...

    def report(self, error: BaseException) -> None: ...

    async def sleep(self, until: int, context: Context) -> None: ...

    async def watch_doc(self, token: Any, *args: Any) -> Any: ...

    async def snapshot(self, token: Any, *args: Any) -> JsonObject | None: ...

    async def snapshot_as_of(self, token: Any, *args: Any) -> JsonObject | None: ...


class TaskInspection(TypedDict):
    """A live task and what the scheduler would do with it under the current registry."""

    record: TaskRecord
    state: dict[str, Any]


class HarnessInspection(TypedDict):
    """Point-in-time view of live work."""

    scheduling: Literal["paused", "running", "closing"]
    tasks: list[TaskInspection]
    submissions: list[SubmissionRecord]


# ── Submissions ─────────────────────────────────────────────────────────────

QueueMode = Literal["all", "one-at-a-time"]
"""How many queued items of one mode a boundary selects."""

UserInput = JsonValue
"""Content of a user message: a string or an ordered content list."""


class InputEntry(TypedDict):
    """The kind and data an input is placed as, instead of a ``pi.user`` entry."""

    kind: str
    data: NotRequired[JsonValue]


class InputSubmissionDraft(TypedDict, total=False):
    """An input submission: admitted as one user-message entry and answered by a run."""

    type: Literal["input"]
    content: UserInput
    entry: InputEntry
    request_id: str
    requestId: str
    when_busy: Literal["steer", "followUp", "reject"]
    whenBusy: Literal["steer", "followUp", "reject"]


class WriteSubmissionDraft(TypedDict, total=False):
    """A passive entry write: appended when a boundary selects it, or at once when display-only."""

    type: Literal["write"]
    entry: EntryDraft
    request_id: str
    requestId: str


SubmissionDraft: TypeAlias = InputSubmissionDraft | WriteSubmissionDraft
"""Either an input or a passive entry write."""

SettledSubmissionRecord: TypeAlias = (
    InputDoneRecord | InputUnansweredRecord | WriteDoneRecord | WriteUnansweredRecord
)
"""A submission that reached ``done`` or ``unanswered``."""


# ── Conversations ───────────────────────────────────────────────────────────


class ConversationAbortOptions(TypedDict, total=False):
    """Options of ``Conversation.abort()``: ``background`` also aborts background work."""

    background: bool


class ConversationHandle(Protocol):
    """Invocation-bound conversation operations for tasks and tools."""

    id: ConversationId

    async def submit(self, submission: Any, context: Context) -> Any: ...

    async def abort(self, context: Context, options: Mapping[str, Any] | None = None) -> None: ...

    async def wait_for_idle(self, context: Context) -> None: ...


class Conversation(Protocol):
    """Stateless handle for one conversation, bound to the Harness that returned it."""

    id: ConversationId

    async def submit(self, submission: SubmissionDraft, context: Context) -> Submission: ...

    async def agent(self, context: Context) -> Agent: ...

    async def configure(self, change: AgentChange, context: Context) -> None: ...

    async def commit(self, change: Callable[[Transaction], Any], context: Context) -> Any: ...

    async def context(
        self, context: Context, options: Mapping[str, Any] | None = None
    ) -> ContextView: ...

    async def entries(
        self,
        query: EntryQuery,
        limit: int,
        cursor: Cursor | None,
        context: Context,
    ) -> Page[EntryRecord, Cursor]: ...

    async def fork(
        self, at: EntryId, options: ConversationCreateOptions, context: Context
    ) -> Conversation: ...

    async def compact(self, instructions: str | None, context: Context) -> TaskId: ...

    async def reset(self, handoff: str | None, context: Context) -> None: ...

    async def abort(
        self, context: Context, options: ConversationAbortOptions | None = None
    ) -> None: ...

    async def wait_for_idle(self, context: Context) -> None: ...

    async def view_state(self, context: Context) -> Any: ...

    async def watch(self, context: Context) -> Any: ...


ConversationWatch: TypeAlias = WatchHandle[Any]
"""A serialized exact-frame watch of one conversation's view."""


class Submission(Protocol):
    """An admitted submission, awaitable until it settles and abortable while queued."""

    id: SubmissionId

    async def status(self, context: Context) -> SubmissionRecord: ...

    async def wait(self, context: Context) -> SettledSubmissionRecord: ...

    async def abort(self, context: Context) -> Literal["aborted", "already_placed", "settled"]: ...


class HarnessOptions(TypedDict, total=False):
    """Options of ``Harness.open()``."""

    models: Any
    registry: RegistryReader
    settings: Any
    env: Callable[..., Any]
    conversation_created: Callable[..., Any]
    now: Callable[[], int]
    on_report: Callable[[BaseException], Any]


T = TypeVar("T")

__all__ = [
    "Agent",
    "AgentChange",
    "AgentState",
    "AnyTask",
    "CommonDocDefinition",
    "CompactionHooks",
    "CompactionPolicy",
    "CompactionReason",
    "CompactionResult",
    "ContextView",
    "Conversation",
    "ConversationAbortOptions",
    "ConversationCreateOptions",
    "ConversationDocFamilyToken",
    "ConversationDocToken",
    "ConversationHandle",
    "ConversationInit",
    "ConversationRetryPolicy",
    "ConversationStreamOptions",
    "DocFamilyToken",
    "DocToken",
    "EnvTarget",
    "Extension",
    "GenerationHooks",
    "HarnessInspection",
    "HarnessOptions",
    "HarnessSettings",
    "HookApi",
    "HookRegistration",
    "HookResult",
    "HookRunner",
    "HooksOf",
    "InputEntry",
    "InputSubmissionDraft",
    "ModelRef",
    "NestedToolExecutionResult",
    "ProgressPolicy",
    "PromptInput",
    "PromptSection",
    "QueueMode",
    "Registry",
    "RegistryReader",
    "RegistrySnapshot",
    "RewindableConversationDocFamilyToken",
    "RewindableConversationDocToken",
    "SessionDocFamilyToken",
    "SessionDocToken",
    "Settings",
    "SettledSubmissionRecord",
    "SettledTask",
    "Submission",
    "SubmissionDraft",
    "TaskDocFamilyToken",
    "TaskDocToken",
    "TaskInspection",
    "TaskRuntime",
    "ThinkingLevel",
    "ToolControl",
    "ToolDiagnostic",
    "ToolExecutionApi",
    "ToolExecutionMode",
    "ToolExecutionResult",
    "ToolHookCall",
    "ToolHooks",
    "ToolRegistration",
    "UserInput",
    "Wrap",
]

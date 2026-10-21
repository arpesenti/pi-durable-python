# Parity Matrix

**Baseline:** `@earendil-works/pi-durable` **v1.3.0**, tag `v1.3.0`, commit
`71023aec1546d6d677a65669c5665c0927ce0dd0`.

**Normative reference:** [`docs/upstream/spec-1.3.0.md`](./docs/upstream/spec-1.3.0.md).

**Decision record:** [`docs/adr/0001-parity-baseline-and-boundary.md`](./docs/adr/0001-parity-baseline-and-boundary.md).

This file is the parity contract. Every public export and spec section gets a row. A row may
not be marked `tested` until the Python implementation exists, its translated test exists, and CI
is green.

## Status legend

| Status | Meaning |
| --- | --- |
| `planned` | No implementation or test yet. |
| `code` | Implementation exists; translated test missing. |
| `tested` | Implementation and translated test pass in CI. |
| `excluded` | Deliberately out of scope for this port. |
| `n/a` | Upstream non-goal; no action. |

## Scope

- **Root:** the v1.3.0 root export (`src/index.ts`).
- **Subpaths:** `env`, `tools`, `tool-search`, `testing`, and the storage backends.
- **Providers:** OpenAI and Anthropic adapters as optional extras.
- **Excluded:** Code Mode, the image implementation (`ImageProcessor` protocol remains),
  Cloudflare adapters, PowerShell, Windows support, and benchmark helpers.
- **Storage:** the v1.3.0 backends are ported faithfully. Cross-language storage
  interoperability is incidental, not a supported contract.

## Root exports

### `./documents.ts` → `pi_durable.documents`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `defineDoc` | value | `define_doc` | test_session_definitions.py, test_session_documents.py, test_session_states.py, test_session_tables.py | tested |
| `defineDocFamily` | value | `define_doc_family` | test_session_definitions.py, test_session_documents.py, test_session_states.py, test_session_tables.py | tested |

### `./entries.ts` → `pi_durable.entries`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `AssistantEntry` | value | `AssistantEntry` | test_entries.py | tested |
| `CompactionEntry` | value | `CompactionEntry` | test_entries.py | tested |
| `defineEntry` | value | `define_entry` | test_entries.py | tested |
| `ResetEntry` | value | `ResetEntry` | test_entries.py | tested |
| `SystemEntry` | value | `SystemEntry` | test_entries.py | tested |
| `ToolResultEntry` | value | `ToolResultEntry` | test_entries.py | tested |
| `UserEntry` | value | `UserEntry` | test_entries.py | tested |

### `./errors.ts` → `pi_durable.errors`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ConversationBusy` | value | `ConversationBusy` | test_errors.py | tested |
| `ReadAfterWrite` | value | `ReadAfterWrite` | test_errors.py | tested |
| `SessionFailed` | value | `SessionFailed` | test_errors.py | tested |
| `StorageRequestError` | value | `StorageRequestError` | test_errors.py | tested |

### `./harness/agent.ts` → `pi_durable.harness.agent`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `AgentDoc` | value | `AgentDoc` | — | planned |
| `configure` | value | `configure` | — | planned |
| `DEFAULT_COMPACTION_POLICY` | value | `DEFAULT_COMPACTION_POLICY` | — | planned |
| `DEFAULT_PROGRESS_POLICY` | value | `DEFAULT_PROGRESS_POLICY` | — | planned |
| `DEFAULT_RETRY_POLICY` | value | `DEFAULT_RETRY_POLICY` | — | planned |

### `./harness/compaction.ts` → `pi_durable.harness.compaction`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `CompactionTask` | value | `CompactionTask` | — | planned |
| `CompactionCheckpoint` | type | `CompactionCheckpoint` | — | planned |
| `CompactionInput` | type | `CompactionInput` | — | planned |
| `SummaryRequest` | type | `SummaryRequest` | — | planned |

### `./harness/define.ts` → `pi_durable.harness.define`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `defineExtension` | value | `define_extension` | — | planned |
| `defineTool` | value | `define_tool` | — | planned |
| `hook` | value | `hook` | — | planned |
| `section` | value | `section` | — | planned |
| `wrapSection` | value | `wrap_section` | — | planned |
| `wrapTool` | value | `wrap_tool` | — | planned |

### `./harness/events.ts` → `pi_durable.harness.events`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `watchEvents` | value | `watch_events` | — | planned |
| `AgentEvent` | type | `AgentEvent` | — | planned |
| `AgentEventStream` | type | `AgentEventStream` | — | planned |
| `MessageChange` | type | `MessageChange` | — | planned |
| `SnapshotEvent` | type | `SnapshotEvent` | — | planned |
| `ToolEventCall` | type | `ToolEventCall` | — | planned |

### `./harness/generation.ts` → `pi_durable.harness.generation`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `GenerationTask` | value | `GenerationTask` | — | planned |
| `GenerationCheckpoint` | type | `GenerationCheckpoint` | — | planned |
| `GenerationInput` | type | `GenerationInput` | — | planned |
| `GenerationResult` | type | `GenerationResult` | — | planned |

### `./harness/harness.ts` → `pi_durable.harness.harness`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `Harness` | value | `Harness` | — | planned |

### `./harness/inbox.ts` → `pi_durable.harness.inbox`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `InboxDoc` | value | `InboxDoc` | — | planned |
| `InboxItem` | type | `InboxItem` | — | planned |
| `InboxState` | type | `InboxState` | — | planned |

### `./harness/live.ts` → `pi_durable.harness.live`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `LiveDoc` | value | `LiveDoc` | — | planned |
| `CompactionFailure` | type | `CompactionFailure` | — | planned |
| `CompactionStatus` | type | `CompactionStatus` | — | planned |
| `LiveState` | type | `LiveState` | — | planned |
| `NestedToolSlot` | type | `NestedToolSlot` | — | planned |
| `NestedToolSummary` | type | `NestedToolSummary` | — | planned |
| `ToolSlot` | type | `ToolSlot` | — | planned |

### `./harness/provider.ts` → `pi_durable.harness.provider`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ProviderDoc` | value | `ProviderDoc` | — | planned |
| `ProviderState` | type | `ProviderState` | — | planned |

### `./harness/registry.ts` → `pi_durable.harness.registry`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createRegistry` | value | `create_registry` | — | planned |

### `./harness/task-graph.ts` → `pi_durable.harness.task_graph`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `TaskGraph` | type | `TaskGraph` | — | planned |
| `TaskGraphNode` | type | `TaskGraphNode` | — | planned |
| `TaskGraphState` | type | `TaskGraphState` | — | planned |
| `TaskGraphWatch` | type | `TaskGraphWatch` | — | planned |

### `./harness/tool.ts` → `pi_durable.harness.tool`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `NestedCallDoc` | value | `NestedCallDoc` | — | planned |
| `ToolTask` | value | `ToolTask` | — | planned |
| `NestedCallState` | type | `NestedCallState` | — | planned |
| `ToolTaskCheckpoint` | type | `ToolTaskCheckpoint` | — | planned |
| `ToolTaskInput` | type | `ToolTaskInput` | — | planned |
| `ToolTaskResult` | type | `ToolTaskResult` | — | planned |

### `./harness/types.ts` → `pi_durable.harness.types`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `Agent` | type | `Agent` | — | planned |
| `AgentChange` | type | `AgentChange` | — | planned |
| `AgentState` | type | `AgentState` | — | planned |
| `AnyTask` | type | `AnyTask` | — | planned |
| `CompactionHooks` | type | `CompactionHooks` | — | planned |
| `CompactionPolicy` | type | `CompactionPolicy` | — | planned |
| `CompactionReason` | type | `CompactionReason` | — | planned |
| `CompactionResult` | type | `CompactionResult` | — | planned |
| `ContextView` | type | `ContextView` | — | planned |
| `Conversation` | type | `Conversation` | — | planned |
| `ConversationAbortOptions` | type | `ConversationAbortOptions` | — | planned |
| `ConversationCreateOptions` | type | `ConversationCreateOptions` | — | planned |
| `ConversationHandle` | type | `ConversationHandle` | — | planned |
| `ConversationInit` | type | `ConversationInit` | — | planned |
| `ConversationRetryPolicy` | type | `ConversationRetryPolicy` | — | planned |
| `ConversationStreamOptions` | type | `ConversationStreamOptions` | — | planned |
| `ConversationWatch` | type | `ConversationWatch` | — | planned |
| `EnvTarget` | type | `EnvTarget` | — | planned |
| `Extension` | type | `Extension` | — | planned |
| `GenerationHooks` | type | `GenerationHooks` | — | planned |
| `HarnessInspection` | type | `HarnessInspection` | — | planned |
| `HarnessOptions` | type | `HarnessOptions` | — | planned |
| `HarnessSettings` | type | `HarnessSettings` | — | planned |
| `HookApi` | type | `HookApi` | — | planned |
| `HookRegistration` | type | `HookRegistration` | — | planned |
| `HookResult` | type | `HookResult` | — | planned |
| `HooksOf` | type | `HooksOf` | — | planned |
| `InputEntry` | type | `InputEntry` | — | planned |
| `InputSubmissionDraft` | type | `InputSubmissionDraft` | — | planned |
| `ModelRef` | type | `ModelRef` | — | planned |
| `NestedToolExecutionResult` | type | `NestedToolExecutionResult` | — | planned |
| `ProgressPolicy` | type | `ProgressPolicy` | — | planned |
| `PromptInput` | type | `PromptInput` | — | planned |
| `PromptSection` | type | `PromptSection` | — | planned |
| `QueueMode` | type | `QueueMode` | — | planned |
| `Registry` | type | `Registry` | — | planned |
| `RegistryReader` | type | `RegistryReader` | — | planned |
| `RegistrySnapshot` | type | `RegistrySnapshot` | — | planned |
| `Settings` | type | `Settings` | — | planned |
| `SettledSubmissionRecord` | type | `SettledSubmissionRecord` | — | planned |
| `SettledTask` | type | `SettledTask` | — | planned |
| `Submission` | type | `Submission` | — | planned |
| `SubmissionDraft` | type | `SubmissionDraft` | — | planned |
| `TaskInspection` | type | `TaskInspection` | — | planned |
| `ToolControl` | type | `ToolControl` | — | planned |
| `ToolDiagnostic` | type | `ToolDiagnostic` | — | planned |
| `ToolExecutionApi` | type | `ToolExecutionApi` | — | planned |
| `ToolExecutionMode` | type | `ToolExecutionMode` | — | planned |
| `ToolExecutionResult` | type | `ToolExecutionResult` | — | planned |
| `ToolHookCall` | type | `ToolHookCall` | — | planned |
| `ToolHooks` | type | `ToolHooks` | — | planned |
| `ToolRegistration` | type | `ToolRegistration` | — | planned |
| `UserInput` | type | `UserInput` | — | planned |
| `Wrap` | type | `Wrap` | — | planned |

### `./harness/usage.ts` → `pi_durable.harness.usage`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `UsageDoc` | value | `UsageDoc` | — | planned |
| `UsageState` | type | `UsageState` | — | planned |

### `./harness/view.ts` → `pi_durable.harness.view`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ConversationView` | type | `ConversationView` | — | planned |

### `./session/session.ts` → `pi_durable.session.session`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createSession` | value | `create_session` | test_session_tables.py, test_session_states.py | tested |

### `./storage/memory.ts` → `pi_durable.storage.memory`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `MemoryStorage` | value | `MemoryStorage` | test_memory_storage.py | tested |

### `./tasks.ts` → `pi_durable.tasks`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `defineTask` | value | `define_task` | test_session_tables.py | tested |

### `./types.ts` → `pi_durable.types`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ROOT_CONVERSATION_ID` | value | `ROOT_CONVERSATION_ID` | test_ids.py | tested |
| `CheckpointInfo` | type | `CheckpointInfo` | test_types.py | tested |
| `CommitChange` | type | `CommitChange` | test_types.py | tested |
| `CommitPublication` | type | `CommitPublication` | test_types.py | tested |
| `CommonDocDefinition` | type | `CommonDocDefinition` | test_session_definitions.py | tested |
| `ContextEdit` | type | `ContextEdit` | test_types.py | tested |
| `ConversationDocFamilyToken` | type | `ConversationDocFamilyToken` | test_session_definitions.py | tested |
| `ConversationDocToken` | type | `ConversationDocToken` | test_session_definitions.py | tested |
| `ConversationId` | type | `ConversationId` | test_ids.py, test_types.py | tested |
| `ConversationOwnership` | type | `ConversationOwnership` | test_types.py | tested |
| `ConversationQuery` | type | `ConversationQuery` | test_types.py, test_storage_protocol.py | tested |
| `ConversationRecord` | type | `ConversationRecord` | test_types.py, test_storage_protocol.py | tested |
| `Cursor` | type | `Cursor` | test_types.py, test_storage_protocol.py | tested |
| `DocDefinition` | type | `DocDefinition` | test_session_definitions.py, test_session_states.py | tested |
| `DocFamilyDefinition` | type | `DocFamilyDefinition` | test_session_definitions.py | tested |
| `DocFamilyToken` | type | `DocFamilyToken` | test_session_definitions.py, test_session_states.py | tested |
| `DocToken` | type | `DocToken` | test_session_definitions.py, test_session_documents.py, test_session_states.py | tested |
| `DocumentAddress` | type | `DocumentAddress` | test_types.py, test_storage_protocol.py | tested |
| `DocumentCommitChange` | type | `DocumentCommitChange` | test_types.py | tested |
| `DocumentContent` | type | `DocumentContent` | test_types.py | tested |
| `DocumentCopySource` | type | `DocumentCopySource` | test_types.py | tested |
| `DocumentCreate` | type | `DocumentCreate` | test_types.py | tested |
| `DocumentId` | type | `DocumentId` | test_ids.py, test_types.py | tested |
| `DocumentObserver` | type | `DocumentObserver` | test_session_watches.py, test_session_checkpoints_migrations.py | tested |
| `DocumentPoint` | type | `DocumentPoint` | test_types.py, test_storage_protocol.py | tested |
| `DocumentQuery` | type | `DocumentQuery` | test_types.py, test_storage_protocol.py | tested |
| `DocumentReader` | type | `DocumentReader` | — | planned |
| `DocumentRecord` | type | `DocumentRecord` | test_types.py | tested |
| `DocumentSemantics` | type | `DocumentSemantics` | test_types.py | tested |
| `DocumentState` | type | `DocumentState` | test_session_states.py, test_session_document_retention.py | tested |
| `DocumentWatch` | type | `DocumentWatch` | test_session_watches.py, test_session_checkpoints_migrations.py | tested |
| `Entry` | type | `Entry` | test_entries.py | tested |
| `EntryDraft` | type | `EntryDraft` | test_entries.py, test_types.py | tested |
| `EntryId` | type | `EntryId` | test_ids.py, test_types.py | tested |
| `EntryQuery` | type | `EntryQuery` | test_types.py, test_storage_protocol.py | tested |
| `EntryRecord` | type | `EntryRecord` | test_entries.py, test_types.py | tested |
| `HookRunner` | type | `HookRunner` | — | planned |
| `Id` | type | `Id` | test_types.py | tested |
| `JoinPolicy` | type | `JoinPolicy` | test_types.py | tested |
| `JsonObject` | type | `JsonObject` | test_types.py | tested |
| `LatestConversationSemantics` | type | `LatestConversationSemantics` | test_types.py | tested |
| `NextTaskState` | type | `NextTaskState` | test_types.py | tested |
| `Page` | type | `Page` | test_types.py, test_storage_protocol.py | tested |
| `PhaseHandler` | type | `PhaseHandler` | — | planned |
| `RewindableConversationDocFamilyToken` | type | `RewindableConversationDocFamilyToken` | test_session_definitions.py | tested |
| `RewindableConversationDocToken` | type | `RewindableConversationDocToken` | test_session_definitions.py | tested |
| `RewindableConversationSemantics` | type | `RewindableConversationSemantics` | test_types.py | tested |
| `RunningTask` | type | `RunningTask` | test_types.py | tested |
| `ScanOrder` | type | `ScanOrder` | test_types.py | tested |
| `Seq` | type | `Seq` | test_ids.py, test_types.py | tested |
| `Session` | type | `Session` | test_session_tables.py | tested |
| `SessionDocFamilyToken` | type | `SessionDocFamilyToken` | test_session_definitions.py | tested |
| `SessionDocToken` | type | `SessionDocToken` | test_session_definitions.py | tested |
| `SessionEnd` | type | `SessionEnd` | — | planned |
| `Storage` | type | `Storage` | test_storage_protocol.py | tested |
| `StorageWrite` | type | `StorageWrite` | test_types.py, test_storage_protocol.py | tested |
| `StoredDocument` | type | `StoredDocument` | test_types.py, test_storage_protocol.py | tested |
| `SubmissionCreate` | type | `SubmissionCreate` | test_types.py | tested |
| `SubmissionId` | type | `SubmissionId` | test_ids.py, test_types.py | tested |
| `SubmissionQuery` | type | `SubmissionQuery` | test_types.py, test_storage_protocol.py | tested |
| `SubmissionRecord` | type | `SubmissionRecord` | test_types.py, test_storage_protocol.py | tested |
| `SubmissionSettlement` | type | `SubmissionSettlement` | test_types.py | tested |
| `TableCommitChange` | type | `TableCommitChange` | test_types.py | tested |
| `Task` | type | `Task` | test_session_tables.py | tested |
| `TaskDefinition` | type | `TaskDefinition` | test_session_tables.py | tested |
| `TaskDocFamilyToken` | type | `TaskDocFamilyToken` | test_session_definitions.py | tested |
| `TaskDocToken` | type | `TaskDocToken` | test_session_definitions.py | tested |
| `TaskId` | type | `TaskId` | test_ids.py, test_types.py | tested |
| `TaskOptions` | type | `TaskOptions` | test_types.py | tested |
| `TaskOutcome` | type | `TaskOutcome` | test_types.py | tested |
| `TaskOutcomeError` | type | `TaskOutcomeError` | test_types.py | tested |
| `TaskOwnership` | type | `TaskOwnership` | test_types.py | tested |
| `TaskQuery` | type | `TaskQuery` | test_types.py, test_storage_protocol.py | tested |
| `TaskRecord` | type | `TaskRecord` | test_types.py, test_storage_protocol.py | tested |
| `TaskRuntime` | type | `TaskRuntime` | — | planned |
| `TaskState` | type | `TaskState` | test_types.py | tested |
| `Tx` | type | `Tx` | test_session_tables.py | tested |
| `TypedEntry` | type | `TypedEntry` | test_entries.py, test_types.py | tested |
| `TypedEntryDraft` | type | `TypedEntryDraft` | test_entries.py, test_types.py | tested |
| `WatchEnd` | type | `WatchEnd` | test_session_watches.py, test_session_checkpoints_migrations.py | tested |
| `WatchHandle` | type | `WatchHandle` | test_session_watches.py, test_session_checkpoints_migrations.py | tested |

## Optional subpaths

### `src/env/index.ts` → `pi_durable.env`

Execution environment protocol and helpers.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `rangeDecoder` | value | `range_decoder` | — | planned |
| `StreamDecoder` | value | `StreamDecoder` | — | planned |
| `startsWithBom` | value | `starts_with_bom` | — | planned |
| `LineScanner` | value | `LineScanner` | — | planned |
| `ok` | value | `ok` | — | planned |
| `err` | value | `err` | — | planned |
| `getOrThrow` | value | `get_or_throw` | — | planned |
| `getOrUndefined` | value | `get_or_undefined` | — | planned |
| `toError` | value | `to_error` | — | planned |
| `FileError` | value | `FileError` | — | planned |
| `ExecutionError` | value | `ExecutionError` | — | planned |
| `Result` | type | `Result` | — | planned |
| `FileKind` | type | `FileKind` | — | planned |
| `FileErrorCode` | type | `FileErrorCode` | — | planned |
| `ExecutionErrorCode` | type | `ExecutionErrorCode` | — | planned |
| `FileInfo` | type | `FileInfo` | — | planned |
| `TextLine` | type | `TextLine` | — | planned |
| `TextLineReader` | type | `TextLineReader` | — | planned |
| `BinaryReader` | type | `BinaryReader` | — | planned |
| `WatchTarget` | type | `WatchTarget` | — | planned |
| `WatchChange` | type | `WatchChange` | — | planned |
| `FileWatcher` | type | `FileWatcher` | — | planned |
| `LineScan` | type | `LineScan` | — | planned |
| `DirReader` | type | `DirReader` | — | planned |
| `ListFilesOptions` | type | `ListFilesOptions` | — | planned |
| `FileListEntry` | type | `FileListEntry` | — | planned |
| `FileListing` | type | `FileListing` | — | planned |
| `FileSystem` | type | `FileSystem` | — | planned |
| `ShellSpillOptions` | type | `ShellSpillOptions` | — | planned |
| `ShellExecResult` | type | `ShellExecResult` | — | planned |
| `ShellExecOptions` | type | `ShellExecOptions` | — | planned |
| `ShellOutputWindow` | type | `ShellOutputWindow` | — | planned |
| `ShellOutputSkip` | type | `ShellOutputSkip` | — | planned |
| `ShellOutputInfo` | type | `ShellOutputInfo` | — | planned |
| `Shell` | type | `Shell` | — | planned |
| `ExecutionEnv` | type | `ExecutionEnv` | — | planned |

### `src/env/node.ts` → `pi_durable.env.posix`

Python replaces the Node backend with a POSIX backend.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `NodeExecutionEnv` | value | `PosixExecutionEnv` | — | planned |
| `NodeWatchOptions` | type | `PosixWatchOptions` | — | planned |

### `src/tools/index.ts` → `pi_durable.tools`

Coding tools; PowerShell is excluded.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createBashTool` | value | `create_bash_tool` | — | planned |
| `createPowerShellTool` | value | `create_power_shell_tool` | — | excluded |
| `createEditTool` | value | `create_edit_tool` | — | planned |
| `DEFAULT_IMAGE_LIMITS` | value | `DEFAULT_IMAGE_LIMITS` | — | planned |
| `createReadTool` | value | `create_read_tool` | — | planned |
| `createWriteTool` | value | `create_write_tool` | — | planned |
| `createCodingTools` | value | `create_coding_tools` | — | planned |
| `CodingTools` | value | `CodingTools` | — | planned |
| `BashExecution` | type | `BashExecution` | — | planned |
| `BashPrepare` | type | `BashPrepare` | — | planned |
| `BashToolInput` | type | `BashToolInput` | — | planned |
| `BashToolOptions` | type | `BashToolOptions` | — | planned |
| `PowerShellToolInput` | type | `PowerShellToolInput` | — | excluded |
| `PowerShellToolOptions` | type | `PowerShellToolOptions` | — | excluded |
| `EditToolDetails` | type | `EditToolDetails` | — | planned |
| `EditToolInput` | type | `EditToolInput` | — | planned |
| `ImageLimits` | type | `ImageLimits` | — | planned |
| `ImageProcessor` | type | `ImageProcessor` | — | planned |
| `PreparedImage` | type | `PreparedImage` | — | planned |
| `ReadToolDetails` | type | `ReadToolDetails` | — | planned |
| `ReadToolInput` | type | `ReadToolInput` | — | planned |
| `ReadToolOptions` | type | `ReadToolOptions` | — | planned |
| `WriteToolInput` | type | `WriteToolInput` | — | planned |

### `src/tool-search/index.ts` → `pi_durable.tool_search`

BM25 tool search.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createToolSearchExtension` | value | `create_tool_search_extension` | — | planned |
| `createToolSearchTool` | value | `create_tool_search_tool` | — | planned |
| `DEFAULT_TOOL_SEARCH_LIMIT` | value | `DEFAULT_TOOL_SEARCH_LIMIT` | — | planned |
| `TOOL_SEARCH_TOOL_NAME` | value | `TOOL_SEARCH_TOOL_NAME` | — | planned |
| `ToolSearchToolDetails` | type | `ToolSearchToolDetails` | — | planned |

### `src/testing/index.ts` → `pi_durable.testing`

Conformance helpers; benchmarks are excluded.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createExpectAssertions` | value | `create_expect_assertions` | — | planned |
| `createEnvConformance` | value | `create_env_conformance` | — | planned |
| `registerEnvConformance` | value | `register_env_conformance` | — | planned |
| `registerStorageConformance` | value | `register_storage_conformance` | — | planned |
| `STORAGE_MEMORY_SCALES` | value | `STORAGE_MEMORY_SCALES` | — | excluded |
| `STORAGE_READ_BENCHMARKS` | value | `STORAGE_READ_BENCHMARKS` | — | excluded |
| `STORAGE_WRITE_BENCHMARKS` | value | `STORAGE_WRITE_BENCHMARKS` | — | excluded |
| `seedStorageBenchmark` | value | `seed_storage_benchmark` | — | excluded |
| `seedStorageWriteBenchmark` | value | `seed_storage_write_benchmark` | — | excluded |
| `storageBenchmarkPrimaryRecordCount` | value | `storageBenchmarkPrimaryRecordCount` | — | excluded |
| `TIMING_SCALE` | value | `TIMING_SCALE` | — | excluded |
| `createStorageConformance` | value | `create_storage_conformance` | — | planned |
| `ExpectLike` | type | `ExpectLike` | — | planned |
| `StorageConformanceRunner` | type | `StorageConformanceRunner` | — | planned |
| `StorageBenchmarkDataset` | type | `StorageBenchmarkDataset` | — | excluded |
| `StorageBenchmarkScale` | type | `StorageBenchmarkScale` | — | excluded |
| `StorageReadBenchmark` | type | `StorageReadBenchmark` | — | excluded |
| `StorageWriteBenchmark` | type | `StorageWriteBenchmark` | — | excluded |
| `EnvConformanceAssertions` | type | `EnvConformanceAssertions` | — | planned |
| `EnvConformanceCase` | type | `EnvConformanceCase` | — | planned |
| `EnvConformanceOptions` | type | `EnvConformanceOptions` | — | planned |
| `EnvConformanceProvider` | type | `EnvConformanceProvider` | — | planned |
| `StorageConformanceAssertions` | type | `StorageConformanceAssertions` | — | planned |
| `StorageConformanceCase` | type | `StorageConformanceCase` | — | planned |
| `StorageConformanceOptions` | type | `StorageConformanceOptions` | — | planned |
| `StorageConformanceProvider` | type | `StorageConformanceProvider` | — | planned |

### `src/storage/memory.ts` → `pi_durable.storage.memory`

In-memory backend.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `MemoryStorage` | value | `MemoryStorage` | test_memory_storage.py | tested |
| `PreparedMemoryCommit` | type | `PreparedMemoryCommit` | test_memory_storage.py | tested |

### `src/storage/jsonl/index.ts` → `pi_durable.storage.jsonl`

JSONL backend.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `JsonlCorruptionError` | value | `JsonlCorruptionError` | — | planned |
| `JsonlStorage` | value | `JsonlStorage` | — | planned |
| `JsonlStoragePoisonedError` | value | `JsonlStoragePoisonedError` | — | planned |
| `JsonlStorageOptions` | type | `JsonlStorageOptions` | — | planned |

### `src/storage/sqlite/index.ts` → `pi_durable.storage.sqlite`

SQLite backend and migrations.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `applySqliteMigrations` | value | `apply_sqlite_migrations` | — | planned |
| `CURRENT_SQLITE_SCHEMA_VERSION` | value | `CURRENT_SQLITE_SCHEMA_VERSION` | — | planned |
| `SQLITE_MIGRATIONS` | value | `SQLITE_MIGRATIONS` | — | planned |
| `SqliteStorage` | value | `SqliteStorage` | — | planned |
| `SqliteDatabase` | type | `SqliteDatabase` | — | planned |
| `SqliteExecutor` | type | `SqliteExecutor` | — | planned |
| `SqliteValue` | type | `SqliteValue` | — | planned |
| `SqliteMigration` | type | `SqliteMigration` | — | planned |
| `SqliteStorageOptions` | type | `SqliteStorageOptions` | — | planned |

## Excluded subpaths

| Upstream subpath | Reason |
| --- | --- |
| `./codemode` | Code Mode is a separate subsystem and needs a Python execution-sandbox design. |
| `./images` | No Python image implementation; the `ImageProcessor` protocol remains in `tools`. |
| `./storage/jsonl/node` | Node filesystem adapter; Python uses `JsonlStorage.open()`. |
| `./storage/sqlite/node` | Node SQLite driver; Python uses the stdlib-backed `SqliteStorage.open()`. |
| `./storage/sqlite/cloudflare` | Cloudflare-only adapter. |

## Spec sections

| Section | Status |
| --- | --- |
| 1 Terms and invariants | code |
| 2 Core records | tested |
| 2.1 Entries and context | planned |
| 2.2 Public Harness surface | planned |
| 3 Documents | code |
| 3.1 Definitions | tested |
| 3.2 Records and lifetimes | tested |
| 3.3 Access and creation | code |
| 3.4 Mutation ownership | tested |
| 3.5 Bases and checkpoints | tested |
| 3.6 Versions and migrations | tested |
| 3.7 Forks | planned |
| 4 Transactions and storage ownership | tested |
| 5 Tasks | planned |
| 5.1 Definition | planned |
| 5.2 Effect sandwich | planned |
| 5.3 Terminal tasks | planned |
| 5.4 Scheduler, abort, and ownership | planned |
| 5.5 Structured concurrency | planned |
| 6 Submissions and inbox | planned |
| 7 Extensions, hooks, tools, and system prompt | planned |
| 7.1 Extensions, registry, and agent resolution | planned |
| 7.2 Hooks | planned |
| 7.3 Tools | planned |
| 7.4 System prompt and dynamic tools | planned |
| 7.5 Extension reload | planned |
| 7.6 Code mode | excluded |
| 8 Built-in tasks | planned |
| 8.1 Built-in entries | planned |
| 8.2 Live document | planned |
| 8.3 Generation | planned |
| 8.4 Tool | planned |
| 8.5 Tool rounds | planned |
| 8.6 Usage | planned |
| 8.7 Compaction | planned |
| 9 Document observation and Chord | code |
| 9.1 Document state | tested |
| 9.2 `watchDoc` | tested |
| 9.3 Conversation view | planned |
| 9.4 Agent events (experimental) | planned |
| 9.5 Task graph view | planned |
| 10 Storage contract | tested |
| 10.1 Failures | code |
| 11 Backends | code |
| 11.1 Memory | tested |
| 11.2 SQLite | planned |
| 11.3 JSONL | planned |
| 12 API footguns | planned |
| 13 Non-goals | n/a |

The upstream document-retention suite has two Harness-level cases: one follows a Harness settings
change at its next use, and one compares agent event streams with documents evicted. They are
translated with the Harness in #12-#22; the Session-level suite covers the retention semantics.

## Allowed deviations

| Area | Upstream | Python |
| --- | --- | --- |
| Function and parameter names | camelCase | snake_case |
| Class and type names | PascalCase | PascalCase |
| Persisted JSON fields | camelCase | camelCase |
| Host execution backend | `NodeExecutionEnv` | `PosixExecutionEnv` |
| Node storage entrypoints | `openNode*` | `JsonlStorage.open()` / `SqliteStorage.open()` |
| PowerShell tool | exported | excluded |
| Document eviction timers | `setTimeout(...).unref()`, commits-only where timers cannot be unreferenced | `asyncio` timers never keep the process alive, so every loop keeps one; a loop that cannot schedule one evicts on commits only |
| Windows support | supported | unsupported |
| Images | `./images` subpath | excluded; `ImageProcessor` protocol only |
| Code Mode | `./codemode` subpath | excluded |
| Cloudflare | SQLite/JSONL adapters | excluded |
| Benchmarks | `./testing` helpers | excluded |
| Chord | public dependency | internal implementation |
| Storage interoperability | shared format | incidental, not a contract |
| `Context` | Chord `Context` | minimal abort signal + value bag |
| Model access | `pi-ai` dependency | `Model` protocol + OpenAI/Anthropic extras |
| Record mutability | TypeScript `readonly` types | entry records are deep-frozen `FrozenDict`/`FrozenList` values |
| Draft property deletion | assigning `undefined` deletes the property | use `del draft.key`; assigning `None` stores JSON `null` |
| Definition callbacks | methods invoked with the definition as `this` | a callback whose first parameter is named `self` or `definition` receives the definition bound |
| `snapshotAsOf` on a latest-only document | rejected by the TypeScript overloads | rejected at runtime with `TypeError` |
| Document observation | Chord `AttachedReplicatedState`/`ReplicatedStateSource` | `DocumentState.subscribe(listener)` delivers `(value, context, Delivery(kind, sequence))`; `CommittedWatch` frames carry `(value, ops, context)` |
| Promise-returning watcher methods | `stop()` returns the one common promise | `CommittedWatch.stop()` and `closed` return the one common `asyncio.Future`, so each is identity-stable and awaiting a cancellation does not cancel it |
| Task result branding | `TaskId<Result>` carries the result type | `TaskId` is a `NewType`; the task record carries the result |
| ID allocation | `mintId<I>()` returns a branded generic | `mint_id()` returns `int`; `id_from_number` applies the brand |
| Version | `1.3.0` | `1.3.0` lockstep |

## Port additions

These are required by the chosen scope but are not upstream 1.3.0 exports.

| Addition | Module | Status |
| --- | --- | --- |
| Context, abort signal, and context-scoped values | `pi_durable.context` | tested (`test_context.py`) |
| Strict JSON validation, copying, and frozen values | `pi_durable.json` | tested (`test_json.py`) |
| Entry draft validation and immutable record construction | `pi_durable.types` | tested (`test_entries.py`) |
| Session test constructor and failure latch | `pi_durable.session.session` | tested (`test_session_tables.py`) |
| Mutable document drafts backed by strict-JSON copies | `pi_durable.session.drafts` | tested (`test_session_states.py`, `test_session_tables.py`) |
| Chord-compatible JSON operation applier | `pi_durable.delta` | tested (`test_memory_storage.py`) |
| Serialized committed document watches | `pi_durable.session.observation` | tested (`test_session_watches.py`, `test_session_checkpoints_migrations.py`) |
| Document cache sizes, retention normalization, and the suite default | `pi_durable.session.session` | tested (`test_session_document_retention.py`) |
| OpenAI model adapter | `pi_durable.providers.openai` | planned |
| Anthropic model adapter | `pi_durable.providers.anthropic` | planned |

## Milestones

- [x] 1. Scaffold: vendored spec, decision record, parity matrix, CI, package layout.
- [ ] 2. Kernel: types, IDs, JSON, errors, entries, documents, Session/Tx/forks.
- [ ] 3. Storage: memory, JSONL, SQLite, conformance suites.
- [ ] 4. Tasks, scheduler, submissions, document observation.
- [ ] 5. Harness: agent, registry, hooks, prompt, model, generation, tool rounds, usage, live.
- [ ] 6. `env`, coding tools, `tool-search`.
- [ ] 7. Compaction, events/view, task graph, extension reload.
- [ ] 8. Full translated suite green, docs, version `1.3.0`, release.

## Updating this file

1. Change status only in the PR that changes the code or test.
2. A row moves to `tested` only when its translated test passes in CI.
3. Add a new row for every new upstream export or spec section; never renumber existing rows.
4. If a row becomes excluded, record why in **Excluded subpaths** or **Allowed deviations**.

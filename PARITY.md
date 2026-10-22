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
| `addTools` | value | `add_tools` | — | planned |
| `AgentDoc` | value | `AgentDoc` | test_harness_registry_agent.py | tested |
| `configure` | value | `configure` | test_harness_registry_agent.py | tested |
| `DEFAULT_COMPACTION_POLICY` | value | `DEFAULT_COMPACTION_POLICY` | test_harness_registry_agent.py | tested |
| `DEFAULT_PROGRESS_POLICY` | value | `DEFAULT_PROGRESS_POLICY` | test_harness_registry_agent.py | tested |
| `DEFAULT_RETRY_POLICY` | value | `DEFAULT_RETRY_POLICY` | test_harness_registry_agent.py | tested |

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
| `defineExtension` | value | `define_extension` | test_harness_tasks.py, test_harness_lifecycle.py | tested |
| `defineTool` | value | `define_tool` | test_harness_tasks.py | tested |
| `hook` | value | `hook` | test_harness_tasks.py | tested |
| `section` | value | `section` | test_harness_registry_agent.py | tested |
| `wrapSection` | value | `wrap_section` | test_harness_registry_agent.py | tested |
| `wrapTool` | value | `wrap_tool` | test_harness_registry_agent.py | tested |

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
| `Harness` | value | `Harness` | test_harness_tasks.py, test_harness_ownership.py, test_harness_lifecycle.py, test_harness_cancellation_barrier.py, test_harness_tasks_recovery.py, test_harness_scheduler_indexes.py, test_harness_scheduler_retention.py, test_harness_storage_failure.py, test_harness_submissions.py, test_harness_inbox.py, test_harness_input_entries.py | tested |
| `safeNow` | value | `safe_now` | test_harness_storage_failure.py | tested |
| `SCHEDULER_INDEX_SIZES` | value | `Harness.scheduler_index_sizes` | test_harness_scheduler_retention.py | tested |

### `./harness/inbox.ts` → `pi_durable.harness.inbox`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `InboxDoc` | value | `InboxDoc` | test_harness_inbox.py, test_harness_input_entries.py | tested |
| `InboxItem` | type | `InboxItem` | test_harness_inbox.py, test_harness_input_entries.py | tested |
| `InboxState` | type | `InboxState` | test_harness_inbox.py | tested |

### `./harness/live.ts` → `pi_durable.harness.live`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `LiveDoc` | value | `LiveDoc` | test_harness_submissions.py, test_harness_inbox.py | tested |
| `CompactionFailure` | type | `CompactionFailure` | — | planned |
| `CompactionStatus` | type | `CompactionStatus` | — | planned |
| `LiveState` | type | `LiveState` | test_harness_submissions.py, test_harness_inbox.py | tested |
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
| `createRegistry` | value | `create_registry` | test_harness_tasks.py, test_harness_ownership.py, test_harness_lifecycle.py, test_harness_cancellation_barrier.py, test_harness_registry_agent.py | tested |

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
| `Agent` | type | `Agent` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `AgentChange` | type | `AgentChange` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `AgentState` | type | `AgentState` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `AnyTask` | type | `AnyTask` | test_harness_tasks.py, test_harness_ownership.py | tested |
| `CompactionHooks` | type | `CompactionHooks` | — | planned |
| `CompactionPolicy` | type | `CompactionPolicy` | — | planned |
| `CompactionReason` | type | `CompactionReason` | — | planned |
| `CompactionResult` | type | `CompactionResult` | — | planned |
| `ContextView` | type | `ContextView` | test_harness_context.py | tested |
| `Conversation` | type | `Conversation` | — | planned |
| `ConversationAbortOptions` | type | `ConversationAbortOptions` | — | planned |
| `ConversationCreateOptions` | type | `ConversationCreateOptions` | — | planned |
| `ConversationHandle` | type | `ConversationHandle` | test_harness_ownership.py | tested |
| `ConversationInit` | type | `ConversationInit` | — | planned |
| `ConversationRetryPolicy` | type | `ConversationRetryPolicy` | — | planned |
| `ConversationStreamOptions` | type | `ConversationStreamOptions` | — | planned |
| `ConversationWatch` | type | `ConversationWatch` | — | planned |
| `EnvTarget` | type | `EnvTarget` | — | planned |
| `Extension` | type | `Extension` | test_harness_tasks.py, test_harness_ownership.py, test_harness_registry_agent.py | tested |
| `GenerationHooks` | type | `GenerationHooks` | — | planned |
| `HarnessInspection` | type | `HarnessInspection` | test_harness_tasks.py, test_harness_cancellation_barrier.py | tested |
| `HarnessOptions` | type | `HarnessOptions` | test_harness_tasks.py, test_harness_lifecycle.py | tested |
| `HarnessSettings` | type | `HarnessSettings` | — | planned |
| `HookApi` | type | `HookApi` | — | planned |
| `HookRegistration` | type | `HookRegistration` | test_harness_tasks.py | tested |
| `HookResult` | type | `HookResult` | — | planned |
| `HooksOf` | type | `HooksOf` | — | planned |
| `InputEntry` | type | `InputEntry` | test_harness_input_entries.py | tested |
| `InputSubmissionDraft` | type | `InputSubmissionDraft` | test_harness_submissions.py, test_harness_input_entries.py | tested |
| `ModelRef` | type | `ModelRef` | test_harness_registry_agent.py | tested |
| `NestedToolExecutionResult` | type | `NestedToolExecutionResult` | — | planned |
| `ProgressPolicy` | type | `ProgressPolicy` | — | planned |
| `PromptInput` | type | `PromptInput` | — | planned |
| `PromptSection` | type | `PromptSection` | test_harness_registry_agent.py | tested |
| `QueueMode` | type | `QueueMode` | test_harness_inbox.py | tested |
| `Registry` | type | `Registry` | test_harness_tasks.py, test_harness_lifecycle.py, test_harness_registry_agent.py | tested |
| `RegistryReader` | type | `RegistryReader` | test_harness_lifecycle.py, test_harness_registry_agent.py | tested |
| `RegistrySnapshot` | type | `RegistrySnapshot` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `Settings` | type | `Settings` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `SettledSubmissionRecord` | type | `SettledSubmissionRecord` | test_harness_submissions.py | tested |
| `SettledTask` | type | `SettledTask` | test_harness_tasks.py | tested |
| `Submission` | type | `Submission` | test_harness_submissions.py, test_harness_inbox.py | tested |
| `SubmissionDraft` | type | `SubmissionDraft` | test_harness_submissions.py, test_harness_input_entries.py | tested |
| `TaskInspection` | type | `TaskInspection` | test_harness_cancellation_barrier.py | tested |
| `ToolControl` | type | `ToolControl` | — | planned |
| `ToolDiagnostic` | type | `ToolDiagnostic` | — | planned |
| `ToolExecutionApi` | type | `ToolExecutionApi` | — | planned |
| `ToolExecutionMode` | type | `ToolExecutionMode` | — | planned |
| `ToolExecutionResult` | type | `ToolExecutionResult` | — | planned |
| `ToolHookCall` | type | `ToolHookCall` | — | planned |
| `ToolHooks` | type | `ToolHooks` | — | planned |
| `ToolRegistration` | type | `ToolRegistration` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `UserInput` | type | `UserInput` | test_harness_input_entries.py | tested |
| `Wrap` | type | `Wrap` | test_harness_registry_agent.py | tested |

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
| `contained` | value | `contained` | test_harness_storage_failure.py | tested |

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
| `HookRunner` | type | `HookRunner` | test_harness_tasks.py | tested |
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
| `TaskRuntime` | type | `TaskRuntime` | test_harness_tasks.py, test_harness_scheduler_indexes.py, test_harness_scheduler_retention.py | tested |
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
| `ok` | value | `ok` | — | code |
| `err` | value | `err` | — | code |
| `getOrThrow` | value | `get_or_throw` | — | code |
| `getOrUndefined` | value | `get_or_undefined` | — | code |
| `toError` | value | `to_error` | — | code |
| `FileError` | value | `FileError` | — | code |
| `ExecutionError` | value | `ExecutionError` | — | planned |
| `Result` | type | `Result` | — | code |
| `FileKind` | type | `FileKind` | — | code |
| `FileErrorCode` | type | `FileErrorCode` | — | code |
| `ExecutionErrorCode` | type | `ExecutionErrorCode` | — | planned |
| `FileInfo` | type | `FileInfo` | — | code |
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
| `FileSystem` | type | `FileSystem` | — | code |
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
| `NodeExecutionEnv` | value | `PosixExecutionEnv` | — | code |
| `NodeWatchOptions` | type | `PosixWatchOptions` | — | planned |

`test_env_filesystem.py` exercises the filesystem operations the JSONL backend and its failure
injection need. The translated `env` conformance suite, shell execution, watching, line and binary
readers, and directory listings land with #16, so these rows remain `code`.

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
| `createExpectAssertions` | value | `create_expect_assertions` | test_testing_conformance.py | tested |
| `createEnvConformance` | value | `create_env_conformance` | — | planned |
| `registerEnvConformance` | value | `register_env_conformance` | — | planned |
| `registerStorageConformance` | value | `register_storage_conformance` | test_testing_conformance.py | tested |
| `STORAGE_MEMORY_SCALES` | value | `STORAGE_MEMORY_SCALES` | — | excluded |
| `STORAGE_READ_BENCHMARKS` | value | `STORAGE_READ_BENCHMARKS` | — | excluded |
| `STORAGE_WRITE_BENCHMARKS` | value | `STORAGE_WRITE_BENCHMARKS` | — | excluded |
| `seedStorageBenchmark` | value | `seed_storage_benchmark` | — | excluded |
| `seedStorageWriteBenchmark` | value | `seed_storage_write_benchmark` | — | excluded |
| `storageBenchmarkPrimaryRecordCount` | value | `storageBenchmarkPrimaryRecordCount` | — | excluded |
| `TIMING_SCALE` | value | `TIMING_SCALE` | — | excluded |
| `createStorageConformance` | value | `create_storage_conformance` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |
| `ExpectLike` | type | `ExpectLike` | test_testing_conformance.py | tested |
| `StorageConformanceRunner` | type | `StorageConformanceRunner` | test_testing_conformance.py | tested |
| `StorageBenchmarkDataset` | type | `StorageBenchmarkDataset` | — | excluded |
| `StorageBenchmarkScale` | type | `StorageBenchmarkScale` | — | excluded |
| `StorageReadBenchmark` | type | `StorageReadBenchmark` | — | excluded |
| `StorageWriteBenchmark` | type | `StorageWriteBenchmark` | — | excluded |
| `EnvConformanceAssertions` | type | `EnvConformanceAssertions` | — | planned |
| `EnvConformanceCase` | type | `EnvConformanceCase` | — | planned |
| `EnvConformanceOptions` | type | `EnvConformanceOptions` | — | planned |
| `EnvConformanceProvider` | type | `EnvConformanceProvider` | — | planned |
| `StorageConformanceAssertions` | type | `StorageConformanceAssertions` | test_testing_conformance.py | tested |
| `StorageConformanceCase` | type | `StorageConformanceCase` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |
| `StorageConformanceOptions` | type | `StorageConformanceOptions` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |
| `StorageConformanceProvider` | type | `StorageConformanceProvider` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |

The environment cases and their types (`createEnvConformance`, `registerEnvConformance`, and the
`EnvConformance*` types) land with the `env` suite in #16. The filesystem capability they depend on is
already in place for the JSONL backend.

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
| `JsonlCorruptionError` | value | `JsonlCorruptionError` | test_jsonl_storage.py | tested |
| `JsonlStorage` | value | `JsonlStorage` | test_jsonl_storage.py | tested |
| `JsonlStoragePoisonedError` | value | `JsonlStoragePoisonedError` | test_jsonl_storage.py | tested |
| `JsonlStorageOptions` | type | `JsonlStorageOptions` | test_jsonl_storage.py | tested |

### `src/storage/sqlite/index.ts` → `pi_durable.storage.sqlite`

SQLite backend and migrations.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `applySqliteMigrations` | value | `apply_sqlite_migrations` | test_sqlite_migrations.py | tested |
| `CURRENT_SQLITE_SCHEMA_VERSION` | value | `CURRENT_SQLITE_SCHEMA_VERSION` | test_sqlite_migrations.py | tested |
| `SQLITE_MIGRATIONS` | value | `SQLITE_MIGRATIONS` | test_sqlite_migrations.py | tested |
| `SqliteStorage` | value | `SqliteStorage` | test_sqlite_storage.py, test_sqlite_facade.py | tested |
| `SqliteDatabase` | type | `SqliteDatabase` | test_sqlite_facade.py, test_sqlite_storage.py | tested |
| `SqliteExecutor` | type | `SqliteExecutor` | test_sqlite_facade.py | tested |
| `SqliteValue` | type | `SqliteValue` | test_sqlite_facade.py | tested |
| `SqliteMigration` | type | `SqliteMigration` | test_sqlite_migrations.py | tested |
| `SqliteStorageOptions` | type | `SqliteStorageOptions` | test_sqlite_storage.py | tested |

## Excluded subpaths

| Upstream subpath | Reason |
| --- | --- |
| `./codemode` | Code Mode is a separate subsystem and needs a Python execution-sandbox design. |
| `./images` | No Python image implementation; the `ImageProcessor` protocol remains in `tools`. |
| `./storage/jsonl/node` | Node filesystem adapter; Python passes a `FileSystem` (such as `PosixExecutionEnv`) to `JsonlStorage.open()`. |
| `./storage/sqlite/node` | Node SQLite driver; Python opens the stdlib-backed `PosixSqliteDatabase` facade and passes it to `SqliteStorage.open()`. |
| `./storage/sqlite/cloudflare` | Cloudflare-only adapter. |

## Spec sections

| Section | Status |
| --- | --- |
| 1 Terms and invariants | code |
| 2 Core records | tested |
| 2.1 Entries and context | tested |
| 2.2 Public Harness surface | planned |
| 3 Documents | code |
| 3.1 Definitions | tested |
| 3.2 Records and lifetimes | tested |
| 3.3 Access and creation | code |
| 3.4 Mutation ownership | tested |
| 3.5 Bases and checkpoints | tested |
| 3.6 Versions and migrations | tested |
| 3.7 Forks | tested |
| 4 Transactions and storage ownership | tested |
| 5 Tasks | tested |
| 5.1 Definition | tested |
| 5.2 Effect sandwich | tested |
| 5.3 Terminal tasks | tested |
| 5.4 Scheduler, abort, and ownership | tested |
| 5.5 Structured concurrency | tested |
| 6 Submissions and inbox | tested |
| 7 Extensions, hooks, tools, and system prompt | code |
| 7.1 Extensions, registry, and agent resolution | tested (built-in tasks with #14/#18/#21) |
| 7.2 Hooks | code |
| 7.3 Tools | planned |
| 7.4 System prompt and dynamic tools | planned |
| 7.5 Extension reload | tested |
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
| 11 Backends | tested |
| 11.1 Memory | tested |
| 11.2 SQLite | tested |
| 11.3 JSONL | tested |
| 12 API footguns | planned |
| 13 Non-goals | n/a |

The upstream document-retention suite has two Harness-level cases: one follows a Harness settings
change at its next use, and one compares agent event streams with documents evicted. They are
translated with the Harness in #12-#22; the Session-level suite covers the retention semantics.

The four task-integrity suites (`harness-tasks-recovery`, `harness-scheduler-indexes`,
`harness-scheduler-retention`, and `harness-storage-failure`) are translated for the task core. The
recovery suite is complete. The scheduler suites translate every case whose work is tasks and
conversations; the retention cases that queue or withdraw inputs, and the storage-failure cases that
watch documents or conversation views, land with submissions (#11), the live document (#14), and the
observation surfaces (#22). `harness-storage-failure` also translates the failure latch, listener
containment, and clock fallback cases the Session can exercise now.

Deferred from earlier tickets: the `harness-tasks` cases that assert built-in generation/tool
cleanup, the `harness-ownership` cases that need submissions, tools, or a live `pi.live` run, and
the `harness-lifecycle` cases that need tool hooks, usage, conversation views, or the task-graph
watch. The `harness-lifecycle` open-error, close, reopen, registry-change, cancellation, and
durable-commit cases are translated now.

The submission suites are translated in three files. `harness-submissions` covers idle input and
write admission, request-ID deduplication, busy rejection, abort results, lookup, waits, close
behavior, and settlement validation. `harness-inbox` covers busy queueing, write-before-user
ordering, queue modes, resets, stale heads, display-only writes, withdrawals, failed-run inbox
retention, positional inbox operations, the empty-inbox base, and reopen. `harness-input-entries`
covers application entry kinds, reserved-kind rejection, kind/data projection, queued entries, and
withdrawal. The suites install a generation probe under the built-in `pi.generation` name; the real
run loop, provider streaming, successful answer boundaries beyond that probe, and generation
presentation in `pi.live` land with #14.

The upstream `harness-context` suite is translated in three parts. Its thirteen derivation cases run
against the Session-level `context()` read. Three of its nine kept-range cases run against
`TaskRuntime.context()`: extended reads scanning only newer entries and honoring an earlier cutoff,
returned views staying apart from the kept range, and dropping the range under zero retention. The
remaining six (clock-driven expiry, busy and late reads, extended-read equivalence, baseline found
in added entries, and shallow-storage freeze) land with #10 and #22.

The upstream `harness-registry` suite is translated in `test_harness_registry_agent.py`: install,
replace in place, uninstall by name with subscriber publications and immutable old snapshots;
validation that publishes nothing; installed task definitions; the progress-interval default;
default/array/`{add, remove}` extension selection with stale objects and uninstalled names;
same-name tool replacement, wrapper order, and tool filters; wrapper throw/rename drops with
reports; section ordering, same-key replacement, and the unwrapped `instructions` section; hook
collection in extension order; and the stored-agent defaults. Hook dispatch order and ordinary-throw
containment, plus reload of an extension in a live Session at its next phase, are covered there too.
The registry's built-in generation, tool, and compaction tasks land with #14/#18/#21, so the
built-in-task case is partial, and the per-hook composition rules (replacement chains, first-block
and first-continuation wins) arrive with those tasks; the `pi.generation` task-collision row is
translated once that built-in exists. The resolution half of `harness-callers` is
translated alongside: model-only, tools-only, and both callers; `modelTools` narrowing without
widening what tools may call; enabled filters; and deferred-tool resolution through an array or
`{remove, add}` (a superset of the upstream resolution cases). Its end-to-end cases need a
generation request and tool execution and land with #14/#18, and
`addTools` (the tool-round edit of `tools`/`modelTools`) lands with tool rounds and tool search
(#14/#18/#20).

## Allowed deviations

| Area | Upstream | Python |
| --- | --- | --- |
| Function and parameter names | camelCase | snake_case |
| Settings and policy objects | camelCase fields | snake_case fields on the Python-facing `Settings` and `DEFAULT_*_POLICY` values (`max_retries`, `keep_recent_tokens`, `partial_interval_ms`); persisted records stay camelCase |
| Class and type names | PascalCase | PascalCase |
| Persisted JSON fields | camelCase | camelCase |
| Host execution backend | `NodeExecutionEnv` | `PosixExecutionEnv` |
| Node storage entrypoints | `openNode*` | `JsonlStorage.open()` / `SqliteStorage.open()`, with the caller supplying a `FileSystem` or a `SqliteDatabase`; file-backed SQLite opens through `open_posix_sqlite_storage()` |
| `Result` values | `{ ok: true, value }` / `{ ok: false, error }` object literals | frozen `Ok(value)` / `Err(error)` dataclasses with the same `ok` role and `value`/`error` fields |
| Storage conformance registration | Vitest's runtime `describe`/`it` globals | `create_storage_conformance` cases are parametrized by pytest; `register_storage_conformance` keeps the runner-injection protocol |
| JSONL serialization | `JSON.stringify` (raw UTF-8, well-formed lone-surrogate escapes) | `json.dumps(ensure_ascii=True)` (escaped non-ASCII); the decoded JSON values are identical |
| SQLite serialization | `JSON.stringify` (raw UTF-8), indexed strings kept as `JSON.stringify` output | `json.dumps(ensure_ascii=True, separators=(",", ":"))`; decoded values are identical, and indexed strings stay JSON-encoded because stdlib `sqlite3` cannot bind lone surrogates |
| PowerShell tool | exported | excluded |
| Document eviction timers | `setTimeout(...).unref()`, commits-only where timers cannot be unreferenced | `asyncio` timers never keep the process alive, so every loop keeps one; a loop that cannot schedule one evicts on commits only |
| Windows support | supported | unsupported |
| Images | `./images` subpath | excluded; `ImageProcessor` protocol only |
| Code Mode | `./codemode` subpath | excluded |
| Cloudflare | SQLite/JSONL adapters | excluded |
| Benchmarks | `./testing` helpers | excluded |
| Chord | public dependency | internal implementation |
| Storage interoperability | shared format | incidental, not a contract |
| Storage close identity | `close()` returns one promise per call | `SqliteStorage.close()` returns the one shared `asyncio.Future`, so a repeated close is identity-stable and the database closes only once; the `Storage` protocol accepts any awaitable |
| `Context` | Chord `Context` | minimal abort signal + value bag |
| Model access | `pi-ai` dependency | `Model` protocol + OpenAI/Anthropic extras |
| Record mutability | TypeScript `readonly` types | entry records are deep-frozen `FrozenDict`/`FrozenList` values |
| Draft property deletion | assigning `undefined` deletes the property | use `del draft.key`; assigning `None` stores JSON `null` |
| Definition callbacks | methods invoked with the definition as `this` | a callback whose first parameter is named `self` or `definition` receives the definition bound |
| Definition constructors | identity functions; they check nothing | `define_*` constructors validate a required non-empty name (and a task's version) eagerly; `section` and the registry check section rules |
| `snapshotAsOf` on a latest-only document | rejected by the TypeScript overloads | rejected at runtime with `TypeError` |
| Context cutoff | `context(context, { at })` and `context(conversationId, context, { at })` | `context(conversation_id, context, at=...)`; `Conversation.context()` and `TaskRuntime.context()` accept the cutoff as a keyword mapping |
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
| Failure and teardown-signal classification | `pi_durable.errors` | tested (`test_harness_tasks_recovery.py`) |
| Entry draft validation and immutable record construction | `pi_durable.types` | tested (`test_entries.py`) |
| Session test constructor and failure latch | `pi_durable.session.session` | tested (`test_session_tables.py`, `test_harness_storage_failure.py`) |
| Internal commit observers and contained host callbacks | `pi_durable.session.session` | tested (`test_harness_storage_failure.py`) |
| Mutable document drafts backed by strict-JSON copies | `pi_durable.session.drafts` | tested (`test_session_states.py`, `test_session_tables.py`) |
| Chord-compatible JSON operation applier | `pi_durable.delta` | tested (`test_memory_storage.py`) |
| Serialized committed document watches | `pi_durable.session.observation` | tested (`test_session_watches.py`, `test_session_checkpoints_migrations.py`) |
| Document cache sizes, retention normalization, and the suite default | `pi_durable.session.session` | tested (`test_session_document_retention.py`) |
| Context bounds capture and derivation | `pi_durable.harness.context` | tested (`test_harness_context.py`) |
| Kept context ranges for extended reads and retention | `pi_durable.harness.context` | tested (`test_harness_context_ranges.py`) |
| Session-level context read, as `Conversation.context()` uses it | `pi_durable.session.session` | tested (`test_harness_context.py`) |
| Task scheduler, ownership indexes, and task runtime | `pi_durable.harness.scheduler` | tested (`test_harness_tasks.py`, `test_harness_ownership.py`, `test_harness_cancellation_barrier.py`, `test_harness_tasks_recovery.py`, `test_harness_scheduler_indexes.py`, `test_harness_scheduler_retention.py`, `test_harness_storage_failure.py`) |
| Harness open/root/conversation/commit/inspect/close surface | `pi_durable.harness.harness` | tested (`test_harness_tasks.py`, `test_harness_lifecycle.py`, `test_harness_tasks_recovery.py`, `test_harness_scheduler_retention.py`, `test_harness_storage_failure.py`) |
| Extension registry, agent resolution, and hook dispatch | `pi_durable.harness.registry`, `pi_durable.harness.agent` | tested (`test_harness_tasks.py`, `test_harness_lifecycle.py`, `test_harness_registry_agent.py`) |
| Session clock, guarded-storage accessor, and before-close join hook | `pi_durable.session.session` | tested (`test_harness_lifecycle.py`, `test_harness_storage_failure.py`) |
| Stdlib SQLite database adapter, file entrypoint, and connection options | `pi_durable.storage.sqlite.posix` | tested (`test_sqlite_facade.py`, `test_sqlite_storage.py`) |
| OpenAI model adapter | `pi_durable.providers.openai` | planned |
| Anthropic model adapter | `pi_durable.providers.anthropic` | planned |

## Milestones

- [x] 1. Scaffold: vendored spec, decision record, parity matrix, CI, package layout.
- [x] 2. Kernel: types, IDs, JSON, errors, entries, documents, Session/Tx/forks.
- [x] 3. Storage: memory, JSONL, SQLite, conformance suites.
- [x] 4. Tasks, scheduler, submissions, document observation.
- [ ] 5. Harness: agent, registry, hooks, prompt, model, generation, tool rounds, usage, live.
- [ ] 6. `env`, coding tools, `tool-search`.
- [ ] 7. Compaction, events/view, task graph, extension reload.
- [ ] 8. Full translated suite green, docs, version `1.3.0`, release.

## Updating this file

1. Change status only in the PR that changes the code or test.
2. A row moves to `tested` only when its translated test passes in CI.
3. Add a new row for every new upstream export or spec section; never renumber existing rows.
4. If a row becomes excluded, record why in **Excluded subpaths** or **Allowed deviations**.

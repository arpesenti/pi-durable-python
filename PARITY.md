# Parity Matrix

**Baseline:** `@earendil-works/pi-durable` **v1.4.0**, tag `v1.4.0`, commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`.

**Normative reference:** [`docs/upstream/spec-1.4.0.md`](./docs/upstream/spec-1.4.0.md).

**Decision records:** [`docs/adr/0001-parity-baseline-and-boundary.md`](./docs/adr/0001-parity-baseline-and-boundary.md)
and [`docs/adr/0002-upstream-1-4-0-scope.md`](./docs/adr/0002-upstream-1-4-0-scope.md).

**Audit report:** [`docs/parity-report.md`](./docs/parity-report.md).

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

- **Root:** the v1.4.0 root export (`src/index.ts`).
- **Subpaths:** `env`, `tools`, `tool-search`, `testing`, and the storage backends.
- **Providers:** OpenAI and Anthropic adapters as optional extras.
- **Excluded:** Code Mode, the image implementation (`ImageProcessor` protocol remains),
  Cloudflare adapters, IndexedDB and `test-workers`, telemetry, pi-ai client identity,
  PowerShell, Windows support, and benchmark helpers.
- **Storage:** the v1.4.0 backends are ported faithfully, including the SQLite schema-version-2
  rebuild. Cross-language storage interoperability is incidental, not a supported contract.

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
| `addTools` | value | `add_tools` | test_harness_tools.py | tested |
| `AgentDoc` | value | `AgentDoc` | test_harness_registry_agent.py | tested |
| `configure` | value | `configure` | test_harness_registry_agent.py | tested |
| `DEFAULT_COMPACTION_POLICY` | value | `DEFAULT_COMPACTION_POLICY` | test_harness_registry_agent.py | tested |
| `DEFAULT_PROGRESS_POLICY` | value | `DEFAULT_PROGRESS_POLICY` | test_harness_registry_agent.py | tested |
| `DEFAULT_RESTART_LIMIT` | value | `DEFAULT_RESTART_LIMIT` | test_harness_blocked.py, test_harness_registry_agent.py | tested |
| `DEFAULT_RETRY_POLICY` | value | `DEFAULT_RETRY_POLICY` | test_harness_registry_agent.py | tested |

### `./harness/compaction.ts` → `pi_durable.harness.compaction`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `CompactionTask` | value | `CompactionTask` | test_harness_compaction.py | tested |
| `CompactionCheckpoint` | type | `CompactionCheckpoint` | test_harness_compaction.py | tested (including the recovery cases) |
| `CompactionInput` | type | `CompactionInput` | test_harness_compaction.py | tested |
| `SummaryRequest` | type | `SummaryRequest` | test_harness_compaction.py | tested |

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
| `watchEvents` | value | `watch_events` | test_harness_events.py | tested |
| `AgentEvent` | type | `AgentEvent` | test_harness_events.py | tested |
| `AgentEventStream` | type | `AgentEventStream` | test_harness_events.py | tested |
| `MessageChange` | type | `MessageChange` | test_harness_events.py | tested |
| `SnapshotEvent` | type | `SnapshotEvent` | test_harness_events.py | tested |
| `ToolEventCall` | type | `ToolEventCall` | test_harness_events.py | tested |

### `./harness/generation.ts` → `pi_durable.harness.generation`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `GenerationTask` | value | `GenerationTask` | test_harness_generation.py | tested |
| `GenerationCheckpoint` | type | `GenerationCheckpoint` | test_harness_generation.py | tested |
| `GenerationInput` | type | `GenerationInput` | test_harness_generation.py | tested |
| `GenerationResult` | type | `GenerationResult` | test_harness_generation.py | tested |

### `./harness/harness.ts` → `pi_durable.harness.harness`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `DEFAULT_HARNESS_IDENTITY` | value | — | — | excluded (pi-ai client identity; see ADR 0002) |
| `Harness` | value | `Harness` | test_harness_tasks.py, test_harness_ownership.py, test_harness_lifecycle.py, test_harness_cancellation_barrier.py, test_harness_tasks_recovery.py, test_harness_scheduler_indexes.py, test_harness_scheduler_retention.py, test_harness_storage_failure.py, test_harness_submissions.py, test_harness_inbox.py, test_harness_input_entries.py, test_harness_generation.py, test_harness_generation_recovery.py, test_harness_usage.py, test_harness_conversations.py, test_harness_inspect.py, test_harness_sleep.py, test_harness_blocked.py, test_harness_reports.py, test_harness_host_inputs.py, test_harness_failed_run_inbox.py | tested |
| `Harness.wake` | value | `Harness.wake` | test_harness_sleep.py, test_harness_blocked.py | tested |
| `Harness.retryTask` | value | `Harness.retry_task` | test_harness_blocked.py | tested |
| `safeNow` | value | `safe_now` | test_harness_storage_failure.py, test_harness_reports.py | tested |
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
| `LiveDoc` | value | `LiveDoc` | test_harness_submissions.py, test_harness_inbox.py, test_harness_generation.py, test_harness_live_deltas.py, test_harness_tools.py, test_harness_tools_recovery.py | tested |
| `CompactionFailure` | type | `CompactionFailure` | test_harness_compaction.py, test_harness_events.py | tested (live status and its event stream) |
| `CompactionStatus` | type | `CompactionStatus` | test_harness_compaction.py, test_harness_events.py | tested (live status and the late-joiner snapshot) |
| `LiveState` | type | `LiveState` | test_harness_submissions.py, test_harness_inbox.py, test_harness_generation.py, test_harness_live_deltas.py, test_harness_tools.py | tested |
| `NestedToolSlot` | type | `NestedToolSlot` | test_harness_nested_tools.py | tested |
| `NestedToolSummary` | type | `NestedToolSummary` | test_harness_nested_tools.py | tested |
| `ToolSlot` | type | `ToolSlot` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_live_deltas.py | tested |

### `./harness/provider.ts` → `pi_durable.harness.provider`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ProviderDoc` | value | `ProviderDoc` | test_harness_generation.py, test_provider_session_cache_e2e.py | tested |
| `ProviderState` | type | `ProviderState` | test_harness_generation.py, test_provider_session_cache_e2e.py | tested |

### `./harness/registry.ts` → `pi_durable.harness.registry`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createRegistry` | value | `create_registry` | test_harness_tasks.py, test_harness_ownership.py, test_harness_lifecycle.py, test_harness_cancellation_barrier.py, test_harness_registry_agent.py | tested |

### `./harness/scheduler.ts` → `pi_durable.harness.scheduler`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `BlockedReason` | type | `BlockedReason` | test_harness_blocked.py, test_harness_inspect.py, test_harness_task_graph.py | tested |

### `./harness/telemetry.ts` → `pi_durable.harness.telemetry`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `DURABLE_TELEMETRY_SCHEMA` | value | — | — | excluded (host observability; see ADR 0002) |

### `./harness/task-graph.ts` → `pi_durable.harness.task_graph`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `TaskGraph` | type | `TaskGraph` | test_harness_task_graph.py | tested |
| `TaskGraphNode` | type | `TaskGraphNode` | test_harness_task_graph.py | tested |
| `TaskGraphState` | type | `TaskGraphState` | test_harness_task_graph.py | tested |
| `TaskGraphWatch` | type | `TaskGraphWatch` | test_harness_task_graph.py | tested |

### `./harness/tool.ts` → `pi_durable.harness.tool`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `appendToolResult` | value | `append_tool_result` | test_harness_tools.py, test_harness_nested_tools.py | tested |
| `harnessError` | value | `harness_error` | test_harness_tools.py, test_harness_nested_tools.py, test_harness_structured_output.py | tested |
| `NestedCallDoc` | value | `NestedCallDoc` | test_harness_nested_tools.py, test_harness_nested_tools_restart.py, test_harness_structured_output.py | tested |
| `ToolTask` | value | `ToolTask` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_nested_tools.py, test_harness_nested_tools_restart.py | tested |
| `NestedCallState` | type | `NestedCallState` | test_harness_nested_tools.py, test_harness_structured_output.py | tested |
| `ToolTaskCheckpoint` | type | `ToolTaskCheckpoint` | test_harness_tools_recovery.py | tested |
| `ToolTaskInput` | type | `ToolTaskInput` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_nested_tools.py, test_harness_nested_tools_restart.py | tested |
| `ToolTaskResult` | type | `ToolTaskResult` | test_harness_tools.py, test_harness_nested_tools.py | tested |

### `./harness/types.ts` → `pi_durable.harness.types`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `AfterFailedRun` | type | `AfterFailedRun` | test_harness_failed_run_inbox.py | tested |
| `Agent` | type | `Agent` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `AgentChange` | type | `AgentChange` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `AgentState` | type | `AgentState` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `AnyTask` | type | `AnyTask` | test_harness_tasks.py, test_harness_ownership.py | tested |
| `CompactionHooks` | type | `CompactionHooks` | test_harness_compaction.py | tested |
| `CompactionPolicy` | type | `CompactionPolicy` | test_harness_generation.py, test_harness_compaction.py | tested |
| `CompactionReason` | type | `CompactionReason` | test_harness_compaction.py | tested |
| `CompactionResult` | type | `CompactionResult` | test_harness_compaction.py | tested |
| `CompactionSettings` | type | `CompactionSettings` | test_harness_compaction.py, test_harness_generation.py | tested |
| `ContextView` | type | `ContextView` | test_harness_context.py | tested |
| `Conversation` | type | `Conversation` | test_harness_conversations.py, test_harness_inbox.py, test_harness_ownership.py, test_harness_compaction.py, test_harness_view.py | tested (creation, fork, agent, context, entries, compact, reset, abort, idle waits, view, and watch) |
| `ConversationAbortOptions` | type | `ConversationAbortOptions` | test_harness_ownership.py | tested |
| `ConversationCreateOptions` | type | `ConversationCreateOptions` | test_harness_conversations.py | tested |
| `ConversationHandle` | type | `ConversationHandle` | test_harness_ownership.py | tested |
| `ConversationInit` | type | `ConversationInit` | test_harness_conversations.py | tested |
| `ConversationRetryPolicy` | type | `ConversationRetryPolicy` | test_harness_generation.py | tested |
| `ConversationStreamOptions` | type | `ConversationStreamOptions` | test_harness_generation.py | tested |
| `ConversationWatch` | type | `ConversationWatch` | test_harness_view.py | tested |
| `EnvTarget` | type | `EnvTarget` | test_harness_generation.py | tested |
| `Extension` | type | `Extension` | test_harness_tasks.py, test_harness_ownership.py, test_harness_registry_agent.py | tested |
| `GenerationHooks` | type | `GenerationHooks` | test_harness_generation.py | tested |
| `HarnessInspection` | type | `HarnessInspection` | test_harness_tasks.py, test_harness_cancellation_barrier.py, test_harness_inspect.py | tested |
| `HarnessOptions` | type | `HarnessOptions` | test_harness_tasks.py, test_harness_lifecycle.py | tested |
| `HarnessSettings` | type | `HarnessSettings` | test_harness_generation.py | tested |
| `HookApi` | type | `HookApi` | test_harness_tools.py, test_harness_tools_recovery.py | tested |
| `HookRegistration` | type | `HookRegistration` | test_harness_tasks.py | tested |
| `HookResult` | type | `HookResult` | test_harness_tools.py | tested |
| `HooksOf` | type | `HooksOf` | test_types.py, test_spec_usage.py | tested (erased to `Any`; see Allowed deviations) |
| `HostCallback` | type | `HostCallback` | test_harness_reports.py | tested |
| `InputEntry` | type | `InputEntry` | test_harness_input_entries.py | tested |
| `InputSubmissionDraft` | type | `InputSubmissionDraft` | test_harness_submissions.py, test_harness_input_entries.py | tested |
| `ModelRef` | type | `ModelRef` | test_harness_registry_agent.py | tested |
| `NestedToolExecutionResult` | type | `NestedToolExecutionResult` | test_harness_nested_tools.py, test_harness_nested_tools_restart.py, test_harness_structured_output.py | tested |
| `ProgressPolicy` | type | `ProgressPolicy` | test_harness_generation.py | tested |
| `PromptInput` | type | `PromptInput` | test_harness_prompt.py | tested |
| `PromptSection` | type | `PromptSection` | test_harness_registry_agent.py, test_harness_prompt.py | tested |
| `QueueMode` | type | `QueueMode` | test_harness_inbox.py | tested |
| `ReportFrom` | type | `ReportFrom` | test_harness_reports.py | tested |
| `ReportInfo` | type | `ReportInfo` | test_harness_reports.py, test_harness_blocked.py, test_harness_sleep.py | tested |
| `ReportTask` | type | `ReportTask` | test_harness_reports.py, test_harness_blocked.py | tested |
| `Registry` | type | `Registry` | test_harness_tasks.py, test_harness_lifecycle.py, test_harness_registry_agent.py | tested |
| `RegistryReader` | type | `RegistryReader` | test_harness_lifecycle.py, test_harness_registry_agent.py | tested |
| `RegistrySnapshot` | type | `RegistrySnapshot` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `SetTimer` | type | `SetTimer` | test_harness_sleep.py | tested |
| `Settings` | type | `Settings` | test_harness_tasks.py, test_harness_registry_agent.py, test_harness_failed_run_inbox.py | tested |
| `SettledSubmissionRecord` | type | `SettledSubmissionRecord` | test_harness_submissions.py | tested |
| `SettledTask` | type | `SettledTask` | test_harness_tasks.py | tested |
| `Submission` | type | `Submission` | test_harness_submissions.py, test_harness_inbox.py | tested |
| `SubmissionDraft` | type | `SubmissionDraft` | test_harness_submissions.py, test_harness_input_entries.py | tested |
| `TaskInspection` | type | `TaskInspection` | test_harness_cancellation_barrier.py, test_harness_inspect.py, test_harness_sleep.py, test_harness_blocked.py | tested (including `sleeping` and `blocked` `restart_limit`) |
| `ToolControl` | type | `ToolControl` | test_harness_tools.py, test_harness_tools_recovery.py | tested (a boolean `terminate`; only `True` ends a round) |
| `ToolDiagnostic` | type | `ToolDiagnostic` | test_harness_tools.py, test_harness_live_deltas.py | tested |
| `ToolExecutionApi` | type | `ToolExecutionApi` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_ownership.py, test_harness_nested_tools.py, test_harness_nested_tools_restart.py, test_harness_structured_output.py | tested (including `now()`) |
| `ToolExecutionMode` | type | `ToolExecutionMode` | test_harness_tools.py | tested |
| `ToolExecutionResult` | type | `ToolExecutionResult` | test_harness_tools.py | tested |
| `ToolHookCall` | type | `ToolHookCall` | test_harness_tools.py | tested |
| `ToolHooks` | type | `ToolHooks` | test_harness_tools.py | tested |
| `ToolRegistration` | type | `ToolRegistration` | test_harness_tasks.py, test_harness_registry_agent.py, test_harness_prompt.py, test_harness_tools.py | tested |
| `UserInput` | type | `UserInput` | test_harness_input_entries.py | tested |
| `Wake` | type | `Wake` | test_harness_sleep.py, test_harness_blocked.py | tested |
| `Wrap` | type | `Wrap` | test_harness_registry_agent.py | tested |

### `./harness/usage.ts` → `pi_durable.harness.usage`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `UsageDoc` | value | `UsageDoc` | test_harness_usage.py, test_harness_generation.py | tested |
| `UsageState` | type | `UsageState` | test_harness_usage.py, test_harness_generation.py | tested |

### `./harness/view.ts` → `pi_durable.harness.view`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ConversationView` | type | `ConversationView` | test_harness_view.py | tested |

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
| `DocumentReader` | type | `DocumentReader` | test_harness_prompt.py | tested |
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
| `NextTaskState` | type | `NextTaskState` | test_types.py, test_harness_sleep.py | tested (includes `sleeping`) |
| `Page` | type | `Page` | test_types.py, test_storage_protocol.py | tested |
| `PhaseHandler` | type | `PhaseHandler` | test_types.py | tested |
| `RewindableConversationDocFamilyToken` | type | `RewindableConversationDocFamilyToken` | test_session_definitions.py | tested |
| `RewindableConversationDocToken` | type | `RewindableConversationDocToken` | test_session_definitions.py | tested |
| `RewindableConversationSemantics` | type | `RewindableConversationSemantics` | test_types.py | tested |
| `RunningTask` | type | `RunningTask` | test_types.py | tested |
| `ScanOrder` | type | `ScanOrder` | test_types.py | tested |
| `Seq` | type | `Seq` | test_ids.py, test_types.py | tested |
| `Session` | type | `Session` | test_session_tables.py | tested |
| `SessionDocFamilyToken` | type | `SessionDocFamilyToken` | test_session_definitions.py | tested |
| `SessionDocToken` | type | `SessionDocToken` | test_session_definitions.py | tested |
| `SessionEnd` | type | `SessionEnd` | test_types.py, test_harness_storage_failure.py | tested |
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
| `TaskQuery` | type | `TaskQuery` | test_types.py, test_storage_protocol.py, test_testing_conformance.py | tested (scans `sleeping` tasks) |
| `TaskRecord` | type | `TaskRecord` | test_types.py, test_storage_protocol.py | tested |
| `TaskRuntime` | type | `TaskRuntime` | test_harness_tasks.py, test_harness_scheduler_indexes.py, test_harness_scheduler_retention.py | tested |
| `TaskState` | type | `TaskState` | test_types.py, test_harness_sleep.py | tested (includes `sleeping`) |
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
| `rangeDecoder` | value | `range_decoder` | test_env_line_scan.py | tested |
| `StreamDecoder` | value | `StreamDecoder` | test_env_line_scan.py | tested |
| `startsWithBom` | value | `starts_with_bom` | test_env_line_scan.py | tested |
| `LineScanner` | value | `LineScanner` | test_env_line_scan.py, test_env_conformance.py | tested |
| `ok` | value | `ok` | test_env_filesystem.py, test_env_posix.py | tested |
| `err` | value | `err` | test_env_filesystem.py, test_env_posix.py | tested |
| `getOrThrow` | value | `get_or_throw` | test_env_filesystem.py, test_env_posix.py | tested |
| `getOrUndefined` | value | `get_or_undefined` | test_env_filesystem.py, test_env_posix.py | tested |
| `toError` | value | `to_error` | test_env_filesystem.py, test_env_posix.py | tested |
| `FileError` | value | `FileError` | test_env_filesystem.py, test_env_posix.py | tested |
| `ExecutionError` | value | `ExecutionError` | test_env_posix.py, test_env_conformance.py | tested |
| `Result` | type | `Result` | test_env_filesystem.py, test_env_posix.py | tested |
| `FileKind` | type | `FileKind` | test_env_filesystem.py, test_env_posix.py | tested |
| `FileErrorCode` | type | `FileErrorCode` | test_env_filesystem.py, test_env_posix.py | tested |
| `ExecutionErrorCode` | type | `ExecutionErrorCode` | test_env_posix.py | tested |
| `FileInfo` | type | `FileInfo` | test_env_filesystem.py, test_env_posix.py | tested |
| `TextLine` | type | `TextLine` | test_env_posix.py, test_env_conformance.py | tested |
| `TextLineReader` | type | `TextLineReader` | test_env_posix.py, test_env_conformance.py | tested |
| `BinaryReader` | type | `BinaryReader` | test_env_posix.py, test_env_conformance.py | tested |
| `WatchTarget` | type | `WatchTarget` | test_env_conformance.py, test_env_posix.py | tested |
| `WatchChange` | type | `WatchChange` | test_env_conformance.py, test_env_posix.py | tested |
| `FileWatcher` | type | `FileWatcher` | test_env_conformance.py, test_env_posix.py | tested |
| `LineScan` | type | `LineScan` | test_env_line_scan.py, test_env_conformance.py | tested |
| `DirReader` | type | `DirReader` | test_env_posix.py, test_env_conformance.py | tested |
| `ListFilesOptions` | type | `ListFilesOptions` | test_env_conformance.py | tested |
| `FileListEntry` | type | `FileListEntry` | test_env_conformance.py | tested |
| `FileListing` | type | `FileListing` | test_env_conformance.py | tested |
| `FileSystem` | type | `FileSystem` | test_env_filesystem.py, test_env_posix.py, test_env_conformance.py | tested |
| `ShellSpillOptions` | type | `ShellSpillOptions` | test_env_posix.py, test_env_conformance.py | tested |
| `ShellExecResult` | type | `ShellExecResult` | test_env_posix.py, test_env_conformance.py | tested |
| `ShellExecOptions` | type | `ShellExecOptions` | test_env_posix.py, test_env_conformance.py | tested |
| `ShellOutputWindow` | type | `ShellOutputWindow` | test_env_posix.py, test_env_conformance.py | tested |
| `ShellOutputSkip` | type | `ShellOutputSkip` | test_env_posix.py, test_env_conformance.py | tested |
| `ShellOutputInfo` | type | `ShellOutputInfo` | test_env_posix.py, test_env_conformance.py | tested |
| `Shell` | type | `Shell` | test_env_posix.py, test_env_conformance.py | tested |
| `ExecutionEnv` | type | `ExecutionEnv` | test_env_posix.py, test_env_conformance.py | tested |

### `src/env/node.ts` → `pi_durable.env.posix`

Python replaces the Node backend with a POSIX backend.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `NodeExecutionEnv` | value | `PosixExecutionEnv` | test_env_posix.py, test_env_conformance.py | tested |
| `NodeWatchOptions` | type | `PosixWatchOptions` | test_env_posix.py | tested |

`test_env_filesystem.py` exercises the filesystem result shapes and the operations the JSONL
backend and its failure-injection tests need. `test_env_conformance.py` drives the translated
public `ExecutionEnv` conformance suite, and `test_env_posix.py` covers the POSIX backend's shell,
reader, spill, watch-budget, and filesystem cases. `test_env_line_scan.py` and
`test_env_truncate.py` cover the decoding, line-scan, and truncation helpers.

### `src/tools/index.ts` → `pi_durable.tools`

Coding tools; PowerShell is excluded.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createBashTool` | value | `create_bash_tool` | test_tools.py | tested |
| `createPowerShellTool` | value | `create_power_shell_tool` | — | excluded |
| `createEditTool` | value | `create_edit_tool` | test_tools.py | tested |
| `DEFAULT_IMAGE_LIMITS` | value | `DEFAULT_IMAGE_LIMITS` | test_tools.py | tested |
| `createReadTool` | value | `create_read_tool` | test_tools.py, test_tools_read_differential.py | tested |
| `createWriteTool` | value | `create_write_tool` | test_tools.py | tested |
| `createCodingTools` | value | `create_coding_tools` | test_tools.py | tested |
| `CodingTools` | value | `CodingTools` | test_tools.py | tested |
| `BashExecution` | type | `BashExecution` | test_tools.py | tested |
| `BashPrepare` | type | `BashPrepare` | test_tools.py | tested |
| `BashToolInput` | type | `BashToolInput` | test_tools.py | tested |
| `BashToolOptions` | type | `BashToolOptions` | test_tools.py | tested |
| `PowerShellToolInput` | type | `PowerShellToolInput` | — | excluded |
| `PowerShellToolOptions` | type | `PowerShellToolOptions` | — | excluded |
| `EditToolDetails` | type | `EditToolDetails` | test_tools.py | tested |
| `EditToolInput` | type | `EditToolInput` | test_tools.py | tested |
| `ImageLimits` | type | `ImageLimits` | test_tools.py | tested |
| `ImageProcessor` | type | `ImageProcessor` | test_tools.py | tested |
| `PreparedImage` | type | `PreparedImage` | test_tools.py | tested |
| `ReadToolDetails` | type | `ReadToolDetails` | test_tools.py | tested |
| `ReadToolInput` | type | `ReadToolInput` | test_tools.py | tested |
| `ReadToolOptions` | type | `ReadToolOptions` | test_tools.py | tested |
| `WriteToolInput` | type | `WriteToolInput` | test_tools.py | tested |

`test_tools.py` translates the upstream `tools` suite: bash streaming, spill, timeout, prefix, and
`prepare`; read offsets, limits, truncation, image detection, and path safety; write and edit
serialization through the per-file mutation queue, diffs, BOM/CRLF preservation, symlinks, and
argument repair. Image cases that need a real decoder use a test-double `ImageProcessor`; there is
no image implementation. `test_tools_read_differential.py` translates the differential suite: 400
random files and four offset/limit pairs each, plus an animated PNG found by walking its chunks.

### `src/tool-search/index.ts` → `pi_durable.tool_search`

BM25 tool search.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createToolSearchExtension` | value | `create_tool_search_extension` | test_tool_search.py | tested |
| `createToolSearchTool` | value | `create_tool_search_tool` | test_tool_search.py | tested |
| `DEFAULT_TOOL_SEARCH_LIMIT` | value | `DEFAULT_TOOL_SEARCH_LIMIT` | test_tool_search.py | tested |
| `TOOL_SEARCH_TOOL_NAME` | value | `TOOL_SEARCH_TOOL_NAME` | test_tool_search.py | tested |
| `ToolSearchToolDetails` | type | `ToolSearchToolDetails` | test_tool_search.py | tested |

`test_tool_search.py` translates the upstream `tool-search` suite and the BM25 behavior it rests
on. The translated cases cover deferred tools staying off the offer until `tool_search` loads them
and remaining callable by other tools; the exact result text and `modelTools` `{ add }` write; load
order rather than registry order; names recorded without the registry, including unknown and
non-deferred ones, with `remove` winning until a load lifts it; an extension disappearing and
coming back; per-branch loads and the post-reset baseline; and an unrelated query loading nothing.
The unit cases pin tokenization (camel-case and acronym boundaries, stop words, naive plural stems),
BM25 length normalization, ties in document order, the limit, and the empty-query and positive-
integer-limit diagnostics. `pi_durable.search` is the internal port of `src/search.ts`; it is not
a public subpath export.

### `src/testing/index.ts` → `pi_durable.testing`

Conformance helpers; benchmarks are excluded.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createExpectAssertions` | value | `create_expect_assertions` | test_testing_conformance.py | tested |
| `createEnvConformance` | value | `create_env_conformance` | test_env_conformance.py | tested |
| `registerEnvConformance` | value | `register_env_conformance` | test_env_conformance.py | tested |
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
| `EnvConformanceAssertions` | type | `EnvConformanceAssertions` | test_env_conformance.py | tested |
| `EnvConformanceCase` | type | `EnvConformanceCase` | test_env_conformance.py | tested |
| `EnvConformanceOptions` | type | `EnvConformanceOptions` | test_env_conformance.py | tested |
| `EnvConformanceProvider` | type | `EnvConformanceProvider` | test_env_conformance.py | tested |
| `StorageConformanceAssertions` | type | `StorageConformanceAssertions` | test_testing_conformance.py | tested |
| `StorageConformanceCase` | type | `StorageConformanceCase` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |
| `StorageConformanceOptions` | type | `StorageConformanceOptions` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |
| `StorageConformanceProvider` | type | `StorageConformanceProvider` | test_testing_conformance.py, test_memory_storage.py, test_jsonl_storage.py | tested |

The environment cases and their types (`createEnvConformance`, `registerEnvConformance`, and the
`EnvConformance*` types) are translated in `test_env_conformance.py`; the cases are parametrized
through the public suite and the registration helper is driven with a recording runner.

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
| `./storage/indexeddb` | Browser storage backend; out of scope like the Cloudflare adapters. |
| `./harness/telemetry` | Host observability tied to `@earendil-works/pi-telemetry`, with no normative spec section; see ADR 0002. |
| `test-workers/*` | Browser and Cloudflare Workers suites; out of scope with the adapters they exercise. |

## Spec sections

| Section | Status |
| --- | --- |
| 1 Terms and invariants | tested (atomic commits, durable publication, revoked drafts, and failure rules across test_session_*, test_harness_storage_failure.py, and test_harness_context_ranges.py) |
| 2 Core records | tested |
| 2.1 Entries and context | tested |
| 2.2 Public Harness surface | tested (runs, recovery, conversations, inspection, usage, provider, compaction, root lazy creation, the view/watch/task-graph surfaces, and the spec usage examples in test_spec_usage.py; OpenAI and Anthropic provider streaming in test_providers_openai.py and test_providers_anthropic.py) |
| 3 Documents | tested (definitions, records, access, ownership, bases, versions, and forks across test_session_*; the audit report lists the section-to-suite mapping) |
| 3.1 Definitions | tested (`checkpoint_when` is required on every definition; test_session_definitions.py) |
| 3.2 Records and lifetimes | tested |
| 3.3 Access and creation | tested (the Harness creation conveniences and `conversationCreated` hook in test_harness_conversations.py; explicit creation, historical access, and document creation in test_session_documents.py, test_session_states.py, test_session_tables.py, and test_session_checkpoints_migrations.py) |
| 3.4 Mutation ownership | tested |
| 3.5 Bases and checkpoints | tested (no default predicate: a definition that keeps only required bases returns `False`, in test_session_checkpoints_migrations.py) |
| 3.6 Versions and migrations | tested |
| 3.7 Forks | tested |
| 4 Transactions and storage ownership | tested |
| 5 Tasks | tested |
| 5.1 Definition | tested |
| 5.2 Effect sandwich | tested |
| 5.3 Terminal tasks | tested |
| 5.4 Scheduler, abort, and ownership | tested (the crash-loop guard, parking, `retry_task()`, and the graph's derived blocked reason in test_harness_blocked.py; close writes stopped tasks back as `pending`) |
| 5.5 Structured concurrency | tested |
| 5.6 Sleeping, timers, and wakeups | tested (durable sleeps, `wake()`, `on_wake`, `set_timer`, invocation timers, and timer disarm in test_harness_sleep.py) |
| 6 Submissions and inbox | tested (`after_failed_run` keep/continue/withdraw in test_harness_failed_run_inbox.py) |
| 7 Extensions, hooks, tools, and system prompt | tested (tool search in test_tool_search.py; code mode excluded) |
| 7.1 Extensions, registry, and agent resolution | tested (built-in tasks) |
| 7.2 Hooks | tested (`beforeRequest`, `afterResponse`, `onYield`, `beforeTool`, `afterTool`, and `afterTools`) |
| 7.3 Tools | tested (model-issued and nested calls, hooks, output bounds, replay, structured output, and the coding tools) |
| 7.4 System prompt and dynamic tools | tested (request integration with #14) |
| 7.5 Extension reload | tested |
| 7.6 Code mode | excluded |
| 8 Built-in tasks | tested (generation, compaction, and tool) |
| 8.1 Built-in entries | tested (`pi.user`, `pi.assistant`, `pi.system`, `pi.tool-result`, `pi.reset`, and `pi.compaction`) |
| 8.2 Live document | tested (run, generation, tool and nested slots, the complete-base rule, compaction status, and compaction failure) |
| 8.3 Generation | tested (prepare, request, retry, poll, defer, abort, streaming partials, classification, tool rounds, threshold and overflow compaction, and reopen recovery in test_harness_generation_recovery.py and test_harness_compaction.py) |
| 8.4 Tool | tested (model-issued and nested calls) |
| 8.5 Tool rounds | tested |
| 8.6 Usage | tested |
| 8.7 Compaction | tested (range selection, serialization, manual/background/blocking starts, retry backoff, placement, abort, failure, recovery, and event ordering in test_harness_compaction.py) |
| 9 Document observation and Chord | tested |
| 9.1 Document state | tested |
| 9.2 `watchDoc` | tested |
| 9.3 Conversation view | tested |
| 9.4 Agent events (experimental) | tested |
| 9.5 Task graph view | tested |
| 10 Storage contract | tested |
| 10.1 Failures | tested (the failure latch, SessionFailed, listener containment, unformattable values, clock fallback, and watch termination in test_harness_storage_failure.py; the typed `ReportInfo` of every report in test_harness_reports.py) |
| 11 Backends | tested |
| 11.1 Memory | tested |
| 11.2 SQLite | tested (schema version 2 rebuilds the task table to admit `sleeping`; the 1.3.0-to-1.4.0 upgrade and the newer-version refusal in test_sqlite_migrations.py) |
| 11.3 JSONL | tested |
| 12 API footguns | tested (every bullet mapped to a translated test in docs/parity-report.md) |
| 13 Non-goals | n/a |

The upstream document-retention suite has two Harness-level cases: one follows a Harness settings
change at its next use, and one compares agent event streams with documents evicted. Both are
translated in `test_session_document_retention.py`.

The four task-integrity suites (`harness-tasks-recovery`, `harness-scheduler-indexes`,
`harness-scheduler-retention`, and `harness-storage-failure`) are fully translated for the task
core. The recovery suite is complete. The scheduler suites translate every case whose work is
tasks and conversations, including the withdrawal cases whose inputs are queued in forgotten
conversations and the no-scan case where nothing cancels. `harness-storage-failure` translates the
failure latch, listener and view-observer containment, unformattable thrown values, and clock
fallback.

The `harness-ownership` cases that hold the Session line under a queued handle operation or drive
an owned subagent from a tool are translated in `test_harness_ownership.py`, alongside the
invocation-bound handle, subagent abort, and child-abort cases. The `harness-lifecycle` cases that
need conversation views or the task-graph watch are translated in
`test_harness_lifecycle_observations.py`: no frame from a commit that settles during close, queued
acquisitions that reject at the seal, and read-only viewers that never enable scheduling. The
tool-owned conversation case for an invocation-bound handle and the tool-hook close case are
translated now. Built-in generation cleanup, generation recovery, and usage in `harness-lifecycle`
were translated earlier, as were the open-error, close, reopen, registry-change, cancellation, and
durable-commit cases.

The submission suites are translated in three files. `harness-submissions` covers idle input and
write admission, request-ID deduplication, busy rejection, abort results, lookup, waits, close
behavior, and settlement validation. `harness-inbox` covers busy queueing, write-before-user
ordering, queue modes, resets, stale heads, display-only writes, withdrawals, failed-run inbox
retention, positional inbox operations, the empty-inbox base, and reopen. `harness-input-entries`
covers application entry kinds, reserved-kind rejection, kind/data projection, queued entries, and
withdrawal. The suites drive a generation probe through the snapshot-override seam in
`task_support` so they keep testing admission and boundaries in isolation; the real `pi.generation`
task, provider streaming, answer boundaries, and `pi.live` presentation are covered by
`test_harness_generation.py`, `test_harness_live_deltas.py`, and `test_harness_usage.py`.

The upstream `harness-live-deltas` suite is translated in `test_harness_live_deltas.py`: throttled
text partials as appends, the complete-base rule in the commits where nothing runs, the pure
`checkpointWhen` case, and the tool cases. A driven tool call shows the generation handing over to
its round with one field write each; head output appends and then only its dropped counts change; a
tail window slides as a front trim plus an append; a window beyond Chord's 64 KiB overlap scan is
written whole; details diff leaf by leaf and diagnostics append; and a sequential two-tool round
keeps every `pi.live` commit's base exactly in the moments nothing runs.

The upstream `harness-context` suite is translated in three parts. Its thirteen derivation cases run
against the Session-level `context()` read. All nine kept-range cases run against
`TaskRuntime.context()` in `test_harness_context_ranges.py`: extended reads scanning only newer
entries and honoring an earlier cutoff, returned views staying apart from the kept range, dropping
the range under zero retention, a kept read across tasks and its retention period, clock-driven
expiry, equivalent derivation from extended reads under random edits, the baseline found among
added entries, the shallow-storage deep-freeze guarantee, and a late read the finished task must
not keep. The baseline case exposed and fixed the `UNDECIDED` sentinel gap described in the audit
report.

The upstream `harness-registry` suite is translated in `test_harness_registry_agent.py`: install,
replace in place, uninstall by name with subscriber publications and immutable old snapshots;
validation that publishes nothing; installed task definitions; the progress-interval default;
default/array/`{add, remove}` extension selection with stale objects and uninstalled names;
same-name tool replacement, wrapper order, and tool filters; wrapper throw/rename drops with
reports; section ordering, same-key replacement, and the unwrapped `instructions` section; hook
collection in extension order; and the stored-agent defaults. Hook dispatch order and ordinary-throw
containment, plus reload of an extension in a live Session at its next phase, are covered there too.
The registry holds the built-in `pi.generation`, `pi.compaction`, and `pi.tool` tasks. The per-hook
composition rules for tool hooks (replacement chains, first-block wins) are covered by
`test_harness_tools.py`; compaction's single `beforeCompact` decision rule is covered by
`test_harness_compaction.py`. The `pi.generation` task-collision row is translated. The resolution
half of `harness-callers` is translated alongside: model-only, tools-only, and both callers;
`modelTools` narrowing without widening what tools may call; enabled filters; and deferred-tool
resolution through an array or `{remove, add}` (a superset of the upstream resolution cases). Its
nested `executeTool` end-to-end cases now run in `test_harness_nested_tools.py`, and its model-issued
cases in `test_harness_tools.py`; `add_tools` (the tool-round edit of
`tools`/`modelTools`) is covered there, and the deferred-loading half through `{add}` is exercised
by the resolution cases and end to end in `test_tool_search.py`.

The upstream `harness-compaction` suite is translated in `test_harness_compaction.py`: range
selection and the context estimate, transcript serialization, manual placement through a write
submission and its idle/busy/reset outcomes, `beforeCompact` decisions, retry backoff with a pinned
request, cap and failure classification, background and blocking thresholds, overflow compaction
and its retry, fork and inbox interactions, recovery across reopen, concurrent summaries, the
one-compaction rule, and the older-head-marker edits. Its `compaction_start`/`compaction_end`
ordering, late-joiner snapshots, and kept-entry view assertions now run through `watch_events` and
`view_state` in the same file, alongside the same committed state checked through `pi.live`, the
task outcome, submissions, and the context view.

The upstream `harness-prompt` suite is translated in `test_harness_prompt.py`: tagged, untagged,
omitted, wrapped, failing, and cancelled section rendering; replay in place, deletion, and
re-addition; minimal value/removal/addition patches; two-entry order rewrites; head-rebaseline
omissions and complete post-head baselines; and tool add/remove/replace/order planning riding on
the last section entry or an entry of its own. `system-order-cache-e2e` has two halves: the
leading-baseline regression is translated against committed transcripts in
`test_system_order_cache_e2e.py`; its live Anthropic prompt-cache measurement needs live
credentials, while the adapter's request ordering is covered with fakes in
`test_providers_anthropic.py`. Generation's `prepare` phase now calls these planners and
commits their entries; `test_harness_generation.py` covers the request-time integration, including
an environment failure and a section that reads a document through `input.read`.

The upstream `harness-nested-tools` and `harness-nested-tools-restart` suites are translated in
`test_harness_nested_tools.py` and `test_harness_nested_tools_restart.py`: one task, slot, and
`NestedCallDoc` member per call; keys, call IDs, reattachment, and rejected collisions; the
`callers` resolution for tools; hooks seeing the parent; unavailable, invalid, blocked, thrown,
interrupted, aborted, faulted, and orphaned results; output and details with `progress: false`;
usage; left-running calls aborted before their caller settles, including during a restart cleanup;
and `abandonOnRestart` for a replay-unsafe caller and the child tasks it creates. Agent-event
assertions for nested calls run in `test_harness_events.py` (parent IDs, the nested result, and
child-before-caller order for deep calls). The late-subscriber agent-event snapshot and the
per-slot updates for several running nested calls run in `test_harness_nested_tools.py`; the same
values are also checked through the stored `NestedCallDoc` and `pi.live` snapshots.

The upstream `harness-structured-output` suite is translated in
`test_harness_structured_output.py`: schema-less output shapes, bounds without rendered
diagnostics, the stored null, validated schema output, every broken-output path for callers and for
the model's calls, Harness-written errors, the schema tool's error-text diagnostic, `afterTool`
replacement, and `bash`'s structured output. `harness-structured` is the structured-concurrency
suite already translated with #9 and #10, and has no nested-call cases; its seven wait-graph
cases (two-task, held-owner, two-hold, background, orchestrator/worker/reviewer, diamond, and
longer-cycle) are translated in `test_harness_structured_cycles.py`.

## Allowed deviations

| Area | Upstream | Python |
| --- | --- | --- |
| Function and parameter names | camelCase | snake_case |
| Settings and policy objects | camelCase fields | snake_case fields on the Python-facing `Settings`, `HarnessSettings`, `ConversationStreamOptions`, and `DEFAULT_*_POLICY` values (`max_retries`, `keep_recent_tokens`, `partial_interval_ms`); persisted records stay camelCase |
| Tool registration fields | `Tool` plus execution fields on `ToolRegistration`, with `constrainedSampling` | `ToolRegistration.constrained_sampling`, `execution_mode`, `structured_output_schema`, `output_limits`, and `prepare_arguments` on the Python-facing registration; declarations persist `constrainedSampling` |
| Tool result and control fields | `structuredOutput`, `isError`, `addTools` | camelCase in a returned result, a stored task outcome, and an entry's message, because they are wire shapes; the registration's own fields are snake_case |
| Tool execution api | `retainedOutput()`, `outputWindow`, camelCase window fields | `retained_output()`, `output_window` with snake_case fields (`max_bytes`, `max_lines`, `min_interval_ms`, `bytes_per_second`); `details()` registers its update synchronously and returns an awaitable |
| Tool argument validation | pi-ai TypeBox `validateToolArguments` | a JSON Schema validator in `pi_durable.harness.validation` with pi-ai's primitive coercion and optional-null removal; the `schema` extra is not needed for argument validation |
| Structured output validation | TypeBox `Compile(schema).Check` | `jsonschema`'s `Draft202012Validator`, imported lazily, so the core package keeps no runtime dependencies; the `schema` extra is required only by a tool that declares `structured_output_schema` |
| Bounded output and progress | Chord `OutputBuffer` and `Progress` | `pi_durable.harness.output` with the same windows and pacing; the bounded overlap search lives in `pi_durable.delta` (`overlap_strings`, 64 KiB) |
| Class and type names | PascalCase | PascalCase |
| Persisted JSON fields | camelCase | camelCase |
| Hook names | camelCase (`beforeRequest`, `afterResponse`, `onYield`) | camelCase, because extensions register handlers by these names |
| Host execution backend | `NodeExecutionEnv`, native or polling watches | `PosixExecutionEnv` with polling watches; `mode` is accepted but the portable backend always polls |
| Execution environment options and result fields | camelCase result fields and option keys | `FileInfo`, `LineScan`, and `ShellExecResult` keep camelCase wire fields (`mtimeMs`, `firstLineEnd`, `exitCode`, `spillPath`); Python-facing option dicts use snake_case (`start_line`, `inherit_env`, `on_output`, `max_entries`, `after_bytes`) |
| Tool path resolution | the environment resolves relative and absolute paths, with no confinement | resolved against the environment and confined to its `cwd`; a path outside it raises `ValueError` |
| Node storage entrypoints | `openNode*` | `JsonlStorage.open()` / `SqliteStorage.open()`, with the caller supplying a `FileSystem` or a `SqliteDatabase`; file-backed SQLite opens through `open_posix_sqlite_storage()` |
| `Result` values | `{ ok: true, value }` / `{ ok: false, error }` object literals | frozen `Ok(value)` / `Err(error)` dataclasses with the same `ok` role and `value`/`error` fields |
| Storage conformance registration | Vitest's runtime `describe`/`it` globals | `create_storage_conformance` cases are parametrized by pytest; `register_storage_conformance` keeps the runner-injection protocol |
| JSONL serialization | `JSON.stringify` (raw UTF-8, well-formed lone-surrogate escapes) | `json.dumps(ensure_ascii=True)` (escaped non-ASCII); the decoded JSON values are identical |
| SQLite serialization | `JSON.stringify` (raw UTF-8), indexed strings kept as `JSON.stringify` output | `json.dumps(ensure_ascii=True, separators=(",", ":"))`; decoded values are identical, and indexed strings stay JSON-encoded because stdlib `sqlite3` cannot bind lone surrogates |
| PowerShell tool | exported | excluded |
| Host timers | `HarnessOptions.setTimer` defaults to `setTimeout`, background timers unreferenced, and declines background timers in Cloudflare Workers | `Timers` in `pi_durable.timers` arms the same foreground/background split through a Python `set_timer(callback, delay_ms, *, background=False)`; the default is an asyncio timer, which cannot keep a process alive, and `None` declines one. `Sleeping` tasks store deadlines on the Harness clock in both |
| Telemetry | `HarnessOptions.telemetry` and `DURABLE_TELEMETRY_SCHEMA` | excluded; see ADR 0002 |
| pi-ai client identity | `HarnessOptions.identity` and `DEFAULT_HARNESS_IDENTITY` on every model request | excluded; the provider adapters keep their own attribution headers (ADR 0002) |
| IndexedDB and `test-workers` | `IndexedDbStorage` and the Workers suites | excluded with the browser and Cloudflare backends (ADR 0002) |
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
| Provider coverage | pi-ai's full catalog and API list | `pi_durable.providers.openai` speaks OpenAI Chat Completions and `pi_durable.providers.anthropic` speaks Anthropic Messages; other APIs, OAuth flows, Bedrock, Vertex, and their compat switches are out of scope |
| Provider model catalog | generated pi-ai model database | the caller passes a sequence of `Model` values to the adapter, which resolves `(provider, modelId)` through it; `Model.cost` holds flat USD-per-million-token rates for usage pricing, so pi-ai's `cost.tiers` long-context pricing is not applied |
| Provider transport | pi-ai's own HTTP and SSE layer | the official `openai` / `anthropic` Python SDKs, imported only when the adapter needs a real client; the SDK's own retries are disabled and pi-ai's request-attempt retry policy is reimplemented against the SDK error shapes |
| Provider request options | camelCase in `settings.stream` and the persisted checkpoint | snake_case on the Python-facing `ConversationStreamOptions` (`timeout_ms`, `max_retries`, `cache_retention`); generation maps them to the camelCase provider request and checkpoint shape |
| Mid-conversation system messages | native system/tool-change beta blocks for capable models | both adapters replay every `pi.system` delta into one leading prompt and drop the later entries, pi-ai's default for models without verified mid-conversation support |
| Provider deferred responses | `fetchDeferred()`/`cancelDeferred()` on capable APIs | the Chat Completions and Messages adapters never produce a `deferred` stop reason and raise `NotImplementedError` from the deferred methods |
| Provider session identity | `sessionId` reaches every API that supports caching | the OpenAI adapter sends it as `prompt_cache_key` (64-character clamp) when cache retention is not `none`; Anthropic's cache control has no session field, so the adapter does not send it |
| Thinking requests | pi-ai effort and adaptive-thinking parameters per model | the Anthropic adapter sends budget-based `thinking` (`budget_tokens` from the thinking level) and fits it under `max_tokens`; the OpenAI adapter sends `reasoning_effort` |
| Thinking replay | signed reasoning fields replayed for matching models | both adapters drop thinking from another model; the OpenAI adapter sends no thinking back to Chat Completions, which has no portable field for it |
| OpenAI max tokens | `max_completion_tokens` or `max_tokens` chosen per endpoint compat | always `max_completion_tokens` |
| Streaming tool-call arguments | partial arguments parsed with `partial-json` while deltas arrive | the finished call carries the assembled arguments; while streaming, arguments stay `{}` until their text parses, so no partial-JSON dependency is needed |
| Anthropic error tool results with images | images split out of an error result and sent after it | images stay in the `tool_result` block; splitting them is not implemented |
| Record mutability | TypeScript `readonly` types | entry records are deep-frozen `FrozenDict`/`FrozenList` values |
| Draft property deletion | assigning `undefined` deletes the property | use `del draft.key`; assigning `None` stores JSON `null` |
| Definition callbacks | methods invoked with the definition as `this` | a callback whose first parameter is named `self` or `definition` receives the definition bound |
| Host callback arity | JavaScript ignores arguments a callback does not declare, so a one-argument `onReport` sees the error alone | `on_report` (and `safe_now`'s sink) declared with one parameter receives the error alone: the Harness inspects the callable's arity once, and passes `info` to a callback that declares it |
| Definition constructors | identity functions; they check nothing | `define_*` constructors validate a required non-empty name (and a task's version) eagerly; `section` and the registry check section rules |
| `snapshotAsOf` on a latest-only document | rejected by the TypeScript overloads | rejected at runtime with `TypeError` |
| Context cutoff | `context(context, { at })` and `context(conversationId, context, { at })` | `context(conversation_id, context, at=...)`; `Conversation.context()` and `TaskRuntime.context()` accept the cutoff as a keyword mapping |
| Document observation | Chord `AttachedReplicatedState`/`ReplicatedStateSource` | `DocumentState.subscribe(listener)` delivers `(value, context, Delivery(kind, sequence))`; `CommittedWatch` frames carry `(value, ops, context)` |
| Promise-returning watcher methods | `stop()` returns the one common promise | `CommittedWatch.stop()` and `closed` return the one common `asyncio.Future`, so each is identity-stable and awaiting a cancellation does not cancel it |
| Task result branding | `TaskId<Result>` carries the result type | `TaskId` is a `NewType`; the task record carries the result |
| Hook map extraction | `HooksOf<K>` conditionally extracts a task's hook map | `HooksOf` is an `Any` alias; Python cannot conditionally extract it from a generic task token, and `hook()` accepts the mapping directly |
| ID allocation | `mintId<I>()` returns a branded generic | `mint_id()` returns `int`; `id_from_number` applies the brand |
| Version | `1.4.0` | `1.4.0` lockstep |

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
| System prompt replay, section rendering, and positional entry planning | `pi_durable.harness.prompt` | tested (`test_harness_prompt.py`, `test_system_order_cache_e2e.py`) |
| Model protocol (identity, input types, input limits, and cost rates) plus pi-ai retry, overflow, and token-estimate classifiers | `pi_durable.providers` | tested (`test_harness_generation.py`, `test_harness_usage.py`, `test_tools.py`, `test_harness_compaction.py`) |
| Live generation presentation and its complete-base rule | `pi_durable.harness.live`, `pi_durable.harness.generation` | tested (`test_harness_generation.py`, `test_harness_live_deltas.py`) |
| Provider session identity and its UUIDv7 | `pi_durable.harness.provider` | tested (`test_harness_generation.py`, `test_provider_session_cache_e2e.py`) |
| Per-conversation usage ledger and Session totals | `pi_durable.harness.usage` | tested (`test_harness_usage.py`, `test_harness_generation.py`) |
| Session-level context read, as `Conversation.context()` uses it | `pi_durable.session.session` | tested (`test_harness_context.py`) |
| Task scheduler, ownership indexes, and task runtime | `pi_durable.harness.scheduler` | tested (`test_harness_tasks.py`, `test_harness_ownership.py`, `test_harness_cancellation_barrier.py`, `test_harness_tasks_recovery.py`, `test_harness_scheduler_indexes.py`, `test_harness_scheduler_retention.py`, `test_harness_storage_failure.py`) |
| Harness open/root/conversation/commit/inspect/close surface | `pi_durable.harness.harness` | tested (`test_harness_tasks.py`, `test_harness_lifecycle.py`, `test_harness_tasks_recovery.py`, `test_harness_scheduler_retention.py`, `test_harness_storage_failure.py`, `test_harness_conversations.py`, `test_harness_inspect.py`) |
| Extension registry, agent resolution, and hook dispatch | `pi_durable.harness.registry`, `pi_durable.harness.agent` | tested (`test_harness_tasks.py`, `test_harness_lifecycle.py`, `test_harness_registry_agent.py`) |
| Session clock, guarded-storage accessor, and before-close join hook | `pi_durable.session.session` | tested (`test_harness_lifecycle.py`, `test_harness_storage_failure.py`) |
| Stdlib SQLite database adapter, file entrypoint, and connection options | `pi_durable.storage.sqlite.posix` | tested (`test_sqlite_facade.py`, `test_sqlite_storage.py`) |
| Bounded output windows and adaptive progress commits | `pi_durable.harness.output` | tested (`test_harness_output.py`, `test_harness_tools.py`, `test_harness_live_deltas.py`) |
| Tool argument repair, coercion, and validation | `pi_durable.harness.validation` | tested (`test_harness_validation.py`, `test_harness_tools.py`) |
| Built-in `pi.tool` task, result entries, and replay policy | `pi_durable.harness.tool` | tested (`test_harness_tools.py`, `test_harness_tools_recovery.py`) |
| BM25 tool search behind `tool_search` | `pi_durable.search` | tested (`test_tool_search.py`) |
| Shared truncation utilities from `truncate.ts` | `pi_durable.truncate` | tested (`test_env_truncate.py`) |
| Shared provider plumbing: catalog lookup, request options, transcript replay, cross-model message adaptation, argument parsing, request-attempt retries, usage pricing, and the stream/abort wrapper | `pi_durable.providers.base`, `.request`, `.transcript`, `.json_parse`, `.retry`, `.usage`, `.stream` | tested through the adapter suites (`test_providers_openai.py`, `test_providers_anthropic.py`) |
| OpenAI model adapter over Chat Completions | `pi_durable.providers.openai` | tested (`test_providers_openai.py`, `test_providers_extras.py`) |
| Anthropic model adapter over Messages | `pi_durable.providers.anthropic` | tested (`test_providers_anthropic.py`, `test_providers_extras.py`) |

| `ReportType`, `ReportReason` | union aliases narrowing `ReportInfo.type` and `.reason` | `pi_durable.harness.types` | used by `ReportInfo`; `test_harness_reports.py` exercises every member |
| `Timers`, `host_timers`, `native_timer` | internal timer arming behind `HarnessOptions.set_timer` (upstream `hostTimers` / `nativeTimer`) | `pi_durable.timers` | `test_harness_sleep.py`, `test_session_document_retention.py` |
| `blocked_subject`, `blocked_reason()` | what the task graph reads of a record to derive its blocked reason | `pi_durable.harness.scheduler` | `test_harness_blocked.py`, `test_harness_task_graph.py` |

The adapter suites inject fake `AsyncOpenAI`- and `AsyncAnthropic`-shaped clients, so they need no
credentials and no network. `test_providers_extras.py` runs in subprocesses that block both SDK
modules, proving the core package and both adapter modules import without either extra, and that a
missing extra names its install command. CI also installs each extra alone and runs only its
adapter suite. The adapters translate text, thinking, and tool-call deltas into the port's event
protocol; usage (including cache reads/writes and cost), stop reasons, provider response IDs, and
tool-call argument assembly are checked end to end through a Harness, and the provider session ID
reaches the request as OpenAI's prompt-cache key. Provider failures become `stopReason: "error"`
messages whose text the shared pi-ai classifiers read, so durable retry and overflow compaction
work unchanged.

## Parity audit

The v1.4.0 parity audit is recorded in [`docs/parity-report.md`](./docs/parity-report.md): all 61
in-scope upstream suites are translated, the excluded suites are listed above (telemetry,
`indexeddb-storage`, `test-workers/*`, and `codemode-checkpoints` among them), two live credential
cases skip by design, and every remaining deviation is explicit.

## Milestones

- [x] 1. Scaffold: vendored spec, decision record, parity matrix, CI, package layout.
- [x] 2. Kernel: types, IDs, JSON, errors, entries, documents, Session/Tx/forks.
- [x] 3. Storage: memory, JSONL, SQLite, conformance suites.
- [x] 4. Tasks, scheduler, submissions, document observation.
- [x] 5. Harness: agent, registry, hooks, prompt, model, generation, tool rounds, usage, live.
- [x] 6. `env`, coding tools, `tool-search`.
- [x] 7. Events/view and task graph, compaction, extension reload, and the OpenAI/Anthropic adapters.
- [x] 8. Full translated suite green, docs, version `1.3.0`, release.
- [x] 9. v1.4.0 sync: durable sleeps, timers and wakeups, the crash-loop guard, typed reports,
  host inputs as functions, `afterFailedRun`, per-model compaction settings, required
  `checkpointWhen`, and the SQLite schema-version-2 migration.
- [ ] 10. v1.4.0 release (separate ticket, as `Release 1.3.0` was).

## Updating this file

1. Change status only in the PR that changes the code or test.
2. A row moves to `tested` only when its translated test passes in CI.
3. Add a new row for every new upstream export or spec section; never renumber existing rows.
4. If a row becomes excluded, record why in **Excluded subpaths** or **Allowed deviations**.

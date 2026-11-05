# Parity Matrix

**Baseline:** `@earendil-works/pi-durable` **v1.5.0**, tag `v1.5.0`, commit
`f9a26365568127461c47a07a37dcb42c140a3ac5`.

**Normative reference:** [`docs/upstream/spec-1.5.0.md`](./docs/upstream/spec-1.5.0.md), with
[`docs/upstream/group-commit-1.5.0.md`](./docs/upstream/group-commit-1.5.0.md) for the group-commit
contract behind section 4.

**Decision records:** [`docs/adr/0001-parity-baseline-and-boundary.md`](./docs/adr/0001-parity-baseline-and-boundary.md),
[`docs/adr/0002-upstream-1-4-0-scope.md`](./docs/adr/0002-upstream-1-4-0-scope.md), and
[`docs/adr/0003-upstream-1-5-0-scope.md`](./docs/adr/0003-upstream-1-5-0-scope.md).

**Audit report:** [`docs/parity-report.md`](./docs/parity-report.md).

This file is the parity contract. Every public export and spec section gets a row. A row may
not be marked `tested` until the Python implementation exists, its translated test exists, and CI
is green.

Under a re-baseline, a row whose section the new text changed or added goes back to `planned`: the
code and suite that satisfied it at the previous baseline do not yet speak this one. The ticket
that ports the section moves the row, and is named in the row's parenthetical. A row still
`tested` describes a section this baseline's text left alone — plus the two rows under the 9.4 and
9.5 keys, which await the retirement and re-key #46 holds.

## Status legend

| Status | Meaning |
| --- | --- |
| `planned` | No implementation or test yet. |
| `code` | Implementation exists; translated test missing. |
| `tested` | Implementation and translated test pass in CI. |
| `excluded` | Deliberately out of scope for this port. |
| `n/a` | Upstream non-goal; no action. |

## Scope

- **Root:** the v1.5.0 root export (`src/index.ts`).
- **Subpaths:** `env`, `tools`, `tool-search`, `testing`, and the storage backends.
- **Providers:** OpenAI and Anthropic adapters as optional extras.
- **Excluded:** Code Mode, the image implementation (`ImageProcessor` protocol remains),
  Cloudflare adapters, IndexedDB and `test-workers`, telemetry and its host-callback member,
  pi-ai client identity, any pi-schema builder analogue (tool parameters stay plain JSON Schema),
  the attached-host storage subpath, PowerShell, Windows support, and benchmark helpers. ADR 0002
  records the 1.4.0 exclusions and ADR 0003 the 1.5.0 ones.
- **Storage:** the v1.5.0 backends are ported faithfully. `commit()` stores a group of commit
  batches in one call, each batch its own atomic commit with its own sequence; the SQLite schema
  version does not move from the previous baseline. Cross-language storage interoperability is
  incidental, not a supported contract.

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
| `ConversationIdle` | value | `ConversationIdle` | — | planned (1.5.0: an input with `when_idle="reject"` against an idle conversation writes nothing; #50) |
| `ReadAfterWrite` | value | `ReadAfterWrite` | test_errors.py | tested |
| `ReadOnlySession` | value | `ReadOnlySession` | — | planned (1.5.0: `HarnessOptions.read_only` opens a viewer of a session another process drives; #50) |
| `SessionFailed` | value | `SessionFailed` | test_errors.py | tested |
| `StorageRequestError` | value | `StorageRequestError` | test_errors.py | tested |

### `./harness/agent.ts` → `pi_durable.harness.agent`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `addTools` | value | `add_tools` | test_harness_tools.py | tested |
| `AgentDoc` | value | `AgentDoc` | test_harness_registry_agent.py, test_harness_live_migrations.py | tested (version 1 at `agent.ts:46`, unchanged by this baseline) |
| `configure` | value | `configure` | test_harness_registry_agent.py | tested |
| `DEFAULT_COMPACTION_POLICY` | value | `DEFAULT_COMPACTION_POLICY` | test_harness_registry_agent.py | tested |
| `DEFAULT_PROGRESS_POLICY` | value | `DEFAULT_PROGRESS_POLICY` | test_harness_registry_agent.py | tested |
| `DEFAULT_RESTART_LIMIT` | value | `DEFAULT_RESTART_LIMIT` | test_harness_blocked.py, test_harness_registry_agent.py | tested |
| `DEFAULT_RETRY_POLICY` | value | `DEFAULT_RETRY_POLICY` | test_harness_registry_agent.py | tested |

### `./harness/compaction.ts` → `pi_durable.harness.compaction`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `CompactionTask` | value | `CompactionTask` | test_harness_compaction.py, test_harness_live_migrations.py | tested (version 1 at `compaction.ts:116`; its input still names the trigger `reason`, which the live status spells `trigger`) |
| `CompactionCheckpoint` | type | `CompactionCheckpoint` | test_harness_compaction.py | tested (including the recovery cases) |
| `CompactionInput` | type | `CompactionInput` | test_harness_compaction.py | tested |
| `SummaryRequest` | type | `SummaryRequest` | test_harness_compaction.py | tested |

### `./harness/define.ts` → `pi_durable.harness.define`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `defineExtension` | value | `define_extension` | test_harness_tasks.py, test_harness_lifecycle.py | tested |
| `defineTool` | value | `define_tool` | test_harness_tasks.py | tested |
| `hook` | value | `hook` | test_harness_tasks.py | tested |
| `runEndTask` | value | `run_end_task` | — | planned (1.5.0: types a task an `afterRun` hook asks the generation to run; #48) |
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
| `GenerationTask` | value | `GenerationTask` | test_harness_generation.py, test_harness_tools.py, test_harness_live_migrations.py | tested (version 2 at `generation.ts:164`: a sequential round may hold calls the request did not offer, and a stored version 1 checkpoint is carried unchanged) |
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
| `LiveDoc` | value | `LiveDoc` | test_harness_submissions.py, test_harness_inbox.py, test_harness_generation.py, test_harness_live_deltas.py, test_harness_tools.py, test_harness_tools_recovery.py, test_harness_live_migrations.py | tested (version 3 at `live.ts:196`, with the in-place migrate from stored versions 1 and 2) |
| `CompactionFailure` | type | `CompactionFailure` | test_harness_compaction.py, test_harness_events.py, test_harness_live_migrations.py | tested (live status and its event stream; `trigger` names what started it, and a stored `reason` is renamed in place) |
| `CompactionStatus` | type | `CompactionStatus` | test_harness_compaction.py, test_harness_events.py, test_harness_live_migrations.py | tested (live status and the late-joiner snapshot; `trigger` names what started it, and a stored `reason` is renamed in its key order) |
| `LiveRun` | type | `LiveRun` | test_harness_live_deltas.py, test_harness_live_migrations.py, test_harness_tools.py | tested (`turns`: a round counts it and a stored run without one counts from 0; `first` and `afterRun` are #48) |
| `LiveState` | type | `LiveState` | test_harness_submissions.py, test_harness_inbox.py, test_harness_generation.py, test_harness_live_deltas.py, test_harness_tools.py, test_harness_live_migrations.py | tested |
| `NestedToolSlot` | type | `NestedToolSlot` | test_harness_nested_tools.py | tested |
| `NestedToolSummary` | type | `NestedToolSummary` | test_harness_nested_tools.py | tested |
| `RunFailure` | type | `RunFailure` | — | planned (1.5.0: why the newest run ended without an answer, kept until a run starts; #48) |
| `RunFailureReason` | type | `RunFailureReason` | — | planned (1.5.0 adds `interrupted`, `restart_limit`, `nothing_to_answer`, and `turn_limit`; #48) |
| `ToolSlot` | type | `ToolSlot` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_live_deltas.py | tested (including the `unoffered` flag a call to a tool the request did not offer carries) |

### `./harness/provider.ts` → `pi_durable.harness.provider`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ProviderDoc` | value | `ProviderDoc` | test_harness_generation.py, test_provider_session_cache_e2e.py | tested |
| `ProviderState` | type | `ProviderState` | test_harness_generation.py, test_provider_session_cache_e2e.py | tested |

### `./harness/registry.ts` → `pi_durable.harness.registry`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createRegistry` | value | `create_registry` | test_harness_tasks.py, test_harness_ownership.py, test_harness_lifecycle.py, test_harness_cancellation_barrier.py, test_harness_registry_agent.py | tested |

### `./harness/registry-reader.ts` → `pi_durable.harness.registry_reader`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `registryReader` | value | `registry_reader` | — | planned (1.5.0: a `RegistryReader` over a host-composed registry value; upstream's Chord v2 registry value is here the host's own snapshot-bearing value, the same Chord boundary ADR 0003 draws for `./storage/store`. No ticket owns it yet) |

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

### `./harness/transcript.ts` → `pi_durable.harness.transcript`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `ConversationTranscript` | value | `ConversationTranscript` | — | planned (1.5.0: keeps a client's whole transcript over a windowed conversation view, with `load_earlier`, `fill_gap`, and `resume`; #45) |
| `ConversationTranscriptOptions` | type | `ConversationTranscriptOptions` | — | planned (1.5.0: the loader and limits a transcript pages through; #45) |
| `EntryLoader` | type | `EntryLoader` | — | planned (1.5.0: the `Conversation.entries()`-shaped query a transcript loads through; #45) |
| `TranscriptItem` | type | `TranscriptItem` | — | planned (1.5.0: an entry, a `gap`, or the `earlier` marker; #45) |
| `TranscriptView` | type | `TranscriptView` | — | planned (1.5.0: a conversation view plus its `items`; #45) |

### `./harness/tool.ts` → `pi_durable.harness.tool`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `appendToolResult` | value | `append_tool_result` | test_harness_tools.py, test_harness_nested_tools.py | tested |
| `harnessError` | value | `harness_error` | test_harness_tools.py, test_harness_nested_tools.py, test_harness_structured_output.py | tested |
| `NestedCallDoc` | value | `NestedCallDoc` | test_harness_nested_tools.py, test_harness_nested_tools_restart.py, test_harness_structured_output.py, test_harness_live_migrations.py | tested (version 1 at `tool.ts:119`, unchanged by this baseline) |
| `ToolTask` | value | `ToolTask` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_nested_tools.py, test_harness_nested_tools_restart.py, test_harness_live_migrations.py | tested (version 3 at `tool.ts:138`: an `unoffered` input resolves `tool_unavailable` without touching the registry, and the migrate adds `kind` to a stored version 1 input) |
| `NestedCallState` | type | `NestedCallState` | test_harness_nested_tools.py, test_harness_structured_output.py | tested |
| `ToolTaskCheckpoint` | type | `ToolTaskCheckpoint` | test_harness_tools_recovery.py | tested |
| `ToolTaskInput` | type | `ToolTaskInput` | test_harness_tools.py, test_harness_tools_recovery.py, test_harness_nested_tools.py, test_harness_nested_tools_restart.py | tested (the model-issued shape carries `unoffered`) |
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
| `HostCallback` | type | `HostCallback` | test_harness_reports.py | tested (the exhaustive set the port reports against; upstream's `telemetry` member is excluded by ADR 0003 and asserted absent) |
| `InputEntry` | type | `InputEntry` | test_harness_input_entries.py | tested |
| `InputSubmissionDraft` | type | `InputSubmissionDraft` | test_harness_submissions.py, test_harness_input_entries.py | tested |
| `ModelRef` | type | `ModelRef` | test_harness_registry_agent.py | tested |
| `NestedToolExecutionResult` | type | `NestedToolExecutionResult` | test_harness_nested_tools.py, test_harness_nested_tools_restart.py, test_harness_structured_output.py | tested |
| `ProgressPolicy` | type | `ProgressPolicy` | test_harness_generation.py | tested |
| `PromptInput` | type | `PromptInput` | test_harness_prompt.py | tested |
| `PromptSection` | type | `PromptSection` | test_harness_registry_agent.py, test_harness_prompt.py | tested |
| `QueueMode` | type | `QueueMode` | test_harness_inbox.py | tested |
| `ReportFrom` | type | `ReportFrom` | test_harness_reports.py, test_discriminant_spellings.py | tested |
| `ReportInfo` | type | `ReportInfo` | test_harness_reports.py, test_harness_blocked.py, test_harness_sleep.py, test_discriminant_spellings.py | tested |
| `ReportTask` | type | `ReportTask` | test_harness_reports.py, test_harness_blocked.py | tested |
| `Registry` | type | `Registry` | test_harness_tasks.py, test_harness_lifecycle.py, test_harness_registry_agent.py | tested |
| `RegistryReader` | type | `RegistryReader` | test_harness_lifecycle.py, test_harness_registry_agent.py | tested |
| `RegistrySnapshot` | type | `RegistrySnapshot` | test_harness_tasks.py, test_harness_registry_agent.py | tested |
| `RunEndTask` | type | `RunEndTask` | — | planned (1.5.0: a task and typed input built by `run_end_task`; #48) |
| `RunSummary` | type | `RunSummary` | — | planned (1.5.0: how a run ended, for `after_run`: `reason`, `first`, `answer`, `last`; #48) |
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
| `ConversationView` | type | `ConversationView` | test_harness_view.py | tested (the 1.4.0 active-transcript shape; the windowed view is #45) |
| `ConversationWindow` | type | `ConversationWindow` | — | planned (1.5.0: `from`, `min` default 50, `max` default `2 * min`, validated per watch and per observer; #45) |

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
| `CommitObservation` | type | `CommitObservation` | — | planned (1.5.0: the idempotent `detach()` of an attached commit observer; #44) |
| `CommitObserver` | type | `CommitObserver` | — | planned (1.5.0: `advance(publication, context)` / `end(end)` replaces the `subscribe_commits()` listener; #44) |
| `CommitOptions` | type | `CommitOptions` | — | planned (1.5.0: the `persistence` mark one commit takes; #47) |
| `CommitPersistence` | type | `CommitPersistence` | — | planned (1.5.0: `{kind: "immediate"}` or `{kind: "deferred", within_ms?}`; #47) |
| `CommitPublication` | type | `CommitPublication` | test_types.py | tested (1.5.0 keeps the shape and narrows when it is published; #42) |
| `CommitResult` | type | `CommitResult` | test_storage_commit_batches.py | tested (1.5.0: `{stored, refused?}` — what one `Storage.commit_batches()` call stored, the refusal a resolved outcome; #38) |
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
| `DocumentCommitChange` | type | `DocumentCommitChange` | test_types.py, test_discriminant_spellings.py | tested |
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
| `NativeWatchCall` | type | `NativeWatchCall` | — | planned (1.5.0: Chord's tagged call a native watch listener takes; #43) |
| `NextTaskState` | type | `NextTaskState` | test_types.py, test_harness_sleep.py | tested (includes `sleeping`) |
| `Observation` | type | `Observation` | — | planned (1.5.0: an attached `Observer` — its value at attachment and its `detach()`; #44) |
| `Observer` | type | `Observer` | — | planned (1.5.0: the synchronous `advance(call, context)` / `end(end)` mirror of a watch, for documents, the conversation view, and the task graph; #44) |
| `Page` | type | `Page` | test_types.py, test_storage_protocol.py | tested |
| `PhaseHandler` | type | `PhaseHandler` | test_types.py | tested |
| `Report` | type | `Report` | test_harness_reports.py | tested (`on_report` takes one of these: the `ReportInfo` fields tagged by `kind`, plus the `error`; #34) |
| `RewindableConversationDocFamilyToken` | type | `RewindableConversationDocFamilyToken` | test_session_definitions.py | tested |
| `RewindableConversationDocToken` | type | `RewindableConversationDocToken` | test_session_definitions.py | tested |
| `RewindableConversationSemantics` | type | `RewindableConversationSemantics` | test_types.py | tested |
| `RunningTask` | type | `RunningTask` | test_types.py | tested |
| `ScanOrder` | type | `ScanOrder` | test_types.py | tested |
| `Seq` | type | `Seq` | test_ids.py, test_types.py | tested |
| `Session` | type | `Session` | test_session_tables.py | tested |
| `SessionDocFamilyToken` | type | `SessionDocFamilyToken` | test_session_definitions.py | tested |
| `SessionDocToken` | type | `SessionDocToken` | test_session_definitions.py | tested |
| `SessionEnd` | type | `SessionEnd` | test_types.py, test_harness_storage_failure.py, test_discriminant_spellings.py | tested (tagged by `kind`: `closed`, or `failed` with the error that failed the Session; a watch mirrors it as `ended`/`failed` — #33, #35) |
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
| `WatchCall` | type | `WatchCall` | — | planned (1.5.0: Chord's tagged listener call — `{kind: "value", value, ops, retired}`; #43) |
| `WatchEnd` | type | `WatchEnd` | test_session_watches.py, test_session_checkpoints_migrations.py, test_discriminant_spellings.py | tested (the shipped build's five ends, tagged by `kind`: `stopped` from `stop()`, `aborted` from the acquisition context, `ended`/`failed` from the Session, `listener_error`; #35) |
| `WatchHandle` | type | `WatchHandle` | test_session_watches.py, test_session_checkpoints_migrations.py | tested (`value`/`start`/`stop`/`closed`; `stop()` is idempotent and yields `{kind: "stopped"}`, `chord/src/watch.ts:46-55`; #35) |

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
| `FileChange` | type | `FileChange` | test_env_conformance.py, test_env_posix.py, test_discriminant_spellings.py | tested (1.5.0 renames `WatchChange` to this and tags the three changes by `kind`: `paths`, `overflow`, `failed`; both spellings are read and only the `kind` one is emitted — #33, #35) |
| `FileWatcher` | type | `FileWatcher` | test_env_conformance.py, test_env_posix.py | tested (`stop(context)` replaces `close(context)` since #35; readers still close) |
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
| `createBashTool` | value | `create_bash_tool` | test_tools.py, test_harness_tool_schemas.py | tested |
| `createPowerShellTool` | value | `create_power_shell_tool` | — | excluded |
| `createEditTool` | value | `create_edit_tool` | test_tools.py, test_harness_tool_schemas.py | tested |
| `DEFAULT_IMAGE_LIMITS` | value | `DEFAULT_IMAGE_LIMITS` | test_tools.py | tested |
| `createReadTool` | value | `create_read_tool` | test_tools.py, test_tools_read_differential.py, test_harness_tool_schemas.py | tested |
| `createWriteTool` | value | `create_write_tool` | test_tools.py, test_harness_tool_schemas.py | tested |
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
| `commitWrites` | value | `commit_writes` | test_storage_commit_batches.py | tested (1.5.0: the sanctioned way to seed a storage with one batch — resolves with its sequence, raises the refusal; #38, and #41 moves every remaining seeding path onto it) |
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
| `CURRENT_SQLITE_SCHEMA_VERSION` | value | `CURRENT_SQLITE_SCHEMA_VERSION` | test_sqlite_migrations.py, test_harness_live_migrations.py | tested (the stored documents retag without moving the schema, and a database written by the previous release still opens) |
| `SQLITE_MIGRATIONS` | value | `SQLITE_MIGRATIONS` | test_sqlite_migrations.py | tested |
| `SqliteStorage` | value | `SqliteStorage` | test_sqlite_storage.py, test_sqlite_facade.py | tested |
| `SqliteDatabase` | type | `SqliteDatabase` | test_sqlite_facade.py, test_sqlite_storage.py | tested |
| `SqliteExecutor` | type | `SqliteExecutor` | test_sqlite_facade.py, test_storage_commit_batches.py | tested |
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
| `./storage/store` | The attached-host storage subpath added at 1.5.0: `StoreStorage` keeps a Session's records as declared tables of a Chord v2 store and publishes each commit inside the batch of the host the store serves. This port has no Chord host to attach to — Chord is an internal implementation detail here (ADR 0003). The optional `publishing()` scope it exists to serve is still ported, as the companion `PublishingStorage` protocol (ADR 0003). |
| `./harness/telemetry` | Host observability tied to `@earendil-works/pi-telemetry`, with no normative spec section; see ADR 0002. |
| `test-workers/*` | Browser and Cloudflare Workers suites; out of scope with the adapters they exercise. |

## Spec sections

| Section | Status |
| --- | --- |
| 1 Terms and invariants | planned (1.5.0: a stored commit publishes once Storage made it durable, in line order, even when the call later rejects; the line runs one commit callback at a time while their storage overlaps; a deferred commit reaches watches before Storage; #42, #47) |
| 2 Core records | planned (1.5.0 retags the unions this section carries and adds the `continue` submission record, `RunFailure`, and `RunSummary`; #33, #48, #50) |
| 2.1 Entries and context | planned (the conversation view is a window over the visible entries with head markers in place; `Conversation.context()` derives the model transcript; #45) |
| 2.2 Public Harness surface | planned (one `Report` on `on_report`, the `observe_*` seams replacing the state getters and the commit subscription, `ConversationTranscript`, `read_only`, `max_turns`, `when_idle`, and `submit(tx, draft)`; #34, #44, #45, #46, #50) |
| 3 Documents | tested (definitions, records, access, ownership, bases, versions, and forks across test_session_*; the audit report lists the section-to-suite mapping) |
| 3.1 Definitions | tested (`checkpoint_when` is required on every definition; test_session_definitions.py) |
| 3.2 Records and lifetimes | tested |
| 3.3 Access and creation | planned (the Chord state getters are gone and observer attachment and windowed conversation watches replace them; #44, #45) |
| 3.4 Mutation ownership | planned (the admission, pending-batch, receipt and publication states of group commit, and what a held deferred commit does and does not show; #42, #47) |
| 3.5 Bases and checkpoints | planned (a held deferred commit runs its predicate once against the stored count and stores its base with the held operations; #47) |
| 3.6 Versions and migrations | planned (`documentState()` leaves the access-driven migration list with #44. `pi.live` is version 3 and stored versions 1 and 2 migrate in place, the persisted version constants are read at their definition sites, and the SQLite schema and JSONL format stand still: test_harness_live_migrations.py, #36) |
| 3.7 Forks | planned (fork metadata hydrates watches and mounted views only; the document-state path is gone; #44) |
| 4 Transactions and storage ownership | planned (group commit: one callback at a time with overlapping storage, the unpublished-key gate, the 256-job bound, and outcomes in line order — see the vendored group-commit document; #42) |
| 4.1 Deferred commits | planned (`CommitPersistence`, what may be held, the store on the next read of the document, the `persist_within_ms` deadline, and the store at close; #47) |
| 5 Tasks | tested |
| 5.1 Definition | planned (`TaskRuntime.entries()`, the `options` a commit takes, and the `kind`-tagged `ReportFrom`; #33, #47, #50) |
| 5.2 Effect sandwich | tested |
| 5.3 Terminal tasks | tested |
| 5.4 Scheduler, abort, and ownership | planned (a task cut off `restart_limit` times in a row ends instead of parking, with the per-task-kind outcome and `task_interrupted`, and the scheduler decides in recorded regions; #48, #49) |
| 5.5 Structured concurrency | planned (the tasks an `afterRun` hook asks for are generation children, so Stop and idle waits include them; #48) |
| 5.6 Sleeping, timers, and wakeups | planned (`on_wake` runs in a microtask the line never waits for, and the foreground timer now also arms a deferred commit's deadline; #47) |
| 6 Submissions and inbox | planned (continue submissions, `when_idle="reject"` with `ConversationIdle`, `submit(tx, draft)` inside a commit, and the answer-resolves-while-busy rule; #48, #50) |
| 7 Extensions, hooks, tools, and system prompt | tested (tool search in test_tool_search.py; code mode excluded) |
| 7.1 Extensions, registry, and agent resolution | tested (a call to a tool its request did not offer resolves `tool_unavailable` through the phase's tool lookup, and the registry is never touched for it; test_harness_tools.py, test_harness_tools_recovery.py) |
| 7.2 Hooks | planned (`afterTool` on every result the tool task writes, `beforeProviderRequest`, `afterRun` with `RunSummary` and `RunEndTask`, and `HookApi.entry()`/`entries()`; #36, #48) |
| 7.3 Tools | planned (parameters as plain JSON Schema closed by default with pi-schema's validator wording is #51, deferred tool progress is #47, and `ConversationHandle.submit(tx, ...)` is #50. `unoffered` slots and the hidden call's own task are tested in test_harness_tools.py and test_harness_live_migrations.py; #36) |
| 7.4 System prompt and dynamic tools | tested (request integration with #14) |
| 7.5 Extension reload | tested |
| 7.6 Code mode | excluded |
| 8 Built-in tasks | planned (generation and tool progress commit deferred, so a crash loses the throttle window plus what was not stored yet; #47) |
| 8.1 Built-in entries | tested (the `pi.tool-result` producer list is the tasks: an aborted round writes its own results, and an unoffered call's `tool_unavailable` result comes from its tool task; test_harness_tools.py, test_harness_tools_recovery.py) |
| 8.2 Live document | planned (`run.first`, `run.afterRun`, and `runFailure` are #48. Version 3 carries `run.turns`, `trigger` on compaction status, and `unoffered` slots, and a stored version 1 or 2 document migrates in place: test_harness_live_migrations.py, test_harness_compaction.py, test_harness_tools.py; #36) |
| 8.3 Generation | planned (the `finish` and `drain` phases, the interrupted-attempt charge under the retry policy, `nothing_to_answer`, and deferred partials are #47 and #48. `unoffered` calls get their own tool task and count the turn they cost the run: test_harness_tools.py, test_harness_generation.py; #36) |
| 8.4 Tool | planned (the `afterTool` chain over every result the task writes, with its bounds, is #49. `pi.tool` is version 3 and an `unoffered` call resolves `tool_unavailable`: test_harness_tools.py, test_harness_tools_recovery.py; #36) |
| 8.5 Tool rounds | planned (a handoff continues the run, a `terminate` round hands to `finish`, and `max_turns` ends the round `turn_limit`; #48) |
| 8.6 Usage | tested |
| 8.7 Compaction | planned (the restart-cutoff failure and the interrupted request's charge are #48 and #49. `trigger` names what started a compaction, in the live status and in what `compaction_start` and `compaction_end` report: test_harness_compaction.py, test_harness_events.py; #36) |
| 9 Document observation and Chord | tested |
| 9.1 Watches | planned (`WatchCall`, the tagged listener call, and the observer's `advance`/`end`; #43, #44. `WatchHandle`'s `value`/`start`/`stop`/`closed` and `WatchEnd`'s five `kind`-tagged ends, `aborted` among them, landed with #35. Supersedes the 1.4.0 Document state section, whose state getters #44 deletes) |
| 9.2 `watchDoc` | planned (one change object per listener, coalesced deliveries with folded operations, the 10,000-operation root replacement, and a watch that follows its address through a retirement; #43) |
| 9.3 Conversation view | planned (a per-watch, per-observer entry window with `before`, `ConversationWindow` bounds, and `ConversationTranscript`; everything a renderer draws is state here; #45, #46) |
| 9.4 Agent events (experimental) | tested (translated in test_harness_events.py; upstream deletes the section and its five types, and #46 retires the row) |
| 9.5 Task graph view | tested (translated in test_harness_task_graph.py; #46 re-keys this row to the vendored 9.4 and #44 adds the task-graph observer) |
| 10 Storage contract | tested (`commit_batches(batches, receive, context)` with per-batch checks, receipts and the refusal beside the single form, the optional `publishing()` scope, and the narrowed `mint_id()` monotonicity across reopen — test_storage_commit_batches.py over the memory, SQLite and JSONL backends; #38. The conformance suite now carries the receipt contract itself — the group stored in order against the state its earlier batches left, the prefix a refusal keeps, a later batch its own actions refuse, a copy resolved against its predecessors, and receipts checked one per stored batch by the wrapper every case runs through; #39. The durable backends' own depth landed with #40, which also pins that every backend module registers the same case list; the single form's deletion remains #41) |
| 10.1 Failures | planned (one outcome per commit of a call in line order, durable-but-unpublished commits after a failure, receipts that stand before a rejection, and the tagged observer ends; #42, #44. The tagged watch ends landed with #35) |
| 11 Backends | tested |
| 11.1 Memory | tested (one call stores its group in order, each batch checked against the state the earlier ones left, so a copy sees what its predecessors wrote and the first refusal keeps its prefix and leaves the storage serving; the receipts ride the applied batches and stand even when a later batch's application throws — test_memory_storage.py, whose conformance cases run over `MemoryStorage` through the receipt-checking wrapper; #39) |
| 11.2 SQLite | tested (one SQL transaction stores the whole call: every batch checked through a handle that tells a refusal from a failed database call, the allocation written with the counted-run member so a statement that changed no row rejects the call instead of handing the same sequence out twice, a driver of the old member set refused at its first commit, and a read concurrent with the group sees none of its batches or all of them — test_sqlite_storage.py and test_storage_commit_batches.py, the conformance cases run over `SqliteStorage` fresh and across reopen through the receipt-checking wrapper; #40) |
| 11.3 JSONL | tested (one group appended as one write of every marker, after its sidecars and their flushes, with reads held off the memory that runs ahead of the file until the append settles, so a failed append leaves nothing observable and a torn marker recovers the complete prefix before it; the refused batch keeps the earlier ones readable across reopen and the ID watermark rebuilds from what the files confirm — test_jsonl_storage.py, the conformance cases run over `JsonlStorage` fresh and across reopen through the receipt-checking wrapper; #40) |
| 12 API footguns | planned (admission inside a commit, hooks in abort handlers, busy-after-the-answer, after-run task traps, buffered watches, and the folded-operations bound; #42, #47, #48, #50) |
| 13 Non-goals | n/a (1.5.0 narrows the visible-undurable bullet to admit deferred commits; reviewed with #42 and #47) |

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
replacement, and `bash`'s structured output. Its wording is asserted on its own in
`test_harness_structured_output_wording.py`: the three `structuredOutputError` messages quoted from
upstream, the `root` path, the dotted path a JSON Pointer becomes through its `~1`/`~0` unescaping,
the `invalid` fallback, and the compile-time refusal of a schema whose `$ref` names nothing.
`test_harness_tool_schemas.py` holds the built-in parameter and structured-output schemas as
`@earendil-works/pi-schema`'s builders emit them, the accepting call for each object upstream opens
and the rejecting call for a host's object that leaves `additionalProperties` unwritten, and the
translated `tool-args-typebox` case: a tool whose `parameters` is a plain JSON Schema object reaches
`prepare_arguments` and `execute` as the value that schema types.

`harness-structured` is the structured-concurrency suite already translated with #9 and #10, and has
no nested-call cases; its seven wait-graph cases (two-task, held-owner, two-hold, background,
orchestrator/worker/reviewer, diamond, and longer-cycle) are translated in
`test_harness_structured_cycles.py`.

## Allowed deviations

| Area | Upstream | Python |
| --- | --- | --- |
| Function and parameter names | camelCase | snake_case |
| Settings and policy objects | camelCase fields | snake_case fields on the Python-facing `Settings`, `HarnessSettings`, `ConversationStreamOptions`, and `DEFAULT_*_POLICY` values (`max_retries`, `keep_recent_tokens`, `partial_interval_ms`); persisted records stay camelCase |
| Tool registration fields | `Tool` plus execution fields on `ToolRegistration`, with `constrainedSampling` | `ToolRegistration.constrained_sampling`, `execution_mode`, `structured_output_schema`, `output_limits`, and `prepare_arguments` on the Python-facing registration; declarations persist `constrainedSampling` |
| Tool result and control fields | `structuredOutput`, `isError`, `addTools` | camelCase in a returned result, a stored task outcome, and an entry's message, because they are wire shapes; the registration's own fields are snake_case |
| Tool execution api | `retainedOutput()`, `outputWindow`, camelCase window fields | `retained_output()`, `output_window` with snake_case fields (`max_bytes`, `max_lines`, `min_interval_ms`, `bytes_per_second`); `details()` registers its update synchronously and returns an awaitable |
| Tool argument validation | pi-ai `validateToolArguments`, which compiles the tool's schema with `@earendil-works/pi-schema` (`coerce: true`, `unresolvedRefs: "reject"`) | a JSON Schema validator in `pi_durable.harness.validation` with pi-ai's primitive coercion and optional-null removal; the `schema` extra is not needed for argument validation |
| Structured output validation | `@earendil-works/pi-schema`'s `compile(schema, { formats, unresolvedRefs: "reject" })`, whose first error `src/harness/tool.ts:777-801` `structuredOutputError` wraps | `jsonschema`'s `Draft202012Validator` with `referencing`, both imported lazily, so the core package keeps no runtime dependencies; the `schema` extra is required only by a tool that declares `structured_output_schema`. pi-schema is a TypeScript package that cannot be imported here, and `jsonschema` is the mature validator for this baseline's draft 2020-12, so it is what the port substitutes -- and the first error it reports is the substitute's own, both its words and the JSON Pointer it names (upstream's explainer reports an undeclared property at the property's own path as `extra: unknown field`, `packages/schema/src/explain.ts:201`, where `jsonschema` reports it at the guarded object). Everything around that substitution is upstream's: the three `structuredOutputError` messages, the `formatValidationPath` dotted path with its `~1`/`~0` unescaping, the `root` path, the `invalid` fallback, and one compiled validator per schema object (a bounded LRU keyed by its identity, Python having no `WeakMap`). Where they part company is a `$ref` that names nothing: `unresolvedRefs: "reject"` resolves it to `false` so only values at its place fail, while the port refuses the whole schema where it is compiled (`ToolSchemaError`, in upstream's own `unresolved reference <ref>` words), because `jsonschema` would otherwise discover the dangling reference lazily while a value was checked |
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
| Telemetry host callback | `HostCallback` carries `"telemetry"` for an adapter that threw while a commit was published | excluded with telemetry: the port's exhaustive host-callback set has no `telemetry` member (ADR 0003) |
| pi-schema builders | `obj()`, `num()`, and `Static<T>` type a tool's `parameters` and `structuredOutputSchema` | excluded; parameters stay plain JSON Schema dicts validated by the `schema` extra, so there is no builder DSL to port and no static type to produce (ADR 0003) |
| Object schemas closed by default | `obj(properties, options)` emits `...(open === true ? {} : { additionalProperties: false })` (`packages/schema/src/builders.ts:197`), and every built-in tool passes `{ open: true }`; an emitted schema therefore carries the keyword only where it is closed, and never for a built-in | with no builder to write the keyword, `pi_durable.harness.validation` compiles an object that names `properties` or `required` and leaves `additionalProperties` unwritten as closed, and reads `additionalProperties: true` as the data spelling of `{ open: true }` (`is_closed_by_default`); an object naming neither constrains no property name, so it stays open. The open spelling is an authoring option rather than part of the interface a host is told, so `without_open_marker` strips it deep at the one boundary a schema leaves the port, `to_tool_declaration` -- a `false` is never stripped, and a keyword holding a subschema is kept -- so an emitted built-in schema is literally the object upstream's builder emitted, not merely equal to it after normalisation |
| Attached-host storage | `StoreStorage` on Chord v2's store (`./storage/store`) | excluded; no Chord host to attach to (ADR 0003). The optional `publishing()` scope is ported as the companion `PublishingStorage` protocol |
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
| Definition constructors | identity functions; they check nothing | `define_*` constructors validate a required non-empty name (and a task's version) eagerly; `section` and the registry check section rules |
| `snapshotAsOf` on a latest-only document | rejected by the TypeScript overloads | rejected at runtime with `TypeError` |
| Context cutoff | `context(context, { at })` and `context(conversationId, context, { at })` | `context(conversation_id, context, at=...)`; `Conversation.context()` and `TaskRuntime.context()` accept the cutoff as a keyword mapping |
| Document observation | Chord `AttachedReplicatedState`/`ReplicatedStateSource` | `DocumentState.subscribe(listener)` delivers `(value, context, Delivery(kind, sequence))`; `CommittedWatch` frames carry `(value, ops, context)` |
| Promise-returning watcher methods | `stop()` returns the one common promise | `CommittedWatch.stop()` and `closed` return the one common `asyncio.Future`, so each is identity-stable and awaiting a cancellation does not cancel it |
| Task result branding | `TaskId<Result>` carries the result type | `TaskId` is a `NewType`; the task record carries the result |
| Hook map extraction | `HooksOf<K>` conditionally extracts a task's hook map | `HooksOf` is an `Any` alias; Python cannot conditionally extract it from a generic task token, and `hook()` accepts the mapping directly |
| ID allocation | `mintId<I>()` returns a branded generic | `mint_id()` returns `int`; `id_from_number` applies the brand |
| Version | `1.5.0` | `1.5.0` lockstep |

## Port additions

These are required by the chosen scope but are not upstream 1.5.0 exports.

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
| Tool argument repair, coercion, and validation; object schemas closed by default; the structured-output validator and upstream's wording | `pi_durable.harness.validation` | tested (`test_harness_validation.py`, `test_harness_tools.py`, `test_harness_structured_output.py`, `test_harness_structured_output_wording.py`, `test_harness_tool_schemas.py`) |
| Built-in `pi.tool` task, result entries, and replay policy | `pi_durable.harness.tool` | tested (`test_harness_tools.py`, `test_harness_tools_recovery.py`) |
| BM25 tool search behind `tool_search` | `pi_durable.search` | tested (`test_tool_search.py`) |
| Shared truncation utilities from `truncate.ts` | `pi_durable.truncate` | tested (`test_env_truncate.py`) |
| Pre-upgrade stored databases, written by this port's `v1.4.0` tag and opened by this release | `tests/fixtures/pre-1.5.0` (see its README) | tested (`test_harness_live_migrations.py`) |
| Shared provider plumbing: catalog lookup, request options, transcript replay, cross-model message adaptation, argument parsing, request-attempt retries, usage pricing, and the stream/abort wrapper | `pi_durable.providers.base`, `.request`, `.transcript`, `.json_parse`, `.retry`, `.usage`, `.stream` | tested through the adapter suites (`test_providers_openai.py`, `test_providers_anthropic.py`) |
| OpenAI model adapter over Chat Completions | `pi_durable.providers.openai` | tested (`test_providers_openai.py`, `test_providers_extras.py`) |
| Anthropic model adapter over Messages | `pi_durable.providers.anthropic` | tested (`test_providers_anthropic.py`, `test_providers_extras.py`) |

| `ReportKind`, `ReportReason` | union aliases narrowing `ReportInfo.kind` and `.reason` | `pi_durable.harness.types` | used by `ReportInfo`; `test_harness_reports.py` exercises every member |
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
cases skip by design, and every remaining deviation is explicit. It is that baseline's record: the
v1.5.0 audit is written beside it, not over it, when this sync's suites are green, and it names the
suites this baseline adds (`group-commit`, `deferred-commit`, `deferred-progress`, `observers`,
`transcript`, `run-failure`, `turn-limit`, `after-run`, `continue`, `submit-in-commit`,
`idle-scope`, and `restart-failure` among them) and every upstream case left untranslated.

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
- [x] 10. v1.4.0 release (separate ticket, as `Release 1.3.0` was).
- [ ] 11. v1.5.0 sync: group commit and deferred commits, observers and coalesced watches, the
  windowed conversation view and `ConversationTranscript`, run failures and the restart cutoff,
  continue submissions, run-end tasks and turn limits, JSON Schema tools, `pi.live` version 3, and
  the deleted single-write commit form, state getters, commit subscription, and agent event stream.
- [ ] 12. v1.5.0 release (separate ticket, as `Release 1.3.0` and the 1.4.0 release were).

## Updating this file

1. Change status only in the PR that changes the code or test.
2. A row moves to `tested` only when its translated test passes in CI.
3. Add a new row for every new upstream export or spec section; never renumber existing rows.
4. If a row becomes excluded, record why in **Excluded subpaths** or **Allowed deviations**.

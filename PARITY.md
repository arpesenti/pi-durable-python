# Parity Matrix

**Baseline:** `@earendil-works/pi-durable` **v1.5.0**, tag `v1.5.0`, commit
`f9a26365568127461c47a07a37dcb42c140a3ac5`.

**Normative reference:** [`docs/upstream/spec-1.5.0.md`](./docs/upstream/spec-1.5.0.md), with
[`docs/upstream/group-commit-1.5.0.md`](./docs/upstream/group-commit-1.5.0.md) for the group-commit
contract behind section 4.

**Decision records:** [`docs/adr/0001-parity-baseline-and-boundary.md`](./docs/adr/0001-parity-baseline-and-boundary.md),
[`docs/adr/0002-upstream-1-4-0-scope.md`](./docs/adr/0002-upstream-1-4-0-scope.md), and
[`docs/adr/0003-upstream-1-5-0-scope.md`](./docs/adr/0003-upstream-1-5-0-scope.md).

**Audit reports:** [`docs/parity-report.md`](./docs/parity-report.md) is the v1.4.0 audit record and
states that baseline only;
[`docs/parity-report-1.5.0.md`](./docs/parity-report-1.5.0.md) is this baseline's record and the one
owner of the v1.5.0 exclusion record.

This file is the parity contract. Every public export and spec section gets a row. A row may
not be marked `tested` until the Python implementation exists, its translated test exists, and CI
is green.

Under a re-baseline, a row whose section the new text changed or added goes back to `planned`: the
code and suite that satisfied it at the previous baseline do not yet speak this one. The ticket
that ports the section moves the row, and is named in the row's parenthetical. A row still
`tested` describes a section this baseline's text left alone.

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
- **Storage:** the v1.5.0 backends are ported faithfully. `commit_batches()` is the one commit
  shape: a call stores a group of commit batches, each batch its own atomic commit with its own
  sequence, and the retired single-write `commit()` is gone from the protocol and every backend
  (#41); code that seeds a storage writes one batch through `pi_durable.testing.commit_writes`. The
  SQLite schema version does not move from the previous baseline. Cross-language storage
  interoperability is incidental, not a supported contract.

## Root exports

### `./documents.ts` → `pi_durable.documents`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `defineDoc` | value | `define_doc` | test_session_definitions.py, test_session_documents.py, test_session_observers.py, test_session_tables.py | tested |
| `defineDocFamily` | value | `define_doc_family` | test_session_definitions.py, test_session_documents.py, test_session_observers.py, test_session_tables.py | tested |

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
| `ConversationIdle` | value | `ConversationIdle` | test_errors.py, test_harness_submit_in_commit.py | tested (an input with `when_idle="reject"` against an idle conversation raises it and writes nothing) |
| `ReadAfterWrite` | value | `ReadAfterWrite` | test_errors.py | tested |
| `ReadOnlySession` | value | `ReadOnlySession` | test_errors.py, test_harness_read_only.py | tested (`HarnessOptions.read_only` opens a viewer of a session another process drives) |
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
| `runEndTask` | value | `run_end_task` | test_harness_after_run.py | tested (a task an `afterRun` hook asks the generation to run, checked off the line and created as its child) |
| `section` | value | `section` | test_harness_registry_agent.py | tested |
| `wrapSection` | value | `wrap_section` | test_harness_registry_agent.py | tested |
| `wrapTool` | value | `wrap_tool` | test_harness_registry_agent.py | tested |

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
| `CompactionFailure` | type | `CompactionFailure` | test_harness_compaction.py, test_harness_live_migrations.py | tested (the live status and the value that records a failure; `trigger` names what started it, and a stored `reason` is renamed in place) |
| `CompactionStatus` | type | `CompactionStatus` | test_harness_compaction.py, test_harness_live_migrations.py | tested (the live status and a late consumer's first value; `trigger` names what started it, and a stored `reason` is renamed in its key order) |
| `LiveRun` | type | `LiveRun` | test_harness_live_deltas.py, test_harness_live_migrations.py, test_harness_tools.py | tested (`turns`: a round counts it and a stored run without one counts from 0; `first` names the run's first entry and a failed run carries it into `runFailure`; `afterRun` marks the run answered and waiting on its run-end tasks: test_harness_after_run.py) |
| `LiveState` | type | `LiveState` | test_harness_submissions.py, test_harness_inbox.py, test_harness_generation.py, test_harness_live_deltas.py, test_harness_tools.py, test_harness_live_migrations.py | tested |
| `NestedToolSlot` | type | `NestedToolSlot` | test_harness_nested_tools.py | tested |
| `NestedToolSummary` | type | `NestedToolSummary` | test_harness_nested_tools.py | tested |
| `RunFailure` | type | `RunFailure` | test_harness_run_failure.py, test_harness_view_run.py | tested (1.5.0: why the newest run ended without an answer, kept until a run starts. It is on the package surface, where upstream's `src/index.ts:51` puts it, with `taskId`/`inputs`/`reason` required and `detail`/`first` `NotRequired` exactly as `spec-1.5.0.md:4644-4668` declares them: `test_the_surface_run_failure_has_upstream_requiredness_and_the_whole_reason_set` pins the surface identity, the required/optional split, and the ten-reason union as a whole (`len(reasons) == 10`, so no reason is dropped, renamed or added silently), while mypy's `typeddict-item` keeps the two `TYPE_CHECKING` negatives red. What a host reads through it — the task, the inputs it was answering, the reason, the detail and the first entry — is `only_failure`, which every port case that ends a run unanswered asserts; it asserts the live document holds *nothing else*, the way upstream's `toEqual` does at `test/harness-run-failure.test.ts:49` and `:91`, where upstream's weaker `toMatchObject` cases (`:73`, `:133`, `:156`) are ones whose document still carries more; the two `harness-view-run` cases that read it now assert it as upstream does; #48) |
| `RunFailureReason` | type | `RunFailureReason` | test_harness_run_failure.py, test_harness_turn_limit.py, test_harness_continue.py | tested (the whole 1.5.0 union, and a run end produces `model_error`, `no_model`, `aborted`, `reset`, `faulted`, `orphaned`, `interrupted`, `nothing_to_answer`, `turn_limit`, and `restart_limit` (the generation cut off `settings.restart_limit` times in a row ends its run with it: test_harness_restart_failure.py)) |
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
| `ConversationTranscript` | value | `ConversationTranscript` | test_conversation_transcript.py | tested (1.5.0: keeps a client's whole transcript over a windowed conversation view, with `load_earlier`, `fill_gap`, and `resume`.) |
| `ConversationTranscriptOptions` | type | `ConversationTranscriptOptions` | test_conversation_transcript.py | tested (1.5.0: `page_size`, the entries one load fetches, default 50; exercised by the page-by-page fill, gap, and shared-load cases.) |
| `EntryLoader` | type | `EntryLoader` | test_conversation_transcript.py | tested (1.5.0: the `Conversation.entries()`-shaped query a transcript loads through.) |
| `TranscriptItem` | type | `TranscriptItem` | test_conversation_transcript.py | tested (1.5.0: an entry, a `gap`, or the `earlier` marker.) |
| `TranscriptView` | type | `TranscriptView` | test_conversation_transcript.py | tested (1.5.0: a conversation view plus its `items`.) |

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
| `Conversation` | type | `Conversation` | test_harness_conversations.py, test_harness_inbox.py, test_harness_ownership.py, test_harness_compaction.py, test_harness_view.py | tested (creation, fork, agent, context, entries, compact, reset, abort, idle waits, and the windowed `watch(context, window)`) |
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
| `RunEndTask` | type | `RunEndTask` | test_harness_after_run.py | tested (1.5.0: a task and typed input built by `run_end_task`. Exactly `{task, input}`, both required, as `src/harness/types.ts:1026` declares them: `test_the_hook_is_handed_the_surface_run_summary_and_asks_with_the_surface_task` builds one through the only entry point that makes sense and asserts its key set, and mypy's `typeddict-item` keeps the missing-`input` negative red; the same case pins the surface — `RunEndTask is pi_durable.harness.types.RunEndTask` and membership in `__all__`, where upstream re-exports it (`src/index.ts:117`) — and the tasks the port's own hooks ask for are built no other way across this suite; #48) |
| `RunSummary` | type | `RunSummary` | test_harness_after_run.py | tested (1.5.0: how a run ended, for `after_run`: `reason`, `first`, `answer`, `last`. The object handed to the hook carries exactly those four and nothing else — `test_the_hook_is_handed_the_surface_run_summary_and_asks_with_the_surface_task`, whose `set(asked[0]) == {…}` is mutation-proven both ways: adding a field to the summary at `src/pi_durable/harness/generation.py:1475` turns it red, and dropping one does too. What the fields mean is the translated cases' — `:435` and `:474` for `answer`/`first` on an answered run, `:904` and `:934` for `terminate`, `:791-799` for reading the history back with `first`/`last`, and the no-recorded-`first` case gives the answer as `first`; the same case pins `RunSummary is pi_durable.harness.types.RunSummary` and membership in `__all__`, where upstream's `src/index.ts:118` puts it; #48) |
| `SetTimer` | type | `SetTimer` | test_harness_sleep.py | tested |
| `Settings` | type | `Settings` | test_harness_tasks.py, test_harness_registry_agent.py, test_harness_failed_run_inbox.py | tested |
| `SettledSubmissionRecord` | type | `SettledSubmissionRecord` | test_harness_submissions.py | tested |
| `SettledTask` | type | `SettledTask` | test_harness_tasks.py | tested |
| `Submission` | type | `Submission` | test_harness_submissions.py, test_harness_inbox.py | tested |
| `SubmissionDraft` | type | `SubmissionDraft` | test_harness_submissions.py, test_harness_input_entries.py | tested |
| `TaskInspection` | type | `TaskInspection` | test_harness_cancellation_barrier.py, test_harness_inspect.py, test_harness_sleep.py, test_harness_blocked.py | tested (including `sleeping` and `blocked`; `pending` is the shape of a reservable task, as upstream spells it) |
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
| `ConversationView` | type | `ConversationView` | test_harness_view.py, test_conversation_transcript.py | tested (1.5.0: the mounted conversation plus this consumer's entry `window` — `entries`, `before`, and `docs`.) |
| `ConversationWindow` | type | `ConversationWindow` | test_harness_view.py | tested (1.5.0: `from`, `min` default 50, `max` default `2 * min`, validated per attached consumer.) |

### `./session/session.ts` → `pi_durable.session.session`

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createSession` | value | `create_session` | test_session_tables.py, test_session_observers.py | tested |
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
| `CommitObservation` | type | `CommitObservation` | test_session_observers.py | tested (the idempotent `detach()` of an attached commit observer, and the close that ends it `failed` after its last publication; #44) |
| `CommitObserver` | type | `CommitObserver` | test_session_observers.py, test_harness_storage_failure.py, test_harness_reports.py | tested (`advance(publication, context)` / `end(end)` replaces the `subscribe_commits()` listener: exactly what Storage made durable, handed on the line after the Session's own followers, a throw reported and the observer detached; #44) |
| `CommitOptions` | type | `CommitOptions` | test_session_deferred_commits.py | tested (1.5.0: the `persistence` mark one commit takes, and the `options` a `Session.commit()`, a `Conversation.commit()`, or a `TaskRuntime.commit()` takes (`types.ts:603-610`); #47) |
| `CommitPersistence` | type | `CommitPersistence` | test_session_deferred_commits.py | tested (1.5.0: `{kind: "immediate"}`, or `{kind: "deferred", within_ms?}` where a negative `within_ms` is refused, `0` stores right after the commit, and `Infinity` waits for another trigger alone (`types.ts:594-602`, `session.ts:515-524`); #47) |
| `CommitPublication` | type | `CommitPublication` | test_types.py | tested (1.5.0 keeps the shape) |
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
| `DocDefinition` | type | `DocDefinition` | test_session_definitions.py, test_session_observers.py | tested |
| `DocFamilyDefinition` | type | `DocFamilyDefinition` | test_session_definitions.py | tested |
| `DocFamilyToken` | type | `DocFamilyToken` | test_session_definitions.py, test_session_observers.py | tested |
| `DocToken` | type | `DocToken` | test_session_definitions.py, test_session_documents.py, test_session_observers.py | tested |
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
| `NativeWatchCall` | type | — | — | excluded (1.5.0: one of the four Chord watch types upstream re-exports without ever using — `src/index.ts:1` names it and nothing else in upstream's `src/` or `test/` does. What durable hands a listener is `WatchCall`, and an observer's `advance`/`end` take `WatchCall`/`WatchEnd` (`src/types.ts:1112-1115`); the `WatchCall` row below is `tested` (#43) and `Conversation.watch()` stays the only way to open one. Chord is an internal implementation detail here (ADR 0001, and the `Chord` row of the deviations): the port has no Chord host that could hand a listener Chord's own native call, and no port type plays the role. Excluded by name under ADR 0003 rather than carried `planned` into a release for a host this port never had; `test_types.py` pins the exclusion beside the three watch re-exports of that same upstream line the port does carry) |
| `NextTaskState` | type | `NextTaskState` | test_types.py, test_harness_sleep.py | tested (includes `sleeping`) |
| `Observation` | type | `Observation` | test_session_observers.py, test_harness_observers.py | tested (an attached `Observer` — the value at attachment, the idempotent `detach()`, and no pin left behind; #44) |
| `Observer` | type | `Observer` | test_session_observers.py, test_harness_observers.py | tested (the synchronous `advance(call, context)` / `end(end)` mirror of a watch, for documents, the conversation view, and the task graph: attached atomically on the line, every change unfolded in line order, `ended` by the owner it observes and `failed` by the close; #44) |
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
| `Storage` | type | `Storage` | test_storage_protocol.py | tested (one commit shape: `commit_batches(batches, receive, context)`, and the retired single-write `commit()` is absent from the protocol and from every backend; #41) |
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
| `TaskQuery` | type | `TaskQuery` | test_types.py, test_storage_protocol.py, test_testing_conformance.py, test_sqlite_migrations.py | tested (scans `sleeping` tasks) |
| `TaskRecord` | type | `TaskRecord` | test_types.py, test_storage_protocol.py | tested |
| `TaskRuntime` | type | `TaskRuntime` | test_harness_tasks.py, test_harness_scheduler_indexes.py, test_harness_scheduler_retention.py, test_harness_hook_reads.py | tested |
| `TaskState` | type | `TaskState` | test_types.py, test_harness_sleep.py | tested (includes `sleeping`) |
| `Tx` | type | `Tx` | test_session_tables.py | tested |
| `TypedEntry` | type | `TypedEntry` | test_entries.py, test_types.py | tested |
| `TypedEntryDraft` | type | `TypedEntryDraft` | test_entries.py, test_types.py | tested |
| `WatchCall` | type | `WatchCall` | test_session_watches.py | tested (1.5.0: Chord's tagged listener call — `{kind: "value", value, ops, retired}`, the one shape the document, conversation-view and task-graph watches hand their listener; `failed` is part of the union and Durable never sends it; #43) |
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
| `FileChange` | type | `FileChange` | test_env_conformance.py, test_env_posix.py, test_discriminant_spellings.py | tested (1.5.0 renames `WatchChange` to this and tags the three changes by `kind`: `paths`, `overflow`, `failed`; the untagged shapes #33 let readers accept are gone, so `kind` is the one tag read and the one emitted — #33, #35, #37) |
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
The unit cases pin tokenization (camel-case and acronym boundaries, stop words, naive plural stems)
and translate the `rankDocuments` cases of upstream `test/search.test.ts` one for one — every
non-Unicode assertion of them, over the same fixtures and the same expected orders. The three v1.5.0
ranking rules each have the case that proves it (#57): the name scored as a field of its own, where
one mention in it beats three in a description, by
`test_ranks_a_name_match_above_mentions_in_descriptions` (`test/search.test.ts:105`); the bonus for a
query carrying a tool's whole local name after the last `__`, by
`test_ranks_a_tool_first_when_the_query_contains_its_whole_name` (`test/search.test.ts:117`), whose
`peekaboo app` query answers `mcp__peekaboo__see` once upstream's own `score += fullNameBonus` line is
deleted from its `src/search.ts`; and exact-name-first, by
`test_ranks_a_tool_first_when_the_query_is_its_name` (`test/search.test.ts:89`) together with
`test_ranks_an_exact_name_query_ahead_of_the_document_that_outscores_it` — the upstream case alone
does not see the rule, because its two documents already land in that order on the name weight and
the bonus, while the second case's competitor outscores the named tool and wins only when the query
is not its exact name. Term relevance and the limit, the namespace field, and coverage above score
follow in `test_ranks_by_term_relevance_and_respects_the_limit`, `test_searches_the_namespace_text`
and `test_ranks_a_tool_matching_more_query_terms_first_whatever_the_scores`; what the ranker already
had stays pinned by `test_ranks_by_bm25_keeps_ties_in_document_order_and_honors_the_limit` (length
normalization, ties in document order, the limit), and the empty-query and positive-integer-limit
diagnostics by the tool cases. The eight Unicode `rankDocuments` cases are not translated: they reach
terms through the word segmenter the port does not have. `pi_durable.search` is the internal port of
`src/search.ts`; it is not a public subpath export.

### `src/testing/index.ts` → `pi_durable.testing`

Conformance helpers; benchmarks are excluded.

| Upstream | Kind | Python target | Tests | Status |
| --- | --- | --- | --- | --- |
| `createExpectAssertions` | value | `create_expect_assertions` | test_testing_conformance.py | tested |
| `commitWrites` | value | `commit_writes` | test_storage_commit_batches.py | tested (1.5.0: the sanctioned way to write one batch to a storage — resolves with its sequence, raises the refusal; with #41 every seeding path in the conformance suite and the port's suites goes through it) |
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
| 1 Terms and invariants | tested (the three 1.5.0 clauses that put this row back to `planned` are each proved where they live: a stored commit publishes once Storage made it durable, in line order, and its outcome stands when the call later rejects, and the line runs one commit callback at a time while their storage overlaps, behind the unpublished-key gate and the 256-job bound -- `tests/test_session_group_commit.py` and `tests/test_harness_group_commit.py`; #42. And a deferred commit reaches the watches and the view before Storage does, the host's commit observer being handed it only when its held changes are stored -- the publications cases of `tests/test_session_deferred_commits.py` and the two audiences of `tests/test_harness_deferred_progress.py`, read from both sides; #47) |
| 2 Core records | tested (1.5.0 retags the unions this section carries — `kind` on the ends and the reports, the document commit change and the file-change union, with the keep list (`StorageWrite.type`, `SubmissionRecord.type`/`.status`, `TaskState.status`, `TaskOutcome.status`, `DocumentContent.kind`) left exactly as it was: test_discriminant_spellings.py, expanded by #33 and contracted by #37 — and adds the `continue` submission record (#50: test_harness_continue.py, test_harness_submit_in_commit.py), `RunFailure` and `RunSummary` — #48: test_harness_run_failure.py, test_harness_after_run.py) |
| 2.1 Entries and context | tested (1.5.0: the conversation view carries one consumer's entry window over the visible entries with head markers in place, and `Conversation.context()` derives the model transcript from the whole visible transcript — test_harness_view.py, test_harness_conversations.py, test_harness_compaction.py; #45) |
| 2.2 Public Harness surface | tested (one `Report` on `on_report` landed with #34, the `observe_*` seams that replaced the state getters and the commit subscription with #44 and #46, `ConversationTranscript` with #45, and `read_only`, `when_idle`, and `submit(tx, draft)` with #50: test_harness_reports.py, test_harness_observers.py, test_conversation_transcript.py, test_harness_read_only.py, test_harness_submit_in_commit.py; `max_turns` and the `turn_limit` end it drives, and the run-end obligations an answer waits on with `run_end_task`, landed with #48: test_harness_turn_limit.py, test_harness_after_run.py) |
| 3 Documents | tested (definitions, records, access, ownership, bases, versions, and forks across test_session_*; the audit report lists the section-to-suite mapping) |
| 3.1 Definitions | tested (`checkpoint_when` is required on every definition; test_session_definitions.py) |
| 3.2 Records and lifetimes | tested |
| 3.3 Access and creation | planned (the Chord state getters are gone and observer attachment and windowed conversation watches replace them; #44, #45) |
| 3.4 Mutation ownership | tested (the admission, pending-batch, receipt and publication states of group commit (`tests/test_session_group_commit.py`, `tests/test_harness_group_commit.py`; #42), and what a held deferred commit does and does not show -- memory at adoption, Storage at the next trigger, both from `tests/test_session_deferred_commits.py`; #47) |
| 3.5 Bases and checkpoints | tested for what #47 adds: a held deferred commit runs its base predicate once, against the count Storage had when the commit was admitted rather than against the held memory, and stores its base with the held operations in the one transaction that stores them (`tests/test_session_deferred_commits.py`; #47). `tests/test_storage_commit_batches.py` carries the storage side. |
| 3.6 Versions and migrations | planned (`documentState()` left the access-driven migration list with #44: what migrates is now what an observer is shown whole. `pi.live` is version 3 and stored versions 1 and 2 migrate in place, the persisted version constants are read at their definition sites, and the SQLite schema and JSONL format stand still: test_harness_live_migrations.py, #36) |
| 3.7 Forks | planned (fork metadata hydrates watches and mounted views only; the document-state path is gone; #44) |
| 4 Transactions and storage ownership | tested (group commit on the mutation line: one callback at a time with overlapping storage, the unpublished-key gate, the 256-job bound enforced and observable, outcomes delivered in line order, and durable-before-published — `tests/test_session_group_commit.py`, `tests/test_harness_group_commit.py`. The deferred half of 3.4 Mutation ownership landed with #47, in `tests/test_session_deferred_commits.py`) |
| 4.1 Deferred commits | tested (`CommitPersistence` and what may be held -- only a commit whose every write touches a latest, initial-fork document of the loaded history, which in practice is a generation partial and running tool progress; the store on the next commit that reads or writes the document, at a drop of its tracker, at a version reload, at close, and at the `settings.progress.persist_within_ms` deadline, which one foreground timer keeps for the earliest of them and which wakes nothing; the two audiences, `AdoptedDocuments` separating what memory was shown from what Storage made durable, so a held store reaches the host's `observe_commits()` observer and not the watches that already saw the partial; one transaction for every held document, its base counted as one delta; and a stream that fails before the first store leaves no aborted entry and no usage -- `tests/test_session_deferred_commits.py`, 25 cases, translated from `test/session-deferred-commits.test.ts`; and the two tool-side proofs the row owed: what a running call reports through `details()` rides the same held commit as its output, and a tool's own `api.commit(change, context, options)` honours the mark it was given -- `test_shows_a_running_tools_details_at_once_and_stores_them_with_the_next_trigger` and `test_defers_a_running_tools_own_commit_only_when_it_asks_for_it` in `tests/test_harness_deferred_progress.py`, each red when the mark is dropped; #47) |
| 5 Tasks | tested |
| 5.1 Definition | tested (the `kind`-tagged `ReportFrom` landed with #33, and `TaskRuntime.entries()` with #56: the committed raw history of the task's own conversation, fork-aware, as `Conversation.entries()` pages it (`src/types.ts:327-337`). Upstream builds both reads' query with one helper, `conversationEntries` (`src/harness/util.ts:69-79`), and takes both on the line (`src/harness/scheduler.ts:2828-2834`); the port shares its `harness/util.py` `conversation_entries` the same way, so the two reads cannot drift. Translated case for case from `test/harness-hook-reads.test.ts:50-93` in test_harness_hook_reads.py, which also proves the read queues on the line an in-flight commit holds and so settles on committed state only) |
| 5.1 Definition | tested for what 1.5.0 adds here: `TaskRuntime.commit` takes the `options` a commit takes, which is how a running task's progress commits are deferred (`src/harness/types.ts:843`, `PROGRESS_COMMIT` at `src/harness/live.ts:230-235`); #47 |
| 5.2 Effect sandwich | tested |
| 5.3 Terminal tasks | tested |
| 5.4 Scheduler, abort, and ownership | tested (a task cut off `restart_limit` times in a row ends instead of running again, each kind visibly: a tool call settles an error result with the `restart_limit` diagnostic built from its slot, a compaction fails into `compactionFailure`, a generation ends its run `restart_limit` (or, having answered, ends completed by crashes), and a task of an extension's kind is faulted by the scheduler; the built-ins that end themselves are reported `task_interrupted`, and nothing parks: no `retry_task()`, no `restart_limit` blocked reason, no wake exclusion (test_harness_restart_failure.py, test_harness_blocked.py, test_harness_after_run.py). The scheduler decides in recorded regions; #48, #49. And the incremental idle state, with the parked state gone from it: a task change settles only the conversations above it (idle waiters and kept context ranges), expiry runs oldest first from a separately kept order, a kept range is extended from the commit that appended to it and carries the `current` flag that refuses to advance one whose scan ran off the line, and `wake()` reads only sleeping tasks while work is active -- `test_harness_idle_scopes.py`, translated case for case from `test/harness-idle-scopes.test.ts` (14 `it()` cases, 24 tests with its two `it.each` tables), and the `harness-context-range` case in `test_harness_context_ranges.py` (`test/harness-context-range.test.ts:8`); #49) |
| 5.5 Structured concurrency | tested (the tasks an `afterRun` hook asks for are created as the generation's own children, so an idle wait stays busy until they are terminal, Stop reaches them, and aborting one on its own still ends the run normally: test_harness_after_run.py; #48) |
| 5.6 Sleeping, timers, and wakeups | planned (the foreground timer now also arms a deferred commit's deadline, and that half landed with #47: one timer for the earliest of the held deadlines, armed through `HarnessOptions.set_timer` -- the deadline cases of `tests/test_session_deferred_commits.py` read the host's own recorder, so arming it anywhere else, the wake scheduler included, leaves that list empty and turns them red -- over the same foreground seam whose decline-to-native fallback and single report test_harness_sleep.py proves. What keeps the row `planned` is the other clause: `on_wake` runs off the line, which that suite shows only through a wake that throws and one that is offered again, not through a commit settling while its host is still inside `on_wake` -- a case #52's audit owes) |
| 6 Submissions and inbox | tested (continue submissions, `when_idle="reject"` with `ConversationIdle`, and `submit(tx, draft)` inside a commit landed with #50: test_harness_continue.py, test_harness_submit_in_commit.py. The answer-resolves-while-busy rule landed with #48: a run that answered keeps `pi.live.run`, with an empty ID list and `run.afterRun` set, until the tasks its `afterRun` hooks asked for are terminal, so a queued input waits behind an answer already settled -- test_harness_after_run.py asserts the wait resolves at the answer and that idle does not settle until the task ends; the rest of that upstream suite is named there |
| 7 Extensions, hooks, tools, and system prompt | tested (tool search in test_tool_search.py, whose ranker cases translate upstream test/search.test.ts and hold the v1.5.0 ranking: the name as a field of its own, the whole-local-name `__` bonus, and exact-name-first, each by the case that proves it (#57); code mode excluded) |
| 7.1 Extensions, registry, and agent resolution | tested (a call to a tool its request did not offer resolves `tool_unavailable` through the phase's tool lookup; test_harness_tools.py, test_harness_tools_recovery.py. That the registry is never touched for it is proven behaviourally only — the cases show a deferred tool the agent has but never offered simply never runs — so that clause still owes a registry lookup spy to be observed rather than inferred, and is claimed no further than that; #36) |
| 7.2 Hooks | planned (`afterTool` on every result the tool task writes and `HookApi.entry()`/`entries()` landed with #36, and `afterRun` with `RunSummary` and `RunEndTask` -- asked once a run really ends, in `finish`, off the line -- with #48: test_harness_after_run.py. `beforeProviderRequest` is what this section still owes, and it has an owner of its own now: #59. (#36 was named here before, but #36 retagged and versioned the persisted documents; it never owned a provider hook, and the port runs no hook by that name.) |
| 7.3 Tools | tested (the deferred tool progress that put this row back to `planned` is landed, and the clause that said so was wrong on both counts, read off the vendored sources rather than off this port: `ToolExecutionApi.commit` does take the `options` a commit takes and passes them to its task's commit (`harness/types.ts:282-283` and `harness/tool.ts:487-496` upstream, `harness/types.py:179-182` and `harness/tool.py:762-774` here), and a running call's `details()` value is written inside the same deferred commit as its output (`harness/tool.ts:634-655` upstream, `harness/tool.py:945-957` here). No upstream case commits through either path with a mark, so the two proofs are port-side, in test_harness_deferred_progress.py: `test_shows_a_running_tools_details_at_once_and_stores_them_with_the_next_trigger`, red when the progress commit is made immediate and when `details` is left out of the held write, and `test_defers_a_running_tools_own_commit_only_when_it_asks_for_it`, red when `options` is dropped on the way to the task's commit. Parameters as plain JSON Schema closed by default with pi-schema's validator wording landed with #51, and `ConversationHandle.submit(tx, ...)` with #50: test_harness_submit_in_commit.py, over the rule in `harness/submissions.py`. `unoffered` slots and the hidden call's own task are tested in test_harness_tools.py and test_harness_live_migrations.py; #36, #47) |
| 7.4 System prompt and dynamic tools | tested (request integration with #14) |
| 7.5 Extension reload | tested |
| 7.6 Code mode | excluded |
| 8 Built-in tasks | tested (the one clause that put this row back to `planned` is #47's and is landed: generation and tool progress commit deferred, so a crash costs the throttle window plus whatever was not stored yet -- `tests/test_harness_deferred_progress.py`, translated case for case from `test/harness-deferred-progress.test.ts`, whose crash cases show a reopened Harness answering a tool without replay with exactly the output Storage had by then, holding nothing that was only in memory, and retrying the attempt whose request was already charged) |
| 8.1 Built-in entries | tested (both halves of the `pi.tool-result` producer list. The half 1.5.0 left alone holds and is proven: an aborted round writes its own results — test_harness_tools.py:1035, test_harness_tools_recovery.py:279 — and a call its request never offered gets its `tool_unavailable` result from its own tool task — test_harness_tools.py:130, `:155`, `:176`, `:232`. The half it rewrote — generation answering for the calls an aborted round never started — now executes, in both of its arms: a sequential round whose first call blocks, and whose run is aborted while it does, leaves the second call in the round's `pending`, and the run's abort handler answers it with its own `aborted` `pi.tool-result` beside the one the running call's tool task writes for itself — test_harness_tools.py, `test_answers_the_calls_an_aborted_round_never_started_with_their_own_aborted_result`; and where that unstarted call is one the request never offered, the same handler answers it `tool_unavailable` rather than `aborted`, which no tool-task case reaches — `test_answers_an_unoffered_call_the_aborted_round_left_pending_with_unavailable`. Both were dead code before those cases: `src/pi_durable/harness/generation.py:544` (`for call in unstarted:`) is reached across the suite while its body (`:545-563`) is not, shown by tracing every line of that file the whole suite runs with these two cases deselected, and iterating nothing in the loop's place turns the first red on the missing `Tool b was aborted` result, as does making the `unoffered` test at `:554` always false on the missing `Tool ghost is not available` one — re-proven at this commit; #48) |
| 8.2 Live document | tested (`run.first`, `run.afterRun` and `runFailure` landed with #48 -- the task, inputs, reason, detail and first entry of the run that never answered, cleared by a later run except the successor its own commit starts, and `run.afterRun` set while an answered run waits on its tasks. Version 3 carries `run.turns`, `trigger` on compaction status, and `unoffered` slots, and a stored version 1 or 2 document migrates in place: test_harness_live_migrations.py, test_harness_compaction.py, test_harness_tools.py, test_harness_after_run.py; #36, #48) |
| 8.3 Generation | tested (an interrupted-attempt charge under the retry policy (`interrupted`, with its committed partial converted and the backoff taken, or the run ended when no retry is left) and `nothing_to_answer`, decided from the derived context before any request, landed with #48: test_harness_continue.py. The `finish` and `drain` phases those run-end tasks need -- the hooks asked in `finish`, the tasks created as generation children on the way to `drain`, and the run ended by `end_answered_run` without a failure however they ended -- landed with #48: test_harness_after_run.py. Deferred partials landed with #47. `unoffered` calls get their own tool task and count the turn they cost the run: test_harness_tools.py, test_harness_generation.py; #36) |
| 8.4 Tool | planned (the `afterTool` chain over every result the task writes, with its bounds, is implemented -- over an executed call's result, over a result the Harness wrote itself, and over an interruption result -- and parts of it are proven by existing cases: replacements chained with the round observed through `afterTools` (test_harness_tools.py), a nested call's hook (test_harness_nested_tools.py), `structuredOutput` kept through a replacement (test_harness_structured_output.py), a hook `terminate` on a blocked call counting toward the round (test_harness_turn_limit.py), and `details()` still waiting when the call is aborted inside `afterTool` (test_harness_tools.py:994). What keeps the row `planned` is upstream's `test/harness-tool-hooks.test.ts` (18 cases), deliberately untranslated at #49 with its reason in `docs/parity-report-1.5.0.md` and its owner #60: no case here runs `afterTool` on an `aborted` result, on an abandoned nested call's result, or on a `restart_limit` result, nor weighs a replacement output's bounds. `pi.tool` is version 3 and an `unoffered` call resolves `tool_unavailable`: test_harness_tools.py, test_harness_tools_recovery.py; #36, #49) |
| 8.5 Tool rounds | tested (all three clauses, over the eleven upstream `harness-turn-limit.test.ts` cases translated here case for case with their upstream lines cited. A handoff continues the run and the count starts over — `test_runs_on_yield_at_the_limit_and_a_continuation_starts_the_count_over` (:108), with the count kept across a restart by `test_keeps_the_stored_count_across_a_restart` (:307) and a changed limit applied only at the round's end by `test_applies_a_changed_limit_at_the_end_of_the_round_and_null_removes_it` (:192). A `terminate` round hands to `finish`, and its handoff's reset is written before the run ends — `test_lets_terminate_settle_done_at_the_limit_and_writes_a_handoffs_reset` (:232), with the `finish` side and the summary a terminate round is handed in test_harness_after_run.py's terminate cases. And `max_turns` ends the round `turn_limit` with the count reported — `test_ends_a_run_after_the_tool_round_that_reaches_the_limit` (:73), which settles `unanswered`/`turn_limit` with detail `"1 turn"`, leaves `only_failure` on the live document, ends the generation completed and reports nothing; #48) |
| 8.6 Usage | tested |
| 8.7 Compaction | tested (the two clauses this row owed landed. A compaction cut off `restart_limit` times in a row records its `compactionFailure` rather than parking: `test_fails_a_compaction_into_compaction_failure` in test_harness_restart_failure.py, upstream `harness-restart-failure.test.ts:438`, beside its port-authored sibling proving the cut-off compaction is removed without failing the run (#49). And a summarization request the host stopped mid-flight is resent with the same messages while only the answered attempt is charged: `test_resends_an_interrupted_summarization_once_and_counts_only_the_answered_attempt` in test_harness_compaction.py (#48). `trigger` names what started a compaction, in the status the live document lists: test_harness_compaction.py, test_harness_live_migrations.py; #36) |
| 9 Document observation and Chord | tested |
| 9.1 Watches | tested (`WatchCall`, the tagged listener call every watch's single listener takes, landed with #43; `WatchHandle`'s `value`/`start`/`stop`/`closed` and `WatchEnd`'s five `kind`-tagged ends, `aborted` among them, landed with #35; and the `Observer` beside it — `advance(call, context)` / `end(end)` attached atomically on the line, its detach idempotent, `ended` only by an observed owner that ended and `failed` by the Session's close, with the commit observer handed exactly what Storage stored: test_session_observers.py, test_harness_observers.py. A task document's watch ends with the task that owns it: the commit that makes the task terminal retires the document, and the watch ends `ended` once that retirement has been delivered and returned, while a watch whose listener never started owes a call that never arrives and so is ended only by the Session's close (`ended`) or failure (`failed`) — and an ended watch keeps nothing of its Session alive, proven on weakrefs: test_harness_observers.py; #54. Supersedes the 1.4.0 Document state section, whose state getters #44 deleted) |
| 9.2 `watchDoc` | tested (one change object per listener, coalesced deliveries with folded operations — appends joined, a per-field set keeping the last, the newest commit's context — the 10,000 pending-operation root replacement in place of the per-commit frame buffer, and a watch that follows its address through a retirement into a recreation: test_session_watches.py, translated case for case; #43) |
| 9.3 Conversation view | tested (1.5.0: the entry window with `before`, the `ConversationWindow` bounds, and `ConversationTranscript` are tested in test_harness_view.py and test_conversation_transcript.py, and the observer that carries them attaches through `observe_conversation()` in test_harness_observers.py; a run as the view shows it is translated in test_harness_view_run.py, where the assertions the deleted agent event adapter carried now ride on the view -- every one of upstream's 13 `test/harness-view-run.test.ts` cases, the streamed-partials tail at `:505`, `:547` and `:581` landing with #47. Those three are told with the port's measures, named in their docstrings and recorded under **Remaining deviations** in `docs/parity-report-1.5.0.md`: what the partial stream costs is the operations its commits carry -- appends at each growing argument, the block set whole at most once per argument, never the raw argument text -- and the two bounds upstream weighs on V8's heap are deviated from, each naming its reason and the bound it keeps here (they are deviations from an otherwise translated case, not rows of their own). The two cases that read `runFailure` (`:372`, `:436`) now assert it as upstream states it, landed with #48: `test_ends_a_call_that_never_runs_and_one_aborted_with_its_generation` asserts `{inputs: [submission.id], reason: "aborted"}` on the newest state and `test_converts_a_streamed_partial_into_one_aborted_answer_entry` asserts the reason on the state that shows the run gone, and deleting `live["runFailure"] = failure` in `src/pi_durable/harness/live.py` turns both red; a run that answered and waits for its after-run tasks shows as `run` with `run.afterRun` and no `generation` in test_harness_after_run.py) |
| 9.4 Task graph view | tested (the vendored 9.4, re-keyed from the 9.5 this port carried and from the retired Agent events section #46 deletes: translated in test_harness_task_graph.py, and the graph observer of #44 mirrors each publication on the line, a blocked reason re-derived with no commit, in test_harness_observers.py) |
| 10 Storage contract | tested (`commit_batches(batches, receive, context)` is the protocol's only commit call — per-batch checks against the state the earlier batches left, one durability receipt per stored batch delivered before the call settles, and the refusal a resolved outcome beside a rejection that stores nothing — with the optional `publishing()` scope and the narrowed `mint_id()` monotonicity across reopen: test_storage_commit_batches.py over the memory, SQLite and JSONL backends, and no backend carries the retired single-write signature; #38, #41. The conformance suite carries the receipt contract itself — the group stored in order against the state its earlier batches left, the prefix a refusal keeps, a later batch its own actions refuse, a copy resolved against its predecessors, receipts checked one per stored batch by the wrapper every case runs through, and every case seeded through `commit_writes`; #39, #41. The durable backends' own depth landed with #40, which also pins that every backend module registers the same case list) |
| 10.1 Failures | planned (one outcome per commit of a call in line order, durable-but-unpublished commits after a failure, receipts that stand before a rejection, and the tagged observer ends; #42, #44. The tagged watch ends landed with #35) |
| 11 Backends | tested |
| 11.1 Memory | tested (one call stores its group in order, each batch checked against the state the earlier ones left, so a copy sees what its predecessors wrote and the first refusal keeps its prefix and leaves the storage serving; the receipts ride the applied batches and stand even when a later batch's application throws — test_memory_storage.py, whose conformance cases run over `MemoryStorage` through the receipt-checking wrapper; #39) |
| 11.2 SQLite | tested (one SQL transaction stores the whole call: every batch checked through a handle that tells a refusal from a failed database call, the allocation written with the counted-run member so a statement that changed no row rejects the call instead of handing the same sequence out twice, a driver of the old member set refused at its first commit, and a read concurrent with the group sees none of its batches or all of them — test_sqlite_storage.py and test_storage_commit_batches.py, the conformance cases run over `SqliteStorage` fresh and across reopen through the receipt-checking wrapper; #40) |
| 11.3 JSONL | tested (one group appended as one write of every marker, after its sidecars and their flushes, with reads held off the memory that runs ahead of the file until the append settles, so a failed append leaves nothing observable and a torn marker recovers the complete prefix before it; the refused batch keeps the earlier ones readable across reopen and the ID watermark rebuilds from what the files confirm — test_jsonl_storage.py, the conformance cases run over `JsonlStorage` fresh and across reopen through the receipt-checking wrapper; #40) |
| 12 API footguns | planned (admission inside a commit, hooks in abort handlers, busy-after-the-answer, after-run task traps, buffered watches, the folded-operations bound, and the durable progress cadence. Admission landed with #42 and #50, and the busy-after-the-answer and after-run task traps with #48 — the answered run stays busy behind its `run.afterRun` tasks until they end, an idle wait includes them, and a task that cannot run keeps the run busy until Stop orphans it: test_harness_after_run.py. The durable-progress-cadence bullet is #47's and landed with it: the crash cases of test_harness_deferred_progress.py show a client seeing only committed progress and losing the window a crash took, and the buffered-watch and folded-operations bullets with #43: the coalesced deliveries and the 10,000-operation root replacement of test_session_watches.py (row 9.2). What keeps the row `planned` is the abort-handler hook bullet — that a tool task's abort handler runs the `afterTool` chain on its `aborted` result, and the last attempt before the restart limit runs without hooks — which no case runs `afterTool` on an aborted result to prove -- the untranslated `harness-tool-hooks` cases that do are #60, recorded under **Cases deliberately left untranslated** in `docs/parity-report-1.5.0.md`; #42, #43, #47, #48, #50, #52) |
| 13 Non-goals | n/a (1.5.0 narrows the visible-undurable bullet to admit deferred commits; reviewed with #42 and #47) |

The upstream document-retention suite has two Harness-level cases: one follows a Harness settings
change at its next use, and one compares the views a run reaches with documents evicted against the
same run with documents kept. Both are translated in `test_session_document_retention.py`.

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
one-compaction rule, and the older-head-marker edits. Its compaction listing from start to end, a
late consumer's first value, and kept-entry view assertions now run through
`harness.observe_conversation` and the windowed `watch(context, window)` in the same file, alongside
the same committed state checked through `pi.live`, the task outcome, submissions, and the context
view.

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
and `abandonOnRestart` for a replay-unsafe caller and the child tasks it creates. The parent IDs, the
nested result, and the child-before-caller order of deep calls are read from the conversation view
and the stored `NestedCallDoc` in the same file, where the late subscriber's first value and the
per-slot outputs of several running nested calls are read from the same values, each frame checked by
replaying its own operations.

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
| Streamed partials | `harness/partial-changes.ts`'s `PartialChanges` collects what grew in the partial since its last commit -- the `text_delta` and `thinking_delta` text and, per streamed call, the changes a `StreamingJsonParser` reports from its raw argument text -- and commits exactly that, so a partial commit costs the new text; Chord's persistent values and V8's cons strings then keep a watcher that holds every frame proportional to the answer | the copied partial is committed and diffed by the draft layer (`harness/util.py`'s `assign_json` and `session/drafts.py`'s `_emit_changed`), which emits the same operations -- appends for grown strings, appends inside a call's arguments -- while comparing the whole strings, so a commit costs the answer's length rather than the new text; and the value a watch is handed is materialized (`delta.py`'s append builds a fresh copy of the whole text, CPython having no substring view over an immutable string), so a watcher that keeps every frame keeps one copy of the answer per frame. No raw argument text reaches durable here at all (the "Streaming tool-call arguments" row), so there is no parser to port. What the commits carry and what a watcher is handed is asserted in test_harness_view_run.py, whose streamed-partials cases name the two heap bounds this row does not claim |
| Class and type names | PascalCase | PascalCase |
| Persisted JSON fields | camelCase | camelCase |
| Hook names | camelCase (`beforeRequest`, `afterResponse`, `onYield`) | camelCase, because extensions register handlers by these names |
| Host execution backend | `NodeExecutionEnv`, native or polling watches | `PosixExecutionEnv` with polling watches; `mode` is accepted but the portable backend always polls |
| Execution environment options and result fields | camelCase result fields and option keys | `FileInfo`, `LineScan`, and `ShellExecResult` keep camelCase wire fields (`mtimeMs`, `firstLineEnd`, `exitCode`, `spillPath`); Python-facing option dicts use snake_case (`start_line`, `inherit_env`, `on_output`, `max_entries`, `after_bytes`) |
| Tool path resolution | the environment resolves relative and absolute paths, with no confinement | resolved against the environment and confined to its `cwd`; a path outside it raises `ValueError` |
| Node storage entrypoints | `openNode*` | `JsonlStorage.open()` / `SqliteStorage.open()`, with the caller supplying a `FileSystem` or a `SqliteDatabase`; file-backed SQLite opens through `open_posix_sqlite_storage()` |
| The one commit call | `Storage.commit(batches, receive, context)` (`storage/storage.ts:250`) — upstream's only commit member: v1.5.0 turned the single-write shape into the group call in place | `Storage.commit_batches(batches, receive, context)`: the port's batched call kept its name when the retired single-write `commit()` it lived beside was deleted (#41), so the name says what the call stores — a group of commit batches — and a host's custom storage implements this one member, while `pi_durable.testing.commit_writes` is how code seeds a storage with one batch |
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
| Document observation | Chord `AttachedReplicatedState`/`ReplicatedStateSource` | `Observer.advance(call, context)` / `Observer.end(end)` mirrors a watch's own `WatchCall` shapes synchronously, and `CommitObserver` replaces the 1.4.0 `subscribe_commits()` listener; both are attached through `observe_doc()`, `observe_conversation()`, `observe_task_graph()`, and `observe_commits()` |
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
| Mutable document drafts backed by strict-JSON copies | `pi_durable.session.drafts` | tested (`test_session_observers.py`, `test_session_tables.py`) |
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
| BM25 tool search behind `tool_search` | `pi_durable.search` | tested (`test_tool_search.py`, which translates upstream `test/search.test.ts`'s `rankDocuments` cases one for one and proves the three v1.5.0 ranking rules each by its own case: `test_ranks_a_name_match_above_mentions_in_descriptions` for the name as a field of its own, `test_ranks_a_tool_first_when_the_query_contains_its_whole_name` for the whole-local-name `__` bonus, `test_ranks_an_exact_name_query_ahead_of_the_document_that_outscores_it` for exact-name-first over two documents; #57) |
| Shared truncation utilities from `truncate.ts` | `pi_durable.truncate` | tested (`test_env_truncate.py`) |
| Pre-upgrade stored databases, written by this port's `v1.4.0` tag and opened by this release | `tests/fixtures/pre-1.5.0` (see its README) | tested (`test_harness_live_migrations.py`) |
| Shared provider plumbing: catalog lookup, request options, transcript replay, cross-model message adaptation, argument parsing, request-attempt retries, usage pricing, and the stream/abort wrapper | `pi_durable.providers.base`, `.request`, `.transcript`, `.json_parse`, `.retry`, `.usage`, `.stream` | tested through the adapter suites (`test_providers_openai.py`, `test_providers_anthropic.py`) |
| OpenAI model adapter over Chat Completions | `pi_durable.providers.openai` | tested (`test_providers_openai.py`, `test_providers_extras.py`) |
| Anthropic model adapter over Messages | `pi_durable.providers.anthropic` | tested (`test_providers_anthropic.py`, `test_providers_extras.py`) |
| `ReportKind`, `ReportReason` — union aliases narrowing `ReportInfo.kind` and `.reason` | `pi_durable.harness.types` | tested (`test_harness_reports.py` exercises every member; `ReportInfo` is their only user) |
| `Timers`, `host_timers`, `native_timer` — internal timer arming behind `HarnessOptions.set_timer` (upstream `hostTimers` / `nativeTimer`) | `pi_durable.timers` | tested (`test_harness_sleep.py`, `test_session_document_retention.py`) |
| `blocked_subject`, `blocked_reason()` — what the task graph reads of a record to derive its blocked reason | `pi_durable.harness.scheduler` | tested (`test_harness_blocked.py`, `test_harness_task_graph.py`) |

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
cases skip by design, and every remaining deviation is explicit. It is that baseline's record and
it stays as written: a later sync writes beside it, never over it.

**The v1.5.0 exclusion record has one owner: the dated v1.5.0 audit report,
[`docs/parity-report-1.5.0.md`](./docs/parity-report-1.5.0.md).** That report —
not this matrix and not the 1.4.0 record — names the suites this baseline adds (`group-commit`,
`deferred-commit`, `deferred-progress`, `observers`, `transcript`, `run-failure`, `turn-limit`,
`after-run`, `continue`, `submit-in-commit`, `idle-scope`, and `restart-failure` among them), every
upstream case deliberately left untranslated, and every 1.5.0 deviation and exclusion. It exists
now, carrying what each porting ticket observed at its own commit; #52 is the audit that signs it
off, and until it does a completion claim there is provisional. An exclusion this baseline takes is
recorded there, and where a row must record it: the `excluded` rows above, each with its reason, and
ADR 0003. Anything about 1.5.0 — an exclusion or a completion claim — that the 1.4.0 record carries
is provisional: it belongs in `docs/parity-report-1.5.0.md`, so the same fact is never signed off
twice in two places and the 1.4.0 record states only the baseline it audited.

**What `test_types.py` proves is an annotation-compile contract, not behaviour.** The rows whose
`Tests` cell cites it, alone or beside a behavioural suite, cite it for the shapes it pins: each is
a `TypedDict` or `Literal` literal asserted against its own annotation, so the case passes by
construction and nothing in it can disagree with the implementation. The check that can is the
`mypy` run CI performs over `tests/`, where `warn_unused_ignores` turns a vanished negative error
into a failure — the same evidence upstream takes from `satisfies` and `@ts-expect-error` in its
`types` suite, where the check is the `tsc` compile. The audit therefore reads these rows as
compile contracts the type checker verifies rather than behavioural proof: a behavioural claim on
such a row still owes a suite that runs.

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

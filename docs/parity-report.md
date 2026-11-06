# Parity report: v1.4.0 audit

**Baseline:** `@earendil-works/pi-durable` v1.4.0, tag `v1.4.0`, commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`.

**Normative reference:** [`spec-1.4.0.md`](./upstream/spec-1.4.0.md). The
[`spec-1.3.0.md`](./upstream/spec-1.3.0.md) baseline stays as the 1.3.0 audit record.

**Contract:** [`PARITY.md`](../PARITY.md). Every root export and subpath row is `tested` or
`excluded` there; this report records how the 61 in-scope upstream suites were translated and
what remains different.

**Scope records:** [ADR 0001](./adr/0001-parity-baseline-and-boundary.md) (baseline and boundary)
and [ADR 0002](./adr/0002-upstream-1-4-0-scope.md) (the 1.4.0 exclusions).

## Result

- All 61 in-scope upstream `test/*.test.ts` suites have a Python translation.
- The excluded suites are `codemode`, `codemode-checkpoints`, `codemode-describe`,
  `codemode-models`, `harness-telemetry`, `harness-telemetry-options`, `images-wasm-module`,
  `indexeddb-storage`, `sqlite-cloudflare`, `test-workers/*` (`storage`, `wake`), and
  `tools-images`; the exclusions are recorded in `PARITY.md` and ADR 0002 (Code Mode, telemetry,
  pi-ai identity, the image implementation, and the Cloudflare, browser, and Workers backends).
- Two translated cases skip without live provider credentials, by design:
  `test_provider_session_cache_e2e.py` and the live half of `test_system_order_cache_e2e.py`.
  Their non-live halves run in the suite, and the request shapes they measure are covered with
  fakes in `test_providers_openai.py` and `test_providers_anthropic.py`.
- `ruff`, `mypy`, and `pytest` are clean. CI runs the suite on Linux and macOS for Python 3.11,
  3.12, 3.13, 3.14, 3.15, and the 3.14t/3.15t free-threaded builds.

## Suite translation

| Upstream suite | Python translation |
| --- | --- |
| `env-line-scan` | `test_env_line_scan.py` |
| `env-node-conformance` | `test_env_conformance.py` |
| `env-node-spill` | `test_env_posix.py` (spill cases) |
| `env-node` | `test_env_posix.py`, `test_env_filesystem.py` |
| `env-truncate` | `test_env_truncate.py` |
| `harness-callers` | `test_harness_registry_agent.py`, `test_harness_nested_tools.py`, `test_harness_tools.py` |
| `harness-cancellation-barrier` | `test_harness_cancellation_barrier.py` |
| `harness-blocked` | `test_harness_blocked.py` |
| `harness-compaction` | `test_harness_compaction.py` |
| `harness-context` | `test_harness_context.py`, `test_harness_context_ranges.py` |
| `harness-conversations` | `test_harness_conversations.py` |
| `harness-events` | `test_harness_events.py` |
| `harness-failed-run-inbox` | `test_harness_failed_run_inbox.py` |
| `harness-generation-recovery` | `test_harness_generation_recovery.py` |
| `harness-host-inputs` | `test_harness_host_inputs.py` |
| `harness-generation` | `test_harness_generation.py` |
| `harness-inbox` | `test_harness_inbox.py`, `test_harness_input_entries.py`, `test_harness_usage.py` |
| `harness-input-entries` | `test_harness_input_entries.py` |
| `harness-inspect` | `test_harness_inspect.py` |
| `harness-lifecycle` | `test_harness_lifecycle.py`, `test_harness_lifecycle_observations.py` |
| `harness-live-deltas` | `test_harness_live_deltas.py` |
| `harness-nested-tools-restart` | `test_harness_nested_tools_restart.py` |
| `harness-nested-tools` | `test_harness_nested_tools.py`, `test_harness_events.py` |
| `harness-ownership` | `test_harness_ownership.py`, `test_harness_scheduler_retention.py`, `test_harness_tasks_recovery.py` |
| `harness-prompt` | `test_harness_prompt.py` |
| `harness-registry` | `test_harness_registry_agent.py` |
| `harness-reports` | `test_harness_reports.py` |
| `harness-scheduler-indexes` | `test_harness_scheduler_indexes.py` |
| `harness-sleep` | `test_harness_sleep.py` |
| `harness-scheduler-retention` | `test_harness_scheduler_retention.py` |
| `harness-storage-failure` | `test_harness_storage_failure.py` |
| `harness-observers` | `test_harness_observers.py` |
| `harness-structured-output` | `test_harness_structured_output.py` |
| `harness-structured` | `test_harness_tasks.py`, `test_harness_ownership.py`, `test_harness_structured_cycles.py` |
| `harness-submissions` | `test_harness_submissions.py`, `test_harness_inbox.py` |
| `harness-task-graph` | `test_harness_task_graph.py` |
| `harness-tasks-recovery` | `test_harness_tasks_recovery.py` |
| `harness-tasks` | `test_harness_tasks.py` |
| `harness-tools-recovery` | `test_harness_tools_recovery.py` |
| `harness-tools` | `test_harness_tools.py` |
| `harness-view` | `test_harness_view.py` |
| `jsonl-storage` | `test_jsonl_storage.py` |
| `memory-storage` | `test_memory_storage.py` |
| `provider-session-cache-e2e` | `test_provider_session_cache_e2e.py` (live case skipped without credentials) |
| `session-checkpoints-migrations` | `test_session_checkpoints_migrations.py` |
| `session-definitions` | `test_session_definitions.py` |
| `session-document-retention` | `test_session_document_retention.py` |
| `session-documents` | `test_session_documents.py` |
| `session-forks` | `test_session_forks.py` |
| `session-observers` | `test_session_observers.py` |
| `session-states` | `test_session_observers.py`, `test_session_document_retention.py`, `test_session_checkpoints_migrations.py` (upstream 1.5.0 deletes the state getters this suite tested; where each of its guarantees is proved now is itemised in "Cases completed in this audit") |
| `session-tables` | `test_session_tables.py` |
| `session-watches` | `test_session_watches.py` |
| `spec-usage` | `test_spec_usage.py` (compile-checked examples, as upstream) |
| `sqlite-facade` | `test_sqlite_facade.py` |
| `sqlite-migrations` | `test_sqlite_migrations.py` |
| `sqlite-storage` | `test_sqlite_storage.py` |
| `storage-runtime-boundary` | `test_storage_runtime_boundary.py` |
| `system-order-cache-e2e` | `test_system_order_cache_e2e.py`, `test_providers_anthropic.py` (live measurement skipped without credentials) |
| `tool-search` | `test_tool_search.py` |
| `tools-read-differential` | `test_tools_read_differential.py` |
| `tools` | `test_tools.py` |
| `types` | `test_types.py` |

Many upstream files split one suite across several Python files, or combine related cases into one
test. The row above names every file that carries translated cases; the `PARITY.md` export rows
name the file that pins each public symbol.

## The 1.4.0 delta

The sync ports the whole in-scope v1.4.0 delta; every breaking change is implemented rather than
grandfathered:

- **Sleeping tasks, timers, and wakeups (spec §5.6).** `TaskState`/`NextTaskState`/`TaskInspection`/
  `TaskGraphState`/`TaskQuery` carry `sleeping`; `TaskRuntime.sleep_until(until, checkpoint, ctx)`
  and a `{status: "sleeping", until, checkpoint}` commit end a phase without holding an invocation,
  and the in-phase `sleep()` is gone. `TaskRuntime.set_timer()`, `HarnessOptions.set_timer`,
  `create_session(..., set_timer=...)`, `HarnessOptions.on_wake`, and `Harness.wake()` are ported,
  including the `background`/foreground split, the decline-and-report fallback, and the
  `active`/`at` change protocol. Generation retry backoffs, deferred polls, and compaction retries
  are durable sleeps; a record of an earlier version resumes by sleeping again.
- **Crash-loop guard and parked tasks (§5.4).** `TaskRecord.restarts`, `settings.restart_limit`
  (default 5, `inf` never parks), the blocked reason `restart_limit`, open's counting, progress
  clearing the count, close writing stopped tasks back as `pending` uncounted, `task_parked`, and
  `Harness.retry_task()`. A parked task is not active in `wake()`; an abort mark still runs the
  handler, and a handler cut off `restart_limit` times faults the task with detail
  `{reason: "restart_limit"}`. The task graph publishes a derived `blocked` reason and follows
  registry changes, failed migrations, and a raised limit.
- **Typed reports (§10.1).** `on_report(error, info)` with the `ReportInfo` union
  (`session_failed`, `task_faulted`, `task_parked`, `migration_failed`, `task_outdated`,
  `task_error`, `extension_error`, `host_error`, `listener_error`), `ReportTask`, and
  `TaskRuntime.report(error, from_)`. A faulted phase reports `task_faulted` once its fault is
  durable; a one-argument callback keeps working (the port inspects its arity).
- **Host inputs and settings.** `HarnessOptions.models` and `.settings` accept a value or a
  zero-argument function resolved at each use; `api.models` reads the current value at each access.
  `settings.after_failed_run` (`keep`/`continue`/`withdraw`) settles the inputs behind a failed
  run, and `settings.compaction.model_overrides` resolves the policy for the model each decision
  acts for (threshold, overflow recovery, and the compaction task). `ToolExecutionApi.now()` reads
  the Harness clock, and the tool-progress throttle paces on it through `set_timer`.
- **Documents.** `checkpoint_when` is required on every `define_doc`/`define_doc_family`, with no
  default; every built-in and test definition that keeps only the required bases passes a
  predicate returning `False`.
- **Tools.** `ToolControl.terminate` is a boolean; only `True` ends a round, and `False` is
  covered by a new case.
- **Storage.** SQLite schema version 2 rebuilds the task table to admit `sleeping`; a database
  migrated by a newer version is refused. JSONL needs no migration, and the port's open keeps a
  sleeping task asleep, with its deadline, across a reopen of either backend.

## Fixed by this audit

Three gaps were found while translating the last deferred cases and were fixed in source:

1. **Extended context reads could invent a baseline.** The Python kept range conflated the
   TypeScript `null` ("this range cannot have a baseline") with `undefined` ("no entry has settled
   it yet"). A later `pi.system` entry added by an extended read could therefore lead the context
   even though an earlier, omitted system entry ruled it out. `find_baseline()` now returns the
   `UNDECIDED` sentinel and `extend_range()` only searches added entries when the previous range is
   undecided. The upstream case "finds the baseline among the entries an extended read adds"
   caught this.
2. **An unformattable thrown value could fail error formatting.** `error_message()` caught a
   failing `str()` for ordinary values but not for exceptions. It now catches both, so a task that
   throws such a value faults only itself and a watch whose listener throws one ends only that
   watch.
3. **Missing root type exports.** `SessionEnd`, `PhaseHandler`, and `HooksOf` are now exported and
   pinned by `test_types.py`; `Session.closed` returns `Future[SessionEnd]`.

## Cases completed in this audit

- `harness-context`: the six Harness-level cases that earlier tickets deferred — a kept read
  across tasks and its retention period, equivalent derivation from extended reads under random
  edits, baseline discovery among added entries, deep freezing with shallow storage, a late read
  that must not be kept, and timer eviction.
- `harness-ownership`: the two deferred groups — a handle operation queued on the Session line
  whose invocation ends first, and the tool-driven owned-subagent cases (abort with the parent,
  abort its child and continue).
- `harness-scheduler-retention`: withdrawing an input queued in a forgotten conversation below an
  abort-marked owner, withdrawing one two ended levels below an owner whose intent appears later,
  and proving a queued input causes no withdrawal scan when nothing cancels.
- `harness-storage-failure`: containment of a throwing view observer, and an unformattable value
  that faults only the task and ends only the watch.
- `harness-nested-tools`: the late-subscriber agent-event snapshot and updates for several running
  nested calls.
- `harness-structured`: the seven wait-graph cases — a two-task cycle, a cycle through a held
  owner, a cycle two holds deep, a background wait on its conversation owner, the
  orchestrator/worker/reviewer cycle, a diamond, and a longer cycle.
- `spec-usage`: the spec's usage examples are now a typed Python module, so mypy checks them the
  way the upstream TypeScript compile does; the test asserts the translation exists.
- `conversation-transcript` and the windowed `harness-view`: every upstream case in both suites is
  translated — 16 in `test_conversation_transcript.py` and 23 in `test_harness_view.py`, each named
  for its upstream case — covering the entry window a conversation view carries (`from`, `min`,
  `max`, `before`), the bounds refused at attach, the one splice that drops past `max` while setting
  `before`, one window per attached consumer, and `ConversationTranscript`'s `earlier` marker, gaps,
  page-by-page fills, shared loads, retried loads, and `resume`. Two of those cases carry
  observer-only assertions and one a garbage-collection probe; all three are named in **Remaining
  deviations**. The new upstream `test/conversation-view-bench.ts` is excluded as a benchmark helper.
  The window is read only through `Conversation.watch(context, window)` and
  `ConversationTranscript.resume`, the two seams upstream v1.5.0 has: `Conversation.viewState()` is
  gone from upstream `src/`, so the port's `view_state` getter and its `ConversationViews.state`
  mount are deleted here rather than carried into #44.

## Section 12: API footguns

Every bullet in spec section 12 has a translated test. The mapping groups bullets by area; a
bullet may be pinned by more than one file.

| Footgun area | Tests |
| --- | --- |
| Detached draft work, read-after-write, long transactions, reads inside a commit | `test_session_tables.py`, `test_session_documents.py`, `test_session_states.py` |
| Writing to a busy conversation; queued items after a failed run; display-only writes | `test_harness_inbox.py`, `test_harness_submissions.py`, `test_harness_input_entries.py` |
| Compaction spend, heads and edits during compaction, summary timestamps | `test_harness_compaction.py` |
| Raw head writes into the past; raw transcript vs model context | `test_harness_view.py`, `test_harness_context.py`, `test_harness_inbox.py` |
| Owned work holds its owner; work created by hooks; compensation in abort handlers | `test_harness_ownership.py`, `test_harness_tasks.py`, `test_harness_tasks_recovery.py` |
| Hook memo names; double-counted subagent spend | `test_harness_tools.py`, `test_harness_usage.py` |
| Explicit creation; family initialization; wrong fork setting; schema stability | `test_session_documents.py`, `test_session_definitions.py`, `test_session_forks.py` |
| Checkpoint starvation (including a `checkpoint_when` that always returns `False`); trusted immutable revisions; watch activation, buffering, and stop | `test_session_checkpoints_migrations.py`, `test_session_watches.py`, `test_session_states.py`, `test_session_definitions.py` |
| Durable progress cadence; large terminal results | `test_harness_live_deltas.py`, `test_harness_tasks.py` |
| Sleeping inside a phase; work before a sleep; sleep deadlines and the Harness clock | `test_harness_sleep.py`, `test_harness_generation_recovery.py`, `test_harness_compaction.py` |
| A parked task's abort handler; a restart that is not progress | `test_harness_blocked.py` |
| A report's `info`; a failed run's queued inputs | `test_harness_reports.py`, `test_harness_failed_run_inbox.py` |
| Reserved `pi.` names; early resource disposal; guards out of a selection | `test_harness_input_entries.py`, `test_harness_registry_agent.py` |
| Selection edits and copies; copies are one-time; moving default selections | `test_harness_conversations.py`, `test_harness_registry_agent.py` |
| Environment on recovery; unstable prompt text | `test_harness_tools_recovery.py`, `test_system_order_cache_e2e.py` |
| Non-cooperative code at close; objects outliving the Harness; fatal storage errors | `test_harness_storage_failure.py`, `test_harness_lifecycle.py`, `test_harness_view.py` |
| JSONL durability | `test_jsonl_storage.py` |

## Remaining deviations

These are deliberate and are also recorded under **Allowed deviations** in `PARITY.md`.

- **Live provider measurements.** Two cases need real provider credentials and network access:
  the Codex prompt-cache measurement and the live Anthropic prompt-cache measurement. They skip
  when the credentials are absent; the committed request and cache-key shapes are asserted with
  fake clients in the adapter suites.
- **Code Mode, images, Cloudflare, PowerShell, Windows, benchmarks.** Out of scope by
  [ADR 0001](./adr/0001-parity-baseline-and-boundary.md); the excluded upstream suites and the
  benchmark helpers in `testing` are listed as `excluded` in `PARITY.md`. The new 1.5.0
  `test/conversation-view-bench.ts` is a benchmark helper over the windowed view and is excluded on
  the same basis; the window shapes it measures are asserted behaviorally in
  `test_harness_view.py`.
- **`harness-view-run` is not translated by the windowed view.** Upstream v1.5.0 adds
  `test/harness-view-run.test.ts` — "a run as the conversation view shows it" and "streamed
  partials", 14 cases that read run state (`pi.live`, `pi.inbox`, streamed partials, tool slots)
  out of the windowed view's operations. The window itself is #45's contract and is tested here;
  these cases belong to the tickets that own what the view renders — #47 for the streamed answer and
  its partials, #48 for the failure and restart-cutoff states — and no ticket claims the file yet.
- **Telemetry and pi-ai client identity.** `HarnessOptions.telemetry`, `DURABLE_TELEMETRY_SCHEMA`,
  `HarnessOptions.identity`, and `DEFAULT_HARNESS_IDENTITY` are excluded by
  [ADR 0002](./adr/0002-upstream-1-4-0-scope.md): they describe pi-ai and pi-telemetry integration
  with no normative spec section, and their observable content is available through `on_report`,
  `inspect()`, the task graph, the view, and the event stream.
- **IndexedDB and Workers.** `IndexedDbStorage` and `test-workers/*` are browser and Cloudflare
  backends, excluded with the adapters they exercise (ADR 0002).
- **Timers.** Python's default `set_timer` arms an asyncio timer for both foreground and background
  timers, because an asyncio timer cannot keep the process alive; the foreground/background split,
  the decline path, the fallback, and the one-time report are still exercised with a fake host
  timer in `test_harness_sleep.py` and `test_session_document_retention.py`.
- **JavaScript host instrumentation.** The upstream ownership case "keeps ownership queries off
  finished subtrees" counts `Set` iterations with a patched prototype. Python has no equivalent
  hook, so the same invariant is covered by the `scheduler_index_sizes` assertions in
  `test_harness_scheduler_retention.py` and `test_harness_scheduler_indexes.py`.
- **A window's dropped entries are proven by what a watch holds, not by a GC probe.** The upstream
  view case "lets a long-lived watch drop the entries that left its window" keeps a `WeakRef` to an
  entry that fell out of the window and asserts it is collected. CPython does not weak-reference a
  `list` or a `dict`, and collection is not observable in a test, so
  `test_harness_view.py::test_lets_a_long_lived_watch_drop_the_entries_that_left_its_window`
  asserts the same retention property from the watch's own value: the published window no longer
  holds the first entry and its `before` names it, which is what would otherwise pin the object.
- **The observer half of two window cases waits for #44.** Upstream `test/harness-view.test.ts`
  proves one window per attached consumer with both `watchConversation` and `observeConversation`:
  the case "gives each observer its own window over one mount" attaches its narrow consumer as an
  observer, and "rejects window bounds that are not integers or put `max` below `min`" asserts one
  `observeConversation(id, recorder, context, { max: 0 })` rejection. `ConversationViews.attach`
  already takes one window per attached consumer — watches are the consumers that exist today — so
  `test_harness_view.py` translates both through `watch(context, window)` alone, and #44 (`Observers
  attach on the line`) adds the observer assertions under the same names.
- **JavaScript realms and thenables.** The two containment cases that use another realm or a
  hand-written thenable become one Python test over a plain awaitable and a rejected coroutine;
  Python has one await protocol and no cross-realm promises.
- **`spec-usage`.** Upstream only compile-checks the spec examples; so does this port. The typed
  example module is checked by mypy, and the behavioral coverage of the same patterns lives in the
  suites named above.
- **`HooksOf`.** Python's type system cannot conditionally extract a task's hook map from a
  generic task token, so `HooksOf` is an `Any` alias. `hook()` still accepts the handler mapping,
  and the hook dispatch behavior is tested in `test_harness_tools.py`,
  `test_harness_compaction.py`, and `test_harness_registry_agent.py`.
- **Persisted shapes.** Records stay camelCase; Python-facing objects are snake_case. This is the
  documented naming deviation in `PARITY.md`, not a semantic difference.
- **A task cut off at open reports nothing until #49.** Upstream v1.5.0 has no parked task: its case
  `test/harness-blocked.test.ts:136` faults an extension's task cut off `settings.restart_limit`
  times in a row at open and reports `{kind: "task_faulted", reason: "restart_limit"}` with the
  message `Task <id> (<kind>) was cut off 3 times without progress`. The port still parks — the
  parked state, its `restart_limit` blocked reason, `retry_task()`, and the wake exclusion are #49's
  to delete — and 1.5.0 carries no report kind for a task that is blocked but not ended, so the
  `task_parked` report #34 deleted has no replacement yet.
  `test_harness_blocked.py::test_interim_parked_task_reports_nothing_until_49` is the port-side
  interim case that stands in for the upstream one until #49 replaces it.
- **The state getters' suite, translated rather than dropped.** Upstream 1.5.0 deletes
  `documentState()`, `ConversationHandle.viewState()`, the task-graph state getter, and
  `subscribeCommits()`, so `test_session_states.py` went with them. Each guarantee it carried is
  proved again on the seams that replaced them:
  - an absent address attaches nothing → `test_session_observers.py::test_returns_none_for_an_absent_address_and_attaches_nothing`;
  - the value at attachment, one call per stored publication with its own operations, and the line's
    atomicity → `test_starts_at_the_committed_value_and_advances_at_each_stored_publication_with_its_ops`
    and `test_attaches_atomically_on_the_line_a_commit_queued_before_it_is_in_its_value`;
  - independent attachments, and nothing after a detach → `test_gets_nothing_after_detach_which_is_idempotent_and_unpins_the_document`
    and `test_gets_nothing_of_the_commit_when_another_observer_detached_it_first`;
  - the delivered value is the object the publication and the snapshot carry, unfrozen and uncopied →
    `test_hands_the_very_committed_value_the_publication_and_the_snapshot_carry`;
  - a fork copy hydrated cold for an observer → `test_hydrates_a_definition_free_fork_copy_for_an_observer`;
  - a migration shown whole, and a document cached by an older token →
    `test_session_checkpoints_migrations.py::test_sends_observers_of_an_older_shape_a_root_replacement`
    and `test_session_document_retention.py::test_keeps_a_reloaded_newer_version_cached_while_an_older_observer_is_attached`;
  - continuing after the cache is emptied → `test_continues_from_the_value_it_holds_once_the_cache_is_emptied`;
  - the close's seal, and a commit that settles behind it →
    `test_takes_nothing_from_a_commit_that_settles_after_the_close_sealed_it`;
  - a retired incarnation held by nothing →
    `test_session_document_retention.py::test_keeps_no_value_of_a_document_retired_for_good_while_an_observer_is_attached`.
  Two of its cases need no proving any more: the `Delivery(kind, sequence)` counter was a property of
  the deleted state subscription — an observer is handed a `WatchCall`, which carries no sequence —
  and the retirement's `null` value was superseded by #43, which made a retirement a value call with
  the last value and `retired` set, so the observer half is `test_follows_a_conversation_documents_address_through_a_retirement_into_its_recreation`
  and the watch half lives in `test_session_watches.py`.
- **The task document's watch, ended by its owner, landed with #54.** All seven upstream
  `harness-observers` task-document cases — `:290`, `:331`, `:352`, `:373`, `:386`, `:417` and
  `:441` — are translated, one for one, in `tests/test_harness_observers.py`: the commit that makes
  the owner task terminal releases the watch's pin and ends it `ended` once the retirement it
  published has been delivered
  (`test_delivers_the_retirement_of_the_commit_that_ends_its_task_then_ends_ended`, whose single
  delivered call is that retirement — the value held before the commit, no operations, `retired` set,
  and the address absent for good afterwards;
  `test_keeps_no_task_document_watch_alive_once_its_task_ended`, whose weakref half proves the ended
  watch keeps neither itself in the Session's cache nor anything of the Harness); a watch whose
  listener never started stays open after its task ended, until the Harness's close ends it `ended`
  (`test_a_watch_never_started_when_its_task_ends_ends_with_the_close_keeping_nothing`) or a storage
  failure ends it `failed` with that error
  (`test_a_watch_never_started_when_its_task_ends_fails_with_the_harness`); a watch busy in an earlier
  call fails with the Harness *before* its retirement is ever delivered
  (`test_a_watch_busy_in_an_earlier_call_when_its_task_ends_fails_before_its_retirement_call`); a
  watch whose retirement call is still running ends only after that call returns
  (`test_a_watch_whose_retirement_call_runs_when_its_task_ends_ends_only_after_it_returns`, which
  asserts `closed` unsettled while the call runs rather than that both events happen); and every
  change a listener was handed across the end is strict JSON carrying nothing of the Harness, which
  the Harness's own collection then proves
  (`test_hands_listeners_changes_that_keep_nothing_of_the_harness`). No case of the family is left
  deferred: `EXCLUDED_WATCH_CASES` is gone, and the module docstring records that all fifteen upstream
  cases are translated. The one case of that family that observes rather than watches, `:308` — "an
  observer gets the retirement, then its end, at the publication of the commit that ends the task" —
  #44 implemented for real, so `ended` was reachable for an observed owner that ended before this
  ticket made it reachable for a watch.
- **Two cases here have no upstream counterpart and are named for what they prove.**
  `test_harness_observers.py::test_catches_an_entry_dropped_by_the_first_page` is the observing face
  of #45's window drop: upstream proves a drop only from a watch, and the bounded window is #45's
  surface, so the observer is proved to take the same drop — the entry its window opened on leaves its
  value by one splice, and the same change moves the `before` marker — and to be handed exactly the
  calls the watch of the same mount is handed. Its sibling in `test_harness_view.py`,
  `test_gives_a_rebuilt_mount_the_entries_its_predecessor_never_held`, replaced this ticket's earlier
  case for the flat active-range mount, which #45's window superseded: it now proves on the observer
  seam that a mount dropped with its last consumer is rebuilt from Storage, head marker and all.
- **A retired value's garbage collection is measured through the cache.** Upstream's "keeps no value
  of a document retired for good, while still attached" keeps only a `WeakRef` of the value and
  expects the collector to take it. CPython cannot take a weak reference to the plain `dict` a
  document value is, so the same guarantee is measured where the Session itself holds the value:
  `document_cache_sizes` empty in
  `test_session_document_retention.py::test_keeps_no_value_of_a_document_retired_for_good_while_an_observer_is_attached`.
  Where an object *can* be weakly held — the host's own observer — the upstream collection case is
  translated directly: `test_keeps_no_detached_observer_alive`.

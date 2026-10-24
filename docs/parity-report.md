# Parity report: v1.3.0 audit

**Baseline:** `@earendil-works/pi-durable` v1.3.0, tag `v1.3.0`, commit
`71023aec1546d6d677a65669c5665c0927ce0dd0`.

**Normative reference:** [`spec-1.3.0.md`](./upstream/spec-1.3.0.md).

**Contract:** [`PARITY.md`](../PARITY.md). Every root export and subpath row is `tested` or
`excluded` there; this report records how the 56 in-scope upstream suites were translated and
what remains different.

## Result

- All 56 in-scope upstream `test/*.test.ts` suites have a Python translation.
- The six excluded suites are `codemode`, `codemode-describe`, `codemode-models`,
  `images-wasm-module`, `sqlite-cloudflare`, and `tools-images`; the exclusions are recorded in
  `PARITY.md` (Code Mode, the image implementation, and the Cloudflare adapter).
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
| `harness-compaction` | `test_harness_compaction.py` |
| `harness-context` | `test_harness_context.py`, `test_harness_context_ranges.py` |
| `harness-conversations` | `test_harness_conversations.py` |
| `harness-events` | `test_harness_events.py` |
| `harness-generation-recovery` | `test_harness_generation_recovery.py` |
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
| `harness-scheduler-indexes` | `test_harness_scheduler_indexes.py` |
| `harness-scheduler-retention` | `test_harness_scheduler_retention.py` |
| `harness-storage-failure` | `test_harness_storage_failure.py` |
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
| `session-states` | `test_session_states.py` |
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
| Checkpoint starvation; trusted immutable revisions; watch activation, buffering, and stop | `test_session_checkpoints_migrations.py`, `test_session_watches.py`, `test_session_states.py` |
| Durable progress cadence; large terminal results | `test_harness_live_deltas.py`, `test_harness_tasks.py` |
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
  [ADR 0001](./adr/0001-parity-baseline-and-boundary.md); the six excluded upstream suites and the
  benchmark helpers in `testing` are listed as `excluded` in `PARITY.md`.
- **JavaScript host instrumentation.** The upstream ownership case "keeps ownership queries off
  finished subtrees" counts `Set` iterations with a patched prototype. Python has no equivalent
  hook, so the same invariant is covered by the `scheduler_index_sizes` assertions in
  `test_harness_scheduler_retention.py` and `test_harness_scheduler_indexes.py`.
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

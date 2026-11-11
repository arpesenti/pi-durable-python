# Parity report: v1.5.0 audit

**Baseline:** `@earendil-works/pi-durable` v1.5.0, tag `v1.5.0`, commit
`f9a26365568127461c47a07a37dcb42c140a3ac5`.

**Normative reference:** [`spec-1.5.0.md`](./upstream/spec-1.5.0.md), with
[`group-commit-1.5.0.md`](./upstream/group-commit-1.5.0.md) for the group-commit contract behind
section 4. The [`spec-1.4.0.md`](./upstream/spec-1.4.0.md) baseline stays as the 1.4.0 audit record,
and [`parity-report.md`](./parity-report.md) as its audit: that file states the v1.4.0 baseline and
this one states this.

**Contract:** [`PARITY.md`](../PARITY.md). The matrix carries the statuses; this report carries the
suites, the exclusions, and the deviations.

**Scope records:** [ADR 0001](./adr/0001-parity-baseline-and-boundary.md) (baseline and boundary),
[ADR 0002](./adr/0002-upstream-1-4-0-scope.md) (the 1.4.0 exclusions, still in force), and
[ADR 0003](./adr/0003-upstream-1-5-0-scope.md) (the 1.5.0 exclusions).

**This file owns the v1.5.0 exclusion record.** One place, so the same fact is never signed off
twice: the suites this baseline adds, the upstream cases deliberately left untranslated, every 1.5.0
deviation, and every 1.5.0 exclusion are named here and nowhere else. `PARITY.md`'s `excluded` rows
carry the reason a reader of the matrix needs, ADR 0003 records the scope decision behind them, and
neither is the record of what remains untranslated — this file is.

## Status of this audit

**Signed off at this commit.** This audit is [#52](https://github.com/arpesenti/pi-durable-python/issues/52), and
it re-derived every claim from the tree at the commit it closes rather than from ticket receipts: the
matrix was read row by row against the vendored upstream at tag `v1.5.0`, each suite's cases counted
against its upstream file, every term the sync added or amended in `GLOSSARY.md` checked against the
seam and the suite that proves it, the release-notes breaking set assembled from the public-surface
diff of the `v1.4.0` tag, and the gates re-run after the fixes this audit makes (**What this audit
found and fixed** below).

**Result.** Every in-scope suite at this baseline has a Python translation, every case of every
translated suite is named in its module docstring or below, and every upstream case deliberately left
untranslated is named with its reason under **Cases deliberately left untranslated** and **Exclusions**.
`PARITY.md` carries no `planned` or `code` row; every `excluded` row carries its reason; the port-
additions section names the v1.5.0 baseline. The `planned` rows the re-baseline left behind are
closed here: 3.3, 3.6, 3.7, 5.6 and 10.1 move to `tested` on the suites named on each row, and the
`registryReader` value is excluded by name under ADR 0003's Chord boundary, its three upstream cases
named below.

**The in-scope suite count for this baseline is 83.** Upstream v1.5.0 holds 99 `test/*.test.ts`
suites. The v1.4.0 record's 61 in-scope suites minus the two upstream deleted at this baseline
(`harness-events` with the agent event stream, `session-states` with the state getters) leave 59;
the 30 suites v1.5.0 adds split into 24 in-scope and 6 excluded, named under **Exclusions this
baseline takes** and **Cases deliberately left untranslated**. The 16 excluded suites of this
baseline are the 10 the v1.4.0 record excluded that upstream still holds at this baseline (Code Mode
×4, telemetry ×2, pi-ai identity, the image implementation, the Cloudflare and browser backends),
beside the 6 new ones; the v1.4.0 record's 12 lost the two `test-workers` suites to the deletion
above. The pass condition every `tested` row rests on: `ruff check src tests`, `mypy`, and
`pytest` on the verified version set, clean at this commit — re-run after the audit's fixes, with
the counts under **Gates** below.

The free-threaded legs of the verified set, 3.14t and 3.15t, are **not installed in the environment
this audit ran in**, so this sign-off states the gate results on the installed legs only and claims no
free-threaded pass.

`PARITY.md` names these as the suites this baseline adds, and each is a real upstream
`test/*.test.ts` at tag `v1.5.0`: `harness-group-commit`, `session-group-commit`,
`session-deferred-commits`, `harness-deferred-progress`, `harness-observers`, `session-observers`,
`conversation-transcript`, `harness-run-failure`, `harness-turn-limit`, `harness-after-run`,
`harness-continue`, `harness-submit-in-commit`, `harness-idle-scopes`, and `harness-restart-failure`.
The table below carries a row for every in-scope suite this baseline adds, translated by the porting
tickets or by this audit, with the case it deliberately leaves untranslated named and reasoned under
**Cases deliberately left untranslated** when it does.

## What this baseline changes in the 1.4.0 record

The 1.4.0 record is kept as it closed at tag `v1.4.0`; these are the places where v1.5.0 upstream
moved the ground under it, recorded here rather than by editing that file.

- **`harness-events` is gone upstream, with the agent event stream.** #46 deleted
  `src/harness/events.ts`, spec section 9.4 (Agent events), and the port's `test_harness_events.py`,
  so the 1.4.0 record's rows for that suite describe a file this tree no longer holds. What a host
  could observe through the adapter is now asserted on the seams that remain, named under
  **Remaining deviations** below.
- **`session-states` has no suite of its own any more.** Upstream 1.5.0 deletes the state getters and
  the commit subscription its cases drove; the 1.4.0 record's `session-states` row maps to the
  suites that carry its guarantees now, itemised under **Remaining deviations** below.
- **The telemetry bullet's event stream is gone.** The 1.4.0 record says the excluded telemetry's
  observable content is available through `on_report`, `inspect()`, the task graph, the view, and the
  event stream. At this baseline it is `on_report`, `inspect()`, the task graph, and the view.
- **Section 9.4 is the task graph view.** The 1.4.0 record's references to 9.4 as the agent event
  section and 9.5 as the task graph are re-keyed here: 1.5.0 numbers the task graph view 9.4.
- **The `test-workers/*` suites are gone upstream.** The v1.4.0 record excludes them with the
  browser and Cloudflare backends (ADR 0002); the v1.5.0 tree no longer holds the directory, so the
  exclusion they recorded now stands on the backend alone. This audit deleted the `PARITY.md` row
  that still named them and corrected the ADR 0003 sentence that said 1.5.0 "touches none" of the
  ADR 0002 exclusions (it deleted one of them).

## The suites this baseline adds or remaps

The 1.4.0 record's [suite table](./parity-report.md#suite-translation) stands for every suite this
baseline left alone. These are the rows this baseline adds or moves.

| Upstream suite | Python translation |
| --- | --- |
| `conversation-transcript` | `test_conversation_transcript.py` (all 16 `it()` cases; see **Cases completed under this baseline**) |
| `harness-after-run` | `test_harness_after_run.py` (all 29 `it()` cases as 30 tests: one case carries two runs; the module docstring names the split) |
| `harness-context-range` | `test_harness_context_ranges.py` (the one case — a kept range whose scan ran off the line is not advanced — beside the `harness-context` kept-read cases it belongs with; #49) |
| `harness-continue` | `test_harness_continue.py` (12 of the 13 `it()` cases; the one excluded is telemetry-only, named in the module docstring, and the one narrower translation is named there too) |
| `harness-deferred-progress` | `test_harness_deferred_progress.py` (all 11 `it()` cases, plus the three port-side cases; see **Cases completed under this baseline**) |
| `harness-group-commit` | `test_harness_group_commit.py` (16 of the 17 `it()` cases; the record-cache case `:646` is excluded, named in the module docstring and under **Exclusions this baseline takes**) |
| `harness-hook-reads` | `test_harness_hook_reads.py` (both `it()` cases; the hook case this audit translated is named under **Cases completed under this baseline**) |
| `harness-idle-scopes` | `test_harness_idle_scopes.py` (all 14 `it()` cases — 24 tests with the two `it.each` tables — plus the incremental idle state they measure; #49) |
| `harness-json` | the module itself, `harness/json.ts` → `harness/util.py`'s `assign_json`, proved where the port commits partials (see the `harness/partial-changes.ts` entry under **Remaining deviations**); the suite's one case is left untranslated, named under **Cases deliberately left untranslated** |
| `harness-nested-tools` | `test_harness_nested_tools.py` (the agent-event half went with `test_harness_events.py`, deleted by #46) |
| `harness-observers` | `test_harness_observers.py` (all fifteen upstream cases, named under **Remaining deviations**) |
| `harness-read-only` | `test_harness_read_only.py` (all four `it()` cases) |
| `harness-restart-failure` | `test_harness_restart_failure.py` (all 18 `it()` cases, plus one port-authored case named under **Remaining deviations**) |
| `harness-run-failure` | `test_harness_run_failure.py` (all five `it()` cases) |
| `harness-submit-in-commit` | `test_harness_submit_in_commit.py` (19 of the 23 `it()` cases; the four excluded are telemetry-only, each named in the module docstring) |
| `harness-tool-hooks` | `test_harness_tool_hooks.py` (all 18 `it()` cases; see **Cases completed under this baseline**) |
| `harness-turn-limit` | `test_harness_turn_limit.py` (all eleven `it()` cases) |
| `harness-view-run` | `test_harness_view_run.py` (all 13 cases; #47 corrected this row, which named three more files whose cases belong to their own upstream suites and cite this one nowhere) |
| `search` | `test_tool_search.py` (the `rankDocuments` cases one for one over the port's BM25 ranker, beside the six `tool-search` deferred-loading cases; the eight Unicode cases left untranslated are the `rankDocuments` `it.each` "matches Unicode queries against %s" of `test/search.test.ts`, one case per row -- `:65` 搜索仓库问题, `:66` 文書を検索, `:67` 문서 검색, `:68` ค้นหาเอกสาร, `:69` Поиск документов, `:70` البحث عن الملفات, `:71` फ़ाइल खोजें, `:72` Rechercher un café -- each named with its line in the module docstring) |
| `session-adoption` | `test_session_adoption.py` (all 13 `it()` cases, translated by this audit; its dispatch-attachment case pins the fix under **What this audit found and fixed**) |
| `session-conversation-tails` | `test_session_conversation_tails.py` (all five `it()` cases, translated by this audit) |
| `session-deferred-commits` | `tests/test_session_deferred_commits.py` (all 25 `it()` cases; see **Cases completed under this baseline**) |
| `session-group-commit` | `test_session_group_commit.py` (30 of the 31 `it()` cases; the read-cache case `:635` is excluded on the ground named in the module docstring and under **Exclusions this baseline takes**, and the file's extra test functions are the parameterized value-rejection and order variants its docstring names) |
| `session-observers` | `test_session_observers.py` (the two upstream cases left untranslated are named in the module docstring, each with where its guarantee lives) |
| `session-states` | `test_session_observers.py`, `test_session_document_retention.py`, `test_session_checkpoints_migrations.py` (upstream 1.5.0 deletes the state getters this suite tested; where each of its guarantees is proved now is itemised under **Remaining deviations**) |

## Cases completed under this baseline

- `conversation-transcript` and the windowed `harness-view`: every upstream case in both suites is
  translated — 16 in `test_conversation_transcript.py` and 23 in `test_harness_view.py`, each named
  for its upstream case — covering the entry window a conversation view carries (`from`, `min`,
  `max`, `before`), the bounds refused at attach, the one splice that drops past `max` while setting
  `before`, one window per attached consumer, and `ConversationTranscript`'s `earlier` marker, gaps,
  page-by-page fills, shared loads, retried loads, and `resume`. Two of those cases carry
  observer-only assertions and one a garbage-collection probe; all three are named in **Remaining
  deviations** below. The new upstream `test/conversation-view-bench.ts` is excluded as a benchmark
  helper. The window is read only through `Conversation.watch(context, window)` and
  `ConversationTranscript.resume`, the two seams upstream v1.5.0 has: `Conversation.viewState()` is
  gone from upstream `src/`, so the port's `view_state` getter and its `ConversationViews.state`
  mount are deleted here rather than carried into #44.
- **Two cases here have no upstream counterpart and are named for what they prove.**
  `test_harness_observers.py::test_catches_an_entry_dropped_by_the_first_page` is the observing face
  of #45's window drop: upstream proves a drop only from a watch, and the bounded window is #45's
  surface, so the observer is proved to take the same drop — the entry its window opened on leaves its
  value by one splice, and the same change moves the `before` marker — and to be handed exactly the
  calls the watch of the same mount is handed. Its sibling in `test_harness_view.py`,
  `test_gives_a_rebuilt_mount_the_entries_its_predecessor_never_held`, replaced this ticket's earlier
  case for the flat active-range mount, which #45's window superseded: it now proves on the observer
  seam that a mount dropped with its last consumer is rebuilt from Storage, head marker and all.

- **`harness-hook-reads` and the task runtime's raw-history read.** #56 translates "pages a task's
  conversation history by entry ID range" (`test/harness-hook-reads.test.ts:50-93`) case for case into
  `test_harness_hook_reads.py`, the suite `PARITY.md`'s 5.1 row now sends a reader checking
  `TaskRuntime.entries()` to. One case there has no upstream counterpart and is named for what it
  proves: `test_a_task_entries_read_waits_for_the_commit_holding_the_line` shows the read is taken on
  the Session line, where upstream leaves it to `readOnLine` in its own implementation
  (`src/harness/scheduler.ts:2828-2834`) and to `TaskRuntime.commit`'s docstring — a commit whose
  storage call is held has admitted its entry and stored nothing of it, the read is still pending
  behind that commit, and it settles afterwards with that entry in the history it returns.
  The file's other upstream case, "reads raw history and single entries from a hook" (`:17-48`),
  deferred to #36 and #48 while they owned the `entry()`/`entries()` seams, was translated by this
  audit, `test_reads_raw_history_and_single_entries_from_a_hook`: the hook's `on_yield` asks the api
  for the conversation's entries and one entry by its token, and the port's `entry()` takes the
  entry's definition kind, so the "another kind" read goes through a `test.other` entry definition
  registered beside the task's.

- **#47, the deferred-commit mechanism and `session-deferred-commits`.** The Session now holds a
  deferred commit's changes in memory and stores them at the next trigger: a commit that reads or
  writes that document, a drop of its tracker, a version reload, close, or one foreground timer held
  for the earliest `within_ms`. `tests/test_session_deferred_commits.py` translates
  `test/session-deferred-commits.test.ts` case for case, 25 of them, with the upstream title in each
  docstring. The two-audience split it proves is the mechanism's point: `AdoptedDocuments` separates
  what the Session's own followers were shown (`observed`) from what Storage made durable (`stored`),
  so a held store is published to the host's `observe_commits()` observer alone, never again to the
  watches and views that saw the partial when it was adopted.
  `tests/test_harness_live_deltas.py` follows upstream here, not its own earlier translation: the
  streamed-partial case follows memory through the internal seam (`followCommits`,
  `session-support.ts:250-256`) and slices the stream positionally, while the three cases that assert
  what Storage received attach a host `CommitObserver` (`harness-live-deltas.test.ts:118,342,385,428`),
  and the tools case sets `progress.persist_within_ms` as upstream's does (`:339`).
  What the same behaviour costs above the Session, translated case for case: `tests/test_harness_deferred_progress.py`
  runs all 11 cases of `test/harness-deferred-progress.test.ts`, plus three port-side cases. The first
  pins the negative the mechanism rests on: the partial answer and the streaming
  tool output visible to watches while Storage is held, converted to durable by a crash, by a clean
  close, or by their deadline, with the progress commits of a cut-off task never counted as progress
  (`#11139`) and a third party reading the held answer inside its own commit. The host's
  `CommitObserver` is asserted there to receive nothing at all for a partial only memory adopted --
  the assertion reads the published values, not the operation paths, because a held revision can reach
  Storage folded into another commit's base write. The other two landed with #47 and hold the tool
  side of the window, which upstream has no case for: a running call's `details()` value riding the
  held commit to the next trigger, and a tool's own `api.commit` honouring the persistence mark it was
  handed.
  In `tests/test_session_group_commit.py`, the eleven cases that had waited on this mechanism (`:325`,
  `:360`, `:390`, `:596`, `:615`, `:662`, `:680`, `:839`, `:857`, `:885`, `:902`) are translated into
  its table, and an entry that named no upstream case at all is gone from that module's deferral block;
  the record-cache case (`:635`) stays the one exclusion it records there.

- **`harness-tool-hooks` and the `afterTool` chain over every result the task writes (#60).** All 18
  cases of `test/harness-tool-hooks.test.ts` are translated case for case into
  `tests/test_harness_tool_hooks.py`, each named for its upstream line: `:137`, `:200`, `:240`, `:260`,
  `:313`, `:372`, `:413`, `:443`, `:466`, `:508`, `:541`, `:585`, `:633`, `:657`, `:675`, `:704`, `:726`
  and `:784`. The translation exposed the one gap the earlier partial proofs left: `harness/tool.py`
  settled the `call` phase's results -- the tool did not resolve, the arguments are invalid, or
  `beforeTool` blocked the call -- without the `afterTool` chain, so no case could see a hook run on
  an unavailable, invalid, or blocked result. The port now carries upstream's `reject` beside
  `settle_from_slot`, which runs the chain and the bounds (the tool's, or the defaults when the tool
  never resolved) over those results before settling; the nine cases that rest on it were red without
  it, and the abort-result and restart-cutoff cases were shown red with the chain's call removed
  (`:413` fails on its `seen` list, `:585` reaches none of its assertions past the signal only the
  pinned hook resolves). The cases that prove row **12 API footguns**' abort-handler bullet are `:413`
  (the chain on an `aborted` result, not the aborted results of calls that never started), `:443`
  (a throwing hook reported, the result settled as built), and `:466` (the last attempt before the
  restart limit without the hooks), beside the `restart_limit` result's own hooks at `:585`. No case
  in the file is port-side: the 18 are upstream's, and the siblings that already prove parts of the
  chain -- named in the module docstring, as this file's other entries do -- are not counted twice.

- **The suites and cases this audit translated.** The re-baseline left two upstream suites of this
  baseline with no translation, no deferral named, and no `PARITY.md` row: `session-adoption` and
  `session-conversation-tails`. Both are now translated case for case, in the upstream order, with
  the upstream line in each docstring:
  - `tests/test_session_adoption.py` carries all 13 cases of `test/session-adoption.test.ts` over
    the port's seams: the upstream `FaultyStorage` records `adopt` and its own resolution in
    `order` over the port's controlled storage, `observe_stored_commits` is the port's
    `followCommits`, and `observe_commits` is the host's. The two audiences and the dispatch rule
    each case pins are the spec §9.1/§10.1 text: the receipt taken inside the commit, the adoption
    once Storage resolved, the Session failing without discarding when Storage rejects after
    adopting, the discard before adoption, the Session's own adoption error that never reaches
    Storage, Storage resolving without adopting, the own observers at resolve and the host
    listeners after, the host listeners registered when the dispatch runs and none for a commit
    published without one, the own observer's throw failing the Session after the hosts got the
    commit, a stored-but-rejected commit published before the failure, exactly what the host is
    handed when held changes are stored, the held store published to the host once Storage
    resolved, the JSONL commit published once the storage reclaimed its sidecar, and the held
    store Storage rejects after adopting. Its second-half dispatch case pins the fix under
    **What this audit found and fixed**.
  - `tests/test_session_conversation_tails.py` carries all five cases of
    `test/session-conversation-tails.test.ts`: read-once-then-follow, a new root known empty and a
    fork known only once read or written, a cutoff answered from the newest marker only at or after
    it, the bounded keep of newest entries (4096) and markers (256) by least-recent use, and the
    markers kept frozen. The upstream class's `size`/`markers` bounds are what the case asserts
    through, and this audit added them to `session/conversation_tails.py`, which lacked them.
  - The clause `PARITY.md` row 5.6 owed a case for -- `on_wake` runs off the line -- is
    `tests/test_harness_sleep.py::test_settles_a_commit_that_changes_the_wake_while_the_host_is_inside_on_wake`,
    port-side and named as such in its docstring, because upstream's `harness-sleep` suite has no
    case of its own for the clause and both sides' callbacks are synchronous.
  - The observer halves of the two window cases of `test/harness-view.test.ts` (above, under
    **Remaining deviations**) and the hook case of `test/harness-hook-reads.test.ts` (above).

## Cases deliberately left untranslated

One place per suite, so an audit reads the reason with the suite it belongs to. An entry here is
named and reasoned, not inferred: every case is an upstream case, and the entry says what stands in
its place in the port -- the seam that carries the guarantee, or the exclusion that removes the
component it measured.

The suite recorded before this audit -- `harness-tool-hooks`, with its reason and its owner
#60 -- is translated in full by #60 in `tests/test_harness_tool_hooks.py` (see **Cases completed under
this baseline**); with it, rows 8.4 Tool and 12 API footguns of `PARITY.md` moved to `tested` in that
commit. The entries below are the rest at this baseline, named case by case.

- **`beforeProviderRequest` in the upstream generation suite — #59.** The 1.5.0 addition (zero
  occurrences in the 1.4.0 spec) is driven upstream by two cases in `test/harness-tools.test.ts` at
  tag v1.5.0 (commit f9a2636): `:755` "chains beforeProviderRequest over every provider request
  body" and `:795` "reports a throwing beforeProviderRequest hook and continues the chain". They are
  not translated here, for a reason recorded rather than inferred: both drive the hook through
  pi-ai's faux provider seam — each request body built by `options.onPayload` inside the faux
  response step — while the port's provider seam is the `Models` protocol, whose payload is the
  model context that seam receives (the port has no pi-ai to build the provider API's own body;
  `PARITY.md` **Allowed deviations**, Model access), so the cases do not carry over one for one.
  The hook's semantics were ported from the vendored specification (the composition table at
  `spec-1.5.0.md:3351`, the `GenerationHooks` declaration at `:3394-3397`, the recovery prose at
  `:4926-4927`) and the shipped v1.5.0 source (the `providerPayload` chain in `harness/generation.ts:668-676`,
  wired through pi-ai's `onPayload`, with one `onPayload` per provider-body build). In their place,
  each clause the two upstream cases and the vendored specification state is proven by the
  port-authored cases named on `PARITY.md` row 7.2: the replacement reaching the provider after the
  messages `beforeRequest` fixed and before the body is sent, for that request only
  (`test_replaces_the_provider_payload_for_that_request_after_before_request`) — the `:755` clause of
  the first body; extension order, each handler seeing the previous one's payload, a throw reported
  and the chain continuing, and a decline leaving the payload alone
  (`test_before_provider_request_hooks_chain_in_extension_order_and_a_throw_is_reported`) — the
  `:755` composition and the `:795` throwing hook; the chain rerun on the rebuilt body when a close
  resends the same attempt, with the first request's replacement not leaking into the second
  (`test_a_close_resend_of_the_same_attempt_sees_the_rebuilt_provider_payload`) — the `:755` resend
  body; plus the two clauses no upstream case pins: compaction requests not using the hook
  (`test_compaction_requests_do_not_run_the_before_provider_request_hook`), and the exhaustive
  hook-name set the port runs updated in one place, the `GenerationHooks`/`ToolHooks`/
  `CompactionHooks` keys, with `hook()` refusing a name outside it for a built-in task
  (`test_hook_rejects_a_handler_name_outside_the_builtin_tasks_hooks`).

- **`harness-json` — the one case of `test/harness-json.test.ts`.** The case
  "does not merge a new `__proto__` member into `Object.prototype"` guards a JavaScript
  prototype-pollution class: assigning a `__proto__` key that the target lacked used to be able to
  merge into `Object.prototype` itself. Python dicts have no such merge: a new `"__proto__"` key is
  an ordinary own key of the target and nothing of the shared type changes, so the bug the case
  pins cannot be represented in this port's execution model, and the case is left untranslated
  rather than translated into a test of Python semantics it does not have. The adjacent guarantee —
  `__proto__`-like keys are taken as ordinary own keys, verbatim — is pinned in
  `test_json.py::test_accepts_null_prototype_like_keys_verbatim`, and the behaviour the case exists
  to protect, `assign_json` writing the partial, is asserted where the port commits partials
  (`test_harness_view_run.py`, `test_harness_live_deltas.py`, `test_harness_deferred_progress.py`).

- **`harness-partial-changes` — all seven cases of `test/harness-partial-changes.test.ts`.** The
  suite tests `PartialChanges`, the upstream module that collects, from the response's events, what
  grew in the partial since the last partial commit and commits exactly that. The port has no such
  module by design: it commits the copied partial and lets the draft layer diff it
  (the `harness/partial-changes.ts` entry under **Remaining deviations**), so the component the
  suite measures does not exist here and none of its seven cases is translated: "commits tool-call
  arguments equal to the final parse, at every piece size and commit rhythm", "handles a number
  ending and a string starting in one piece, and escapes split across pieces", "appends growing
  argument and text strings instead of reassigning them", "applies the changes of a failed commit in
  the next one", "re-applies a failed commit's changes even when it would have added the call",
  "falls back to the snapshot when the committed message lost a call whose changes were committed",
  and "commits the final arguments after the call ends, and drops text appends when a block ends".
  The observable guarantees those cases exist for — what a watch is handed, what Storage stores,
  what a reopened host recovers — are asserted case for case by the three suites named in that
  entry.

- **`session-cached-storage` — all seven cases of `test/session-cached-storage.test.ts`.** The suite
  measures the read-side record cache (`CachedStorage` in upstream's `session-support.ts`): a read
  kept only when no commit overlapped it, the bounded keep of ended tasks, a batch applied at its
  publication and not its receipt, the versions kept only for in-flight reads, the written record
  itself kept frozen and un-copied. This port carries no read-side record cache — the ground
  `PARITY.md` and the module docstrings name for the two excluded record-cache cases of
  `harness-group-commit` (`:646`) and `session-group-commit` (`:635`) — so the component the suite
  measures does not exist here and none of its seven cases is translated. The port's storage is
  asserted instead on what it stores: the group-commit suites, the adoption suite's receipt cases,
  and the storage conformance suite in `pi_durable.testing`.

- **`harness-registry-reader` — all three cases of `test/harness-registry-reader.test.ts`**
  (`:76`, `:100`, `:174`). The `registryReader` value adapts upstream's Chord v2 registry value;
  the port's harness reads the `registry` option through the ported `RegistryReader` type, and the
  value it wraps is the host's own. The three cases drive real Chord hosts — `host.registry()` over
  a composed value, a task that runs once a contribution brings its missing definition, and a
  follower of a contributed Harness failing when that Harness's Session closes — and no host to
  attach to exists here. Excluded under ADR 0003's Chord boundary, the same one drawn for
  `./storage/store`; the reason a reader of the matrix needs is on `PARITY.md`'s `excluded` row.

## Exclusions this baseline takes

The 1.4.0 exclusions ([ADR 0002](./adr/0002-upstream-1-4-0-scope.md)) stay in force and stay listed in
the 1.4.0 record. On top of them:

- **`test/conversation-view-bench.ts`.** The new 1.5.0 benchmark helper over the windowed view is
  excluded with the other benchmark helpers by
  [ADR 0001](./adr/0001-parity-baseline-and-boundary.md), on the same basis as the benchmark helpers
  in `testing` listed as `excluded` in `PARITY.md`; the window shapes it measures are asserted
  behaviorally in `test_harness_view.py`.
- **The attached-host storage subpath, pi-schema, and the telemetry host-callback member.** Excluded
  by [ADR 0003](./adr/0003-upstream-1-5-0-scope.md), each with its reason on the matching `excluded`
  row of `PARITY.md`'s **Excluded subpaths** table.
- **`NativeWatchCall`.** #56 excludes it by name. Upstream v1.5.0 re-exports it as one of four Chord
  watch types (`src/index.ts:1`) and names it nowhere else in its `src/` or `test/`. What durable hands
  a listener is `WatchCall`, and an observer's `advance`/`end` take `WatchCall`/`WatchEnd`
  (`src/types.ts:1112-1115`); this port's watches are opened only by `Conversation.watch()` and hand
  their listener exactly that. Chord is an internal implementation detail here
  ([ADR 0001](./adr/0001-parity-baseline-and-boundary.md), and the `Chord` row of `PARITY.md`'s
  deviations), so the port holds no Chord host that could hand a listener Chord's own native call and
  no port type plays the role: the name goes under ADR 0003's lockstep rule rather than riding into a
  release as a `planned` row for a host this port never had. The reason a reader of the matrix needs is
  on `PARITY.md`'s `excluded` row, and
  `test_types.py::test_the_public_types_exclude_the_native_watch_call_by_name` pins it beside the
  three watch re-exports of that same upstream line the port does carry.

- **`registryReader`.** The v1.5.0 value of `harness/registry-reader.ts`, excluded by name under the
  same Chord boundary [ADR 0003](./adr/0003-upstream-1-5-0-scope.md) draws for `./storage/store`:
  the value adapts upstream's Chord v2 registry value, and this port reads the harness's `registry`
  option through the ported `RegistryReader` type instead. Its three upstream cases are named under
  **Cases deliberately left untranslated**.
- **`codemode-imports`.** The fifth Code Mode file upstream added at v1.5.0 beside the four the
  v1.4.0 record excludes, under the same [ADR 0002](./adr/0002-upstream-1-4-0-scope.md) exclusion.
- **`store-storage` and `store-storage-commit`.** The suites of the attached-host storage subpath
  ADR 0003 excludes: the component they drive is Chord's store, and this port's storage is the
  `Storage` protocol with its bundled backends.
- **`session-cached-storage` and `harness-partial-changes`.** The suites of components this port does
  not carry: the read-side record cache (the ground named for the two excluded record-cache cases of
  the group-commit suites) and the `PartialChanges` module (the `harness/partial-changes.ts` entry
  under **Remaining deviations**). All seven cases of each are named under **Cases deliberately left
  untranslated**, with what stands in their place in the port.

- **The record-cache case of `harness-group-commit`.** `test/harness-group-commit.test.ts:646` — "reads
  no task record from Storage for more live and awaited tasks than the record cache's count" — is
  excluded outright: this port has no read-side record cache and no `CachedStorage`, so the component the
  case measures does not exist. Its session sibling, `test/session-group-commit.test.ts:635`, is excluded
  on the same ground and is the one exclusion `tests/test_session_group_commit.py` records there; each of
  the two modules names its own case in its header, and this is the record both point at.

## Remaining deviations under this baseline

These are deliberate and are also recorded under **Allowed deviations** in `PARITY.md`.

- **The streamed-partials tail of `harness-view-run` is told with the port's measures (#47).**
  Upstream v1.5.0 adds `test/harness-view-run.test.ts` — its one describe, "a run as the conversation
  view shows it", 13 cases that read run state (`pi.live`, `pi.inbox`, streamed partials, tool slots)
  out of the windowed view's operations. All 13 are in `test_harness_view_run.py`: #46 carried the ten
  run cases — they are where the assertions the deleted agent event adapter carried now ride — and #47
  added the streamed-partials tail, `:505` "streams a tool-call partial as appends at its argument
  paths", `:547` "commits a growing text and tool call at a cost that follows the new text, not the
  string lengths", and `:581` "keeps a watcher's memory proportional to a long streamed text and tool
  call, not to their commits". The first is portable whole; the other two weigh V8's heap, which this
  tree has no way to weigh, so each takes the measure its docstring names:
  - `:547` counts the bytes V8 allocated (`GCProfiler` beside `used_heap_size`). `tracemalloc` is
    disabled on the free-threaded builds the verified set runs, and a commit allocates its own share of
    plain Python strings however its operations read, so the case counts the bytes the commits *carry*
    instead: per commit that stays flat while the answer grows tenfold, where a commit that reassigned
    the whole string made it grow with the answer — the regression upstream wrote `PartialChanges` for.
    Making `session/drafts.py` never append a growing string -- writing every changed leaf as a
    whole-value set -- turns both bounds red, and so does making `harness/generation.py`'s partial
    writer commit only its first revision (five cases of this file fail that way, three of them here).
  - `:581` weighs the watcher's heap — 20 MB of slack for the 3 MB two 140 KB strings cost, where a
    copy of the text per commit cost well over 100 MB. This port cannot keep that bound and does not
    claim it: applying an append here builds a fresh copy of the whole text (`delta.py`), so a watcher
    that keeps every frame keeps one copy of the answer per frame, and
    `PARITY.md` records it under **Allowed deviations**. The portable half is asserted: the operations
    the watcher is handed stay proportional to the two strings -- the case asserts between 0.9x and 4x
    their bytes, and measures 0.998x of them over the 1611 commits it produces -- and no commit
    re-sends either string whole, a bound taken over every str any operation carries rather than over
    the appends alone, so a representation that re-sent the answer as a whole-value set fails here too.
  - `:505` needs no measure, only the shape. Upstream's faux streams a call's raw argument text and
    durable parses what has arrived (`harness/partial-changes.ts:64-84`); the port's adapters hold a
    call's arguments at `{}` until its whole text parses (the "Streaming tool-call arguments" row of
    `PARITY.md`), so the faux here reports them member by member as far as they have parsed, and the
    case pins durable's half: appends at each growing argument, the block set whole at most once per
    argument, and never the raw text beside the parsed arguments. Injecting a raw member into the
    committed partial, or writing leaves as whole-value sets, turns it red.
  - **#47 corrected the file list this entry and the suite table above carried.** Both named three
    further files as translations of this suite; nothing in those files cites `harness-view-run`, and
    all 13 cases are in the one file. The `runFailure` reads this entry quoted are assertions inside
    the two cases #46 did translate (`:372` and `:436`), not cases of their own, and what they now
    assert is written up on the 9.3 row of `PARITY.md`: `{inputs: [submission.id], reason: "aborted"}`
    on the newest state, the reason on the state that shows the run gone, both red when
    `live["runFailure"] = failure` is deleted from `src/pi_durable/harness/live.py`.
  - **#55 corrected the counts as it moved this entry.** The moved text said "14 cases", a second
    "streamed partials" describe, and "two upstream cases that read `runFailure`". The vendored
    upstream file has one describe and 13 `it()` cases, the three quoted above are its
    streamed-partials tail, and the two `runFailure` reads are assertions inside the two cases #46
    did translate.
- **`harness/partial-changes.ts` has no module of its own.** Upstream collects, from the response's
  events, what grew in the partial since the last partial commit — the text and thinking deltas, and a
  `StreamingJsonParser` fed with each call's raw argument text — and commits exactly that, so a partial
  commit costs the new text. This port commits the copied partial and lets the draft layer diff it
  (`harness/util.py`'s `assign_json` and `session/drafts.py`'s `_emit_changed`), which produces the same
  operations — appends for grown strings, appends inside a call's arguments — while comparing the whole
  strings, so its per-commit cost follows the answer's length rather than the new text. Nothing a host
  can read differs: what a watch is handed, what Storage stores, and what a reopened host recovers are
  asserted case for case by `test_harness_view_run.py`, `test_harness_live_deltas.py`, and
  `test_harness_deferred_progress.py`. The raw argument text a provider streams never reaches durable
  here at all, so there is no parser to port. `PARITY.md` records the whole of it as one
  **Allowed deviations** row beside the watcher-memory cost it leaves behind.
- **The deleted agent event adapter's own batching bound is not carried over.** `MAX_PENDING_EVENT_BATCHES`
  (100 undelivered batches before one snapshot replaced them) was the deleted adapter's private rule,
  owned by no seam the port keeps, and upstream v1.5.0 deletes it with the section, so no case is kept
  for the number itself. What a host could observe through it — that a consumer which falls behind
  converges on a single bounded delivery rather than promising every transition — is asserted on the
  windowed view in `test_harness_view.py`: `test_folds_undelivered_commits_into_one_call_whose_ops_reach_the_newest_view`
  (101 undelivered commits arrive as one call whose operations reach the newest view),
  `test_bounds_an_overflow_replacement_by_max`,
  `test_keeps_the_newest_min_of_a_commit_that_appends_more_than_max_at_once`, and
  `test_lets_a_long_lived_watch_drop_the_entries_that_left_its_window`. The same holds for the
  adapter's event vocabulary (`tool_execution_start`/`update`/`end`, `message_start`/`update`/`end`,
  `run_start`/`run_end`, `turn_start`/`turn_end`): it was the deleted module's rendering of committed
  state, and the state itself is asserted where it lives — `pi.live`, the entry window, the task graph
  and reports.
- **A window's dropped entries are proven by what a watch holds, not by a GC probe.** The upstream
  view case "lets a long-lived watch drop the entries that left its window" keeps a `WeakRef` to an
  entry that fell out of the window and asserts it is collected. CPython does not weak-reference a
  `list` or a `dict`, and collection is not observable in a test, so
  `test_harness_view.py::test_lets_a_long_lived_watch_drop_the_entries_that_left_its_window`
  asserts the same retention property from the watch's own value: the published window no longer
  holds the first entry and its `before` names it, which is what would otherwise pin the object.
- **The observer half of two window cases is translated.** Upstream `test/harness-view.test.ts`
  proves one window per attached consumer with both `watchConversation` and `observeConversation`:
  the case "gives each observer its own window over one mount" attaches its narrow consumer as an
  observer, and "rejects window bounds that are not integers or put `max` below `min`" asserts one
  `observeConversation(id, recorder, context, { max: 0 })` rejection. `ConversationViews.attach`
  already takes one window per attached consumer, and this audit translated the observer halves
  under the same names in `test_harness_view.py`: `test_gives_each_consumer_its_own_window_over_one_mount`
  now attaches its narrow consumer through `harness.observe_conversation` with a window beside the
  watch's, sharing the mount's documents, and
  `test_rejects_window_bounds_that_are_not_integers_or_put_max_below_min` asserts the
  `observe_conversation` rejection beside the watch's.
  - **#55 corrected the last sentence of the moved text.** #44 merged in `c5cfa8a` without those two
    assertions, and no ticket after owned them; #52 (this audit) translated them, named above.
    `ConversationViews.observe` takes a window (`src/pi_durable/harness/view.py:169`), so nothing
    blocked the translation.
- **#49 ends a cut-off task instead of parking it.** Upstream v1.5.0 has no parked task: a task cut
  off `settings.restart_limit` times in a row ends, each kind visibly (spec §5.4). What landed for it:
  `test_harness_blocked.py::test_faults_an_extension_task_cut_off_restart_limit_times_instead_of_running_it_again`
  is `test/harness-blocked.test.ts:136`, faulting an extension's task with the message
  `Task <id> (<kind>) was cut off 3 times without progress` and one `task_faulted` report of reason
  `restart_limit`; `tests/test_harness_restart_failure.py` translates all 18 cases of
  `test/harness-restart-failure.test.ts` -- nothing in that file is left untranslated. Six came with
  the built-in kinds: a tool call settled with an error result carrying the `restart_limit`
  diagnostic and a task failed with that detail, a compaction failed into `compactionFailure`, and a
  generation ending its run `unanswered` with `runFailure` reason `restart_limit`, each also reported
  `task_interrupted` with its `restarts`. The other 12 cover the charged attempt under `settings.retry`
  (kept partial, backoff, the run ending `interrupted` with every partial kept, retries disabled, a
  clean close that is not a crash and resends, a generation sleeping in a retry or waiting for its
  tools that is not charged), the crash-then-clean-close charging a request once, the limit ending a run
  that keeps being cut off while charging, the partial dropped past the limit where converting it may
  be what fails, an aborted run whose abort handler is cut off, and `task_interrupted` reporting. The
  answered case is
  `test_harness_after_run.py::test_ends_an_answered_run_completed_without_a_failure_once_finish_is_cut_off`,
  which ends `completed` with no failure and keeps what is queued. Two port notes this suite forced:
  `settle_scheduler_outcome` distinguishes a task cut off at the limit from one that merely faulted, as
  upstream `live.ts:421-447` does, so an abort handler cut off at the limit ends its run `aborted` with
  the limit as its detail rather than `faulted`, skips a cleanup that may be what cut the task off, and
  fails a cut-off compaction into no failure at all; and `_cancel_deferred` reports a *failure* but
  re-raises `asyncio.CancelledError` (`generation.ts:589-596`), because aborting in Node is not
  cancelling -- vitest abandons a handler that never settles, while asyncio must be allowed to cancel
  one or the task can never end. The compaction half of that distinction had **no coverage at all**:
  upstream's file never faults a compaction for the limit, so deleting the `cutOff` branch from
  `settle_scheduler_outcome` left the whole suite green. `test_harness_restart_failure.py::test_removes_a_cut_off_compaction_without_failing_it`
  is therefore **port-authored, not translated** -- it is the port's own case for `live.ts:421-447`, and
  disabling that branch reddens it on the `compactionFailure` assertion. It is the nineteenth case in a
  file whose other eighteen are upstream's, and it is not counted as upstream coverage. Deleted with
  it: the parked state, its `restart_limit` blocked reason of `BlockedReason` and `TaskInspection`,
  `TaskScheduler.retry`, `Harness.retry_task`, and the wake exclusion, all of which 1.5.0 removed
  (`Harness.retryTask()`, the `restart_limit` blocked reason, and the `task_parked` report are gone
  upstream). The `task_parked` kind #34 deleted stays deleted; `task_interrupted` and `task_faulted`
  carry the crash-loop signal in its place.
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
  #44 implemented for real, so `ended` was reachable for an observed owner that ended before #54 made
  it reachable for a watch.
- **A retired value's garbage collection is measured through the cache.** Upstream's "keeps no value
  of a document retired for good, while still attached" keeps only a `WeakRef` of the value and
  expects the collector to take it. CPython cannot take a weak reference to the plain `dict` a
  document value is, so the same guarantee is measured where the Session itself holds the value:
  `document_cache_sizes` empty in
  `test_session_document_retention.py::test_keeps_no_value_of_a_document_retired_for_good_while_an_observer_is_attached`.
  Where an object *can* be weakly held — the host's own observer — the upstream collection case is
  translated directly: `test_keeps_no_detached_observer_alive`.
- **The `beforeProviderRequest` chain declines a returned `payload` of `None` (#59).** Upstream's
  replacement chain takes the replacement on any defined result -- `harness/generation.ts:677-678`
  at tag v1.5.0, `if (replaced !== undefined) current = replaced.payload` inside `providerPayload`
  (`:668-681`) -- so a handler that returns a mapping whose `payload` is `undefined` (the Python
  spelling, `None`) replaces the body with it. The port's `stream_response` replaces only when the
  returned mapping carries a non-`None` `payload` (`harness/generation.py`'s
  `before_provider_request` closure, `:693-699`), so the same handler declines here and the body is
  sent as built. No spec clause pins the return value -- the composition table at
  `spec-1.5.0.md:3351` defines only the replacement chain, with a handler that returns nothing
  leaving the payload alone, and the `GenerationHooks` declaration at `:3394-3398` types the
  result's `payload` as `unknown` -- and no upstream case drives it, so the port keeps its stricter
  rule; the audit records the edge here rather than change the behaviour.

## What moved here from the 1.4.0 record

#55 restored [`parity-report.md`](./parity-report.md) to the bytes the v1.4.0 tag closed with and
moved every 1.5.0 claim out of it. Everything above except the four sections that frame it — this
file's header, **Status of this audit**, **What this baseline changes in the 1.4.0 record**, and
**What moved here** — is that moved content, with three corrections made where the moved text was
itself stale, each verified against the tree or the vendored upstream file:

- the `session-states` row above points at **Remaining deviations**, which is where its itemisation
  lives in this file; the moved text pointed at the 1.4.0 record's "Cases completed in this audit",
  which never held it.
- the observer-window bullet's "#44 adds the observer assertions" sentence is corrected in place,
  with the evidence, rather than carried on crediting a merged ticket; the `harness-view-run` entry's
  case counts are corrected against the vendored file, as that entry records.
- the benchmark-helper and telemetry sentences, which the sync had grafted onto two existing bullets
  of the 1.4.0 record, stand as their own entries here so the 1.4.0 record keeps its own wording.

Every case name this file cites was checked to exist in the file that cites it, and `EXCLUDED_WATCH_CASES`
and the port's `view_state` getter were checked to be gone. Nothing here is a claim this ticket could
not read off the tree.

## What this audit found and fixed

The audit re-derived the record from the tree and found the record and the code each out of step in
seven places. Each fix below is in this commit, and the case that pins one is named where there is
one to name.

- **A dispatch taken for an observer attached during its own publication.**
  `Session._publish` ran the commit's own followers -- its watches, its document observers, and its
  internal commit listeners -- before it checked whether the host was attached, so an `observe_commits`
  observer attached by one of this commit's own followers (a hook, an `on_wake`, an observer callback
  reacting to the commit) would have received the very commit it attached for: the check saw the
  attachment the callback had just made. The spec §9.1 text is that the dispatch is taken only when
  an observer is attached **at the publication**, and upstream takes the decision before the own
  listeners run -- `session.ts`'s `#publish` asks `#commitListeners.size > 0` (line 1366) ahead of its
  own-listeners loop (line 1376), and reads the set at delivery (line 1403). This audit read the set
  before the own followers run in `src/pi_durable/session/session.py`'s `_publish` --
  `host_attached = bool(self._commit_observers)` ahead of `_advance_followers`, the dispatch behind
  it -- and the case that proves it is the translated upstream one, the "none for a commit published
  without one" half of `test_calls_the_host_listeners_registered_when_its_dispatch_runs`
  (`tests/test_session_adoption.py`, upstream `:153`): its own listener attaches the host observer
  during the publication, and the commit must not reach it. This audit ran that half against the
  pre-fix order and it fails there -- the observer receives the commit it attached for -- and passes
  on the post-fix order. The port's host dispatch reads the set at delivery, as upstream's does,
  so an observer a preceding advance detached takes nothing of the commit it was attached for.
- **Stale upstream citations.** Four docstring lines in `tests/test_harness_run_failure.py` cited
  the lines the clauses held at v1.4.0 -- `test/harness-run-failure.test.ts:34/:63/:101/:145` --
  where the vendored v1.5.0 file holds the cited `it()` clauses at `:38/:64/:100/:139`, and one
  line in `tests/test_harness_blocked.py` cited `test/harness-blocked.test.ts:136` where the
  v1.5.0 file holds the clause at `:137`. The cited text is identical on both sides; the lines
  were off by the shift the v1.5.0 file took over v1.4.0, and each was corrected to the line the
  vendored file holds the clause at.
- **Stale module claims.** Three module docstrings stated a deferral or a seam that the merged
  tickets had already resolved: `tests/test_harness_continue.py` claimed its host-died case's crash
  charge "cannot run as-is" when #38 had landed the crash charge on the port's `kill()` seam —
  the case is now named as a narrower translation, with the crash charge's real seam
  (`test_harness_restart_failure.py` over `kill()`); `tests/test_harness_view.py` claimed its two
  observer-side cases stayed for #44 — #52 translated them (above); `tests/test_session_observers.py`
  claimed its deferred-commit case waited on #47 — #47 landed, and the case is
  `test_session_deferred_commits.py::test_show_every_held_commit_to_watches_and_the_sessions_own_observers_once`
  beside `test_give_observe_commits_exactly_what_was_stored_and_nothing_for_a_held_commit`.
- **A stale public-surface sentence.** The `src/pi_durable/__init__.py` module docstring still named
  the agent event stream, which #46 deleted with `test_harness_events.py`; the sentence now names
  the three watch and observer families it hands out.
- **A port class that lacked the upstream class's bounds.** `ConversationTails` carried the
  upstream `session-conversation-tails.ts` class's behaviour but not its `size` and `markers`
  properties, which the translated cases assert through. Added in
  `src/pi_durable/session/conversation_tails.py`, both reading the live state — the frozen-marker
  case asserts `size` grows while `markers` is pinned, and a property that answered from a snapshot
  would not prove the pin.
- **The record-cache suites and the un-owned suites.** `session-cached-storage`,
  `harness-partial-changes`, and `harness-registry-reader` reached this baseline with no
  translation, no named deferral, and no `PARITY.md` row; `session-adoption` and
  `session-conversation-tails` the same. The two are translated (above); the three are excluded and
  their cases named, each with what stands in its place in the port (**Exclusions this baseline
  takes**, **Cases deliberately left untranslated**).
- **A baseline claim the tree no longer held.** ADR 0003 says 1.5.0 "touches none" of the ADR 0002
  exclusions, and `PARITY.md` still carried the `test-workers/*` exclusion row; the v1.5.0 tree no
  longer holds the `test-workers` directory, so 1.5.0 touched one of the excluded things — by
  deleting it. The exclusion itself stands on the backend (ADR 0003 now says so); this audit
  corrected the ADR sentence and deleted the `PARITY.md` row, and the deletion is recorded under
  **What this baseline changes in the 1.4.0 record**.

## Post-sign-off corrections

The two-axis review of this close-out (standards and spec, each against the tree and the vendored
upstream) found the record out of step in four places after the sign-off above. Each correction is
in the commit that follows the sign-off, and none of the four changes code behaviour:

- the **Stale upstream citations** bullet of **What this audit found and fixed** is rewritten with
  the old and new positions in the order the diff holds them and the suite files it corrected --
  `test/harness-run-failure.test.ts` and `test/harness-blocked.test.ts` -- in place of the
  `scheduler.ts` the bullet named.
- the `search` row of the suites table and `tests/test_tool_search.py`'s module docstring now name
  the eight Unicode cases left untranslated, each with its upstream line.
- the **Gates** free-threaded note now records the disposition the #52 ruling gives the legs --
  #53's release verification, not this sync's green column, an approved exception to the umbrella
  Definition of done -- rather than leaving them an open question.
- **Remaining deviations under this baseline** now records the `beforeProviderRequest` payload
  edge the spec review found -- a handler's `{"payload": None}` replaces the body upstream and
  declines in the port, pinned by no spec clause and no upstream case -- with the port's behaviour
  left as-is.

## Gates

Run at this commit, after the fixes above:

- `ruff check src tests` — clean.
- `mypy` — clean: `Success: no issues found in 201 source files`, the project config on
  `--python-version 3.14 --warn-unused-ignores`, so every `# type: ignore` in the tree is one
  mypy fires on.
- `PYTHONPATH=src pytest -q -o addopts=""` — **1748 passed, 2 skipped** on the interpreter this
  audit ran on (3.15, the newest of the verified set): the 1728 the re-baseline closed with, plus the
  20 cases this audit translated and added (13 adoption, 5 conversation-tails, the hook case of
  `harness-hook-reads`, the `on_wake` case of `harness-sleep`). The two skips are the live-credential
  cases the v1.4.0 record names (`test_provider_session_cache_e2e.py`, the live half of
  `test_system_order_cache_e2e.py`), skipped by design.
- The free-threaded legs, 3.14t and 3.15t, are **not installed in this environment**: the gate is
  reported on the installed legs only, and no free-threaded pass is claimed by this sign-off.
  Their disposition is the ruling recorded on #52: the legs belong to #53's release verification,
  not to this sync's green column -- an approved exception to the umbrella Definition of done, not
  an open defect.

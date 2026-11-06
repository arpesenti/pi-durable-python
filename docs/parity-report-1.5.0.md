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

Not signed off. The v1.5.0 audit is [#52](https://github.com/arpesenti/pi-durable-python/issues/52),
and it is the ticket that turns the claims below into a signed-off result: the in-scope suite count
for this baseline, the per-suite translation table, and the pass condition every `tested` row in
`PARITY.md` rests on. Until then every completion claim here is what the porting ticket observed at
its own commit, not an audit finding, and a claim that a case is deliberately left out is read here
as a claim awaiting that sign-off.

The v1.4.0 record's Result — all 61 in-scope suites translated — is that baseline's number. This
baseline deletes one of them upstream (`harness-events`) and adds the suites named below, so the
v1.5.0 count is #52's to state, not this file's to guess.

`PARITY.md` names these as the suites this baseline adds, and each is a real upstream
`test/*.test.ts` at tag `v1.5.0`: `harness-group-commit`, `session-group-commit`,
`session-deferred-commits`, `harness-deferred-progress`, `harness-observers`, `session-observers`,
`conversation-transcript`, `harness-run-failure`, `harness-turn-limit`, `harness-after-run`,
`harness-continue`, `harness-submit-in-commit`, `harness-idle-scopes`, and `harness-restart-failure`.
This file already carries the rows for `harness-observers`, `session-observers`,
`conversation-transcript`, and `harness-view-run`; the rest are #52's table to fill in, suite by
suite, with the case it deliberately leaves untranslated named and reasoned here when it does.

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

## The suites this baseline adds or remaps

The 1.4.0 record's [suite table](./parity-report.md#suite-translation) stands for every suite this
baseline left alone. These are the rows this baseline adds or moves.

| Upstream suite | Python translation |
| --- | --- |
| `harness-observers` | `test_harness_observers.py` |
| `harness-hook-reads` | `test_harness_hook_reads.py` (the hook half of the upstream file is deferred; see **Cases completed under this baseline**) |
| `harness-view-run` | `test_harness_view_run.py`, `test_harness_nested_tools.py`, `test_harness_compaction.py`, `test_session_document_retention.py` |
| `harness-nested-tools` | `test_harness_nested_tools.py` (the agent-event half went with `test_harness_events.py`, deleted by #46) |
| `session-observers` | `test_session_observers.py` |
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
  behind that commit, and it settles afterwards with that entry in the history it returns. The file's
  other upstream case, "reads raw history and single entries from a hook" (`:17-48`), is deferred to
  #36 and #48, which own `HookApi.entry()`/`entries()`: every assertion of it runs through a hook
  `api` the port does not carry, and the module docstring names it.

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
  runs all 11 cases of `test/harness-deferred-progress.test.ts`, plus one port-side case that pins the
  negative the mechanism rests on: the partial answer and the streaming
  tool output visible to watches while Storage is held, converted to durable by a crash, by a clean
  close, or by their deadline, with the progress commits of a cut-off task never counted as progress
  (`#11139`) and a third party reading the held answer inside its own commit. The host's
  `CommitObserver` is asserted there to receive nothing at all for a partial only memory adopted --
  the assertion reads the published values, not the operation paths, because a held revision can reach
  Storage folded into another commit's base write.
  In `tests/test_session_group_commit.py`, the eleven cases that had waited on this mechanism (`:325`,
  `:360`, `:390`, `:596`, `:615`, `:662`, `:680`, `:839`, `:857`, `:885`, `:902`) are translated into
  its table, and an entry that named no upstream case at all is gone from that module's deferral block;
  the record-cache case (`:635`) stays the one exclusion it records there.

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

## Remaining deviations under this baseline

These are deliberate and are also recorded under **Allowed deviations** in `PARITY.md`.

- **The streamed-partials half of `harness-view-run` waits for #47 and #48.** Upstream v1.5.0 adds
  `test/harness-view-run.test.ts` — its one describe, "a run as the conversation view shows it", 13
  cases that read run state (`pi.live`, `pi.inbox`, streamed partials, tool slots) out of the windowed
  view's operations. #46 translated the run cases there — they are where the assertions the deleted
  agent event adapter carried now ride — in `test_harness_view_run.py`, and the nested-call and
  compaction cases of that file in `test_harness_nested_tools.py`, `test_harness_compaction.py`, and
  `test_session_document_retention.py`. What remains is the streamed-partials tail of the file —
  `:505` "streams a tool-call partial as appends at its argument paths", `:547` "commits a growing
  text and tool call at a cost that follows the new text, not the string lengths", and `:581` "keeps a
  watcher's memory proportional to a long streamed text and tool call, not to their commits" — which
  belongs to #47 and #48. Two of the translated cases also read `runFailure` in their assertions
  (`:372` and `:436`); the port asserts everything beside them, because it carries no run-failure
  state until #48 lands it.
  - **#55 corrected the counts as it moved this entry.** The moved text said "14 cases", a second
    "streamed partials" describe, and "two upstream cases that read `runFailure`". The vendored
    upstream file has one describe and 13 `it()` cases, the three quoted above are its
    streamed-partials tail, and the two `runFailure` reads are assertions inside the two cases #46
    did translate.
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
- **The observer half of two window cases has no owner.** Upstream `test/harness-view.test.ts`
  proves one window per attached consumer with both `watchConversation` and `observeConversation`:
  the case "gives each observer its own window over one mount" attaches its narrow consumer as an
  observer, and "rejects window bounds that are not integers or put `max` below `min`" asserts one
  `observeConversation(id, recorder, context, { max: 0 })` rejection. `ConversationViews.attach`
  already takes one window per attached consumer — watches are the consumers that exist today — so
  `test_harness_view.py` translates both through `watch(context, window)` alone, and #44 (`Observers
  attach on the line`) adds the observer assertions under the same names.
  - **#55 corrects the last sentence.** #44 merged in `c5cfa8a` without those two assertions:
    `tests/test_harness_observers.py` holds neither case, and the only window-bound and
    own-window cases in the tree are the watch halves in `tests/test_harness_view.py:653` and
    `tests/test_harness_view.py:676`. `ConversationViews.observe` does take a window
    (`src/pi_durable/harness/view.py:169`), so nothing blocks translating them; no open ticket holds
    them, and naming their owner is #52's.
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

# Upstream 1.5.0 scope decisions

**Status:** accepted

The port follows `@earendil-works/pi-durable` in lockstep, so the 1.5.0 sync re-baselines it on tag
`v1.5.0` (commit `f9a26365568127461c47a07a37dcb42c140a3ac5`, retrieved 2026-11-05) and vendors
[`docs/upstream/spec-1.5.0.md`](../upstream/spec-1.5.0.md) together with
[`docs/upstream/group-commit-1.5.0.md`](../upstream/group-commit-1.5.0.md), the design behind
specification section 4. The 1.4.0 and 1.3.0 vendored baselines stay where they are, as the audit
record of those releases.

The delta is ported in full, including its breaking changes: group commit on the mutation line
(spec §4 and the vendored group-commit document) and deferred commits (§4.1); the batched
`Storage.commit()` with its per-batch checks, receipts and refusal, the optional `publishing()`
scope, and the narrowed `mintId()` monotonicity; synchronous `Observer`s replacing the Chord state
getters and the commit subscription; watches that take Chord's tagged `WatchCall` and `WatchEnd` and
deliver coalesced changes; the conversation view as a bounded entry window with
`ConversationTranscript`; `pi.live` at version 3 with `runFailure`, `run.turns`, `run.first`, and
`run.afterRun`; the `kind`-tagged report, session end, file change, task state, and document commit
change; run-end tasks, continue submissions, `whenIdle: "reject"`, per-conversation turn limits, and
`submit(tx, draft)` inside a commit; the restart-cutoff task ending instead of parking; the model
request that was in flight when the host died charged as a failed attempt; and tool parameters as
plain JSON Schema. The SQLite schema version does not move: the persisted change is the live
document's version, and a database the previous release of this port wrote still opens and migrates
in place.

Four things stay out of scope.

## Excluded

- **The attached-host storage subpath.** `StoreStorage` (`./storage/store`) keeps a Session's records
  as declared tables of a Chord v2 store and its documents in that store's document engine, takes its
  ids from the store's one allocator, stores each `commit()` call in one store transaction, and
  publishes each commit inside the batch of the host the store serves. Each of those is a Chord host
  this port does not have: Chord is an internal implementation detail here (ADR 0001, and the `Chord`
  row of `PARITY.md`'s deviations), there is no Python store to attach to, and what the backend
  actually delegates — `ChordStore.commitSeries` and the store's outbox — is Chord's own contract.
  Porting it would port a host rather than a backend, and its suite would test an adapter that could
  never be attached. `excluded`. The optional `publishing(publish, context)` scope it exists to serve
  is a `Storage` protocol member and stays: it is the seam through which a host that owns a batch runs
  each commit's publication, and the port's own backends are tested against its absence.
- **Any pi-schema builder analogue.** 1.5.0 types `ToolRegistration.parameters` and
  `structuredOutputSchema` with `@earendil-works/pi-schema`, whose builders (`obj()`, `num()`,
  `{ open: true }`) are a DSL for the compile-time `Static<T>` type of the parsed arguments. Python
  has neither the compile-time type nor the need: a tool's parameters are any JSON Schema object,
  which this port already accepts as a plain mapping validated by the `schema` extra's validator
  (§7.3). The behaviour the builders encode is ported — object schemas closed unless explicitly
  opened, an unresolvable reference refused where the schema is compiled, and the validator's wording
  in the `invalid_structured_output` diagnostic — the builder DSL is not. `excluded`; parameters stay
  plain JSON Schema, and `PARITY.md` records what the port substitutes for upstream's validator.
- **The telemetry host-callback member.** `HostCallback` gains `"telemetry"`, the subject of a
  `host_error` report about a telemetry adapter that threw while a commit was published. Telemetry is
  excluded (ADR 0002) and this port has no adapter that could throw, so the member names a
  host-callback the surface cannot carry; a Python `HostCallback` stays exactly the exhaustive set of
  seams that exist here (`now`, `settings`, `registry`, `env`, `models`, `setTimer`, `onWake`).
  `excluded`, with its report kind.
- **Everything ADR 0002 already excluded.** Telemetry and `DURABLE_TELEMETRY_SCHEMA`, the pi-ai
  client identity (`HarnessOptions.identity`, `DEFAULT_HARNESS_IDENTITY`), the IndexedDB backend and
  the Cloudflare Workers suites (`test-workers/*`), and Code Mode with `codemode-checkpoints` stay
  excluded, unchanged, for the reasons recorded there; Windows and WSL stay out with ADR 0001. 1.5.0
  touches none of them: the `tool_search` ranking fix is ported as behaviour, and Code Mode's own
  fixes are not.

## Consequences

- `PARITY.md` names this baseline in its header and links both vendored documents. Every 1.5.0
  export and every specification section the release changed or added has a row there, and each of
  those rows is `planned` until the ticket that ports it lands with its translated suite; a row whose
  section 1.5.0 left alone keeps its `tested`.
- The four exclusions are visible in the surface rather than merely listed: `./storage/store` appears
  only under Excluded subpaths, a tool's `parameters` is a JSON Schema mapping with no builder to
  call, no host-callback name is `telemetry`, and `HarnessOptions` carries neither `telemetry` nor
  `identity`.
- The watch/observer split is a ported distinction, not a Python convenience: a watch renders state
  and its listener may await, an observer mirrors state synchronously on the mutation line and may
  not.
- Nothing upstream-deleted survives as a Python-side extension: the agent event stream goes with #46,
  the Chord state getters and the commit subscription with #44, the single-write commit form with
  #41, and the parked task state with #49 — each with its matrix rows and glossary entries.
- [ADR 0001](./0001-parity-baseline-and-boundary.md) keeps its boundary, its public-surface rule and
  its naming conventions; this record supersedes it in part only where 0001 pinned the baseline.

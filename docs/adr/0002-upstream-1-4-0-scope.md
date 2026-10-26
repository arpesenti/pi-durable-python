# Upstream 1.4.0 scope decisions

**Status:** accepted

The port follows `@earendil-works/pi-durable` in lockstep, so the 1.4.0 sync re-baselines it on tag
`v1.4.0` (commit `4a080252f06caf63a525d7873c5d94d3c23887e0`) and vendors
[`docs/upstream/spec-1.4.0.md`](../upstream/spec-1.4.0.md). The durable delta is ported in full,
including its breaking changes: durable sleeps and `sleep_until()` replacing `runtime.sleep()`, the
`restart_limit` crash-loop guard with parked tasks, typed reports, `after_failed_run`, per-model
compaction overrides, host timers and wakeups, the required `checkpoint_when` predicate, and the
SQLite schema-version-2 rebuild. Four upstream additions stay out of scope.

## Excluded

- **Telemetry.** `HarnessOptions.telemetry` and `DURABLE_TELEMETRY_SCHEMA` are host observability
  tied to `@earendil-works/pi-telemetry`. The 1.4.0 specification has no normative section for
  them: the adapter records spans, events and metrics that the spec only mentions in passing. A
  Python port would have to invent a tracing dependency and a span model, and the content it
  carries (submission admission, task phases, nested-tool replay) is already observable through
  `inspect()`, the task graph, the conversation view, and the agent event stream. Translating
  `harness-telemetry` and `harness-telemetry-options` would therefore test an invented contract.
  Both exports are `excluded` in `PARITY.md`, `harness-telemetry` is not translated, and a host
  that wants spans wraps `on_report`, the scheduler's live state, and the event stream.
- **pi-ai client identity.** `HarnessOptions.identity` and `DEFAULT_HARNESS_IDENTITY` configure how
  pi-ai identifies the app to providers (`User-Agent`, attribution headers). Python has no pi-ai
  and no shared `Models` wrapper: the optional OpenAI and Anthropic adapters each build their own
  requests and keep their own attribution headers. The specification has no section for the setting,
  and `api.models` in this port is already the host's own object rather than an identity-adding
  wrapper. `excluded`.
- **IndexedDB and Workers backends.** `IndexedDbStorage` (`./storage/indexeddb`) is a browser storage
  backend, and `test-workers/*` covers Cloudflare Workers: both are out of scope for the same reason
  the Cloudflare SQLite adapters already are — no Python runtime for them, and a conformance suite
  that cannot run in CI. `excluded`.
- **Code Mode.** Already excluded at 1.3.0: `codemode-checkpoints` and every codemode delta stay out.
  Code Mode needs a Python execution-sandbox design of its own; the `pi.tool` nested-call surface it
  builds on is ported and tested.

Windows and WSL remain out of scope, as recorded in
[ADR 0001](./0001-parity-baseline-and-boundary.md).

## Consequences

- `PARITY.md` stays the contract: every new 1.4.0 export and spec section has a row, and the four
  exclusions above carry `excluded` with their reason.
- The telemetry and identity exclusions are visible in the port's surface: there is no
  `HarnessOptions.telemetry` and no `HarnessOptions.identity`, and `Settings` carries no
  observability fields. A host that needs either wraps the port's own seams.
- The SQLite schema version bump is *not* excluded: opening a 1.3.0 database migrates it to version
  2, and a newer version is refused, both covered by `test_sqlite_migrations.py`. A port database
  cannot be rolled back to 1.3.0, exactly as upstream documents.
- Telemetry tests stay excluded rather than translated, so the translated-suite count in
  `docs/parity-report.md` names them explicitly.

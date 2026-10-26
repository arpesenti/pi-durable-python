# pi-durable-python 1.4.0

A mechanical Python translation of
[`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/v1.4.0/packages/durable)
**v1.4.0** (tag `v1.4.0`, commit `4a080252f06caf63a525d7873c5d94d3c23887e0`).

```console
pip install pi-durable-python
pip install "pi-durable-python[openai]"    # or [anthropic], [schema], [all]
```

## What's new

- **Durable sleeps, timers, and wakeups.** A task can end a phase asleep with
  `runtime.sleep_until(until, checkpoint, context)` and holds no invocation until the Harness clock
  reaches the deadline; `Harness.wake()` reports `active` and the earliest `at`, and
  `HarnessOptions.on_wake` lets a host arm its own wakeup (a Durable Object alarm, say) and call
  `resume()`. Every other timer goes through `HarnessOptions.set_timer`, split into foreground
  timers that pace or resume work and background ones that only free memory. Generation retry
  backoffs, deferred polls, and compaction retries are now durable sleeps rather than waits inside a
  running phase.
- **Crash-loop guard.** Open counts a task a crash cut off (`TaskRecord.restarts`); at
  `settings.restart_limit` (default `5`, `math.inf` never parks) the task is **parked**: blocked
  with reason `restart_limit`, reported once, not active in `wake()`, and run again by
  `Harness.retry_task()`. Committing a new state is progress and clears the count; a close writes
  the tasks it stopped back as `pending` uncounted.
- **Typed reports.** `on_report(error, info)` now receives a `ReportInfo` saying what happened:
  `session_failed`, `task_faulted`, `task_parked`, `migration_failed`, `task_outdated`,
  `task_error`, `extension_error`, `host_error`, or `listener_error`, with the task and the
  extension whose code failed where known.
- **Host inputs and settings as functions.** `HarnessOptions.models` and `settings` accept a value or
  a zero-argument function resolved at each use, so a host can replace its model catalog or run
  settings without reopening the Harness. `settings.after_failed_run` chooses what a failed run does
  with the inputs queued behind it (`keep`, `continue`, `withdraw`), and
  `settings.compaction.model_overrides` sets compaction thresholds per `"provider/modelId"`.
  Tools and hooks get `ToolExecutionApi.now()`, the Harness clock.
- **Documents and tools.** `checkpointWhen`/`checkpoint_when` is required on every document
  definition, with no default; `ToolControl.terminate` is a boolean whose `false` is a no-op.
- **Storage.** SQLite schema version 2 rebuilds the task table to admit the `sleeping` status, and a
  database migrated by a newer version is refused.

## Upgrading from 1.3.0

- **`TaskRuntime.sleep()` is gone.** Move the work after a wait into the next phase and carry what
  it needs in the checkpoint: the old call waited inside a phase, which holds an invocation and
  reruns the phase after a restart, restarting the wait.
- **`sleeping` is a task status.** Code that handles every `TaskState`, `TaskGraphState`, or
  `TaskInspection` state needs a case for it.
- **A SQLite database opened by 1.4.0 cannot be opened by 1.3.0.** The migration runs automatically;
  there is no rollback past this release. JSONL needs no migration, but a 1.3.0 process never
  resumes a sleeping task it finds.
- **The crash-loop guard changes restart behavior.** A task cut off five times in a row without
  committing progress is parked instead of run again; set `restart_limit` to `math.inf` for the old
  behavior.
- **Every definition needs `checkpoint_when`.** Use `lambda value, ops, info: False` to keep only
  the required bases, or a policy that periodically writes one.
- **Fakes need the new members.** A hand-built `TaskRuntime` or `ToolExecutionApi` must add
  `sleep_until`/`set_timer` and `now()`; spreading the original still works.
- `on_report` gains a second argument and `runtime.report()` an optional `from`. A callback written
  for one argument keeps working.

## What it is

Durable conversations, tasks, and documents for agents on `asyncio`. Every effect is committed
before it happens, so a restart resumes a run, a tool round, a compaction, or an inbox placement
from storage instead of from memory.

- **Session kernel.** One writer, explicit `commit`, typed documents with versions, migrations,
  histories, and forks, plus snapshots, document states, and exact-frame watches.
- **Harness.** Conversations, submissions and the inbox, the task scheduler, agents and registry
  resolution, extensions, hooks, tools, generation, compaction, usage, the conversation view, the
  agent event stream (experimental), and the task graph.
- **Tasks.** Durable phased state machines with checkpoint re-entry, ownership, background work,
  waits, sleeps, and abort settlement.
- **Storage.** Memory (reference), SQLite, and JSONL backends, each held to the shipped
  `pi_durable.testing` conformance suites.
- **Providers.** OpenAI Chat Completions and Anthropic Messages adapters as independent extras,
  implementing the core `Models` protocol over an injected or lazily built SDK client.
- **Environment and tools.** A POSIX `ExecutionEnv`, output truncation and line scanning, the
  `read`/`write`/`edit`/`bash` coding tools, and BM25 deferred-tool discovery.

The core package has no runtime dependencies. Extras are `openai`, `anthropic`, `schema`, `all`, and
`dev`.

## Parity

[`PARITY.md`](https://github.com/arpesenti/pi-durable-python/blob/main/PARITY.md) has one row per
v1.4.0 public export and specification section; [`docs/parity-report.md`](https://github.com/arpesenti/pi-durable-python/blob/main/docs/parity-report.md)
records how all 61 in-scope upstream test suites were translated and which deviations remain.

- Python names follow PEP 8 (`max_tokens`, `when_busy`, `model_id`, `restart_limit`); classes stay
  PascalCase and persisted JSON keeps camelCase.
- Excluded, as [ADR 0001](https://github.com/arpesenti/pi-durable-python/blob/main/docs/adr/0001-parity-baseline-and-boundary.md)
  and [ADR 0002](https://github.com/arpesenti/pi-durable-python/blob/main/docs/adr/0002-upstream-1-4-0-scope.md)
  record: Code Mode, the image implementation (the `ImageProcessor` protocol remains), Cloudflare,
  IndexedDB, and Workers backends, telemetry, pi-ai client identity, PowerShell, Windows support, and
  benchmark helpers.
- Cross-language storage interoperability is incidental, not a contract.

## Runtime

CPython 3.11–3.15 on Linux and macOS, verified on every version plus the 3.14t/3.15t free-threaded
builds. The suite passes with the GIL disabled, so the code does not depend on it; nothing claims
thread safety, and a `Session` or `Storage` is not shared between threads.

## Getting started

The [README](https://github.com/arpesenti/pi-durable-python/blob/main/README.md) has installable
quick starts, and [`docs/usage.md`](https://github.com/arpesenti/pi-durable-python/blob/main/docs/usage.md)
walks through documents, tasks, the Harness, providers, storage, tools, and testing, including the
new sections on sleeping, restarts and parking, timers and wakeups, typed reports, and
`after_failed_run`. Examples marked `<!-- example -->` in those files run in CI.

## License

MIT, as is the vendored v1.4.0 specification, copyright (c) 2025 Mario Zechner.

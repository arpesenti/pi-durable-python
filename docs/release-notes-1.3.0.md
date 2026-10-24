# pi-durable-python 1.3.0

The first release of the rewrite: a mechanical Python translation of
[`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/v1.3.0/packages/durable)
**v1.3.0** (tag `v1.3.0`, commit `71023aec1546d6d677a65669c5665c0927ce0dd0`).

```console
pip install pi-durable-python
pip install "pi-durable-python[openai]"    # or [anthropic], [schema], [all]
```

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
  waits, and abort settlement.
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
v1.3.0 public export and specification section; [`docs/parity-report.md`](https://github.com/arpesenti/pi-durable-python/blob/main/docs/parity-report.md)
records how all 56 in-scope upstream test suites were translated and which deviations remain.

- Python names follow PEP 8 (`max_tokens`, `when_busy`, `model_id`); classes stay PascalCase and
  persisted JSON keeps camelCase.
- Excluded, as [ADR 0001](https://github.com/arpesenti/pi-durable-python/blob/main/docs/adr/0001-parity-baseline-and-boundary.md)
  records: Code Mode, the image implementation (the `ImageProcessor` protocol remains), Cloudflare
  adapters, PowerShell, Windows support, and benchmark helpers.
- Cross-language storage interoperability is incidental, not a contract.

## Runtime

CPython 3.11–3.15 on Linux and macOS, verified on every version plus the 3.14t/3.15t free-threaded
builds. The suite passes with the GIL disabled, so the code does not depend on it; nothing claims
thread safety, and a `Session` or `Storage` is not shared between threads.

## Getting started

The [README](https://github.com/arpesenti/pi-durable-python/blob/main/README.md) has installable
quick starts, and [`docs/usage.md`](https://github.com/arpesenti/pi-durable-python/blob/main/docs/usage.md)
walks through documents, tasks, the Harness, providers, storage, tools, and testing. Examples marked
`<!-- example -->` in those files run in CI.

## License

MIT, as is the vendored v1.3.0 specification, copyright (c) 2025 Mario Zechner.

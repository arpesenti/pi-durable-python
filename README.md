# pi-durable-python

A Python port of [`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/v1.3.0/packages/durable)
**v1.3.0**: durable conversations, tasks, and documents for agents, on `asyncio`.

The process can die at any point. A run, a tool round, a compaction, or an inbox placement only
happens after the state it depends on is committed, so a restart resumes from what storage holds
rather than from what memory held.

- **Durable by construction.** Every effect is sandwiched between committed state: tasks re-enter
  their checkpoint, submissions survive a crash, documents version as they change.
- **Small core.** The base install has no runtime dependencies, and the memory, SQLite, and JSONL
  storage backends all ship in it. Model adapters and JSON Schema validation are separate extras.
- **Composable.** A `Session` is the kernel; the `Harness` adds conversations, submissions,
  scheduling, compaction, extensions, and hooks on top of it.
- **Parity, not resemblance.** This is a mechanical translation of the v1.3.0 source held to its
  semantics and persisted record shapes. [`PARITY.md`](./PARITY.md) is the contract.

## Requirements

- **Python.** CPython 3.11 or newer. The verified set is 3.11, 3.12, 3.13, 3.14, and 3.15 on Linux
  and macOS, plus the 3.14t and 3.15t free-threaded builds; the classifiers list the plain CPython
  versions 3.11 through 3.15.
- **Free-threaded parity.** The suite passes on builds with the GIL disabled, so the code does not
  depend on it. Nothing claims thread safety: do not share one `Session` or `Storage` between
  threads. Concurrent `asyncio` tasks are the supported concurrency.
- **Dependencies.** The core package has no runtime dependencies. The `openai` and `anthropic`
  extras carry their SDKs, and `schema` carries `jsonschema`.

## Install

```console
pip install pi-durable-python
```

Extras are independent; install only the ones you use:

| Extra | Adds | Install |
| --- | --- | --- |
| `openai` | OpenAI Chat Completions adapter (`AsyncOpenAI`) | `pip install "pi-durable-python[openai]"` |
| `anthropic` | Anthropic Messages adapter (`AsyncAnthropic`) | `pip install "pi-durable-python[anthropic]"` |
| `schema` | JSON Schema validation for structured output and tool arguments | `pip install "pi-durable-python[schema]"` |
| `all` | Everything in `openai`, `anthropic`, and `schema` | `pip install "pi-durable-python[all]"` |
| `dev` | pytest, pytest-asyncio, ruff, and mypy | `pip install "pi-durable-python[dev]"` |

From a checkout, `uv sync --extra dev` or
`python -m pip install -e ".[dev,openai,anthropic,schema]"` matches CI.

## Quick start

### A durable session and a document

A `Session` is the only writer: every change goes through `commit`, and readers see committed
state. Documents are typed JSON records, defined once and used as tokens.

<!-- example -->
```python
import asyncio

from pi_durable import MemoryStorage, create_session, define_doc
from pi_durable.context import BACKGROUND_CONTEXT

# Define once: the kind, version, scope, and initial value migrate and materialize the document.
CounterDoc = define_doc(
    {"kind": "demo.counter", "version": 1, "scope": "session", "initial": lambda: {"count": 0}}
)


async def main() -> None:
    session = create_session(MemoryStorage())

    async def bump(tx):
        counter = await tx.doc(CounterDoc)  # a draft, materialized only if the commit lands
        counter.count += 1

    await session.commit(bump, BACKGROUND_CONTEXT)

    async def bump_again(tx):
        (await tx.doc(CounterDoc)).count += 1

    await session.commit(bump_again, BACKGROUND_CONTEXT)

    snapshot = await session.snapshot(CounterDoc, BACKGROUND_CONTEXT)
    print(snapshot)  # {'count': 2}

    await session.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

Snapshots, document states, and watches are disposable reads: `snapshot()` copies the committed
value, `document_state()` subscribes to later commits, and `watch_doc()` delivers every committed
frame. `MemoryStorage` is the reference backend; SQLite and JSONL live in
`pi_durable.storage.sqlite` and `pi_durable.storage.jsonl`.

### A conversation

The `Harness` adds conversations on top of a `Session`. A submission is the only entry point: it is
admitted to the inbox, placed as an entry, and settled once a run answers it. Passive writes (such
as seeding a transcript) need no model at all.

<!-- example -->
```python
import asyncio

from pi_durable import Harness, MemoryStorage, UserEntry, create_registry
from pi_durable.context import BACKGROUND_CONTEXT


async def main() -> None:
    harness = await Harness.open(
        MemoryStorage(), {"models": {}, "registry": create_registry()}, BACKGROUND_CONTEXT
    )
    try:
        conversation = await harness.root(BACKGROUND_CONTEXT)
        await conversation.submit(
            {
                "type": "write",
                "entry": {
                    "kind": UserEntry.kind,
                    "model": [{"role": "user", "content": "hello", "timestamp": 0}],
                },
            },
            BACKGROUND_CONTEXT,
        )

        view = await conversation.context(BACKGROUND_CONTEXT)
        print([message["role"] for message in view["messages"]])  # ['user']

        state = await conversation.view_state(BACKGROUND_CONTEXT)
        print([entry["kind"] for entry in state.value["entries"]])  # ['pi.user']
        state.dispose()
    finally:
        await harness.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

### Answering with a model

The provider adapters implement the `Models` protocol, so the core never imports an SDK. A model
reference in an agent change uses the persisted camelCase spelling. Opening a `Harness` starts it
paused, and admitting a submission resumes scheduling, so a host can restore state before work
begins.

```python
import asyncio

from pi_durable import Harness, MemoryStorage, Model, create_registry
from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.providers.openai import OpenAIModels


async def main() -> None:
    model = Model(provider="openai", model_id="gpt-4.1", context_window=1_000_000, max_tokens=32_768)
    models = OpenAIModels([model])  # reads OPENAI_API_KEY, or pass api_key=...
    harness = await Harness.open(
        MemoryStorage(), {"models": models, "registry": create_registry()}, BACKGROUND_CONTEXT
    )
    try:
        conversation = await harness.root(
            BACKGROUND_CONTEXT,
            {"agent": {"model": {"provider": "openai", "modelId": "gpt-4.1"}}},
        )
        submission = await conversation.submit(
            {"type": "input", "content": "Explain durable execution in one sentence."},
            BACKGROUND_CONTEXT,
        )
        settled = await submission.wait(BACKGROUND_CONTEXT)
        print(settled["status"])  # 'done' or 'unanswered'
    finally:
        await harness.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

The answer is an entry, not a return value: `settled["answer"]` names the assistant entry, which the
conversation view and the entry scan both expose. This snippet needs credentials, so the test suite
does not run it; README examples that run offline carry an `<!-- example -->` marker and are
executed by `tests/test_release_docs.py`.

## API at a glance

| Module | Holds |
| --- | --- |
| `pi_durable` | Root export: `Session`/`create_session`, `Harness`, entry tokens, document and task definitions, errors, provider protocol, `MemoryStorage`, and the built-in task types. |
| `pi_durable.env` | The `ExecutionEnv` contract, the POSIX backend, output truncation, and line scanning. |
| `pi_durable.harness` | Conversations, submissions and the inbox, the scheduler, agents and the registry, extensions and hooks, tools, generation, compaction, usage, the conversation view, the agent event stream, and the task graph. |
| `pi_durable.providers` | The `Model` catalog entry, the `Models` protocol, transcript and request helpers, retry and overflow classifiers, and the OpenAI/Anthropic adapters (`pi_durable.providers.openai`, `pi_durable.providers.anthropic`). |
| `pi_durable.session` | The Session kernel, the transaction API, document drafts, forks, and observation. |
| `pi_durable.storage` | The reference memory backend (`pi_durable.storage.memory`), SQLite (`pi_durable.storage.sqlite`), and JSONL (`pi_durable.storage.jsonl`). |
| `pi_durable.testing` | Storage and environment conformance suites, plus assertions for writing your own backend. |
| `pi_durable.tool_search` | BM25 deferred-tool discovery for large tool sets. |
| `pi_durable.tools` | The built-in coding tools: `read`, `write`, `edit`, and `bash`. |

The [usage guide](./docs/usage.md) walks through documents, tasks, the Harness, providers, storage
backends, and extensions with runnable examples. The normative details are in the
[vendored 1.3.0 specification](./docs/upstream/spec-1.3.0.md); the parity matrix is
[`PARITY.md`](./PARITY.md), and the [audit report](./docs/parity-report.md) lists the translated
suites and the remaining explicit deviations.

## Stability and versioning

- **Lockstep.** The package version tracks the upstream release it translates: 1.3.0 here is
  `@earendil-works/pi-durable` v1.3.0 (tag `v1.3.0`, commit
  `71023aec1546d6d677a65669c5665c0927ce0dd0`). Adopting a later upstream release is a version bump,
  not a routine sync.
- **Parity is semantics and persisted shapes.** Python names follow PEP 8 while classes stay
  PascalCase and persisted JSON keeps camelCase, so a stored record round-trips as the specification
  describes. Cross-language storage interoperability is incidental, not a contract; see
  [ADR 0001](./docs/adr/0001-parity-baseline-and-boundary.md).
- **Stable surface.** The modules in the table above, the names exported from them, and the
  specification sections they implement. `AgentEventStream` is experimental, as upstream says.
- **Excluded.** Code Mode, the image implementation (the `ImageProcessor` protocol remains),
  Cloudflare adapters, PowerShell, Windows support, and the benchmark helpers. [`PARITY.md`](./PARITY.md)
  marks every exclusion.

## Development

```console
uv sync --extra dev,openai,anthropic,schema
uv run ruff check src tests
uv run mypy
uv run pytest
```

CI runs the suite on Linux and macOS for Python 3.11–3.15 and the two free-threaded builds, builds
and installs the sdist and wheel in fresh environments, then installs each extra from the wheel to
prove the extras resolve and stay independent. [`docs/release.md`](./docs/release.md) records the
full release steps: verify, build, tag, publish.

## License

MIT. The vendored v1.3.0 specification is MIT, copyright (c) 2025 Mario Zechner; see
[`docs/upstream/LICENSE`](./docs/upstream/LICENSE).

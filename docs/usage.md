# Usage guide

A tour of the 1.3.0 surface: the Session kernel, tasks, the Harness, providers, storage, tools, and
the testing helpers. Snippets marked with an `<!-- example -->` marker run in the test suite
(`tests/test_release_docs.py`); the rest are shaped like the code but need credentials, an SDK, or a
deployment decision.

The normative description is the [vendored 1.3.0 specification](./upstream/spec-1.3.0.md). This
guide shows the Python spelling of it; [`PARITY.md`](../PARITY.md) is the export-by-export contract.

- Python names follow PEP 8: `max_tokens`, `when_busy`, `model_id`.
- Persisted JSON keeps camelCase: a `ModelRef` is `{"provider": ..., "modelId": ...}`, an agent change
  key is `thinkingLevel`.
- Every operation takes an explicit `Context`. `BACKGROUND_CONTEXT` is the one without an abort
  signal; a `Context` with an `AbortSignal` cancels the work derived from it.

## Sessions

A `Session` owns one `Storage`. It serializes writes, versions documents, mints IDs, and publishes
commits. There is no other writer, and no uncommitted state leaks: drafts become visible only with
the commit that wrote them.

```python
from pi_durable import MemoryStorage, create_session

session = create_session(MemoryStorage())
```

`create_session()` also accepts `now` (the clock, in milliseconds) and `document_retention_ms` (how
long an unobserved document stays cached, default ten minutes).

### Documents

A document is a typed JSON record with one logical address. `define_doc` binds its kind, version,
scope, history policy, fork policy, and initial value; the returned token is what every read and
write takes.

| Field | Meaning |
| --- | --- |
| `kind` | Persisted discriminator, e.g. `demo.counter`. Stable across versions. |
| `version` | Positive integer; a stored lower version goes through `migrate` on read. |
| `scope` | `"session"`, `"conversation"`, or `"task"`; the address carries the owner ID. |
| `history` | `"latest"` keeps only the current value; `"rewindable"` keeps superseded values readable at an entry. |
| `fork` | For conversation documents: `"initial"` restarts at the initial value, `"asOf"` copies the value at the fork entry. |
| `initial` | Zero-argument callable returning the initial value; a family's `initial` takes the seed instead. |
| `migrate` | Optional `(value, from_version)` callable upgrading a lower stored version. |

A family is a keyed document: `define_doc_family` takes a string key, so one kind holds one
document per member. Creating a family member needs its seed, the value `initial` receives; later
writes pass any value and the seed is ignored.

<!-- example -->
```python
import asyncio

from pi_durable import MemoryStorage, create_session, define_doc, define_doc_family
from pi_durable.context import BACKGROUND_CONTEXT

# One session-wide counter.
CounterDoc = define_doc(
    {"kind": "demo.counter", "version": 1, "scope": "session", "initial": lambda: {"count": 0}}
)

# One document per key; the seed only reaches initial() when the member is created.
HitDoc = define_doc_family(
    {"kind": "demo.hit", "version": 1, "scope": "session", "initial": lambda seed: {"hits": 0}}
)


async def main() -> None:
    session = create_session(MemoryStorage())

    async def record_hit(tx):
        (await tx.doc(CounterDoc)).count += 1
        # Creating with a seed ("GET /"); a later write may pass anything, even None.
        (await tx.doc(HitDoc, "GET /", "seed")).hits += 1

    await session.commit(record_hit, BACKGROUND_CONTEXT)

    print(await session.snapshot(CounterDoc, BACKGROUND_CONTEXT))  # {'count': 1}
    print(await session.snapshot(HitDoc, "GET /", BACKGROUND_CONTEXT))  # {'hits': 1}
    print(await session.snapshot(HitDoc, "POST /", BACKGROUND_CONTEXT))  # None

    await session.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

Writes go through `tx.doc(...)`; reads go through `session.snapshot()` (a copy),
`session.document_state()` (a disposable state that delivers later commits), or
`session.watch_doc()` (every committed frame, exact and ordered). A retirement
(`tx.retire_doc()`) ends an incarnation; the next write starts a fresh one from `initial`.

<!-- example -->
```python
import asyncio

from pi_durable import MemoryStorage, create_session, define_doc
from pi_durable.context import BACKGROUND_CONTEXT

CounterDoc = define_doc(
    {"kind": "demo.counter", "version": 1, "scope": "session", "initial": lambda: {"count": 0}}
)


async def main() -> None:
    session = create_session(MemoryStorage())
    await session.commit(lambda tx: tx.doc(CounterDoc), BACKGROUND_CONTEXT)

    state = await session.document_state(CounterDoc, BACKGROUND_CONTEXT)
    assert state is not None
    seen: list[int] = []
    # The listener runs with the acquisition value first, then once per committed change.
    state.subscribe(lambda value, context, delivery: seen.append(value["count"]))

    async def count_to_five(tx):
        (await tx.doc(CounterDoc)).count = 5

    await session.commit(count_to_five, BACKGROUND_CONTEXT)
    print(seen)  # [0, 5]

    state.dispose()
    await session.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

### Entries and context

A conversation holds an ordered transcript of entries. Built-in tokens cover the entries durable
writes itself (`UserEntry`, `AssistantEntry`, `SystemEntry`, `ToolResultEntry`, `ResetEntry`,
`CompactionEntry`); `define_entry("app.note")` gives an application its own kind.

`session.context()` derives the provider request from the transcript: `entries` is the active
transcript, `contributions` is each entry's model messages after edits and excluded stop reasons,
and `messages` orders tool results after their calls, synthesizes missing ones, and puts a baseline
system message first when an entry supplies one.

<!-- example -->
```python
import asyncio

from pi_durable import MemoryStorage, UserEntry, create_session
from pi_durable.context import BACKGROUND_CONTEXT


async def main() -> None:
    session = create_session(MemoryStorage())

    async def create(tx):
        conversation = await tx.create_conversation({"ownership": {"kind": "ownerless"}})
        return conversation["id"]

    conversation_id = await session.commit(create, BACKGROUND_CONTEXT)

    async def seed(tx):
        await tx.append_entry(
            UserEntry,
            conversation_id,
            {"model": [{"role": "user", "content": "ping", "timestamp": 1}]},
        )

    await session.commit(seed, BACKGROUND_CONTEXT)

    view = await session.context(conversation_id, BACKGROUND_CONTEXT)
    print([message["role"] for message in view["messages"]])  # ['user']
    print([entry["kind"] for entry in view["entries"]])  # ['pi.user']

    await session.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

`session.context(conversation_id, context, at)` derives the context as of an earlier visible entry,
which is exactly what a fork at that entry starts with.

## Tasks

A task is a durable state machine. `initial(input)` picks the starting checkpoint, `phases` maps a
phase name to its handler, and `abort` settles the task when the host aborts it. A handler writes
its next state with `runtime.commit()`; a state of `{"status": "running", "checkpoint": {...}}`
re-enters the named phase after a restart, and a terminal state completes or aborts the task.

<!-- example -->
```python
import asyncio

from pi_durable import Harness, MemoryStorage, create_registry, define_extension, define_task
from pi_durable.context import BACKGROUND_CONTEXT


async def double(task, runtime, context):
    # Committing the terminal state is the effect; a crash before it re-enters this phase.
    await runtime.commit(
        lambda: {
            "status": "terminal",
            "outcome": {"status": "completed", "result": task["input"] * 2},
        },
        context,
    )


async def abort(task, runtime, context):
    await runtime.commit(
        lambda: {"status": "terminal", "outcome": {"status": "aborted", "reason": "host"}},
        context,
    )


Doubler = define_task(
    {
        "name": "demo.doubler",
        "version": 1,
        "initial": lambda input: {"phase": "double"},
        "phases": {"double": double},
        "abort": abort,
    }
)


async def main() -> None:
    registry = create_registry()
    registry.install(define_extension({"name": "demo", "tasks": [Doubler]}))
    harness = await Harness.open(
        MemoryStorage(), {"models": {}, "registry": registry}, BACKGROUND_CONTEXT
    )
    try:
        conversation = await harness.root(BACKGROUND_CONTEXT)
        task_id = await conversation.commit(
            lambda tx: tx.create_task(Doubler, 21, {"ownership": {"kind": "conversation"}}),
            BACKGROUND_CONTEXT,
        )
        harness.resume()  # a fresh Harness is paused, so a host can restore state first
        settled = await harness.wait_for_task(task_id, BACKGROUND_CONTEXT)
        print(settled["state"]["outcome"])  # {'status': 'completed', 'result': 42}
    finally:
        await harness.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

Details worth knowing:

- **Ownership.** `{"kind": "conversation"}` binds the task to the committing conversation;
  `{"kind": "task", "taskId": ...}` makes it a child of another task. `background: True` keeps a
  background task out of its conversation's idle condition, and `abandonOnRestart: True` drops it
  instead of resuming it.
- **One invocation per task.** The scheduler drives a task through consecutive checkpoint advances
  without a phase boundary when it can, and re-enters the checkpointed phase after a crash.
- **Waits and outcomes.** `runtime.wait_for_task()`, `runtime.outcomes()`, `runtime.owned_tasks()`,
  and `harness.abort_task()` are the scheduling surface a task sees.
- **Hooks.** A definition may carry `hooks` for `beforeTask`/`afterTask` style observation; see the
  specification's section 5.

## The Harness

`Harness` is a `Session` plus a scheduler, conversations, submissions, an inbox, compaction, and
extension resolution. Open it with the model catalog and a registry; CI and the examples above use
`create_registry()`, which installs the built-in generation, compaction, and tool tasks.

```python
from pi_durable import Harness, create_registry

harness = await Harness.open(
    storage,
    {"models": models, "registry": create_registry()},
    context,
)
```

`HarnessOptions` accepts `models`, `registry`, `settings`, `env`, `conversation_created`, `now`, and
`on_report`. `on_report` receives failures that do not fail the calling operation. A Harness starts
paused; `resume()` (or the first submission) enables scheduling, and `close(context)` seals admission
and joins the handlers.

### Conversations

`harness.root()` creates or returns the root conversation; `create_conversation({"ownership": ...})`
starts another; `fork()` copies a conversation at an entry. Each returns a stateless handle bound to
the Harness: `submit`, `commit`, `entries`, `context`, `agent`, `configure`, `fork`, `compact`,
`reset`, `abort`, `wait_for_idle`, `view_state`, and `watch`.

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
        entry = await conversation.commit(
            lambda tx: tx.append_entry(
                UserEntry,
                conversation.id,
                {"model": [{"role": "user", "content": "ping", "timestamp": 0}]},
            ),
            BACKGROUND_CONTEXT,
        )

        fork = await conversation.fork(
            entry["id"], {"ownership": {"kind": "ownerless"}}, BACKGROUND_CONTEXT
        )
        fork_view = await fork.context(BACKGROUND_CONTEXT)
        print([message["role"] for message in fork_view["messages"]])  # ['user']

        graph = await harness.task_graph(BACKGROUND_CONTEXT)
        print(sorted(graph.value))  # ['tasks']
        graph.dispose()
    finally:
        await harness.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

### Submissions and the inbox

A submission is the only way into a conversation. `{"type": "write"}` places a passive entry and
needs no run; `{"type": "input"}` is queued in the `pi.inbox` document, placed as a `pi.user` entry
at a boundary, and settled once a run answers it.

```python
admitted = await conversation.submit({"type": "input", "content": "hi"}, context)
status = await admitted.status(context)  # the current SubmissionRecord
settled = await admitted.wait(context)  # {'status': 'done' | 'unanswered', ...}
```

- `when_busy` (upstream `whenBusy`) decides what happens when the conversation is already busy:
  `"steer"` joins the current run's next boundary, `"followUp"` waits for the next run, `"reject"`
  raises `ConversationBusy`. An input without `when_busy` queues as a follow-up;
  `settings.steering_mode` and `settings.follow_up_mode` decide how many queued items of each mode a
  boundary selects (`all` or `one-at-a-time`, the default).
- `request_id` makes admission idempotent: submitting the same request ID returns the existing
  submission.
- `harness.inspect(context)` reports scheduling state, live tasks, and queued submissions;
  `harness.submission(id, context)` reads one; `harness.abort_submission()` withdraws one.
- `settled["entry"]` names the placed entry and `settled["answer"]` the assistant entry that
  answered it. The answer is an entry, not a return value.

### The agent

A conversation's choices live in its `pi.agent` document. Read them resolved with
`conversation.agent(context)`; replace them in one commit with `conversation.configure({...})`.

```python
await conversation.configure(
    {
        "model": {"provider": "openai", "modelId": "gpt-4.1"},
        "thinkingLevel": "medium",
        "instructions": "Answer with one sentence.",
        "tools": ["read", "lookup"],
    },
    context,
)
```

`model` is the only field generation needs. `extensions`, `tools`, and `modelTools` accept a list of
names or an object of additions and removals, following the specification's agent-resolution rules;
unset fields follow the Harness settings and the installed extensions.

### Compaction

Compaction replaces an old prefix of the model context with a summary entry. It runs automatically
above the thresholds in `settings.compaction` (`reserve_tokens`, `keep_recent_tokens`,
`background_tokens`), or on demand:

```python
task_id = await conversation.compact("Keep the decisions, drop the exploration.", context)
```

Only a blocking compaction makes the run wait; a background one runs beside it. The summary is a
`pi.compaction` entry whose `head` is the first kept entry, the cut.

### Observation

- `conversation.view_state()` and `conversation.watch()` expose the conversation view: active
  entries plus the built-in documents, advanced only by committed publications.
- `harness.task_graph()` and `harness.watch_task_graph()` expose every live task, its waits, and
  the conversations it owns.
- `watch_events(harness, conversation_id, context)` returns the experimental `AgentEventStream`
  that translates committed publications into coding-agent-shaped events.

Close the Harness when the host stops. `close(context)` seals admission synchronously and joins the
running work; storages and execution environments are the host's to close.

## Tools, extensions, and hooks

A registry holds extensions. An extension contributes tasks, tools, hook registrations, prompt
sections, and wraps; `define_extension`, `define_tool`, `section`, `hook`, and `wrap_tool` are
identity constructors that type their input.

A tool's `execute(args, api, context)` receives validated arguments, the `ToolExecutionApi`, and the
context, and returns a `ToolExecutionResult`: `output` for the model, `structured_output` for
programs, `details` for UIs, plus optional `diagnostics`, `usage`, and `control`. Omitted `replay` is
`"unsafe"`; omitted output limits are 50 KiB, 2,000 lines, retaining the head.

<!-- example -->
```python
import asyncio

from pi_durable import Harness, MemoryStorage, create_registry, define_extension, define_tool
from pi_durable.context import BACKGROUND_CONTEXT


async def lookup(args, api, context):
    return {"output": [{"type": "text", "text": f"looked up {args['id']}"}]}


async def main() -> None:
    registry = create_registry()
    registry.install(
        define_extension(
            {
                "name": "catalog",
                "tools": [
                    define_tool(
                        {
                            "name": "lookup",
                            "description": "Look up one catalog entry by ID.",
                            "parameters": {
                                "type": "object",
                                "properties": {"id": {"type": "string"}},
                                "required": ["id"],
                            },
                            "execute": lookup,
                        }
                    )
                ],
            }
        )
    )

    harness = await Harness.open(
        MemoryStorage(), {"models": {}, "registry": registry}, BACKGROUND_CONTEXT
    )
    try:
        conversation = await harness.root(BACKGROUND_CONTEXT, {"agent": {"tools": ["lookup"]}})
        agent = await conversation.agent(BACKGROUND_CONTEXT)
        print([tool.name for tool in agent.tools])  # ['lookup']
        print([extension.name for extension in agent.extensions])  # ['catalog']
    finally:
        await harness.close(BACKGROUND_CONTEXT)


asyncio.run(main())
```

- **Nested calls.** A running tool calls another with `api.execute_tool(name, args, context, key=...)`.
  The nested call is its own task with its own ID and never enters the transcript.
- **Deferred tools.** Mark a tool `deferred=True` and install the `tool_search` extension
  (`pi_durable.tool_search`): the model searches deferred metadata by BM25 and the matches load with
  `control["addTools"]` for the next request.
- **System prompt.** `section(key, render)` contributes a prompt section; rendering receives a
  `PromptInput` with the conversation ID, resolved agent, environment, and a document reader.
- **Hooks and wraps.** `hook(task, handlers)` registers handlers for one task's hook points:
  `beforeRequest`, `afterResponse`, `onYield`, and `afterTools` on generation, `beforeTool` and
  `afterTool` on tools, and `beforeCompact` on compaction. `wrap_tool(tool, wrapper)` and
  `wrap_section(key, wrapper)` reshape the resolved tool or section wherever the wrapping extension
  is selected.
- **Coding tools.** `pi_durable.tools.create_coding_tools()` is an extension with `read`, `write`,
  `edit`, and `bash`. Nothing installs it automatically; pass an `ImageProcessor` to `read` if you
  want image placeholders.

## Models and providers

The core defines the `Models` protocol, not an SDK. `Model` is one catalog entry: provider, model ID,
context window, max tokens, reasoning flag, accepted inputs, input limits, and cost rates.

```python
from pi_durable import Model

model = Model(
    provider="openai",
    model_id="gpt-4.1",
    context_window=1_000_000,
    max_tokens=32_768,
    cost={"input": 2.0, "output": 8.0, "cacheRead": 0.5, "cacheWrite": 0.0},
)
```

Generation resolves `(provider, model_id)` through the catalog you pass as `models`. The adapters in
the extras implement the whole protocol:

```python
from pi_durable.providers.anthropic import AnthropicModels
from pi_durable.providers.openai import OpenAIModels

models = OpenAIModels([model])  # reads OPENAI_API_KEY unless api_key= is passed
models = AnthropicModels([model], api_key="...")
```

Both adapters accept `client=` to inject an SDK-shaped client, `base_url=`, and `headers=`. They
translate the pi-ai transcript into the provider request, stream text, thinking, and tool-call
deltas, and return the terminal assistant message with usage and a mapped stop reason.

A custom adapter implements `get_model`, `stream_simple`, `complete_simple`, `stream`, `complete`,
`fetch_deferred`, and `cancel_deferred`. The shared retry and overflow classifiers live in
`pi_durable.providers`: `is_retryable_assistant_error()`, `is_context_overflow()`, and
`retry_delay_ms()`.

## Storage

`Storage` is the persistence contract: conversations, entries, tasks, submissions, documents, and
the publication sequence. Three backends ship in the package.

- **Memory** (`pi_durable.storage.memory.MemoryStorage`) is the reference backend: no I/O, ids minted
  from a counter, commits applied atomically. Use it for tests and single-process prototypes.
- **SQLite** (`pi_durable.storage.sqlite`) is portable and needs a database facade; the stdlib-backed
  POSIX adapter lives in `pi_durable.storage.sqlite.posix`, and Cloudflare is out of scope.
- **JSONL** (`pi_durable.storage.jsonl`) appends one line per commit into a directory and needs only
  the filesystem capability of an `ExecutionEnv`.

<!-- example -->
```python
import asyncio
import shutil
import tempfile

from pi_durable import create_session, define_doc
from pi_durable.context import BACKGROUND_CONTEXT
from pi_durable.env.posix import PosixExecutionEnv
from pi_durable.storage.jsonl import JsonlStorage
from pi_durable.storage.sqlite import SqliteStorage
from pi_durable.storage.sqlite.posix import open_posix_sqlite_database

CounterDoc = define_doc(
    {"kind": "demo.counter", "version": 1, "scope": "session", "initial": lambda: {"count": 0}}
)


async def main() -> None:
    directory = tempfile.mkdtemp(prefix="pi-durable-usage-")
    try:
        sqlite = await SqliteStorage.open(
            await open_posix_sqlite_database(f"{directory}/durable.sqlite3")
        )
        session = create_session(sqlite)
        await session.commit(lambda tx: tx.doc(CounterDoc), BACKGROUND_CONTEXT)
        print(await session.snapshot(CounterDoc, BACKGROUND_CONTEXT))  # {'count': 0}
        await session.close(BACKGROUND_CONTEXT)

        jsonl = await JsonlStorage.open(
            f"{directory}/jsonl", PosixExecutionEnv(cwd=directory), BACKGROUND_CONTEXT
        )
        session = create_session(jsonl)
        await session.commit(lambda tx: tx.doc(CounterDoc), BACKGROUND_CONTEXT)
        print(await session.snapshot(CounterDoc, BACKGROUND_CONTEXT))  # {'count': 0}
        await session.close(BACKGROUND_CONTEXT)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


asyncio.run(main())
```

Both backends accept options: SQLite takes a `prefix` for every table and index, and JSONL takes
`fsync`. A JSONL append whose outcome is unknown poisons the backend; reopen it. Cross-language
storage interoperability is incidental, not a contract.

## Execution environment and coding tools

`pi_durable.env` publishes the `ExecutionEnv` protocol: a `FileSystem` and a `Shell` whose expected
failures are returned as `Ok`/`Err` instead of raised. `PosixExecutionEnv` is the local-machine
backend, and `pi_durable.storage.jsonl` needs only the filesystem half. A `Harness` builds an
environment per conversation through the `env` option:

```python
from pi_durable.env.posix import PosixExecutionEnv

harness = await Harness.open(
    storage,
    {
        "models": models,
        "registry": create_registry(),
        "env": lambda target, context: PosixExecutionEnv(cwd=target.get("cwd") or "."),
    },
    context,
)
```

Tools reach it as `api.env`. Windows and PowerShell are out of scope; the POSIX backend covers
Linux and macOS.

## Testing

`pi_durable.testing` ships the conformance suites, so a new backend can be held to the same contract
as the built-ins. The cases are runner independent: pass an adapter for your assertion style and a
provider that hands each case a fresh backend.

```python
from pi_durable import MemoryStorage
from pi_durable.testing import create_expect_assertions, create_storage_conformance


async def with_storage(use):
    await use(MemoryStorage())


cases = create_storage_conformance(
    {"assertions": create_expect_assertions(expect), "with_storage": with_storage}
)
for case in cases:
    await case.run()
```

`create_env_conformance()` does the same for an `ExecutionEnv`, and `register_storage_conformance()`
plugs the cases into a Vitest/Jest-shaped runner. `create_expect_assertions()` adapts an `expect`
offering `to_be`, `to_equal`, `to_match_object`, `to_be_greater_than`, `to_be_truthy`, `rejects.to_throw`,
and `resolves.to_be`.

Provider adapters take an injected client, so an integration test can run the whole generation path
offline by passing an SDK-shaped fake to `OpenAIModels(client=...)` or `AnthropicModels(client=...)`.
The adapter suites in `tests/` do exactly that.

## What next

- [`../PARITY.md`](../PARITY.md) — every export, its Python target, and its tests.
- [`../docs/parity-report.md`](./parity-report.md) — the translated suites and the deviations that
  remain.
- [`upstream/spec-1.3.0.md`](./upstream/spec-1.3.0.md) — the normative specification.
- [`../GLOSSARY.md`](../GLOSSARY.md) — the vocabulary this guide uses.

# pi-durable-python

A Python port of the API and architecture of
[`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/main/packages/durable),
the durable agent harness behind pi.

```bash
pip install pi-durable-python
```

The distribution is `pi-durable-python`; the import is `pi_durable`.

A **Session** owns one mutation line. Immutable transcript entries, full task records,
submissions, and tracked JSON documents commit atomically through it. Only committed state
is observable. A **Harness** runs durable generation and tool tasks on top of that kernel.

```python
import asyncio

from pi_durable import (
    FauxModel, Harness, MemoryStorage, Models, create_registry,
)


async def main() -> None:
    models = Models().set("faux", "faux-1", FauxModel(["Paris is the capital."]))
    harness = await Harness.open(
        MemoryStorage(),
        models=models,
        registry=create_registry(),
    )
    root = await harness.root(agent={"model": {"provider": "faux", "modelId": "faux-1"}})

    submission = await root.submit({"type": "input", "content": "What is the capital of France?"})
    settled = await submission.wait()
    print(settled["status"])  # "done"

    view = await root.context()
    print([message["role"] for message in view["messages"]])  # ['user', 'assistant']
    await harness.close()


asyncio.run(main())
```

The reference package is TypeScript. This port keeps its record shapes, state machines,
commit rules, and public concepts, while adapting the mechanics to Python:

- Records are plain JSON objects (`dict`), not dataclasses, so storage, JSONL, and SQLite
  carry exactly the committed representation.
- Async is native `asyncio`; the Chord `Context` parameter is optional and cancellation is
  ordinary task cancellation.
- Documents use a revocable, path-tracking draft (`draft["key"] = value`,
  `draft["items"].append(...)`) and Chord-compatible operation tuples.
- Model access is a small `Model` protocol instead of `@earendil-works/pi-ai`.

## Architecture

```
Caller ── submit()/commit() ──► Harness ──► TaskScheduler ──► Session (one mutation line)
                                                    │                 │
                                                    │                 ├─ Transaction (Tx)
                                                    │                 │    ├─ table writes
                                                    │                 │    └─ document drafts → ops
                                                    │                 └─ Storage.commit(atomic batch)
                                                    │                          │
                                                    └─ generation / tool tasks ┘
                                                       (durable state machines)
```

Invariants carried over from the specification:

1. One Session commit is atomic across records and document writes.
2. A document update is published only after its storage commit succeeds.
3. There is no volatile publication path; visible progress is durable.
4. External effects never run inside the Session mutation transaction.
5. Entries and IDs are immutable and never reused after a committed write.
6. Drafts are revoked when their transaction settles; assigned values are strict JSON copies.
7. Commit/close observers run synchronously on the line; document state and watch callbacks
   run later, serialized.
8. A storage error is fatal to the open Session and publishes nothing.

## Concepts

- **Session** — one mutation line over one storage backend. `create_session(storage)`.
- **Transaction (`Tx`)** — the callback surface of one atomic commit. Reads are committed-only;
  writing a table and then reading one raises `ReadAfterWrite`. Document access remains allowed.
- **Conversation** — a transcript scope. Conversation IDs are Session-global. A conversation
  may fork another at one visible entry.
- **Entry** — immutable record with `kind`, optional model messages, JSON `data`, optional
  `head` (start a new active context at this entry), and `edits` (omit/replace an earlier
  entry's model contribution).
- **Document** — typed JSON state with a scope (`session`, `conversation`, `task`), definition
  version, migration, checkpoint policy, and for conversation documents `history`
  (`latest`/`rewindable`) and `fork` (`asOf`/`current`/`initial`).
- **Task** — durable state machine: `pending → running → waiting/sleeping/completing/terminal`,
  with checkpoint phases, abort protocol, first-writer-wins memos, migration, and restart
  parking.
- **Submission** — a durably admitted user input or passive entry write, trackable to
  `done`/`unanswered` and withdrawable while queued.
- **Run / turn** — a run is the generation sequence from admitted inputs to an answer; a turn
  is one model response and the tool calls it makes.
- **Agent** — model, thinking level, extension/tool selection, instructions, and cwd stored in
  `pi.agent` as names and resolved against the process registry at each use.
- **Extension / Registry** — process-owned bundles of tools, prompt sections, and task
  definitions. Nothing executable is stored in conversations.

## Kernel API

```python
from pi_durable import MemoryStorage, create_session, define_doc

Notes = define_doc(
    kind="app.notes",
    version=1,
    scope="conversation",
    history="rewindable",
    fork="asOf",
    initial=lambda: {"text": "", "items": []},
    checkpoint_when=lambda value, ops, info: False,   # deltas, not a full base
)

session = create_session(MemoryStorage())

conversation = await session.commit(
    lambda tx: tx.create_conversation({"ownership": {"kind": "ownerless"}})
)

async def write(tx):
    entry = await tx.append_entry(conversation["id"], {"kind": "app.note", "data": "hi"})
    draft = await tx.doc(Notes, conversation["id"])
    draft["text"] = "hello"
    draft["items"].append("first")
    return entry

entry = await session.commit(write)
print(await session.snapshot(Notes, conversation["id"]))
print(await session.snapshot_as_of(Notes, conversation["id"], entry["id"]))

watch = await session.watch_doc(Notes, conversation["id"])
watch.start(lambda value, ops: print("update", value, ops))
await session.close()
```

Kernel surface (`Session`):

- `commit(fn)`, `close()`, `closed`, `subscribe_commits`, `subscribe_close`
- `snapshot(token, *args)`, `snapshot_as_of(token, conversation_id, *args)`
- `document_state(token, *args)` — disposable read-only state with committed updates
- `watch_doc(token, *args)` — serialized exact-frame watch, bounded at 100 pending frames
- `commit_with(fn, scope)` / `read_on_line(job)` for Harness internals

`Tx` surface:

- reads: `conversation`, `entry`, `task`, `scan_conversations`, `scan_entries`,
  `scan_tasks`, `latest_head_marker`, `submission_by_request`
- writes: `create_conversation`, `fork_conversation`, `append_entry`, `create_task`,
  `create_submission`, `settle_submission`, `place_submission`
- documents: `doc`, `retire_doc`, including keyed families with `initial(seed)`

## Documents

```python
draft["text"] = "value"        # ["s", ["text"], "value"]
draft["items"].append("x")     # ["p", ["items"], n, 0, ["x"]]
del draft["text"]              # ["d", ["text"]]
```

`checkpoint_when(value, ops, info)` decides whether a change is stored as a full base or a
delta after the newest base. Rewindable documents keep history so `snapshot_as_of()` can read
the incarnation alive at an entry's commit. Forks copy each source document according to its
policy: `asOf` takes the ancestor's value at the fork entry, `current` takes the parent's
current value, and `initial` does not copy (the first `tx.doc()` initializes it).

Document definitions validate scope/history/fork at definition time. Typed access rejects a
persisted incarnation whose lifetime semantics disagree with the token. `snapshot`,
`document_state`, and `watch_doc` never create or persist migration.

## Tasks

```python
from pi_durable import TaskOptions, define_task

async def wait(task, runtime):
    await runtime.sleep_until(runtime.now() + 60_000, {"phase": "deliver"})

async def deliver(task, runtime):
    await runtime.commit(
        lambda tx, current: {
            "status": "terminal",
            "outcome": {"status": "completed", "result": "done"},
        }
    )

async def abort(task, runtime):
    await runtime.commit(
        lambda tx, current: {"status": "terminal", "outcome": {"status": "aborted"}}
    )

Reminder = define_task(
    name="app.reminder",
    version=1,
    initial=lambda value: {"phase": "wait"},
    phases={"wait": wait, "deliver": deliver},
    abort=abort,
)
```

Creation:

```python
task_id = await conversation.commit(
    lambda tx: tx.create_task(
        Reminder,
        {"text": "stand up"},
        TaskOptions(ownership={"kind": "conversation"}),
    )
)
harness.resume()
record = await harness.wait_for_task(task_id)
```

A phase must commit a changed checkpoint, a wait, a sleep, or an outcome; returning without
durable progress faults the task. A returned terminal outcome becomes `completing` while the
task's ordinary owned work is still live, and terminal only when that work drains. A task
cannot create owned work in the commit that finishes it. `memo(name, value)` is a durable
first-writer-wins read; `memo(name)` reads it. On reopen, `running` records are counted as
restarts and returned to `pending`; at `restartLimit` (default 5) a task is parked until
`retry_task()`. `abandonOnRestart` tasks are abort-marked instead.

## Harness API

```python
harness = await Harness.open(
    storage,
    models=Models().set("provider", "model-id", model),
    registry=create_registry(),
    settings={"followUpMode": "all", "afterFailedRun": "continue"},
    env=lambda target: my_env(target["cwd"]),
    on_report=lambda error, info: log(error, info),
)
root = await harness.root(agent={"model": {"provider": "provider", "modelId": "model-id"}})
```

Harness adds:

- lazy, reserved-ID root (`1`), `create_conversation`, `fork`, `conversation`
- `submit` with `whenBusy` (`steer` / `followUp` / `reject`) and `requestId` deduplication
- inbox placement at turn and final boundaries, writes before steers before follow-ups
- `abort`, `abort_submission`, `abort_task`, `retry_task`, `wait_for_idle`
- `inspect`, `task_graph`, `usage`, `wake`, `resume`
- built-in `pi.generation` and `pi.tool` tasks, `pi.agent`, `pi.provider`, `pi.live`,
  `pi.inbox`, and `pi.usage` documents
- system prompt rendering as ordered `pi.system` section/tool patches, including a baseline
  that leads the request when the provider protocol needs it
- durable tool calls as child tasks; arguments and results are committed, and a restart
  re-runs only `replay="safe"` tools (others receive an `interrupted` error result)

A conversation is busy while a run task is live. Input arriving while busy — or while an
idle conversation still has a non-empty inbox, for example after a failed run — is queued as
a steer or follow-up; writes queue as passive entries and trigger a final boundary in the
same commit. At each assistant turn boundary, writes and selected steers are placed; at the
final boundary follow-ups also place, and any placed input starts the next run. Order is by
submission ID, and `steeringMode`/`followUpMode` select one or all per mode. A queued or
immediate head write older than the active range start settles `unanswered` with reason
`stale`, so it cannot resurrect history a reset cut. `Conversation.abort()` withdraws queued
inputs, aborts the conversation's ordinary task scope, and waits for it to be idle.

## Models and tools

```python
from pi_durable import ToolResult, define_extension, define_tool, section


async def echo(args, api):
    api.output("echoing")
    return ToolResult(output=f"echo:{args['text']}", details={"seen": args["text"]})


echo_tool = define_tool(
    name="echo",
    description="Echo text",
    parameters={"type": "object", "properties": {"text": {"type": "string"}}},
    execute=echo,
)

extension = define_extension(
    name="demo",
    tools=[echo_tool],
    sections=[section("preamble", lambda context: "You are concise.")],
)
registry = create_registry()
registry.install(extension)
```

A `Model` exposes `async complete(ModelRequest) -> ModelResponse`. `FauxModel` scripts tests
with strings, assistant-message dicts, `ModelResponse`s, or callables. `FauxModel.requests`
records every request, including the resolved tools and provider session ID.

`ToolResult` channels are `output` (for the model), `details` (for UIs), `diagnostics`,
`usage`, and `is_error`. A thrown tool or an unavailable tool becomes an error tool result;
the run continues so the model can react. `api.output()` accumulates running text.

## Storage

- `MemoryStorage` — detached in-memory reference backend.
- `open_jsonl_storage(directory)` — one JSON line per atomic commit, replayed on open.
- `open_sqlite_storage(path)` — one row per atomic commit in a WAL SQLite database, replayed
  on open.

All backends enforce global numeric ID ownership, immutable conversation/entry/document
creation, document incarnation intervals and address uniqueness, and detached reads. Commit
sequence order is preserved across reopen, so document history and `snapshot_as_of()` remain
correct. The JSONL and SQLite backends journal whole commits and rebuild their index on open;
they are durable but not SQL-queryable by record table.

## Errors

- `ReadAfterWrite` — a table read after the first table write.
- `StorageRequestError` — an invalid storage read (unknown conversation, bad cursor,
  historical read of a latest-only document).
- `SessionFailed` — a storage error failed the Session; close and reopen from storage.
- `ConversationBusy` — `whenBusy="reject"` met a busy conversation.
- `SessionClosed` / `InvocationEnded` / `TaskFaulted`.

A plain Session drops reports. The Harness forwards them to `on_report(error, info)` with the
same `info` taxonomy as the reference (`session_failed`, `task_faulted`, `task_parked`,
`migration_failed`, `task_outdated`, `extension_error`, `host_error`, `listener_error`).

## Testing

```bash
uv venv .venv
uv pip install --python .venv/bin/python pytest pytest-asyncio ruff mypy
.venv/bin/pytest
.venv/bin/ruff check src tests
.venv/bin/mypy src
```

The suite covers the kernel, documents and forks, storage conformance across all three
backends, context derivation, tasks/scheduler recovery, and the harness (inbox, tools,
abort, failures, usage, persistence).

## Scope and known deviations

This is an architectural port, not a line-by-line translation of every optional subsystem.
Implemented faithfully: the Session/Tx kernel, Chord-style tracked documents, forks and
document history, context derivation, task scheduling and recovery, submissions/inbox,
generation and tool tasks, agent/prompt resolution, and the three storage backends.

Not ported (raising a clear `NotImplementedError` where an API exists):

- compaction (`Conversation.compact`)
- conversation watches (`Conversation.watch`) — `Conversation.view()` returns a structural
  snapshot instead
- task-graph watches (`watchTaskGraph`)
- nested tool calls (`api.execute_tool`)
- Code Mode, tool search, image processing, and the built-in coding tools
- extension hooks/wrappers and the `setTimer`/`onWake` timer abstraction (a basic
  `on_wake` callback is implemented)
- provider retries, tool output truncation/spill, structured-output validation, and
  parallel-vs-sequential tool-round policy
- exact `pi.live` presentation fields (`generation`, retry/deferred state, nested slots);
  the port keeps `run` and the current round's `tools` slots
- telemetry spans/metrics and the `Context` cancellation parameter (asyncio cancellation
  replaces it)

The full normative reference is the upstream `packages/durable/docs/spec.md`.

## License

MIT, matching the reference package.

# pi-durable-python

A Python port of `@earendil-works/pi-durable`: durable conversations, tasks, and documents on
`asyncio`, held to the upstream semantics and persisted record shapes. The package version follows
the upstream release it translates; `PARITY.md` pins the current baseline, and the
[vendored specification](./docs/upstream/spec-1.5.0.md) is the normative reference every term here
describes.

## Python support

**Claimed version set**:
The Python versions the package says it runs on: `requires-python` plus the classifiers it publishes.
_Avoid_: supported versions, target versions

**Verified version set**:
The Python versions a blocking CI job runs the full suite against.
_Avoid_: tested versions, CI matrix

**Blessed version set**:
The part of the verified set a distribution statement names: the `Programming Language :: Python`
classifiers and the README runtime line. Classifiers mirror the verified set; a version is never
blessed before it is verified.
_Avoid_: supported versions, official versions

**Free-threaded-clean**:
Passes the suite on a build with the GIL disabled. It says the code does not depend on the GIL.
_Avoid_: free-threading support, thread-safe

**Thread-safe**:
Safe for concurrent threads sharing one `Session` or `Storage`. Reserved: nothing claims it today.
_Avoid_: free-threaded-clean

**Submission**:
A durable admission of user input, a passive entry write, or a continue.
An input or a write is `queued` in the inbox, `placed` as an entry, then `done` or `unanswered`; a
continue is never queued and places no entry of its own.
_Avoid_: message, job

**Continue submission**:
A submission of `type: "continue"`: it starts a run on the context as it is, without a user message.
It is admitted only while the conversation is idle — a busy one refuses it with `ConversationBusy` —
it is never queued, and it places no entry: it is `placed` with no `entry`, then settles `done` with
an `answer` like an input. Items a failed run left in the inbox are placed first and join its run.
_Avoid_: retry, resume, replay

**Idle rejection**:
An input draft's `when_idle: "reject"`: an idle conversation refuses it with `ConversationIdle` and
writes nothing, so the input never starts a run of its own. It pairs with `when_busy`, which decides
what a working conversation does with the same draft.
_Avoid_: busy rejection, not-found, no-op

**In-commit admission**:
A submission admitted inside the caller's own commit — `submit(tx, draft)` — so it is stored or lost
with that commit. It must come before the commit's first table write, another admission's included;
after it, the call raises `ReadAfterWrite`. A transaction of another Session, or one merely wrapped,
is refused before anything is read.
_Avoid_: nested commit, side write, two-phase submit

**Inbox**:
The built-in `pi.inbox` conversation document holding queued submissions in submission-ID order.
_Avoid_: queue, buffer

**Boundary**:
A point in a run (`postTools` or `final`) where queued submissions are selected and placed.
_Avoid_: flush, drain, checkpoint

**Run**:
The `pi.generation` task named by `pi.live.run` that owns the placed submissions it must answer: an
input's, a queued input's placed at its boundary, or a continue's.
A run is present exactly while the conversation is busy.
_Avoid_: turn, request, generation

**Compaction**:
A `pi.compaction` task that replaces an old prefix of the model context with a summary entry.
It is manual, threshold, or overflow; only a blocking one makes the run wait for it.
_Avoid_: truncation, pruning

**Cut**:
The first entry a compaction keeps verbatim. Range selection picks it so the kept entries cover
`keepRecentTokens`; the summary's `head` is the cut's entry ID.
_Avoid_: boundary, split

**Summary**:
The `pi.compaction` entry a compaction places: the wrapped summary text as a user message, with
`head` naming the cut and `data.reason` naming what started the compaction.
_Avoid_: checkpoint, digest

**Watch**:
One serialized observation of a committed value, held by a `WatchHandle`: `value`, `start`, `stop`,
and `closed`. Its one listener takes a `WatchCall` — `value`, `ops`, `retired` — shared by document,
conversation-view and task-graph watches. A watch is stopped, never closed, and a retirement is a
change it is given rather than an end, so a watch whose document retired lives until it is stopped or
its Session ends. A document watch follows its address, not the incarnation it found there: the
retirement carries the last value and no operations, and a document created at that address later
arrives on the same watch as a root replacement. Termination drops every reference the watch holds
into its Session and into its listener, keeping only its last value.
_Avoid_: subscription, event stream, listener

**Watch end**:
Why a watch stopped, tagged by `kind`: `stopped` from `stop()`, `aborted` from the acquisition
context's abort signal, `ended` or `failed` from its Session, and `listener_error` from the error its
own listener raised. There is no `cancelled` end, and there is no retirement end.
_Avoid_: close reason, cancellation, terminal state

**Coalesced change**:
One watch delivery that folds the commits made while its listener ran, or before it started: the
newest value, the operations of every commit since the previous call folded into one batch — appends
joined, a per-field set keeping the last, a root replacement dropping everything before it — and the
newest commit's `Context`. Past `MAX_PENDING_OPS` (10,000) pending operations the batch is one
self-contained root replacement instead. Convergent observation, not an audit stream: a consumer that
must see every transition persists those facts separately.
_Avoid_: buffered frames, replay, event batch, snapshot

**Conversation view**:
A fixed structural mount of one conversation's active entries and built-in documents.
It advances only from committed publications and keeps at most one mount per conversation.
_Avoid_: snapshot, projection

**Agent event stream**:
The experimental adapter that translates one conversation's committed publications into events
shaped like the coding agent's session events.
_Avoid_: audit log, replay

**Task graph**:
The Session-wide mount of every live task, its waits, and the conversations it owns.
_Avoid_: task tree, scheduler state

**Sleep**:
A task state that holds no invocation until the Harness clock reaches its `until`, then resumes at
its checkpoint. A phase ends asleep; nothing waits for time inside a phase.
_Avoid_: wait, delay, timeout

**Timer**:
A host-armed callback through `HarnessOptions.set_timer`. A foreground timer paces or resumes work
and must fire; a background timer only frees memory and may be declined.
_Avoid_: sleep, scheduler tick

**Wake**:
`Harness.wake()`'s answer to when the Session next needs a process: `active` while some task can
run, and `at` for the earliest sleep not yet due. `on_wake` replaces the built-in timer for it.
_Avoid_: heartbeat, poll

**Restart**:
A cut-off of a running task by a crash, eviction, or reset, counted in `TaskRecord.restarts` at
open. A close is not a restart; committing a new state clears the count.
_Avoid_: retry, rerun

**Report**:
A failure that does not fail the calling operation, or the Session's own failure, delivered to
`on_report(report)` as one `Report`: the fields of a `ReportInfo` tagged by `kind` — what happened,
from `storage_failed` to `listener_error` — beside the `error` itself.
_Avoid_: log, error event, notification, report info argument

**Read-only Harness**:
A Harness opened with `HarnessOptions.read_only`: a viewer of a session another process drives, such
as a synced copy on another device. It loads live tasks as committed, `running` ones included, and
schedules nothing; a commit with writes, and every call that would run tasks, is refused with
`ReadOnlySession`, while reads and a commit that writes nothing keep working. Recovery is left to the
next writable open.
_Avoid_: replica, mirror, offline session

**Commit batch**:
One group of writes a storage stores as one atomic commit with its own sequence. One
`Storage.commit_batches()` call stores a group of them in order, each checked against the state the
earlier ones left, and stops at the first batch its own rules refuse: those before it stand, it and
every batch after it do not.
_Avoid_: transaction, chunk, group of writes

**Durability receipt**:
The `receive(index, seq)` call a storage makes once per stored batch, in order, after all of them
are durable and before the next call settles. It says the batch is stored, not that it is
published; the Session publishes later.
_Avoid_: publication, acknowledgement, commit callback

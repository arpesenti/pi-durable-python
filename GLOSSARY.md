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

**Group commit**:
The invariant of the mutation line: the Session admits commits one at a time, a commit's logical admission — its
write keys joining the unpublished write set — happens on the line, and its storage call runs off it. A job on the
line therefore never reads a state older than what the line admitted before it, outcomes are delivered in line order,
and nothing is published before it is durable. Several commits sharing one storage call is a consequence, never the
point; a line that publishes before durability, or delivers an outcome out of line order, is wrong rather than slow.
_Avoid_: batching, coalescing, write aggregation, flush

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

**Observer**:
One host's synchronous mirror of committed state — a document, a conversation view, the task graph,
or every stored commit — attached atomically on the Session's mutation line and handed the very
`WatchCall` a watch of the same address receives, each change unfolded and in line order rather than
folded into a coalesced delivery. `advance(call, context)` runs as part of the commit that made the
change, so a host that awaits its own commit finds the observer already advanced; `end(end)` says why
it stopped. A state observer ends `ended` only when the owner it observes ended — the task of a task
document, after that document's retirement — and `failed` when its Session closed, carrying the error
that failed it or a session-closed error. A commit observer mirrors `CommitPublication`s instead:
exactly what Storage made durable, and nothing an earlier advance detached away. `detach()` is
idempotent and nothing reaches the observer after it.
_Avoid_: watch (that is the coalesced, stoppable face of the same value), subscription, polling loop

**Observation**:
What an attachment hands back: the value as it stood at the moment the observer was admitted on the
line, and the idempotent `detach()` that ends the host's interest in it. A detached observation leaves
no pin behind, and an observation of an address whose owner ended is handed `None` next time.
_Avoid_: handle, snapshot, cursor

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
The value one consumer of one conversation sees: that conversation plus its own entry window and
the built-in documents, mounted structurally so an unchanged part is the same object between
revisions. It advances only from committed publications, and the conversation keeps one mount that
every consumer shares.
_Avoid_: snapshot, projection, transcript

**Entry window**:
The range of recent entries a conversation view carries: at most `max` of them, at least `min` while
they exist, starting at `from` when one is given, with `before` naming the entry just beneath the
first one. Each attached consumer gets its own window over the shared mount; bounds are validated
when the consumer attaches, not per commit. The whole transcript is never mounted: a client pages
past a window with `ConversationTranscript`.
_Avoid_: slice, page, buffer

**Task graph**:
The Session-wide mount of every live task, its waits, and the conversations it owns.
_Avoid_: task tree, scheduler state

**Sleep**:
A task state that holds no invocation until the Harness clock reaches its `until`, then resumes at
its checkpoint. A phase ends asleep; nothing waits for time inside a phase.
_Avoid_: wait, delay, timeout

**Timer**:
A host-armed callback through `HarnessOptions.set_timer`. A foreground timer paces or resumes work
and must fire; a background timer only frees memory and may be declined. Two uses are foreground:
the scheduler's timer for the earliest sleep not yet due, and the Session's timer for the earliest
persist-within deadline of its held changes. Neither wakes the other: a deadline stores progress, it
does not make a task runnable.
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

**Deferred commit**:
A commit whose changes memory adopts at once and whose storage is held back to a later one. Only a
commit that touches nothing but a latest, initial-fork document of the loaded history is eligible: a
generation partial and running tool progress. Its own followers -- watches, views, document
observers -- are shown each revision as it is adopted; the host's `observe_commits()` observer sees
it only when its held changes are stored, together with whatever stored them.
_Avoid_: background commit, lazy write, delayed commit

**Persist-within deadline**:
The time by which a deferred commit's held changes are stored, `settings.progress.persist_within_ms`
or the commit's own `within_ms`. `0` stores them right after the commit; a larger number waits for
whichever comes first, another trigger -- a commit reading or writing the document, a drop of its
tracker, a version reload, or close -- or the deadline, which one foreground timer keeps for the
earliest of them. `Infinity` waits for another trigger alone and arms no timer.
_Avoid_: flush interval, write-behind window, timeout

**Durability receipt**:
The `receive(index, seq)` call a storage makes once per stored batch, in order, after all of them
are durable and before the next call settles. It says the batch is stored, not that it is
published; the Session publishes later.
_Avoid_: publication, acknowledgement, commit callback

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
A durable admission of user input or a passive entry write.
It is `queued` in the inbox, `placed` as an entry, then `done` or `unanswered`.
_Avoid_: message, job

**Inbox**:
The built-in `pi.inbox` conversation document holding queued submissions in submission-ID order.
_Avoid_: queue, buffer

**Boundary**:
A point in a run (`postTools` or `final`) where queued submissions are selected and placed.
_Avoid_: flush, drain, checkpoint

**Run**:
The `pi.generation` task named by `pi.live.run` that owns the placed input submissions it must answer.
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

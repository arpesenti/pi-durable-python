# pi-durable-python

A Python port of `@earendil-works/pi-durable` v1.3.0: durable conversations, tasks, and documents on
`asyncio`, held to the upstream semantics and persisted record shapes.

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

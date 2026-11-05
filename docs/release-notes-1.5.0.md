# pi-durable-python 1.5.0

A mechanical Python translation of
[`@earendil-works/pi-durable`](https://github.com/earendil-works/pi/tree/v1.5.0/packages/durable)
**v1.5.0** (tag `v1.5.0`, commit `f9a26365568127461c47a07a37dcb42c140a3ac5`, pinned in
[`docs/upstream/README.md`](./upstream/README.md)).

```console
pip install pi-durable-python
pip install "pi-durable-python[openai]"    # or [anthropic], [schema], [all]
```

These notes carry the breaking changes as the sync's tickets land; the audit assembles the complete
set from the merged ticket receipts.

## Breaking changes for a host

### Storage: one commit shape, and a migration for a custom `Storage`

`Storage` has exactly one commit call:

```python
async def commit_batches(
    self,
    batches: Sequence[Sequence[StorageWrite]],
    receive: CommitReceipt,          # receive(index, seq), once per stored batch
    context: Context,
) -> CommitResult:                   # {"stored": int, "refused": BaseException?}
    ...
```

The single-write form this port shipped at 1.4.0 — `async def commit(self, writes, context) -> Seq`
— is **gone** from the protocol and from the memory, SQLite and JSONL backends. A class that
implements it no longer satisfies `Storage`: the Session, the conformance suite and every backend
call the batched one. Each batch is one atomic commit with its own sequence, checked against the
state the batches before it left; the first batch Durable's own rules refuse (global IDs, document
actions, copy sources) is not stored, nor is anything after it, while the batches before it are.

The two failure shapes stay two, and a custom storage must tell them apart:

- **A refusal is a resolved outcome.** The call settles with `{"stored": n, "refused": error}`; the
  `stored` batches are durable, they were received, and the storage goes on serving commits.
- **A database or I/O error rejects the call** and stores nothing the backend could roll back. A
  failed database call *during a check* is not a refusal: raise it, do not report it as `refused`.

`receive(index, seq)` runs once per stored batch, in order, with increasing sequences, once every
one of them is durable — after `COMMIT`, after the marker append — and before the call settles.

```python
# Before (1.4.0)
class CustomStorage:
    async def commit(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        seq = await self._db.transaction(lambda tx: apply(tx, check(tx, writes)))
        return seq                    # the Session adopted the sequence itself


# After (1.5.0)
class CustomStorage:
    async def commit_batches(
        self,
        batches: Sequence[Sequence[StorageWrite]],
        receive: CommitReceipt,
        context: Context,
    ) -> CommitResult:
        refused: BaseException | None = None
        seqs: list[Seq] = []

        async def run(tx: Any) -> None:
            nonlocal refused
            for writes in batches:
                try:
                    checked = check(tx, writes)   # only Durable's own checks; a database error
                    ...                           # here must reject the call, not refuse the batch
                except Exception as error:
                    refused = error               # a refusal: keep the batches before it
                    break
                seqs.append(apply(tx, checked))   # a throw rejects the call and rolls back all of it

        await self._db.transaction(run)           # COMMIT returned: what ran is durable
        for index, seq in enumerate(seqs):
            receive(index, seq)
        if refused is not None:
            return {"stored": len(seqs), "refused": refused}
        return {"stored": len(seqs)}
```

A wrapper or decorator around a storage forwards `commit_batches()` — and, if the wrapped storage
carries one, the `publishing()` scope of the companion `PublishingStorage` protocol
(`pi_durable.types`), which the Session asks for with `isinstance` rather than requiring it.

Code that called the retired form directly — above all a test seeding a storage — calls
`pi_durable.testing.commit_writes` instead: it hands one batch to `commit_batches()`, resolves with
that batch's sequence, and raises the refusal the storage reported, so a seed the storage refuses
raises where its caller expects it to. Every seeding path in the shared conformance suite and in
this port's suites goes through it; a hand-built storage double, including a reopening provider,
implements `commit_batches()` and nothing else.

The bundled backends need no work from their users: `MemoryStorage`, `SqliteStorage` and
`JsonlStorage` implement the one call, and the storage conformance suite in `pi_durable.testing`
holds each of them — fresh and across reopen — to the group stored in order, the receipts and the
refusal.

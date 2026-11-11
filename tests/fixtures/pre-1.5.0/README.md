# Pre-upgrade fixtures (`pre-1.5.0/`)

Databases written by this port's **previous release**, checked in so `test_harness_live_migrations.py`
can prove the migration promise: a database written by v1.4.0 opens under 1.5.0 and its `pi.live`
document migrates in place. These files are the previous release's bytes — nothing here is
hand-written to look like an old record.

## Provenance

Generated on 2026-11-05 from the `v1.4.0` tag (`3f3418f`, "Draft 1.4.0 release notes (#29)") in a
detached worktree of this repository, using that tag's own sources and no code from this branch:

```bash
git worktree add -f --detach /tmp/pdp-v140 v1.4.0
# the generator below, copied into the v1.4.0 worktree
PYTHONPATH=/tmp/pdp-v140/src python /tmp/pdp-v140/make_pre_upgrade_fixture.py sqlite \
  tests/fixtures/pre-1.5.0/sqlite/storage.sqlite3
PYTHONPATH=/tmp/pdp-v140/src python /tmp/pdp-v140/make_pre_upgrade_fixture.py jsonl \
  tests/fixtures/pre-1.5.0/jsonl
```

Only v1.4.0 writers touch the stored state:

- `pi_durable.harness.live.start_run` writes the run — so it has **no turn count**, the exact shape a
  run in flight across the upgrade carries (#11043);
- `pi_durable.harness.live.add_compaction_status` writes each status, and
  `pi_durable.harness.live.fail_compaction_status` writes the failure — so each names its trigger
  **`reason`**, the v1 spelling;
- `pi_durable.harness.generation.GenerationTask` (version 1) owns the run, and
  `pi_durable.harness.tool.ToolTask` (version 2) holds the round's one model-issued call;
- the SQLite schema (version 2) and the JSONL format (version 1) are the ones v1.4.0 created.

The only member set directly is the running tool slot: the writer that sets it is a private phase
handler of the generation task.

## What the fixtures hold

One root conversation with a run in flight: `run` (task 5, submission 2, no turn count), one running
model-issued slot for call `c1` on tool task 6, one compaction status (`901`, `threshold`, blocking,
attempt 2) and one compaction failure (`902`, `manual`, "no model") — both spelling the trigger
`reason`.

| Path | Backend | Written by |
| --- | --- | --- |
| `sqlite/storage.sqlite3` | SQLite, `durable_schema.version = 2` | `open_posix_sqlite_storage` + `SessionImpl` |
| `jsonl/*.jsonl` | JSONL, `"format": 1` | `JsonlStorage.open` + `SessionImpl` |

## Generator

`make_pre_upgrade_fixture.py`, kept here as the record of what produced these bytes:

```python
async def _write(tx):
    root = await tx.create_root_conversation()
    conversation_id = root["id"]
    submission = await tx.create_submission(
        {"type": "input", "status": "queued", "conversationId": conversation_id}
    )
    assistant = await tx.append_entry(
        AssistantEntry,
        conversation_id,
        {"model": [{
            "role": "assistant",
            "content": [{"type": "toolCall", "id": "c1", "name": "echo", "arguments": {}}],
            "api": "faux", "provider": "faux", "model": "faux-1",
            "usage": {}, "stopReason": "toolUse", "timestamp": 1_700_000_000_000,
        }]},
    )
    live = await tx.doc(LiveDoc, conversation_id)
    await start_run(tx, conversation_id, live, [submission["id"]], GenerationTask)
    run = live["run"]
    tool_task = await tx.create_task(
        ToolTask,
        {"kind": "model", "assistant": assistant["id"], "callId": "c1"},
        {"ownership": {"kind": "task", "taskId": run["taskId"]}},
    )
    live["tools"] = [
        {"callId": "c1", "name": "echo", "taskId": tool_task, "status": "running", "output": "echo"}
    ]
    add_compaction_status(live, {"taskId": 901, "reason": "threshold", "blocking": True, "attempt": 2})
    add_compaction_status(live, {"taskId": 902, "reason": "manual", "blocking": False, "attempt": 1})
    fail_compaction_status(live, 902, "no model")
    return conversation_id
```

Opened, written, and closed by `SessionImpl(storage, lambda: 1_700_000_000_000, lambda: 600_000)` in
one commit per backend, then closed — no fixture is edited after the previous release wrote it.

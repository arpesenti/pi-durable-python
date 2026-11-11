"""Translated v1.5.0 ``harness-hook-reads`` cases: raw history read from inside a task.

Upstream ``test/harness-hook-reads.test.ts`` holds two cases.

Translated here: "pages a task's conversation history by entry ID range" (:50-93, upstream #11141),
the case that exercises ``TaskRuntime.entries()`` — the committed raw history of the task's own
conversation, fork-aware, as ``Conversation.entries()`` pages it (upstream ``src/types.ts:327-337``,
``src/harness/scheduler.ts:2828-2834``). Upstream leaves that read's line discipline to ``readOnLine``
in its own implementation and states it in ``TaskRuntime.commit``'s docstring (``src/types.ts:286``:
inside the callback read only through ``tx``, because the other runtime reads queue on the line the
callback holds), so the second case below asserts the discipline instead of inferring it from the call.

The other case, "reads raw history and single entries from a hook" (:17-48), was deferred to
#36 and #48, which own ``HookApi.entry()``/``entries()`` (``PARITY.md`` §7.2); both have landed,
and the audit #52 translates it here as ``test_reads_raw_history_and_single_entries_from_a_hook``.
Where it differs from upstream: the "another kind" read passes a second entry definition
(``OtherEntry``) in place of upstream's ``GenerationTask as never`` -- a task definition standing in
for a token of another kind -- because the port's ``entry()`` takes an entry definition's ``kind``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from chat_support import all_entries, assistant_message, chat_setup, faux_text, open_chat
from session_support import ControlledStorage, context, flush
from task_support import aborted_with, completed, deferred, eventually

from pi_durable import (
    GenerationTask,
    MemoryStorage,
    UserEntry,
    define_entry,
    define_extension,
    define_task,
    hook,
)


async def _abort(_task: Any, runtime: Any, ctx: Any) -> None:
    await runtime.commit(lambda: aborted_with("test"), ctx)


async def test_pages_a_task_conversation_history_by_entry_id_range() -> None:
    """Upstream ``test/harness-hook-reads.test.ts:50-93``: three pages of the task's conversation."""

    setup = chat_setup()
    pages: list[list[str]] = []

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        first = await runtime.entries({"order": "ascending"}, 1, None, ctx)
        pages.append([entry["id"] for entry in first["items"]])
        rest = await runtime.entries({"order": "ascending"}, 1, first["next"], ctx)
        pages.append([entry["id"] for entry in rest["items"]])
        bounded = await runtime.entries(
            {"minEntryId": rest["items"][0]["id"], "maxEntryId": rest["items"][0]["id"]},
            10,
            None,
            ctx,
        )
        pages.append([entry["id"] for entry in bounded["items"]])
        await runtime.commit(lambda: completed(None), ctx)

    Reader = define_task(
        {
            "name": "test.reader",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": _abort,
        }
    )
    setup.registry.install(define_extension({"name": "reader", "tasks": [Reader]}))
    setup.faux.set_responses([assistant_message([faux_text("done")])])
    opened = await open_chat(ControlledStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    harness.resume()
    id = await root.commit(
        lambda tx: tx.create_task(Reader, None, {"ownership": {"kind": "conversation"}}), context
    )
    await harness.wait_for_task(id, context)
    entries = await all_entries(root)
    assert [entry["kind"] for entry in entries] == ["pi.user", "pi.assistant"]
    # One entry per page: the first ascending page, the cursor's continuation, and the range alone.
    assert pages == [[entries[0]["id"]], [entries[1]["id"]], [entries[1]["id"]]]
    await harness.close(context)


async def test_a_task_entries_read_waits_for_the_commit_holding_the_line() -> None:
    """Port case for the same member: the read is on the line, not a scan of storage.

    A commit whose storage call is held has admitted its writes and not stored them. A read that
    reached ``Storage.scan_entries()`` directly would settle over that in-flight commit and hand
    back a history missing an entry the Session had already admitted; the read taken here queues on
    the line and settles only once the commit is stored, so it never reads ahead of publication.
    """

    storage = ControlledStorage()
    setup = chat_setup()
    entered = deferred()
    read = deferred()
    started = deferred()
    history: dict[str, Any] = {}

    async def run(_task: Any, runtime: Any, ctx: Any) -> None:
        started.resolve()
        await entered.promise
        page = await runtime.entries({}, 100, None, ctx)
        history["kinds"] = [entry["kind"] for entry in page["items"]]
        read.resolve()
        await runtime.commit(lambda: completed(None), ctx)

    Reader = define_task(
        {
            "name": "test.reader",
            "version": 1,
            "initial": lambda _input: {"phase": "run"},
            "phases": {"run": run},
            "abort": _abort,
        }
    )
    setup.registry.install(define_extension({"name": "reader", "tasks": [Reader]}))
    setup.faux.set_responses([assistant_message([faux_text("done")])])
    opened = await open_chat(storage, setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    harness.resume()
    id = await root.commit(
        lambda tx: tx.create_task(Reader, None, {"ownership": {"kind": "conversation"}}), context
    )
    await started.promise
    # The phase is parked before its read, so the only work queued on the line is the held commit.
    held = storage.hold_commits()

    def note_admitted() -> bool:
        return any(
            write["type"] == "entry" and write["value"]["kind"] == "note"
            for batch in storage.admitted_commits
            for write in batch
        )

    note = asyncio.ensure_future(
        root.commit(lambda tx: tx.append_entry(root.id, {"kind": "note"}), context)
    )
    # Admitted, and nothing of it stored: the storage call is the one the gate holds.
    await eventually(note_admitted)
    entered.resolve()
    for _ in range(5):
        await flush()
    # Still pending: the read is queued behind the commit that holds the line, not served by Storage.
    assert not read.promise.done()
    assert history == {}
    held.release()
    await note
    await read.promise
    await harness.wait_for_task(id, context)
    # What it settled with is the committed history, the admitted note among it, newest first as an
    # unordered scan asks for.
    assert history["kinds"] == ["note", "pi.assistant", "pi.user"]
    await harness.close(context)


async def test_reads_raw_history_and_single_entries_from_a_hook() -> None:
    """Upstream ``test/harness-hook-reads.test.ts:17-48``: the hook asks the api for the history and a
    single entry."""

    setup = chat_setup()
    seen: list[dict[str, Any]] = []
    other: Any = define_entry("test.other")

    async def on_yield(_answer: Any, api: Any, ctx: Any) -> None:
        page = await api.entries({"order": "ascending"}, 10, None, ctx)
        first = page["items"][0]
        user = await api.entry(UserEntry, first["id"], ctx)
        seen.append(
            {
                "kinds": [entry["kind"] for entry in page["items"]],
                **({} if user is None else {"user": user["kind"]}),
            }
        )
        # Another kind: None.
        assert await api.entry(other, first["id"], ctx) is None

    setup.registry.install(
        define_extension(
            {"name": "reader", "hooks": [hook(GenerationTask, {"onYield": on_yield})]}
        )
    )
    setup.faux.set_responses([assistant_message("done")])
    opened = await open_chat(MemoryStorage(), setup)
    harness, root = opened["harness"], opened["root"]
    await (await root.submit({"type": "input", "content": "go"}, context)).wait(context)
    # The answer is not committed yet when ``onYield`` asks.
    assert seen == [{"kinds": ["pi.user"], "user": "pi.user"}]
    await harness.close(context)

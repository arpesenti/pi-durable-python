"""Session kernel: one mutation line, document tracking, and committed publication.

Only committed state is observable. Every commit callback, preparation, storage
settlement, adoption, and publication enqueue runs while the line is held; listeners run
later. A storage error is fatal: the Session fails, publishes nothing further, and must be
reopened from storage.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, TypeVar

from .delta import Tracker
from .documents import (
    DocToken,
    LoadedDocument,
    address_id,
    check_record_scope,
    check_record_version,
    materialize_document,
    materialize_document_value,
    resolve_address,
)
from .errors import SessionClosed, SessionFailed
from .json import copy_json
from .state import CommittedWatch, DocumentState
from .storage.base import Storage
from .transaction import Transaction, TransactionHost
from .types import CommitPublication, StorageWrite, TransactionScope

T = TypeVar("T")

DEFAULT_DOCUMENT_RETENTION_MS = 600_000


class Session(TransactionHost):
    """Owner of one mutation line, its records, and its tracked documents."""

    def __init__(
        self,
        storage: Storage,
        *,
        now: Callable[[], int] | None = None,
        document_retention_ms: int = DEFAULT_DOCUMENT_RETENTION_MS,
    ) -> None:
        self._storage_backend = storage
        self._clock = now or (lambda: int(time.time() * 1000))
        self._retention_ms = document_retention_ms
        self._line = asyncio.Lock()
        self._documents: dict[str, LoadedDocument] = {}
        self._idle_documents: dict[str, int] = {}
        self._attached: dict[int, int] = {}
        self._observers: dict[int, list[Any]] = {}
        self._commit_listeners: list[Callable[[CommitPublication], None]] = []
        self._internal_listeners: list[Callable[[CommitPublication], None]] = []
        self._close_listeners: list[Callable[[], None]] = []
        self._accepting = True
        self._closing: asyncio.Task[None] | None = None
        self._failure: BaseException | None = None
        self._closed: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._closed.add_done_callback(lambda future: future.exception())

    # ── Public surface ─────────────────────────────────────────────────────

    @property
    def storage(self) -> Storage:
        return self._storage_backend

    @property
    def closed(self) -> Awaitable[dict[str, Any]]:
        """Settles once the Session closed, Storage included."""
        return self._closed

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    async def commit(self, change: Callable[[Transaction], Any], context: Any = None) -> Any:
        """Run one atomic transaction on the Session mutation line."""
        return await self.commit_with(change)

    async def commit_with(
        self,
        change: Callable[[Transaction], Any],
        scope: TransactionScope | None = None,
        context: Any = None,
    ) -> Any:
        self._assert_usable()
        return await self._enqueue(lambda: self._run_commit(change, scope))

    async def read_on_line(self, job: Callable[[], Awaitable[T]]) -> T:
        self._assert_usable()
        return await self._enqueue(job)

    def subscribe_commits(self, listener: Callable[[CommitPublication], None]) -> Callable[[], None]:
        self._assert_usable()
        self._commit_listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._commit_listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def observe_commits(self, listener: Callable[[CommitPublication], None]) -> Callable[[], None]:
        """Internal listeners keep Session-owned state in step with storage; a throw fails the Session."""
        self._assert_usable()
        self._internal_listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._internal_listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def subscribe_close(self, listener: Callable[[], None]) -> Callable[[], None]:
        self._assert_usable()
        self._close_listeners.append(listener)

        def unsubscribe() -> None:
            try:
                self._close_listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def now(self) -> int:
        return self._clock()

    # ── Documents ──────────────────────────────────────────────────────────

    async def snapshot(self, token: DocToken, *args: Any, context: Any = None) -> Any:
        self._assert_usable()
        resolved = resolve_address(token.definition, args)

        async def load() -> Any:
            document = await self.load_document(token.definition, resolved.id, resolved.address)
            if document is None:
                return None
            check_record_scope(token.definition, document.record)
            check_record_version(token.definition, document.record, document.stored_version)
            return copy_json(document.tracker.value)

        return await self._enqueue(load)

    async def document_state(self, token: DocToken, *args: Any, context: Any = None) -> DocumentState | None:
        self._assert_usable()
        resolved = resolve_address(token.definition, args)

        async def attach() -> DocumentState | None:
            document = await self.load_document(token.definition, resolved.id, resolved.address)
            if document is None:
                return None
            detach = self._pin(resolved.id, document.record["id"])
            state = DocumentState(copy_json(document.tracker.value), detach, lambda error: self.report(error))
            self._observers.setdefault(document.record["id"], []).append(state)
            return state

        return await self._enqueue(attach)

    async def watch_doc(self, token: DocToken, *args: Any, context: Any = None) -> CommittedWatch | None:
        self._assert_usable()
        resolved = resolve_address(token.definition, args)

        async def attach() -> CommittedWatch | None:
            document = await self.load_document(token.definition, resolved.id, resolved.address)
            if document is None:
                return None
            detach = self._pin(resolved.id, document.record["id"])
            watch = CommittedWatch(copy_json(document.tracker.value), detach, lambda error: self.report(error))
            self._observers.setdefault(document.record["id"], []).append(watch)
            return watch

        return await self._enqueue(attach)

    async def snapshot_as_of(
        self, token: DocToken, conversation_id: int, *args: Any, context: Any = None
    ) -> Any:
        """Read a rewindable conversation document as of one visible entry.

        Singleton: ``snapshot_as_of(Token, conversation_id, entry_id)``.
        Family: ``snapshot_as_of(Token, conversation_id, key, entry_id)``.
        """
        self._assert_usable()
        definition = token.definition
        raw = (conversation_id, *args)
        resolved = resolve_address(definition, raw)
        if resolved.address["scope"]["kind"] != "conversation":
            raise TypeError("snapshot_as_of() requires a conversation document")
        if resolved.next_argument >= len(raw):
            raise TypeError("snapshot_as_of() requires an entry ID")
        at = raw[resolved.next_argument]
        if not isinstance(at, int) or isinstance(at, bool):
            raise TypeError("snapshot_as_of() requires an entry ID")

        async def read() -> Any:
            stored = await self._storage_call(self._storage_backend.entry(conversation_id, at))
            if stored is None:
                raise ValueError(f"Entry {at} is not visible from conversation {conversation_id}")
            address = {
                **resolved.address,
                "scope": {
                    "kind": "conversation",
                    "conversationId": stored["entry"]["conversationId"],
                },
            }
            record = await self._storage_call(self._storage_backend.find_document(address, stored["commitSeq"]))
            if record is None:
                return None
            document = await self._storage_call(self._storage_backend.document(record["id"], stored["commitSeq"]))
            if document is None:
                raise ValueError(f"Historical document {record['id']} ({record['kind']}) cannot be read")
            return materialize_document(definition, document)

        return await self._enqueue(read)

    # ── TransactionHost ────────────────────────────────────────────────────

    async def load_document(
        self, definition: Any, address_id_value: str, address: Mapping[str, Any]
    ) -> LoadedDocument | None:
        cached = self._documents.get(address_id_value)
        if cached is not None:
            self._touch(address_id_value)
            if cached.value_version == definition.version:
                return cached
            # A newer definition was installed in this process. Migrate the cached value in
            # place so attached observers keep their tracker.
            value = materialize_document_value(
                definition, cached.record, cached.stored_version, cached.tracker.value
            )
            cached.tracker.value = value
            cached.value_version = definition.version
            return cached
        record = await self._storage_call(self._storage_backend.find_document(address, "current"))
        if record is None:
            return None
        stored = await self._storage_call(self._storage_backend.document(record["id"], "current"))
        if stored is None:
            raise ValueError(f"Current document {record['id']} cannot be read")
        value = materialize_document(definition, stored)
        loaded = LoadedDocument(
            address_id=address_id_value,
            record=copy_json(stored["record"]),
            stored_version=stored["version"],
            value_version=definition.version,
            deltas_since_base=stored["deltasSinceBase"],
            tracker=Tracker(value),
        )
        self.install_document(loaded)
        return loaded

    def cached_document(self, address_id_value: str) -> LoadedDocument | None:
        cached = self._documents.get(address_id_value)
        if cached is not None:
            self._touch(address_id_value)
        return cached

    def install_document(self, document: LoadedDocument) -> None:
        self._documents[document.address_id] = document
        self._touch(document.address_id)

    def evict_document(self, address_id_value: str, record_id: int) -> None:
        cached = self._documents.get(address_id_value)
        if cached is not None and cached.record["id"] == record_id:
            self._documents.pop(address_id_value, None)
            self._idle_documents.pop(address_id_value, None)

    async def conversation_created(self, tx: Transaction, record: Mapping[str, Any]) -> None:
        """A plain Session stages no built-in documents."""

    # ── Close / failure ────────────────────────────────────────────────────

    async def close(self, context: Any = None) -> None:
        if self._closing is None:
            self._accepting = False
            self._notify_close()
            self._closing = asyncio.ensure_future(self._shutdown())
        await self._closing

    def _fail(self, error: BaseException) -> None:
        if self._failure is not None:
            return
        self._failure = error
        self._accepting = False
        self._notify_close()
        self.report(error, {"type": "session_failed"})
        if self._closing is None:
            self._closing = asyncio.ensure_future(self._shutdown())

    async def _shutdown(self) -> None:
        try:
            await self._before_close()
        except BaseException as error:  # noqa: BLE001 - close must finish
            if self._failure is None:
                self._failure = error
        async with self._line:
            self._commit_listeners.clear()
            self._internal_listeners.clear()
            self._documents.clear()
            self._idle_documents.clear()
            self._attached.clear()
            self._close_observers(self._failure)
            self._observers.clear()
            try:
                await self._storage_backend.close()
            except BaseException as error:  # noqa: BLE001 - close must finish
                if self._failure is None:
                    self._failure = error
                    self.report(error, {"type": "session_failed"})
        if not self._closed.done():
            self._closed.set_result(
                {"reason": "closed"} if self._failure is None else {"reason": "failed", "error": self._failure}
            )

    async def _before_close(self) -> None:
        """Runs after admission seals and before the line closes storage."""

    async def write_at_close(self, provider: Callable[[], Sequence[StorageWrite]]) -> None:
        """Persist Session-owned cleanup writes without publishing them."""
        self._assert_healthy()

        async def run() -> None:
            if self._failure is not None:
                return
            batch = list(provider())
            if batch:
                await self._storage_call(self._storage_backend.commit(batch))

        await self._enqueue(run)

    def fail(self, error: BaseException) -> None:
        """Fail the Session with the first error (internal)."""
        self._fail(error)

    def _notify_close(self) -> None:
        listeners = list(self._close_listeners)
        self._close_listeners.clear()
        for listener in listeners:
            try:
                listener()
            except BaseException as error:  # noqa: BLE001 - listener containment
                self.report(error)

    def _close_observers(self, failure: BaseException | None) -> None:
        for observers in self._observers.values():
            for observer in observers:
                if isinstance(observer, CommittedWatch):
                    observer.close_session(failure)
                else:
                    observer.close()

    # ── Commit pipeline ────────────────────────────────────────────────────

    async def _enqueue(self, job: Callable[[], Awaitable[T]]) -> T:
        async with self._line:
            self._assert_healthy()
            return await job()

    async def _run_commit(self, change: Callable[[Transaction], Any], scope: TransactionScope | None) -> Any:
        self._assert_healthy()
        tx = Transaction(self, scope)
        try:
            result = change(tx)
            if inspect.isawaitable(result):
                result = await result
            self._assert_healthy()
        except BaseException:
            await tx.settle_failure()
            self._sweep_documents()
            raise
        try:
            writes = await tx.settle_success()
        except BaseException:
            self._sweep_documents()
            raise
        if not writes:
            tx.discard()
            self._sweep_documents()
            return result
        try:
            seq = await self._storage_call(self._storage_backend.commit(writes))
        except BaseException:
            tx.discard()
            raise
        try:
            publications = tx.adopt(seq)
        except BaseException as error:
            self._fail(error)
            raise
        for publication in publications:
            record = publication.get("record")
            if isinstance(record, dict) and "scope" in record:
                self._touch(address_id(record))
        self._publish(seq, writes, publications)
        self._sweep_documents()
        return result

    def _publish(
        self,
        seq: int,
        writes: Sequence[StorageWrite],
        documents: Sequence[Mapping[str, Any]],
    ) -> None:
        needs_changes = bool(self._internal_listeners or self._commit_listeners)
        changes: list[dict[str, Any]] = []
        if needs_changes:
            for write in writes:
                if write["type"] in ("conversation", "entry", "task", "submission"):
                    changes.append(write)
            changes.extend(dict(change) for change in documents)
        publication = {"seq": seq, "changes": changes}
        for listener in list(self._internal_listeners):
            try:
                listener(publication)
            except BaseException as error:
                self._fail(error)
                raise
        for listener in list(self._commit_listeners):
            try:
                listener(publication)
            except BaseException as error:  # noqa: BLE001 - listener containment
                self.report(error, {"type": "listener_error"})
        for document in documents:
            record_id = document["record"]["id"]
            observers = list(self._observers.get(record_id, []))
            if not observers:
                continue
            if document["type"] == "document.copy":
                continue
            if document.get("value") is None:
                for observer in observers:
                    observer.advance(None, document.get("ops") or [["r", None]])
                self._observers.pop(record_id, None)
            else:
                for observer in observers:
                    observer.advance(document["value"], document.get("ops") or [])

    # ── Document cache ─────────────────────────────────────────────────────

    def _pin(self, address_id_value: str, record_id: int) -> Callable[[], None]:
        self._attached[record_id] = self._attached.get(record_id, 0) + 1
        self._idle_documents.pop(address_id_value, None)
        released = False

        def release() -> None:
            nonlocal released
            if released:
                return
            released = True
            count = self._attached.get(record_id, 1) - 1
            if count > 0:
                self._attached[record_id] = count
                return
            self._attached.pop(record_id, None)
            cached = self._documents.get(address_id_value)
            if cached is not None and cached.record["id"] == record_id:
                self._idle_documents[address_id_value] = self._clock()

        return release

    def _touch(self, address_id_value: str) -> None:
        if not address_id_value:
            return
        cached = self._documents.get(address_id_value)
        if cached is None or cached.record["id"] in self._attached:
            return
        self._idle_documents[address_id_value] = self._clock()

    def _sweep_documents(self) -> None:
        if self._retention_ms < 0:
            return
        now = self._clock()
        for address_id_value, since in list(self._idle_documents.items()):
            if now - since < self._retention_ms:
                continue
            cached = self._documents.get(address_id_value)
            if cached is None or cached.record["id"] in self._attached:
                continue
            self._documents.pop(address_id_value, None)
            self._idle_documents.pop(address_id_value, None)

    # ── Health guards ──────────────────────────────────────────────────────

    async def _storage_call(self, operation: Awaitable[T]) -> T:
        try:
            return await operation
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._fail(error)
            raise

    def _assert_usable(self) -> None:
        if self._failure is not None:
            raise SessionFailed(self._failure) from self._failure
        if not self._accepting:
            raise SessionClosed("Session is closing")

    def _assert_healthy(self) -> None:
        if self._failure is not None:
            raise SessionFailed(self._failure) from self._failure
        if self._closed.done():
            raise SessionClosed("Session is closed")

    # ── Reporting (Harness overrides) ──────────────────────────────────────

    def report(self, error: BaseException, info: Mapping[str, Any] | None = None) -> None:
        """A plain Session drops reports."""


__all__ = ["DEFAULT_DOCUMENT_RETENTION_MS", "Session"]

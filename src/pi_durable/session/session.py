"""Session kernel: one mutation line, committed reads, and a bounded document cache."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

from ..context import BACKGROUND_CONTEXT, Context, without_abort_signal
from ..documents import (
    AnyDocToken,
    DocDefinition,
    check_record_scope,
    check_record_version,
    materialize_document,
    resolve_address,
)
from ..errors import SessionFailed, StorageRequestError
from ..ids import ConversationId, DocumentId, EntryId, Seq, TaskId
from ..json import JsonObject
from ..types import (
    CommitChange,
    CommitPublication,
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentCommitChange,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    EntryLookup,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Page,
    Storage,
    StorageWrite,
    StoredDocument,
    SubmissionQuery,
    SubmissionRecord,
    TaskQuery,
    TaskRecord,
)
from .observation import CommittedWatch, ObservedDocumentValue, observed_operations
from .transaction import LoadedDocument, Transaction, TransactionScope

_DEFAULT_DOCUMENT_RETENTION_MS = 600_000


def _document_retention(value: int | None) -> int:
    """Return a retention as given when valid, otherwise the default."""

    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return _DEFAULT_DOCUMENT_RETENTION_MS


def create_session(storage: Storage, options: dict[str, Any] | None = None, **kwargs: Any) -> SessionImpl:
    """Open one Session kernel over a storage backend."""

    values = dict(options or {})
    values.update(kwargs)
    now = values.get("now")
    retention = values.get("document_retention_ms", values.get("documentRetentionMs"))
    if retention is not None and callable(retention):
        retention = retention()
    return SessionImpl(
        storage,
        now if callable(now) else _now_ms,
        retention,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _validate_document(definition: DocDefinition, loaded: LoadedDocument) -> None:
    """Reject typed access whose token disagrees with the stored incarnation or cannot use it."""

    check_record_scope(definition, loaded.record)
    check_record_version(definition, loaded.record, loaded.stored_version)


@dataclass(slots=True)
class Delivery:
    """One state delivery: hydration at sequence 0, then one per committed update."""

    kind: str
    sequence: int


@dataclass(slots=True)
class _WatchAttachment:
    """One watch subscribed to a committed incarnation, and the incarnation it pins."""

    record_id: DocumentId
    watch: CommittedWatch


class DocumentState:
    """Disposable, read-only committed state bound to one document incarnation."""

    __slots__ = (
        "_disposed",
        "_listeners",
        "_observed_version",
        "_record_id",
        "_session",
        "_sequence",
        "_value",
    )

    def __init__(
        self,
        session: SessionImpl,
        record_id: DocumentId,
        value: JsonObject,
        observed_version: int,
    ) -> None:
        self._session = session
        self._record_id = record_id
        self._value: JsonObject | None = value
        self._observed_version = observed_version
        self._sequence = 0
        self._listeners: list[Callable[[Any, Context, Delivery], Any]] = []
        self._disposed = False

    @property
    def value(self) -> JsonObject | None:
        return self._value

    def subscribe(self, listener: Callable[[Any, Context, Delivery], Any]) -> Callable[[], None]:
        """Subscribe; hydration is delivered immediately with sequence 0."""

        if self._disposed:
            raise RuntimeError("Document state is disposed")
        self._listeners.append(listener)
        self._deliver(listener, self._value, BACKGROUND_CONTEXT, Delivery("hydrate", self._sequence))

        def unsubscribe() -> None:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return unsubscribe

    def dispose(self) -> None:
        """Idempotently stop future deliveries; the last value remains readable."""

        if self._disposed:
            return
        self._disposed = True
        self._listeners.clear()
        self._session._detach_state(self)

    def _advance(self, value: JsonObject | None, context: Context) -> None:
        if self._disposed:
            return
        self._value = value
        self._sequence += 1
        delivery = Delivery("update", self._sequence)
        for listener in list(self._listeners):
            self._deliver(listener, value, without_abort_signal(context), delivery)

    def _close_session(self) -> None:
        self._listeners.clear()

    @staticmethod
    def _deliver(
        listener: Callable[[Any, Context, Delivery], Any],
        value: JsonObject | None,
        context: Context,
        delivery: Delivery,
    ) -> None:
        try:
            result = listener(value, context, delivery)
            if hasattr(result, "__await__"):
                asyncio.ensure_future(cast(Awaitable[Any], result))
        except Exception:
            pass


class _SessionHost:
    def __init__(self, session: SessionImpl) -> None:
        self._session = session
        self.storage = cast(Storage, session._storage)  # noqa: SLF001

    def now(self) -> int:
        return self._session._clock()

    def cached(self, address_id: str) -> LoadedDocument | None:
        return self._session._cached(address_id)

    async def load(
        self,
        definition: DocDefinition,
        address_id: str,
        address: DocumentAddress,
        context: Context,
    ) -> LoadedDocument | None:
        return await self._session._load_document(definition, address_id, address, context)

    def install(self, document: LoadedDocument) -> None:
        self._session._install(document)

    def evict(self, address_id: str, record_id: DocumentId) -> None:
        self._session._evict(address_id, record_id)

    async def conversation_created(self, tx: Transaction, record: ConversationRecord) -> None:
        await self._session.conversation_created(tx, record)


class SessionImpl:
    """Session kernel over one guarded storage backend."""

    def __init__(
        self,
        storage: Storage,
        now: Callable[[], int] | None = None,
        document_retention_ms: int | Callable[[], int | None] | None = None,
    ) -> None:
        self._documents: dict[str, LoadedDocument] = {}
        self._states: list[DocumentState] = []
        self._watches: list[_WatchAttachment] = []
        self._commit_listeners: list[Callable[[CommitPublication, Context], Any]] = []
        self._close_listeners: list[Callable[[], Any]] = []
        self._line = asyncio.Lock()
        self._closing: asyncio.Future[None] | None = None
        self._failure: BaseException | None = None
        loop = _current_loop()
        self._failed: asyncio.Future[SessionFailed] = loop.create_future()
        self._closed: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._failed.add_done_callback(lambda future: future.exception())
        self._clock_fn = now or _now_ms
        self._retention_setting = document_retention_ms

        def clock() -> int:
            try:
                return self._clock_fn()
            except Exception:
                return _now_ms()

        self._clock = clock
        self._storage = GuardedStorage(storage, self)
        self._host = _SessionHost(self)

    # ── Lifecycle and commits ───────────────────────────────────────────────

    @property
    def closed(self) -> asyncio.Future[dict[str, Any]]:
        return self._closed

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    @property
    def failed(self) -> asyncio.Future[SessionFailed]:
        return self._failed

    def fail(self, error: BaseException) -> None:
        """Fail the Session once; the first error wins and close listeners run immediately."""

        if self._failure is not None:
            return
        self._failure = error
        if not self._failed.done():
            self._failed.set_exception(SessionFailed(error))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._close_after_failure())

    async def _close_after_failure(self) -> None:
        try:
            await self.close(BACKGROUND_CONTEXT)
        except Exception:
            pass

    def report(self, error: BaseException) -> None:
        """A plain Session has no external report sink."""

    async def commit(
        self,
        change: Callable[[Transaction], Any],
        context: Context,
    ) -> Any:
        return await self.commit_with(change, context)

    async def commit_with(
        self,
        change: Callable[[Transaction], Any],
        context: Context,
        scope: TransactionScope | None = None,
    ) -> Any:
        try:
            self.assert_usable()
        except Exception as error:
            raise error
        async with self._line:
            self._assert_healthy()
            signal = context.abort_signal
            if signal is not None:
                signal.raise_if_aborted()
            tx = Transaction(self._host, context, scope)
            try:
                result = await change(tx)
                self._assert_healthy()
            except BaseException:
                await tx.settle_failure()
                raise
            try:
                writes = await tx.settle_success()
            except BaseException:
                raise
            if not writes:
                tx.discard()
                self._sweep_documents()
                return result
            try:
                seq = await self._storage.commit(writes, without_abort_signal(context))
            except BaseException:
                tx.discard()
                raise
            try:
                documents = tx.adopt(seq)
            except BaseException as error:
                self.fail(error)
                raise
            self._publish(seq, writes, documents, context)
            self._sweep_documents()
            return result

    async def read_on_line(self, job: Callable[[], Awaitable[Any]]) -> Any:
        self.assert_usable()
        async with self._line:
            self._assert_healthy()
            return await job()

    def assert_usable(self) -> None:
        self._assert_healthy()
        if self._closing is not None:
            raise RuntimeError("Session is closed")

    def _assert_healthy(self) -> None:
        if self._failure is not None:
            raise SessionFailed(self._failure)

    async def close(self, context: Context) -> None:
        if self._closing is None:
            loop = asyncio.get_running_loop()
            closing: asyncio.Future[None] = loop.create_future()
            closing.add_done_callback(lambda future: future.exception())
            self._closing = closing
            self._notify_close()
            try:
                loop.create_task(self._run_close())
            except BaseException:
                self._closing = None
                raise
        await _await_with_context(self._closing, context)

    async def _run_close(self) -> None:
        assert self._closing is not None
        error: BaseException | None = None
        try:
            async with self._line:
                self._commit_listeners.clear()
                self._documents.clear()
                for state in list(self._states):
                    state._close_session()
                self._states.clear()
                try:
                    await self._storage.close(without_abort_signal(BACKGROUND_CONTEXT))
                except BaseException as close_error:
                    error = close_error
                    if self._failure is None:
                        self._failure = close_error
                        self.report(close_error)
                    raise
        except BaseException as caught:
            error = caught
        finally:
            if not self._closed.done():
                if self._failure is None:
                    self._closed.set_result({"reason": "closed"})
                else:
                    self._closed.set_result({"reason": "failed", "error": self._failure})
            if not self._closing.done():
                if error is None:
                    self._closing.set_result(None)
                else:
                    self._closing.set_exception(error)

    def _notify_close(self) -> None:
        listeners = self._close_listeners
        self._close_listeners = []
        for listener in listeners:
            try:
                listener()
            except Exception as error:
                self.report(error)
        watches = self._watches
        self._watches = []
        for entry in watches:
            entry.watch.close_session(self._failure)

    def subscribe_commits(
        self,
        listener: Callable[[CommitPublication, Context], Any],
    ) -> Callable[[], None]:
        self.assert_usable()
        self._commit_listeners.append(listener)
        return lambda: self._commit_listeners.remove(listener) if listener in self._commit_listeners else None

    def subscribe_close(self, listener: Callable[[], Any]) -> Callable[[], None]:
        self.assert_usable()
        self._close_listeners.append(listener)
        return lambda: self._close_listeners.remove(listener) if listener in self._close_listeners else None

    def _publish(
        self,
        seq: Seq,
        writes: list[StorageWrite],
        documents: list[DocumentCommitChange],
        context: Context,
    ) -> None:
        if not self._commit_listeners and not self._states and not self._watches:
            return
        changes: list[CommitChange] = [
            cast(CommitChange, write)
            for write in writes
            if write["type"] in ("conversation", "entry", "task", "submission")
        ]
        changes.extend(documents)
        publication: CommitPublication = {"seq": seq, "changes": changes}
        for state in list(self._states):
            for change in documents:
                if change["type"] != "document" or change["record"]["id"] != state._record_id:
                    continue
                ops, version = observed_operations(state._observed_version, change)
                # A migration-only base changes nothing for an observer of the new version.
                if not ops:
                    continue
                state._observed_version = version
                state._advance(change["value"], context)
                if change["value"] is None:
                    self._detach_state(state)
        for entry in list(self._watches):
            for change in documents:
                if change["type"] != "document" or change["record"]["id"] != entry.record_id:
                    continue
                entry.watch.advance(change, context)
        for listener in list(self._commit_listeners):
            try:
                result = listener(publication, context)
                if hasattr(result, "__await__"):
                    asyncio.ensure_future(cast(Awaitable[Any], result))
            except Exception as error:
                self.report(error)

    # ── Document cache ─────────────────────────────────────────────────────

    def _cached(self, address_id: str) -> LoadedDocument | None:
        return self._documents.get(address_id)

    def _install(self, document: LoadedDocument) -> None:
        self._documents[document.address_id] = document

    def _evict(self, address_id: str, record_id: DocumentId) -> None:
        cached = self._documents.get(address_id)
        if cached is not None and cached.record["id"] == record_id:
            del self._documents[address_id]

    async def _load_document(
        self,
        definition: DocDefinition,
        address_id: str,
        address: DocumentAddress,
        context: Context,
    ) -> LoadedDocument | None:
        cached = self._documents.get(address_id)
        if cached is not None and cached.value_version == definition.version:
            return cached
        if cached is not None:
            self._documents.pop(address_id, None)
        record = await self._storage.find_document(address, "current", context)
        if record is None:
            return None
        stored = await self._storage.document(record["id"], "current", context)
        if stored is None:
            raise RuntimeError(f"Current document {record['id']} ({record['kind']}) cannot be read")
        value = materialize_document(definition, stored)
        loaded = LoadedDocument(
            address_id=address_id,
            record=stored["record"],
            stored_version=stored["version"],
            value_version=definition.version,
            deltas_since_base=stored["deltasSinceBase"],
            value=value,
        )
        self._documents[address_id] = loaded
        return loaded

    def _detach_state(self, state: DocumentState) -> None:
        if state in self._states:
            self._states.remove(state)

    async def unload_documents(self) -> None:
        """Drop every loaded document value; later access cold-loads from Storage."""

        async with self._line:
            self._documents.clear()

    # ── Document reads and observation ─────────────────────────────────────

    async def snapshot(self, token: AnyDocToken, *args: Any) -> JsonObject | None:
        self.assert_usable()
        definition = token.definition
        resolved = resolve_address(definition, args)
        context = args[resolved.next_argument]
        # A cached document serves its committed revision without waiting for an in-flight commit,
        # which holds the mutation line until Storage settles.
        cached = self._documents.get(resolved.id)
        if cached is not None and cached.value_version == definition.version:
            _validate_document(definition, cached)
            return cached.value
        async with self._line:
            self._assert_healthy()
            loaded = await self._load_document(definition, resolved.id, resolved.address, context)
        if loaded is None:
            return None
        _validate_document(definition, loaded)
        return loaded.value

    async def document_state(self, token: AnyDocToken, *args: Any) -> DocumentState | None:
        self.assert_usable()
        definition = token.definition
        resolved = resolve_address(definition, args)
        context = args[resolved.next_argument]
        async with self._line:
            self._assert_healthy()
            loaded = await self._load_document(definition, resolved.id, resolved.address, context)
            if loaded is None:
                return None
            _validate_document(definition, loaded)
            state = DocumentState(self, loaded.record["id"], loaded.value, loaded.value_version)
            self._states.append(state)
            return state

    async def watch_doc(self, token: AnyDocToken, *args: Any) -> CommittedWatch | None:
        self.assert_usable()
        definition = token.definition
        resolved = resolve_address(definition, args)
        context = args[resolved.next_argument]
        signal = context.abort_signal
        cancelled = signal is not None and signal.aborted
        remove_listener: Callable[[], None] | None = None
        if signal is not None:

            def mark_cancelled() -> None:
                nonlocal cancelled
                cancelled = True

            remove_listener = signal.add_listener(mark_cancelled)

        def raise_if_cancelled() -> None:
            if cancelled:
                assert signal is not None
                signal.raise_if_aborted()

        try:
            async with self._line:
                self._assert_healthy()
                raise_if_cancelled()
                loaded = await self._load_document(
                    definition, resolved.id, resolved.address, context
                )
                raise_if_cancelled()
                if loaded is None:
                    return None
                watch = self._attach_watch(definition, loaded)
            if cancelled:
                watch.cancel()
                raise_if_cancelled()
            if signal is not None:
                watch.observe_cancellation(signal)
            return watch
        finally:
            if remove_listener is not None:
                remove_listener()

    def _attach_watch(self, definition: DocDefinition, loaded: LoadedDocument) -> CommittedWatch:
        _validate_document(definition, loaded)

        def detach() -> None:
            self._watches = [entry for entry in self._watches if entry.watch is not watch]

        watch = CommittedWatch(
            cast(ObservedDocumentValue, loaded.value),
            detach,
            self.report,
            loaded.value_version,
        )
        self._watches.append(_WatchAttachment(loaded.record["id"], watch))
        return watch

    async def snapshot_as_of(self, token: AnyDocToken, *args: Any) -> JsonObject | None:
        self.assert_usable()
        definition = token.definition
        resolved = resolve_address(definition, args)
        if resolved.address["scope"]["kind"] != "conversation":
            raise TypeError("Session.snapshot_as_of() requires a conversation document")
        if definition.history != "rewindable":
            raise TypeError("Session.snapshot_as_of() requires a rewindable conversation document")
        conversation_id = resolved.address["scope"]["conversationId"]
        at_value = args[resolved.next_argument]
        if not isinstance(at_value, int) or isinstance(at_value, bool) or at_value < 1:
            raise TypeError("Session.snapshot_as_of() requires an entry ID")
        context = args[resolved.next_argument + 1]
        async with self._line:
            self._assert_healthy()
            stored_entry = await self._storage.entry(conversation_id, EntryId(at_value), context)
            if stored_entry is None:
                raise ValueError(f"Entry {at_value} is not visible from conversation {conversation_id}")
            address = dict(resolved.address)
            address["scope"] = {
                "kind": "conversation",
                "conversationId": stored_entry["entry"]["conversationId"],
            }
            record = await self._storage.find_document(
                cast(DocumentAddress, address), stored_entry["commitSeq"], context
            )
            if record is None:
                return None
            stored = await self._storage.document(record["id"], stored_entry["commitSeq"], context)
            if stored is None:
                raise ValueError(f"Historical document {record['id']} ({record['kind']}) cannot be read")
            return materialize_document(definition, stored)

    def _retention(self) -> int:
        value = self._retention_setting
        try:
            if callable(value):
                value = value()
            return _document_retention(value)
        except Exception:
            return _document_retention(None)

    def _sweep_documents(self) -> None:
        """Evict idle documents immediately when the retention is zero.

        Positive retention timers arrive with the observation ticket; an observed document is
        pinned by its state and must survive any sweep.
        """

        if self._retention() != 0:
            return
        attached = {state._record_id for state in self._states}
        attached.update(entry.record_id for entry in self._watches if not entry.watch.retired)
        for address_id, document in list(self._documents.items()):
            if document.record["id"] not in attached:
                del self._documents[address_id]

    # ── Host callbacks ─────────────────────────────────────────────────────

    async def conversation_created(self, tx: Transaction, record: ConversationRecord) -> None:
        """A plain Session stages nothing extra for a new conversation."""

    # ── Internal ───────────────────────────────────────────────────────────



class GuardedStorage:
    """The Session's Storage behind its failure guard."""

    def __init__(self, storage: Storage, session: SessionImpl) -> None:
        self._storage = storage
        self._session = session
        self._underway: set[asyncio.Future[Any]] = set()
        self._closing = False

    def _assert_healthy(self) -> None:
        if self._session.failure is not None:
            raise SessionFailed(self._session.failure)

    async def _call(self, read: Context | None, run: Callable[[], Awaitable[Any]]) -> Any:
        self._assert_healthy()
        if self._closing:
            raise RuntimeError("Session is closed")
        call = asyncio.ensure_future(run())
        self._underway.add(call)
        try:
            result = await call
        except BaseException as error:
            self._assert_healthy()
            exempt = isinstance(error, asyncio.CancelledError) or (
                read is not None
                and (
                    isinstance(error, StorageRequestError)
                    or (read.abort_signal is not None and read.abort_signal.aborted)
                )
            )
            if not exempt:
                self._session.fail(error)
            raise
        finally:
            self._underway.discard(call)
        self._assert_healthy()
        return result

    async def commit(self, writes: Any, context: Context) -> Seq:
        return cast(Seq, await self._call(None, lambda: self._storage.commit(writes, context)))

    async def mint_id(self) -> int:
        return cast(int, await self._call(None, lambda: self._storage.mint_id()))

    async def conversation(self, id: ConversationId, context: Context) -> ConversationRecord | None:
        return cast(
            ConversationRecord | None,
            await self._call(context, lambda: self._storage.conversation(id, context)),
        )

    async def scan_conversations(
        self, query: ConversationQuery, limit: int, cursor: Cursor | None, context: Context
    ) -> Page[ConversationRecord, Cursor]:
        return cast(
            Page[ConversationRecord, Cursor],
            await self._call(
                context,
                lambda: self._storage.scan_conversations(query, limit, cursor, context),
            ),
        )

    async def entry(self, *args: Any, context: Context | None = None, **kwargs: Any) -> EntryLookup | None:
        if context is None and len(args) == 3:
            resolved: Context = args[2]
            return cast(
                EntryLookup | None,
                await self._call(resolved, lambda: self._storage.entry(args[0], args[1], resolved)),
            )
        if context is None and len(args) == 2 and isinstance(args[1], Context):
            resolved = args[1]
            return cast(
                EntryLookup | None,
                await self._call(resolved, lambda: self._storage.entry(args[0], resolved)),
            )
        if context is None:
            raise TypeError("Storage.entry() requires a context")
        return cast(
            EntryLookup | None,
            await self._call(context, lambda: self._storage.entry(args[0], context)),
        )

    async def find_latest_head_marker(
        self,
        conversation_id: ConversationId,
        at_or_before_entry_id: EntryId | None,
        context: Context,
    ) -> EntryWithHead | None:
        return cast(
            EntryWithHead | None,
            await self._call(
                context,
                lambda: self._storage.find_latest_head_marker(
                    conversation_id, at_or_before_entry_id, context
                ),
            ),
        )

    async def scan_entries(
        self, query: EntryQuery, limit: int, cursor: Cursor | None, context: Context
    ) -> Page[EntryRecord, Cursor]:
        return cast(
            Page[EntryRecord, Cursor],
            await self._call(context, lambda: self._storage.scan_entries(query, limit, cursor, context)),
        )

    async def task(self, id: TaskId, context: Context) -> TaskRecord | None:
        return cast(TaskRecord | None, await self._call(context, lambda: self._storage.task(id, context)))

    async def scan_tasks(
        self, query: TaskQuery, limit: int, cursor: Cursor | None, context: Context
    ) -> Page[TaskRecord, Cursor]:
        return cast(
            Page[TaskRecord, Cursor],
            await self._call(context, lambda: self._storage.scan_tasks(query, limit, cursor, context)),
        )

    async def submission(self, id: Any, context: Context) -> SubmissionRecord | None:
        return cast(
            SubmissionRecord | None,
            await self._call(context, lambda: self._storage.submission(id, context)),
        )

    async def scan_submissions(
        self, query: SubmissionQuery, limit: int, cursor: Cursor | None, context: Context
    ) -> Page[SubmissionRecord, Cursor]:
        return cast(
            Page[SubmissionRecord, Cursor],
            await self._call(context, lambda: self._storage.scan_submissions(query, limit, cursor, context)),
        )

    async def submission_by_request(
        self, conversation_id: ConversationId, request_id: str, context: Context
    ) -> SubmissionRecord | None:
        return cast(
            SubmissionRecord | None,
            await self._call(
                context,
                lambda: self._storage.submission_by_request(conversation_id, request_id, context),
            ),
        )

    async def find_document(
        self, address: DocumentAddress, at: DocumentPoint, context: Context
    ) -> DocumentRecord | None:
        return cast(
            DocumentRecord | None,
            await self._call(context, lambda: self._storage.find_document(address, at, context)),
        )

    async def document(self, id: DocumentId, at: DocumentPoint, context: Context) -> StoredDocument | None:
        return cast(
            StoredDocument | None,
            await self._call(context, lambda: self._storage.document(id, at, context)),
        )

    async def scan_documents(
        self, query: DocumentQuery, limit: int, cursor: Cursor | None, context: Context
    ) -> Page[DocumentRecord, Cursor]:
        return cast(
            Page[DocumentRecord, Cursor],
            await self._call(context, lambda: self._storage.scan_documents(query, limit, cursor, context)),
        )

    async def close(self, context: Context) -> None:
        self._closing = True
        if self._underway:
            await asyncio.gather(*list(self._underway), return_exceptions=True)
        await self._storage.close(context)


def _current_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        pass
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        return asyncio.new_event_loop()


async def _await_with_context(awaitable: Awaitable[Any], context: Context) -> Any:
    signal = context.abort_signal
    if signal is None:
        return await awaitable
    if signal.aborted:
        raise _abort_error(signal)
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[None] = loop.create_future()

    def abort() -> None:
        if not waiter.done():
            waiter.set_exception(_abort_error(signal))

    remove = signal.add_listener(abort)
    if signal.aborted:
        abort()
    task: asyncio.Future[Any] = asyncio.ensure_future(awaitable)
    try:
        await asyncio.wait((task, waiter), return_when=asyncio.FIRST_COMPLETED)
        if task.done():
            return task.result()
        waiter.result()
    finally:
        remove()
        if not waiter.done():
            waiter.cancel()
    raise AssertionError("unreachable")


def _abort_error(signal: Any) -> BaseException:
    reason = signal.reason
    if isinstance(reason, BaseException):
        return reason
    from ..context import AbortError

    return AbortError(reason)


__all__ = ["DocumentState", "SessionImpl", "create_session"]

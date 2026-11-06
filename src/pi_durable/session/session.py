"""Session kernel: one mutation line, committed reads, and a bounded document cache."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, TypedDict, cast

from ..context import BACKGROUND_CONTEXT, Context, without_abort_signal
from ..documents import (
    AnyDocToken,
    DocDefinition,
    address_id,
    check_record_scope,
    check_record_version,
    materialize_document,
    resolve_address,
)
from ..errors import ReadOnlySession, SessionFailed, StorageRequestError, is_failure
from ..harness.context import read_context
from ..harness.types import LISTENER, ContextView, ReportInfo, SetTimer, host_error
from ..ids import ConversationId, DocumentId, EntryId, Seq, TaskId
from ..json import JsonObject
from ..timers import host_timers
from ..types import (
    CommitChange,
    CommitObservation,
    CommitObserver,
    CommitPublication,
    CommitPublicationScope,
    CommitReceipt,
    CommitResult,
    ConversationQuery,
    ConversationRecord,
    Cursor,
    DocumentAddress,
    DocumentCommitChange,
    DocumentCommittedChange,
    DocumentPoint,
    DocumentQuery,
    DocumentRecord,
    DocumentRetiredChange,
    EntryLookup,
    EntryQuery,
    EntryRecord,
    EntryWithHead,
    Observer,
    Page,
    PublishingStorage,
    SessionEnd,
    Storage,
    StorageWrite,
    StoredDocument,
    SubmissionQuery,
    SubmissionRecord,
    TaskQuery,
    TaskRecord,
)
from .observation import (
    ENDED,
    AttachedObservation,
    AttachedObserver,
    CommittedWatch,
    GuardedObserver,
    contained,
    session_closed_error,
)
from .transaction import LoadedDocument, Transaction, TransactionScope

#: Default document retention: ten minutes, as for kept contexts.
_DEFAULT_DOCUMENT_RETENTION_MS = 600_000

_default_document_retention: int | float = _DEFAULT_DOCUMENT_RETENTION_MS


def set_default_document_retention(ms: int | float) -> None:
    """Internal, for tests: the retention of every Session that keeps the default."""

    global _default_document_retention
    _default_document_retention = ms


def document_retention(ms: int | float | None) -> int | float:
    """Return a retention as given when it is a number of milliseconds, ``0`` or more.

    ``Infinity`` keeps documents; a value that is not a number of milliseconds means the
    default, as v1.3.0 does.
    """

    if isinstance(ms, (int, float)) and not isinstance(ms, bool) and ms >= 0:
        return ms
    return _default_document_retention


def create_session(storage: Storage, options: dict[str, Any] | None = None, **kwargs: Any) -> SessionImpl:
    """Open one Session kernel over a storage backend.

    ``set_timer`` arms the background timer that evicts idle documents; the default is an asyncio
    timer (spec §5.6).
    """

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
        values.get("set_timer", values.get("setTimer")),
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _validate_document(definition: DocDefinition, loaded: LoadedDocument) -> None:
    """Reject typed access whose token disagrees with the stored incarnation or cannot use it."""

    check_record_scope(definition, loaded.record)
    check_record_version(definition, loaded.record, loaded.stored_version)


def _record_address_id(record: DocumentRecord) -> str:
    """Return the cache identity of one committed document record."""

    address: DocumentAddress = {"kind": record["kind"], "scope": record["scope"]}
    if "key" in record:
        address["key"] = record["key"]
    return address_id(address)


@dataclass(slots=True)
class _WatchAttachment:
    """One follower of a document address — a watch or an observer — and the incarnation it pins.

    Both followers follow the address rather than one incarnation, so a retirement unpins what the
    attachment held and a document created at that address later is pinned again for the same
    attachment (spec §9.2). The follower decides what it does with the change: a watch folds it
    behind its listener, an observer mirrors it synchronously.
    """

    address_id: str
    record_id: DocumentId | None
    follower: CommittedWatch | AttachedObserver
    release: Callable[[], None] | None


@dataclass(slots=True)
class _DocumentSweep:
    """The pending eviction: a background timer for the oldest idle document, or a line job."""

    at: int | float
    disarm: Callable[[], None] | None = None


class DocumentCacheSizes(TypedDict):
    """Internal, for tests: how many documents the cache holds and pins, and its sweep jobs."""

    cached: int
    idle: int
    attached: int
    sweeps: int


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
        document_retention_ms: int | float | Callable[[], int | float | None] | None = None,
        set_timer: SetTimer | None = None,
        read_only: bool = False,
    ) -> None:
        self._read_only = read_only
        self._documents: dict[str, LoadedDocument] = {}
        # Cached documents with no observer attached, by address, oldest use first.
        self._idle_documents: dict[str, int | float] = {}
        # Observers attached per incarnation (record ID); a cached incarnation with any stays.
        self._attached_documents: dict[DocumentId, int] = {}
        self._document_sweep: _DocumentSweep | None = None
        self._sweep_jobs = 0
        self._sweep_tasks: set[asyncio.Task[None]] = set()
        self._document_timers_unavailable = False
        self._watches: list[_WatchAttachment] = []
        # Host observers of one document address each, following the address as a watch does.
        self._observers: list[_WatchAttachment] = []
        # Host observers of every stored commit (`observe_commits`), ended at the close's last publication.
        self._commit_observers: list[GuardedObserver] = []
        # The Session's own components follow every commit; a throw leaves their state behind storage.
        self._internal_listeners: list[Callable[[CommitPublication, Context], Any]] = []
        self._close_listeners: list[Callable[[], Any]] = []
        self._line = asyncio.Lock()
        self._closing: asyncio.Future[None] | None = None
        self._failure: BaseException | None = None
        loop = _current_loop()
        self._failed: asyncio.Future[SessionFailed] = loop.create_future()
        self._closed: asyncio.Future[SessionEnd] = loop.create_future()
        self._failed.add_done_callback(lambda future: future.exception())
        self._clock_fn = now or _now_ms

        def clock() -> int:
            try:
                return self._clock_fn()
            except Exception:
                return _now_ms()

        def retention() -> int | float:
            try:
                value = document_retention_ms() if callable(document_retention_ms) else document_retention_ms
                return document_retention(value)
            except Exception:
                return document_retention(None)

        self._clock = clock
        self._document_retention_ms = retention
        self._storage = GuardedStorage(storage, self)
        self._host = _SessionHost(self)
        self._timers = host_timers(
            set_timer,
            lambda error: self.report(error, host_error("setTimer")),
        )

    # ── Lifecycle and commits ───────────────────────────────────────────────

    @property
    def closed(self) -> asyncio.Future[SessionEnd]:
        return self._closed

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    @property
    def now(self) -> int:
        """The Session's current Harness clock value."""

        return self._clock()

    @property
    def storage(self) -> Storage:
        """The Session's guarded Storage, as the Harness components use it."""

        return cast(Storage, self._storage)

    def owns_transaction(self, tx: Any) -> bool:
        """Internal: whether ``tx`` is the transaction of one of this Session's commits.

        A wrapper that forwards attribute reads is not one of them: only a Transaction this Session
        created, holding this Session's own host, is.
        """

        return type(tx) is Transaction and tx._host is self._host

    @property
    def failed(self) -> asyncio.Future[SessionFailed]:
        return self._failed

    def fail(self, error: BaseException) -> None:
        """Fail the Session once; the first failure wins and close listeners run immediately.

        A teardown signal is not a failure: cancellation and interpreter shutdown unwind the work
        that is running instead of ending the Session.
        """

        if not is_failure(error) or self._failure is not None:
            return
        self._failure = error
        if not self._failed.done():
            self._failed.set_exception(SessionFailed(error))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._close_after_failure())
        self.report(error, {"kind": "storage_failed"})

    async def _close_after_failure(self) -> None:
        try:
            await self.close(BACKGROUND_CONTEXT)
        except Exception:
            pass

    def report(self, error: BaseException, info: ReportInfo | None = None) -> None:
        """Internal: the Session's failure and listener errors, which never fail what ran them.

        ``info`` defaults to a listener. A plain Session has no external report sink.
        """

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
            if self._read_only:
                # Refused before Storage: the Session stays healthy, and the caller sees why.
                tx.discard()
                self._sweep_documents()
                raise ReadOnlySession("a commit with writes")
            try:
                seq = await self._store_one_batch(writes, context)
            except BaseException:
                tx.discard()
                raise
            try:
                documents = tx.adopt(seq)
            except BaseException as error:
                self.fail(error)
                raise
            for document in documents:
                self._touch_document(_record_address_id(document["record"]))
            self._publishing(partial(self._publish, seq, writes, documents, context), context)
            self._sweep_documents()
            return result

    async def _store_one_batch(self, writes: Sequence[StorageWrite], context: Context) -> Seq:
        """Store one commit batch through the batched call and return its sequence.

        The Session commits one batch at a time until group commit (#42) puts several in one call.
        A batch Durable's own rules refuse is a resolved outcome, not a thrown error: nothing of it
        is stored, so it fails the Session and raises here, which is what a storage that threw the
        refusal did from inside the guard before the batched call was the only commit shape.
        """

        received: list[Seq] = []

        def receive(index: int, seq: Seq) -> None:
            received.append(seq)

        result = await self._storage.commit_batches(
            [writes], receive, without_abort_signal(context)
        )
        if result["stored"] == 0 or not received:
            error = result.get("refused", RuntimeError("Storage refused the commit batch"))
            self.fail(error)
            raise error
        return received[-1]

    def _publishing(self, publish: Callable[[], None], context: Context) -> None:
        """Run one commit's publication inside the storage's own scope, or directly without one.

        The optional ``publishing`` member (see
        :class:`~pi_durable.types.PublishingStorage`) lets a storage that owns a batch take each
        publication inside that batch, so what the publication marks lands together. No bundled
        backend has one, so the Session normally publishes directly. A storage that throws instead
        of running the publication is reported and the publication runs anyway: the commit is
        stored, so it must be published.
        """

        scope = self._storage.publishing
        if scope is None:
            publish()
            return
        ran = False

        def once() -> None:
            nonlocal ran
            if ran:
                return
            ran = True
            publish()

        try:
            scope(once, context)
        except BaseException as error:
            self.report(error, LISTENER)
            once()

    async def read_on_line(self, job: Callable[[], Awaitable[Any]]) -> Any:
        self.assert_usable()
        return await self._read_on_line(job)

    async def read_queued_before_close(self, job: Callable[[], Awaitable[Any]]) -> Any:
        """Run a read admitted before ``close()`` sealed admission; a later read rejects.

        The Harness uses this for ``inspect()``: an inspection already queued on the Session line at
        the seal still runs and reports ``scheduling: "closing"``, while one asked for after the seal
        rejects like every other new operation.
        """

        self._assert_healthy()
        if self._closing is not None:
            raise self._closed_error()
        return await self._read_on_line(job)

    async def _read_on_line(self, job: Callable[[], Awaitable[Any]]) -> Any:
        async with self._line:
            self._assert_healthy()
            if self._closed.done():
                raise self._closed_error()
            return await job()

    async def context(
        self,
        conversation_id: ConversationId,
        context: Context,
        at: EntryId | None = None,
    ) -> ContextView:
        """Return one conversation's committed transcript and derived model context.

        With ``at``, the context as of that visible entry, as a fork at that entry starts with.
        Bounds are captured on the Session line; the entries at or below them are immutable, so
        derivation runs off the line.
        """

        self.assert_usable()
        return await read_context(self, self._storage, conversation_id, context, at)

    def assert_usable(self) -> None:
        self._assert_healthy()
        if self._closing is not None:
            raise self._closed_error()

    def _closed_error(self) -> Exception:
        """The error a closed Session rejects with; a Harness names itself."""

        return RuntimeError("Session is closed")

    def _assert_healthy(self) -> None:
        if self._failure is not None:
            raise SessionFailed(self._failure)

    async def close(self, context: Context) -> None:
        self._begin_close()
        await self._wait_close(context)

    def _begin_close(self) -> None:
        """Seal admission and start the shutdown; a repeated close shares the same future."""

        if self._closing is not None:
            return
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

    async def _wait_close(self, context: Context) -> None:
        assert self._closing is not None
        await _await_with_context(self._closing, context)

    async def _run_close(self) -> None:
        assert self._closing is not None
        error: BaseException | None = None
        try:
            try:
                await self.before_close()
            except Exception as close_error:
                self.report(close_error)
            async with self._line:
                # After the last publication: every commit admitted before the seal has settled, so
                # a commit observer's end follows the last publication it was handed.
                commit_observers, self._commit_observers = self._commit_observers, []
                closed = self._failure if self._failure is not None else session_closed_error()
                for observer in commit_observers:
                    observer.close_session(closed)
                self._internal_listeners.clear()
                self._documents.clear()
                self._idle_documents.clear()
                self._attached_documents.clear()
                self._observers.clear()
                try:
                    await self._storage.close(without_abort_signal(BACKGROUND_CONTEXT))
                except BaseException as close_error:
                    error = close_error
                    if self._failure is None:
                        self._failure = close_error
                        self.report(close_error, {"kind": "storage_failed"})
                    raise
        except BaseException as caught:
            error = caught
        finally:
            self._settle_closed(
                {"kind": "closed"} if self._failure is None else {"kind": "failed", "error": self._failure}
            )
            if not self._closing.done():
                if error is None:
                    self._closing.set_result(None)
                else:
                    self._closing.set_exception(error)

    def _settle_closed(self, end: SessionEnd) -> None:
        """Settle ``closed``, once, with the end the Session ended by."""

        if not self._closed.done():
            self._closed.set_result(end)

    def _notify_close(self) -> None:
        self._cancel_document_sweep()
        for task in list(self._sweep_tasks):
            task.cancel()
        listeners = self._close_listeners
        self._close_listeners = []
        for listener in listeners:
            contained(listener, self.report)
        watches = self._watches
        self._watches = []
        for entry in watches:
            entry.follower.close_session(self._failure)
        observers = self._observers
        self._observers = []
        # One close, one error: an observer follows the Session through its close, so the close ends
        # it `failed` — the `ended` kind stays for an observed owner that ended (spec §9.1).
        closed = self._failure if self._failure is not None else session_closed_error()
        for entry in observers:
            entry.follower.close_session(closed)

    def observe_commits(self, observer: CommitObserver) -> CommitObservation:
        """Attach a synchronous observer of every stored commit (spec §9.1).

        Its ``advance`` is handed exactly what Storage made durable, one microtask from the commit's
        publication, and its ``end`` gets the Session's close after its last publication. The
        detach is idempotent: no call and no end follows it.
        """

        self.assert_usable()
        guarded = GuardedObserver(
            observer,
            lambda: (
                self._commit_observers.remove(guarded)
                if guarded in self._commit_observers
                else None
            ),
            lambda error: self.report(error, LISTENER),
        )
        self._commit_observers.append(guarded)
        return AttachedObservation(None, guarded)

    def observe_stored_commits(
        self,
        listener: Callable[[CommitPublication, Context], Any],
    ) -> Callable[[], None]:
        """Internal: a commit listener whose state follows storage; a throw fails the Session."""

        self.assert_usable()
        self._internal_listeners.append(listener)
        return (
            lambda: self._internal_listeners.remove(listener)
            if listener in self._internal_listeners
            else None
        )

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
        if (
            not self._internal_listeners
            and not self._commit_observers
            and not self._watches
            and not self._observers
        ):
            return
        changes: list[CommitChange] = [
            cast(CommitChange, write)
            for write in writes
            if write["type"] in ("conversation", "entry", "task", "submission")
        ]
        changes.extend(documents)
        publication: CommitPublication = {"seq": seq, "changes": changes}
        for entry in list(self._observers):
            self._advance_observer(entry, documents, writes, context)
        for entry in list(self._watches):
            for settled in documents:
                if (
                    settled["type"] != "document"
                    or _record_address_id(settled["record"]) != entry.address_id
                ):
                    continue
                # A watch follows its address: while it is absent the watch pins nothing, and a new
                # incarnation is pinned for the same watch.
                if settled["kind"] == "retire":
                    if entry.release is not None:
                        entry.release()
                    entry.release = None
                    entry.record_id = None
                elif entry.record_id != settled["record"]["id"]:
                    if entry.release is not None:
                        entry.release()
                    entry.record_id = settled["record"]["id"]
                    entry.release = self._pin_document(entry.address_id, settled["record"]["id"])
                watch = cast(CommittedWatch, entry.follower)
                watch.advance_document(settled, context)
        for listener in list(self._internal_listeners):
            contained(partial(listener, publication, context), self.fail)
        for observer in list(self._commit_observers):
            # A commit observer mirrors what Storage made durable, in line order, and in no
            # particular order with the watch listeners. It is read at delivery: an observer that an
            # earlier advance detached — its own or another observer's — takes nothing of the commit
            # it was attached for (§9.1).
            if observer.attached:
                observer.advance(publication, context)

    def _follow_address(
        self,
        entry: _WatchAttachment,
        settled: DocumentCommittedChange | DocumentRetiredChange,
    ) -> None:
        """Move one attachment's pin to the incarnation its address now holds.

        A watch and an observer follow their address the same way: while it is absent nothing is
        pinned for them, and a document created at it later is pinned again for the same attachment.
        """

        if settled["kind"] == "retire":
            if entry.release is not None:
                entry.release()
            entry.release = None
            entry.record_id = None
        elif entry.record_id != settled["record"]["id"]:
            if entry.release is not None:
                entry.release()
            entry.record_id = settled["record"]["id"]
            entry.release = self._pin_document(entry.address_id, settled["record"]["id"])

    def _advance_observer(
        self,
        entry: _WatchAttachment,
        documents: list[DocumentCommitChange],
        writes: list[StorageWrite],
        context: Context,
    ) -> None:
        """Hand one observer every change of this commit at its address, unfolded and in line order.

        The observer may detach in one of its own calls, or throw and be detached by the guard: what
        is left of the commit then reaches neither it nor a pin taken for it. A commit that makes the
        owner task of a task document terminal is the one case an observer ends on its own: its
        retirement is the task document's last change, and the end follows it on the same publication
        (spec §9.1).
        """

        follower = cast(AttachedObserver, entry.follower)
        for change in documents:
            # A call of this same commit may have detached the observer, its own or another
            # observer's: the rest of the commit then reaches neither it nor a pin taken for it.
            if not follower.attached:
                return
            if (
                change["type"] != "document"
                or _record_address_id(change["record"]) != entry.address_id
            ):
                continue
            self._follow_address(entry, change)
            follower.advance_document(change, context)
            if not follower.attached:
                return
        owner = follower.owner_task_id
        if owner is None:
            return
        for write in writes:
            if write["type"] != "task":
                continue
            record = write["value"]
            if record["id"] == owner and record["state"]["status"] == "terminal":
                follower.end(ENDED)
                return

    # ── Document cache ─────────────────────────────────────────────────────

    def _cached(self, address_id: str) -> LoadedDocument | None:
        if self._read_only:
            # A read-only Session shows a Storage another process writes, so nothing it reads stays current.
            return None
        cached = self._documents.get(address_id)
        if cached is not None:
            self._touch_document(address_id)
        return cached

    def _install(self, document: LoadedDocument) -> None:
        if self._read_only:
            return
        self._documents[document.address_id] = document
        self._touch_document(document.address_id)

    def _evict(self, address_id: str, record_id: DocumentId) -> None:
        cached = self._documents.get(address_id)
        if cached is not None and cached.record["id"] == record_id:
            self._drop_document(address_id)

    async def _load_document(
        self,
        definition: DocDefinition,
        address_id: str,
        address: DocumentAddress,
        context: Context,
    ) -> LoadedDocument | None:
        cached = None if self._read_only else self._documents.get(address_id)
        if cached is not None and cached.value_version == definition.version:
            self._touch_document(address_id)
            return cached
        if cached is not None:
            self._drop_document(address_id)
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
        self._install(loaded)
        return loaded

    async def unload_documents(self) -> None:
        """Drop every loaded document value; later access cold-loads from Storage."""

        async with self._line:
            self._documents.clear()
            self._idle_documents.clear()

    # ── Document reads and observation ─────────────────────────────────────

    async def _document_on_line(self, token: AnyDocToken, *args: Any) -> LoadedDocument | None:
        """Load and validate one document while the caller holds the Session line.

        Internal: conversation-view mounts capture their documents in the same line job as their
        entries, so no commit can land between the two reads.
        """

        definition = token.definition
        resolved = resolve_address(definition, args)
        context = args[resolved.next_argument]
        loaded = await self._load_document(definition, resolved.id, resolved.address, context)
        if loaded is not None:
            _validate_document(definition, loaded)
        return loaded

    async def snapshot(self, token: AnyDocToken, *args: Any) -> JsonObject | None:
        self.assert_usable()
        definition = token.definition
        resolved = resolve_address(definition, args)
        context = args[resolved.next_argument]
        # A cached document serves its committed revision without waiting for an in-flight commit,
        # which holds the mutation line until Storage settles.
        cached = self._documents.get(resolved.id)
        if cached is not None and cached.value_version == definition.version:
            self._touch_document(resolved.id)
            _validate_document(definition, cached)
            return cached.value
        async with self._line:
            self._assert_healthy()
            loaded = await self._load_document(definition, resolved.id, resolved.address, context)
        if loaded is None:
            return None
        _validate_document(definition, loaded)
        return loaded.value

    async def observe_doc(self, token: AnyDocToken, *args: Any) -> AttachedObservation | None:
        """Attach a synchronous observer of one document address (spec §9.1).

        The attachment is atomic on the mutation line: it takes the committed revision with no
        commit between the read and the attachment, so a commit queued behind the attachment is
        already in the value the observation hands back, and every later change at the address
        reaches the observer in line order. An absent address attaches nothing and answers ``None``.
        The detach is idempotent and unpins the document.
        """

        definition = token.definition
        resolved = resolve_address(definition, args)
        observer = cast(Observer, args[resolved.next_argument])
        context = args[resolved.next_argument + 1]
        return await self._attach_document(
            definition,
            resolved,
            context,
            lambda loaded: self._attach_observer(definition, loaded, observer),
            lambda observation: observation.detach(),
        )

    async def watch_doc(self, token: AnyDocToken, *args: Any) -> CommittedWatch | None:
        definition = token.definition
        resolved = resolve_address(definition, args)
        context = args[resolved.next_argument]
        signal = context.abort_signal

        def create(loaded: LoadedDocument) -> CommittedWatch:
            watch = self._attach_watch(definition, loaded)
            if signal is not None:
                watch.observe_cancellation(signal)
            return watch

        return await self._attach_document(
            definition,
            resolved,
            context,
            create,
            lambda watch: watch.abort_acquisition(),
        )

    async def _attach_document(
        self,
        definition: DocDefinition,
        resolved: Any,
        context: Context,
        attach: Callable[[LoadedDocument], Any],
        cancel: Callable[[Any], None],
    ) -> Any:
        """Load one document on the line and attach one follower to its address.

        One acquisition serves a watch and an observer alike, which differ only in what they build
        from the loaded document and in how an acquisition cancelled while it waited on the line
        undoes itself: a watch aborts its acquisition, an observation detaches (spec §9.2).
        """

        self.assert_usable()
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
                attached = attach(loaded)
            if cancelled:
                cancel(attached)
                raise_if_cancelled()
            return attached
        finally:
            if remove_listener is not None:
                remove_listener()

    def _attach_watch(self, definition: DocDefinition, loaded: LoadedDocument) -> CommittedWatch:
        _validate_document(definition, loaded)
        # Close seals admission while the load was underway: no watch, and so no pin, remains.
        self.assert_usable()
        release = self._pin_document(loaded.address_id, loaded.record["id"])

        def detach() -> None:
            self._watches = [
                held for held in self._watches if held.follower is not watch
            ]
            # What is unpinned is what the address holds now, not what it held at acquisition.
            release_now, attachment.release = attachment.release, None
            if release_now is not None:
                release_now()

        watch = CommittedWatch(
            loaded.value,
            detach,
            self.report,
            loaded.value_version,
            loaded.record["id"],
        )
        attachment = _WatchAttachment(
            loaded.address_id, loaded.record["id"], watch, release
        )
        self._watches.append(attachment)
        return watch

    def _attach_observer(
        self, definition: DocDefinition, loaded: LoadedDocument, observer: Observer
    ) -> AttachedObservation:
        """Follow one address with a host observer, pinned the way a watch of it is pinned."""

        _validate_document(definition, loaded)
        # Close seals admission while the load was underway: no observer, and so no pin, remains.
        self.assert_usable()
        release = self._pin_document(loaded.address_id, loaded.record["id"])

        def detach() -> None:
            self._observers = [
                held for held in self._observers if held.follower is not follower
            ]
            # What is unpinned is what the address holds now, not what it held at attachment.
            release_now, attachment.release = attachment.release, None
            if release_now is not None:
                release_now()

        scope = loaded.record["scope"]
        owner_task_id = scope["taskId"] if scope["kind"] == "task" else None
        follower = AttachedObserver(
            observer,
            detach,
            self.report,
            loaded.value,
            loaded.value_version,
            loaded.record["id"],
            owner_task_id,
        )
        attachment = _WatchAttachment(
            loaded.address_id, loaded.record["id"], follower, release
        )
        self._observers.append(attachment)
        return AttachedObservation(loaded.value, follower)

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

    @property
    def document_cache_sizes(self) -> DocumentCacheSizes:
        """Internal, for tests: cached, idle, and pinned documents, and sweep jobs so far."""

        return DocumentCacheSizes(
            cached=len(self._documents),
            idle=len(self._idle_documents),
            attached=len(self._attached_documents),
            sweeps=self._sweep_jobs,
        )

    def _touch_document(self, address_id: str) -> None:
        """Note a use: an unattached document becomes the newest idle one and schedules a sweep."""

        cached = self._documents.get(address_id)
        if cached is None or cached.record["id"] in self._attached_documents:
            return
        now = self._clock()
        since = self._idle_documents.get(address_id)
        # A hot document moves to the back at most once per granule, a tenth of the retention up to
        # 1 s: it may then leave a granule early, which costs a reload, while a commit-heavy
        # workload skips the reordering.
        if since is not None and now - since < min(1_000, self._document_retention_ms() / 10):
            return
        self._idle_documents.pop(address_id, None)
        self._idle_documents[address_id] = now
        # A pending sweep for the oldest document comes first unless the retention was shortened.
        if self._document_sweep is None or self._document_sweep.at > now + self._document_retention_ms():
            self._schedule_document_sweep()

    def _drop_document(self, address_id: str) -> None:
        self._documents.pop(address_id, None)
        self._idle_documents.pop(address_id, None)

    def _pin_document(self, address_id: str, record_id: DocumentId) -> Callable[[], None]:
        """Pin one incarnation for one observer; the returned release unpins it once."""

        self._attached_documents[record_id] = self._attached_documents.get(record_id, 0) + 1
        self._idle_documents.pop(address_id, None)
        pinned = True

        def release() -> None:
            nonlocal pinned
            if not pinned:
                return
            pinned = False
            count = self._attached_documents.get(record_id, 1) - 1
            if count > 0:
                self._attached_documents[record_id] = count
                return
            self._attached_documents.pop(record_id, None)
            cached = self._documents.get(address_id)
            if cached is not None and cached.record["id"] == record_id:
                self._touch_document(address_id)

        return release

    def _schedule_document_sweep(self) -> None:
        """Schedule the eviction of the oldest idle document, or right away when retention is 0."""

        if self._closing is not None or not self._idle_documents:
            return
        # A synchronous observer method (`stop()`, `dispose()`) may run without a loop: then only
        # the next commit evicts, as where the host declines background timers.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        at = next(iter(self._idle_documents.values())) + self._document_retention_ms()
        if self._document_sweep is not None and self._document_sweep.at <= at:
            return
        if self._document_sweep is not None and self._document_sweep.disarm is not None:
            self._document_sweep.disarm()
        sweep = _DocumentSweep(at=at)
        delay = at - self._clock()

        def run() -> None:
            self._sweep_jobs += 1
            self._queue_sweep_job(sweep)

        # The pending sweep stays set until its job runs, so touches while the line is busy queue
        # no more than one.
        self._document_sweep = sweep
        if delay <= 0:
            loop.call_soon(run)
            return
        if self._document_timers_unavailable:
            self._document_sweep = None
            return
        # A timer that fires early sweeps nothing and arms the next one.
        disarm = self._timers.background(run, delay)
        if disarm is None:
            self._document_timers_unavailable = True
            self._document_sweep = None
            return
        sweep.disarm = disarm

    def _cancel_document_sweep(self) -> None:
        sweep = self._document_sweep
        self._document_sweep = None
        if sweep is not None and sweep.disarm is not None:
            sweep.disarm()

    def _queue_sweep_job(self, sweep: _DocumentSweep) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._run_sweep_job(sweep))
        self._sweep_tasks.add(task)
        task.add_done_callback(self._sweep_tasks.discard)

    async def _run_sweep_job(self, sweep: _DocumentSweep) -> None:
        try:
            async with self._line:
                if self._document_sweep is sweep:
                    self._document_sweep = None
                if self._closing is None:
                    self._sweep_documents()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    def _sweep_documents(self) -> None:
        """On the line: evict the idle documents whose retention passed, then schedule the next."""

        expired = self._clock() - self._document_retention_ms()
        for cached_id, since in list(self._idle_documents.items()):
            if since > expired:
                break
            self._drop_document(cached_id)
        self._schedule_document_sweep()

    # ── Host callbacks ─────────────────────────────────────────────────────

    async def conversation_created(self, tx: Transaction, record: ConversationRecord) -> None:
        """A plain Session stages nothing extra for a new conversation."""

    async def before_close(self) -> None:
        """Run after close seals admission and before the line closes Storage; a Harness joins tasks here."""

    async def write_at_close(self, writes: Callable[[], Sequence[StorageWrite]]) -> None:
        """Internal, for ``before_close()``: on the line, after every commit admitted before the
        seal has settled, write what ``writes()`` returns straight to Storage.

        Nothing is published, since the Session is closing; a failed Session writes nothing.
        """

        async def job() -> None:
            if self._failure is not None:
                return
            batch = list(writes())
            if batch:
                await self._store_one_batch(batch, BACKGROUND_CONTEXT)

        async with self._line:
            await job()

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

    @property
    def publishing(self) -> CommitPublicationScope | None:
        """The backend's own scope for a publication, when it carries one; not a call to guard."""

        return self._storage.publishing if isinstance(self._storage, PublishingStorage) else None

    async def _call(self, read: Context | None, run: Callable[[], Awaitable[Any]]) -> Any:
        self._assert_healthy()
        if self._closing:
            raise RuntimeError("Session is closed")
        call = asyncio.ensure_future(run())
        self._underway.add(call)
        try:
            result = await call
        except BaseException as error:
            if not is_failure(error):
                raise
            self._assert_healthy()
            exempt = read is not None and (
                isinstance(error, StorageRequestError)
                or (read.abort_signal is not None and read.abort_signal.aborted)
            )
            if not exempt:
                self._session.fail(error)
            raise
        finally:
            self._underway.discard(call)
        self._assert_healthy()
        return result

    async def commit_batches(
        self, batches: Any, receive: CommitReceipt, context: Context
    ) -> CommitResult:
        return cast(
            CommitResult,
            await self._call(
                None, lambda: self._storage.commit_batches(batches, receive, context)
            ),
        )

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


__all__ = [
    "DocumentCacheSizes",
    "SessionImpl",
    "contained",
    "create_session",
    "document_retention",
    "set_default_document_retention",
]

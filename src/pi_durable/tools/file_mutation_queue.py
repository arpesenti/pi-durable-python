"""Serialization of ``write`` and ``edit`` mutations of one file within this process.

Port of the v1.3.0 ``src/tools/file-mutation-queue.ts``. Files are keyed by the environment's
:attr:`~pi_durable.env.FileSystem.id` and the canonical path.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from ..context import Context
from ..env import ExecutionEnv, get_or_throw
from .path_utils import canonical_path

T = TypeVar("T")

#: Tail of the mutation chain of each file, keyed by file system id and canonical path.
_queues: dict[str, asyncio.Future[None]] = {}


async def _mutation_key(env: ExecutionEnv, path: str, context: Context) -> str:
    absolute_path = get_or_throw(await env.absolute_path(path, context))
    return f"{getattr(env, 'id', '')}\0{await canonical_path(env, absolute_path, context)}"


async def with_file_mutation_queue(
    env: ExecutionEnv,
    path: str,
    fn: Callable[[], Awaitable[T]],
    context: Context,
) -> T:
    """Run ``fn`` after earlier mutations of the same file system and canonical path settle.

    Other files, and other file systems, never wait. This is not a lock against ``bash`` or other
    processes.
    """

    key = await _mutation_key(env, path, context)
    loop = asyncio.get_running_loop()
    previous = _queues.get(key)
    if previous is None:
        ready: asyncio.Future[None] = loop.create_future()
        ready.set_result(None)
        previous = ready
    done: asyncio.Future[None] = loop.create_future()
    # Take the slot without awaiting, so no other call can take it in between.
    _queues[key] = done
    entered = False
    try:
        # Shield the shared tail: cancelling this waiter must not cancel the mutation it waits on.
        await asyncio.shield(previous)
        entered = True
        return await fn()
    finally:
        if not done.done():
            done.set_result(None)
        if _queues.get(key) is done:
            # A cancelled waiter never mutates, so the next call must still wait for the mutation
            # it was queued behind rather than proceed beside it.
            if not entered and not previous.done():
                _queues[key] = previous
            else:
                del _queues[key]


__all__ = ["with_file_mutation_queue"]

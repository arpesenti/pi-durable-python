"""One batch through ``Storage.commit_batches()``, for code that seeds or writes a storage."""

from __future__ import annotations

from collections.abc import Sequence

from ..context import Context
from ..ids import Seq
from ..types import Storage, StorageWrite


async def commit_writes(
    storage: Storage, writes: Sequence[StorageWrite], context: Context
) -> Seq:
    """Commit ``writes`` as one batch and resolve with its sequence.

    The sanctioned entry point for a caller that writes a storage directly, such as a test seeding
    it: it hands the batch to :meth:`~pi_durable.types.Storage.commit_batches` and turns the
    resolved refusal back into the error Durable's own checks raised, so a seed the storage refuses
    raises where its caller expects it to.
    """

    received: list[Seq] = []

    def receive(index: int, seq: Seq) -> None:
        received.append(seq)

    result = await storage.commit_batches([writes], receive, context)
    if result["stored"] == 0:
        raise result.get("refused", RuntimeError("Storage refused the batch without a reason"))
    if not received:
        raise RuntimeError("Storage stored a batch without receiving it")
    return received[-1]


__all__ = ["commit_writes"]

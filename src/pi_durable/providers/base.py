"""Catalog lookup and non-streaming conveniences shared by the model adapters.

Both adapters resolve models through a caller-supplied catalog and implement the same non-streaming
and deferred halves of the :class:`~pi_durable.providers.Models` protocol. Only their SDK client and
stream translation differ, so those stay in the adapter modules.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from typing import Any

from . import Model
from .stream import ProviderStream, complete_stream


class ProviderModels:
    """The part of the ``Models`` protocol every adapter shares.

    Subclasses set :attr:`api`, implement :meth:`stream_simple` and :meth:`stream`, and own their
    SDK client. Deferred provider responses are unsupported by the Chat Completions and Messages
    adapters, so the base rejects them with a clear error.
    """

    api = "model"

    def __init__(self, models: Sequence[Model] = ()) -> None:
        self._models = list(models)

    def get_model(self, provider: str, model_id: str) -> Model | None:
        """Resolve one catalog entry by provider and model ID."""

        return next(
            (
                model
                for model in self._models
                if model.provider == provider and model.model_id == model_id
            ),
            None,
        )

    def stream_simple(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> ProviderStream:
        raise NotImplementedError

    def stream(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> ProviderStream:
        raise NotImplementedError

    def complete_simple(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]:
        """Run one simple request to completion and return its terminal message."""

        return complete_stream(self.stream_simple(model, context, options))

    def complete(
        self,
        model: Model,
        context: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> Awaitable[dict[str, Any]]:
        """Run one full-option request to completion and return its terminal message."""

        return complete_stream(self.stream(model, context, options))

    async def fetch_deferred(
        self,
        model: Model,
        handle: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        del model, handle, options
        raise NotImplementedError(f"The {self.api} adapter has no deferred responses")

    async def cancel_deferred(
        self,
        model: Model,
        handle: Mapping[str, Any],
        options: Mapping[str, Any] | None = None,
    ) -> None:
        del model, handle, options
        raise NotImplementedError(f"The {self.api} adapter has no deferred responses")


__all__ = ["ProviderModels"]

"""Harness-wide run policy, resolved fresh at every use.

Settings are never stored. A host may pass a mapping, an object with attributes, or a
function returning either; absent fields fall back to the defaults below.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any


@dataclass
class HarnessSettings:
    extensions: list[Any] | None = None
    stream: dict[str, Any] = field(default_factory=dict)
    retry: dict[str, Any] = field(default_factory=lambda: {"enabled": True, "maxRetries": 3, "baseDelayMs": 2000, "maxAgentDelayMs": 60000})
    compaction: dict[str, Any] = field(
        default_factory=lambda: {
            "enabled": True,
            "reserveTokens": 16384,
            "keepRecentTokens": 20000,
            "backgroundTokens": 32768,
        }
    )
    tool_execution: str = "parallel"
    steering_mode: str = "one-at-a-time"
    follow_up_mode: str = "one-at-a-time"
    context_retention_ms: int = 600_000
    document_retention_ms: int = 600_000
    after_failed_run: str = "keep"
    restart_limit: int = 5


CAMEL_KEYS = {
    "toolExecution": "tool_execution",
    "steeringMode": "steering_mode",
    "followUpMode": "follow_up_mode",
    "contextRetentionMs": "context_retention_ms",
    "documentRetentionMs": "document_retention_ms",
    "afterFailedRun": "after_failed_run",
    "restartLimit": "restart_limit",
}


def resolve_settings(source: Any) -> HarnessSettings:
    """Resolve a settings object, mapping, or function into a fresh HarnessSettings."""
    if source is None:
        return HarnessSettings()
    if callable(source):
        return resolve_settings(source())
    if isinstance(source, HarnessSettings):
        return source
    if isinstance(source, Mapping):
        defaults = HarnessSettings()
        values = dict(defaults.__dict__)
        for key, value in source.items():
            values[CAMEL_KEYS.get(str(key), str(key))] = value
        unknown = set(values) - set(defaults.__dict__)
        if unknown:
            raise TypeError(f"Unknown harness setting(s): {', '.join(sorted(unknown))}")
        return HarnessSettings(**values)
    # Attribute-style object (getters are evaluated now).
    defaults = HarnessSettings()
    values = dict(defaults.__dict__)
    for name in values:
        if hasattr(source, name):
            values[name] = getattr(source, name)
    if hasattr(source, "toolExecution"):
        values["tool_execution"] = source.toolExecution
    if hasattr(source, "steeringMode"):
        values["steering_mode"] = source.steeringMode
    if hasattr(source, "followUpMode"):
        values["follow_up_mode"] = source.followUpMode
    if hasattr(source, "afterFailedRun"):
        values["after_failed_run"] = source.afterFailedRun
    if hasattr(source, "restartLimit"):
        values["restart_limit"] = source.restartLimit
    return HarnessSettings(**values)


__all__ = ["HarnessSettings", "resolve_settings"]

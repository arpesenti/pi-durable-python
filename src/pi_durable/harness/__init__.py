"""Harness surface: agents, tools, submissions, and the durable generation loop."""

from ..tasks import Task, TaskDefinition, TaskFaulted, TaskRuntime, define_task
from .agent import Agent, configure, render_system_message, resolve_agent
from .docs import AgentDoc, InboxDoc, LiveDoc, ProviderDoc, UsageDoc
from .generation import (
    FauxModel,
    GenerationTask,
    Model,
    ModelRequest,
    ModelResponse,
    Models,
    ModelUnavailable,
    ToolCallApi,
    ToolResult,
    ToolTask,
    faux_assistant_message,
    faux_tool_call,
)
from .harness import Conversation, DocumentReader, Harness, HarnessInspection
from .registry import (
    Extension,
    Registry,
    RegistrySnapshot,
    Section,
    Tool,
    create_registry,
    define_extension,
    define_tool,
    section,
)
from .settings import HarnessSettings, resolve_settings
from .submissions import Submission

__all__ = [
    "Agent",
    "AgentDoc",
    "Conversation",
    "DocumentReader",
    "Extension",
    "FauxModel",
    "GenerationTask",
    "Harness",
    "HarnessInspection",
    "HarnessSettings",
    "InboxDoc",
    "LiveDoc",
    "Model",
    "ModelRequest",
    "ModelResponse",
    "ModelUnavailable",
    "Models",
    "ProviderDoc",
    "Registry",
    "RegistrySnapshot",
    "Section",
    "Submission",
    "Task",
    "TaskDefinition",
    "TaskFaulted",
    "TaskRuntime",
    "Tool",
    "ToolCallApi",
    "ToolResult",
    "ToolTask",
    "UsageDoc",
    "configure",
    "create_registry",
    "define_extension",
    "define_task",
    "define_tool",
    "faux_assistant_message",
    "faux_tool_call",
    "render_system_message",
    "resolve_agent",
    "resolve_settings",
    "section",
]

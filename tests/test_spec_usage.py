"""Translation of the upstream ``spec-usage`` suite: the spec's examples compile.

Upstream compiles ``docs/spec.md``'s usage blocks with TypeScript and asserts only that the
example function exists. Python has no equivalent of ``expectTypeOf``, so the examples are
ordinary typed code that CI's mypy run checks; the test itself asserts the translation exists.
Behavioral coverage for every pattern lives in the suite files named in
``docs/parity-report.md``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Mapping
from typing import Any, cast

from pi_durable import (
    DEFAULT_COMPACTION_POLICY,
    AgentDoc,
    CompactionTask,
    ContextView,
    GenerationTask,
    Harness,
    LiveDoc,
    MemoryStorage,
    Session,
    ToolTask,
    configure,
    define_doc,
    define_extension,
    define_task,
    define_tool,
    hook,
    section,
    wrap_tool,
)
from pi_durable.context import Context
from pi_durable.session.transaction import Transaction
from pi_durable.tools import create_bash_tool, create_edit_tool, create_read_tool
from pi_durable.types import EntryDraft, EntryId, Storage

# ─── Names the spec leaves to the application ────────────────────────────────

storage: Storage = MemoryStorage()
models: Any = None
session: Session = cast(Session, object())
conversation_id: Any = 1
message: EntryDraft = {"kind": "app.message"}
haiku: Mapping[str, str] = {"provider": "anthropic", "modelId": "claude-haiku"}
sonnet: Mapping[str, str] = {"provider": "anthropic", "modelId": "claude-sonnet"}
worktree = "/work"
name = "reviewer"
context: Context = cast(Context, object())


def local_env(cwd: str) -> Any:
    return cwd


def render_agents_md() -> str | None:
    return None


def render_skills() -> str | None:
    return None


def is_dangerous(call: Any) -> bool:
    return False


def writes(call: Any) -> bool:
    return False


def request_second_pass(
    answer: Any, api: Any, context: Context
) -> Awaitable[Mapping[str, Any] | None]:
    async def decide() -> Mapping[str, Any] | None:
        return None

    return decide()


def answer_text(api: Any, entry: EntryId, context: Context) -> Awaitable[str]:
    async def read() -> str:
        return ""

    return read()


class SettingsManager:
    def get(self, key: str) -> Any:
        return None

    def set(self, key: str, value: Any) -> None:
        pass


manager = SettingsManager()


# ─── Examples ────────────────────────────────────────────────────────────────


def examples() -> dict[str, Any]:
    """The spec's usage examples, type-checked and never run."""

    read_tool = create_read_tool()
    edit_tool = create_edit_tool()
    bash_tool = create_bash_tool()

    # Section 7.1: extensions and host setup.
    ContextFiles = define_extension(
        {"name": "context-files", "sections": [section("agents-md", render_agents_md)]}
    )
    Skills = define_extension({"name": "skills", "sections": [section("skills", render_skills)]})
    Coding = define_extension(
        {
            "name": "coding",
            "sections": [
                section("preamble", lambda: "You are an expert coding assistant.", {"tag": False}),
                section(
                    "cwd",
                    lambda input: input.env and f"Working directory: {input.env.cwd}",
                ),
            ],
        }
    )
    Permissions = define_extension(
        {
            "name": "permissions",
            "hooks": [
                hook(
                    ToolTask,
                    {
                        "beforeTool": lambda call: {"block": "Needs approval"}
                        if is_dangerous(call)
                        else None
                    },
                )
            ],
        }
    )
    Reviewer = define_extension(
        {
            "name": "reviewer",
            "sections": [
                section(
                    "role",
                    lambda: "You review diffs. Report problems as a list. Never edit files.",
                )
            ],
            "hooks": [hook(GenerationTask, {"onYield": request_second_pass})],
        }
    )

    def time_bash(tool: Any) -> Any:
        def execute(args: Any, api: Any, context: Context) -> Awaitable[Any]:
            async def run() -> Any:
                return await tool.execute(args, api, context)

            return run()

        return {**tool, "execute": execute}

    Timing = define_extension({"name": "timing", "wraps": [wrap_tool(bash_tool, time_bash)]})
    CodingTools = define_extension(
        {"name": "coding-tools", "tools": [read_tool, edit_tool, bash_tool]}
    )
    # A bash inside a Python virtualenv for one conversation.
    Venv = define_extension(
        {
            "name": "venv",
            "tools": [create_bash_tool({"command_prefix": "source .venv/bin/activate"})],
        }
    )

    # Section 7.2: hooks reading extension state.
    PlanModeDoc = define_doc(
        {
            "kind": "app.plan-mode",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"enabled": False},
        }
    )
    PlanMode = define_extension(
        {
            "name": "plan-mode",
            "hooks": [
                hook(
                    ToolTask,
                    {
                        "beforeTool": lambda call, api, context: _plan_mode_block(
                            call, api, context, PlanModeDoc
                        )
                    },
                )
            ],
        }
    )

    # Section 7.3: subagents.
    async def subagent(args: Mapping[str, Any], api: Any, call_context: Context) -> Any:
        async def create(tx: Transaction) -> Any:
            existing = (await tx.scan_conversations({"ownerTaskId": api.task_id}, 1))["items"]
            if existing:
                return existing[0]["id"]
            created = await tx.create_conversation(
                {"ownership": {"kind": "task", "taskId": api.task_id}}
            )
            await configure(tx, created["id"], {"extensions": {"remove": [Subagent]}})
            return created["id"]

        child = await api.commit(create, call_context)
        await api.details({"conversationId": child}, call_context)
        handle = await api.conversation(child, call_context)
        submission = await handle.submit(
            {
                "type": "input",
                "content": args["task"],
                "requestId": f"subagent:{api.task_id}",
            },
            call_context,
        )
        settled = await submission.wait(call_context)
        if settled["status"] != "done":
            raise RuntimeError(f"Subagent failed: {settled['status']}")
        text = await answer_text(api, settled["answer"], call_context)
        return {"output": [{"type": "text", "text": text}]}

    Subagent = define_extension(
        {
            "name": "subagent",
            "tools": [
                define_tool(
                    {
                        "name": "subagent",
                        "description": "Delegate a self-contained task to a subagent.",
                        "parameters": {
                            "type": "object",
                            "properties": {"task": {"type": "string"}},
                        },
                        "replay": "safe",
                        "execute": subagent,
                    }
                )
            ],
        }
    )

    # Section 5.1: a task with intent, effect, outcome.
    def payment() -> Any:
        def initial(_input: Any) -> dict[str, Any]:
            return {"phase": "prepare"}

        def prepare(_task: Any, runtime: Any, context: Context) -> Awaitable[None]:
            async def commit() -> None:
                await runtime.commit(
                    lambda: {
                        "status": "running",
                        "checkpoint": {"phase": "charge", "key": "key"},
                    },
                    context,
                )

            return commit()

        def charge(task: Any, runtime: Any, context: Context) -> Awaitable[None]:
            async def commit() -> None:
                async def change(tx: Transaction, current: Any) -> Mapping[str, Any]:
                    entry = await tx.append_entry(
                        current["conversationId"], {"kind": "app.receipt", "data": {}}
                    )
                    return {
                        "status": "terminal",
                        "outcome": {"status": "completed", "result": {"entryId": entry["id"]}},
                    }

                await runtime.commit(change, context)

            return commit()

        def abort(_task: Any, runtime: Any, context: Context) -> Awaitable[None]:
            async def commit() -> None:
                await runtime.commit(
                    lambda: {
                        "status": "terminal",
                        "outcome": {"status": "aborted", "reason": "user"},
                    },
                    context,
                )

            return commit()

        return define_task(
            {
                "name": "app.payment",
                "version": 1,
                "initial": initial,
                "phases": {"prepare": prepare, "charge": charge},
                "abort": abort,
            }
        )

    Payment = payment()

    # Section 2.2: settings read live through getters.
    class UserSettings:
        def __init__(self, manager: SettingsManager) -> None:
            self.manager = manager

        @property
        def stream(self) -> Mapping[str, Any]:
            return {"timeout_ms": self.manager.get("timeoutMs")}

        @property
        def compaction(self) -> Mapping[str, Any]:
            return {"enabled": self.manager.get("autoCompact")}

    # Section 2.2: an environment per conversation.
    ContainerDoc = define_doc(
        {
            "kind": "app.container",
            "version": 1,
            "scope": "conversation",
            "history": "latest",
            "fork": "current",
            "initial": lambda: {"image": "python:3.13"},
        }
    )

    async def container_env(
        target: Any, read: Any, read_context: Context
    ) -> Any:
        container = await read.snapshot(ContainerDoc, target["conversation_id"], read_context)
        return local_env(target.get("cwd") or "/work") if container is None else container["image"]

    # Section 4: table reads before the first table write; documents stay usable.
    async def table_rules() -> None:
        async def change(tx: Transaction) -> None:
            conversation = await tx.conversation(conversation_id)
            live = await tx.doc(LiveDoc, conversation_id)
            await tx.append_entry(conversation_id, message)
            del live["generation"]
            await tx.create_task(
                GenerationTask,
                {},
                {"ownership": {"kind": "conversation"}, "conversationId": conversation_id},
            )
            del conversation

        await session.commit(change, context)

    # Section 3.4: drafts are revoked after their commit.
    async def revoked_draft() -> None:
        escaped: dict[str, Any] = {}

        async def change(tx: Transaction) -> None:
            escaped["draft"] = await tx.doc(LiveDoc, conversation_id)

        await session.commit(change, context)
        escaped["draft"]["generation"] = None  # throws: the draft was revoked

    def host() -> Awaitable[Harness]:
        async def open() -> Harness:
            namespace: Any = None
            for extension in (CodingTools, Coding, ContextFiles, Skills, Permissions, Reviewer):
                namespace.install(extension)
            harness = await Harness.open(
                storage,
                {
                    "models": models,
                    "registry": namespace,
                    "settings": {
                        "extensions": [CodingTools, Coding, ContextFiles, Skills, Permissions]
                    },
                    "env": lambda target: local_env(target.get("cwd") or "."),
                },
                context,
            )
            root = await harness.root(
                context,
                {"agent": {"model": sonnet, "cwd": worktree}},
            )
            await root.configure({"tools": {"remove": [edit_tool]}}, context)
            await root.configure({"tools": {"remove": [bash_tool]}}, context)
            await root.configure({"tools": None}, context)
            namespace.install(Timing)
            namespace.install(Venv)
            await root.configure({"extensions": {"add": [Venv]}}, context)
            namespace.install(PlanMode)
            await root.commit(
                lambda tx: _enable_plan_mode(tx, root.id, PlanModeDoc), context
            )
            namespace.install(Subagent)
            return harness

        return open()

    return {
        "Payment": Payment,
        "ContextFiles": ContextFiles,
        "Skills": Skills,
        "Coding": Coding,
        "Permissions": Permissions,
        "Reviewer": Reviewer,
        "Timing": Timing,
        "Venv": Venv,
        "PlanMode": PlanMode,
        "Subagent": Subagent,
        "CodingTools": CodingTools,
        "AgentDoc": AgentDoc,
        "CompactionTask": CompactionTask,
        "DEFAULT_COMPACTION_POLICY": DEFAULT_COMPACTION_POLICY,
        "context_view": ContextView,
        "sequences": {
            "table_rules": table_rules,
            "revoked_draft": revoked_draft,
            "container_env": container_env,
            "host": host,
            "live_settings": lambda: UserSettings(manager),
        },
    }


async def _plan_mode_block(
    call: Any, api: Any, context: Context, token: Any
) -> Mapping[str, Any] | None:
    state = await api.snapshot(token, api.conversation_id, context)
    return {"block": "Plan mode: read-only"} if state and state["enabled"] and writes(call) else None


async def _enable_plan_mode(tx: Transaction, id: Any, token: Any) -> None:
    document = await tx.doc(token, id)
    document["enabled"] = True


def test_compiles_the_specs_usage_examples() -> None:
    import inspect

    assert callable(examples)
    assert inspect.signature(examples).parameters == {}

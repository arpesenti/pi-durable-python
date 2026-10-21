"""Built-in durable generation and tool tasks, plus the model boundary.

Generation is a durable state machine: one phase per model turn. Tool calls become child
``pi.tool`` tasks; the generation waits on them and resumes at its next turn. Every
assistant message and tool result is committed before the next request.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..context import assistant_message, text_content, tool_result_message
from ..json import copy_json, error_message
from ..tasks import define_task
from ..types import TaskOptions
from .agent import Agent, render_system_message
from .docs import InboxDoc, LiveDoc, UsageDoc, add_usage
from .submissions import apply_boundary, withdraw_queued_inputs


class ModelUnavailable(RuntimeError):
    pass


@dataclass
class ModelRequest:
    messages: list[dict[str, Any]]
    tools: list[dict[str, Any]]
    provider: str
    model_id: str
    thinking_level: str = "off"
    session_id: str | None = None
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelResponse:
    message: dict[str, Any]
    usage: dict[str, Any] | None = None


class Model:
    """A provider model. Hosts adapt their SDK to ``complete()``."""

    async def complete(self, request: ModelRequest) -> ModelResponse:
        raise NotImplementedError


class Models:
    """A mutable catalog of provider models, resolved at each use."""

    def __init__(self) -> None:
        self._models: dict[tuple[str, str], Model] = {}

    def set(self, provider: str, model_id: str, model: Model) -> Models:
        self._models[(provider, model_id)] = model
        return self

    def add(self, provider: str, model_id: str, model: Model) -> Models:
        return self.set(provider, model_id, model)

    def get(self, provider: str, model_id: str) -> Model:
        model = self._models.get((provider, model_id))
        if model is None:
            raise ModelUnavailable(f"Model {provider}/{model_id} is not configured")
        return model


def resolve_model(models: Any, provider: str, model_id: str) -> Model:
    if callable(models) and not hasattr(models, "get"):
        models = models()
    if hasattr(models, "get") and not isinstance(models, Mapping):
        try:
            return models.get(provider, model_id)
        except TypeError:
            return models.get(f"{provider}/{model_id}")
    if isinstance(models, Mapping):
        model = models.get((provider, model_id)) or models.get(f"{provider}/{model_id}")
        if model is None:
            raise ModelUnavailable(f"Model {provider}/{model_id} is not configured")
        return model
    if hasattr(models, "complete"):
        return models
    raise ModelUnavailable("No model catalog is configured")


class FauxModel(Model):
    """Deterministic scripted model for tests.

    A step is a string (answer text), an assistant message dict, a ``ModelResponse``, or a
    callable receiving the request.
    """

    def __init__(self, responses: Sequence[Any] | None = None) -> None:
        self.responses: list[Any] = list(responses or [])
        self.requests: list[ModelRequest] = []

    def set_responses(self, responses: Sequence[Any]) -> FauxModel:
        self.responses = list(responses)
        return self

    def push(self, *responses: Any) -> FauxModel:
        self.responses.extend(responses)
        return self

    async def complete(self, request: ModelRequest) -> ModelResponse:
        self.requests.append(request)
        if not self.responses:
            raise RuntimeError("FauxModel has no response left")
        step = self.responses.pop(0)
        if callable(step):
            step = step(request)
            if inspect.isawaitable(step):
                step = await step
        if isinstance(step, ModelResponse):
            return step
        if isinstance(step, str):
            return ModelResponse(assistant_message([text_content(step)], stop_reason="stop", timestamp=0))
        if isinstance(step, Mapping):
            return ModelResponse(copy_json(step), step.get("usage") if isinstance(step, Mapping) else None)
        raise TypeError(f"Unsupported faux response {step!r}")


def faux_assistant_message(
    text: str | None = None,
    *,
    tool_calls: Sequence[Mapping[str, Any]] | None = None,
    stop_reason: str = "stop",
    usage: Mapping[str, Any] | None = None,
    timestamp: int = 0,
) -> dict[str, Any]:
    content: list[dict[str, Any]] = []
    if text is not None:
        content.append(text_content(text))
    content.extend(copy_json(list(tool_calls or [])))
    message = assistant_message(content, stop_reason=stop_reason, timestamp=timestamp)
    if usage is not None:
        message["usage"] = copy_json(usage)
    return message


def faux_tool_call(call_id: str, name: str, arguments: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"type": "toolCall", "id": call_id, "name": name, "arguments": copy_json(dict(arguments or {}))}


@dataclass
class ToolResult:
    output: Any = None
    is_error: bool = False
    details: Any = None
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    usage: dict[str, Any] | None = None


class ToolCallApi:
    """Operations available to one tool invocation."""

    def __init__(self, harness: Any, runtime: Any, task: Mapping[str, Any], agent: Agent, env: Any) -> None:
        self._harness = harness
        self._runtime = runtime
        self.task_id = task["id"]
        self.conversation_id = task["conversationId"]
        self.call_id = task["input"]["call"]["id"]
        self.registry = runtime.registry
        self.agent = agent
        self.models = harness.models
        self.env = env
        self._chunks: list[str] = []
        self._diagnostics: list[dict[str, Any]] = []
        self._details: Any = None

    def now(self) -> int:
        return self._runtime.now()

    def output(self, chunk: str, skipped: int = 0) -> None:
        self._chunks.append(str(chunk))

    def diagnostic(self, diagnostic: Mapping[str, Any]) -> None:
        self._diagnostics.append(copy_json(dict(diagnostic)))

    def details(self, value: Any) -> None:
        self._details = copy_json(value)

    def retained_output(self) -> dict[str, Any]:
        return {"text": "".join(self._chunks), "truncated": False}

    @property
    def diagnostics(self) -> list[dict[str, Any]]:
        return self._diagnostics

    @property
    def collected_output(self) -> str:
        return "".join(self._chunks)

    async def snapshot(self, token: Any, *args: Any) -> Any:
        return await self._harness.snapshot(token, *args)

    async def snapshot_as_of(self, token: Any, conversation_id: int, *args: Any) -> Any:
        return await self._harness.snapshot_as_of(token, conversation_id, *args)

    async def watch_doc(self, token: Any, *args: Any) -> Any:
        return await self._harness.watch_doc(token, *args)

    async def conversation(self, conversation_id: int) -> Any:
        return await self._harness.conversation(conversation_id)

    async def execute_tool(self, name: str, arguments: Mapping[str, Any], options: Mapping[str, Any] | None = None) -> Any:
        raise NotImplementedError(
            "Nested tool calls are not implemented in this port; call the tool directly instead"
        )


def _normalize_usage(usage: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not usage:
        return None
    normalized: dict[str, Any] = {}
    for key in ("input", "output", "total"):
        value = usage.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            normalized[key] = value
    cost = usage.get("cost")
    if isinstance(cost, (int, float)) and not isinstance(cost, bool):
        normalized["cost"] = cost
    elif isinstance(cost, Mapping):
        total = cost.get("total")
        if isinstance(total, (int, float)) and not isinstance(total, bool):
            normalized["cost"] = total
    return normalized or None


def _normalize_response(response: Any, timestamp: int) -> tuple[dict[str, Any], dict[str, Any] | None]:
    if isinstance(response, ModelResponse):
        message, usage = response.message, response.usage
    elif isinstance(response, Mapping) and response.get("role") == "assistant":
        message, usage = dict(response), response.get("usage")
    elif isinstance(response, Mapping) and "message" in response:
        message, usage = dict(response["message"]), response.get("usage")
    else:
        raise TypeError(f"Unsupported model response {response!r}")
    message = copy_json(message)
    message.setdefault("timestamp", timestamp)
    message.setdefault("stopReason", "stop")
    return message, _normalize_usage(usage if usage is not None else message.get("usage"))


def _usage_key(agent: Agent) -> str:
    model = agent.model or {}
    return f"{model.get('provider', 'unknown')}/{model.get('modelId', 'unknown')}"


def _record_usage(tx: Any, conversation_id: int, usage: Mapping[str, Any] | None, key: str) -> Any:
    # Called inside a document-aware transaction; returns the usage draft for the caller.
    return None


async def _add_usage(tx: Any, conversation_id: int, usage: Mapping[str, Any] | None, key: str) -> None:
    if not usage:
        return
    doc = await tx.doc(UsageDoc, conversation_id)
    models = doc["models"]
    if key not in models:
        models[key] = {"input": 0, "output": 0, "total": 0, "cost": 0.0}
    add_usage(models[key], usage)


def _render_tool_content(output: Any, is_error: bool, diagnostics: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if output is None:
        content: list[dict[str, Any]] = []
    elif isinstance(output, str):
        content = [text_content(output)] if output else []
    elif isinstance(output, list):
        content = copy_json(output)
    else:
        content = [text_content(json.dumps(output, ensure_ascii=False))]
    if is_error and not content:
        content = [text_content("Tool call failed.")]
    if diagnostics:
        lines = []
        for diagnostic in diagnostics:
            code = f" ({diagnostic['code']})" if diagnostic.get("code") else ""
            lines.append(f"[{diagnostic.get('severity', 'info')}]{code} {diagnostic.get('message', '')}")
        block = "\n".join(lines)
        if content and content[-1].get("type") == "text":
            content[-1] = {**content[-1], "text": f"{content[-1]['text']}\n\n{block}"}
        else:
            content.append(text_content(block))
    return content


async def _appended_tool_result(
    runtime: Any,
    task: Mapping[str, Any],
    *,
    output: Any,
    is_error: bool,
    details: Any,
    diagnostics: Sequence[Mapping[str, Any]],
    usage: Mapping[str, Any] | None,
) -> int:
    usage = _normalize_usage(usage)
    conversation_id = task["conversationId"]
    call = task["input"]["call"]
    key = f"{task['input'].get('toolName', call.get('name'))}"

    async def commit(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
        content = _render_tool_content(output, is_error, diagnostics)
        entry = await tx.append_entry(
            conversation_id,
            {
                "kind": "pi.tool-result",
                "model": [
                    tool_result_message(
                        call["id"],
                        call.get("name"),
                        content,
                        is_error=is_error,
                        details=details,
                        timestamp=runtime.now(),
                    )
                ],
                "data": {"diagnostics": copy_json(list(diagnostics))},
            },
        )
        if usage:
            doc = await tx.doc(UsageDoc, conversation_id)
            if key not in doc["tools"]:
                doc["tools"][key] = {"input": 0, "output": 0, "total": 0, "cost": 0.0}
            add_usage(doc["tools"][key], usage)
        return {
            "status": "terminal",
            "outcome": {"status": "completed", "result": {"entryId": entry["id"]}},
        }

    await runtime.commit(commit)
    return 0


async def generation_turn(task: dict[str, Any], runtime: Any) -> None:
    harness = runtime._scheduler.harness
    conversation_id = task["conversationId"]
    checkpoint = task["state"].get("checkpoint") or {}
    runtime.check_active()
    agent = await harness.resolve_agent(conversation_id)
    if agent.model is None:
        await _fail_run(harness, runtime, task, "no_model", "This conversation has no model")
        return
    env = await harness.build_env(conversation_id, agent)
    view = await harness.read_context(conversation_id)
    system = await render_system_message(
        agent,
        view["messages"],
        context={"conversationId": conversation_id, "env": env, "agent": agent},
        timestamp=runtime.now(),
        report=lambda error, info=None: harness.report(error, info or {"type": "extension_error"}),
    )
    if system is not None:
        prepared = {**checkpoint, "prepared": int(checkpoint.get("prepared") or 0) + 1}

        async def write_system(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
            await tx.append_entry(conversation_id, {"kind": "pi.system", "model": [system]})
            return {"status": "running", "checkpoint": prepared}

        await runtime.commit(write_system)
        view = await harness.read_context(conversation_id)

    session_id = await harness.provider_session_id(conversation_id)
    request = ModelRequest(
        messages=view["messages"],
        tools=[tool.declaration for tool in agent.tools],
        provider=str(agent.model.get("provider", "")),
        model_id=str(agent.model.get("modelId", "")),
        thinking_level=agent.thinking_level,
        session_id=session_id,
        options=copy_json(getattr(harness.settings, "stream", {}) or {}),
    )
    try:
        response = await harness.complete(agent, request)
        message, usage = _normalize_response(response, runtime.now())
    except BaseException as error:  # noqa: BLE001 - model failure becomes a durable run outcome
        await _fail_run(harness, runtime, task, "model_error", error_message(error))
        return
    calls = [item for item in message.get("content") or [] if item.get("type") == "toolCall"]
    placed = await harness.placed_input_ids(conversation_id)
    settings = harness.settings
    active_start = await harness.active_start(conversation_id)

    if calls:

        async def commit_tools(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
            entry = await tx.append_entry(
                conversation_id, {"kind": "pi.assistant", "model": [message]}
            )
            await _add_usage(tx, conversation_id, usage, _usage_key(agent))
            task_ids: list[int] = []
            slots: list[dict[str, Any]] = []
            for call in calls:
                tool_task = await tx.create_task(
                    ToolTask,
                    {
                        "call": copy_json(call),
                        "assistantEntryId": entry["id"],
                        "toolName": call.get("name"),
                    },
                    TaskOptions(ownership={"kind": "task", "taskId": task["id"]}),
                )
                task_ids.append(tool_task)
                slots.append(
                    {
                        "taskId": tool_task,
                        "callId": call.get("id"),
                        "name": call.get("name"),
                        "status": "running",
                    }
                )
            await apply_boundary(
                tx,
                conversation_id=conversation_id,
                final=False,
                settings=settings,
                now=runtime.now(),
                active_start=active_start,
            )
            live = await tx.doc(LiveDoc, conversation_id)
            live["tools"] = slots
            return {
                "status": "waiting",
                "checkpoint": {**checkpoint, "round": int(checkpoint.get("round") or 0) + 1},
                "on": task_ids,
                "policy": "allSettled",
            }

        await runtime.commit(commit_tools)
        return

    async def commit_final(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
        entry = await tx.append_entry(conversation_id, {"kind": "pi.assistant", "model": [message]})
        await _add_usage(tx, conversation_id, usage, _usage_key(agent))
        for submission_id in placed:
            tx.settle_submission(submission_id, {"status": "done", "answer": entry["id"]})
        boundary = await apply_boundary(
            tx,
            conversation_id=conversation_id,
            final=True,
            settings=settings,
            now=runtime.now(),
            active_start=active_start,
        )
        live = await tx.doc(LiveDoc, conversation_id)
        live.pop("tools", None)
        if boundary["inputs"]:
            next_task = await tx.create_task(
                GenerationTask,
                {"submissionIds": boundary["inputs"]},
                TaskOptions(ownership={"kind": "conversation"}),
            )
            live["run"] = {"taskId": next_task, "inputs": boundary["inputs"]}
        else:
            live.pop("run", None)
        return {
            "status": "terminal",
            "outcome": {"status": "completed", "result": {"entryId": entry["id"]}},
        }

    await runtime.commit(commit_final)


async def _fail_run(
    harness: Any,
    runtime: Any,
    task: Mapping[str, Any],
    reason: str,
    message: str,
) -> None:
    conversation_id = task["conversationId"]
    placed = await harness.placed_input_ids(conversation_id)
    settings = harness.settings

    async def commit(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
        for submission_id in placed:
            tx.settle_submission(
                submission_id,
                {
                    "status": "unanswered",
                    "reason": reason,
                    "detail": {"message": message},
                },
            )
        live = await tx.doc(LiveDoc, conversation_id)
        live.pop("tools", None)
        live.pop("run", None)
        inbox = await tx.doc(InboxDoc, conversation_id)
        if settings.after_failed_run == "withdraw":
            await withdraw_queued_inputs(tx, conversation_id, reason="run_failed")
        elif settings.after_failed_run == "continue" and inbox.get("items"):
            boundary = await apply_boundary(
                tx,
                conversation_id=conversation_id,
                final=True,
                settings=settings,
                now=runtime.now(),
                active_start=await harness.active_start(conversation_id),
            )
            if boundary["inputs"]:
                next_task = await tx.create_task(
                    GenerationTask,
                    {"submissionIds": boundary["inputs"]},
                    TaskOptions(ownership={"kind": "conversation"}),
                )
                live["run"] = {"taskId": next_task, "inputs": boundary["inputs"]}
        return {
            "status": "terminal",
            "outcome": {"status": "failed", "error": {"message": message}},
        }

    await runtime.commit(commit)


async def generation_abort(task: dict[str, Any], runtime: Any) -> None:
    harness = runtime._scheduler.harness
    conversation_id = task["conversationId"]
    placed = await harness.placed_input_ids(conversation_id)

    async def commit(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
        for submission_id in placed:
            tx.settle_submission(submission_id, {"status": "unanswered", "reason": "aborted"})
        await withdraw_queued_inputs(tx, conversation_id, reason="aborted")
        live = await tx.doc(LiveDoc, conversation_id)
        live.pop("tools", None)
        live.pop("run", None)
        return {"status": "terminal", "outcome": {"status": "aborted", "reason": "aborted"}}

    await runtime.commit(commit)


async def _execute_tool(task: dict[str, Any], runtime: Any) -> None:
    harness = runtime._scheduler.harness
    conversation_id = task["conversationId"]
    call = task["input"]["call"]
    runtime.check_active()
    agent = await harness.resolve_agent(conversation_id)
    tool = next(
        (candidate for candidate in agent.callable_tools if candidate.name == call.get("name")),
        None,
    )
    if tool is None:
        await _appended_tool_result(
            runtime,
            task,
            output=f"Tool {call.get('name')} is not available.",
            is_error=True,
            details=None,
            diagnostics=[
                {
                    "severity": "error",
                    "message": f"Tool {call.get('name')} is not available.",
                    "code": "tool_unavailable",
                }
            ],
            usage=None,
        )
        return
    started = await runtime.memo("started")
    if started is not None and tool.replay != "safe":
        await _appended_tool_result(
            runtime,
            task,
            output="Tool call interrupted by a restart; it was not re-run because it is not replay-safe.",
            is_error=True,
            details=None,
            diagnostics=[
                {
                    "severity": "error",
                    "message": "Tool call interrupted by a restart.",
                    "code": "interrupted",
                }
            ],
            usage=None,
        )
        return
    await runtime.memo("started", True)
    env = await harness.build_env(conversation_id, agent)
    api = ToolCallApi(harness, runtime, task, agent, env)
    arguments = call.get("arguments") if isinstance(call.get("arguments"), dict) else {}
    diagnostics: list[dict[str, Any]] = []
    usage: dict[str, Any] | None = None
    details: Any = None
    is_error = False
    output: Any = None
    try:
        result = tool.execute(copy_json(arguments), api)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, ToolResult):
            output = result.output
            is_error = result.is_error
            details = result.details
            diagnostics.extend(result.diagnostics)
            usage = result.usage
        else:
            output = result
    except BaseException as error:  # noqa: BLE001 - a tool throw becomes an error result
        is_error = True
        output = f"{call.get('name')} failed: {error_message(error)}"
        diagnostics.append({"severity": "error", "message": error_message(error), "code": "tool_error"})
    diagnostics = [*api.diagnostics, *diagnostics]
    if details is None and api._details is not None:
        details = api._details
    if not isinstance(output, (str, list, type(None))):
        output = json.dumps(output, ensure_ascii=False)
    await _appended_tool_result(
        runtime,
        task,
        output=output if output is not None else api.collected_output,
        is_error=is_error,
        details=details,
        diagnostics=diagnostics,
        usage=usage,
    )


async def tool_abort(task: dict[str, Any], runtime: Any) -> None:
    """Commit an aborted tool result, then end the tool task aborted."""
    conversation_id = task["conversationId"]
    call = task["input"]["call"]

    async def commit(tx: Any, current: dict[str, Any]) -> dict[str, Any]:
        entry = await tx.append_entry(
            conversation_id,
            {
                "kind": "pi.tool-result",
                "model": [
                    tool_result_message(
                        call["id"],
                        call.get("name"),
                        [text_content("Tool call aborted.")],
                        is_error=True,
                        timestamp=runtime.now(),
                    )
                ],
                "data": {
                    "diagnostics": [
                        {"severity": "error", "message": "Tool call aborted.", "code": "aborted"}
                    ]
                },
            },
        )
        return {
            "status": "terminal",
            "outcome": {"status": "aborted", "reason": "aborted", "result": {"entryId": entry["id"]}},
        }

    await runtime.commit(commit)


GenerationTask = define_task(
    name="pi.generation",
    version=1,
    initial=lambda input: {
        "phase": "turn",
        "round": 0,
        "submissionIds": copy_json(list((input or {}).get("submissionIds") or [])),
    },
    phases={"turn": generation_turn},
    abort=generation_abort,
)


ToolTask = define_task(
    name="pi.tool",
    version=1,
    initial=lambda input: {"phase": "execute"},
    phases={"execute": _execute_tool},
    abort=tool_abort,
)


__all__ = [
    "FauxModel",
    "GenerationTask",
    "Model",
    "ModelRequest",
    "ModelResponse",
    "ModelUnavailable",
    "Models",
    "ToolCallApi",
    "ToolResult",
    "ToolTask",
    "faux_assistant_message",
    "faux_tool_call",
    "resolve_model",
]

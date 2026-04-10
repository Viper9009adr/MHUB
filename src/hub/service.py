"""gRPC HubService servicer implementation."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict
from collections.abc import AsyncIterator, Iterator
from typing import Any

import grpc
import grpc.aio

from src.hub.hub_pb2 import (
    CreateHubRequest,
    CreateHubResponse,
    TerminateHubRequest,
    TerminateHubResponse,
    JoinHubRequest,
    JoinHubEvent,
    TapHubRequest,
    TapHubResponse,
    CreateCheckpointRequest,
    CreateCheckpointResponse,
    RollbackCheckpointRequest,
    RollbackCheckpointResponse,
    HubStatusRequest,
    HubStatusResponse,
    DetectForkRequest,
    DetectForkResponse,
    DecideForkRequest,
    DecideForkResponse,
    AgentMessage,
    OrchEvent,
)
from src.hub.hub_pb2_grpc import HubServiceServicer
from src.hub.stream_router import StreamRouter
from src.storage.models import HubRecord, CheckpointRecord, ForkScenarioRecord
from src.orc.fork_handler import ForkHandler, DivergenceEvent
from src.orc.dispatch import resolve_pre_llm_hub_intent

logger = logging.getLogger(__name__)

ORC_PROMPT = (
    "You are ORC. Restate the user task in 1-2 sentences and define the final answer shape."
)
ARC_PROMPT = (
    "You are ARC. Produce a concise execution plan in 3-5 bullets for the task."
)
CRT_PROMPT = (
    "You are CRT. Review ARC's plan with ORC context. Reply with APPROVED or "
    "REJECTED: <reason>, then one short rationale."
)


class OrchEventBus:
    """In-process async pub/sub bus for OrchEvent frames."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[OrchEvent]] = set()

    async def publish(self, event: OrchEvent) -> None:
        """Publish an event to all active subscribers."""
        if not self._subscribers:
            return
        stale: list[asyncio.Queue[OrchEvent]] = []
        for queue in self._subscribers:
            try:
                queue.put_nowait(event)
            except Exception:
                stale.append(queue)
        for queue in stale:
            self._subscribers.discard(queue)

    def subscribe(self) -> asyncio.Queue[OrchEvent]:
        """Create and register a subscriber queue."""
        queue: asyncio.Queue[OrchEvent] = asyncio.Queue()
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[OrchEvent]) -> None:
        """Remove a subscriber queue."""
        self._subscribers.discard(queue)


class OrchEventStream:
    """Async iterator wrapper that supports concurrent __anext__ calls."""

    def __init__(
        self,
        bus: OrchEventBus,
        request_iterator: AsyncIterator[OrchEvent] | Iterator[OrchEvent],
    ) -> None:
        self._bus = bus
        self._request_iterator = request_iterator
        self._queue = bus.subscribe()
        self._closed = False
        self._next_lock = asyncio.Lock()
        self._reader = asyncio.create_task(self._drain_requests())

    async def _drain_requests(self) -> None:
        try:
            if hasattr(self._request_iterator, "__aiter__"):
                async for event in self._request_iterator:
                    logger.debug(
                        "OrchestratorEvents inbound: run_id=%d type=%s status=%s",
                        event.run_id,
                        event.type,
                        event.status,
                    )
            else:
                for event in self._request_iterator:
                    logger.debug(
                        "OrchestratorEvents inbound: run_id=%d type=%s status=%s",
                        event.run_id,
                        event.type,
                        event.status,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("OrchestratorEvents inbound stream closed: %s", exc)

    def __aiter__(self) -> OrchEventStream:
        return self

    async def __anext__(self) -> OrchEvent:
        async with self._next_lock:
            if self._closed:
                raise StopAsyncIteration
            return await self._queue.get()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._bus.unsubscribe(self._queue)
        self._reader.cancel()
        try:
            await self._reader
        except asyncio.CancelledError:
            pass


class HubService(HubServiceServicer):
    """Concrete implementation of the HubService gRPC servicer.

    When a storage backend is provided, hub and checkpoint data is
    persisted through it. Otherwise, in-memory dicts are used as a
    fallback (original behaviour).

    A ``StreamRouter`` singleton is created on construction and used by
    ``AgentStream`` to register, broadcast to, and unregister per-run
    outbound queues.  When an LLM provider is supplied, ``AgentStream``
    forwards decoded payloads to ``llm_provider.stream()`` and yields the
    response chunks back to the caller.  Without a provider, payloads are
    echoed verbatim.
    """

    def __init__(
        self,
        storage: Any | None = None,
        fork_handler: ForkHandler | None = None,
        llm_provider: Any = None,
        llm_model: str = "",
        orch_event_bus: OrchEventBus | None = None,
    ) -> None:
        """Initialise HubService.

        Args:
            storage: Optional async storage backend for hub/checkpoint
                persistence.  When ``None``, in-memory dicts are used.
            fork_handler: Optional ``ForkHandler`` for DetectFork /
                DecideFork RPCs.  When ``None`` those RPCs return
                ``UNAVAILABLE``.
            llm_provider: Optional LLM provider instance.  Must expose an
                ``async def stream(prompt, model) -> AsyncGenerator[LLMChunk, None]``
                interface.  When ``None``, ``AgentStream`` echoes payloads.
        """
        self._storage = storage
        self._fork_handler = fork_handler
        self._llm_provider = llm_provider
        self._llm_model = llm_model
        self._orch_event_bus = orch_event_bus or OrchEventBus()
        self._stream_router = StreamRouter()
        self._hubs: dict[str, dict[str, Any]] = {}
        self._events: dict[str, list[JoinHubEvent]] = defaultdict(list)
        self._checkpoints: dict[str, dict[str, str]] = defaultdict(dict)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _has_storage(self) -> bool:
        return self._storage is not None

    def _get_event_loop(self) -> asyncio.AbstractEventLoop:
        """Get an event loop, compatible with both running and non-running contexts.

        Uses get_event_loop() with try/except fallback to avoid RuntimeError
        in test environments without a running loop.
        """
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            return asyncio.new_event_loop()

    def _schedule_coroutine(self, loop: asyncio.AbstractEventLoop, coro: Any) -> None:
        """Schedule a coroutine safely from a synchronous context.

        Uses fire-and-forget in all cases to avoid blocking the event loop.
        Blocking the running loop (e.g. via ThreadPoolExecutor.result()) while
        inside a sync gRPC handler prevents the response from being sent and
        causes client-side DEADLINE_EXCEEDED at the timeout boundary.

        - Same thread as the loop (grpc.aio calls sync handlers on the loop):
          use ensure_future — schedules the coroutine as a task without blocking.
        - Different thread (worker thread outside the loop):
          use run_coroutine_threadsafe — schedules without blocking this thread.
        """
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is loop:
            # Same thread — schedule as a task; do NOT block the event loop.
            asyncio.ensure_future(coro, loop=loop)
        else:
            # Different thread — schedule on the loop from this thread.
            asyncio.run_coroutine_threadsafe(coro, loop)

    def _decode_hub_agent(self, agent_id: str) -> tuple[str, str]:
        """Decode `hub:<hub_id>:...:<agent_name>` style agent_id."""
        if not agent_id.startswith("hub:"):
            return "", ""
        parts = agent_id.split(":", 3)
        hub_id = parts[1] if len(parts) > 1 else ""
        agent_name = parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else "")
        return hub_id, agent_name

    def _hub_meta_intent(self, prompt_text: str) -> str | None:
        """Return hub meta intent for deny-default bypass routing."""
        return resolve_pre_llm_hub_intent(prompt_text)

    async def _resolve_hub_status_for_stream(self, hub_id: str) -> HubStatusResponse | None:
        """Resolve hub status without mutating stream RPC context status."""
        if not hub_id:
            return None
        if self._has_storage():
            hub_record = await self._storage.get_hub(hub_id)
            if hub_record is None:
                return None
            return HubStatusResponse(
                hub_id=hub_id,
                state=hub_record.state,
                updated_at=hub_record.created_at,
            )
        hub = self._hubs.get(hub_id)
        if hub is None:
            return None
        return HubStatusResponse(
            hub_id=hub_id,
            state=hub["state"],
            updated_at=hub["created_at"],
        )

    def _render_hub_meta_payload(
        self,
        intent: str,
        status: HubStatusResponse,
        prompt_text: str,
    ) -> bytes:
        """Render deterministic payload for hub-state/report/location intents."""
        if intent == "location":
            return status.hub_id.encode("utf-8")
        normalized = " ".join(prompt_text.strip().lower().lstrip("/").split())
        if intent == "status" and normalized in {"hub-state", "hub state"}:
            return status.state.encode("utf-8")
        return json.dumps(
            {
                "id": status.hub_id,
                "current": status.state,
            },
            separators=(",", ":"),
        ).encode("utf-8")

    # ------------------------------------------------------------------
    # RPCs
    # ------------------------------------------------------------------

    async def _publish_orch_event(
        self,
        run_id: int,
        agent: str,
        status: str,
        payload: str,
    ) -> None:
        """Fan out a single orchestrator event to all subscribers."""
        event = OrchEvent(
            run_id=run_id,
            type=agent,
            status=status,
            payload=payload.encode("utf-8"),
            ts=int(time.time()),
        )
        await self._orch_event_bus.publish(event)

    def _publish_orch_event_sync(
        self,
        run_id: int,
        agent: str,
        status: str,
        payload: str,
    ) -> None:
        """Publish an OrchEvent from sync RPC handlers."""
        try:
            loop = self._get_event_loop()
            coro = self._publish_orch_event(run_id, agent, status, payload)
            if loop.is_running():
                self._schedule_coroutine(loop, coro)
            else:
                loop.run_until_complete(coro)
        except RuntimeError:
            return

    async def _run_orc_orchestration(self, run_id: int, prompt: str) -> AsyncIterator[str]:
        """Run ORC->ARC->CRT discussion and stream ORC final response.

        HUB-VIEW receives only ARC/CRT planning-review events; ORC output is
        streamed back on the main chat channel via AgentStream responses.
        """
        if self._llm_provider is None:
            yield prompt
            return

        model = self._llm_model
        shared_ctx: dict[str, str] = {"user_task": prompt}

        orc_result = await self._llm_provider.complete(
            f"{ORC_PROMPT}\n\nUSER TASK:\n{prompt}",
            model=model,
        )
        shared_ctx["orc_summary"] = orc_result.text.strip()

        arc_result = await self._llm_provider.complete(
            (
                f"{ARC_PROMPT}\n\n"
                f"ORC CONTEXT:\n{shared_ctx['orc_summary']}\n\n"
                f"USER TASK:\n{shared_ctx['user_task']}"
            ),
            model=model,
        )
        shared_ctx["arc_plan"] = arc_result.text.strip()
        await self._publish_orch_event(run_id, "ARC", "plan", shared_ctx["arc_plan"][:240])

        crt_result = await self._llm_provider.complete(
            (
                f"{CRT_PROMPT}\n\n"
                f"ORC CONTEXT:\n{shared_ctx['orc_summary']}\n\n"
                f"ARC PLAN:\n{shared_ctx['arc_plan']}"
            ),
            model=model,
        )
        shared_ctx["crt_review"] = crt_result.text.strip()
        await self._publish_orch_event(run_id, "CRT", "review", shared_ctx["crt_review"][:240])

        if not shared_ctx["crt_review"].upper().startswith("APPROVED"):
            arc_revise = await self._llm_provider.complete(
                (
                    f"{ARC_PROMPT}\n\n"
                    f"ORC CONTEXT:\n{shared_ctx['orc_summary']}\n\n"
                    f"CRT FEEDBACK:\n{shared_ctx['crt_review']}\n\n"
                    "Revise the plan to address the critique."
                ),
                model=model,
            )
            shared_ctx["arc_plan"] = arc_revise.text.strip()
            await self._publish_orch_event(run_id, "ARC", "revise", shared_ctx["arc_plan"][:240])

            crt_result = await self._llm_provider.complete(
                (
                    f"{CRT_PROMPT}\n\n"
                    f"ORC CONTEXT:\n{shared_ctx['orc_summary']}\n\n"
                    f"ARC PLAN:\n{shared_ctx['arc_plan']}"
                ),
                model=model,
            )
            shared_ctx["crt_review"] = crt_result.text.strip()
            await self._publish_orch_event(run_id, "CRT", "review", shared_ctx["crt_review"][:240])

        final_prompt = (
            "You are ORC. Using the shared context below, deliver the final response "
            "for the user task.\n\n"
            f"USER TASK:\n{shared_ctx['user_task']}\n\n"
            f"ORC CONTEXT:\n{shared_ctx['orc_summary']}\n\n"
            f"ARC PLAN:\n{shared_ctx['arc_plan']}\n\n"
            f"CRT REVIEW:\n{shared_ctx['crt_review']}"
        )
        async for chunk in self._llm_provider.stream(final_prompt, model=model):
            text = getattr(chunk, "text", "")
            if text:
                yield text
            if getattr(chunk, "done", False):
                break

    def CreateHub(
        self, request: CreateHubRequest, context: grpc.ServicerContext
    ) -> CreateHubResponse:
        """Create a new hub instance."""
        hub_id = str(uuid.uuid4())
        created_at = int(time.time())

        if self._has_storage():
            record = HubRecord(
                hub_id=hub_id,
                workspace_id=request.workspace_id,
                initiator=request.initiator,
                created_at=created_at,
                state="active",
            )
            # Storage insert is async but gRPC servicer methods are sync.
            # Always update in-memory dict first, then persist to storage.
            self._hubs[hub_id] = {
                "workspace_id": request.workspace_id,
                "initiator": request.initiator,
                "created_at": created_at,
                "state": "active",
            }
            try:
                loop = self._get_event_loop()
                if loop.is_running():
                    self._schedule_coroutine(loop, self._storage.insert_hub(record))
                else:
                    loop.run_until_complete(self._storage.insert_hub(record))
            except RuntimeError:
                pass  # in-memory dict already updated above
        else:
            self._hubs[hub_id] = {
                "workspace_id": request.workspace_id,
                "initiator": request.initiator,
                "created_at": created_at,
                "state": "active",
            }

        self._publish_orch_event_sync(0, "HUB", "create", hub_id)
        return CreateHubResponse(hub_id=hub_id, created_at=created_at)

    def TerminateHub(
        self, request: TerminateHubRequest, context: grpc.ServicerContext
    ) -> TerminateHubResponse:
        """Terminate an existing hub."""
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if loop.is_running():
                    self._schedule_coroutine(
                        loop,
                        self._storage.update_hub_state(
                            request.hub_id, "terminated", request.reason
                        ),
                    )
                    # Update in-memory dict; fall through to dict logic below.
                else:
                    ok = loop.run_until_complete(
                        self._storage.update_hub_state(
                            request.hub_id, "terminated", request.reason
                        )
                    )
                    if not ok:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return TerminateHubResponse(ok=False)
                    return TerminateHubResponse(ok=True)
            except RuntimeError:
                pass  # fall through to dict logic

        hub = self._hubs.get(request.hub_id)
        if hub is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Hub {request.hub_id} not found")
            return TerminateHubResponse(ok=False)
        hub["state"] = "terminated"
        hub["terminated_reason"] = request.reason
        return TerminateHubResponse(ok=True)

    def JoinHub(
        self, request_iterator: Iterator[JoinHubRequest], context: grpc.ServicerContext
    ) -> Iterator[JoinHubEvent]:
        """Bidirectional streaming for hub membership events."""
        for req in request_iterator:
            event = JoinHubEvent(
                hub_id=req.hub_id,
                member_id=req.member_id,
                event=req.event,
                at=int(time.time()),
            )
            self._events[req.hub_id].append(event)
            yield event

    def TapHub(
        self, request: TapHubRequest, context: grpc.ServicerContext
    ) -> TapHubResponse:
        """Tap into hub event stream with a limit."""
        events = self._events.get(request.hub_id, [])
        limit = request.limit if request.limit > 0 else len(events)
        return TapHubResponse(events=events[:limit])

    def CreateCheckpoint(
        self, request: CreateCheckpointRequest, context: grpc.ServicerContext
    ) -> CreateCheckpointResponse:
        """Create a checkpoint for a hub."""
        # Verify hub exists
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if not loop.is_running():
                    hub_record = loop.run_until_complete(
                        self._storage.get_hub(request.hub_id)
                    )
                    if hub_record is None:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return CreateCheckpointResponse(checkpoint_id="")
                else:
                    if request.hub_id not in self._hubs:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return CreateCheckpointResponse(checkpoint_id="")
            except RuntimeError:
                if request.hub_id not in self._hubs:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details(f"Hub {request.hub_id} not found")
                    return CreateCheckpointResponse(checkpoint_id="")
        else:
            if request.hub_id not in self._hubs:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Hub {request.hub_id} not found")
                return CreateCheckpointResponse(checkpoint_id="")

        checkpoint_id = str(uuid.uuid4())

        if self._has_storage():
            record = CheckpointRecord(
                checkpoint_id=checkpoint_id,
                hub_id=request.hub_id,
                label=request.label,
            )
            try:
                loop = self._get_event_loop()
                if loop.is_running():
                    self._schedule_coroutine(
                        loop, self._storage.insert_checkpoint(record)
                    )
                else:
                    loop.run_until_complete(self._storage.insert_checkpoint(record))
            except RuntimeError:
                self._checkpoints[request.hub_id][checkpoint_id] = request.label
        else:
            self._checkpoints[request.hub_id][checkpoint_id] = request.label

        return CreateCheckpointResponse(checkpoint_id=checkpoint_id)

    def RollbackCheckpoint(
        self, request: RollbackCheckpointRequest, context: grpc.ServicerContext
    ) -> RollbackCheckpointResponse:
        """Rollback a hub to a checkpoint."""
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if not loop.is_running():
                    cp = loop.run_until_complete(
                        self._storage.get_checkpoint(request.checkpoint_id)
                    )
                    if cp is None:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(
                            f"Checkpoint {request.checkpoint_id} not found"
                        )
                        return RollbackCheckpointResponse(ok=False)
                    return RollbackCheckpointResponse(ok=True)
            except RuntimeError:
                pass # fall through

        hub_checkpoints = self._checkpoints.get(request.hub_id, {})
        if request.checkpoint_id not in hub_checkpoints:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Checkpoint {request.checkpoint_id} not found")
            return RollbackCheckpointResponse(ok=False)
        return RollbackCheckpointResponse(ok=True)

    def HubStatus(
        self, request: HubStatusRequest, context: grpc.ServicerContext
    ) -> HubStatusResponse:
        """Get the current status of a hub."""
        self._publish_orch_event_sync(0, "HUB", "status_check", request.hub_id)
        if self._has_storage():
            try:
                loop = self._get_event_loop()
                if not loop.is_running():
                    hub_record = loop.run_until_complete(
                        self._storage.get_hub(request.hub_id)
                    )
                    if hub_record is None:
                        context.set_code(grpc.StatusCode.NOT_FOUND)
                        context.set_details(f"Hub {request.hub_id} not found")
                        return HubStatusResponse(
                            hub_id=request.hub_id, state="unknown", updated_at=0
                        )
                    return HubStatusResponse(
                        hub_id=request.hub_id,
                        state=hub_record.state,
                        updated_at=hub_record.created_at,
                    )
            except RuntimeError:
                pass # fall through

        hub = self._hubs.get(request.hub_id)
        if hub is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Hub {request.hub_id} not found")
            return HubStatusResponse(hub_id=request.hub_id, state="unknown", updated_at=0)
        return HubStatusResponse(
            hub_id=request.hub_id,
            state=hub["state"],
            updated_at=hub["created_at"],
        )

    def DetectFork(
        self, request: DetectForkRequest, context: grpc.ServicerContext
    ) -> DetectForkResponse:
        """Detect a fork divergence scenario.

        Delegates to ForkHandler for consistent processing across
        both Redis and gRPC paths.
        """
        if self._fork_handler is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("ForkHandler not available")
            return DetectForkResponse(fork_id="", status="error")

        event = DivergenceEvent(
            parent_hub_id=request.parent_hub_id,
            hal_agent_id=request.hal_agent_id,
            divergence_type=request.divergence_type,
            divergence_reason=request.divergence_reason,
            evidence=request.divergence_payload or b"",
            detected_at=int(time.time()),
        )

        try:
            loop = self._get_event_loop()
            if loop.is_running():
                self._schedule_coroutine(
                    loop, self._fork_handler.handle_detection(event)
                )
                return DetectForkResponse(fork_id="pending", status="detected")
            else:
                record: ForkScenarioRecord = loop.run_until_complete(
                    self._fork_handler.handle_detection(event)
                )
                return DetectForkResponse(
                    fork_id=record.fork_id,
                    status=record.status,
                )
        except Exception:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to process fork detection")
            return DetectForkResponse(fork_id="", status="error")

    def DecideFork(
        self, request: DecideForkRequest, context: grpc.ServicerContext
    ) -> DecideForkResponse:
        """Submit a decision for a fork scenario.

        Delegates to ForkHandler for consistent processing across
        both Redis and gRPC paths.
        """
        if self._fork_handler is None:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("ForkHandler not available")
            return DecideForkResponse(ok=False)

        if request.decision not in ("approve", "reject"):
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Invalid decision: {request.decision!r}. Must be 'approve' or 'reject'."
            )
            return DecideForkResponse(ok=False)

        try:
            loop = self._get_event_loop()
            if loop.is_running():
                self._schedule_coroutine(
                    loop, self._fork_handler.process_decision(
                        request.fork_id, request.decision
                    )
                )
                return DecideForkResponse(ok=True, new_hub_id="pending")
            else:
                new_hub_id = loop.run_until_complete(
                    self._fork_handler.process_decision(
                        request.fork_id, request.decision
                    )
                )
                return DecideForkResponse(
                    ok=True,
                    new_hub_id=new_hub_id or "",
                )
        except Exception:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to process fork decision")
            return DecideForkResponse(ok=False)

    # ------------------------------------------------------------------
    # Bidirectional streaming RPCs
    # ------------------------------------------------------------------

    async def AgentStream(
        self,
        request_iterator: Iterator[AgentMessage],
        context: grpc.ServicerContext,
    ) -> Iterator[AgentMessage]:
        """Bidirectional streaming for agent-to-agent communication.

        Agents send AgentMessage with run_id, agent_id, payload, seq, done, err.
        The hub_id is encoded in the agent_id field as "hub:<hub_id>:<agent_id>".
        When an LLM provider is configured, payload is forwarded to it and the
        response is streamed back. Falls back to echo when no provider is set.
        Termination: done=true or cancel signal.
        Reconnect on UNAVAILABLE.

        Args:
            request_iterator: Stream of AgentMessage from the client agent.
            context: gRPC servicer context.

        Yields:
            AgentMessage responses routed to the target agent.
        """
        outbound_queue: asyncio.Queue = asyncio.Queue()
        registered_run_id: int | None = None
        registered_agent_id: str | None = None
        registered_hub_id: str = ""

        async for msg in request_iterator:
            logger.debug(
                "AgentStream: run_id=%d agent_id=%s seq=%d done=%s",
                msg.run_id,
                msg.agent_id,
                msg.seq,
                msg.done,
            )

            # Register on first message
            if registered_run_id is None:
                registered_run_id = msg.run_id
                registered_agent_id = msg.agent_id
                registered_hub_id, agent_name = self._decode_hub_agent(msg.agent_id)
                if registered_hub_id:
                    logger.debug(
                        "AgentStream: decoded hub_id=%s agent_name=%s",
                        registered_hub_id,
                        agent_name,
                    )
                await self._stream_router.register_handler(
                    msg.run_id, msg.agent_id, outbound_queue
                )
                await asyncio.sleep(0)

            # Handle error messages
            if msg.err:
                logger.warning(
                    "AgentStream error: run_id=%d agent_id=%s err=%s",
                    msg.run_id,
                    msg.agent_id,
                    msg.err,
                )
                if registered_run_id is not None and registered_agent_id is not None:
                    await self._stream_router.unregister_handler(
                        registered_run_id, registered_agent_id
                    )
                yield AgentMessage(
                    run_id=msg.run_id,
                    agent_id="orchestrator",
                    payload=b"",
                    seq=msg.seq + 1,
                    done=True,
                    err=msg.err,
                )
                return

            # Handle termination
            if msg.done:
                logger.info(
                    "AgentStream done: run_id=%d agent_id=%s",
                    msg.run_id,
                    msg.agent_id,
                )
                if registered_run_id is not None and registered_agent_id is not None:
                    await self._stream_router.unregister_handler(
                        registered_run_id, registered_agent_id
                    )
                yield AgentMessage(
                    run_id=msg.run_id,
                    agent_id="orchestrator",
                    payload=b"ack",
                    seq=msg.seq + 1,
                    done=True,
                    err="",
                )
                return

            # Route payload
            prompt_text = msg.payload.decode("utf-8", errors="replace")
            hub_intent = self._hub_meta_intent(prompt_text)
            if hub_intent is not None:
                await self._publish_orch_event(
                    msg.run_id,
                    "HUB",
                    "status_check",
                    registered_hub_id or "",
                )
                status = await self._resolve_hub_status_for_stream(registered_hub_id)
                if status is None:
                    yield AgentMessage(
                        run_id=msg.run_id,
                        agent_id="HubStatus",
                        payload=b"",
                        seq=msg.seq + 1,
                        done=True,
                        err="no-hub",
                    )
                    return

                payload = self._render_hub_meta_payload(hub_intent, status, prompt_text)
                response_msg = AgentMessage(
                    run_id=msg.run_id,
                    agent_id="HubStatus",
                    payload=payload,
                    seq=msg.seq + 1,
                    done=False,
                    err="",
                )
                await self._stream_router.broadcast(msg.run_id, response_msg)
                yield response_msg
                continue

            if self._llm_provider is not None:
                try:
                    await self._publish_orch_event(msg.run_id, "ORC", "recv", prompt_text)
                    await self._publish_orch_event(msg.run_id, "ORC", "scope", "orchestrate")
                    seq = msg.seq + 1
                    async for chunk_text in self._run_orc_orchestration(msg.run_id, prompt_text):
                        chunk_bytes = chunk_text.encode("utf-8")
                        response_msg = AgentMessage(
                            run_id=msg.run_id,
                            agent_id="ORC",
                            payload=chunk_bytes,
                            seq=seq,
                            done=False,
                            err="",
                        )
                        await self._stream_router.broadcast(msg.run_id, response_msg)
                        yield response_msg
                        seq += 1
                    await self._publish_orch_event(msg.run_id, "ORC", "final", "")
                except Exception as exc:
                    logger.error("LLM stream error: %s", exc)
                    if registered_run_id is not None and registered_agent_id is not None:
                        await self._stream_router.unregister_handler(
                            registered_run_id, registered_agent_id
                        )
                    yield AgentMessage(
                        run_id=msg.run_id,
                        agent_id="ORC",
                        payload=b"",
                        seq=msg.seq + 1,
                        done=True,
                        err=str(exc),
                    )
                    return
            else:
                yield AgentMessage(
                    run_id=msg.run_id,
                    agent_id="ORC",
                    payload=b"",
                    seq=msg.seq + 1,
                    done=True,
                    err="No LLM provider configured for orchestration",
                )
                return

        # Clean up registration if iterator exhausted without done=True
        if registered_run_id is not None and registered_agent_id is not None:
            await self._stream_router.unregister_handler(
                registered_run_id, registered_agent_id
            )

    async def OrchestratorEvents(
        self,
        request_iterator: AsyncIterator[OrchEvent],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[OrchEvent]:
        """Bidirectional streaming for orchestrator events.

        Orchestrator sends OrchEvent with run_id, type, status, payload, ts.
        Used for real-time run status updates between orchestrator and agents.

        Args:
            request_iterator: Async stream of OrchEvent from the client.
            context: gRPC async servicer context.

        Yields:
            OrchEvent frames published to the internal event bus.
        """
        queue: asyncio.Queue[OrchEvent] = self._orch_event_bus.subscribe()
        # Drain client subscribe frames in background so the client send-side
        # stays alive (keeps the bidirectional stream open).
        async def _drain() -> None:
            try:
                async for event in request_iterator:
                    logger.debug(
                        "OrchestratorEvents inbound: type=%s status=%s",
                        event.type,
                        event.status,
                    )
            except Exception as exc:
                logger.debug("OrchestratorEvents inbound closed: %s", exc)

        drain_task = asyncio.create_task(_drain())
        try:
            while True:
                event = await queue.get()
                yield event
        except asyncio.CancelledError:
            pass
        finally:
            drain_task.cancel()
            try:
                await drain_task
            except (asyncio.CancelledError, Exception):
                pass
            self._orch_event_bus.unsubscribe(queue)

    @property
    def orch_event_bus(self) -> OrchEventBus:
        """Expose injected OrchEventBus for DI identity checks."""
        return self._orch_event_bus

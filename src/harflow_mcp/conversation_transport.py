"""Bounded native conversation driver; persistence and activity binding stay with owner.

The stdout drainer retains raw output and calls observe() without awaiting owner
I/O. This module never spawns, signals, resumes, changes grants or publishes task
settlement. It serializes bytes, not outstanding native RPC lifetimes.
"""
from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from time import monotonic
from typing import Any, Protocol

from .backends.conversation import ConversationIdentity, ConversationObservation, ConversationPort, ConversationRefused
from .backends.conversation_native import restore_conversation_port


@dataclass(frozen=True)
class ConversationInput:
    logical_turn: str
    text: str


@dataclass(frozen=True)
class ConversationInterrupt:
    identity: ConversationIdentity
    request_id: str


class ConversationInterruptRefused(ConversationRefused):
    """Expected pre-write admission race; no native interrupt was issued."""


class ConversationInterruptDeferred(ConversationRefused):
    """Known busy native phase; original durable Now remains queued for retry."""
    code = "tool_interruption_unqualified"


class ConversationOwner(Protocol):
    async def poll_control(self, identity: ConversationIdentity | None) -> ConversationInterrupt | None:
        """Read durable cross-process Now intent; no native effects in this callback."""
        ...

    async def before_issue(self, candidate: ConversationInput, previous: ConversationIdentity | None) -> None:
        """Durably reserve exact candidate; idempotent. Raise to refuse before bytes."""
        ...

    async def next_input(self, previous: ConversationIdentity | None) -> ConversationInput | None:
        """Return exactly one FIFO claimed input, or None. Never count a spawn as consumption."""
        ...

    async def observe(self, observation: ConversationObservation) -> None:
        """Persist native observations/driver intents; issue/interrupt intents precede I/O.

        Kinds include session_ready, input_issued, write_started, write_completed,
        input_accepted, response_terminal, interrupt_intent, unknown and closed.
        `closed` is stdin closure only, never process or descendant settlement.
        """
        ...

    async def before_close(self, previous: ConversationIdentity | None) -> ConversationInput | None:
        """Atomically claim one racing Queue input or seal admission and return None."""
        ...


class ConversationWriter(Protocol):
    def write(self, data: bytes) -> Any: ...
    async def drain(self) -> None: ...
    def close(self) -> Any: ...


class ConversationDriver:
    def __init__(self, port: ConversationPort, writer: ConversationWriter, owner: ConversationOwner,
                 *, initial_input: ConversationInput, idle_timeout: float = 1.0,
                 operation_timeout: float = 120.0, max_buffered_frames: int = 256,
                 control_interval: float = .25) -> None:
        if not 0 < idle_timeout <= 60 or not 0 < operation_timeout <= 3600 or not 1 <= max_buffered_frames <= 4096:
            raise ValueError("finite conversation bounds required")
        self.port, self.writer, self.owner = port, writer, owner
        self.initial_input = initial_input
        self.idle_timeout, self.operation_timeout = idle_timeout, operation_timeout
        if not .01 <= control_interval <= 5:
            raise ValueError("bounded control polling interval required")
        self.control_interval = control_interval
        self._frames: asyncio.Queue[tuple[int, dict[str, Any]]] = asyncio.Queue(max_buffered_frames)
        self._wake = asyncio.Event()
        self._writer_lock = asyncio.Lock()
        self._observation_lock = asyncio.Lock()
        self._receive_sequence = 0
        self._task: asyncio.Task[None] | None = None
        self._candidate: ConversationInput | None = initial_input
        self._reserved = False
        self._initial_issued = False
        self._closing = False
        self._closed = False
        self._fault: str | None = None
        self._eof = False
        self._operation_deadline = monotonic() + operation_timeout
        self._idle_deadline: float | None = None
        self._seen_interrupts: set[str] = set()
        self._issue_needs_fence = False
        self._issue_intent_ready = asyncio.Event()
        self._issue_intent_ready.set()

    @property
    def closed(self) -> bool:
        return self._closed

    def start(self, *, task_factory=None) -> asyncio.Task[None]:
        if self._task is not None:
            raise ConversationRefused("conversation driver already started")
        create = task_factory or asyncio.create_task
        self._task = create(self._run(), name="harflow-conversation")
        return self._task

    def observe(self, frame: dict[str, Any]) -> bool:
        """Nonblocking drainer hook. False means transport evidence is unknown.

        Raw output must already be retained by the drainer. Capture sequence is
        assigned before buffering, so pre-issue buffered output cannot become a
        new-turn observation. Queue overflow fails closed, never drops silently.
        """
        if self._closed:
            return False
        self._receive_sequence += 1
        try:
            self._frames.put_nowait((self._receive_sequence, frame))
        except asyncio.QueueFull:
            self._fault = "native conversation receive buffer exceeded"
            self._wake.set()
            return False
        self._wake.set()
        return True

    def wake(self) -> None:
        """Owner calls after durable Queue admission or positive tool settlement."""
        self._wake.set()

    def eof(self) -> None:
        self._eof = True
        self._wake.set()

    async def _event(self, kind: str, **payload: Any) -> None:
        await self.owner.observe(ConversationObservation(kind, self.port.identity, payload))

    async def _unknown(self, reason: str) -> None:
        self.port.mark_unknown(reason)
        self._wake.set()
        await self._event("unknown", reason=reason, replay_allowed=False)

    async def _flush(self) -> None:
        # Only the byte writer is serialized. Native prompt RPC completion is
        # observed independently; interrupt frames can be written meanwhile.
        await self._issue_intent_ready.wait()
        if self.port.unknown:
            return
        async with self._writer_lock:
            if self.port.unknown:
                return
            frames = self.port.outgoing_frames()
            if not frames:
                return
            await self._event("write_started", frame_count=len(frames), semantic_consumption="unknown")
            try:
                if self._issue_needs_fence:
                    self.port.advance_issue_fence(self._receive_sequence)
                    self._issue_needs_fence = False
                for frame in frames:
                    self.writer.write(frame)
                    await asyncio.wait_for(self.writer.drain(), self.operation_timeout)
            except (Exception, asyncio.CancelledError):
                # Even an exception on the first write does not prove zero
                # bytes reached the peer. Never requeue the taken frames.
                self.writer.close()
                await self._unknown("native frame write failed; delivery unknown")
                raise
            await self._event("write_completed", frame_count=len(frames), semantic_consumption="unknown")

    async def _drain(self) -> None:
        async with self._observation_lock:
            await self._drain_locked()

    async def _drain_locked(self, *, flush: bool = True) -> bool:
        saw_busy = False
        while not self._frames.empty():
            sequence, frame = self._frames.get_nowait()
            observations = self.port.observe(frame, sequence=sequence)
            saw_busy = saw_busy or any(o.kind in {"tool_pending", "approval_pending"} for o in observations)
            for observation in observations:
                await self.owner.observe(observation)
            if flush:
                await self._flush()
        return saw_busy

    def _check_interrupt_phase(self, identity: ConversationIdentity) -> None:
        if (self._closed or self._closing or self._fault or self.port.unknown or identity != self.port.identity
                or self.port.terminal is not None):
            raise ConversationInterruptRefused("native interrupt target is stale, closed or unknown")
        if self.port.pending_tools or self.port.pending_approvals or self.port.unsettled_effects:
            raise ConversationInterruptDeferred("native tool/approval/effect phase is not qualified for interruption")

    async def _maybe_issue(self) -> None:
        if not self.port.can_issue or self._closing:
            return
        if self._candidate is None:
            self._candidate = await self.owner.next_input(self.port.identity)
            self._reserved = False
        if self._candidate is None:
            return
        candidate = self._candidate
        if not self._reserved:
            try:
                await self.owner.before_issue(candidate, self.port.identity)
            except ConversationRefused as exc:
                await self._event("input_refused", logical_turn=candidate.logical_turn, reason=str(exc))
                self._candidate = None
                self._reserved = False
                return
            self._reserved = True
        # Owner callbacks may have awaited storage while native frames arrived.
        # Reconcile all buffered tool/approval evidence before committing input.
        await self._drain()
        if not self.port.can_issue or self._closing:
            return
        self.port.issue(candidate.text, candidate.logical_turn, receive_fence=self._receive_sequence)
        self._issue_needs_fence = True
        # Normal response/tool duration belongs to the task execution policy.
        self._operation_deadline = None
        self._issue_intent_ready.clear()
        try:
            await self._event("input_issued", logical_turn=candidate.logical_turn,
                              text=candidate.text, semantic_consumption="unknown", initial=not self._initial_issued)
        except BaseException:
            self.port.mark_unknown("native input issuance persistence failed before write")
            raise
        finally:
            self._issue_intent_ready.set()
        self._initial_issued = True
        self._candidate = None
        self._reserved = False
        await self._flush()

    async def interrupt(self, identity: ConversationIdentity, *, request_id: str | None = None) -> None:
        """Durable exact-turn intent, native interrupt only; no process kill fallback."""
        await self._drain()
        self._check_interrupt_phase(identity)
        try:
            await self._event("interrupt_intent", expected_identity=asdict(identity), request_id=request_id)
        except ConversationRefused as exc:
            raise ConversationInterruptRefused(str(exc)) from exc
        # Serialize observations through the final byte boundary. No prompt RPC
        # lifetime is locked; only bounded native observations/storage and bytes.
        async with self._observation_lock:
            await self._drain_locked()
            self._check_interrupt_phase(identity)
            await self._event("write_started", frame_count=1, control="interrupt", semantic_consumption="unknown")
            async with self._writer_lock:
                # The write-intent callback or writer-lock acquisition may have
                # awaited storage while a tool/permission frame was captured.
                # Do not build the native cancel at all until this last check.
                saw_busy = await self._drain_locked(flush=False)
                self._check_interrupt_phase(identity)
                if saw_busy:
                    raise ConversationInterruptDeferred("native busy phase appeared before interrupt bytes")
                self.port.interrupt(identity)
                # Busy deferrals above never start this settlement clock.
                self._operation_deadline = monotonic() + self.operation_timeout
                frames = self.port.outgoing_frames()
                try:
                    for frame in frames:
                        self.writer.write(frame)
                        await asyncio.wait_for(self.writer.drain(), self.operation_timeout)
                except (Exception, asyncio.CancelledError):
                    self.writer.close()
                    await self._unknown("native interrupt frame write failed; delivery unknown")
                    raise
            await self._event("write_completed", frame_count=len(frames), control="interrupt", semantic_consumption="unknown")
        self._wake.set()

    async def _poll_control(self) -> None:
        intent = await self.owner.poll_control(self.port.identity)
        if intent is None:
            return
        if not isinstance(intent, ConversationInterrupt) or not intent.request_id:
            raise ConversationRefused("invalid durable native interrupt intent")
        if intent.request_id in self._seen_interrupts:
            return
        if self.port.terminal is not None or intent.identity != self.port.identity:
            await self._event("interrupt_stale", request_id=intent.request_id)
            self._seen_interrupts.add(intent.request_id)
            return
        if not self.port.identity.turn_id:
            return  # Wait for exact native acceptance; do not infer an id.
        try:
            await self.interrupt(intent.identity, request_id=intent.request_id)
        except ConversationInterruptDeferred as exc:
            await self._event("interrupt_deferred", request_id=intent.request_id, code=exc.code, reason=str(exc))
            await self._flush()
            return  # No dedup: the same queued intent retries after a safe phase.
        except ConversationInterruptRefused as exc:
            await self._event("interrupt_stale", request_id=intent.request_id, reason=str(exc))
        self._seen_interrupts.add(intent.request_id)

    async def close(self, reason: str = "owner_close") -> None:
        self._closing = True
        self._wake.set()
        if self._task is None:
            await self._close_writer(reason)
        elif asyncio.current_task() is not self._task:
            await self._task

    async def _close_writer(self, reason: str) -> None:
        if self._closed:
            return
        async with self._writer_lock:
            self.writer.close()
            self._closed = True
        await self._event("closed", reason=reason, process_settlement="unknown", os_settlement="unknown",
                          unresolved_tools=sorted(self.port.pending_tools | self.port.unsettled_effects),
                          unresolved_approvals=len(self.port.pending_approvals))

    async def _native_close(self) -> None:
        if not self.port.begin_close():
            return
        await self._flush()
        deadline = monotonic() + self.operation_timeout
        while not self.port.close_acknowledged and not self.port.unknown:
            self._wake.clear()
            await self._drain()
            if self.port.close_acknowledged:
                return
            if self._eof:
                await self._unknown("native close reached EOF without correlated acknowledgement")
                return
            try:
                await asyncio.wait_for(self._wake.wait(), max(.000001, deadline - monotonic()))
            except asyncio.TimeoutError:
                await self._unknown("native close acknowledgement deadline")
                return

    async def _run(self) -> None:
        reason = "idle_close"
        try:
            await self._flush()
            while not self._closing:
                self._wake.clear()
                if self._fault:
                    await self._unknown(self._fault)
                    reason = "unknown"
                    break
                await self._drain()
                if self.port.unknown:
                    reason = "unknown"
                    break
                if self._eof:
                    if not self.port.can_issue:
                        await self._unknown("native EOF without settled conversation")
                    reason = "native_eof"
                    break
                await self._poll_control()
                await self._maybe_issue()
                if self.port.unknown:
                    reason = "unknown"
                    break
                idle = self.port.can_issue and self._candidate is None
                if idle and self._idle_deadline is None:
                    self._idle_deadline = monotonic() + self.idle_timeout
                elif not idle:
                    self._idle_deadline = None
                # Active responses and ordinary effect waits have no transport
                # wall-clock limit. Startup and actually issued cancellation do.
                deadline = self._idle_deadline if idle else self._operation_deadline
                timeout = self.control_interval if deadline is None else max(
                    .000001, min(self.control_interval, deadline - monotonic()))
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout)
                except asyncio.TimeoutError:
                    if deadline is None or monotonic() < deadline:
                        continue
                    if idle:
                        # Admission and close have one durable owner-side winner.
                        candidate = await self.owner.before_close(self.port.identity)
                        if candidate is None:
                            reason = "idle_close"
                            break
                        self._candidate = candidate
                        self._reserved = False
                        await self._maybe_issue()
                    else:
                        await self._unknown("native conversation handshake/interrupt settlement deadline")
                        reason = "settlement_deadline"
                        break
            if self._closing:
                reason = "owner_close"
            if reason in {"idle_close", "owner_close"} and self.port.can_issue:
                await self._native_close()
        except asyncio.CancelledError:
            reason = "driver_cancelled"
            await self._unknown("conversation driver cancelled; native outcome unknown")
            raise
        except Exception as exc:
            reason = "driver_failure"
            if self.port.unknown is None:
                await self._unknown("conversation driver failure: " + type(exc).__name__ + ": " + str(exc)[:1024])
        finally:
            await self._close_writer(reason)


def attach_conversation_driver(context: dict[str, Any], *, validated_profile: dict[str, Any],
                               writer: ConversationWriter, owner: ConversationOwner,
                               initial_logical_turn: str, **bounds: Any) -> ConversationDriver:
    """Attach only to a process launched with this independently validated context."""
    port = restore_conversation_port(context, validated_profile=validated_profile)
    return ConversationDriver(port, writer, owner,
                              initial_input=ConversationInput(initial_logical_turn, context["initial_prompt"]), **bounds)

"""Native conversation ports, independent of task scheduling and durable delivery.

The owner persists intent before draining outgoing frames. Taking a frame is not a
write receipt. A write, RPC acknowledgement, native input acceptance, response
terminal and OS tool settlement are different evidence. Ports never spawn or retry.
The reader supplies a monotonically increasing receive sequence; a new issue's
fence must include every frame already read, including buffered frames.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .base import Invocation, UnsupportedCapability


class ConversationRefused(UnsupportedCapability):
    """The exact transport, identity or profile cannot satisfy this operation."""


@dataclass(frozen=True)
class ConversationIdentity:
    session_id: str | None
    turn_id: str | None
    generation: int
    logical_turn: str


@dataclass(frozen=True)
class ConversationObservation:
    kind: str
    identity: ConversationIdentity | None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConversationLaunch:
    invocation: Invocation
    port: ConversationPort


def frame_bytes(frame: dict[str, Any]) -> bytes:
    return (json.dumps(frame, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()


def rpc_key(value: Any) -> tuple[type, Any] | None:
    if type(value) is int or (type(value) is str and 0 < len(value) <= 256):
        return type(value), value
    return None


class ConversationPort:
    """Single-reader synchronous state machine; serialize writes, never RPC lifetimes.

    `issue` returns an identity immediately, before the native turn id is known.
    `input_accepted` is native acceptance only, never an input-consumed receipt.
    No supported protocol proves the user's message was semantically consumed.
    Tool terminal notifications establish protocol settlement only; interrupted
    tool effects require separate trusted settlement before `release_tool`.
    """
    transport = ""

    def __init__(self, *, session_id: str | None = None) -> None:
        self.session_id = session_id
        self.identity: ConversationIdentity | None = None
        self.ready = False
        self.terminal: str | None = None
        self.interrupt_requested = False
        self.interrupt_acknowledged = False
        self.unknown: str | None = None
        self.pending_tools: set[str] = set()
        self.pending_approvals: dict[tuple[type, Any], Any] = {}
        self.unsettled_effects: set[str] = set()
        self._outbox: list[dict[str, Any]] = []
        self._pending: dict[tuple[type, Any], tuple[str, dict[str, Any]]] = {}
        self._counter = 0
        self._generation = 0
        self._sequence = 0
        self._fence = 0
        self._used_logical: set[str] = set()
        self._observations: list[ConversationObservation] = []
        self.close_acknowledged = False

    @property
    def can_issue(self) -> bool:
        return bool(self.ready and self.unknown is None and (self.identity is None or self.terminal is not None)
                    and not self.pending_tools and not self.pending_approvals and not self.unsettled_effects)

    def classify(self, exit_code: int | None) -> str:
        """Final physical owner calls only after exit/drain; never sets Task.done."""
        if (exit_code != 0 or self.unknown is not None or self.identity is None
                or self.pending_tools or self.pending_approvals or self.unsettled_effects):
            return "failed"
        return "completed" if self.terminal in {"completed", "end_turn"} else "failed"

    def begin_close(self) -> bool:
        """True only when a qualified native close frame requires an ACK drain."""
        return False

    def outgoing_frames(self) -> list[bytes]:
        """Take encoded frames. Caller owns partial-write/unknown receipts; never replay."""
        result = [frame_bytes(frame) for frame in self._outbox]
        self._outbox.clear()
        return result

    def _emit(self, kind: str, **payload: Any) -> None:
        self._observations.append(ConversationObservation(kind, self.identity, payload))

    def _request(self, method: str, params: dict[str, Any], purpose: str | None = None) -> str:
        self._counter += 1
        request_id = f"hf-conversation-{self._counter}"
        self._pending[rpc_key(request_id)] = (purpose or method, params)
        self._outbox.append({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        return request_id

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        self._outbox.append({"jsonrpc": "2.0", "method": method, "params": params})

    def issue(self, text: str, logical_turn: str, *, receive_fence: int) -> ConversationIdentity:
        if not self.can_issue:
            raise ConversationRefused("conversation is not settled and ready for input")
        if type(text) is not str or not text.strip() or type(logical_turn) is not str or not logical_turn:
            raise ConversationRefused("nonempty text and logical turn identity required")
        if logical_turn in self._used_logical:
            raise ConversationRefused("logical turn already issued; no replay")
        if type(receive_fence) is not int or receive_fence < self._sequence:
            raise ConversationRefused("receive fence must cover all previously read frames")
        self._fence = receive_fence
        self._generation += 1
        self.identity = ConversationIdentity(self.session_id, None, self._generation, logical_turn)
        self._used_logical.add(logical_turn)
        self.terminal = None
        self.interrupt_requested = self.interrupt_acknowledged = False
        self._issue(text)
        return self.identity

    def interrupt(self, identity: ConversationIdentity) -> None:
        if identity != self.identity or self.terminal is not None or self.unknown or self.interrupt_requested:
            raise ConversationRefused("interrupt requires the exact active turn identity")
        self._interrupt()
        self.interrupt_requested = True
        self._cancel_approvals()

    def observe(self, frame: dict[str, Any], *, sequence: int) -> list[ConversationObservation]:
        if type(sequence) is not int or sequence <= self._sequence:
            raise ConversationRefused("receive sequence must increase")
        self._sequence = sequence
        self._observations = []
        if type(frame) is not dict:
            self.mark_unknown("invalid native frame")
        elif self.identity is not None and sequence <= self._fence:
            self._emit("historical_frame", frame=frame)
        else:
            try:
                self._observe(frame)
            except (AttributeError, KeyError, TypeError, ValueError):
                self.mark_unknown("invalid native frame shape")
        return list(self._observations)

    def advance_issue_fence(self, receive_fence: int) -> None:
        """Driver's final pre-write fence after durable intent callbacks awaited."""
        if type(receive_fence) is not int or receive_fence < self._fence:
            raise ConversationRefused("issue write fence cannot move backwards")
        self._fence = receive_fence

    def mark_unknown(self, reason: str) -> None:
        self.unknown = reason
        self._outbox.clear()
        self._emit("unknown", reason=reason)

    def release_tool(self, tool_id: str, *, identity: ConversationIdentity, trusted_settlement: bool) -> None:
        """Owner-supplied positive effect settlement; native cancel alone never calls this."""
        if identity != self.identity or not trusted_settlement or tool_id not in self.unsettled_effects:
            raise ConversationRefused("exact identity and trusted settlement required")
        self.unsettled_effects.remove(tool_id)
        self.pending_tools.discard(tool_id)

    def _accept(self, turn_id: str) -> None:
        if self.identity is None or type(turn_id) is not str or not turn_id:
            self.mark_unknown("native acceptance lacks active identity")
            return
        if self.identity.turn_id is not None and self.identity.turn_id != turn_id:
            self.mark_unknown("native turn identity changed")
            return
        self.identity = ConversationIdentity(self.session_id, turn_id, self.identity.generation, self.identity.logical_turn)
        self._emit("input_accepted", evidence="native turn accepted; semantic consumption unknown")

    def _finish(self, status: str, frame: dict[str, Any]) -> None:
        if self.identity is None or self.terminal is not None:
            self._emit("historical_frame", frame=frame)
            return
        self.terminal = status
        if status in {"interrupted", "cancelled", "aborted_streaming"}:
            self.unsettled_effects.update(self.pending_tools)
            self._cancel_approvals()
        self._emit("response_terminal", status=status, native=frame,
                   tool_settlement="unknown" if self.pending_tools or self.unsettled_effects else "no_pending_observed_tools",
                   os_settlement="unknown")

    def normalize_observation(self, observation: ConversationObservation, acc: Any) -> list[dict[str, Any]]:
        """Return existing monitor event shapes from already correlated evidence.

        Subclasses adapt their native streams. Historical frames never reach this
        seam. Native usage with unknown aggregation semantics remains explicit
        metadata rather than being summed or reported as zero.
        """
        return []

    def _tool(self, tool_id: Any, status: Any) -> None:
        if type(tool_id) is not str or not tool_id:
            self.mark_unknown("tool update lacks identity")
            return
        if status in {"completed", "failed", "cancelled", "DONE", "ERROR"}:
            self.pending_tools.discard(tool_id)
            if status == "cancelled" or self.interrupt_requested:
                self.unsettled_effects.add(tool_id)
            self._emit("tool_terminal", tool_id=tool_id, status=status, os_settlement="unknown")
        else:
            self.pending_tools.add(tool_id)
            self._emit("tool_pending", tool_id=tool_id)

    def _issue(self, text: str) -> None:
        raise NotImplementedError

    def _interrupt(self) -> None:
        raise NotImplementedError

    def _observe(self, frame: dict[str, Any]) -> None:
        raise NotImplementedError

    def _cancel_approvals(self) -> None:
        raise NotImplementedError

"""Backend-owned native execution adapters; unavailable is the safe default.

A native adapter consumes the owning harness stream, never fabricated process
records. Eligibility is a scoped certification claim, not installed-CLI discovery.
"""
from __future__ import annotations

from typing import Any, Protocol


class NativeAdapter(Protocol):
    activity_level: str
    supports_parallel: bool
    max_batch_size: int

    def configure(self, invocation: Any, settings: dict[str, Any] | None = None) -> Any: ...

    def eligible(self, parent: Any, candidate: dict[str, Any], settings: dict[str, Any]) -> str | None: ...
    def prompt(self, assignment: str, nonce: str, settings: dict[str, Any] | None = None) -> str: ...
    def observe(self, event: dict[str, Any], nonce: str, state: dict[str, Any]) -> list[dict[str, Any]]: ...


def adapter(backend: Any) -> NativeAdapter | None:
    return getattr(backend, "native_subagent_adapter", None)


def available() -> bool:
    from . import BACKENDS
    return any(adapter(b) is not None for b in BACKENDS.values())

"""Claude seed measurements and shared behavior-qualification adapter."""
from __future__ import annotations

import re
from typing import Any

from .base import UnsupportedCapability

# 2.1.302 keeps its historical Bash-only approval scope and is not installed, so it cannot be
# re-measured. 2.1.303 is never added because it was not measured.
MEASURED: dict[str, dict[str, Any]] = {
    "2.1.302": {"caller_approval_tools": ("Bash",)},
    "2.1.310": {"caller_approval_tools": ("Bash", "Read", "Write", "Edit"), "caller_write_tools": ("Write", "Edit"),
                "path_safety_prompts": True, "denial_texts": True, "allowed_commands": True},
    "2.1.312": {"caller_approval_tools": ("Bash", "Read", "Write", "Edit"), "caller_write_tools": ("Write", "Edit"),
                "path_safety_prompts": True, "denial_texts": True, "allowed_commands": True},
}

_VERSION = re.compile(r"(\d+\.\d+\.\d+)(?: \(Claude Code\))?")


def parse_version(observed: Any) -> str | None:
    match = _VERSION.fullmatch(observed.strip()) if isinstance(observed, str) else None
    return match.group(1) if match else None


def measured(version: Any) -> dict[str, Any] | None:
    from .qualification import runtime_facts
    if not isinstance(version, str):
        return None
    return runtime_facts("claude", version) or MEASURED.get(version)


def supports(version: Any, feature: str) -> bool:
    return bool((measured(version) or {}).get(feature))


def approval_tools(version: Any, write: bool = False) -> tuple[str, ...]:
    """Tools a caller may be asked about on `version`; Write and Edit additionally need the write opt-in."""
    row = measured(version) or {}
    writes = row.get("caller_write_tools", ())
    return tuple(dict.fromkeys(
        tool for tool in (*row.get("caller_approval_tools", ()), *(writes if write else ()))
        if write or tool not in {*writes, "Write", "Edit"}))


def feature_specs():
    from .qualification import FeatureSpec
    from .qualification_probes import claude_probe
    features = ("caller_approval_tools", "caller_write_tools", "path_safety_prompts", "denial_texts", "allowed_commands")
    return {feature: FeatureSpec(feature, "claude-permissions-v1",
                                seeds={v: dict(row) for v, row in MEASURED.items() if row.get(feature)},
                                probe=claude_probe, timeout_seconds=180, seed_platforms=("darwin",)) for feature in features}


def qualify(backend: Any, feature: str) -> str:
    """Require a receipt for the installed binary before dispatch."""
    from .qualification import require
    return require(backend, feature).identity.version

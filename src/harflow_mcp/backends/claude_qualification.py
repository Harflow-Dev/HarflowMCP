"""The one measured Claude Code qualification table: every feature gates on it, so none can drift."""
from __future__ import annotations

import re
from typing import Any

from .base import UnsupportedCapability

# 2.1.302 keeps its historical Bash-only approval scope and is not installed, so it cannot be
# re-measured. 2.1.303 is never added because it was not measured.
MEASURED: dict[str, dict[str, Any]] = {
    "2.1.302": {"caller_approval_tools": ("Bash",)},
    "2.1.310": {"caller_approval_tools": ("Bash", "Read"), "denial_texts": True, "allowed_commands": True},
}

_VERSION = re.compile(r"(\d+\.\d+\.\d+)(?: \(Claude Code\))?")


def parse_version(observed: Any) -> str | None:
    match = _VERSION.fullmatch(observed.strip()) if isinstance(observed, str) else None
    return match.group(1) if match else None


def measured(version: Any) -> dict[str, Any] | None:
    return MEASURED.get(version) if isinstance(version, str) else None


def supports(version: Any, feature: str) -> bool:
    return bool((measured(version) or {}).get(feature))


def qualify(backend: Any, feature: str) -> str:
    """The exact installed version, which must be measured for `feature`; probed before every spawn."""
    from . import version as probe
    observed = parse_version(probe(backend))
    if observed is None or not supports(observed, feature):
        raise UnsupportedCapability(f"{feature} requires a measured Claude Code version ({', '.join(v for v, row in MEASURED.items() if row.get(feature))}); observed version is not qualified")
    return observed

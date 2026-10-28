"""Validation and inheritance for the opt-in, group-scoped review output grant."""
from __future__ import annotations

from pathlib import Path
from collections.abc import Mapping
from typing import Any



def effective(value: bool | None, source: object | None = None) -> bool:
    if value is None and source is not None:
        saved = getattr(source, "enforcement", None)
        if isinstance(saved, dict):
            value = saved.get("group_report_output", False)
    if value is None:
        return False
    if type(value) is not bool:
        raise ValueError("group_report_output must be a boolean or null")
    return value


def has_authority(record: object) -> bool:
    """Detect the scoped grant, including malformed saved markers, without filesystem I/O."""
    saved = record.get("enforcement") if isinstance(record, Mapping) else getattr(record, "enforcement", None)
    return isinstance(saved, Mapping) and saved.get("group_report_output", False) is not False


def path_for(log_dir: Path, group: str, repo: Path) -> Path:
    """Compute without allocating, and refuse overlap or existing symlink components."""
    from . import shared_scratch
    path = shared_scratch.directory(log_dir, "group", group)
    stop = Path(log_dir).parent
    current = path
    while True:
        if current.is_symlink():
            raise ValueError("Group report output refuses a symlink in its scratch path")
        if current.exists() and not current.is_dir():
            raise ValueError("Group report output requires a directory path")
        if current == stop:
            break
        if stop not in current.parents:
            raise ValueError("Group report output path is outside the selected state root")
        current = current.parent
    canonical, repository = path.resolve(), repo.resolve()
    if canonical == repository or canonical in repository.parents or repository in canonical.parents:
        raise ValueError("Group report output must not overlap the repository")
    return path


def validate(enabled: bool, *, backend: Any, log_dir: Path, repo: Path, group: str | None,
             freedom: str, caller_approvals: bool = False, caller_write_approvals: bool = False,
             native: bool = False, builder: bool = False, extra_paths: bool = False, network: bool | None = None) -> Path | None:
    if not enabled:
        return None
    if freedom != "read_only":
        raise ValueError("group_report_output requires read_only repository access")
    if network is True:
        raise ValueError("group_report_output requires network disabled")
    if not isinstance(group, str) or not group.strip():
        raise ValueError("group_report_output requires a nonempty group")
    if len(group) > 128:
        raise ValueError("group_report_output group must be at most 128 characters")
    if caller_approvals or caller_write_approvals or native or builder or extra_paths:
        raise ValueError("group_report_output requires headless execution without caller approvals, native execution, workflow builders or extra writable paths")
    from .backends.base import UnsupportedCapability
    support = getattr(backend, "group_report_output_support", lambda: {"supported": False})()
    if not support.get("supported"):
        reason = support.get("reason", "this backend has no qualified group-scoped read-only output mechanism")
        raise UnsupportedCapability(f"group_report_output is unavailable: {reason}. Omit the option and return findings for engine-generated output, or use a qualified backend.")
    return path_for(log_dir, group, repo)

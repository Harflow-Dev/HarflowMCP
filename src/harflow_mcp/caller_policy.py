"""Cross-product environment signals for authority-bearing operations."""
from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path


def signals(environ: Mapping[str, str] | None = None) -> tuple[bool, int]:
    env = os.environ if environ is None else environ
    current, legacy = env.get('HARFLOW_TASK_ID'), env.get('PB_TASK_ID')
    present = any(key in env for key in ('HARFLOW_TASK_ID', 'PB_TASK_ID', 'HARFLOW_DEPTH', 'PB_DEPTH'))
    depths = []
    for key in ('HARFLOW_DEPTH', 'PB_DEPTH'):
        if key in env:
            raw = env[key]
            if not raw.isascii() or not raw.isdecimal():
                raise ValueError(f'{key} is malformed; unset stale HARFLOW_TASK_ID, PB_TASK_ID, HARFLOW_DEPTH and PB_DEPTH in a human shell')
            depths.append(int(raw))
    if present:
        from .store import validate_task_id
        if not current or legacy is not None and legacy != current:
            raise ValueError('Foreign or conflicting HARFLOW_TASK_ID/PB_TASK_ID; unset stale HARFLOW_TASK_ID, PB_TASK_ID, HARFLOW_DEPTH and PB_DEPTH in a human shell')
        validate_task_id(current)
    return present, max(depths, default=0)


def require_mutation(log_dir: Path, *, caller=None, environ: Mapping[str, str] | None = None):
    """Require a live, related Harflow parent whenever either product signals a worker."""
    present, _ = signals(environ)
    if not present:
        return caller
    from . import lineage
    if caller is None:
        result = lineage.detect_caller_detail(log_dir, environ=environ)
        caller = result.caller
    if caller is None:
        raise ValueError('HARFLOW_TASK_ID/PB_TASK_ID parent is missing or unconfirmed; unset stale HARFLOW_TASK_ID, PB_TASK_ID, HARFLOW_DEPTH and PB_DEPTH in a human shell')
    env = os.environ if environ is None else environ
    if caller.record.task_id != env['HARFLOW_TASK_ID']:
        raise ValueError('HARFLOW_TASK_ID/PB_TASK_ID does not match the confirmed live caller; unset stale worker variables in a human shell')
    return caller

"""Independent display-only recovery activity page."""
from __future__ import annotations
from typing import Any
from pathlib import Path
from . import store, supervision
from .backends import get


def page(log_dir: Path, record: Any, cursor: str | None = None, limit: int = 50, *, supervision_evidence: dict | None = None) -> dict[str, Any]:
    evidence = supervision_evidence or supervision.evidence(record, store.log_path(log_dir, record.task_id),
        observed_terminal=record.status in store.TERMINAL_RECORD_STATUSES and not store.outcome_unobserved(record))
    if evidence['capture_state'] != 'lost_with_owner':
        return {'source': 'backend_transcript', 'display_only': True, 'events': [],
                'next_cursor': None, 'has_more': False, 'reset': False,
                'notices': ['Transcript recovery requires confirmed capture loss']}
    return get(record.backend).recovery_activity(record, evidence['last_captured_output_at'], cursor, limit)

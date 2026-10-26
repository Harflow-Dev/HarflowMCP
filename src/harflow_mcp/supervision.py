"""Read-only supervision evidence, separate from conservative execution guards."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import identity, inbox


def evidence(record: Any, raw_log: Path, *, observed_terminal: bool = False,
             live: bool = False, input_closed: bool = False) -> dict[str, Any]:
    owner = "undecidable" if observed_terminal else "alive" if live else identity.identity_check(record.owner)
    pid = getattr(record, "pid", None)
    if pid is None:
        proc = getattr(record, "proc", None)
        pid = getattr(proc, "pid", None)
    process = "undecidable" if observed_terminal or not pid else identity.check_detail(
        identity.task_identity(pid, record.start_time, record.markers if hasattr(record, "markers") else [])
    )[0]
    capture = ("observed_terminal" if observed_terminal else "live" if owner == "alive"
               else "lost_with_owner" if owner == "dead" else "unknown")
    stamp = None
    try:
        stamp = datetime.fromtimestamp(raw_log.stat().st_mtime, timezone.utc).isoformat()
    except OSError:
        pass
    # Owner-local closure is known in memory. Recovery only claims closed from bounded durable
    # evidence; absent evidence stays unknown, never an assertion that the channel is open.
    can_inspect_input = record.live_input and pid is not None
    input_closed = input_closed or (can_inspect_input and not live and inbox.is_closed(raw_log.parent, record.task_id, tail_bytes=65536))
    reason = inbox.closure_reason(raw_log.parent, record.task_id) if can_inspect_input else None
    channel = ("not_applicable" if not record.live_input else "closed_with_owner"
               if owner == "dead" and not observed_terminal else "closed"
               if input_closed or observed_terminal else "open"
               if live else "unknown")
    return {"owner_state": owner, "process_state": process, "capture_state": capture,
            "live_input_state": channel, **({"live_input_closed_reason": reason} if reason is not None else {}), "last_captured_output_at": stamp,
            "last_captured_output_source": "raw_stream_log_mtime" if stamp else None}


def fields(record: Any, raw_log: Path, **kwargs) -> dict[str, Any]:
    value = evidence(record, raw_log, **kwargs)
    return {"supervision": value, "output_capture": value["capture_state"],
            "input_channel": value["live_input_state"],
            **({"input_closed_reason": value["live_input_closed_reason"]} if 'live_input_closed_reason' in value else {}),
            "last_captured_output_at": value["last_captured_output_at"]}

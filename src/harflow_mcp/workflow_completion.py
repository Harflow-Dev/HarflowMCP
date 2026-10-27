"""Pure qualification of workflow completion evidence across compatible writers."""
from __future__ import annotations

import math
from typing import Any

TERMINAL_METADATA_STATUSES = frozenset({"completed", "failed", "cancelled", "timed_out"})


def qualified_finished_at(run: dict[str, Any]) -> int | float | None:
    """Reject completion evidence invalidated by a writer without witness support."""
    timestamp = run.get("finished_at")
    sequence = run.get("sequence")
    witness = run.get("finished_at_sequence")
    if run.get("status") not in TERMINAL_METADATA_STATUSES:
        return None
    if type(sequence) is not int or sequence < 0 or type(witness) is not int or witness != sequence:
        return None
    if type(timestamp) not in (int, float) or type(timestamp) is float and not math.isfinite(timestamp):
        return None
    return timestamp

"""Bounded ctl request transport; authority and semantics belong to the steering service."""
from __future__ import annotations

from pathlib import Path
import sys

MAX_REQUEST_BYTES = 128 * 1024


def load_request(filename: str) -> dict:
    """Read a complete bounded UTF-8 JSON object without normalizing caller prose."""
    from .workflow_steering import SteeringError, strict_loads
    try:
        if filename == "-":
            raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
        else:
            with Path(filename).open("rb") as stream:
                raw = stream.read(MAX_REQUEST_BYTES + 1)
        if len(raw) > MAX_REQUEST_BYTES:
            raise ValueError("Steering request exceeds 128 KiB")
        value = strict_loads(raw.decode("utf-8"))
        if type(value) is not dict:
            raise ValueError("Steering request must be a JSON object")
        return value
    except (OSError, ValueError, UnicodeError) as exc:
        raise SteeringError("steering_invalid_request", str(exc)) from None

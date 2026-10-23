"""Validation and dispatch-time resolution of the live background idle bound."""
from __future__ import annotations
import math
import os
from typing import Any

DEFAULT_SECONDS = 600.0
MAX_SECONDS = 21600.0


def validate(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 < value <= MAX_SECONDS:
        raise ValueError('background_idle_seconds must be a finite positive number no greater than 21600')
    return float(value)


def resolve(value: Any = None, inherited: Any = None, environ: Any = None) -> float:
    if value is not None:
        return validate(value)
    if inherited is not None:
        return validate(inherited)
    raw = (os.environ if environ is None else environ).get('HARFLOW_LIVE_IDLE_SECONDS')
    if raw is not None:
        try:
            return validate(float(raw))
        except (ValueError, TypeError, OverflowError):
            pass
    return DEFAULT_SECONDS

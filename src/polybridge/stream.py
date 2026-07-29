"""Line-level parsing of an agent's JSONL output.

Deliberately knows nothing about any particular agent: what the events *mean* is each backend's
business (`backends/*.ingest`). All that happens here is turning a line into a dict, and refusing to
fail on a line we did not expect — every backend's stream carries warnings, notices, and whatever
the next CLI release adds.
"""

from __future__ import annotations

import json
import logging
from typing import Any

log = logging.getLogger(__name__)


def parse_line(line: str) -> dict[str, Any] | None:
    """Decode one stream line, or return None if it is blank, truncated, or not a JSON object."""
    line = line.strip()
    if not line:
        return None
    try:
        event = json.loads(line)
    except ValueError:
        log.debug("skipping non-JSON stream line: %.200s", line)
        return None
    if not isinstance(event, dict):
        log.debug("skipping non-object stream event: %.200s", line)
        return None
    return event

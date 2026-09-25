"""Per-task normalized event log: `<task_id>.events.jsonl`.

Where the raw stream log (`<task_id>.jsonl`) is whatever bytes the backend's CLI produced, this is
the bridge's own normalized view of the same run — one JSON object per line, with envelope fields
that mean the same thing across every backend (see `backends/base.py`'s `normalize` contract for
what a backend hands back). Written only by the server process that owns the task: single-writer by
design, so no locking is needed here the way `retention.py` needs it for cross-process deletion.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import store

log = logging.getLogger(__name__)

EVENTS_SUFFIX = ".events.jsonl"
EVENT_LOG_VERSION = 1

# The closed set of `kind` values in a v1 events log — the Monitor app (Stage C) switches on these,
# so adding one is a contract change: update this set, README.md's list, and the app's
# `TaskEvent.Kind` (macos/PbCore/MonitorCore/Sources/MonitorCore/Events.swift) together.
# Found by reading every emit site: the bridge's own writes in `tasks.py` (`task_started`,
# `task_finished`, `user_message`, `notice`, `undelivered`) and the helpers every backend's
# `normalize` builds events with (`backends/normalize.py`). Pinned by `tests/test_events.py`.
EVENT_KINDS = frozenset(
    {
        "task_started",
        "assistant_text",
        "tool_call",
        "tool_result",
        "user_message",
        "usage",
        "notice",
        "task_finished",
        "undelivered",
    }
)


class UnknownEventKind(ValueError):
    """A write named a `kind` outside `EVENT_KINDS` — a programming error, not an I/O failure."""


def events_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{EVENTS_SUFFIX}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventLog:
    """Append-only writer for one task's normalized event stream.

    Every failure mode disables the log rather than raising: a normalized event is a convenience
    view over the raw stream, and losing it must never touch a run's outcome (CLAUDE.md:
    bookkeeping must never change an outcome).
    """

    def __init__(self, path: Path, task_id: str) -> None:
        self._task_id = task_id
        self._seq = 0
        self._handle = None
        try:
            self._handle = path.open("ab")
        except OSError:
            log.warning(
                "task %s: cannot open %s; events will not be recorded", task_id, path, exc_info=True
            )

    def write(
        self,
        kind: str,
        fields: dict[str, Any],
        *,
        raw_offset: int | None = None,
        source_ts: str | None = None,
    ) -> bool:
        """Append one envelope. False if the log is disabled or the write failed. Raises
        `UnknownEventKind` for a `kind` outside `EVENT_KINDS` — the only thing it ever raises.

        Envelope keys win over any same-named key in `fields`, so a caller can pass a backend's own
        normalized dict straight through without stripping fields that happen to collide.
        """
        if kind not in EVENT_KINDS:
            # Raised rather than written, so an unlisted kind never reaches a reader of the frozen
            # v1 schema. Every call site already guards its write, so this cannot change a run's
            # outcome; it fails the test that exercises it (see `tests/conftest.py`).
            raise UnknownEventKind(f"{kind!r} is not a v1 event kind; see events.EVENT_KINDS")
        if self._handle is None:
            return False
        envelope: dict[str, Any] = {
            "v": EVENT_LOG_VERSION,
            "seq": self._seq,
            "observed_at": _now_iso(),
            "source_ts": source_ts,
            "raw_offset": raw_offset,
            "task_id": self._task_id,
            "kind": kind,
        }
        envelope.update((key, value) for key, value in fields.items() if key not in envelope)
        try:
            line = json.dumps(envelope, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            # One unserializable event is dropped; the log itself stays usable for the next.
            log.warning("task %s: dropping an unserializable %s event", self._task_id, kind)
            return False
        try:
            self._handle.write((line + "\n").encode("utf-8"))
            self._handle.flush()
        except OSError:
            log.warning(
                "task %s: events log write failed; disabling it", self._task_id, exc_info=True
            )
            self.close()
            return False
        self._seq += 1
        return True

    def close(self) -> None:
        if self._handle is not None:
            try:
                self._handle.close()
            except OSError:
                pass
            self._handle = None

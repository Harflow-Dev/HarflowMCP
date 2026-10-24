"""Bounded incremental raw-stream replay for long-lived attached task views."""
from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
import logging
import re
import threading
from pathlib import Path

from .backends import Accumulator, get as get_backend
from .state_access import task_operation
from .stream import parse_line

MAX_ENTRIES = 32
READ_BYTES = 64 * 1024
_NEWLINE = re.compile(br'\r\n|\r|\n')
_entries = OrderedDict()
_lock = threading.RLock()
log = logging.getLogger(__name__)


@dataclass
class _Entry:
    backend: object
    source: tuple
    size: int = 0
    modified: tuple = ()
    offset: int = 0
    state: Accumulator = field(default_factory=Accumulator)
    tail: list[str] = field(default_factory=list)
    visible: Accumulator = field(default_factory=Accumulator)
    visible_tail: list[str] = field(default_factory=list)


def _apply(raw, state, tail, backend):
    from .store import TAIL_LINE_CHARS, TAIL_LINES_RETURNED
    # Match text-mode UTF-8/universal-newline iteration in store.replay_log, including the
    # display tail for whitespace and malformed JSON. Only nonblank lines enter the tail.
    line = raw.decode('utf-8')
    if not line.strip():
        return
    tail.append(line[:TAIL_LINE_CHARS])
    del tail[:-TAIL_LINES_RETURNED]
    event = parse_line(line)
    if event is not None:
        backend.ingest(event, state)


def _update(path, backend, entry):
    with path.open('rb') as handle:
        import os
        info = os.fstat(handle.fileno())
        source = info.st_dev, info.st_ino
        modified = info.st_mtime_ns, info.st_ctime_ns
        if (entry is None or entry.backend is not backend or entry.source != source
                or info.st_size < entry.size
                or info.st_size == entry.size and modified != entry.modified):
            entry = _Entry(backend, source)
        if info.st_size == entry.size and modified == entry.modified:
            return entry
        handle.seek(entry.offset)
        pending = b''
        while chunk := handle.read(READ_BYTES):
            pending += chunk
            start = 0
            for match in _NEWLINE.finditer(pending):
                _apply(pending[start:match.start()], entry.state, entry.tail, backend)
                start = match.end()
            pending = pending[start:]
        consumed = handle.tell()
        entry.offset = consumed - len(pending)
        # A final valid JSON record without newline is visible immediately, but never committed:
        # appending even one byte must reevaluate that whole line from its previous checkpoint.
        entry.visible = deepcopy(entry.state)
        entry.visible_tail = list(entry.tail)
        if pending:
            _apply(pending, entry.visible, entry.visible_tail, backend)
        final = os.fstat(handle.fileno())
        current = path.stat()
        if (current.st_dev, current.st_ino) != source or (final.st_dev, final.st_ino) != source:
            raise OSError('Raw stream source changed during incremental replay')
        entry.size = consumed
        # If an append raced EOF, force the next read to examine the new suffix.
        entry.modified = (final.st_mtime_ns, final.st_ctime_ns) if final.st_size == consumed else ()
        return entry


@task_operation
def replay(log_dir: Path, task_id: str, backend_name: str):
    """Replay appended bytes and return an isolated view; metadata and liveness stay uncached."""
    from .store import log_path
    backend = get_backend(backend_name)
    path = log_path(log_dir, task_id)
    key = str(path), id(backend)
    with _lock:
        entry = _entries.pop(key, None)
        try:
            entry = _update(path, backend, entry)
        except FileNotFoundError:
            return Accumulator(), []
        except OSError:
            log.warning('could not replay stream log for task %s', task_id, exc_info=True)
            return Accumulator(), []
        # Exceptions from decoding or the backend also invalidate the popped cache entry.
        _entries[key] = entry
        while len(_entries) > MAX_ENTRIES:
            _entries.popitem(last=False)
        return deepcopy(entry.visible), list(entry.visible_tail)

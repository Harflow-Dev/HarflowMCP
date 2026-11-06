"""Read native task presentation without replaying another transport's grammar.

Owner-recorded display fields do not establish a native terminal or physical
settlement. The normal record/process ownership resolver remains authoritative.
"""
import os
import stat

from .backends.base import Accumulator
from .state_access import open_descriptor, task_layout


def replay(log_dir, record):
    from .store import log_path, TAIL_LINES_RETURNED, TAIL_LINE_CHARS
    state = Accumulator(session_id=record.session_id, summary=record.terminal_summary,
                        total_cost_usd=record.total_cost_usd, is_error=record.status == 'failed')
    tail = []
    try:
        fd = open_descriptor(log_path(log_dir, record.task_id),
            os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, layout=task_layout(log_dir))
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                return state, tail
            offset = max(0, info.st_size - 256 * 1024)
            os.lseek(fd, offset, os.SEEK_SET)
            raw = os.read(fd, 256 * 1024)
            if offset:
                raw = raw.partition(b'\n')[2]
            tail = [line[:TAIL_LINE_CHARS] for line in raw.decode('utf-8', errors='replace').splitlines()[-TAIL_LINES_RETURNED:]]
        finally:
            fd.close()
    except OSError:
        # Presentation loss never changes an observed process outcome.
        pass
    return state, tail

"""The `harflow-ctl task-prompt` result: a bounded, integrity-reported view of a task's stored prompt."""
from __future__ import annotations

import hashlib
import os
import re
import stat as stat_module
from pathlib import Path
from typing import Any

from . import store
from .state_access import open_descriptor, task_layout

DEFAULT_MAX_BYTES = 65_536
MAX_MAX_BYTES = 131_072
VERIFY_CEILING_BYTES = 4 * 1024 * 1024


def _cut(data: bytes, max_bytes: int) -> tuple[str, bool]:
    """Text whose UTF-8 encoding is at most `max_bytes`, and whether it was cut.

    The byte prefix drops only an incomplete trailing character. Invalid bytes elsewhere (possible
    only in an unverified prefix) become U+FFFD, which is longer than a byte, so the decoded text is
    cut again, on a character boundary, if that pushed it over the limit.
    """
    cut = len(data) > max_bytes
    kept = data[:max_bytes]
    if cut:
        for back in range(1, min(4, len(kept)) + 1):
            lead = kept[-back]
            if lead & 0xC0 == 0x80:
                continue  # a continuation byte: keep looking for its lead byte
            need = 1 if lead < 0x80 else 2 if lead < 0xE0 else 3 if lead < 0xF0 else 4
            if need > back:
                kept = kept[:-back]
            break
    text = kept.decode("utf-8", errors="replace")
    used = 0
    for index, character in enumerate(text):
        used += len(character.encode("utf-8"))
        if used > max_bytes:
            return text[:index], True
    return text, cut


def _sidecar(log_dir: Path, record: store.TaskRecord, max_bytes: int) -> tuple[bytes, int, bool] | None:
    """`(bytes, total_size, verified)` of the task-bound sidecar; None when it cannot be read.

    Up to `VERIFY_CEILING_BYTES` the whole file is read and checked against the digest in its name;
    above that only a prefix of `max_bytes` plus a character of slack is read. The ceiling applies to the read itself, so a file that
    grows after `fstat` cannot be read past it.
    """
    try:
        fd = open_descriptor(log_dir / str(record.prompt_source), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                             layout=task_layout(log_dir))
    except OSError:
        return None
    try:
        info = os.fstat(fd)
        if not stat_module.S_ISREG(info.st_mode):
            return None
        verify = info.st_size <= VERIFY_CEILING_BYTES
        wanted = VERIFY_CEILING_BYTES + 1 if verify else max_bytes + 4
        chunks: list[bytes] = []
        received = 0
        while received < wanted:
            chunk = os.read(fd, min(1_048_576, wanted - received))
            if not chunk:
                break
            chunks.append(chunk)
            received += len(chunk)
        data = b"".join(chunks)
        return data, info.st_size, verify and len(data) <= VERIFY_CEILING_BYTES
    except OSError:
        return None
    finally:
        fd.close()


def task_prompt_result(log_dir: Path, record: store.TaskRecord, max_bytes: int) -> dict[str, Any]:
    """`record` is read with `include_prompt=False`: its prompt is the whole text or a 2000-character preview."""
    text: str | None = None
    state, total, integrity, complete = "available", None, "not_applicable", True
    data: bytes | None = None
    if not record.prompt_truncated:
        data = record.prompt.encode("utf-8")
        total = len(data)
    else:
        name = re.fullmatch(re.escape(record.task_id) + r"\.prompt\.([0-9a-f]{64})\.txt", record.prompt_source or "")
        if name is None:
            state = "unavailable"
        else:
            found = _sidecar(log_dir, record, max_bytes)
            if found is None:
                state, data, complete = "preview_only", record.prompt.encode("utf-8"), False
            else:
                data, total, verified = found
                if verified:
                    if hashlib.sha256(data).hexdigest() != name[1]:
                        state, data, total, integrity = "integrity_failed", None, None, "failed"
                    else:
                        integrity = "verified"
                else:
                    integrity = "unverified"
    returned = 0
    truncated = False
    if data is not None:
        text, truncated = _cut(data, max_bytes)
        returned = len(text.encode("utf-8"))
        if state == "available":
            complete = not truncated and (total is None or returned == total)
    else:
        complete = False
    return {"task_id": record.task_id, "title": record.title,
            "prompt": {"state": state, "text": text, "returned_bytes": returned, "total_bytes": total,
                       "truncated": truncated, "complete": complete, "integrity": integrity}}

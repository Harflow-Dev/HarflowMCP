"""Bounded exact-session transcript display reads; never execution evidence."""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
from datetime import datetime
from pathlib import Path
from typing import Any

TAIL_BYTES = 262144
DIRECTORY_LIMIT = 512


def _time(value: str | None) -> float | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() if value else None
    except (ValueError, TypeError, OverflowError):
        return None


def read_activity(record: Any, cutoff: str | None, cursor: str | None = None,
                  limit: int = 50, *, projects: Path | None = None) -> dict[str, Any]:
    page = {"source": "backend_transcript", "display_only": True, "events": [],
            "next_cursor": None, "reset": False, "has_more": False, "notices": []}
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("recovery activity limit must be 1..100")
    session = record.session_id
    if not isinstance(session, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', session):
        page['notices'].append('Exact backend session identity unavailable')
        return page
    root = projects or Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home() / '.claude'))) / 'projects'
    matches = []
    try:
        with os.scandir(root) as entries:
            for number, entry in enumerate(entries):
                if number >= DIRECTORY_LIMIT:
                    page['notices'].append('Transcript directory search budget reached')
                    break
                if entry.is_dir(follow_symlinks=False):
                    candidate = Path(entry.path) / (session + '.jsonl')
                    try:
                        if stat.S_ISREG(candidate.lstat().st_mode):
                            matches.append(candidate)
                    except OSError:
                        pass
    except OSError:
        page['notices'].append('Backend transcript directory unavailable')
        return page
    if len(matches) != 1:
        page['notices'].append('Exact session transcript unavailable or ambiguous')
        return page
    try:
        directory_fd = os.open(matches[0].parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fd = os.open(matches[0].name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
        finally:
            os.close(directory_fd)
        with os.fdopen(fd, 'rb') as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise OSError('not a regular transcript')
            begin = max(0, info.st_size - TAIL_BYTES)
            handle.seek(begin)
            raw = handle.read(TAIL_BYTES)
    except OSError:
        page['notices'].append('Backend transcript unreadable')
        return page
    if begin:
        split = raw.find(b'\n')
        raw = raw[split + 1:] if split >= 0 else b''
        begin += split + 1 if split >= 0 else len(raw)
        page['notices'].append('Only bounded transcript tail available')
    complete = raw.rfind(b'\n') + 1
    if complete != len(raw):
        page['notices'].append('Incomplete trailing transcript line omitted')
    boundary = begin + complete
    fingerprint = f'{info.st_dev}:{info.st_ino}'
    after = begin
    if cursor:
        try:
            if len(cursor) > 1024:
                raise ValueError()
            old = json.loads(base64.urlsafe_b64decode(cursor))
            if old['file'] != fingerprint or old['session'] != session or not begin <= old['offset'] <= boundary:
                page['reset'] = True
            else:
                anchor_start = max(begin, old['offset'] - 64)
                anchor = raw[anchor_start - begin:old['offset'] - begin]
                if old.get('anchor') != hashlib.sha256(anchor).hexdigest():
                    page['reset'] = True
                else:
                    after = old['offset']
        except (ValueError, KeyError, TypeError, UnicodeError):
            page['reset'] = True
    start = _time(record.started_at)
    stop = _time(cutoff)
    threshold = max(v for v in (start, stop) if v is not None) if start is not None or stop is not None else None
    if threshold is None or stop is None:
        page['notices'].append('Capture cutoff uncertain; transcript activity may overlap captured output')
    offset = begin
    normalized = []
    for line in raw[:complete].splitlines(keepends=True):
        line_start = offset
        offset += len(line)
        if offset <= after:
            continue
        try:
            value = json.loads(line)
        except (ValueError, UnicodeError, RecursionError):
            continue
        if not isinstance(value, dict) or value.get('sessionId') != session:
            continue
        ts = _time(value.get('timestamp'))
        if ts is None or threshold is not None and ts <= threshold:
            continue
        message = value.get('message')
        if value.get('type') not in ('assistant', 'user') or not isinstance(message, dict):
            continue
        content = message.get('content')
        if not isinstance(content, list):
            continue
        for index, block in enumerate(content[:100]):
            if not isinstance(block, dict):
                continue
            from . import normalize as nz
            kind = block.get('type')
            if kind == 'text' and value.get('type') == 'assistant' and isinstance(block.get('text'), str):
                event = {'kind': 'assistant_text', 'text': block['text'][:2000]}
            elif kind == 'tool_use' and value.get('type') == 'assistant' and isinstance(block.get('id'), str) and isinstance(block.get('name'), str) and isinstance(block.get('input'), dict):
                event = nz.tool_call(call_id='recovery:' + block['id'], tool=block['name'], category='other', input=block['input'])
            elif kind == 'tool_result' and value.get('type') == 'user' and isinstance(block.get('tool_use_id'), str) and isinstance(block.get('content'), (str, list)):
                event = nz.tool_result(call_id='recovery:' + block['tool_use_id'], ok=not block.get('is_error', False), output=block['content'])
            else:
                continue
            source_id = hashlib.sha256(f'{session}:{value.get("uuid", line_start)}:{index}'.encode()).hexdigest()
            normalized.append({**event, 'source_id': source_id, 'source_ts': value['timestamp']})
    if len(normalized) > limit:
        page['notices'].append('Recovery page bounded; older display activity omitted')
    selected = normalized[-limit:]
    while selected and len(json.dumps(selected, ensure_ascii=True).encode()) > 240000:
        selected.pop(0)
        if 'Recovery page byte budget reached' not in page['notices']:
            page['notices'].append('Recovery page byte budget reached')
    page['events'] = selected
    page['next_cursor'] = base64.urlsafe_b64encode(json.dumps({'file': fingerprint, 'session': session, 'offset': boundary, 'anchor': hashlib.sha256(raw[max(0, complete - 64):complete]).hexdigest()}).encode()).decode()
    return page

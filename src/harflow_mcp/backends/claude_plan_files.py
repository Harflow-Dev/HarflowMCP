"""Metadata-only observation of Claude's built-in plan writes; grants no write authority."""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path


def _plain(path: Path) -> bool:
    return path.is_absolute() and not any(p.is_symlink() for p in (path, *path.parents))


def plan_roots(repo: Path) -> tuple[Path, ...]:
    config = Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home() / '.claude'))).absolute()
    roots = [config / 'plans']
    # Claude accepts plansDirectory only inside its project root. These are metadata
    # candidates, never execution grants. CLI settings precedence cannot widen this set
    # beyond the explicit project namespace.
    for settings in (config / 'settings.json', repo / '.claude/settings.json', repo / '.claude/settings.local.json'):
        try:
            if not _plain(settings) or settings.stat().st_size > 65536:
                continue
            descriptor = os.open(settings, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    continue
                raw = os.read(descriptor, 65537)
                if len(raw) > 65536:
                    continue
                value = json.loads(raw).get('plansDirectory')
            finally:
                os.close(descriptor)
            if not isinstance(value, str) or not value or len(value) > 4096:
                continue
            target = Path(value)
            target = target if target.is_absolute() else repo / target
            target = Path(os.path.abspath(target))
            if target != repo and target.is_relative_to(repo) and _plain(target):
                roots.append(target)
        except (OSError, ValueError, TypeError, AttributeError):
            continue
    return tuple(dict.fromkeys(roots))


def observe(event: dict, state: dict, repo: Path) -> list[dict]:
    """Join successful top-level Write/Edit results to bounded pending calls.

    Paths must be regular files directly in an explicit plan directory. Returned
    references contain stat metadata only; neither plan content nor arbitrary config
    files are read. Duplicate/replayed results produce no additional reference.
    """
    if event.get('parent_tool_use_id'):
        return []
    message = event.get('message')
    blocks = message.get('content') if isinstance(message, dict) else None
    if not isinstance(blocks, list):
        return []
    pending = state.setdefault('plan_file_calls', {})
    result = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if event.get('type') == 'assistant' and block.get('type') == 'tool_use' and block.get('name') in ('Write', 'Edit'):
            call_id, arguments = block.get('id'), block.get('input')
            path = arguments.get('file_path') if isinstance(arguments, dict) else None
            if isinstance(call_id, str) and len(call_id) <= 256 and isinstance(path, str) and len(path) <= 4096 and len(pending) < 100:
                pending.setdefault(call_id, path)
        if event.get('type') != 'user' or block.get('type') != 'tool_result':
            continue
        call_id = block.get('tool_use_id')
        if not isinstance(call_id, str):
            continue
        raw = pending.pop(call_id, None)
        if raw is None or block.get('is_error'):
            continue
        path = Path(raw)
        try:
            if not _plain(path) or path.parent not in plan_roots(repo):
                continue
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode):
                continue
            result.append({'path': str(path), 'tool_use_id': call_id,
                           'registered_observation': {'device': info.st_dev, 'inode': info.st_ino,
                                                      'size': info.st_size, 'mtime_ns': info.st_mtime_ns}})
        except (OSError, ValueError):
            continue
    return result

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


def candidates(event: dict) -> tuple[dict, ...]:
    """Extract bounded metadata only, without filesystem or configuration access."""
    if event.get('parent_tool_use_id'):
        return ()
    message = event.get('message')
    blocks = message.get('content') if isinstance(message, dict) else None
    if not isinstance(blocks, list):
        return ()
    frames = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if event.get('type') == 'assistant' and block.get('type') == 'tool_use' and block.get('name') in ('Write', 'Edit'):
            arguments = block.get('input')
            call_id = block.get('id')
            path = arguments.get('file_path') if isinstance(arguments, dict) else None
            if (isinstance(call_id, str) and 0 < len(call_id) <= 256 and len(call_id.encode('utf-8')) <= 256 and
                    isinstance(path, str) and 0 < len(path) <= 4096 and len(path.encode('utf-8')) <= 4096):
                frames.append({'id': call_id, 'path': path})
        elif event.get('type') == 'user' and block.get('type') == 'tool_result':
            call_id = block.get('tool_use_id')
            if isinstance(call_id, str) and 0 < len(call_id) <= 256 and len(call_id.encode('utf-8')) <= 256:
                frames.append({'id': call_id, 'error': bool(block.get('is_error'))})
        if len(frames) == 100:
            break
    return tuple(frames)


def observe_candidates(frames, state: dict, repo: Path) -> list[dict]:
    """Observe extracted frames using exclusively worker-owned correlation state."""
    from ..artifacts import _regular_stat, _observation
    pending = state.setdefault('plan_file_calls', {})
    result = []
    for frame in frames:
        call_id = frame['id']
        if 'path' in frame:
            if len(pending) < 100:
                pending.setdefault(call_id, frame['path'])
            continue
        raw = pending.pop(call_id, None)
        if raw is None or frame['error']:
            continue
        path = Path(raw)
        try:
            if not _plain(path) or path.parent not in plan_roots(repo):
                continue
            directory = path.parent.stat(follow_symlinks=False)
            directory_identity = {'device': directory.st_dev, 'inode': directory.st_ino}
            info = _regular_stat(path, directory_identity=directory_identity)
            result.append({'path': str(path), 'tool_use_id': call_id,
                           'plan_directory': str(path.parent),
                           'plan_directory_identity': directory_identity,
                           'registered_observation': _observation(info)})
        except (OSError, ValueError):
            continue
    return result


def observe(event: dict, state: dict, repo: Path) -> list[dict]:
    """Compatibility adapter for callers already running observation off-thread."""
    return observe_candidates(candidates(event), state, repo)

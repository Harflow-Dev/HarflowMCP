"""Bounded, observation-only inventory of candidate Claude settings files."""
from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import re
import select
import stat
import subprocess
import sys
import time

MAX_BYTES = 256 * 1024
MAX_TOTAL_BYTES = 1024 * 1024
MAX_SOURCES = 24
MAX_RULES = 100
MAX_RULE_LENGTH = 512
BUDGET = 0.8
LIMITATION = ("Settings files are candidates, not proof of applied policy. Managed tier selection, "
              "MDM, registry, server, embedding and helper sources are unknown; this is not a complete policy resolver.")


def _managed_directory(platform):
    """Known file candidates; platform management and selection remain unqualified."""
    if platform == "darwin":
        return Path("/Library/Application Support/ClaudeCode")
    if platform.startswith("linux"):
        return Path("/etc/claude-code")
    return None


def unavailable(enforcement):
    return replace(enforcement, inherited_rules=None, settings_observation={
        "version": None, "applicability": "unknown", "sources": [], "completeness": "partial",
        "limitations": [LIMITATION, "Settings observation was unavailable or timed out."]},
        observation_notices=("Claude inherited permissions are unknown; settings observation was unavailable or timed out.",))


def _may_overlap_publishing(rule):
    """Conservative syntax hints only; this does not implement Claude matching."""
    if rule in {'Bash', 'Bash(*)'}:
        return True
    if re.fullmatch(r'Bash\((?:git|gh)(?::\*| \*)\)', rule):
        return True
    if re.fullmatch(r'Bash\(gh pr(?::\*| \*)\)', rule):
        return True
    return bool(re.search(r'\b(?:git (?:commit|push)|gh pr create)\b', rule))


def _bounded_notices(notices):
    """Summaries stay within 8 KiB and 25 entries, including uncertainty."""
    partial = 'Claude inherited policy is only partially observed; managed and other non-file sources and applicability are unknown.'
    omitted = 'Additional candidate notice text omitted; see inherited_rules and settings_observation.'
    remaining = 8192 - len((partial + omitted).encode('utf-8'))
    result = []
    omission = False
    for notice in notices:
        raw = notice.encode('utf-8')
        if len(result) >= 23 or len(notice) > 4096 or len(raw) > remaining:
            omission = True
            continue
        result.append(notice)
        remaining -= len(raw)
    if omission:
        result.append(omitted)
    result.append(partial)
    return result


def observe(enforcement, cwd, env, executable):
    deadline = time.monotonic() + BUDGET
    version = None
    try:
        proc = subprocess.Popen([executable, "--version"], env=env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, cwd=cwd)
        try:
            ready, _, _ = select.select([proc.stdout], [], [], 0.3)
            output = os.read(proc.stdout.fileno(), 1024) if ready else b""
            proc.wait(timeout=0.3)
            # A cold CLI may emit its version after the initial select timed out.
            ready, _, _ = select.select([proc.stdout], [], [], 0)
            if ready and len(output) < 1024:
                output += os.read(proc.stdout.fileno(), 1024 - len(output))
            match = re.search(rb"\b(\d+\.\d+\.\d+)\b", output)
            if proc.returncode == 0 and match:
                version = match.group(1).decode("ascii")
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            proc.stdout.close()
    except (OSError, subprocess.SubprocessError):
        pass
    applicability = "cwd_discovery_measured" if version == "2.1.310" and sys.platform == "darwin" else "unqualified"
    launch_root = Path(cwd).resolve()
    home = env.get("HOME")
    config = env.get("CLAUDE_CONFIG_DIR") or (str(Path(home) / ".claude") if home else None)
    candidates = []
    if config:
        config_path = Path(config)
        if not config_path.is_absolute():
            config_path = launch_root / config_path
        config_path = config_path.resolve()
        candidates.append((config_path / "settings.json", "user", config_path))
    candidates.extend(((launch_root / ".claude" / "settings.json", "project", launch_root), (launch_root / ".claude" / "settings.local.json", "local", launch_root)))
    managed = _managed_directory(sys.platform)
    limitations = [LIMITATION]
    if not config:
        limitations.append("User settings location is unknown because HOME and CLAUDE_CONFIG_DIR are absent.")
    if managed:
        managed = managed.resolve()
        candidates.append((managed / 'managed-settings.json', 'managed', managed))
        fragments = managed / 'managed-settings.d'
        try:
            # Never follow a symlinked fragment directory.
            if fragments.is_symlink():
                limitations.append("Managed fragment directory is a symlink and was not read.")
            elif fragments.is_dir():
                with os.scandir(fragments) as entries:
                    names = []
                    for count, entry in enumerate(entries):
                        if count >= MAX_SOURCES:
                            limitations.append('Managed fragment directory entry limit reached.')
                            break
                        if entry.name.endswith('.json'):
                            names.append(entry.name)
                    names.sort()
                if len(names) > MAX_SOURCES - len(candidates):
                    limitations.append("Managed fragment source limit reached.")
                candidates.extend((fragments / name, 'managed', managed) for name in names[:max(0, MAX_SOURCES - len(candidates))])
        except OSError:
            limitations.append("Managed fragment directory could not be inventoried.")
    else:
        limitations.append("Managed file paths on this platform are unqualified.")
    sources, rules = [], []
    total_bytes = 0
    for path, scope, root in candidates[:MAX_SOURCES]:
        source = {"path": str(path), "scope": scope, "status": "unknown", "applicability": "candidate"}
        sources.append(source)
        if time.monotonic() > deadline:
            source['status'] = 'truncated'
            limitations.append('Settings observation time limit reached.')
            continue
        if total_bytes >= MAX_TOTAL_BYTES:
            source['status'] = 'truncated'
            if 'Settings total byte limit reached.' not in limitations:
                limitations.append('Settings total byte limit reached.')
            continue
        try:
            flags = os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0) | getattr(os, 'O_NOFOLLOW', 0)
            # lstat first prevents opening FIFOs and symlinks; fstat protects replacement races.
            if any(parent.is_symlink() for parent in path.parents if parent != root and root in parent.parents):
                source['status'] = 'symlink'; continue
            info = os.lstat(path)
            if stat.S_ISLNK(info.st_mode):
                source['status'] = 'symlink'; continue
            if not stat.S_ISREG(info.st_mode):
                source['status'] = 'special'; continue
            if info.st_size > MAX_BYTES:
                source['status'] = 'oversize'; continue
            if total_bytes + info.st_size > MAX_TOTAL_BYTES:
                source['status'] = 'truncated'
                if 'Settings total byte limit reached.' not in limitations:
                    limitations.append('Settings total byte limit reached.')
                continue
            fd = os.open(path, flags)
            with os.fdopen(fd, 'rb') as handle:
                info = os.fstat(handle.fileno())
                if not stat.S_ISREG(info.st_mode):
                    source['status'] = 'special'; continue
                read_limit = min(MAX_BYTES, MAX_TOTAL_BYTES - total_bytes)
                # Charge every byte, including the one-byte overflow sentinel.
                raw = handle.read(read_limit + 1)
                total_bytes += len(raw)
            if len(raw) > read_limit:
                source['status'] = 'truncated'
                limitation = ('Settings total byte limit reached.' if total_bytes > MAX_TOTAL_BYTES
                              else 'Settings file byte limit reached.')
                if limitation not in limitations:
                    limitations.append(limitation)
                continue
            data = json.loads(raw)
            if not isinstance(data, dict) or not isinstance(data.get('permissions', {}), dict):
                source['status'] = 'invalid'; continue
            permissions = data.get('permissions', {})
            if any(key in permissions and (not isinstance(permissions[key], list) or
                   any(not isinstance(rule, str) for rule in permissions[key])) for key in ('ask', 'deny')):
                source['status'] = 'invalid'; continue
            source['status'] = 'readable'
            for effect in ('deny', 'ask'):
                for rule in permissions.get(effect, []):
                    if len(rules) >= MAX_RULES or len(rule) > MAX_RULE_LENGTH:
                        if 'Rule count or length limit reached; some rules omitted.' not in limitations:
                            limitations.append('Rule count or length limit reached; some rules omitted.')
                        continue
                    rules.append({'source': str(path), 'effect': effect, 'rule': rule, 'applicability': 'candidate'})
            if permissions.get('allowManagedPermissionRulesOnly') is True or data.get('allowManagedPermissionRulesOnly') is True:
                limitations.append('A managed-only permission setting was observed; its selection and effect are unknown.')
        except FileNotFoundError:
            source['status'] = 'absent'
        except (UnicodeError, ValueError, json.JSONDecodeError):
            source['status'] = 'invalid'
        except OSError:
            source['status'] = 'unreadable'
    skipped = {}
    for source in sources:
        if source['status'] not in {'readable', 'absent'}:
            skipped[source['status']] = skipped.get(source['status'], 0) + 1
    notices = []
    if skipped:
        summary = 'Settings candidates skipped: ' + ', '.join(f'{count} {status}' for status, count in sorted(skipped.items())) + '.'
        limitations.append(summary)
        notices.append('Claude ' + summary)
    for source in sources:
        observed = [item for item in rules if item['source'] == source['path']]
        if not observed:
            continue
        counts = ', '.join(f"{sum(item['effect'] == effect for item in observed)} {effect}" for effect in ('deny', 'ask'))
        overlap = ' These may overlap publishing commands.' if any(_may_overlap_publishing(item['rule']) for item in observed) else ''
        notices.append(f"Claude candidate rules in {source['path']}: {counts}; they may restrict this run.{overlap} See inherited_rules.")
    if any('Managed fragment directory' in item for item in limitations):
        notices.append('Claude managed fragment inventory was skipped or incomplete; see settings_observation.limitations.')
    notices = _bounded_notices(notices)
    return replace(enforcement, inherited_rules=tuple(rules), settings_observation={
        'version': version, 'applicability': applicability, 'sources': sources,
        'completeness': 'partial', 'limitations': list(dict.fromkeys(limitations))}, observation_notices=tuple(notices))

"""Bounded qualification of configuration that can execute outside Codex's sandbox.

This is a conservative supported profile, not a general configuration auditor.
"""
import hashlib
import os
import pwd
from pathlib import Path
import stat
import tomllib

MAX_BYTES = 256 * 1024
MAX_FILES = 32
MAX_NODES = 4096
MAX_DEPTH = 24
EXECUTION_KEYS = frozenset(('notify', 'hooks', 'mcp_servers', 'plugins'))


def _mdm_absence() -> tuple:
    """Read effective macOS preferences without decoding or exposing values."""
    import ctypes
    try:
        cf = ctypes.CDLL('/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation')
        cf.CFStringCreateWithCString.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_uint32]
        cf.CFStringCreateWithCString.restype = ctypes.c_void_p
        cf.CFPreferencesCopyAppValue.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        cf.CFPreferencesCopyAppValue.restype = ctypes.c_void_p
        cf.CFPreferencesAppValueIsForced.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
        cf.CFPreferencesAppValueIsForced.restype = ctypes.c_bool
        cf.CFRelease.argtypes = [ctypes.c_void_p]
        domain = cf.CFStringCreateWithCString(None, b'com.openai.codex', 0x08000100)
        if not domain:
            raise ValueError('Codex managed preferences cannot be qualified')
        try:
            for name in (b'config_toml_base64', b'requirements_toml_base64'):
                key = cf.CFStringCreateWithCString(None, name, 0x08000100)
                if not key:
                    raise ValueError('Codex managed preferences cannot be qualified')
                try:
                    value = cf.CFPreferencesCopyAppValue(key, domain)
                    forced = cf.CFPreferencesAppValueIsForced(key, domain)
                    present = bool(value)
                    if value:
                        cf.CFRelease(value)
                    if present or forced:
                        raise ValueError('Codex managed preferences are not qualified for extra writable paths')
                finally:
                    cf.CFRelease(key)
        finally:
            cf.CFRelease(domain)
    except (OSError, AttributeError):
        raise ValueError('Codex managed preferences cannot be qualified') from None
    return ('managed_preferences_absent',)


def qualify_configuration(repo: Path, scopes: list[tuple[Path, str]], grants: list[tuple[Path, str]] = ()) -> tuple:
    from ..runtime_paths import _chain
    repo = repo.resolve(strict=True)
    home = Path(os.environ.get('CODEX_HOME', str(Path.home() / '.codex'))).expanduser()
    if not home.is_absolute():
        raise ValueError('extra writable paths require an absolute CODEX_HOME')
    home = home.absolute()
    pending = [(home / 'config.toml', False)]
    observations = [_mdm_absence()]
    visited = set()
    nodes = 0

    def protect(path):
        chain, target = _chain(path)
        for scope, kind in scopes:
            if any(scope == part or (kind == 'directory' and part.is_relative_to(scope)) for part in chain):
                raise ValueError('writable authority overlaps Codex configuration namespace')
        return chain, target

    def identities(chain):
        result = []
        for component in sorted(chain):
            try:
                info = component.lstat()
            except FileNotFoundError:
                result.append((str(component), None))
                continue
            common = (str(component), info.st_dev, info.st_ino, stat.S_IFMT(info.st_mode))
            if stat.S_ISDIR(info.st_mode):
                # Sibling creation (including task scratch) changes directory
                # metadata without changing this configuration namespace.
                result.append(common)
            elif stat.S_ISLNK(info.st_mode):
                result.append((*common, os.readlink(component)))
            else:
                result.append((*common, info.st_mode, info.st_mtime_ns, info.st_size))
        return tuple(result)

    # These independently discovered execution/policy surfaces are outside the
    # certified minimal profile. Protect absent names and refuse any presence.
    auxiliary = [home / name for name in (
        'hooks/hooks.json', 'hooks.json', 'managed_config.toml', 'requirements.toml')]
    auxiliary += [Path('/etc/codex') / name for name in (
        'config.toml', 'managed_config.toml', 'requirements.toml', 'hooks.json', 'hooks/hooks.json')]
    preference_paths = [Path.home() / 'Library/Preferences/com.openai.codex.plist',
                        Path('/Library/Preferences/com.openai.codex.plist')]
    for path in preference_paths:
        chain, target = protect(path)
        observations.append(('preferences_namespace', str(path), str(target), identities(chain)))
    for root in (Path.home() / 'Library/Preferences/ByHost', Path('/Library/Preferences/ByHost')):
        chain, target = protect(root)
        if any(scope.is_relative_to(root) or scope.is_relative_to(target) for scope, _ in scopes):
            raise ValueError('writable authority overlaps host preferences namespace')
        observations.append(('host_preferences_namespace', str(root), str(target), identities(chain)))
    auxiliary += [Path('/Library/Managed Preferences/com.openai.codex.plist'),
                  Path('/Library/Managed Preferences') / pwd.getpwuid(os.getuid()).pw_name / 'com.openai.codex.plist']
    for path in auxiliary:
        chain, target = protect(path)
        if path.exists() or path.is_symlink():
            raise ValueError('auxiliary Codex configuration is not qualified for extra writable paths')
        observations.append(('auxiliary_absent', str(path), str(target), identities(chain)))

    plugin_root = home / 'plugins'
    chain, target = protect(plugin_root)
    if any(scope.is_relative_to(plugin_root) or scope.is_relative_to(target) for scope, _ in scopes):
        raise ValueError('writable authority overlaps Codex plugin namespace')
    if plugin_root.is_symlink():
        raise ValueError('Codex plugin state aliases are not qualified')
    plugin_directories = []
    if plugin_root.exists():
        pending_directories = [(plugin_root, 0)]
        count = 0
        entries_seen = 0
        while pending_directories:
            directory, depth = pending_directories.pop()
            count += 1
            if count > 64 or depth > 8:
                raise ValueError('Codex plugin state exceeds qualification bound')
            plugin_directories.append((str(directory), identities(_chain(directory)[0])))
            with os.scandir(directory) as entries:
                for entry in entries:
                    entries_seen += 1
                    if entries_seen > 64:
                        raise ValueError('Codex plugin state exceeds qualification bound')
                    if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                        raise ValueError('nonempty Codex plugin state is not qualified for extra writable paths')
                    pending_directories.append((Path(entry.path), depth + 1))
    observations.append(('plugins_empty', str(plugin_root), str(target), identities(chain), tuple(plugin_directories)))

    def disabled(value):
        if value is None or value is False or value == '' or value == [] or value == {}:
            return True
        if isinstance(value, dict):
            if value.get('enabled') is False:
                return True
            return all(isinstance(v, dict) and v.get('enabled') is False for v in value.values())
        return False

    def inspect(value, source, depth=0):
        nonlocal nodes
        nodes += 1
        if nodes > MAX_NODES or depth > MAX_DEPTH:
            raise ValueError('Codex configuration exceeds structural bound')
        if isinstance(value, dict):
            for key, item in value.items():
                if key == 'codex_hooks' and item is not False:
                    raise ValueError('Codex hook feature configuration is not qualified')
                if key in EXECUTION_KEYS and not disabled(item):
                    raise ValueError('extra writable paths require Codex notify, hooks, MCP and plugins disabled')
                if key in ('config_file', 'config_files'):
                    refs = [item] if isinstance(item, str) else item
                    if not isinstance(refs, list) or not refs or any(not isinstance(ref, str) or not ref for ref in refs):
                        raise ValueError('Codex referenced configuration cannot be qualified')
                    for ref in refs:
                        path = Path(ref).expanduser()
                        pending.append((path if path.is_absolute() else source.parent / path, True))
                inspect(item, source, depth + 1)
        elif isinstance(value, list):
            for item in value:
                inspect(item, source, depth + 1)

    # Project configuration discovery is not certified for extra-path execution.
    # Record absence at every ancestor so introducing one invalidates final checks.
    for parent in (repo, *repo.parents):
        path = parent / '.codex' / 'config.toml'
        if path.absolute() == (home / 'config.toml').absolute():
            continue
        chain, target = _chain(path)
        if path.parent.is_symlink():
            raise ValueError('project Codex configuration aliases are not qualified')
        if any(scope.is_relative_to(path.parent) for scope, _ in grants):
            raise ValueError('explicit writable grants overlap protected project Codex configuration')
        for name in ('hooks.json', 'hooks/hooks.json', 'managed_config.toml', 'requirements.toml'):
            extra = path.parent / name
            extra_chain, extra_target = _chain(extra)
            if extra.exists() or extra.is_symlink():
                raise ValueError('auxiliary project Codex configuration is not qualified')
            observations.append(('project_auxiliary_absent', str(extra), str(extra_target), identities(extra_chain)))
        if path.exists() or path.is_symlink():
            raise ValueError('project Codex configuration is not qualified for extra writable paths')
        observations.append(('project_absent', str(path), identities(chain), str(target)))

    while pending:
        path, required = pending.pop(0)
        path = path.absolute()
        chain, target = protect(path)
        if path in visited:
            continue
        visited.add(path)
        if len(visited) > MAX_FILES:
            raise ValueError('Codex configuration exceeds file bound')
        try:
            info = target.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BYTES or info.st_nlink != 1:
                raise ValueError('Codex configuration must be a bounded single-link regular file')
            with target.open('rb') as stream:
                data = stream.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise ValueError('Codex configuration exceeds byte bound')
            parsed = tomllib.loads(data.decode('utf-8'))
        except FileNotFoundError:
            if required:
                raise ValueError('Codex referenced configuration is missing') from None
            data = b''
            parsed = {}
        except (OSError, UnicodeError, tomllib.TOMLDecodeError):
            raise ValueError('Codex configuration cannot be safely read or parsed') from None
        observations.append((str(path), str(target), identities(chain), hashlib.sha256(data).hexdigest()))
        inspect(parsed, path)
    return (str(home), str(repo), tuple(observations))

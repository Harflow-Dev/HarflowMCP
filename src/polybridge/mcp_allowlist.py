"""Explicit, human-only edits to harness-native global MCP approval rules."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

import tomlkit
from .clients import jsonc

_ENTRY = re.compile(r"([A-Za-z0-9_-]+)/([A-Za-z0-9_.-]+|\*)\Z")


def config_path(backend: str) -> Path:
    home = Path.home()
    if backend == "codex":
        return Path(os.environ.get("CODEX_HOME", home / ".codex")) / "config.toml"
    if backend == "claude":
        return Path(os.environ.get("CLAUDE_CONFIG_DIR", home / ".claude")) / "settings.json"
    if backend == "antigravity":
        return home / ".gemini/antigravity-cli/settings.json"
    if backend == "opencode":
        root = Path(os.environ.get("XDG_CONFIG_HOME", home / ".config")) / "opencode"
        return root / ("opencode.jsonc" if (root / "opencode.jsonc").exists() else "opencode.json")
    if backend == "vibe":
        return Path(os.environ.get("VIBE_HOME", home / ".vibe")) / "config.toml"
    raise ValueError("Unknown harness")


def _native(backend: str, entry: str) -> str:
    server, tool = entry.split("/")
    if backend == "claude":
        return f"mcp__{server}__{tool}"
    if backend == "antigravity":
        return f"mcp({server}/{tool})"
    return f"{server}_{tool}"


def _entries(backend: str, data: dict) -> list[str]:
    found = []
    if backend == "codex":
        for server, policy in data.get("mcp_servers", {}).items():
            if policy.get("default_tools_approval_mode") == "approve":
                found.append(f"{server}/*")
            for tool, rule in policy.get("tools", {}).items():
                if rule.get("approval_mode") == "approve":
                    found.append(f"{server}/{tool}")
    elif backend in ("claude", "antigravity"):
        pattern = r"mcp__([A-Za-z0-9_-]+)__([A-Za-z0-9_.-]+|\*)" if backend == "claude" else r"mcp\(([A-Za-z0-9_-]+)/([A-Za-z0-9_.-]+|\*)\)"
        for rule in data.get("permissions", {}).get("allow", []):
            if backend == "claude" and "__" not in rule[5:] and re.fullmatch(r"mcp__[A-Za-z0-9_-]+", rule):
                found.append(rule[5:] + "/*")
                continue
            match = re.fullmatch(pattern, rule)
            if match:
                found.append("/".join(match.groups()))
    elif backend == "vibe":
        for native, policy in data.get("tools", {}).items():
            if policy.get("permission") != "always": continue
            for server in sorted((s.get("name", "") for s in data.get("mcp_servers", [])), key=len, reverse=True):
                if server and native.startswith(server + "_"):
                    entry = server + "/" + native[len(server)+1:]
                    if _ENTRY.fullmatch(entry): found.append(entry)
                    break
    elif backend == "opencode":
        for native, mode in data.get("permission", {}).items():
            if mode != "allow": continue
            for server in sorted(data.get("mcp", {}), key=len, reverse=True):
                if native.startswith(server + "_"):
                    entry = server + "/" + native[len(server) + 1:]
                    if _ENTRY.fullmatch(entry): found.append(entry)
                    break
    return sorted(set(found))


def _json_root_patch(raw: str, key: str, value: object) -> str:
    """Replace one root value, keeping JSONC comments and other settings byte-for-byte."""
    if not raw.strip():
        raw = "{}\n"
    jsonc.loads(raw)
    # Tokenize while preserving offsets; braces in strings/comments never count.
    tokens = []
    i = 0
    while i < len(raw):
        if raw[i].isspace():
            i += 1
        elif raw.startswith("//", i):
            end = raw.find("\n", i); i = len(raw) if end < 0 else end
        elif raw.startswith("/*", i):
            i = raw.index("*/", i + 2) + 2
        elif raw[i] == '"':
            end = jsonc._string_end(raw, i); tokens.append((raw[i:end], i, end)); i = end
        else:
            tokens.append((raw[i], i, i + 1)); i += 1
    depth = 0
    for n, (token, start, end) in enumerate(tokens):
        if token in ("{", "["): depth += 1
        elif token in ("}", "]"): depth -= 1
        elif depth == 1 and token.startswith('"') and json.loads(token) == key and tokens[n+1][0] == ":":
            begin = tokens[n+2][1]
            nesting = 0
            for t, pos, stop in tokens[n+2:]:
                if nesting == 0 and t in (",", "}"): return raw[:begin] + json.dumps(value, indent=2) + raw[pos:]
                if t in ("{", "["): nesting += 1
                elif t in ("}", "]"): nesting -= 1
    close = tokens[-1][1]
    previous = tokens[-2][0]
    comma = "" if previous in ("{", ",") else ","
    return raw[:close] + comma + "\n" + json.dumps(key) + ": " + json.dumps(value, indent=2) + "\n" + raw[close:]


def edit(backend: str, *, allow: str | None = None, remove: str | None = None) -> dict:
    if allow is not None and remove is not None:
        raise ValueError("Choose add or remove")
    entry = allow if allow is not None else remove
    if entry is not None and not _ENTRY.fullmatch(entry):
        raise ValueError("Use server/tool or server/* with valid MCP names")
    path = config_path(backend)
    if backend == "opencode":
        binary = shutil.which("opencode")
        if not binary:
            return dict(backend=backend, supported=False, config_path=str(path), entries=[], detail="Install OpenCode before editing its native approval configuration.")
        try:
            version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=5, check=False)
            match = re.search(r"(?:^|\s)v?(\d+)\.", version.stdout.strip())
            if version.returncode != 0 or match is None:
                return dict(backend=backend, supported=False, config_path=str(path), entries=[], detail="Could not verify the installed OpenCode permissions schema.")
            if int(match.group(1)) >= 2:
                return dict(backend=backend, supported=False, config_path=str(path), entries=[], detail="This OpenCode version uses a different permissions schema; global approval editing is not supported yet.")
        except (OSError, subprocess.TimeoutExpired):
            return dict(backend=backend, supported=False, config_path=str(path), entries=[], detail="Could not verify the installed OpenCode permissions schema.")
    if entry is None:
        raw = path.read_text() if path.exists() else ""
        data = tomlkit.parse(raw) if backend in ("codex", "vibe") else jsonc.loads(raw or "{}")
        return dict(backend=backend, supported=True, config_path=str(path), entries=_entries(backend, data), detail=("Vibe requires individual server/tool entries. " if backend == "vibe" else "") + "Global rules apply to future sessions. Harness deny rules and project overrides still apply.")
    path.parent.mkdir(parents=True, exist_ok=True)
    backup = path.with_suffix(path.suffix + ".polybridge-backup")
    lock_path = path.with_suffix(path.suffix + ".polybridge.lock")
    if any(p.is_symlink() for p in (path, backup, lock_path)):
        raise ValueError("Refusing to edit symlinked harness configuration or backup")
    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        raw = path.read_text() if path.exists() else ""
        data = tomlkit.parse(raw) if backend in ("codex", "vibe") else jsonc.loads(raw or "{}")
        server, tool = entry.split("/")
        if backend == "codex":
            policy = data.setdefault("mcp_servers", {}).setdefault(server, {})
            rule = policy if tool == "*" else policy.setdefault("tools", {}).setdefault(tool, {})
            key = "default_tools_approval_mode" if tool == "*" else "approval_mode"
            if allow is not None:
                rule[key] = "approve"
            elif rule.get(key) == "approve": del rule[key]
            updated = tomlkit.dumps(data)
        elif backend == "vibe":
            if tool == "*": raise ValueError("Vibe requires individual tool names; add server/tool entries")
            if server not in [s.get("name") for s in data.get("mcp_servers", [])]: raise ValueError("Register this MCP server in the global Vibe configuration first")
            rule = data.setdefault("tools", {}).setdefault(_native(backend, entry), {})
            if allow is not None: rule["permission"] = "always"
            elif rule.get("permission") == "always": del rule["permission"]
            updated = tomlkit.dumps(data)
        elif backend in ("claude", "antigravity"):
            permissions = data.setdefault("permissions", {})
            native = _native(backend, entry)
            values = permissions.setdefault("allow", [])
            if allow is not None and native not in values: values.append(native)
            if remove is not None:
                aliases = {native}
                if backend == "claude" and tool == "*": aliases.add(f"mcp__{server}")
                permissions["allow"] = [v for v in values if v not in aliases]
            updated = _json_root_patch(raw, "permissions", permissions)
        else:
            permission = data.setdefault("permission", {})
            if not isinstance(permission, dict): raise ValueError("OpenCode permission must be an object before adding individual rules")
            native = _native(backend, entry)
            if server not in data.get("mcp", {}): raise ValueError("Register this MCP server in the global OpenCode configuration first")
            if allow is not None:
                permission.pop(native, None); permission[native] = "allow"
            elif permission.get(native) == "allow": del permission[native]
            updated = _json_root_patch(raw, "permission", permission)
        if path.exists() and path.read_text() != raw: raise ValueError("Harness configuration changed; reload and try again")
        if raw: _atomic_write(backup, raw, 0o600)
        _atomic_write(path, updated, path.stat().st_mode & 0o777 if path.exists() else 0o600)
        return dict(backend=backend, supported=True, config_path=str(path), entries=_entries(backend, data), detail="Saved global MCP approval rule. Start a new session or continue the builder to load it; project and deny rules may override it.")


def _atomic_write(path: Path, text: str, mode: int) -> None:
    fd, temporary = tempfile.mkstemp(dir=path.parent)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)

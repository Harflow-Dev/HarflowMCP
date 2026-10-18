"""Harness-owned native MCP approval formats, without filesystem mutations.

The generic editor owns validation, locking and atomic persistence. Each backend
selects its approval adapter, so adding a harness never expands a central switch.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from typing import Protocol

import tomlkit
from ..clients import jsonc

_ENTRY = re.compile(r"([A-Za-z0-9_-]+)/([A-Za-z0-9_.-]+|\*)\Z")


class MCPApprovalPolicy(Protocol):
    """Native configuration location, interpretation and approval-rule editing."""
    detail_prefix: str

    def config_path(self) -> Path: ...
    def unsupported_reason(self) -> str | None: ...
    def parse(self, raw: str) -> dict: ...
    def entries(self, data: dict) -> list[str]: ...
    def update(self, raw: str, data: dict, entry: str, *, allow: bool) -> str: ...


class ApprovalPolicy:
    detail_prefix = ""

    def unsupported_reason(self) -> str | None:
        return None


class TomlApproval(ApprovalPolicy):
    def parse(self, raw: str) -> dict:
        return tomlkit.parse(raw)


class JsonApproval(ApprovalPolicy):
    def parse(self, raw: str) -> dict:
        return jsonc.loads(raw or "{}")


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



class CodexApproval(TomlApproval):
    def config_path(self) -> Path:
        return Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "config.toml"

    def entries(self, data: dict) -> list[str]:
        found = []
        for server, policy in data.get("mcp_servers", {}).items():
            if policy.get("default_tools_approval_mode") == "approve":
                found.append(f"{server}/*")
            for tool, rule in policy.get("tools", {}).items():
                if rule.get("approval_mode") == "approve":
                    found.append(f"{server}/{tool}")
        return sorted(set(found))

    def update(self, raw: str, data: dict, entry: str, *, allow: bool) -> str:
        server, tool = entry.split("/")
        policy = data.get("mcp_servers", {}).get(server)
        if not isinstance(policy, dict) or not any(isinstance(policy.get(key), str) and policy[key].strip() for key in ("command", "url")):
            raise ValueError("Register this MCP server in the global Codex configuration first; approval rules cannot create a server without a command or URL")
        rule = policy if tool == "*" else policy.setdefault("tools", {}).setdefault(tool, {})
        key = "default_tools_approval_mode" if tool == "*" else "approval_mode"
        if allow:
            rule[key] = "approve"
        elif rule.get(key) == "approve":
            del rule[key]
        return tomlkit.dumps(data)


class PermissionListApproval(JsonApproval):
    pattern: str

    def native(self, server: str, tool: str) -> str:
        raise NotImplementedError

    def aliases(self, server: str, tool: str) -> set[str]:
        return {self.native(server, tool)}

    def bare_entry(self, rule: str) -> str | None:
        return None

    def entries(self, data: dict) -> list[str]:
        found = []
        for rule in data.get("permissions", {}).get("allow", []):
            if bare := self.bare_entry(rule):
                found.append(bare)
                continue
            if match := re.fullmatch(self.pattern, rule):
                found.append("/".join(match.groups()))
        return sorted(set(found))

    def update(self, raw: str, data: dict, entry: str, *, allow: bool) -> str:
        server, tool = entry.split("/")
        permissions = data.setdefault("permissions", {})
        native = self.native(server, tool)
        values = permissions.setdefault("allow", [])
        if allow and native not in values:
            values.append(native)
        if not allow:
            aliases = self.aliases(server, tool)
            permissions["allow"] = [v for v in values if v not in aliases]
        return _json_root_patch(raw, "permissions", permissions)


class ClaudeApproval(PermissionListApproval):
    pattern = r"mcp__([A-Za-z0-9_-]+)__([A-Za-z0-9_.-]+|\*)"

    def config_path(self) -> Path:
        return Path(os.environ.get("CLAUDE_CONFIG_DIR", Path.home() / ".claude")) / "settings.json"

    def native(self, server: str, tool: str) -> str:
        return f"mcp__{server}__{tool}"

    def aliases(self, server: str, tool: str) -> set[str]:
        result = super().aliases(server, tool)
        if tool == "*":
            result.add(f"mcp__{server}")
        return result

    def bare_entry(self, rule: str) -> str | None:
        if "__" not in rule[5:] and re.fullmatch(r"mcp__[A-Za-z0-9_-]+", rule):
            return rule[5:] + "/*"
        return None


class AntigravityApproval(PermissionListApproval):
    pattern = r"mcp\(([A-Za-z0-9_-]+)/([A-Za-z0-9_.-]+|\*)\)"

    def config_path(self) -> Path:
        return Path.home() / ".gemini/antigravity-cli/settings.json"

    def native(self, server: str, tool: str) -> str:
        return f"mcp({server}/{tool})"


class VibeApproval(TomlApproval):
    detail_prefix = "Vibe requires individual server/tool entries. "

    def config_path(self) -> Path:
        return Path(os.environ.get("VIBE_HOME", Path.home() / ".vibe")) / "config.toml"

    def entries(self, data: dict) -> list[str]:
        found = []
        for native, policy in data.get("tools", {}).items():
            if policy.get("permission") != "always":
                continue
            for server in sorted((s.get("name", "") for s in data.get("mcp_servers", [])), key=len, reverse=True):
                if server and native.startswith(server + "_"):
                    entry = server + "/" + native[len(server) + 1:]
                    if _ENTRY.fullmatch(entry):
                        found.append(entry)
                    break
        return sorted(set(found))

    def update(self, raw: str, data: dict, entry: str, *, allow: bool) -> str:
        server, tool = entry.split("/")
        if tool == "*":
            raise ValueError("Vibe requires individual tool names; add server/tool entries")
        if server not in [s.get("name") for s in data.get("mcp_servers", [])]:
            raise ValueError("Register this MCP server in the global Vibe configuration first")
        rule = data.setdefault("tools", {}).setdefault(f"{server}_{tool}", {})
        if allow:
            rule["permission"] = "always"
        elif rule.get("permission") == "always":
            del rule["permission"]
        return tomlkit.dumps(data)


class OpencodeApproval(JsonApproval):
    def config_path(self) -> Path:
        root = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "opencode"
        return root / ("opencode.jsonc" if (root / "opencode.jsonc").exists() else "opencode.json")

    def unsupported_reason(self) -> str | None:
        binary = shutil.which("opencode")
        if not binary:
            return "Install OpenCode before editing its native approval configuration."
        try:
            version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=5, check=False)
            match = re.search(r"(?:^|\s)v?(\d+)\.", version.stdout.strip())
            if version.returncode != 0 or match is None:
                return "Could not verify the installed OpenCode permissions schema."
            if int(match.group(1)) >= 2:
                return "This OpenCode version uses a different permissions schema; global approval editing is not supported yet."
        except (OSError, subprocess.TimeoutExpired):
            return "Could not verify the installed OpenCode permissions schema."
        return None

    def entries(self, data: dict) -> list[str]:
        found = []
        for native, mode in data.get("permission", {}).items():
            if mode != "allow":
                continue
            for server in sorted(data.get("mcp", {}), key=len, reverse=True):
                if native.startswith(server + "_"):
                    entry = server + "/" + native[len(server) + 1:]
                    if _ENTRY.fullmatch(entry):
                        found.append(entry)
                    break
        return sorted(set(found))

    def update(self, raw: str, data: dict, entry: str, *, allow: bool) -> str:
        server, tool = entry.split("/")
        permission = data.setdefault("permission", {})
        if not isinstance(permission, dict):
            raise ValueError("OpenCode permission must be an object before adding individual rules")
        if server not in data.get("mcp", {}):
            raise ValueError("Register this MCP server in the global OpenCode configuration first")
        native = f"{server}_{tool}"
        if allow:
            permission.pop(native, None)
            permission[native] = "allow"
        elif permission.get(native) == "allow":
            del permission[native]
        return _json_root_patch(raw, "permission", permission)

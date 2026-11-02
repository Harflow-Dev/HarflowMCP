"""Exact, callback-time Vibe command grants; never edits Vibe's policy profiles.

Native allowlists are prefixes, including entries without wildcards. Using them for
Harflow's exact grants would broaden authority and replace inherited list values.
The ACP bridge instead answers one native permission request after native policy
has already run. A dispatch grant authorizes that request; native NEVER denials
do not produce an approval request and remain denied.
"""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
from typing import Any

from ..command_grants import CommandGrantError, check_bounds
from . import claude_commands as grammar
from .base import Invocation, UnsupportedCapability

VERSION = "3.0.3"
KEY = "vibe_commands"
GRAMMAR = grammar.GRAMMAR


def validate_all(patterns, freedom: str, repo: Path | str | None) -> None:
    grammar.validate_all(check_bounds(patterns), freedom, repo)


def _digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def configure(invocation: Invocation, patterns, freedom: str, repo: Path | str | None,
              version: str | None = None) -> Invocation:
    """Freeze dispatch grants in the invocation; allocate no files or credentials.

    The backend must select qualified ACP transport, including on resume.
    This function adds no argv
    allowlists and does not itself qualify a runtime or policy configuration.
    """
    patterns = check_bounds(patterns)
    validate_all(patterns, freedom, repo)
    if version != VERSION:
        raise UnsupportedCapability("Vibe command grants require qualified Vibe 3.0.3")
    if repo is None:
        raise CommandGrantError("Vibe command grants require an explicit repository")
    payload = {"version": version, "patterns": list(patterns), "freedom": freedom,
               "repo": str(Path(repo).resolve())}
    settings = dict(invocation.native_settings or {})
    settings[KEY] = {**payload, "sha256": _digest(payload)}
    return replace(invocation, allowed_commands=patterns, allowed_commands_version=version,
                   native_settings=settings)


# Backend naming convenience; preparation is pure and safe to repeat on resume.
prepare = configure


def assert_safe(invocation: Invocation, freedom: str) -> None:
    settings = (invocation.native_settings or {}).get(KEY)
    if not isinstance(settings, dict) or set(settings) != {"version", "patterns", "freedom", "repo", "sha256"}:
        raise CommandGrantError("Vibe command grant metadata missing or malformed")
    payload = {key: value for key, value in settings.items() if key != "sha256"}
    if settings["sha256"] != _digest(payload):
        raise CommandGrantError("Vibe command grant metadata changed")
    if settings["version"] != VERSION or invocation.allowed_commands_version != VERSION:
        raise CommandGrantError("Vibe command grant version is unqualified")
    if freedom != settings["freedom"] or list(invocation.allowed_commands) != settings["patterns"]:
        raise CommandGrantError("Vibe command grant authority differs from invocation")
    if not isinstance(settings["repo"], str) or not Path(settings["repo"]).is_absolute():
        raise CommandGrantError("Vibe command grant repository is malformed")
    validate_all(invocation.allowed_commands, freedom, settings["repo"])


def _matches(pattern: str, command: str) -> bool:
    if grammar.match_mode(pattern) == "exact":
        return command == pattern
    prefix = pattern[:-2]
    return command == prefix or command.startswith(prefix + " ")


def automatic_decision(request: dict[str, Any], invocation: Invocation | dict[str, Any]) -> bool:
    """True means answer this native request once; False leaves normal handling.

    Require full original Bash input plus the native command permission scopes.
    Redirections, expansions, compound commands, extra path grants, malformed
    requests and unknown tool actions never receive a dispatch grant.
    """
    try:
        if isinstance(invocation, dict):
            # A control context may carry the payload directly or under KEY.
            settings = invocation.get(KEY, invocation)
            if not isinstance(settings, dict):
                return False
            invocation = Invocation(
                ["vibe-acp"], allowed_commands=tuple(settings["patterns"]),
                allowed_commands_version=settings["version"], native_settings={KEY: settings})
        settings = (invocation.native_settings or {})[KEY]
        assert_safe(invocation, settings["freedom"])
        action = request["action"]
        if request["native_method"] != "session/request_permission" or action["tool_name"] != "bash":
            return False
        if not isinstance(action["input"], dict) or set(action["input"]) != {"command"}:
            return False
        command = action["input"]["command"]
        if not isinstance(command, str) or "*" in command:
            return False
        # Validate the actual invocation too: prefix operands must stay inside
        # the repository and cannot introduce shell syntax or evaluator flags.
        grammar.validate(command, settings["freedom"], settings["repo"])
        permissions = request["required_permissions"]
        if not isinstance(permissions, list) or not permissions:
            return False
        if any(not isinstance(permission, dict)
               or permission.get("scope") != "command_pattern"
               or permission.get("invocation_pattern") != command
               for permission in permissions):
            return False
        return any(_matches(pattern, command) for pattern in invocation.allowed_commands)
    except (KeyError, TypeError, ValueError, UnsupportedCapability):
        return False

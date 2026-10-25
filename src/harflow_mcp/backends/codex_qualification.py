"""The one measured Codex qualification table for caller approvals through `codex app-server`.

Only a version listed here, on a listed platform, may run a task with `caller_approvals`; every other
version refuses at dispatch and again against the version the app-server reports. A row records the
measured facts the design depends on (docs/backend-cli-facts.md, "Codex app-server"), so a version bump
must re-measure each of them rather than inherit the row.
"""
from __future__ import annotations

import re
import sys
from typing import Any

from .base import UnsupportedCapability

MEASURED: dict[str, dict[str, Any]] = {
    "0.163.0": {
        "caller_approval_tools": ("commandExecution",),
        "platforms": ("darwin",),
        "freedoms": ("write_in_repo", "publish"),
        "facts": {
            # Pins win over every config layer; the thread response reports the effective values.
            "explicit_pins_override_config": True,
            "thread_response_reports_effective_policy": True,
            # Nothing reloads mid-run: rules, project config and user config are read at process start.
            "no_mid_run_config_reload": True,
            # `-c features.hooks=false` makes `hooks/list` return no hooks, and no hook fires.
            "argv_hooks_pin_reflected_by_hooks_list": True,
            # `projects."<root>".trust_level = "untrusted"` in the thread `config` map ignores project
            # config, rules and hooks and writes nothing to CODEX_HOME/config.toml.
            "trust_map_override_writes_nothing": True,
            # `decline` is accepted although `availableDecisions` never lists it.
            "decline_accepted_though_unlisted": True,
            "available_decisions_accept_and_cancel": True,
            "command_request_params_omit_optional_keys": True,
            "write_stdin_reuses_item_id": True,
            "turn_start_overrides_replace_pins": True,
            "resume_of_loaded_thread_ignores_pins": True,
            "tool_processes_do_not_hold_app_server_pipes": True,
        },
    },
}

_USER_AGENT = re.compile(r"^[^/\s]+/(\d+\.\d+\.\d+)(?:[\s(]|$)")
_CLI_VERSION = re.compile(r"(?:codex-cli\s+)?(\d+\.\d+\.\d+)")


def parse_version(observed: Any) -> str | None:
    """The version in `codex --version` output (`codex-cli 0.163.0`)."""
    match = _CLI_VERSION.fullmatch(observed.strip()) if isinstance(observed, str) else None
    return match.group(1) if match else None


def parse_user_agent(user_agent: Any) -> str | None:
    """The version in an `initialize` `userAgent` (`<client>/0.163.0 (<os>) ...`)."""
    match = _USER_AGENT.match(user_agent) if isinstance(user_agent, str) else None
    return match.group(1) if match else None


def measured(version: Any) -> dict[str, Any] | None:
    return MEASURED.get(version) if isinstance(version, str) else None


def supports(version: Any, feature: str) -> bool:
    return bool((measured(version) or {}).get(feature))


def versions() -> list[str]:
    return list(MEASURED)


def platform_supported(version: Any, platform: str | None = None) -> bool:
    return (platform or sys.platform) in (measured(version) or {}).get("platforms", ())


def qualify(backend: Any, platform: str | None = None) -> str:
    """The exact installed version, which must be measured on this platform; probed before every spawn."""
    from . import version as probe
    observed = parse_version(probe(backend))
    if observed is None or not supports(observed, "caller_approval_tools"):
        raise UnsupportedCapability(f"caller approvals require a measured Codex version ({', '.join(MEASURED)}); the observed version is not qualified")
    if not platform_supported(observed, platform):
        raise UnsupportedCapability(f"caller approvals on Codex {observed} were measured only on {', '.join(MEASURED[observed]['platforms'])}")
    return observed

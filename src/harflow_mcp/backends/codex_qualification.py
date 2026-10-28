"""Codex seed measurements and shared behavior-qualification adapter.

Seeds retain their measured platform scope. Unknown identities require an isolated behavior probe.
"""
from __future__ import annotations

import re
import sys
from typing import Any

from .base import UnsupportedCapability

MEASURED: dict[str, dict[str, Any]] = {
    "0.163.0": {
        "caller_approval_tools": ("commandExecution", "fileChange"),
        "caller_write_tools": ("fileChange",),
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
            # fileChange requests carry only ids: the paths come from the preceding `item/started`, whose changes equal the completion's.
            "file_change_request_joins_started_item": True,
            "file_change_accept_does_not_grant_root": True,
            "denial_stderr_router_lines": True,
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
    from .qualification import runtime_facts
    if not isinstance(version, str):
        return None
    return runtime_facts("codex", version) or MEASURED.get(version)


def supports(version: Any, feature: str) -> bool:
    return bool((measured(version) or {}).get(feature))


def versions() -> list[str]:
    return list(MEASURED)


def platform_supported(version: Any, platform: str | None = None) -> bool:
    return (platform or sys.platform) in (measured(version) or {}).get("platforms", ())


def feature_specs():
    from .qualification import FeatureSpec
    from .qualification_probes import codex_probe
    features = ("caller_approval_tools", "caller_write_tools")
    specs = {feature: FeatureSpec(feature, "codex-permissions-v1",
                                seeds={v: dict(row) for v, row in MEASURED.items() if row.get(feature)},
                                probe=codex_probe, timeout_seconds=180, seed_platforms=("darwin",)) for feature in features}
    specs["group_report_output"] = FeatureSpec("group_report_output", "codex-group-reports-v1", seeds={"0.165.0": {"group_report_output": True}}, platforms=("darwin",), seed_platforms=("darwin",))
    return specs


def qualify(backend: Any, platform: str | None = None) -> str:
    from .qualification import require
    if platform is not None and platform != sys.platform:
        raise UnsupportedCapability("qualification platform differs from the running platform")
    return require(backend, "caller_approval_tools").identity.version

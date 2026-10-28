"""Public, cached-only qualification requirements for launch inputs.

This module never invokes a qualifier. Identity discovery may execute --version and
hash the installed executable; feature evidence only reads seeds and receipts.
"""
from __future__ import annotations

from dataclasses import asdict
import json
from typing import Any, Sequence


def required_features(*, caller_approvals: bool = False,
                      caller_write_approvals: bool = False,
                      allowed_commands: Sequence[str] | None = None, group_report_output: bool | None = None) -> tuple[str, ...]:
    """The gated permission features requested by one validated launch candidate."""
    return tuple(name for name, enabled in (
        ("caller_approval_tools", caller_approvals),
        ("caller_write_tools", caller_write_approvals),
        ("allowed_commands", bool(allowed_commands)),
        ("group_report_output", bool(group_report_output)),
    ) if enabled)


def discover(candidates: list[tuple[str, dict[str, Any]]]) -> dict[str, Any]:
    from . import backends
    from .backends import qualification
    if len(candidates) > 4096:
        raise ValueError("Qualification requirements exceed the 4096-origin budget")
    observed: dict[str, tuple[dict | None, dict, str | None]] = {}
    requirements = []
    from .wire_contracts import CTL_FORMAT, envelope
    result = {"qualification_version": 1, "discovery_only": True, "requirements": requirements}
    # Match ctl's default ASCII-escaped JSON and trailing print newline. Empty-array
    # bytes already include brackets; each row contributes its encoding plus separators.
    encoded_bytes = len(json.dumps(envelope(CTL_FORMAT, {"result": result})).encode()) + 1

    def append(row):
        nonlocal encoded_bytes
        remaining = 1024 * 1024 - encoded_bytes - (2 if requirements else 0)
        size = 0
        for chunk in json.JSONEncoder().iterencode(row):
            size += len(chunk.encode())
            if size > remaining:
                raise ValueError("Qualification requirements exceed the 1 MiB response budget")
        encoded_bytes += size + (2 if requirements else 0)
        requirements.append(row)
    for origin, candidate in candidates:
        name = candidate["backend"]
        required = required_features(**{key: candidate.get(key) for key in
            ("caller_approvals", "caller_write_approvals", "allowed_commands", "group_report_output")})
        if not required:
            append({"id": origin, "backend": name, "required_features": [], "features": {}})
            continue
        if name not in observed:
            try:
                backend = backends.get(name)
                identity = qualification.identify(backend)
                rows = qualification.inspect(backend, identity=identity)
                observed[name] = (asdict(identity), rows, None)
            except (OSError, ValueError, backends.UnsupportedCapability) as exc:
                observed[name] = (None, {}, str(exc)[:1000])
        identity, rows, error = observed[name]
        features = {feature: rows.get(feature, {"ok": False, "source": "unsupported",
                    "reason": "backend does not declare this required feature",
                    **({"identity": identity} if identity is not None else {})}) for feature in required}
        row = {"id": origin, "backend": name, "required_features": list(required),
               "features": features}
        if identity is not None:
            row["identity"] = identity
        if error is not None:
            row["unavailable_reason"] = error
        elif any(not value["ok"] for value in features.values()):
            row["unavailable_reason"] = "One or more required features are unavailable"
        append(row)
    return result


def workflow_candidates(tree: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Keep every origin in an already validated, bounded resolved dependency tree."""
    candidates = []
    owners = {tree["root_workflow_id"]} | {
        edge["to"] for edge in tree.get("edges", [])
        if edge.get("orchestrator_mode", "child") == "child"
    }

    def append(origin, candidate):
        if len(candidates) >= 4096:
            raise ValueError("Qualification requirements exceed the 4096-origin budget")
        candidates.append((origin, candidate))

    for ident, entry in tree["workflows"].items():
        definition = entry["definition"]
        if ident in owners:
            orchestrator = definition["orchestrator"]
            for position, candidate in enumerate([orchestrator, *orchestrator.get("fallbacks", [])]):
                append(f"{ident}/orchestrator/{position}", candidate)
        for kind, nodes in (("node", definition.get("nodes", [])),
                            ("on-demand", definition.get("on_demand_nodes", []))):
            for node in nodes:
                if kind == "node" and node.get("type") != "agent":
                    continue
                agent = node["agent"]
                for position, candidate in enumerate([agent, *agent.get("fallbacks", [])]):
                    append(f"{ident}/{kind}/{node['id']}/{position}", candidate)
    return candidates

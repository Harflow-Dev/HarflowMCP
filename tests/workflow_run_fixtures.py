"""Helpers for persisting positive workflow-run fixtures through the codec."""

from __future__ import annotations

import copy
import json
from pathlib import Path

from harflow_mcp import workflow_run_documents


CORE_FIELDS = {
    "workflow_run_id", "kind", "name", "definition", "definition_hash", "prompt", "repo_path",
    "freedom", "network", "status", "instructions", "permission_policy", "revision", "sequence",
    "transitions", "created_at", "updated_at", "activations", "decisions", "tasks", "sessions",
    "pending", "joins", "attempt_grants", "retry_counts", "retry_grants", "suppressed_candidates",
    "supervisor_pid",
}


def qualified_run(run: dict, *, template: dict | None = None) -> dict:
    candidate = copy.deepcopy(run)
    if template is not None:
        for key in CORE_FIELDS - candidate.keys():
            candidate[key] = copy.deepcopy(template[key])
    return workflow_run_documents.encode(candidate)


def persist_run(path: Path, run: dict, *, template: dict | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(qualified_run(run, template=template)), encoding="utf-8")

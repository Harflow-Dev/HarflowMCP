"""Unimplemented harness adapters remain the primary fallback explanation."""
import asyncio
from dataclasses import replace

import pytest

from polybridge import store, workflows as w
from test_workflow_delegation import Registry
from test_workflow_explicit_parallel import explicit
from test_workflow_run_node_execution import workflow_policy


class ConfiguredRegistry(Registry):
    async def start(self, prompt, repo, **kwargs):
        task = await super().start(prompt, repo, **kwargs)
        record = store.read(self._log_dir, task.task_id)
        store.write(self._log_dir, replace(record, model=kwargs.get("model"), max_turns=kwargs.get("max_turns"), reasoning_effort=kwargs.get("reasoning_effort")))
        return task


@pytest.mark.parametrize("backend", ["vibe", "opencode", "antigravity"])
async def test_parallel_preview_and_runtime_report_absent_adapter(tmp_path, monkeypatch, backend):
    monkeypatch.setattr(w.backends, "is_installed", lambda _: True)
    storage = w.WorkflowStore(tmp_path)
    definition = explicit()
    definition["orchestrator"] = {"backend": backend}
    for node in definition["nodes"]:
        if node["type"] == "agent":
            node.update(role="review", agent={"backend": backend}, execution_mode="prefer_subagent", session_mode="fresh")
    saved = storage.save(definition["name"], definition)
    preview = w.preview_workflow_run(saved["name"], tmp_path, root=storage.root)
    expected = "This harness has no certified native execution adapter"
    outcomes = preview["effective_owner_contract"]["nodes"]
    assert outcomes and all(node["fallback_reason"] == expected for node in outcomes)
    assert all(node["execution_kind"] == "headless" for node in outcomes)
    assert preview["effective_owner_contract"]["contributing_nodes"] == []
    run = storage.create_run(saved, "Review", tmp_path)
    outputs = {node["id"]: {"status": "succeeded", "result": {"summary": "Reviewed", "verdict": "approved"}, "evidence": []} for node in saved["nodes"] if node["type"] == "agent"}
    registry = ConfiguredRegistry(tmp_path, policy=workflow_policy, outputs=outputs)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 10)
    observed = storage.get_run(run["workflow_run_id"])
    assert observed["status"] == "completed", observed.get("attention_reason")
    workers = [activation for activation in observed["activations"] if activation["role"] == "node"]
    assert workers and all(node["execution_kind"] == "headless" for node in workers)
    assert all(node["execution_fallback_reason"] == expected for node in workers)

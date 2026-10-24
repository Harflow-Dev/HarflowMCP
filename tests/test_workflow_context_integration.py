"""Real supervisor with deterministic fake harnesses; no model calls."""
import copy
import hashlib
import json

import pytest

from harflow_mcp import workflows as w
from test_workflow_delegation import Registry, default_decision, graph, run_flow


class ContextRegistry(Registry):
    def __init__(self, root, *, ack_mode="valid", invalid_first=False):
        super().__init__(root)
        self.ack_mode = ack_mode
        self.invalid_first = invalid_first
        self.deliveries = []
        self.bootstrap = {}
        self.section_state = {}

    def decision(self, context):
        return default_decision(context, self)

    async def start(self, prompt, repo, **kwargs):
        try:
            envelope = json.JSONDecoder().raw_decode(prompt)[0]
        except (ValueError, TypeError):
            return await super().start(prompt, repo, **kwargs)
        if "checkpoint" not in envelope or "delivery" not in envelope:
            return await super().start(prompt, repo, **kwargs)
        if "bootstrap" in envelope:
            self.bootstrap = copy.deepcopy(envelope["bootstrap"]["context"])
        self.deliveries.append(envelope)
        delivery = envelope["delivery"]
        session = delivery["acknowledgement"]["session_owner"]
        checkpoint = copy.deepcopy(envelope["checkpoint"])
        state = self.section_state.setdefault(session, {})
        if "bootstrap" in envelope:
            if delivery.get("mode") == "bootstrap" and self.deliveries[:-1]:
                state.clear()
            state["bootstrap"] = copy.deepcopy(envelope["bootstrap"]["context"])
        # Reconstruct the stable section projection exactly as a receiving
        # session would: references require an acknowledged section, while
        # append requires the exact recorded list prefix.
        for key, value in list(checkpoint.items()):
            section = key
            if key == "node_instructions" and isinstance(value, dict):
                rebuilt = {}
                for node_id, item in value.items():
                    rebuilt[node_id] = self._resolve_section(session, "node_instructions:" + node_id, item, delivery.get("sections", {}).get("node_instructions:" + node_id))
                checkpoint[key] = rebuilt
                continue
            checkpoint[key] = self._resolve_section(session, section, value, delivery.get("sections", {}).get(section))
        context = {**self.bootstrap, **state.get("bootstrap", {}), **checkpoint}
        for key, value in checkpoint.items():
            if key != "node_instructions":
                state[key] = copy.deepcopy(value)
        if isinstance(checkpoint.get("node_instructions"), dict):
            for node_id, value in checkpoint["node_instructions"].items():
                state["node_instructions:" + node_id] = copy.deepcopy(value)
        result = self.decision(context)
        if self.invalid_first and len(self.deliveries) == 1:
            result["next"][0]["continuation_id"] = "invalid"
        if self.ack_mode != "missing":
            result["context_ack"] = copy.deepcopy(envelope["delivery"]["acknowledgement"])
            if self.ack_mode == "stale":
                result["context_ack"]["revision"] += 100
        original_policy = self.policy
        self.policy = lambda context, registry: result
        try:
            task = await super().start("Context:\n" + json.dumps(context), repo, **kwargs)
            result = getattr(task, "result", {})
            actual_id = result.get("session_id") if isinstance(result, dict) else None
            if session and actual_id:
                prefix = session.rsplit(":", 1)[0]
                actual_owner = prefix + ":" + actual_id
                if actual_owner != session:
                    self.section_state[actual_owner] = self.section_state.pop(session)
            return task
        finally:
            self.policy = original_policy

    def _resolve_section(self, session, section, value, metadata=None):
        if not isinstance(value, dict) or value.get("mode") not in {"reference", "append"}:
            return value
        if section not in self.section_state.get(session, {}):
            raise AssertionError(f"dangling context section reference: {section}")
        previous = copy.deepcopy(self.section_state[session][section])
        digest = hashlib.sha256(json.dumps(previous, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        if value.get("mode") == "reference" and value.get("digest") and value["digest"] != digest:
            raise AssertionError(f"context section digest mismatch: {section}")
        if metadata and metadata.get("digest") and metadata["digest"] != digest and value.get("mode") == "reference":
            raise AssertionError(f"context section metadata mismatch: {section}")
        if value["mode"] == "reference":
            return previous
        if not isinstance(previous, list) or not isinstance(value.get("items"), list):
            raise AssertionError(f"invalid append section: {section}")
        if value.get("base_digest") and value["base_digest"] != digest:
            raise AssertionError(f"context append base mismatch: {section}")
        rebuilt = previous + copy.deepcopy(value["items"])
        if metadata and metadata.get("digest"):
            actual = hashlib.sha256(json.dumps(rebuilt, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            if metadata["digest"] != actual:
                raise AssertionError(f"context append digest mismatch: {section}")
        return rebuilt


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    return w.WorkflowStore(tmp_path)


@pytest.mark.parametrize("ack_mode", ["valid", "missing", "stale"])
async def test_supervisor_context_receipts_and_acknowledgement(storage, tmp_path, ack_mode):
    definition = graph()
    definition["context_delivery"] = "optimized_v1"
    registry = ContextRegistry(storage.root, ack_mode=ack_mode)
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "completed"
    assert len(registry.deliveries) == 2
    assert registry.deliveries[0]["delivery"]["mode"] == "bootstrap"
    modes = [item["delivery"]["mode"] for item in registry.deliveries]
    if ack_mode == "valid":
        assert modes[1:] == ["delta"]
    else:
        assert modes == ["bootstrap"] * 2
    orchestrators = [activation for activation in run["activations"] if activation["role"] == "orchestrator"]
    for activation in orchestrators:
        task = activation["tasks"][-1]
        assert task["context_delivery"]["version"] == 2
        assert task["context_delivery"]["serialized_bytes"] > 0
    assert len([activation for activation in run["activations"] if activation["role"] == "node"]) == 1


async def test_protocol_repair_receives_bootstrap(storage, tmp_path):
    definition = graph()
    definition["context_delivery"] = "optimized_v1"
    registry = ContextRegistry(storage.root, invalid_first=True)
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "completed"
    assert len(registry.deliveries) == 3
    assert registry.deliveries[1]["delivery"]["mode"] == "bootstrap"
    assert len([activation for activation in run["activations"] if activation["role"] == "node"]) == 1


async def test_on_demand_catalog_is_slim_but_specialist_receives_verbatim_instructions(storage, tmp_path):
    definition = graph()
    exact = "Return exact evidence 🙂\n\tPreserve whitespace and symbols: {raw}."
    definition.update({"context_delivery": "optimized_v1", "on_demand_nodes": [{
        "id": "research", "title": "API researcher", "instructions": exact,
        "call_condition": "Call when API behaviour needs verification.",
        "agent": {"backend": "codex"}, "freedom": "read_only", "session_mode": "fresh", "execution_mode": "headless",
    }]})

    class SpecialistRegistry(ContextRegistry):
        def __init__(self, root):
            super().__init__(root)
            self.specialist_prompts = []
            self.all_prompts = []

        def decision(self, context):
            return self.policy(context, self)

        async def start(self, prompt, repo, **kwargs):
            self.all_prompts.append((prompt, kwargs))
            if "research" in kwargs.get("title", "") or "API researcher" in kwargs.get("title", "") or "Verify API details" in prompt:
                self.specialist_prompts.append(prompt)
            return await super().start(prompt, repo, **kwargs)

    called = False
    def policy(context, registry):
        nonlocal called
        if not called and context.get("on_demand_nodes"):
            called = True
            return {"decision_id": context["decision_id"], "action": "call_on_demand",
                    "node_id": "research", "assignment": "Verify API details", "reason": "Need specialist evidence"}
        return default_decision(context, registry)

    registry = SpecialistRegistry(storage.root)
    registry.policy = policy
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "completed", run.get("attention_reason") or run.get("failure_reason")
    assert any(exact in prompt and "Verify API details" in prompt for prompt, _ in registry.all_prompts)
    offered = [item for envelope in registry.deliveries for item in envelope["checkpoint"].get("on_demand_nodes", [])]
    assert offered and all("instructions" not in item for item in offered)
    first_actions = registry.deliveries[0]["checkpoint"]["valid_actions"]
    assert "call_on_demand" in first_actions and "inspect" in first_actions
    exhausted = [envelope["checkpoint"] for envelope in registry.deliveries if envelope["checkpoint"].get("on_demand_calls_remaining") == 0]
    if exhausted:
        assert "call_on_demand" not in exhausted[-1]["valid_actions"]


def test_deterministic_benchmark_has_every_scenario():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).parents[1] / "scripts" / "benchmark-workflow-context.py"
    spec = importlib.util.spec_from_file_location("context_benchmark", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.benchmark()
    assert report == module.benchmark()
    assert report["model_calls"] == 0 and report["token_counts"] is None
    assert {row["fixture"] for row in report["fixtures"]} == {"small", "large-history", "parallel", "child", "resume", "clarification", "repair", "acknowledged-sections"}
    rows = {row["fixture"]: row for row in report["fixtures"]}
    assert rows["large-history"]["bytes_saved"] > 200000
    assert rows["parallel"]["optimized"]["budget_overflow_bytes"] == 0
    assert rows["resume"]["optimized"]["delivery_mode"] == "delta"
    assert rows["repair"]["optimized"]["delivery_mode"] == "bootstrap"


async def test_fallback_is_fresh_bootstrap(storage, tmp_path):
    from harflow_mcp.backends import UnsupportedCapability
    class FallbackRegistry(ContextRegistry):
        async def start(self, prompt, repo, **kwargs):
            if kwargs.get("model") == "unavailable-fixture":
                envelope = json.JSONDecoder().raw_decode(prompt)[0]
                self.refused_delivery = envelope["delivery"]
                raise UnsupportedCapability("Fixture positively refused before spawn")
            return await super().start(prompt, repo, **kwargs)
    definition = graph()
    definition["context_delivery"] = "optimized_v1"
    definition["orchestrator"] = {"backend": "codex", "model": "unavailable-fixture", "fallbacks": [{"backend": "codex", "model": "available-fixture"}]}
    registry = FallbackRegistry(storage.root)
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "completed"
    assert registry.refused_delivery["mode"] == "bootstrap"
    assert registry.deliveries[0]["delivery"]["mode"] == "bootstrap"
    assert registry.refused_delivery["acknowledgement"]["session_owner"] != registry.deliveries[0]["delivery"]["acknowledgement"]["session_owner"]
    attempts = [task for activation in run["activations"] if activation["role"] == "orchestrator" for task in activation["tasks"]]
    assert attempts[0]["status"] == "not_started"
    assert attempts[0]["context_delivery"]["mode"] == "bootstrap"
    assert len([activation for activation in run["activations"] if activation["role"] == "node"]) == 1


def test_saved_definitions_always_optimize_and_historical_runs_keep_legacy(storage):
    saved = storage.save("new", graph())
    assert saved["context_delivery"] == "optimized_v1"
    old_graph = graph()
    old_graph["context_delivery"] = "legacy"
    old = storage.save("old", old_graph)
    assert old["context_delivery"] == "optimized_v1"
    historical = {"definition": {**graph(), "context_delivery": "legacy"}}
    from harflow_mcp.workflow_prompt_delivery import optimized
    assert not optimized(historical)
    assert not optimized({"definition": graph()})
    raw = graph()
    updated = storage.save("old", raw, old["revision"])
    assert updated["context_delivery"] == "optimized_v1"
    raw["context_delivery"] = "optimized_v1"
    upgraded = storage.save("old", raw, updated["revision"])
    assert upgraded["context_delivery"] == "optimized_v1"
    with pytest.raises(w.WorkflowError, match="context_delivery"):
        w.validate_definition({**graph(), "context_delivery": "unknown_v2"})


@pytest.mark.parametrize("value", [[], {}, ["legacy"], {"mode": "legacy"}, None, True, 1, "unknown_v2"])
def test_invalid_context_delivery_is_clean_validation_error(storage, value):
    definition = {**graph(), "context_delivery": value}
    with pytest.raises(w.WorkflowError, match="context_delivery"):
        w.validate_definition(definition)
    with pytest.raises(w.WorkflowError, match="context_delivery"):
        storage.save("invalid", definition)
    assert not (storage.definitions / "invalid.json").exists()


async def test_supervisor_restart_bootstraps_retained_session(storage, tmp_path):
    import asyncio
    class QuestionRegistry(ContextRegistry):
        ask_once = True
        def decision(self, context):
            if self.ask_once:
                self.ask_once = False
                return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Need a clarification", "question": "Continue?"}
            return super().decision(context)
    definition = {**graph(), "context_delivery": "optimized_v1"}
    registry = QuestionRegistry(storage.root)
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "needs_input"
    assert any(task.get("context_acknowledged") for activation in run["activations"] for task in activation["tasks"])
    storage.control(run["workflow_run_id"], "resume", instructions="Continue", decision_id=run["decisions"][-1]["decision_id"])
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert registry.deliveries[1]["delivery"]["mode"] == "bootstrap"


async def test_oversized_worker_evidence_is_authorized_before_spawn(storage, tmp_path):
    from harflow_mcp.workflow_inspection import assigned_input_page
    definition = graph()
    definition["context_delivery"] = "optimized_v1"
    definition["nodes"].insert(2, {"id": "consume", "type": "agent", "instructions": "Consume complete predecessor evidence", "execution_mode": "headless", "agent": {"backend": "codex"}})
    definition["nodes"][1]["execution_mode"] = "headless"
    definition["connections"] = [{"id": "begin", "source": "start", "target": "work"}, {"id": "consume", "source": "work", "target": "consume"}, {"id": "finish", "source": "consume", "target": "end"}]
    evidence = "雪🙂 observed evidence " * 6000
    class EvidenceRegistry(ContextRegistry):
        async def start(self, prompt, repo, **kwargs):
            if kwargs.get("title", "").endswith(" · consume"):
                run = next(run for run in storage.list_runs() if run["status"] == "running")
                worker = next(a for a in run["activations"] if a["node_id"] == "consume" and a["role"] == "node")
                assert worker["authorized_input_refs"]
                reference = worker["authorized_input_refs"][0]
                managed = ({"role": "node", "activation_id": worker["id"]}, run)
                chunks, cursor = [], None
                while True:
                    page = assigned_input_page(managed, storage.root, run["workflow_run_id"], reference["execution_id"], cursor=cursor)
                    chunks.append(page["chunk"])
                    cursor = page["next_cursor"]
                    if cursor is None:
                        break
                assert evidence in "".join(chunks)
                assert "workflow-assigned-input" in prompt
                assert evidence not in prompt
                assert len(prompt.encode()) <= 65536
                self.verified_worker = True
            return await super().start(prompt, repo, **kwargs)
    registry = EvidenceRegistry(storage.root)
    registry.outputs = {"work": {"status": "succeeded", "result": {"summary": evidence}, "evidence": ["observed"]}}
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "completed"
    assert registry.verified_worker


def test_linked_child_large_evidence_has_separate_authorized_manifest(storage, tmp_path):
    from harflow_mcp.workflow_delegation import result_inputs, worker_prompt
    from harflow_mcp.workflow_inspection import assigned_input_page
    from harflow_mcp.workflow_prompt_delivery import render_worker_inputs, worker_input_refs
    parent = storage.create_run(w.validate_definition(graph()), "Parent", tmp_path)
    child = storage.create_run(w.validate_definition(graph()), "Child", tmp_path)
    evidence = "雪🙂 child evidence " * 10000
    source = {"id": "invoke", "role": "node", "node_id": "work", "status": "completed", "created_at": 0.0, "tasks": [], "raw_output": "Child completed", "invocation": {"child_workflow_run_id": child["workflow_run_id"]}, "node_result": {"status": "succeeded", "result": {"child_outcome": {"status": "completed", "final_result_refs": [{"workflow_run_id": child["workflow_run_id"], "execution_id": "leaf"}]}}, "evidence": []}}
    leaf = {"id": "leaf", "role": "node", "node_id": "work", "status": "completed", "created_at": 0.0, "tasks": [], "node_result": {"status": "succeeded", "result": {"summary": evidence}, "evidence": ["observed"]}, "raw_output": evidence}
    storage.update_run(child["workflow_run_id"], lambda r: r.update(workflow_id="child-definition", parent_link={"workflow_run_id": parent["workflow_run_id"], "execution_id": "invoke", "node_id": "work"}, activations=[leaf]), "fixture_child")
    storage.update_run(parent["workflow_run_id"], lambda r: r.update(workflow_id="parent-definition", dependency_tree={"edges": [{"from": "parent-definition", "to": "child-definition", "node_id": "work"}]}, activations=[source]), "fixture_parent")
    parent = storage.get_run(parent["workflow_run_id"])
    token = {"input_result_refs": ["invoke"], "assignment_prompt": "Preserve constraints"}
    manifests = worker_input_refs(parent, storage.root, token)
    assert {(ref["workflow_run_id"], ref["execution_id"]) for ref in manifests} == {(parent["workflow_run_id"], "invoke"), (child["workflow_run_id"], "leaf")}
    worker = {"id": "consumer", "role": "node", "node_id": "work", "status": "running", "tasks": [], "token": token, "authorized_input_refs": manifests}
    parent["activations"].append(worker)
    inputs = result_inputs(parent, ["invoke"], preview=False, root=storage.root)
    node = next(n for n in parent['definition']['nodes'] if n['id'] == 'work')
    prompt = worker_prompt(parent, node, token, root=storage.root) + "\nReturn final result"
    rendered, accounting = render_worker_inputs(prompt, parent, node, worker, root=storage.root)
    projected = json.JSONDecoder().raw_decode(rendered.split("\nInput results:\n", 1)[1])[0]
    assert projected[0]["child_outcome"] == source["node_result"]["result"]["child_outcome"]
    child_ref = projected[0]["child_retrieval"][0]
    assert child_ref["workflow_run_id"] == child["workflow_run_id"]
    assert child_ref["execution_id"] == "leaf"
    assert evidence not in rendered
    assert accounting["serialized_bytes"] < 65536
    managed = ({"role": "node", "activation_id": "consumer"}, parent)
    chunks, cursor = [], None
    while True:
        page = assigned_input_page(managed, storage.root, parent["workflow_run_id"], "leaf", source_run_id=child["workflow_run_id"], cursor=cursor, limit=16000)
        chunks.append(page["chunk"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    recovered = json.loads("".join(chunks))
    assert recovered["node_result"] == leaf["node_result"]
    assert recovered["raw_output"] == evidence
    assert page["content_sha256"] == child_ref["content_sha256"]


async def test_exhausted_orchestrator_inspections_preserve_oversized_evidence(storage, tmp_path):
    definition = graph()
    definition["context_delivery"] = "optimized_v1"
    definition["max_inspections"] = 1
    evidence = "observed evidence 雪🙂 " * 6000
    registry = ContextRegistry(storage.root)
    registry.outputs = {"work": {"status": "succeeded", "result": {"summary": evidence}, "evidence": ["observed"]}}
    # Force the production checkpoint to reflect already exhausted inspection authority.
    from harflow_mcp import workflow_delegation as delegation
    original = delegation.decision_context
    def exhausted(*args, **kwargs):
        context = original(*args, **kwargs)
        context["inspections_remaining"] = 0
        if context["input_results"]:
            context["input_results"] = delegation.result_inputs(args[0], [item["result_ref"] for item in context["input_results"]], preview=False, root=storage.root)
        return context
    from unittest.mock import patch
    with patch.object(delegation, "decision_context", exhausted):
        run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run["status"] == "completed"
    routed = next(item for item in registry.deliveries if item["checkpoint"]["input_results"])
    assert routed["checkpoint"]["inspections_remaining"] == 0
    input_result = routed["checkpoint"]["input_results"][0]
    # Exhaustion cannot replace complete evidence with an unusable inspect manifest.
    assert "retrieval" not in input_result
    assert input_result["result_ref"]
    assert input_result["node_result"]["result"]["summary"] == evidence


async def test_whole_workflow_context_benchmark(storage, tmp_path):
    """Compare every dispatched turn; neither registry contacts a model."""
    definition = graph()
    definition["nodes"].insert(2, {"id": "verify", "type": "agent", "instructions": "Verify predecessor evidence", "execution_mode": "headless", "agent": {"backend": "codex"}})
    definition["nodes"][1]["execution_mode"] = "headless"
    definition["connections"] = [{"id": "begin", "source": "start", "target": "work"}, {"id": "verify", "source": "work", "target": "verify"}, {"id": "finish", "source": "verify", "target": "end"}]
    def assignment(context, registry):
        decision = default_decision(context, registry)
        for entry in decision.get("next", []):
            if "prompt" in entry:
                entry["prompt"] += " Preserve the assigned compatibility constraint." * 400
        return decision
    class BenchmarkRegistry(ContextRegistry):
        def decision(self, context):
            return assignment(context, self)
    report = {"model_calls": 0, "measurement": "complete workflow injected context", "usage": None, "cost_usd": None, "runs": []}
    for mode in ("legacy", "optimized_v1"):
        current = copy.deepcopy(definition)
        current["context_delivery"] = mode
        registry = Registry(storage.root, assignment) if mode == "legacy" else BenchmarkRegistry(storage.root)
        run, registry = await run_flow(storage, tmp_path, current, registry, guided=True)
        assert run["status"] == "completed"
        groups = {}
        tasks = []
        for activation in run["activations"]:
            for task in activation["tasks"]:
                delivery = task["context_delivery"]
                key = "/".join((activation["role"], delivery["session_mode"], delivery["classification"]))
                group = groups.setdefault(key, {"turns": 0, "bytes": 0})
                group["turns"] += 1
                group["bytes"] += delivery["total_bytes"]
                tasks.append(task)
        result = {"mode": mode, "status": run["status"], "turns": len(tasks), "worker_executions": sum(a["role"] == "node" for a in run["activations"]), "repair_turns": sum(t["context_delivery"]["classification"] == "protocol_repair" for t in tasks), "inspection_turns": sum(t["context_delivery"]["classification"] == "inspection" for t in tasks), "injected_bytes": sum(t["context_delivery"]["total_bytes"] for t in tasks), "by_role_session_classification": groups, "reported_usage": [t.get("prompt_usage", {}).get("usage") for t in tasks], "reported_cost_usd": [t.get("prompt_usage", {}).get("cost_usd") for t in tasks]}
        assert all(value is None for value in result["reported_usage"] + result["reported_cost_usd"])
        report["runs"].append(result)
    legacy, optimized = report["runs"]
    assert legacy["turns"] == optimized["turns"]
    assert legacy["worker_executions"] == optimized["worker_executions"] == 2
    assert legacy["repair_turns"] == optimized["repair_turns"] == 0
    assert optimized["injected_bytes"] < legacy["injected_bytes"]
    report["bytes_saved"] = legacy["injected_bytes"] - optimized["injected_bytes"]
    print("WORKFLOW_CONTEXT_BENCHMARK " + json.dumps(report, sort_keys=True))


async def test_inspection_turn_has_persisted_classification(storage, tmp_path):
    class InspectionRegistry(ContextRegistry):
        inspected = False
        def decision(self, context):
            if context["input_results"] and not self.inspected:
                self.inspected = True
                return {"decision_id": context["decision_id"], "action": "inspect", "reason": "Verify complete evidence", "requests": [{"execution_id": context["input_results"][0]["execution_id"], "view": "result", "limit": 16000}]}
            return super().decision(context)
    registry = InspectionRegistry(storage.root)
    run, registry = await run_flow(storage, tmp_path, {**graph(), "context_delivery": "optimized_v1"}, registry, guided=True)
    assert run["status"] == "completed"
    inspection_tasks = [task for activation in run["activations"] for task in activation["tasks"] if task.get("context_delivery", {}).get("classification") == "inspection"]
    assert len(inspection_tasks) == 1
    assert inspection_tasks[0]["context_delivery"]["role"] == "orchestrator"
    assert any(item["checkpoint"]["inspection_results"] for item in registry.deliveries)


def test_issued_inspection_pages_fit_escaped_response_and_remaining_budget():
    from harflow_mcp.workflow_inspection import result_page
    from harflow_mcp.workflow_prompt_delivery import orchestrator_input_manifests
    text = '"\\' * 40000
    run = {'workflow_run_id': 'escaped-run', 'execution_contract': 'delegation',
           'activations': [{'id': 'source', 'role': 'node', 'node_id': 'work', 'status': 'completed',
                            'tasks': [], 'node_result': {'status': 'succeeded', 'result': {'summary': text}, 'evidence': []},
                            'raw_output': ''}]}
    context = {'input_results': [{'result_ref': 'source'}], 'inspections_remaining': 20}
    manifest = orchestrator_input_manifests(run, context)['source']
    request = manifest['request']
    chunks, cursor = [], None
    while True:
        page = result_page(run, 'source', cursor=cursor, limit=request['limit'])
        assert len(json.dumps([page], ensure_ascii=False)) <= 32768
        assert page['content_sha256'] == manifest['content_sha256']
        chunks.append(page['chunk'])
        cursor = page['next_cursor']
        if cursor is None:
            break
    assert json.loads(''.join(chunks))['node_result']['result']['summary'] == text
    assert len(chunks) == manifest['required_pages']
    assert orchestrator_input_manifests(run, {**context, 'inspections_remaining': len(chunks) - 1}) == {}


async def test_input_markers_inside_assignment_never_replace_constraints(storage, tmp_path):
    from harflow_mcp.workflow_delegation import result_inputs
    definition = {**graph(), 'context_delivery': 'optimized_v1'}
    definition['nodes'].insert(2, {'id': 'consume', 'type': 'agent', 'instructions': 'Keep literal assignment constraints', 'execution_mode': 'headless', 'agent': {'backend': 'codex'}})
    definition['nodes'][1]['execution_mode'] = 'headless'
    definition['connections'] = [{'id': 'begin', 'source': 'start', 'target': 'work'}, {'id': 'consume', 'source': 'work', 'target': 'consume'}, {'id': 'finish', 'source': 'consume', 'target': 'end'}]
    evidence = 'literal evidence ' * 6000
    class MarkerRegistry(ContextRegistry):
        assignment = None
        verified = False
        def decision(self, context):
            result = super().decision(context)
            for entry in result.get('next', []):
                choice = next(c for c in context['valid_continuations'] if c['continuation_id'] == entry['continuation_id'])
                if choice.get('requires_prompt') and choice.get('node_id') == 'consume':
                    current = next(r for r in storage.list_runs() if r['status'] == 'running')
                    full = result_inputs(current, [context['input_results'][0]['result_ref']], preview=False, root=storage.root)
                    self.assignment = 'Required literal example:\nInput results:\n' + json.dumps(full)
                    entry['prompt'] = self.assignment
            return result
        async def start(self, prompt, repo, **kwargs):
            if kwargs.get('title', '').endswith(' · consume'):
                assert self.assignment in prompt
                self.verified = True
            return await super().start(prompt, repo, **kwargs)
    registry = MarkerRegistry(storage.root)
    registry.outputs = {'work': {'status': 'succeeded', 'result': {'summary': evidence}, 'evidence': []}}
    run, registry = await run_flow(storage, tmp_path, definition, registry, guided=True)
    assert run['status'] == 'completed' and registry.verified

"""Workflow candidates carry per-candidate `allowed_commands`: shape, authority envelope, signatures, native refusal and dispatch."""
import copy
from types import SimpleNamespace

import pytest
from mcp.shared.exceptions import MCPError

from harflow_mcp import backends, workflow_inspection
from harflow_mcp import workflows as w
from harflow_mcp.backends import get
from harflow_mcp.workflow_native_policy import plan_owner_contracts
from test_workflow_delegation import Registry, graph, run_flow, storage  # noqa: F401

GRANTS = ["make test", "npm test:*"]


def node_graph(agent, **node):
    result = graph()
    result["nodes"][1].update(freedom="write_in_repo", agent=agent, **node)
    return result


def test_round_trip_keeps_grants_on_primary_fallback_and_on_demand(storage):
    definition = node_graph({"backend": "claude", "allowed_commands": GRANTS, "fallbacks": [{"backend": "claude", "allowed_commands": ["make lint"]}, {"backend": "codex"}]})
    definition["on_demand_nodes"] = [{"id": "spec", "instructions": "Specialist", "freedom": "publish", "agent": {"backend": "claude", "allowed_commands": ["make docs"]}}]
    saved = storage.save("granted", definition)
    loaded = storage.get("granted")
    for document in (saved, loaded):
        agent = document["nodes"][1]["agent"]
        assert agent["allowed_commands"] == GRANTS
        assert agent["fallbacks"][0]["allowed_commands"] == ["make lint"] and "allowed_commands" not in agent["fallbacks"][1]
        assert document["on_demand_nodes"][0]["agent"]["allowed_commands"] == ["make docs"]


def test_empty_grants_are_dropped_and_legacy_definitions_are_unchanged(storage):
    assert "allowed_commands" not in w._candidate({"backend": "claude", "allowed_commands": []})
    legacy = storage.save("legacy", node_graph({"backend": "claude"}))
    assert "allowed_commands" not in legacy["nodes"][1]["agent"]


@pytest.mark.parametrize("agent, freedom, match", [
    ({"backend": "claude", "allowed_commands": "make test"}, "write_in_repo", "list of strings"),
    ({"backend": "claude", "allowed_commands": ["bash -c x"]}, "write_in_repo", "refused"),
    ({"backend": "claude", "allowed_commands": GRANTS}, "read_only", "not read_only"),
    ({"backend": "codex", "allowed_commands": GRANTS}, "write_in_repo", "not qualified"),
    ({"backend": "claude", "fallbacks": [{"backend": "codex", "allowed_commands": GRANTS}]}, "write_in_repo", "not qualified"),
])
def test_save_validates_each_candidate_for_its_backend_and_freedom(storage, agent, freedom, match):
    definition = node_graph(agent)
    definition["nodes"][1]["freedom"] = freedom
    with pytest.raises(w.WorkflowError, match=match):
        storage.save("bad", definition)


def test_orchestrators_and_builders_do_not_take_grants(storage):
    definition = node_graph({"backend": "claude"})
    definition["orchestrator"] = {"backend": "claude", "allowed_commands": GRANTS}
    with pytest.raises(w.WorkflowError, match="agent nodes"):
        storage.save("orch", definition)
    with pytest.raises(w.WorkflowError, match="agent nodes"):
        w._candidate({"backend": "claude", "allowed_commands": GRANTS}, False)


def test_launch_checks_paths_against_the_repository(storage, tmp_path):
    definition = node_graph({"backend": "claude", "allowed_commands": [f"make -C {tmp_path} test"]})
    storage.save("inside", definition)
    node = storage.get("inside")["nodes"][1]
    w.check_node_grants(node, "work", "write_in_repo", tmp_path)
    with pytest.raises(w.WorkflowError, match="outside the repository"):
        w.check_node_grants(node, "work", "write_in_repo", tmp_path / "other")


# --- authority envelope ---------------------------------------------------------------------------

def record(*patterns, freedom="write_in_repo"):
    backend = get("claude")
    enforcement = backend.allowed_commands_enforcement(backend.enforcement(freedom), tuple(patterns)) if patterns else backend.enforcement(freedom)
    return SimpleNamespace(freedom=freedom, network=None, enforcement=enforcement.as_dict(), backend="claude", repo_path="/r", caller_write_approvals=False)


def configs(**agent):
    return [({"backend": "claude", **agent}, "write_in_repo", None)]


def test_agent_caller_cannot_save_grants_beyond_its_own():
    with pytest.raises(backends.NestedDispatchRefused) as raised:
        workflow_inspection._check_configs(record(), configs(allowed_commands=GRANTS))
    assert raised.value.rule == "extra_allowed_commands"
    with pytest.raises(backends.NestedDispatchRefused):
        workflow_inspection._check_configs(record("make test"), configs(allowed_commands=GRANTS))
    with pytest.raises(backends.NestedDispatchRefused):
        workflow_inspection._check_configs(record(), configs(fallbacks=[{"backend": "claude", "allowed_commands": ["make test"]}]))


def test_agent_caller_may_save_a_subset_and_an_unrestricted_caller_any_grants():
    workflow_inspection._check_configs(record(*GRANTS, "make lint"), configs(allowed_commands=GRANTS))
    workflow_inspection._check_configs(record("make test", freedom="write_in_repo"), configs(allowed_commands=["make test"]))
    workflow_inspection._check_configs(record(freedom="unrestricted"), [({"backend": "claude", "allowed_commands": GRANTS}, "write_in_repo", None)])
    workflow_inspection._check_configs(record(), configs())


# --- signature and sessions -----------------------------------------------------------------------

def test_candidate_signature_changes_only_with_non_empty_grants():
    legacy = {"backend": "claude", "model": None, "reasoning_effort": None, "max_turns": None}
    frozen = '{"backend": "claude", "max_turns": null, "model": null, "reasoning_effort": null}'
    assert w._candidate_key(legacy) == frozen
    assert w._candidate_key({**legacy, "allowed_commands": []}) == frozen
    granted = w._candidate_key({**legacy, "allowed_commands": GRANTS})
    assert granted != frozen and granted == w._candidate_key({**legacy, "allowed_commands": list(GRANTS)})
    assert granted != w._candidate_key({**legacy, "allowed_commands": ["make test"]})


def test_resume_explanation_flags_changed_grants_but_not_legacy_nodes():
    from harflow_mcp import workflow_execution_policy as policy
    source = {"id": "a1", "role": "node", "node_id": "prev", "tasks": [{"candidate": {"backend": "claude"}, "freedom": "write_in_repo", "network": None, "repo_path": "/r"}]}
    node = {"id": "work", "type": "agent", "agent": {"backend": "claude"}}
    run = {"activations": [source], "definition": {"connections": [], "nodes": [{"id": "prev", "type": "agent"}]}, "network": None, "repo_path": "/r", "freedom": "write_in_repo"}
    token = {"input_result_refs": ["a1"]}
    assert "allowed_commands" not in policy.previous_session_reason(run, node, token)
    node["agent"]["allowed_commands"] = GRANTS
    assert "allowed_commands differs" in policy.previous_session_reason(run, node, token)


# --- native refusal -------------------------------------------------------------------------------

def test_native_execution_refuses_a_candidate_with_grants(monkeypatch):
    class Adapter:
        def eligible(self, parent, candidate, settings):
            return None
    monkeypatch.setattr("harflow_mcp.backends.native.adapter", lambda backend: Adapter())
    monkeypatch.setattr(w.backends, "version", lambda _: "certified")
    definition = node_graph({"backend": "claude"}, execution_mode="prefer_subagent")
    definition["orchestrator"] = {"backend": "claude"}
    plain = w.validate_definition(copy.deepcopy(definition))
    granted = copy.deepcopy(definition)
    granted["nodes"][1]["agent"]["allowed_commands"] = GRANTS
    granted = w.validate_definition(granted)

    def outcome(value):
        plan = plan_owner_contracts(value, freedom="unrestricted", permission_policy="saved_node")
        return next(iter(next(iter(plan["owners"].values()))["candidates"].values()))["nodes"][0]

    assert outcome(plain)["execution_kind"] == "native_subagent"
    refused = outcome(granted)
    assert refused["execution_kind"] == "headless" and "allowed_commands" in refused["fallback_reason"]


# --- dispatch -------------------------------------------------------------------------------------

async def test_dispatch_passes_grants_and_a_fallback_uses_its_own(storage, tmp_path):
    definition = node_graph({"backend": "claude", "allowed_commands": GRANTS, "fallbacks": [{"backend": "claude", "allowed_commands": ["make lint"]}]})
    attempts = []

    def output(prompt, kwargs):
        attempts.append(kwargs.get("allowed_commands"))
        if len(attempts) == 1:
            return {"status": "failed", "summary": "", "stderr_tail": ["API Error: 503 Service unavailable"], "backend": "claude"}
        return {"status": "succeeded", "result": {"summary": "Done"}, "evidence": []}

    run, _ = await run_flow(storage, tmp_path, definition, Registry(storage.root, outputs={"work": output}))
    assert run["status"] == "completed"
    assert attempts == [GRANTS, ["make lint"]]
    activation = next(a for a in run["activations"] if a["role"] == "node")
    assert [t["candidate"].get("allowed_commands") for t in activation["tasks"]] == [GRANTS, ["make lint"]]


async def test_dispatch_omits_the_field_for_a_legacy_candidate(storage, tmp_path):
    seen = []
    run, registry = await run_flow(storage, tmp_path, node_graph({"backend": "claude"}), Registry(storage.root, outputs={"work": lambda p, k: seen.append("allowed_commands" in k) or {"status": "succeeded", "result": {"summary": "Done"}, "evidence": []}}))
    assert run["status"] == "completed" and seen == [False]


# --- server.py start/build lineage pre-checks -----------------------------------------------------

@pytest.fixture
def agent_caller_server(tmp_path, monkeypatch):
    """`_workflow_call` as an agent caller holding `held` grants; the pre-check loops reach the real `_resolve_lineage`."""
    from harflow_mcp import lineage, server, state_ownership, workflow_hooks
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.tasks import TaskRegistry
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        (root / "tasks").mkdir()
    holder = SimpleNamespace(held=(), write=False)

    def caller():
        from harflow_mcp import store
        backend = get("claude")
        enforcement = backend.allowed_commands_enforcement(backend.enforcement("write_in_repo"), holder.held) if holder.held else backend.enforcement("write_in_repo")
        rec = store.TaskRecord(task_id="parent", backend="claude", session_id="s", repo_path=str(tmp_path), started_at="2026-10-25T00:00:00+00:00", prompt="p", status="running",
                               freedom="write_in_repo", enforcement=enforcement.as_dict(), max_depth=3, depth=0, caller_write_approvals=holder.write)
        return lineage.Caller(rec, "test")

    async def verified():
        return caller()

    async def noop(*_a, **_k):
        return None

    async def repo(value):
        return tmp_path

    shell = SimpleNamespace(log_dir=root / "tasks", _log_dir=root / "tasks")
    shell._resolve_lineage = lambda c, **kw: TaskRegistry._resolve_lineage(shell, c, **kw)
    monkeypatch.setattr(server, "_reg", lambda: shell)
    monkeypatch.setattr(server, "_verified_workflow_caller", verified)
    monkeypatch.setattr(server, "_guard_mutation", noop)
    monkeypatch.setattr(server, "_guard_launch_layout", lambda: None)
    monkeypatch.setattr(server, "_validate_repo_path", repo)
    monkeypatch.setattr(workflow_hooks, "refuse_managed", lambda *_a: None)
    monkeypatch.setattr("harflow_mcp.caller_policy.signals", lambda: (None, 0))
    monkeypatch.setattr(w.backends, "is_installed", lambda _b: True)
    monkeypatch.setattr(w, "_launch", lambda *_a: None)
    holder.server, holder.store, holder.repo = server, w.WorkflowStore(root), tmp_path
    return holder


def saved(store, name, **extra):
    definition = node_graph({"backend": "claude"})
    definition["orchestrator"] = {"backend": "claude"}
    definition.update(extra)
    store.save(name, definition)


def on_demand(**agent):
    return [{"id": "spec", "instructions": "Specialist", "freedom": "write_in_repo", "agent": {"backend": "claude", **agent}}]


async def start(holder, name):
    return await holder.server._workflow_call("start", name=name, prompt="Go", repo_path=str(holder.repo))


async def test_start_refuses_on_demand_and_node_grants_beyond_the_agent_callers(agent_caller_server):
    holder = agent_caller_server
    saved(holder.store, "ondemand", on_demand_nodes=on_demand(allowed_commands=GRANTS))
    with pytest.raises(MCPError) as raised:
        await start(holder, "ondemand")
    assert "extra_allowed_commands" in str(raised.value)
    node = node_graph({"backend": "claude", "allowed_commands": GRANTS})
    node["orchestrator"] = {"backend": "claude"}
    holder.store.save("node", node)
    with pytest.raises(MCPError, match='nested dispatch refused'):
        await start(holder, "node")
    saved(holder.store, "fallback", on_demand_nodes=on_demand(fallbacks=[{"backend": "claude", "allowed_commands": ["make test"]}]))
    with pytest.raises(MCPError, match='nested dispatch refused'):
        await start(holder, "fallback")


async def test_start_refuses_on_demand_write_approvals_the_caller_lacks(agent_caller_server, monkeypatch):
    import harflow_mcp.backends as backends
    monkeypatch.setattr(backends, "version", lambda backend: "2.1.310 (Claude Code)")
    holder = agent_caller_server
    saved(holder.store, "writes", on_demand_nodes=on_demand(caller_approvals=True, caller_write_approvals=True))
    with pytest.raises(MCPError) as raised:
        await start(holder, "writes")
    assert "caller_write_approvals" in str(raised.value)


async def test_start_allows_grants_the_caller_holds(agent_caller_server):
    holder = agent_caller_server
    holder.held = tuple(GRANTS)
    saved(holder.store, "held", on_demand_nodes=on_demand(allowed_commands=["make test"]))
    result = await start(holder, "held")
    assert result["workflow_run_id"]


async def test_build_precheck_refuses_write_approvals_the_caller_lacks(agent_caller_server):
    holder = agent_caller_server
    with pytest.raises(MCPError) as raised:
        await holder.server._workflow_call("build", name="b", prompt="Create", repo_path=str(holder.repo), agent={"backend": "claude", "caller_approvals": True, "caller_write_approvals": True})
    assert "caller_write_approvals" in str(raised.value)

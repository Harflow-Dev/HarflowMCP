"""No-model coverage for explicit usage-limit recovery decisions."""
import copy

import pytest

from polybridge import workflows as w
from polybridge import workflow_delegation as d
from test_workflow_delegation import Registry, graph, run_flow, storage, default_decision


LIMIT = {"status": "failed", "summary": "Partial progress retained", "failure_diagnostic": {"category": "usage_limit", "reason": "Provider usage exhausted", "source": "stream:error", "reset_at": "2026-10-10T00:00:00Z"}}


class LimitedRegistry(Registry):
    def __init__(self, root, policy=None, *, limited_role="worker", unknown=False):
        super().__init__(root, policy)
        self.limited_role = limited_role
        self.unknown = unknown

    async def start(self, prompt, repo, **kwargs):
        task = await super().start(prompt, repo, **kwargs)
        worker = "Assignment:\n" in prompt
        if worker == (self.limited_role == "worker") and kwargs.get("model") != "fallback-fixture":
            task.result.update(copy.deepcopy(LIMIT))
            if self.unknown:
                task.result["outcome_unknown"] = True
        return task


def definition():
    value = graph()
    value["nodes"][1]["agent"]["fallbacks"] = [{"backend": "codex", "model": "fallback-fixture"}]
    value["orchestrator"]["fallbacks"] = [{"backend": "codex", "model": "fallback-fixture"}]
    return value


async def test_worker_limit_never_automatically_dispatches_fallback(storage, tmp_path):
    def policy(context, registry):
        if context["current_stage"]["node_id"] == "work":
            return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Decide quota recovery", "question": "Choose a fallback?"}
        return default_decision(context, registry)
    run, registry = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, policy), guided=True)
    assert run["status"] == "needs_input"
    workers = [a for a in run["activations"] if a["role"] == "node"]
    assert len(workers) == 1 and len(workers[0]["tasks"]) == 1
    result = workers[0]["node_result"]
    assert result["result"]["failure_kind"] == "usage_limit"
    assert result["result"]["failure_diagnostic"]["reset_at"] == LIMIT["failure_diagnostic"]["reset_at"]
    choices = registry.contexts[-1]["valid_continuations"]
    retry = next(c for c in choices if c["kind"] == "retry_execution")
    assert len(retry["available_candidates"]) == 2


async def test_explicit_worker_candidate_selection_dispatches_fresh_once(storage, tmp_path):
    def policy(context, registry):
        retry = next((c for c in context["valid_continuations"] if c.get("usage_limit_recovery")), None)
        if retry:
            candidate = next(c for c in retry["available_candidates"] if c["candidate"].get("model") == "fallback-fixture")
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Choose configured fallback", "next": [{"continuation_id": retry["continuation_id"], "prompt": "Finish retained work", "session_mode": "fresh", "candidate_id": candidate["candidate_id"]}]}
        return default_decision(context, registry)
    run, registry = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, policy), guided=True)
    assert run["status"] == "completed"
    workers = [a for a in run["activations"] if a["role"] == "node"]
    assert len(workers) == 2
    assert workers[1]["tasks"][0]["candidate"]["model"] == "fallback-fixture"
    assert workers[1]["tasks"][0]["session_mode"] == "fresh"
    assert len(workers[0]["tasks"]) == 1


async def test_omitted_candidate_is_rejected_without_replacement(storage, tmp_path):
    def policy(context, registry):
        retry = next((c for c in context["valid_continuations"] if c.get("usage_limit_recovery")), None)
        if retry:
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Retry", "next": [{"continuation_id": retry["continuation_id"], "prompt": "Finish", "session_mode": "fresh"}]}
        return default_decision(context, registry)
    run, _ = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, policy), guided=True)
    assert run["status"] == "needs_attention"
    assert len([a for a in run["activations"] if a["role"] == "node"]) == 1
    assert any("candidate_id" in e["error"] for e in run["decision_errors"])


async def test_orchestrator_limit_requires_exact_caller_answer_and_preserves_checkpoint(storage, tmp_path):
    registry = LimitedRegistry(storage.root, limited_role="orchestrator")
    run, _ = await run_flow(storage, tmp_path, definition(), registry, guided=True)
    assert run["status"] == "needs_input"
    assert not any(a["role"] == "node" for a in run["activations"])
    assert LIMIT["failure_diagnostic"]["reset_at"] in run["input_question"]
    pending = copy.deepcopy(run["pending"])
    for answer in ("yes", "continue", "probably fallback please"):
        with pytest.raises(w.WorkflowError, match="exact offered recovery"):
            storage.control(run["workflow_run_id"], "resume", answer, decision_id=run["input_decision_id"])
    approved = storage.control(run["workflow_run_id"], "resume", "fallback", decision_id=run["input_decision_id"])
    assert approved["pending"][0]["id"] == pending[0]["id"]
    assert approved["orchestrator_recovery_candidate"]["model"] == "fallback-fixture"
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    finished = storage.get_run(run["workflow_run_id"])
    assert finished["status"] == "completed"
    orchestrators = [a for a in finished["activations"] if a["role"] == "orchestrator"]
    assert orchestrators[1]["tasks"][0]["session_mode"] == "fresh"
    assert orchestrators[2]["tasks"][0]["session_mode"] == "resume"
    assert len([a for a in finished["activations"] if a["role"] == "node"]) == 1


async def test_unsettled_limit_blocks_every_replacement(storage, tmp_path):
    run, _ = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, unknown=True), guided=True)
    assert run["status"] == "needs_attention"
    assert len([a for a in run["activations"] if a["role"] == "node"]) == 1


def test_usage_limit_never_authorizes_legacy_availability_fallback():
    assert w.availability_failure({**LIMIT, "backend": "codex", "stderr_tail": ["Usage limit exceeded"]}) is None


async def test_limit_after_completed_worker_restores_checkpoint_without_replaying_work(storage, tmp_path):
    class LateLimit(Registry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if self.decode_context(prompt) is not None and len(self.contexts) == 2:
                task.result.update(copy.deepcopy(LIMIT))
            return task
    registry = LateLimit(storage.root)
    run, _ = await run_flow(storage, tmp_path, definition(), registry, guided=True)
    assert run["status"] == "needs_input"
    completed = next(a for a in run["activations"] if a["role"] == "node")
    assert completed["node_result"]["status"] == "succeeded"
    checkpoint_id = run["pending"][0]["id"]
    restarted = w.WorkflowStore(storage.root)
    restarted.control(run["workflow_run_id"], "resume", "fallback", decision_id=run["input_decision_id"])
    await w.WorkflowSupervisor(registry, restarted).execute(run["workflow_run_id"])
    finished = restarted.get_run(run["workflow_run_id"])
    assert finished["status"] == "completed"
    workers = [a for a in finished["activations"] if a["role"] == "node"]
    assert [a["id"] for a in workers] == [completed["id"]]
    assert finished["usage_recovery_history"][0]["token_id"] == checkpoint_id


async def test_live_unsettled_limit_returns_attention_without_waiting_forever(storage, tmp_path):
    class HangingLimit(LimitedRegistry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if "Assignment:\n" in prompt:
                task.done.clear()
                task.result.update(status="running", failure_diagnostic={**LIMIT["failure_diagnostic"], "settlement": "needs_attention"})
            return task
    run, _ = await run_flow(storage, tmp_path, definition(), HangingLimit(storage.root), guided=True)
    assert run["status"] == "needs_attention"
    workers = [a for a in run["activations"] if a["role"] == "node"]
    assert len(workers) == 1 and workers[0]["tasks"][0]["status"] == "uncertain"
    assert workers[0]["tasks"][0]["result"]["status"] == "running"


def test_recovery_choices_are_exposed_in_bounded_response():
    from polybridge.workflow_responses import compact
    run = {"workflow_run_id": "r", "status": "needs_input", "input_decision_id": "d", "orchestrator_usage_recovery": {"decision_id": "d", "failure_diagnostic": LIMIT["failure_diagnostic"], "choices": [{"answer": "fallback", "candidate": {"backend": "codex", "model": "fallback-fixture"}}]}}
    response = compact(run)
    assert response["usage_limit_recovery"]["choices"][0]["answer"] == "fallback"
    assert response["usage_limit_recovery"]["failure_diagnostic"]["reset_at"] == LIMIT["failure_diagnostic"]["reset_at"]


async def test_pause_retains_limit_checkpoint_and_resume_requires_fallback_consent(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, limited_role="orchestrator"), guided=True)
    paused = storage.control(run["workflow_run_id"], "pause")
    assert paused["status"] == "paused"
    assert paused["input_decision_id"] == run["input_decision_id"]
    assert paused["orchestrator_usage_recovery"] == run["orchestrator_usage_recovery"]
    assert paused["pending"] == run["pending"]
    with pytest.raises(w.WorkflowError, match="exact offered recovery"):
        storage.control(run["workflow_run_id"], "resume", "continue", decision_id=run["input_decision_id"])
    with pytest.raises(w.WorkflowError, match="current input decision_id"):
        storage.control(run["workflow_run_id"], "resume", "fallback", decision_id="stale")
    approved = storage.control(run["workflow_run_id"], "resume", "fallback", decision_id=run["input_decision_id"])
    assert approved["status"] == "running"
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"


async def test_cancel_limit_checkpoint_never_dispatches_fallback(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, limited_role="orchestrator"), guided=True)
    before = len(registry.calls)
    storage.control(run["workflow_run_id"], "cancel")
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    cancelled = storage.get_run(run["workflow_run_id"])
    assert cancelled["status"] == "cancelled"
    assert len(registry.calls) == before
    assert cancelled["orchestrator_usage_recovery"]["failure_diagnostic"] == LIMIT["failure_diagnostic"]


async def test_worker_limit_can_ask_caller_then_pause_and_select_fallback(storage, tmp_path):
    def policy(context, registry):
        retry = next((c for c in context["valid_continuations"] if c.get("usage_limit_recovery")), None)
        if retry and not context.get("recovery_instructions"):
            return {"decision_id": context["decision_id"], "action": "needs_input", "reason": "Let caller choose recovery", "question": "work reached quota; pause, cancel or choose configured fallback-fixture?"}
        if retry:
            selected = next(c for c in retry["available_candidates"] if c["candidate"].get("model") == "fallback-fixture")
            return {"decision_id": context["decision_id"], "action": "continue", "reason": "Caller chose fallback", "next": [{"continuation_id": retry["continuation_id"], "candidate_id": selected["candidate_id"], "session_mode": "fresh", "prompt": "Finish retained work"}]}
        return default_decision(context, registry)
    registry = LimitedRegistry(storage.root, policy)
    run, _ = await run_flow(storage, tmp_path, definition(), registry, guided=True)
    assert run["worker_usage_recovery"]["node_id"] == "work"
    assert storage.control(run["workflow_run_id"], "pause")["status"] == "paused"
    storage.control(run["workflow_run_id"], "resume", "Use configured fallback-fixture for work", decision_id=run["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    finished = storage.get_run(run["workflow_run_id"])
    assert finished["status"] == "completed"
    workers = [a for a in finished["activations"] if a["role"] == "node"]
    assert len(workers) == 2 and len(workers[0]["tasks"]) == 1
    assert workers[1]["tasks"][0]["candidate"]["model"] == "fallback-fixture"


async def test_current_child_uses_owner_config_before_restricting_approved_candidate(storage, tmp_path, monkeypatch):
    # This direct dispatch test bypasses execute()'s supervisor receipt; lease safety
    # has separate coverage, while this test exercises owner candidate selection.
    monkeypatch.setattr(w.CheckoutLease, "_orphan_owner", lambda self: None)
    owner_definition = definition()
    owner = storage.create_run(w.validate_definition(owner_definition), "Owner", tmp_path)
    child_definition = definition()
    child_definition["orchestrator"] = {"backend": "codex", "model": "different-child-config"}
    child = storage.create_run(w.validate_definition(child_definition), "Child", tmp_path)
    storage.update_run(child["workflow_run_id"], lambda r: r.update(orchestrator_recovery_candidate={"backend": "codex", "model": "fallback-fixture"}), "test_approval")
    storage.update_run(owner["workflow_run_id"], lambda r: r.update(status="running"), "test_running")
    storage.update_run(child["workflow_run_id"], lambda r: r.update(status="running"), "test_running")
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = child["workflow_run_id"]
    supervisor.orchestrator_override = (owner["workflow_run_id"], owner["definition"]["orchestrator"])
    activation = supervisor._activation("orchestrator", "orchestrator", {})
    outcome = await supervisor._dispatch({"id": "orchestrator"}, "Control assignment", "orchestrator", activation)
    assert outcome["status"] == "completed"
    observed = storage.get_run(child["workflow_run_id"])["activations"][0]["tasks"][0]
    assert observed["candidate"]["model"] == "fallback-fixture"
    assert observed["session_mode"] == "fresh"
    assert "orchestrator_recovery_candidate" not in storage.get_run(child["workflow_run_id"])


@pytest.mark.parametrize("failure", ["missing", "removed", "capability", "not_started", "unavailable", "uncertain", "unsettled_unavailable", "repo_unavailable", "failed", "repeated_limit"])
async def test_failed_approved_control_fallback_requires_new_consent(storage, tmp_path, monkeypatch, failure):
    value = definition()
    value["orchestrator"]["fallbacks"].append({"backend": "claude", "model": "remaining-fixture"})
    registry = LimitedRegistry(storage.root, limited_role="orchestrator")
    run, _ = await run_flow(storage, tmp_path, value, registry, guided=True)
    old_decision = run["input_decision_id"]
    pending = copy.deepcopy(run["pending"])
    storage.control(run["workflow_run_id"], "resume", "fallback", decision_id=old_decision)
    if failure == "removed":
        storage.update_run(run["workflow_run_id"], lambda r: r["definition"]["orchestrator"]["fallbacks"].pop(0), "remove_selected")
    elif failure == "missing":
        # Different backend makes installed-state failure specific to the selected CLI.
        monkeypatch.setattr(w.backends, "is_installed", lambda backend: backend.name != "codex")
    else:
        original_start = registry.start
        async def fail_selected(prompt, repo, **kwargs):
            if kwargs.get("model") == "fallback-fixture":
                if failure == "repo_unavailable":
                    from polybridge.tasks import RepoUnavailableError
                    raise RepoUnavailableError("fixture repo unavailable")
                if failure == "capability":
                    raise w.backends.UnsupportedCapability("fixture capability unavailable")
                if failure == "not_started":
                    exc = RuntimeError("fixture definitely did not spawn")
                    exc.polybridge_not_started = True
                    raise exc
                if failure == "uncertain":
                    raise RuntimeError("fixture spawn uncertain")
                task = await original_start(prompt, repo, **kwargs)
                if failure == "repeated_limit":
                    task.result.update(copy.deepcopy(LIMIT))
                else:
                    task.result.update(status="failed", stderr_tail=[] if failure == "failed" else ["model_not_found"], is_error=True, **({"outcome_unknown": True} if failure == "unsettled_unavailable" else {}))
                return task
            return await original_start(prompt, repo, **kwargs)
        registry.start = fail_selected
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    recovered = storage.get_run(run["workflow_run_id"])
    if failure in {"uncertain", "unsettled_unavailable"}:
        assert recovered["status"] == "needs_attention"
        assert "orchestrator_usage_recovery" not in recovered
        assert recovered["orchestrator_recovery_candidate"]["model"] == "fallback-fixture"
        if failure == "uncertain":
            assert any(t["status"] == "uncertain" for a in recovered["activations"] for t in a["tasks"])
        return
    assert recovered["status"] == "needs_input"
    assert "orchestrator_recovery_candidate" not in recovered
    assert recovered["input_decision_id"] != old_decision
    assert recovered["pending"][0]["id"] == pending[0]["id"]
    recovery = recovered["orchestrator_usage_recovery"]
    assert recovery["failure_diagnostic"] == LIMIT["failure_diagnostic"]
    assert [c["candidate"]["model"] for c in recovery["choices"] if c["answer"] != "retry_current"] == ["remaining-fixture"]
    assert recovered["usage_recovery_history"][-1]["selection_failure"]
    with pytest.raises(w.WorkflowError, match="current input decision_id"):
        storage.control(run["workflow_run_id"], "resume", "fallback", decision_id=old_decision)
    assert not any(kwargs.get("model") == "remaining-fixture" for _, kwargs in registry.calls)
    registry.start = Registry.start.__get__(registry)
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    storage.control(run["workflow_run_id"], "resume", "fallback", decision_id=recovered["input_decision_id"])
    await w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"])
    assert storage.get_run(run["workflow_run_id"])["status"] == "completed"
    assert sum(kwargs.get("model") == "remaining-fixture" and "Assignment:\n" not in prompt for prompt, kwargs in registry.calls) >= 1


async def test_builder_success_consumes_fallback_and_feedback_resumes_session(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w.CheckoutLease, "_orphan_owner", lambda self: None)
    run = storage.create_run(w.validate_definition(definition()), "Builder", tmp_path)
    storage.update_run(run["workflow_run_id"], lambda r: r.update(status="running"), "test_running")
    registry = LimitedRegistry(storage.root, limited_role="orchestrator")
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run["workflow_run_id"]
    node = {"id": "builder"}
    first = supervisor._activation("builder", "builder")
    assert await supervisor._dispatch(node, "Control assignment", "builder", first) is None
    limited = supervisor.run()
    storage.control(run["workflow_run_id"], "resume", "fallback", decision_id=limited["input_decision_id"])
    second = supervisor._activation("builder", "builder")
    assert (await supervisor._dispatch(node, "Control assignment", "builder", second))["status"] == "completed"
    assert "orchestrator_recovery_candidate" not in supervisor.run()
    storage.update_run(run["workflow_run_id"], lambda r: r.update(builder_followup=True), "builder_feedback")
    third = supervisor._activation("builder", "builder")
    assert (await supervisor._dispatch(node, "Control feedback", "builder", third))["status"] == "completed"
    task = supervisor.run()["activations"][-1]["tasks"][0]
    assert task["candidate"]["model"] == "fallback-fixture"
    assert task["session_mode"] == "resume"
    assert task["resume_task_id"] == supervisor.run()["activations"][-2]["tasks"][0]["task_id"]


@pytest.mark.parametrize('verdict,known', [('dead', True), ('alive', False), ('undecidable', False)])
@pytest.mark.parametrize('diagnostic', [LIMIT['failure_diagnostic'], None, {'category': 'other', 'reason': 'generic error'}])
def test_restart_quota_authority_requires_confirmed_dead_process(tmp_path, monkeypatch, verdict, known, diagnostic):
    from types import SimpleNamespace
    from polybridge import identity
    from polybridge.backends.base import Accumulator
    record = SimpleNamespace(status='running', exit_code=None, pid=123, start_time='old', markers=[])
    monkeypatch.setattr(identity, 'check_detail', lambda *_: (verdict, 'fixture'))
    monkeypatch.setattr(w.task_store, 'resolve_status', lambda *a, **k: ('failed', 'Recovered', Accumulator(failure_diagnostic=diagnostic), []))
    observed = w.task_liveness(tmp_path, record)
    assert observed['outcome_known'] is (known and diagnostic == LIMIT['failure_diagnostic'])
    assert observed['process_alive'] is {'dead': False, 'alive': True, 'undecidable': None}[verdict]


async def test_orchestrator_restart_with_dead_quota_reissues_explicit_caller_recovery(storage, tmp_path, monkeypatch):
    from polybridge import identity, store
    registry = LimitedRegistry(storage.root, limited_role='orchestrator')
    run, _ = await run_flow(storage, tmp_path, definition(), registry, guided=True)
    pending = copy.deepcopy(run['pending'])
    task_id = run['activations'][0]['tasks'][0]['task_id']
    record = store.read(registry._log_dir, task_id)
    from dataclasses import replace
    record = replace(record, status='running', exit_code=None, finished_at=None, pid=123, start_time='fixture', failure_diagnostic=copy.deepcopy(LIMIT['failure_diagnostic']))
    import json
    store.log_path(registry._log_dir, task_id).write_text(json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'Partial progress retained'}}) + '\n')
    store.record_path(registry._log_dir, task_id).unlink()
    store.write(registry._log_dir, record)
    def crash(r):
        r.update(status='needs_attention')
        for key in ('orchestrator_usage_recovery', 'input_decision_id', 'input_question', 'supervisor_identity', 'supervisor_pid'):
            r.pop(key, None)
        activation = r['activations'][0]
        activation['status'] = 'running'
        activation['tasks'][0].update(status='running')
        activation['tasks'][0].pop('result', None)
    storage.update_run(run['workflow_run_id'], crash, 'simulate_owner_crash')
    registry.tasks.clear()
    monkeypatch.setattr(w.CheckoutLease, '_orphan_owner', lambda *_: None)
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('dead', 'pid_absent') if value.get('pid') == 123 else ('alive', 'start_time_match'))
    monkeypatch.setattr(identity, 'identity_check', lambda value: 'dead' if value and value.get('pid') == 123 else 'alive')
    restored = storage.reconcile_run(run['workflow_run_id'])
    result = restored['activations'][0]['tasks'][0]['result']
    assert result['status'] == 'failed' and not result.get('outcome_unknown')
    assert result['failure_diagnostic'] == LIMIT['failure_diagnostic']
    assert result['session_id'] == record.session_id
    assert result['summary'] == 'Partial progress retained'
    before = len(registry.calls)
    storage.control(run['workflow_run_id'], 'resume', 'Inspect recovered quota')
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    checkpoint = storage.get_run(run['workflow_run_id'])
    assert checkpoint['status'] == 'needs_input'
    assert len(registry.calls) == before
    assert checkpoint['pending'][0]['id'] == pending[0]['id']
    storage.control(run['workflow_run_id'], 'resume', 'fallback', decision_id=checkpoint['input_decision_id'])
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    assert storage.get_run(run['workflow_run_id'])['status'] == 'completed'


async def test_worker_restart_with_dead_quota_returns_choice_to_orchestrator(storage, tmp_path, monkeypatch):
    import json
    from dataclasses import replace
    from polybridge import identity, store
    def policy(context, registry):
        retry = next((c for c in context['valid_continuations'] if c.get('usage_limit_recovery')), None)
        if retry and 'choose fallback' in context.get('recovery_instructions', ''):
            selected = next(c for c in retry['available_candidates'] if c['candidate'].get('model') == 'fallback-fixture')
            return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Explicit choice', 'next': [{'continuation_id': retry['continuation_id'], 'candidate_id': selected['candidate_id'], 'session_mode': 'fresh', 'prompt': 'Finish retained work'}]}
        if retry:
            return {'decision_id': context['decision_id'], 'action': 'needs_input', 'reason': 'Ask caller', 'question': 'Pause, cancel or choose fallback for work?'}
        return default_decision(context, registry)
    registry = LimitedRegistry(storage.root, policy)
    run, _ = await run_flow(storage, tmp_path, definition(), registry, guided=True)
    worker = next(a for a in run['activations'] if a['role'] == 'node')
    task_id = worker['tasks'][0]['task_id']
    record = replace(store.read(registry._log_dir, task_id), status='running', exit_code=None, finished_at=None, pid=123, start_time='fixture', failure_diagnostic=copy.deepcopy(LIMIT['failure_diagnostic']))
    store.record_path(registry._log_dir, task_id).unlink()
    store.write(registry._log_dir, record)
    store.log_path(registry._log_dir, task_id).write_text(json.dumps({'type': 'item.completed', 'item': {'type': 'agent_message', 'text': 'Partial progress retained'}}) + '\n')
    def crash(r):
        r.update(status='needs_attention')
        for key in ('worker_usage_recovery', 'input_decision_id', 'input_question', 'supervisor_identity', 'supervisor_pid'):
            r.pop(key, None)
        activation = next(a for a in r['activations'] if a['id'] == worker['id'])
        activation.update(status='running')
        activation.pop('node_result', None)
        activation['tasks'][0].update(status='running')
        activation['tasks'][0].pop('result', None)
        token = next(t for t in r['pending'] if t['id'] == activation['token']['id'])
        token.pop('execution_complete', None)
    storage.update_run(run['workflow_run_id'], crash, 'simulate_owner_crash')
    registry.tasks.pop(task_id)
    monkeypatch.setattr(w.CheckoutLease, '_orphan_owner', lambda *_: None)
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('dead', 'pid_absent') if value.get('pid') == 123 else ('alive', 'start_time_match'))
    monkeypatch.setattr(identity, 'identity_check', lambda value: 'dead' if value and value.get('pid') == 123 else 'alive')
    recovered = storage.reconcile_run(run['workflow_run_id'])
    observed = next(a for a in recovered['activations'] if a['id'] == worker['id'])['tasks'][0]['result']
    assert not observed.get('outcome_unknown')
    assert observed['summary'] == 'Partial progress retained'
    storage.control(run['workflow_run_id'], 'resume', 'Inspect recovered quota')
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    checkpoint = storage.get_run(run['workflow_run_id'])
    assert checkpoint['status'] == 'needs_input'
    assert len([a for a in checkpoint['activations'] if a['role'] == 'node']) == 1
    assert any(c.get('usage_limit_recovery') for c in registry.contexts[-1]['valid_continuations'])
    storage.control(run['workflow_run_id'], 'resume', 'choose fallback', decision_id=checkpoint['input_decision_id'])
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    finished = storage.get_run(run['workflow_run_id'])
    assert finished['status'] == 'completed'
    workers = [a for a in finished['activations'] if a['role'] == 'node']
    assert len(workers) == 2
    assert workers[-1]['tasks'][0]['candidate']['model'] == 'fallback-fixture'


async def test_paused_control_limit_without_fallback_can_explicitly_retry_current(storage, tmp_path):
    value = definition()
    value['orchestrator'].pop('fallbacks')
    registry = LimitedRegistry(storage.root, limited_role='orchestrator')
    run, _ = await run_flow(storage, tmp_path, value, registry, guided=True)
    assert [c['answer'] for c in run['orchestrator_usage_recovery']['choices']] == ['retry_current']
    before = len(registry.calls)
    paused = storage.control(run['workflow_run_id'], 'pause')
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    assert len(registry.calls) == before
    assert paused['input_decision_id'] == run['input_decision_id']
    with pytest.raises(w.WorkflowError, match='current input decision_id'):
        storage.control(run['workflow_run_id'], 'resume', 'retry_current', decision_id='stale')
    identity = 'orchestrator:' + w._candidate_key(run['orchestrator_usage_recovery']['failed_candidate'])
    storage.update_run(run['workflow_run_id'], lambda r: r['suppressed_candidates'].extend([identity, 'orchestrator:unrelated']), 'test_suppression')
    approved = storage.control(run['workflow_run_id'], 'resume', 'retry_current', decision_id=run['input_decision_id'])
    assert 'orchestrator:unrelated' in approved['suppressed_candidates']
    assert identity not in approved['suppressed_candidates']
    registry.start = Registry.start.__get__(registry)
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    finished = storage.get_run(run['workflow_run_id'])
    assert finished['status'] == 'completed'
    controls = [a for a in finished['activations'] if a['role'] == 'orchestrator']
    assert controls[1]['tasks'][0]['session_mode'] == 'fresh'
    assert controls[2]['tasks'][0]['session_mode'] == 'resume'
    assert len([a for a in finished['activations'] if a['role'] == 'node']) == 1


async def test_retry_current_repeated_limit_reissues_consent_without_auto_launch(storage, tmp_path):
    value = definition()
    value['orchestrator'].pop('fallbacks')
    registry = LimitedRegistry(storage.root, limited_role='orchestrator')
    run, _ = await run_flow(storage, tmp_path, value, registry, guided=True)
    storage.control(run['workflow_run_id'], 'resume', 'retry_current', decision_id=run['input_decision_id'])
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    checkpoint = storage.get_run(run['workflow_run_id'])
    assert checkpoint['status'] == 'needs_input'
    assert checkpoint['input_decision_id'] != run['input_decision_id']
    assert [c['answer'] for c in checkpoint['orchestrator_usage_recovery']['choices']] == ['retry_current']
    assert len(registry.calls) == 2
    assert 'orchestrator_recovery_candidate' not in checkpoint
    with pytest.raises(w.WorkflowError, match='current input decision_id'):
        storage.control(run['workflow_run_id'], 'resume', 'retry_current', decision_id=run['input_decision_id'])


async def test_retry_current_config_removal_reoffers_current_configuration(storage, tmp_path):
    value = definition()
    value['orchestrator'].pop('fallbacks')
    registry = LimitedRegistry(storage.root, limited_role='orchestrator')
    run, _ = await run_flow(storage, tmp_path, value, registry, guided=True)
    storage.update_run(run['workflow_run_id'], lambda r: r['definition']['orchestrator'].update(model='new-config'), 'replace_config')
    storage.control(run['workflow_run_id'], 'resume', 'retry_current', decision_id=run['input_decision_id'])
    before = len(registry.calls)
    await w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id'])
    checkpoint = storage.get_run(run['workflow_run_id'])
    assert checkpoint['status'] == 'needs_input'
    assert len(registry.calls) == before
    choices = checkpoint['orchestrator_usage_recovery']['choices']
    assert [c['answer'] for c in choices] == ['fallback']
    assert choices[0]['candidate']['model'] == 'new-config'


async def test_unsettled_orchestrator_limit_never_offers_retry_current(storage, tmp_path):
    run, registry = await run_flow(storage, tmp_path, definition(), LimitedRegistry(storage.root, limited_role='orchestrator', unknown=True), guided=True)
    assert run['status'] == 'needs_attention'
    assert 'orchestrator_usage_recovery' not in run
    assert len(registry.calls) == 1


async def test_builder_without_fallback_can_explicitly_retry_current(storage, tmp_path, monkeypatch):
    monkeypatch.setattr(w.CheckoutLease, '_orphan_owner', lambda *_: None)
    value = definition()
    value['orchestrator'].pop('fallbacks')
    run = storage.create_run(w.validate_definition(value), 'Builder', tmp_path, kind='builder')
    storage.update_run(run['workflow_run_id'], lambda r: r.update(status='running'), 'test_running')
    registry = LimitedRegistry(storage.root, limited_role='orchestrator')
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run['workflow_run_id']
    activation = supervisor._activation('builder', 'builder')
    assert await supervisor._dispatch({'id': 'builder'}, 'Control assignment', 'builder', activation) is None
    checkpoint = supervisor.run()
    assert checkpoint['orchestrator_usage_recovery']['role'] == 'builder'
    assert [c['answer'] for c in checkpoint['orchestrator_usage_recovery']['choices']] == ['retry_current']
    storage.control(run['workflow_run_id'], 'resume', 'retry_current', decision_id=checkpoint['input_decision_id'])
    registry.start = Registry.start.__get__(registry)
    activation = supervisor._activation('builder', 'builder')
    assert (await supervisor._dispatch({'id': 'builder'}, 'Control retry', 'builder', activation))['status'] == 'completed'
    assert supervisor.run()['activations'][-1]['tasks'][0]['session_mode'] == 'fresh'
    assert 'orchestrator_recovery_candidate' not in supervisor.run()


@pytest.mark.parametrize('reset', [0, 1791594123, 1791594123.5, '2026-10-10T01:02:03Z'])
async def test_reset_type_survives_workflow_public_projections(storage, tmp_path, reset):
    from polybridge.workflow_responses import compact, monitor, history_page
    class ResetRegistry(LimitedRegistry):
        async def start(self, prompt, repo, **kwargs):
            task = await super().start(prompt, repo, **kwargs)
            if task.result.get('failure_diagnostic'):
                task.result['failure_diagnostic']['reset_at'] = reset
            return task
    run, registry = await run_flow(storage, tmp_path, definition(), ResetRegistry(storage.root, limited_role='orchestrator'), guided=True)
    full = storage.get_run(run['workflow_run_id'])
    for response in (compact(run), monitor(run), history_page([run])['runs'][0]):
        value = response['usage_limit_recovery']['failure_diagnostic']['reset_at']
        assert type(value) is type(reset) and value == reset
    value = full['orchestrator_usage_recovery']['failure_diagnostic']['reset_at']
    assert type(value) is type(reset) and value == reset
    diagnostic = next(task.result['failure_diagnostic'] for task in registry.tasks.values() if task.result.get('failure_diagnostic'))
    assert type(diagnostic['reset_at']) is type(reset)


@pytest.mark.parametrize('reset', [True, float('nan'), float('inf'), 10 ** 1000, 'tomorrow', 'Authorization: Bearer synthetic-private-token'])
def test_compact_omits_invalid_reset_metadata(reset):
    from polybridge.workflow_responses import compact, monitor, history_page
    run = {'workflow_run_id': 'r', 'status': 'needs_input', 'orchestrator_usage_recovery': {'failure_diagnostic': {**LIMIT['failure_diagnostic'], 'reset_at': reset}}}
    for response in (compact(run), monitor(run), history_page([run])['runs'][0]):
        assert 'reset_at' not in response['usage_limit_recovery']['failure_diagnostic']


def test_oversized_iso_reset_is_omitted_from_bounded_projection():
    import json
    from polybridge.backends.workflow_diagnostics import valid_reset_value
    from polybridge.workflow_responses import BUDGET, compact
    reset = '2026-10-10T01:02:03.' + '1' * 30000 + 'Z'
    assert not valid_reset_value(reset)
    run = {'workflow_run_id': 'r', 'status': 'needs_input', 'orchestrator_usage_recovery': {'failure_diagnostic': {**LIMIT['failure_diagnostic'], 'reset_at': reset}}}
    response = compact(run)
    assert 'reset_at' not in response['usage_limit_recovery']['failure_diagnostic']
    assert len(json.dumps(response).encode()) < BUDGET

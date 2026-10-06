"""Codex native executions use the same durable workflow contract as Claude."""
import asyncio
import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from polybridge import backends, store, workflows as w
from polybridge.backends.codex_native import CodexNativeAdapter, CERTIFIED_MODEL
from test_codex_native import PARENT, lifecycle, native_rollouts
import test_codex_native as fixtures
from test_workflow_delegation import Registry, Task, graph


def codex_graph():
    definition = graph("review")
    definition["orchestrator"]["model"] = CERTIFIED_MODEL
    definition["nodes"][1].update(agent={"backend": "codex", "model": CERTIFIED_MODEL}, execution_mode="prefer_subagent", session_mode="fresh")
    return definition


class CodexRegistry(Registry):
    def __init__(self, root, monkeypatch, *, mutation=None, terminal=True):
        super().__init__(root)
        self.monkeypatch = monkeypatch
        self.mutation = mutation
        self.terminal = terminal
        self.native_calls = []

    async def start(self, prompt, repo, **kwargs):
        task = await super().start(prompt, repo, **kwargs)
        record = store.read(self._log_dir, task.task_id)
        task.result["session_id"] = PARENT
        store.write(self._log_dir, replace(record, session_id=PARENT, model=CERTIFIED_MODEL))
        return task

    async def resume_record(self, record, prompt, **kwargs):
        previous = Task(record.task_id, {"session_id": record.session_id})
        previous.repo = Path(record.repo_path)
        previous.kwargs = {"backend": backends.get("codex"), "freedom": record.freedom}
        return await self.resume(previous, prompt, **kwargs)

    async def resume(self, previous, prompt, **kwargs):
        if not kwargs.get("native_subagent"):
            return await super().resume(previous, prompt, **kwargs)
        self.native_calls.append(kwargs)
        args = json.loads(prompt.split("Spawn arguments:\n", 1)[1])
        nonce = args["message"].splitlines()[0].split(": ", 1)[1]
        assignment = args["message"].split("\n\n", 1)[1]
        child = f"00000000-0000-0000-0000-{100 + len(self.native_calls):012d}"
        self.monkeypatch.setattr(fixtures, "CHILD", child)
        native_rollouts(self._log_dir.parent, self.monkeypatch, nonce, assignment, self.mutation)
        # The worker result is distinct from the parent's dispatch acknowledgement.
        directory = self._log_dir.parent / "sessions" / "2026" / "10" / "06"
        child_file = next(directory.glob(f"*-{child}.jsonl"))
        records = [json.loads(line) for line in child_file.read_text().splitlines()]
        for record in records:
            if record["payload"].get("type") == "task_complete":
                record["payload"]["last_agent_message"] = json.dumps({"status": "succeeded", "result": {"summary": "Native reviewed", "verdict": "approved"}, "evidence": ["observed"]})
        # The fixture location and workflow checkout are the same tmp directory.
        child_file.write_text("\n".join(json.dumps(record) for record in records) + "\n")
        events = lifecycle(nonce, assignment, PARENT, child)
        if not self.terminal:
            events = events[:-1]
        for event in events:
            self._workflow_native_observers[kwargs["task_id"]](event)
        record = store.read(self._log_dir, previous.task_id)
        task = Task(kwargs["task_id"], {"summary": "Parent dispatch acknowledgement", "session_id": PARENT})
        task.kwargs, task.repo = previous.kwargs, previous.repo
        self.tasks[task.task_id] = task
        store.write(self._log_dir, replace(record, task_id=task.task_id))
        return task


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(backends, "is_installed", lambda _: True)
    monkeypatch.setattr(backends, "version", lambda _: "codex-cli 0.160.1")
    # Exercise conformance even while production activation awaits CLI certification.
    monkeypatch.setattr(type(backends.get("codex")), "native_subagent_adapter", property(lambda _: CodexNativeAdapter()), raising=False)
    return w.WorkflowStore(tmp_path), CodexRegistry(tmp_path, monkeypatch)


async def execute(storage, registry, tmp_path, definition=None):
    run = storage.create_run(w.validate_definition(definition or codex_graph()), "Review", tmp_path)
    supervisor = w.WorkflowSupervisor(registry, storage)
    await asyncio.wait_for(supervisor.execute(run["workflow_run_id"]), 5)
    return storage.get_run(run["workflow_run_id"]), supervisor


async def test_codex_native_result_identity_metadata_and_slot_release(setup, tmp_path):
    storage, registry = setup
    run, supervisor = await execute(storage, registry, tmp_path)
    assert run["status"] == "completed", run.get("attention_reason")
    worker = next(a for a in run["activations"] if a["role"] == "node")
    attempt = worker["tasks"][0]
    assert attempt["execution_kind"] == "native_subagent"
    assert worker["node_result"]["result"]["summary"] == "Native reviewed"
    assert store.read(registry._log_dir, attempt["task_id"]) is None
    assert store.read(registry._log_dir, attempt["transport_task_id"]) is not None
    assert attempt["harness_metadata"]["observed"]["approval_policy"] == "never"
    assert attempt["can_cancel_child"] is attempt["can_resume_child"] is False
    assert registry.native_calls[0]["max_turns"] is None
    assert supervisor.tree.slots.free == run["definition"]["max_parallel"]
    assert supervisor.tree.control_slots.free == 1


@pytest.mark.parametrize("mutation,terminal", [("nonce", True), ("missing_terminal", True), ("wrong_output", True), (None, False)])
async def test_codex_unproven_child_never_duplicates_or_releases_checkout(setup, tmp_path, mutation, terminal):
    storage, registry = setup
    registry.mutation, registry.terminal = mutation, terminal
    run, _ = await execute(storage, registry, tmp_path)
    assert run["status"] == "needs_attention"
    worker = next(a for a in run["activations"] if a["role"] == "node")
    assert worker["tasks"][0]["status"] == "uncertain"
    assert len(registry.native_calls) == 1
    assert all("Context:\n" in prompt for prompt, _ in registry.calls)
    assert worker["tasks"][0]["task_id"] in storage.pinned_tasks()
    storage.reconcile_run(run["workflow_run_id"])
    assert next(a for a in storage.get_run(run["workflow_run_id"])["activations"] if a["role"] == "node")["tasks"][0]["status"] == "uncertain"


@pytest.mark.parametrize("setting,value", [("model", "other-model"), ("reasoning_effort", "low")])
async def test_codex_unsupported_settings_fall_back_before_launch(setup, tmp_path, setting, value):
    storage, registry = setup
    definition = codex_graph()
    definition["nodes"][1]["agent"][setting] = value
    run, _ = await execute(storage, registry, tmp_path, definition)
    assert not registry.native_calls
    worker = next(a for a in run["activations"] if a["role"] == "node")
    assert worker["execution_kind"] == "headless"
    assert worker["execution_fallback_reason"]


async def test_two_codex_native_nodes_keep_owner_and_distinct_children(setup, tmp_path):
    storage, registry = setup
    definition = codex_graph()
    second = copy.deepcopy(definition["nodes"][1])
    second["id"] = "second"
    definition["nodes"].insert(2, second)
    definition["connections"][1]["target"] = "second"
    definition["connections"].append({"id": "done", "source": "second", "target": "end"})
    run, _ = await execute(storage, registry, tmp_path, definition)
    assert run["status"] == "completed", run.get("attention_reason")
    attempts = [a["tasks"][0] for a in run["activations"] if a["role"] == "node"]
    assert len(registry.native_calls) == 2
    assert len({attempt["native_child_id"] for attempt in attempts}) == 2
    assert {attempt["owner_session_id"] for attempt in attempts} == {PARENT}


async def test_codex_native_can_resume_persisted_owner(setup, tmp_path, monkeypatch):
    storage, registry = setup
    monkeypatch.setattr(registry, "get", lambda _: None)
    run, _ = await execute(storage, registry, tmp_path)
    assert run["status"] == "completed", run.get("attention_reason")
    assert len(registry.native_calls) == 1


@pytest.mark.parametrize("root_cancel", [False, True])
async def test_codex_cancel_and_timeout_preserve_unresolved_child(setup, tmp_path, root_cancel):
    storage, registry = setup
    registry.terminal = False
    original = registry.resume
    cancellations = []
    definition = codex_graph()
    definition["nodes"][1]["timeout_seconds"] = 1
    run = storage.create_run(w.validate_definition(definition), "Review", tmp_path)

    async def hanging(previous, prompt, **kwargs):
        task = await original(previous, prompt, **kwargs)
        if kwargs.get("native_subagent"):
            task.done.clear()
            if root_cancel:
                storage.update_run(run["workflow_run_id"], lambda r: r.update(status="cancelling"), "root_cancel_fixture")
        return task

    async def cancel(task_id, **kwargs):
        cancellations.append(task_id)
        task = registry.tasks[task_id]
        task.result["status"] = "cancelled"
        task.done.set()

    registry.resume, registry.cancel_cascade = hanging, cancel
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run["workflow_run_id"]), 5)
    observed = storage.get_run(run["workflow_run_id"])
    attempt = next(a for a in observed["activations"] if a["role"] == "node")["tasks"][0]
    assert observed["status"] == "needs_attention"
    assert attempt["status"] == "uncertain"
    assert set(cancellations) == {attempt["transport_task_id"]}
    assert attempt["task_id"] in storage.pinned_tasks()
    assert len(registry.native_calls) == 1

"""Supervisor shutdown and live-feedback admission races use real root transactions."""
import asyncio
import fcntl
import threading
import time
import uuid
from types import SimpleNamespace

from harflow_mcp import state_ownership, tasks, workflow_steering as s, workflows as w
from harflow_mcp.state_layout import resolve_layout
from test_workflow_steering_service import Registry, Task, request, setup


def fresh(storage, rid):
    return w.WorkflowStore(storage.root).get_run(rid, refresh_atomic=True)


async def main_owned(storage, rid):
    with state_ownership.state_session(resolve_layout(root=storage.root)):
        await w._main(SimpleNamespace(root=str(storage.root), run_id=rid))


async def test_main_hands_fast_explicit_reply_to_new_owner_after_unlock(setup, monkeypatch):
    storage, rid, _ = setup
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = Registry(storage.root, "Discuss this, then wait for an explicit reply.")
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    original = s.run_builder_turn
    children, locks, receipts = [], [], []
    first = True

    def launch(store, run_id):
        with (store.runs / f"{run_id}.supervisor.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                locks.append("busy")
            else:
                locks.append("free")
        children.append(asyncio.create_task(main_owned(store, run_id)))

    async def classify_then_reply(supervisor, run_id):
        nonlocal first
        await original(supervisor, run_id)
        if first:
            first = False
            ep = fresh(storage, rid)["steering"]["episodes"][0]
            receipts.append(await s.steer_workflow(rid, request(storage, rid, action="reply", ep=ep, text="One explicit second turn."), root=storage.root))
            # Admission launched while the old _main still owns its flock. The
            # actual child must finish its nonblocking refusal before old finally.
            await children[0]
            assert len(registry.calls) == 1

    monkeypatch.setattr(w, "_launch", launch)
    monkeypatch.setattr(s, "run_builder_turn", classify_then_reply)
    await main_owned(storage, rid)
    assert locks == ["busy", "free"]
    await asyncio.gather(*children)
    persisted = fresh(storage, rid)["steering"]
    assert len(registry.calls) == len(persisted["turns"]) == 2
    assert registry.calls[1][2]["task_id"] == receipts[0]["task_id"]
    assert all(turn["state"] == "settled" for turn in persisted["turns"])
    assert persisted["episodes"][0]["state"] == "awaiting_caller"
    assert len(children) == 2


async def test_main_unknown_old_issuance_does_not_spawn_shutdown_loop(setup, monkeypatch):
    storage, rid, _ = setup
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    await s.steer_workflow(rid, request(storage, rid), root=storage.root)
    registry = Registry(storage.root, "Capture is incomplete.", output_drained=False)
    launches = []
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    monkeypatch.setattr(w, "_launch", lambda store, run_id: launches.append(run_id))
    await main_owned(storage, rid)
    assert launches == []
    assert len(registry.calls) == 1
    assert fresh(storage, rid)["steering"]["episodes"][0]["state"] == "delivery_unknown"
    await main_owned(storage, rid)
    assert launches == []
    assert len(registry.calls) == len(fresh(storage, rid)["steering"]["turns"]) == 1


async def test_live_feedback_claim_rechecks_steering_admitted_after_outer_read(setup, monkeypatch):
    storage, rid, _ = setup
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    task_id, activation_id = uuid.uuid4().hex, uuid.uuid4().hex
    def active(run):
        run.update(status="running", builder_messages=[{"id": "feedback", "prompt": "Ordinary live feedback", "status": "pending"}])
        run["activations"].append({"id": activation_id, "node_id": "builder", "role": "builder", "status": "running", "created_at": time.time(), "token": None,
                                   "tasks": [{"task_id": task_id, "status": "running", "candidate": {"backend": "claude"}}]})
    storage.update_run(rid, active, "test_active_live_builder")
    registry = Registry(storage.root)
    sent, admitted, errors = [], [], []
    async def send_message(task, prompt):
        sent.append(prompt)
        return {"message_id": "must-not-send"}
    registry.send_message = send_message
    task = Task(task_id, "Old live builder settles.")
    task.live_input = True
    task.done.clear()
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = rid
    original = supervisor.update

    def update(mutate, event, *args, **kwargs):
        if event == "builder_feedback_forwarding":
            # This hook is reached only after the outer held=false read. A real
            # service transaction wins before claim_live enters its root CAS.
            req = request(storage, rid)
            def admit():
                try:
                    admitted.append(asyncio.run(s.steer_workflow(rid, req, root=storage.root)))
                except BaseException as exc:
                    errors.append(exc)
            thread = threading.Thread(target=admit)
            thread.start()
            thread.join(timeout=10)
            assert not thread.is_alive()
            assert errors == [] and len(admitted) == 1
            result = original(mutate, event, *args, **kwargs)
            task.done.set()
            return result
        return original(mutate, event, *args, **kwargs)

    monkeypatch.setattr(supervisor, "update", update)
    activation = fresh(storage, rid)["activations"][0]
    await supervisor._observe_attempt(task, {}, "builder", activation, activation["tasks"][0])
    persisted = fresh(storage, rid)
    assert admitted[0]["scheduled"] is True
    assert persisted["steering"]["turns"][0]["state"] == "queued"
    assert persisted["steering"]["episodes"][0]["hold"] is True
    assert persisted["builder_messages"][0]["status"] == "pending"
    assert sent == []


async def test_terminal_main_exit_wakes_open_admitted_after_initial_completed_read(setup, monkeypatch):
    storage, rid, _ = setup
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(storage.root))
    assert fresh(storage, rid)["status"] == "completed"
    req = request(storage, rid)
    registry = Registry(storage.root, "Exactly one explicitly opened turn.")
    monkeypatch.setattr(tasks, "TaskRegistry", lambda **kwargs: registry)
    original = w.WorkflowStore.get_run
    injected = False
    locks, lost_children, handed_children, receipts, errors = [], [], [], [], []

    def launch(store, run_id):
        with (store.runs / f"{run_id}.supervisor.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                locks.append("busy")
                lost_children.append(asyncio.create_task(main_owned(store, run_id)))
            else:
                locks.append("free")
                handed_children.append(asyncio.create_task(main_owned(store, run_id)))

    async def admit_with_losing_child():
        receipts.append(await s.steer_workflow(rid, req, root=storage.root))
        await asyncio.gather(*lost_children)

    def get_run(store, run_id, *args, **kwargs):
        nonlocal injected
        snapshot = original(store, run_id, *args, **kwargs)
        if run_id == rid and not injected:
            injected = True
            assert snapshot["status"] == "completed"
            # The duplicate owner already holds the flock but will return this
            # terminal snapshot. Admission races strictly after that read.
            def admit():
                try:
                    asyncio.run(admit_with_losing_child())
                except BaseException as exc:
                    errors.append(exc)
            thread = threading.Thread(target=admit)
            thread.start()
            thread.join(timeout=10)
            assert not thread.is_alive()
            assert errors == [] and len(receipts) == 1
            assert len(lost_children) == 1 and lost_children[0].done()
            assert registry.calls == []
        return snapshot

    monkeypatch.setattr(w, "_launch", launch)
    monkeypatch.setattr(w.WorkflowStore, "get_run", get_run)
    await main_owned(storage, rid)
    assert locks == ["busy", "free"]
    assert len(handed_children) == 1
    await asyncio.gather(*handed_children)
    persisted = fresh(storage, rid)["steering"]
    assert len(registry.calls) == len(persisted["turns"]) == len(persisted["requests"]) == 1
    assert registry.calls[0][2]["task_id"] == receipts[0]["task_id"]
    assert persisted["turns"][0]["state"] == "settled"
    assert persisted["episodes"][0]["state"] == "awaiting_caller"
    assert locks == ["busy", "free"]

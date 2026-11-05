"""Generation-bound withdrawal faults; no processes or providers are launched."""
import copy
from types import SimpleNamespace

import pytest

from harflow_mcp import task_supervision as launch, task_withdrawal as withdrawal
from harflow_mcp.workflow_receipt_documents import encode


@pytest.fixture
def pending_owner(tmp_path, monkeypatch):
    from harflow_mcp import state_ownership
    from harflow_mcp.state_layout import resolve_layout
    with state_ownership.state_session(resolve_layout(root=tmp_path)):
        pass
    log_dir = tmp_path / "tasks"
    log_dir.mkdir()
    info = log_dir.stat()
    owner = dict(pid=424242, start_time="independent-owner-generation", markers=[])
    value = dict(format=launch.FORMAT, product="harflow", product_version=launch.PRODUCT_VERSION, v=1,
        task_id="pending-original", generation="c" * 32, root=str(tmp_path),
        directory_identity=[info.st_dev, info.st_ino], directory_binding=dict(path=str(log_dir.resolve()), inode=info.st_ino),
        state="waiting_for_resources", request_sha256="d" * 64, configuration_sha256="e" * 64, backend="claude",
        session_id="original-session", parent_task_id=None, chain_root_task_id=None, owner=owner,
        repo_path=str(tmp_path), freedom="read_only")
    launch._write(log_dir / "pending-original.launch.json", value)
    owners = tmp_path / "workflow-owners"
    owners.mkdir()
    launch._write(owners / "pending-original.json", encode(dict(workflow_run_id="root", root_workflow_run_id="root",
        workflow_execution_id="original-execution"), "pending-original"))
    registry = SimpleNamespace(log_dir=log_dir, _owner=owner, _launch_generations={"pending-original": "c" * 32})
    effects = []
    async def forbidden_signal(*args, **kwargs):
        effects.append("SIGNAL")
        pytest.fail("withdraw-only protocol must never signal")
    registry.cancel_cascade = forbidden_signal
    with state_ownership.state_session(resolve_layout(root=tmp_path)):
        yield registry, value, effects


def publish(registry, value):
    return withdrawal.request_withdrawal(registry.log_dir, value["task_id"], value["generation"], value["request_sha256"],
        value["configuration_sha256"], "replan-one", root_workflow_run_id="root", source_execution_id="original-execution")


def test_prestart_owner_ack_is_positive_and_identity_cannot_be_reused(pending_owner):
    registry, value, effects = pending_owner
    publish(registry, value)
    with pytest.raises(withdrawal.LaunchWithdrawn) as stopped:
        withdrawal.check_prestart(registry, value["task_id"])
    ack = withdrawal.settle_withdrawn(registry, value["task_id"], stopped.value)
    assert ack["outcome"] == "withdrawn"
    assert launch.read_launch(registry.log_dir, value["task_id"])["state"] == "not_started"
    assert withdrawal.reconcile_withdrawal(registry.log_dir, value["task_id"], value["generation"]) == ack
    with pytest.raises(withdrawal.WithdrawalError):
        withdrawal.refuse_retired_task_id(registry.log_dir, value["task_id"])
    assert effects == []
    assert not list(registry.log_dir.glob("*.launch-cancel.json"))


def test_intent_after_last_check_never_signals_or_claims_no_spawn(pending_owner):
    registry, value, effects = pending_owner
    withdrawal.check_prestart(registry, value["task_id"])
    effects.append("SPAWN")  # Independent owner event after its last prestart check.
    publish(registry, value)
    ack = withdrawal.acknowledge_issued(registry, value["task_id"])
    assert ack["outcome"] == "issued_or_unknown" and ack["proof"] is None
    assert withdrawal.reconcile_withdrawal(registry.log_dir, value["task_id"], value["generation"]) == ack
    assert effects == ["SPAWN"]
    assert not list(registry.log_dir.glob("*.launch-cancel.json"))


@pytest.mark.parametrize("field", ["generation", "request_sha256", "configuration_sha256"])
def test_stale_generation_or_fingerprint_cannot_withdraw_current_launch(pending_owner, field):
    registry, value, effects = pending_owner
    publish(registry, value)
    changed = copy.deepcopy(value)
    changed[field] = "f" * len(changed[field])
    launch._write(registry.log_dir / "pending-original.launch.json", changed)
    withdrawal.check_prestart(registry, value["task_id"])
    assert withdrawal.read_ack(registry.log_dir, value["task_id"], value["generation"]) is None
    assert launch.read_launch(registry.log_dir, value["task_id"])[field] == changed[field]
    assert effects == []


@pytest.mark.parametrize("state,positive", [("waiting_for_resources", True), ("claimed", False), ("granted", False)])
def test_dead_owner_recovery_needs_waiting_no_spawn_proof(pending_owner, monkeypatch, state, positive):
    from harflow_mcp import identity
    registry, value, effects = pending_owner
    value["state"] = state
    launch._write(registry.log_dir / "pending-original.launch.json", value)
    publish(registry, value)
    monkeypatch.setattr(identity, "identity_check", lambda owner: "dead")
    ack = withdrawal.reconcile_withdrawal(registry.log_dir, value["task_id"], value["generation"])
    if positive:
        assert ack["outcome"] == "withdrawn"
        assert ack["proof"]["origin"] == "dead_waiting_owner"
    else:
        assert ack is None
        assert launch.read_launch(registry.log_dir, value["task_id"])["state"] == state
    assert effects == []


def test_owner_generation_mismatch_cannot_publish_positive_ack(pending_owner):
    registry, value, effects = pending_owner
    publish(registry, value)
    registry._owner = {**registry._owner, "start_time": "different-owner"}
    with pytest.raises(withdrawal.WithdrawalError):
        withdrawal.check_prestart(registry, value["task_id"])
    assert withdrawal.read_ack(registry.log_dir, value["task_id"], value["generation"]) is None
    assert effects == []


def test_release_failure_retains_launch_without_positive_ack(pending_owner, monkeypatch):
    from harflow_mcp import task_resources
    registry, value, effects = pending_owner
    value["resources"] = dict(reservation_id="a" * 32, state="granted", names=["fixture-resource"])
    launch._write(registry.log_dir / "pending-original.launch.json", value)
    publish(registry, value)
    class RefusedLedger:
        def release_owned_task(self, reservation_id, task_id):
            effects.append(("RELEASE_REFUSED", reservation_id, task_id))
            raise RuntimeError("fixture ownership mismatch")
    monkeypatch.setattr(task_resources, "ledger_for", lambda directory: RefusedLedger())
    with pytest.raises(withdrawal.LaunchWithdrawn) as stopped:
        withdrawal.check_prestart(registry, value["task_id"])
    with pytest.raises(RuntimeError, match="ownership mismatch"):
        withdrawal.settle_withdrawn(registry, value["task_id"], stopped.value)
    assert withdrawal.read_ack(registry.log_dir, value["task_id"], value["generation"]) is None
    assert launch.read_launch(registry.log_dir, value["task_id"])["state"] == "waiting_for_resources"
    assert effects == [("RELEASE_REFUSED", "a" * 32, "pending-original")]


def test_lost_ack_recovers_positive_terminal_proof_without_new_effect(pending_owner, monkeypatch):
    registry, value, effects = pending_owner
    publish(registry, value)
    with pytest.raises(withdrawal.LaunchWithdrawn) as stopped:
        withdrawal.check_prestart(registry, value["task_id"])
    original_write = withdrawal._write
    class PowerLoss(BaseException):
        pass
    def cut_ack(path, document):
        if path.name.endswith(".withdraw-ack.json"):
            raise PowerLoss()
        return original_write(path, document)
    monkeypatch.setattr(withdrawal, "_write", cut_ack)
    with pytest.raises(PowerLoss):
        withdrawal.settle_withdrawn(registry, value["task_id"], stopped.value)
    assert launch.read_launch(registry.log_dir, value["task_id"])["not_started_reason"] == "withdrawn"
    assert withdrawal.read_ack(registry.log_dir, value["task_id"], value["generation"]) is None
    monkeypatch.setattr(withdrawal, "_write", original_write)
    ack = withdrawal.reconcile_withdrawal(registry.log_dir, value["task_id"], value["generation"])
    assert ack["outcome"] == "withdrawn"
    assert withdrawal.reconcile_withdrawal(registry.log_dir, value["task_id"], value["generation"]) == ack
    assert effects == []


def test_saved_precheck_exception_cannot_certify_later_spawn_ack_gap(pending_owner):
    registry, value, effects = pending_owner
    publish(registry, value)
    with pytest.raises(withdrawal.LaunchWithdrawn) as stopped:
        withdrawal.check_prestart(registry, value["task_id"])
    effects.append("SPAWN")
    value["state"] = "spawned"
    launch._write(registry.log_dir / "pending-original.launch.json", value)
    with pytest.raises(withdrawal.WithdrawalError):
        withdrawal.settle_withdrawn(registry, value["task_id"], stopped.value)
    assert launch.read_launch(registry.log_dir, value["task_id"])["state"] == "spawned"
    ack = withdrawal.read_ack(registry.log_dir, value["task_id"], value["generation"])
    assert ack is None or ack["outcome"] == "issued_or_unknown"
    assert effects == ["SPAWN"]


@pytest.mark.parametrize("corruption", ["missing", "different_execution", "malformed"])
def test_workflow_owner_receipt_refusal_cannot_create_withdrawal(pending_owner, corruption):
    registry, value, effects = pending_owner
    path = registry.log_dir.parent / "workflow-owners" / "pending-original.json"
    if corruption == "missing":
        path.unlink()
    elif corruption == "malformed":
        path.write_text("{broken")
    else:
        launch._write(path, encode(dict(workflow_run_id="root", root_workflow_run_id="root",
            workflow_execution_id="different-execution"), "pending-original"))
    from harflow_mcp.workflow_receipt_documents import WorkflowReceiptDocumentError
    with pytest.raises((withdrawal.WithdrawalError, WorkflowReceiptDocumentError, FileNotFoundError)):
        publish(registry, value)
    assert withdrawal.read_withdrawal(registry.log_dir, value["task_id"], value["generation"]) is None
    assert withdrawal.read_ack(registry.log_dir, value["task_id"], value["generation"]) is None
    assert launch.read_launch(registry.log_dir, value["task_id"])["state"] == "waiting_for_resources"
    assert effects == []

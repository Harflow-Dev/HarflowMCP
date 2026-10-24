"""Focused contract tests for Claude caller approval admission and ledger semantics."""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approvals, store
from harflow_mcp import approval_runtime
from harflow_mcp.backends import get
from harflow_mcp.backends.base import UnsupportedCapability
from harflow_mcp.backends.claude_approvals import decode, encode, validate
from harflow_mcp.backends import Accumulator
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("caller-approvals") / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        tasks = root / "tasks"
        tasks.mkdir()
        yield tasks


QUALIFIED = {"version": "2.1.310", "observed": "2.1.310", "write": False}


def _task(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        task_id="task-1",
        proc=SimpleNamespace(pid=os.getpid()),
        start_time="self-start",
        markers=("session-1",),
        owner={"pid": os.getpid(), "start_time": "bridge-start", "markers": []},
        approval_generation="generation-1",
        session_id="session-1",
        repo_path=tmp_path,
        freedom="write_in_repo",
        caller_approval_version="2.1.310",
        caller_write_approvals=False,
        protected_paths=None,
    )


def _request(task: SimpleNamespace, index: int = 1) -> dict:
    return {
        "native_request_id": f"native-{index}",
        "tool_use_id": f"tool-{index}",
        "action": {"tool_name": "Bash", "input": {"command": f"printf {index}"}},
    }


def _admit(tmp_path: Path, index: int = 1) -> dict:
    task = _task(tmp_path)
    return approvals.admit(
        tmp_path,
        task,
        _request(task, index),
        {
            "interaction_owner": "caller",
            "workflow_run_id": None,
            "root_workflow_run_id": None,
            "workflow_node_id": None,
            "workflow_execution_id": None,
        },
    )


def _reserve_worker(directory: str, request_id: str, action_digest: str, generation: str, output) -> None:
    try:
        approvals.reserve(Path(directory), "task-1", request_id, action_digest, 1, "approve", generation)
    except Exception as exc:  # pragma: no cover - result is asserted by parent
        output.put(type(exc).__name__)
    else:
        output.put("won")


def test_claude_caller_approval_contract_is_strict() -> None:
    with pytest.raises(UnsupportedCapability):
        validate(True, "read_only", None)
    with pytest.raises(UnsupportedCapability):
        validate(True, "write_in_repo", 1)
    with pytest.raises(UnsupportedCapability):
        validate(True, "write_in_repo", None, native=True)
    validate(False, "read_only", 1)


@pytest.mark.parametrize(
    "event",
    [
        {"type": "assistant", "message": "please approve this"},
        {"type": "control_request", "request_id": "r", "request": {"subtype": "selection"}},
        {
            "type": "control_request",
            "request_id": "r",
            "request": {"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "t", "input": {"command": "x", "dangerouslyDisableSandbox": True}},
        },
        {
            "type": "control_request",
            "request_id": "r",
            "request": {"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "t", "input": {"command": "x"}, "parent_tool_use_id": "child"},
        },
    ],
)
def test_only_exact_root_bash_control_request_is_decodable(event: dict) -> None:
    if event.get("type") != "control_request":
        assert decode(event, QUALIFIED) is None
    else:
        with pytest.raises(ValueError):
            decode(event, QUALIFIED)


def test_claude_decode_and_encode_preserve_exact_action() -> None:
    event = {
        "type": "control_request",
        "request_id": "native-1",
        "request": {
            "subtype": "can_use_tool",
            "tool_name": "Bash",
            "tool_use_id": "tool-1",
            "input": {"command": "printf 'a  b\\n'", "timeout": 17},
        },
    }
    decoded = decode(event, QUALIFIED)
    assert decoded == {"native_request_id": "native-1", "tool_use_id": "tool-1", "action": {"tool_name": "Bash", "input": {"command": "printf 'a  b\\n'", "timeout": 17}}, "decision_reason": None, "decision_reason_type": None}
    import json
    payload = json.loads(encode(decoded, "approve"))
    assert payload["response"]["request_id"] == "native-1"
    assert payload["response"]["response"]["updatedInput"] == event["request"]["input"]
    reject = json.loads(encode(decoded, "reject"))
    assert reject["response"]["response"]["message"] == "Caller rejected this exact action"


def test_ledger_hides_action_from_metadata_and_requires_exact_cas(tmp_path: Path) -> None:
    directory = tmp_path
    row = _admit(directory)
    metadata = approvals.metadata(row)
    assert "action" not in metadata
    assert "bridge_owner" not in metadata
    full = approvals.get(directory, "task-1", row["request_id"])
    assert full["action"] == row["action"] and full["complete"] is True
    with pytest.raises(ValueError, match="Stale"):
        approvals.reserve(directory, "task-1", row["request_id"], "wrong", row["owner_revision"], "approve", row["generation"])
    reserved = approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    with pytest.raises(ValueError, match="expired, settled"):
        approvals.reserve(directory, "task-1", row["request_id"], row["action_digest"], reserved["owner_revision"], "reject", row["generation"])


def test_ledger_rejects_duplicate_native_identity_and_bounds_pending(tmp_path: Path) -> None:
    directory = tmp_path
    first = _admit(directory)
    with pytest.raises(ValueError, match="Repeated"):
        approvals.admit(directory, _task(directory), _request(_task(directory)), {"interaction_owner": "caller"})
    for index in range(2, approvals.MAX_PENDING + 1):
        _admit(directory, index)
    with pytest.raises(ValueError, match="count bound"):
        _admit(directory, approvals.MAX_PENDING + 1)
    assert len(approvals.rows(directory, "task-1")) == approvals.MAX_PENDING


def test_cross_process_reservation_has_one_winner(tmp_path: Path) -> None:
    row = _admit(tmp_path)
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    processes = [context.Process(target=_reserve_worker, args=(str(tmp_path), row["request_id"], row["action_digest"], row["generation"], output)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=10)
        assert process.exitcode == 0
    assert sorted(output.get(timeout=2) for _ in processes) == ["ValueError", "won"]


def test_approval_answer_crossing_deadline_is_transmitted_as_deny(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    row = _admit(tmp_path)
    reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    monkeypatch.setattr(approvals.time, "time", lambda: reserved["deadline"] + 1)
    sending = approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    assert sending["decision"] == "approve"
    assert sending["effective_decision"] == "reject"
    approvals.finish_delivery(tmp_path, "task-1", row["request_id"], row["generation"], True, sending["effective_decision"])
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    assert final["decision"] == "approve"
    assert final["effective_decision"] == "reject"
    assert final["delivered_decision"] == "reject"
    assert json.loads(encode(final, final["effective_decision"]))["response"]["response"]["behavior"] == "deny"


def test_large_control_request_is_rejected_before_action_is_exposed(tmp_path: Path) -> None:
    task = _task(tmp_path)
    oversized = _request(task)
    oversized["action"]["input"]["command"] = "x" * approvals.MAX_REQUEST_BYTES
    with pytest.raises(ValueError, match="64 KiB"):
        approvals.admit(tmp_path, task, oversized, {"interaction_owner": "caller"})


def _ledger_bytes(path: Path) -> bytes:
    return path.read_bytes()


def test_qualified_ledger_header_and_row_task_identity_are_required(tmp_path: Path) -> None:
    row = _admit(tmp_path)
    path = approvals._path(tmp_path, "task-1")
    original = _ledger_bytes(path)
    document = json.loads(original)
    assert document["product"] == "harflow"
    assert document["format"] == "harflow.task.approvals"
    assert document["version"] == 1
    assert document["task_id"] == "task-1"

    for field, value in (("product", "foreign"), ("version", True), ("task_id", "other-task")):
        candidate = dict(document)
        candidate[field] = value
        path.write_bytes(json.dumps(candidate).encode())
        with pytest.raises(ValueError, match="unreadable|Invalid approval"):
            approvals.rows(tmp_path, "task-1")
        assert path.read_bytes() == json.dumps(candidate).encode()

    candidate = dict(document)
    candidate["requests"] = [dict(row, task_id="other-task")]
    raw = json.dumps(candidate).encode()
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="unreadable|Invalid approval"):
        approvals.rows(tmp_path, "task-1")
    assert path.read_bytes() == raw
    path.write_bytes(original)
    assert approvals.rows(tmp_path, "task-1")[0]["request_id"] == row["request_id"]


@pytest.mark.parametrize("product_version, accepted", [("0.0.99+fixture", True), ("2.0.0", False), ("0.0.1-rc.1", False)])
def test_product_version_qualification_preserves_stable_line_policy(
    tmp_path: Path, product_version: str, accepted: bool
) -> None:
    _admit(tmp_path)
    path = approvals._path(tmp_path, "task-1")
    document = json.loads(path.read_bytes())
    document["product_version"] = product_version
    path.write_bytes(approvals.canonical(document))
    if accepted:
        assert len(approvals.rows(tmp_path, "task-1")) == 1
    else:
        with pytest.raises(ValueError, match="unreadable|Invalid approval"):
            approvals.rows(tmp_path, "task-1")


@pytest.mark.parametrize(
    "raw",
    [b"not json", b"\xff\xfe", b"[1, 2, 3]"],
)
def test_invalid_or_non_object_ledger_is_unavailable_without_synthesis(tmp_path: Path, raw: bytes) -> None:
    path = approvals._path(tmp_path, "task-1")
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="unreadable|Invalid approval"):
        approvals.rows(tmp_path, "task-1")
    assert path.read_bytes() == raw


def test_oversized_and_recursive_ledgers_are_rejected_without_truncation(tmp_path: Path) -> None:
    path = approvals._path(tmp_path, "task-1")
    oversized = b"{" + b"x" * approvals.MAX_LEDGER_BYTES + b"}"
    path.write_bytes(oversized)
    with pytest.raises(ValueError, match="exceeds|unreadable"):
        approvals.rows(tmp_path, "task-1")
    assert path.read_bytes() == oversized

    deeply_nested = b"[" * 1200 + b"]" * 1200
    path.write_bytes(deeply_nested)
    with pytest.raises(ValueError, match="unreadable|Invalid approval"):
        approvals.rows(tmp_path, "task-1")


def test_bounded_nofollow_reads_reject_symlink_and_nonregular_sources(tmp_path: Path) -> None:
    path = approvals._path(tmp_path, "task-1")
    target = tmp_path / "target.json"
    target.write_bytes(b"{}")
    path.symlink_to(target)
    with pytest.raises((OSError, ValueError)):
        approvals.rows(tmp_path, "task-1")
    path.unlink()
    path.mkdir()
    with pytest.raises(ValueError, match="regular|unreadable"):
        approvals.rows(tmp_path, "task-1")


def test_missing_ledger_is_verified_absence_and_dangling_link_is_not_empty(tmp_path: Path) -> None:
    path = approvals._path(tmp_path, "task-1")
    assert approvals.rows(tmp_path, "task-1") == []
    assert not path.exists()
    path.symlink_to(tmp_path / "missing-target.json")
    with pytest.raises((OSError, ValueError)):
        approvals.rows(tmp_path, "task-1")


def test_candidate_qualification_happens_before_temporary_publication(tmp_path: Path) -> None:
    path = approvals._path(tmp_path, "task-1")
    before = sorted(tmp_path.iterdir())
    created: list[Path] = []
    real_mkstemp = approvals.tempfile.mkstemp

    def spy_mkstemp(*args, **kwargs):
        created.append(Path(kwargs["dir"]) / "candidate")
        return real_mkstemp(*args, **kwargs)

    # Invalid candidates must be rejected before any temporary file is created.
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(approvals.tempfile, "mkstemp", spy_mkstemp)
    with pytest.raises(ValueError, match="full|Invalid approval"):
        approvals._write(tmp_path, path, "task-1", {"product": "harflow", "version": 1})
    monkeypatch.undo()
    assert created == []
    assert sorted(tmp_path.iterdir()) == before


@pytest.mark.parametrize("extra", [float("nan"), object()])
def test_nonfinite_or_nonserializable_qualified_candidate_never_creates_temp(
    tmp_path: Path, extra: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _admit(tmp_path)
    path = approvals._path(tmp_path, "task-1")
    original = path.read_bytes()
    original_inode = path.stat().st_ino
    candidate = json.loads(original)
    candidate["diagnostic"] = extra
    created: list[object] = []
    monkeypatch.setattr(approvals.tempfile, "mkstemp", lambda *args, **kwargs: created.append(True))
    with pytest.raises((TypeError, ValueError), match="Invalid approval|allow_nan|[Oo]ut of range|serializ"):
        approvals._write(tmp_path, path, "task-1", candidate)
    assert created == []
    assert path.read_bytes() == original
    assert path.stat().st_ino == original_inode
    assert approvals.rows(tmp_path, "task-1")[0]["request_id"] == row["request_id"]


def test_finite_over_limit_candidate_never_creates_temp_or_changes_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    row = _admit(tmp_path)
    path = approvals._path(tmp_path, "task-1")
    original = path.read_bytes()
    original_inode = path.stat().st_ino
    candidate = json.loads(original)
    candidate["diagnostic"] = "x" * approvals.MAX_LEDGER_BYTES
    created: list[object] = []
    monkeypatch.setattr(approvals.tempfile, "mkstemp", lambda *args, **kwargs: created.append(True))
    with pytest.raises(ValueError, match="full"):
        approvals._write(tmp_path, path, "task-1", candidate)
    assert created == []
    assert path.read_bytes() == original
    assert path.stat().st_ino == original_inode
    assert approvals.rows(tmp_path, "task-1")[0]["request_id"] == row["request_id"]


def test_finite_additive_outer_and_row_metadata_survive_decision_mutations(tmp_path: Path) -> None:
    row = _admit(tmp_path)
    path = approvals._path(tmp_path, "task-1")
    document = json.loads(path.read_bytes())
    document["diagnostic"] = {"source": "fixture", "count": 1}
    document["requests"][0]["row_extension"] = {"preserve": True}
    approvals._write(tmp_path, path, "task-1", document)
    reserved = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    approvals.update(tmp_path, "task-1", row["request_id"], row["generation"], "reserved", delivery="sending")
    final = approvals.get(tmp_path, "task-1", row["request_id"])
    persisted = approvals._read(tmp_path, path, "task-1")
    assert persisted["diagnostic"] == {"source": "fixture", "count": 1}
    assert final["row_extension"] == {"preserve": True}
    assert final["action_digest"] == row["action_digest"] == approvals.digest(final["action"])
    assert reserved["decision"] == "approve"


def test_full_header_counts_toward_admission_storage_bound(tmp_path: Path) -> None:
    task = _task(tmp_path)
    context = {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None,
               "workflow_node_id": None, "workflow_execution_id": None, "diagnostic": "x" * approvals.ADMISSION_BYTES}
    with pytest.raises(ValueError, match="storage bound"):
        approvals.admit(tmp_path, task, _request(task), context)
    assert not approvals._path(tmp_path, "task-1").exists()


def test_complete_generation_remains_readable_after_atomic_replacement(tmp_path: Path) -> None:
    row = _admit(tmp_path)
    path = approvals._path(tmp_path, "task-1")
    first = json.loads(path.read_bytes())
    second = dict(first)
    second["extra"] = {"fixture": True}
    replacement = tmp_path / "replacement.json"
    replacement.write_bytes(approvals.canonical(second))
    real_qualified = approvals._qualified
    replaced = False

    def replace_after_qualification(document, task_id):
        nonlocal replaced
        result = real_qualified(document, task_id)
        if not replaced:
            replaced = True
            os.replace(replacement, path)
        return result

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(approvals, "_qualified", replace_after_qualification)
    try:
        assert approvals.rows(tmp_path, "task-1")[0]["request_id"] == row["request_id"]
    finally:
        monkeypatch.undo()
    assert replaced is True
    assert json.loads(path.read_bytes())["extra"] == {"fixture": True}


@pytest.mark.asyncio
async def test_runtime_admits_only_a_real_control_request_and_keeps_prose_out_of_ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = _task(tmp_path)
    task.caller_approvals = True
    task.interaction_owner = "caller"
    task.acc = Accumulator()
    task.acc.stream_state["claude_code_version"] = "2.1.310"
    task.approval_pending = False
    task.approval_receiving = False
    task.approval_view = {"pending_count": 0, "interaction_state": None, "requests": []}
    task.cancel_requested = False
    task.bridge_notices = []
    task.pump_wake = asyncio.Event()

    class Registry:
        log_dir = tmp_path

        @property
        def state_layout(self):
            return resolve_layout(log_dir=self.log_dir)

        def persist(self, value):
            return None

        def _stop_native_transport(self, value):
            value.cancel_requested = True

    registry = Registry()
    monkeypatch.setattr(approval_runtime, "context", lambda *_args: {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None})
    backend = get("claude")
    prose = {"type": "assistant", "message": {"content": [{"type": "text", "text": "please approve"}]}}
    assert await approval_runtime.observe(registry, task, backend, prose) is False
    assert not (tmp_path / "task-1.approvals.json").exists()

    event = {"type": "control_request", "request_id": "native-1", "request": {"subtype": "can_use_tool", "tool_name": "Bash", "tool_use_id": "tool-1", "input": {"command": "printf exact"}}}
    assert await approval_runtime.observe(registry, task, backend, event) is True
    records = approvals.rows(tmp_path, "task-1")
    assert len(records) == 1
    assert records[0]["action"]["input"]["command"] == "printf exact"
    assert task.approval_pending is True


@pytest.mark.asyncio
async def test_respond_refuses_foreign_row_before_reservation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A qualified task record cannot be bypassed by changing a persisted row identity."""
    directory = tmp_path
    task = _task(directory)
    store_record = store.TaskRecord(
        task_id="task-1", backend="claude", session_id="session-1", repo_path=str(directory),
        started_at="now", status="running", approval_generation=task.approval_generation,
        start_time=task.start_time, owner=task.owner,
    )
    store.write(directory, store_record)
    row = _admit(directory)

    class Registry:
        log_dir = directory
        state_layout = resolve_layout(log_dir=directory)
        task = SimpleNamespace(pump_wake=SimpleNamespace(set=lambda: None))

        async def _mutation_caller(self):
            return None

        def get(self, _task_id):
            return self.task

    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _identity: "alive")
    path = approvals._path(directory, "task-1")
    valid = path.read_bytes()
    document = json.loads(path.read_bytes())
    document["requests"][0]["task_id"] = "foreign-task"
    raw = approvals.canonical(document)
    path.write_bytes(raw)
    before = path.stat()
    with pytest.raises(ValueError, match="unreadable|Invalid approval"):
        await approval_runtime.respond_request(
            Registry(), "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve"
        )
    assert path.read_bytes() == raw
    assert json.loads(raw)["requests"][0]["owner_revision"] == row["owner_revision"]
    assert path.stat().st_ino == before.st_ino

    path.write_bytes(valid)
    answered = await approval_runtime.respond_request(
        Registry(), "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve"
    )
    assert answered["decision"] == "approve"
    assert answered["delivery"] == "reserved"


@pytest.mark.asyncio
async def test_pump_refuses_foreign_row_without_writing_stdin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = _task(tmp_path)
    task.backend = "claude"
    task.caller_approvals = True
    task.interaction_owner = "caller"
    task.cancel_requested = False
    task.input_closed = False
    task.proc.returncode = None
    task.acc = Accumulator()
    task.approval_receiving = False
    task.approval_pending = True
    task.approval_view = {"pending_count": 1, "interaction_state": "waiting_for_approval", "requests": []}
    row = _admit(tmp_path)
    approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    path = approvals._path(tmp_path, "task-1")
    valid = path.read_bytes()
    document = json.loads(path.read_bytes())
    document["requests"][0]["task_id"] = "foreign-task"
    raw = approvals.canonical(document)
    path.write_bytes(raw)
    before = path.stat()
    writes: list[bytes] = []
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _identity: "alive")
    monkeypatch.setattr("harflow_mcp.tasks._write_stdin", lambda _task, wire: writes.append(wire) or True)

    class Registry:
        log_dir = tmp_path
        state_layout = resolve_layout(log_dir=tmp_path)

        def get(self, _task_id):
            return task

        def _stop_native_transport(self, value):
            value.cancel_requested = True

    with pytest.raises(ValueError, match="unreadable|Invalid approval"):
        await approval_runtime.pump(Registry(), task)
    assert writes == []
    assert path.read_bytes() == raw
    assert path.stat().st_ino == before.st_ino

    # Restore the otherwise eligible generation as a causal positive control.
    path.write_bytes(valid)
    assert await approval_runtime.pump(Registry(), task) is False
    assert len(writes) == 1


@pytest.mark.asyncio
async def test_tool_result_racing_local_send_is_reconciled_without_stale_pump_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A fast Claude tool_result can arrive between send reservation and final ledger update."""
    task = _task(tmp_path)
    task.backend = "claude"
    task.caller_approvals = True
    task.interaction_owner = "caller"
    task.cancel_requested = False
    task.input_closed = False
    task.approval_pending = True
    task.approval_receiving = False
    task.approval_view = {"pending_count": 1, "interaction_state": "waiting_for_approval", "requests": []}
    task.pump_wake = asyncio.Event()
    task.proc.returncode = None

    class Registry:
        log_dir = tmp_path

        @property
        def state_layout(self):
            return resolve_layout(log_dir=self.log_dir)

        def get(self, _task_id):
            return task

    registry = Registry()
    row = _admit(tmp_path)
    row = approvals.reserve(tmp_path, "task-1", row["request_id"], row["action_digest"], row["owner_revision"], "approve", row["generation"])
    monkeypatch.setattr(approval_runtime, "context", lambda *_args: {"interaction_owner": "caller", "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None})
    monkeypatch.setattr(approval_runtime.approvals, "summary", lambda *_args: {"pending_count": 0, "interaction_state": None, "requests": []})
    monkeypatch.setattr(approval_runtime.identity, "identity_check", lambda _identity: "alive")
    import harflow_mcp.tasks
    monkeypatch.setattr(harflow_mcp.tasks, "_write_stdin", lambda *_args: True)

    original_update = approvals.update
    raced = False

    def update_with_fast_outcome(directory, task_id, request_id, generation, expected_delivery=None, **changes):
        nonlocal raced
        result = original_update(directory, task_id, request_id, generation, expected_delivery, **changes)
        if changes.get("delivery") == "sending" and not raced:
            raced = True
            original_update(directory, task_id, request_id, generation, "sending", execution_outcome="succeeded", disposition="decided", delivery="sent")
        return result

    monkeypatch.setattr(approval_runtime.approvals, "update", update_with_fast_outcome)
    assert await approval_runtime.pump(registry, task) is False
    settled = approvals.rows(tmp_path, "task-1")[0]
    assert settled["execution_outcome"] == "succeeded"
    assert settled["disposition"] == "decided"
    assert settled["delivery"] == "sent"


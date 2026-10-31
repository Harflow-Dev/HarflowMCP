"""Generate phase-slot fixtures from actual ctl output against isolated seeded storage.

Run with --output DIRECTORY for a complete temporary bundle, --provenance FILE for
controlled-fact evidence, or --check to verify the canonical bundle without writes.
Only the seven named resource-slot fixtures and manifest entries are owned here.
Acquisition, task authority and cleanup execution are not exercised: storage is
seeded using qualified synthetic ledger entries, then read through actual ctl.main.
IDs and UTC timestamps are deterministic; process identity, caller detection and
unknown supervisor liveness are controlled as in generate-approval-fixtures.py.
"""
import argparse
from contextlib import ExitStack, redirect_stdout, redirect_stderr
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

from harflow_mcp import ctl, identity, lineage, phase_slots, resources, state_ownership
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, PRODUCT_VERSION, validate_envelope

FIXTURES = Path(__file__).resolve().parents[1] / "contracts" / "v1"
OUTPUT = None
AT = "2026-10-31T00:00:00+00:00"
GRANTED = "2026-10-31T00:00:01+00:00"
IDENTITY = {"pid": 6789, "start_time": "fixture-owner-generation", "markers": []}
provenance = {}


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode()


def entry(rid, name, *, state="held", owner=None, **extra):
    row = {"reservation_id": rid, "names": [name], "state": state,
           "owner": owner or {"kind": "task", "task_id": "task-" + rid},
           "generation": 1, "enqueued_at": AT}
    if state == "held":
        row["granted_at"] = GRANTED
    return row | extra


def phase(rid, name, *, state="held", phase_state=None, **extra):
    task = "task-" + rid
    return entry(rid, name, state=state, purpose="phase", phase_state=phase_state or state,
                 execution_generation="a" * 64, scope_chain=["task:" + task],
                 owner_identity=IDENTITY, agent_identity={"pid": 6790, "start_time": "fixture-agent-generation", "markers": []},
                 task_authority={"freedom": "unrestricted", "network": None, "network_access": "not_controlled"},
                 cleanup_policy=None, policy_epoch=1, **extra)


def seed(root, entries=(), names=None, events=()):
    document = resources._empty_document() | {"v": 2, "entries": list(entries),
        "slot_configuration": {"epoch": 1, "names": names or {}}, "slot_events": list(events)}
    resources._validate_document(document)
    resources._atomic_write(root / "resources" / "ledger.json", document)
    # No receipts are needed for these read-only ledger-backed cases. No transaction,
    # admission, reconciliation or cleanup runs after seeding.


def record(root, filename, command, note, expected=0):
    out, err = io.StringIO(), io.StringIO()
    with patch.dict(os.environ, {"HARFLOW_STATE_ROOT": str(root)}), redirect_stdout(out), redirect_stderr(err):
        code = ctl.main([*command, "--json"])
    if code != expected:
        raise RuntimeError((filename, code, out.getvalue(), err.getvalue()))
    document = json.loads(out.getvalue())
    validate_envelope(document, CTL_FORMAT, live=True, expected_version=PRODUCT_VERSION)
    data = canonical(document)
    (OUTPUT / filename).write_bytes(data)
    provenance[filename] = {"command": ["harflow-ctl", *command, "--json"], "exit_code": code,
        "source": "actual ctl.main output; validated synthetic isolated storage", "note": note,
        "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
    return document


def generate(output, provenance_path=None):
    global OUTPUT
    OUTPUT = output
    OUTPUT.mkdir(parents=True, exist_ok=True)
    provenance.clear()
    with tempfile.TemporaryDirectory(prefix="mac45-phase-candidates-") as base, ExitStack() as stack:
        base = Path(base)
        home = base / "home"
        home.mkdir()
        stack.enter_context(patch.dict(os.environ, {"HOME": str(home), "HARFLOW_TASK_ID": "", "HARFLOW_DEPTH": ""}))
        stack.enter_context(patch.object(identity, "own_identity", lambda: IDENTITY))
        stack.enter_context(patch.object(identity, "identity_check", lambda value: "unknown"))
        stack.enter_context(patch.object(lineage, "detect_caller_detail", lambda *a, **k: SimpleNamespace(caller=None, undecidable=None)))
        root = base / "state"
        with state_ownership.state_session(resolve_layout(root=root)):
            (root / "tasks").mkdir(exist_ok=True)
        record(root, "ctl-phase-slot-not-enabled.json", ["slot", "status"], "Unchanged layout 1; typed disabled-layout refusal.", expected=1)
        phase_slots.enable(root)
        seed(root)
        record(root, "ctl-phase-slot-status-empty.json", ["slot", "status"], "Enabled empty inventory/configuration; supervisor missing.")
        idle_names = {"build": {"capacity": 2}, "simulator": {"capacity": 1}}
        seed(root, names=idle_names)
        record(root, "ctl-phase-slot-status-configured-idle.json", ["slot", "status"], "Configured names remain visible with no reservations.")

        rows = [
            entry("legacy-holder", "build"),
            entry("workflow-holder", "build", owner={"kind": "workflow_run", "workflow_run_id": "workflow-fixture-1"}, release_requested=True),
            entry("workflow-borrower", "build", owner={"kind": "workflow_attempt", "workflow_run_id": "workflow-fixture-1", "activation_id": "activation-fixture-1"}, borrows_from="workflow-holder"),
            phase("build-waiter-1", "build", state="queued", deadline_at="2026-10-31T00:05:00+00:00"),
            phase("build-waiter-2", "build", state="queued", deadline_at="2026-10-31T00:05:01+00:00"),
            phase("simulator-cleanup", "simulator", phase_state="cleanup_uncertain", cleanup_started_at="2026-10-31T00:00:02+00:00", diagnostic="Cleanup runner state is uncertain; human inspection is required."),
            phase("simulator-waiter", "simulator", state="queued"),
            phase("unconfigured-holder", "unconfigured-device"),
        ]
        events = [{"at": AT, "kind": "queued", "lease_id": "build-waiter-1", "task_id": "task-build-waiter-1", "name": "build"}]
        seed(root, rows, idle_names, events)
        record(root, "ctl-phase-slot-status-populated.json", ["slot", "status"], "Phase FIFO positions, whole-task/workflow blockers, uncertain cleanup and unconfigured capacity one. Lease array is ledger order, not queue order.")
        record(root, "ctl-resources-mixed.json", ["resources"], "Same isolated seed as populated status: whole-task legacy shape, workflow_run, borrowed workflow_attempt, queued phases, retained release request, cleanup hold and unconfigured active name.")
        service = {"product": "harflow", "format": "harflow.slot-service", "v": 1,
                   "product_version": PRODUCT_VERSION, "generation": "b" * 32, "identity": IDENTITY}
        resources._atomic_write(root / "resources" / "slot-service.json", service)
        record(root, "ctl-phase-slot-status-service-unavailable.json", ["slot", "status"], "Qualified service record with controlled unknown process liveness; occupancy remains readable and unchanged. No invented service state.")
        (root / "resources" / "slot-service.json").unlink()
        seed(root, [phase(f"lease-{i:03}", f"resource-{i:03}") for i in range(257)])
        truncated = record(root, "ctl-phase-slot-status-truncated.json", ["slot", "status"], "257 validated independent active phase entries; actual serializer returns first 256 and truncated=true.")
        assert truncated["result"]["truncated"] is True and len(truncated["result"]["leases"]) == 256
    write_bundle(output)
    if provenance_path is not None:
        provenance_path.parent.mkdir(parents=True, exist_ok=True)
        provenance_path.write_text(json.dumps({"fixtures": provenance, "controlled_facts": ["seeded IDs, UTC timestamps and raw validated ledger storage", "own process identity and caller detection", "service process liveness unknown"], "no_output_redaction": True}, indent=2, sort_keys=True) + "\n")


def write_bundle(output):
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    owned = set(provenance)
    for name, expected in manifest["files"].items():
        if name in owned:
            continue
        payload = (FIXTURES / name).read_bytes()
        if hashlib.sha256(payload).hexdigest() != expected:
            raise SystemExit(f"{name} differs from its recorded hash; this generator does not own it")
        if output.resolve() != FIXTURES.resolve():
            (output / name).write_bytes(payload)
    for name in sorted(owned):
        manifest["files"][name] = provenance[name]["sha256"]
        manifest["cases"][name] = {"encoding": "json", "expected_product_version": PRODUCT_VERSION,
            "format": CTL_FORMAT, "required_capabilities": ["phase-slots.v1"], "valid": True}
    payload = (json.dumps(manifest, indent=2, ensure_ascii=False) + "\n").encode()
    (output / "manifest.json").write_bytes(payload)
    (output / "manifest.sha256").write_text(f"{hashlib.sha256(payload).hexdigest()}  manifest.json\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=FIXTURES, help="Complete bundle output directory (default: canonical contracts/v1)")
    parser.add_argument("--provenance", type=Path, help="Optional controlled-fact provenance output")
    parser.add_argument("--check", action="store_true", help="Check canonical bytes without modifying them")
    args = parser.parse_args()
    if args.check:
        if args.output != FIXTURES:
            parser.error("--check cannot be combined with --output")
        with tempfile.TemporaryDirectory(prefix="harflow-resource-slot-check-") as temporary:
            output = Path(temporary)
            generate(output, args.provenance)
            for path in output.iterdir():
                canonical_path = FIXTURES / path.name
                if not canonical_path.exists() or canonical_path.read_bytes() != path.read_bytes():
                    raise SystemExit(f"Canonical fixture differs: {path.name}")
        print("Resource-slot fixtures and manifest match isolated generation")
    else:
        generate(args.output, args.provenance)
        print(f"Generated {len(provenance)} resource-slot fixtures and complete bundle in {args.output}")


if __name__ == "__main__":
    main()

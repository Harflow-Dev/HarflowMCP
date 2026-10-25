"""Contract evidence for the live, deterministic caller-approval fixture bundle."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from harflow_mcp import approvals


ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "contracts" / "v1"
GENERATOR = ROOT / "scripts" / "generate-approval-fixtures.py"


def _walk(value):
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


def _approval_documents():
    return {
        path.name: json.loads(path.read_text())
        for path in FIXTURES.glob("ctl-approval-*.json")
    }


def test_fixture_actions_recompute_digests_and_private_ledger_fields_stay_hidden():
    documents = _approval_documents()
    action_digests = {}
    for name, document in documents.items():
        text = json.dumps(document, sort_keys=True)
        assert "path_snapshot" not in text
        assert "bridge_owner" not in text
        assert "process_identity" not in text
        for node in _walk(document):
            if isinstance(node, dict) and isinstance(node.get("action"), dict) and isinstance(node.get("action_digest"), str):
                assert node["action_digest"] == approvals.digest(node["action"]), name
                action_digests[node["action_digest"]] = node["action"]

    assert action_digests, "the fixture bundle must contain complete approval-get actions"
    for name, document in documents.items():
        for node in _walk(document):
            if isinstance(node, dict) and isinstance(node.get("action_digest"), str):
                assert node["action_digest"] in action_digests, name


def test_synthetic_fixture_provenance_is_explicit_and_optional_fields_are_omitted_only_there():
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    synthetic = [name for name in manifest["cases"] if name.startswith("ctl-approval-") and name.endswith("-synthetic.json")]
    assert synthetic
    for name in synthetic:
        document = json.loads((FIXTURES / name).read_text())
        text = json.dumps(document)
        if "optional-fields-omitted" in name:
            assert "responder" not in text
            assert "decision_reason" not in text
            assert "decision_reason_type" not in text
        assert manifest["cases"][name]["valid"] is True
    live = [name for name in manifest["cases"] if name.startswith("ctl-approval-") and not name.endswith("-synthetic.json")]
    assert live and all(manifest["cases"][name]["format"] == "harflow.ctl" for name in live)


def test_generator_check_reproduces_the_committed_bundle():
    result = subprocess.run([sys.executable, str(GENERATOR), "--check"], cwd=ROOT, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr


def test_generator_provenance_distinguishes_live_ctl_from_synthetic_transformations(tmp_path: Path):
    provenance = tmp_path / "provenance.json"
    output = tmp_path / "fixtures"
    subprocess.run(
        [sys.executable, str(GENERATOR), "--output", str(output), "--provenance", str(provenance)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    data = json.loads(provenance.read_text())
    fixtures = data["fixtures"]
    assert fixtures
    for name, metadata in fixtures.items():
        if name.endswith("-synthetic.json"):
            assert metadata["provenance"] == "synthetic"
            assert metadata["source"] == "synthetic"
        else:
            assert metadata["provenance"] == "live"
            assert metadata["source"].startswith("ctl ")

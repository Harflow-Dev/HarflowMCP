"""Phase 3.1 public wire contract regressions."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import tomllib

from harflow_mcp import events
from harflow_mcp import state_ownership
from harflow_mcp import workflow_definition_documents
from harflow_mcp import workflow_run_documents
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import (
    CAPABILITIES,
    EVENT_FORMAT,
    MAX_VERSION_COMPONENT,
    compatible_line,
    parse_version,
    validate_envelope,
    PRODUCT_VERSION,
)


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("wire-contracts") / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


def test_version_parser_rejects_bool_float_overflow_prerelease_and_long_metadata():
    for value in (True, 1.0, f"1.0.{MAX_VERSION_COMPONENT + 1}", "1.0.0-alpha", "1.0.0+" + "x" * 256):
        with pytest.raises(ValueError):
            parse_version(value)


@pytest.mark.parametrize("actual, expected", [("1.2.0", "1.2.7"), ("1.2.7", "1.2.0")])
def test_compatibility_accepts_patch_differences_in_both_directions(actual, expected):
    assert compatible_line(actual, expected)


def test_event_provenance_accepts_historical_release_line_but_rejects_foreign_complete_line(tmp_path):
    historical = {"product": "harflow", "format": EVENT_FORMAT, "v": 1, "product_version": "1.1.0", "seq": 1, "kind": "notice", "text": "old"}
    foreign = {**historical, "product": "other"}
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps(foreign) + "\n" + json.dumps(historical) + "\n" + '{"product":"harflow"')
    result = events.read_page(path, limit=10)
    assert [item["text"] for item in result.events] == ["old"]


def test_canonical_fixture_manifest_digest_matches_bytes():
    root = Path(__file__).parents[1] / "contracts" / "v1"
    manifest = json.loads((root / "manifest.json").read_text())
    digest = hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest()
    expected = (root / "manifest.sha256").read_text().split()[0]
    assert digest == expected
    assert set(manifest["files"]) == set(manifest["cases"])
    for name, expected_hash in manifest["files"].items():
        path = root / name
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == expected_hash


def test_project_release_declaration_matches_wire_contract_version():
    project = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert project["project"]["version"] == PRODUCT_VERSION


def test_version_info_advertises_additive_monitor_approval_capability():
    assert "caller-approvals.monitor-respond.v1" in CAPABILITIES
    document = json.loads((Path(__file__).parents[1] / "contracts" / "v1" / "version-info.json").read_text())
    assert "caller-approvals.monitor-respond.v1" in document["capabilities"]


def test_manifest_cases_reach_the_wire_validator():
    root = Path(__file__).parents[1] / "contracts" / "v1"
    manifest = json.loads((root / "manifest.json").read_text())
    for name, case in manifest["cases"].items():
        documents = [json.loads(line) for line in (root / name).read_text().splitlines() if line.strip()]
        for document in documents:
            live = case["format"] != EVENT_FORMAT
            if case["valid"]:
                if case["format"] == workflow_definition_documents.DEFINITION_FORMAT:
                    assert workflow_definition_documents.decode(document, document["definition"]["name"]) == document["definition"]
                elif case["format"] == workflow_run_documents.RUN_FORMAT:
                    assert workflow_run_documents.decode(document, document["run"]["workflow_run_id"]) == document["run"]
                else:
                    validate_envelope(document, case["format"], live=live, expected_version=case["expected_product_version"])
            elif name == "invalid-ctl-inner-event.json":
                validate_envelope(document, case["format"], live=True)
                with pytest.raises(ValueError, match="product/format"):
                    validate_envelope(document["result"]["events"][0], EVENT_FORMAT, live=False)
            elif name == "invalid-missing-capabilities.json":
                validate_envelope(document, case["format"], live=True)
                required = {"ctl.v1", "setup.v1", "events.v1", "version-info.v1"}
                assert not required.issubset(set(document.get("capabilities", [])))
            else:
                with pytest.raises((ValueError, workflow_definition_documents.WorkflowDefinitionDocumentError, workflow_run_documents.WorkflowRunDocumentError)) as caught:
                    if case["format"] == workflow_definition_documents.DEFINITION_FORMAT:
                        workflow_definition_documents.decode(document, document.get("definition", {}).get("name", "fixture"))
                    elif case["format"] == workflow_run_documents.RUN_FORMAT:
                        workflow_run_documents.decode(document, document.get("run", {}).get("workflow_run_id", "fixture"))
                    else:
                        validate_envelope(document, case["format"], live=live, expected_version=case["expected_product_version"])
                reason = str(caught.value)
                if name in {"invalid-major-mismatch.json", "invalid-minor-mismatch.json"}:
                    assert "major/minor" in reason
                elif name == "invalid-prerelease.json":
                    assert "prerelease" in reason
                elif name == "invalid-malformed-product-version.json":
                    assert "Malformed product version" in reason
                elif name == "invalid-overflow-version.json":
                    assert "exceeds" in reason
                elif name in {"invalid-foreign-product.json", "invalid-foreign-format.json", "invalid-missing-product.json"}:
                    assert "product/format" in reason
                elif name in {"invalid-boolean-version.json", "invalid-unsupported-version.json"}:
                    assert "wire version" in reason.lower()
                elif name == "invalid-workflow-definition-version.json":
                    assert "major/minor" in reason
                elif name == "invalid-workflow-definition-foreign.json":
                    assert "product/format" in reason
                elif name == "invalid-workflow-run.json":
                    assert "document version" in reason.lower()


def test_event_validation_requires_current_identity_before_payload_interpretation():
    for wire_version in (True, 1.0):
        with pytest.raises(ValueError):
            validate_envelope({"product": "harflow", "format": EVENT_FORMAT, "v": wire_version, "product_version": "1.0.0", "seq": 1, "kind": "notice"}, EVENT_FORMAT, live=False)
    with pytest.raises(ValueError):
        validate_envelope({"v": 1, "seq": 1, "kind": "notice"}, EVENT_FORMAT, live=False)


def test_version_info_uses_the_same_effective_home_as_path_home_without_state(tmp_path):
    entrypoint = Path(sys.executable).with_name("harflow-ctl")
    assert entrypoint.is_file()
    for home in (str(tmp_path / "normal"), str(tmp_path / "a" / ".." / "b") + "//", ""):
        env = dict(os.environ)
        env.pop("HOME", None)
        if home != "":
            env["HOME"] = home
        control = subprocess.check_output(
            [sys.executable, "-c", "from pathlib import Path; print(Path.home())"],
            env=env,
            text=True,
        ).strip()
        state_path = Path(control) / ".harflow"
        existed_before = state_path.exists()
        result = subprocess.run(
            [str(entrypoint), "version-info", "--json"], env=env,
            text=True, capture_output=True, check=False,
        )
        assert result.returncode == 0, result.stderr
        document = json.loads(result.stdout)
        assert document["state_root"] == os.path.abspath(os.path.join(control, ".harflow"))
        assert state_path.exists() is existed_before

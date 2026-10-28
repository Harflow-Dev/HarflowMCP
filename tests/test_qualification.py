"""Shared qualification state, identity and bounded probe behavior, without paid harnesses."""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import time
from types import SimpleNamespace

import pytest

from harflow_mcp.backends import qualification as q
from harflow_mcp.backends.base import UnsupportedCapability

IDENTITY = q.Identity("fixture", "1.2.3", "/fixture", "a" * 64, "darwin")


def passing(context):
    path = __import__("pathlib").Path(context.identity.executable)
    with path.open("a") as stream:
        stream.write("probe\n")
    facts = {"first": True, "second": True}
    return q.ProbeResult(True, "behavior matched", facts, {"second": facts})


def failing(context):
    return q.ProbeResult(False, "approval was skipped")


def slow(context):
    time.sleep(5)
    return q.ProbeResult(True, "too late")


@pytest.fixture
def setup(tmp_path, monkeypatch):
    identity = replace(IDENTITY, executable=str(tmp_path / "calls"))
    monkeypatch.setattr(q, "identify", lambda backend: identity)
    q._receipts.clear()
    q._identities.clear()
    def backend(callback=passing, **kwargs):
        specs = {name: q.FeatureSpec(name, "r1", probe=callback, timeout_seconds=3, **kwargs) for name in ("first", "second")}
        return SimpleNamespace(name="fixture", binary="fixture", qualification_features=lambda: specs)
    return backend, identity, tmp_path / "state"


def test_inspection_does_not_create_store_or_probe(setup):
    make, identity, root = setup
    result = q.inspect(make(), root=root)
    assert result["first"]["source"] == "unknown"
    assert not root.exists()
    assert not __import__("pathlib").Path(identity.executable).exists()


def test_success_cached_and_bundle_shared(setup):
    make, identity, root = setup
    backend = make()
    assert q.require(backend, "first", root=root).source == "auto-probed"
    assert q.require(backend, "second", root=root).facts["second"]
    assert q.require(backend, "first", root=root).ok
    assert __import__("pathlib").Path(identity.executable).read_text() == "probe\n"
    assert q.runtime_identity("fixture", "1.2.3") == identity


def test_concurrent_launches_probe_once(setup):
    make, identity, root = setup
    backend = make()
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(lambda _: q.require(backend, "first", root=root), range(5)))
    assert all(result.ok for result in results)
    assert __import__("pathlib").Path(identity.executable).read_text() == "probe\n"


def test_failure_persisted_and_refresh_required(setup):
    make, identity, root = setup
    with pytest.raises(UnsupportedCapability, match="approval was skipped"):
        q.require(make(failing), "first", root=root)
    with pytest.raises(UnsupportedCapability, match="did not establish"):
        q.require(make(), "second", root=root)
    assert not __import__("pathlib").Path(identity.executable).exists()
    assert q.require(make(), "first", root=root, refresh=True).ok


def test_timeout_fails_closed(setup):
    make, identity, root = setup
    backend = make(slow)
    specs = backend.qualification_features()
    specs["first"] = replace(specs["first"], timeout_seconds=.15)
    backend.qualification_features = lambda: specs
    started = time.monotonic()
    with pytest.raises(UnsupportedCapability, match="timed out"):
        q.require(backend, "first", root=root)
    assert time.monotonic() - started < 3


def test_runtime_failure_overrides_seed_until_refresh(setup):
    make, identity, root = setup
    backend = make(seeds={"1.2.3": {"first": True}})
    assert q.require(backend, "first", root=root).source == "seeded"
    result = q.invalidate(backend, "first", "unknown request shape", root=root, expected_version="1.2.3")
    assert result.source == "failed-at-runtime"
    with pytest.raises(UnsupportedCapability, match="unknown request shape"):
        q.require(backend, "first", root=root)
    assert q.require(backend, "first", root=root, refresh=True).source == "auto-probed"


def test_bad_hash_refused_before_state_write(setup, monkeypatch):
    make, identity, root = setup
    monkeypatch.setattr(q, "identify", lambda _: replace(identity, binary_sha256="bad"))
    with pytest.raises(UnsupportedCapability, match="hash is invalid"):
        q.require(make(), "first", root=root)
    assert not root.exists()


def test_seed_platform_and_deny_list_fail_closed(setup):
    make, identity, root = setup
    with pytest.raises(UnsupportedCapability, match="not qualified on this platform"):
        q.require(make(seeds={"1.2.3": {"first": True}}, platforms=("linux",)), "first", root=root)
    with pytest.raises(UnsupportedCapability, match="known broken"):
        q.require(make(deny_versions={"1.2.3": "known broken"}), "first", root=root, refresh=True)


def test_hash_change_cannot_reuse_receipt(setup, monkeypatch):
    make, identity, root = setup
    assert q.require(make(), "first", root=root).ok
    changed = replace(identity, binary_sha256="b" * 64)
    monkeypatch.setattr(q, "identify", lambda _: changed)
    with pytest.raises(UnsupportedCapability, match="approval was skipped"):
        q.require(make(failing), "first", root=root)
    assert q.runtime_facts("fixture", "1.2.3") is None
    # A runtime failure targets the old launch identity, never the new binary's receipt.
    q.invalidate(make(), "first", "old run mismatch", root=root, identity=identity)
    assert q.inspect(make(), root=root)["first"]["reason"] == "approval was skipped"


def test_malformed_cache_and_symlink_refused(setup):
    make, identity, root = setup
    backend = make()
    assert q.require(backend, "first", root=root).ok
    path = next((root / "qualification").glob("*.json"))
    path.write_text('{"v":1}')
    with pytest.raises(UnsupportedCapability, match="invalid qualification receipt"):
        q.require(backend, "first", root=root)
    path.unlink()
    path.symlink_to(root / "absent")
    with pytest.raises(UnsupportedCapability):
        q.require(backend, "first", root=root)


def test_unknown_platform_probes_instead_of_trusting_seed(setup):
    make, identity, root = setup
    backend = make(seeds={"1.2.3": {"first": True}}, seed_platforms=("linux",))
    assert q.require(backend, "first", root=root).source == "auto-probed"


def test_inspection_reports_seed_and_unsupported_without_writes(setup):
    make, identity, root = setup
    result = q.inspect(make(seeds={"1.2.3": {"first": True}}), root=root)
    assert result["first"]["source"] == "seeded"
    assert not root.exists()
    result = q.inspect(make(None), root=root)
    assert result["first"]["source"] == "unsupported"
    assert not root.exists()


def test_success_without_feature_evidence_fails_closed(setup):
    make, identity, root = setup
    backend = make()
    specs = backend.qualification_features()
    specs["first"] = replace(specs["first"], feature="unmeasured")
    backend.qualification_features = lambda: {"unmeasured": specs["first"]}
    with pytest.raises(UnsupportedCapability, match="did not establish"):
        q.require(backend, "unmeasured", root=root)


@pytest.mark.parametrize("field,value", [("product", "foreign"), ("format", "other"), ("product_version", "99.0.0"), ("v", True)])
def test_foreign_or_incompatible_receipt_refuses(setup, field, value):
    make, identity, root = setup
    backend = make()
    q.require(backend, "first", root=root)
    path = next((root / "qualification").glob("*.json"))
    receipt = json.loads(path.read_text())
    assert receipt["format"] == "harflow.qualification"
    receipt[field] = value
    path.write_text(json.dumps(receipt))
    with pytest.raises(UnsupportedCapability, match="invalid qualification receipt"):
        q.require(backend, "first", root=root)


def test_fifo_cache_refuses_without_blocking(setup):
    import os
    make, identity, root = setup
    backend = make()
    q.require(backend, "first", root=root)
    path = next((root / "qualification").glob("*.json"))
    path.unlink()
    os.mkfifo(path)
    started = time.monotonic()
    with pytest.raises(UnsupportedCapability):
        q.require(backend, "first", root=root)
    assert time.monotonic() - started < 1


def partial(context):
    path = __import__("pathlib").Path(context.identity.executable)
    with path.open("a") as stream:
        stream.write("partial\n")
    return q.ProbeResult(False, "first feature failed", features={
        "second": {"first": True, "second": True}}, failures={"first": "grant behavior changed"})


def partial_omitted(context):
    return q.ProbeResult(False, "requested feature failed", features={
        "second": {"first": True}}, failures={"first": "first unavailable"})


def test_partial_probe_preserves_only_established_feature_success(setup):
    make, identity, root = setup
    backend = make(partial)
    with pytest.raises(UnsupportedCapability, match="grant behavior changed"):
        q.require(backend, "first", root=root)
    result = q.require(backend, "second", root=root)
    assert result.ok and result.facts == {"second": True}
    assert q.runtime_facts("fixture", "1.2.3") == {"second": True}
    assert __import__("pathlib").Path(identity.executable).read_text() == "partial\n"
    with pytest.raises(UnsupportedCapability, match="grant behavior changed"):
        q.require(make(), "first", root=root)
    q.invalidate(backend, "second", "runtime protocol failed", root=root, identity=identity)
    with pytest.raises(UnsupportedCapability, match="runtime protocol failed"):
        q.require(backend, "second", root=root)
    assert q.runtime_facts("fixture", "1.2.3") is None


def test_unestablished_sibling_is_cached_failed_without_probe_retry(setup):
    make, identity, root = setup
    with pytest.raises(UnsupportedCapability, match="first unavailable"):
        q.require(make(partial_omitted), "first", root=root)
    with pytest.raises(UnsupportedCapability, match="did not establish"):
        q.require(make(), "second", root=root)
    assert not __import__("pathlib").Path(identity.executable).exists()


def test_runtime_tools_combine_only_independently_proven_write_tools(monkeypatch):
    from harflow_mcp.backends import claude_qualification
    monkeypatch.setattr(claude_qualification, "measured", lambda _: {
        "caller_approval_tools": ["Bash", "Read"], "caller_write_tools": ["Write", "Edit"]})
    assert claude_qualification.approval_tools("unknown") == ("Bash", "Read")
    assert claude_qualification.approval_tools("unknown", write=True) == ("Bash", "Read", "Write", "Edit")

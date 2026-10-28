"""Consequence tests for the local engine release validators."""

import hashlib
import importlib.util
from pathlib import Path
import shutil
from unittest import mock

import pytest


ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("release", ROOT / "scripts" / "release.py")
release = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(release)


def test_source_accepts_current_identity_and_exact_metadata():
    assert release.validate_source(ROOT) == "0.0.4"


def test_installed_version_info_requires_exact_engine_envelope():
    valid = {"product": "harflow", "component": "engine", "format": "harflow.version-info", "v": 1, "product_version": "0.0.2"}
    release.validate_version_info(valid, "0.0.2")
    for key, value in (("product", "other"), ("format", "wrong"), ("v", 2), ("product_version", "0.0.1")):
        candidate = dict(valid, **{key: value})
        with pytest.raises(ValueError, match="version-info"):
            release.validate_version_info(candidate, "0.0.2")
    with pytest.raises(ValueError, match="version-info"):
        release.validate_version_info({"product": "harflow"}, "0.0.2")


def test_source_rejects_tag_mismatch_before_git_lookup():
    with mock.patch.object(release.subprocess, "check_output") as check_output:
        with pytest.raises(ValueError, match="tag/version mismatch"):
            release.validate_source(ROOT, tag="v0.0.5", sha="0" * 40)
    check_output.assert_not_called()


def test_source_rejects_unexpected_entrypoint_without_running_git(tmp_path):
    for name in ("pyproject.toml", "server.json", "README.md"):
        shutil.copy(ROOT / name, tmp_path / name)
    pyproject = (tmp_path / "pyproject.toml").read_text()
    (tmp_path / "pyproject.toml").write_text(pyproject.replace("harflow-server =", "unexpected =", 1))

    with mock.patch.object(release.subprocess, "check_output") as check_output:
        with pytest.raises(ValueError, match="entrypoints"):
            release.validate_source(tmp_path, tag="v0.0.1", sha="0" * 40)
    check_output.assert_not_called()


def _pypi_metadata(wheel_bytes=b"wheel"):
    filename = "harflow_mcp-0.0.1-py3-none-any.whl"
    return {
        "info": {"name": "harflow-mcp", "version": "0.0.1"},
        "urls": [{
            "filename": filename,
            "packagetype": "bdist_wheel",
            "yanked": False,
            "url": "https://files.pythonhosted.org/packages/xx/" + filename,
            "size": len(wheel_bytes),
            "digests": {"sha256": hashlib.sha256(wheel_bytes).hexdigest()},
        }],
    }


def test_pypi_metadata_requires_the_exact_canonical_wheel(tmp_path):
    wheel = tmp_path / "harflow_mcp-0.0.1-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    assert release.validate_pypi_metadata(_pypi_metadata(), "0.0.1", wheel)["filename"] == wheel.name

    wrong = _pypi_metadata()
    wrong["urls"][0]["url"] = "http://files.pythonhosted.org/packages/xx/" + wheel.name
    with pytest.raises(ValueError, match="invalid canonical wheel metadata"):
        release.validate_pypi_metadata(wrong, "0.0.1", wheel)

    mismatch = _pypi_metadata(b"other")
    with pytest.raises(ValueError, match="published wheel differs"):
        release.validate_pypi_metadata(mismatch, "0.0.1", wheel)

    yanked = _pypi_metadata()
    yanked["urls"][0]["yanked"] = True
    assert release.validate_pypi_metadata(yanked, "0.0.1")["filename"] == wheel.name


def test_wait_for_pypi_retries_without_falling_back_to_latest(tmp_path):
    wheel = tmp_path / "harflow_mcp-0.0.1-py3-none-any.whl"
    wheel.write_bytes(b"wheel")
    calls = []
    sleeps = []

    def fetch():
        calls.append(len(calls))
        if len(calls) < 3:
            raise OSError("not published yet")
        return _pypi_metadata()

    result = release.wait_for_pypi("0.0.1", wheel, fetch=fetch, sleep=sleeps.append, attempts=3)
    assert result["filename"] == wheel.name
    assert calls == [0, 1, 2]
    assert sleeps == [10, 10]


def test_wait_for_pypi_reports_unavailable_exact_release():
    with pytest.raises(ValueError, match="exact PyPI release unavailable"):
        release.wait_for_pypi("0.0.1", fetch=lambda: {"info": {}, "urls": []}, sleep=lambda _: None, attempts=2)


def test_checksums_reject_changed_artifact_bytes(tmp_path):
    wheel = tmp_path / "harflow_mcp-0.0.1-py3-none-any.whl"
    sdist = tmp_path / "harflow_mcp-0.0.1.tar.gz"
    wheel.write_bytes(b"wheel")
    sdist.write_bytes(b"sdist")
    (tmp_path / "SHA256SUMS").write_text(
        f"{hashlib.sha256(wheel.read_bytes()).hexdigest()}  {wheel.name}\n"
        f"{hashlib.sha256(sdist.read_bytes()).hexdigest()}  {sdist.name}\n"
    )
    release.verify_checksums(tmp_path)
    wheel.write_bytes(b"changed")
    with pytest.raises(ValueError, match="artifact checksum mismatch"):
        release.verify_checksums(tmp_path)


def test_artifacts_reject_unexpected_build_sentinel_before_unpacking(tmp_path):
    for name in ("pyproject.toml", "server.json", "README.md", "release-build-constraints.txt"):
        shutil.copy(ROOT / name, tmp_path / name)
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "harflow_mcp-0.0.1-py3-none-any.whl").write_bytes(b"wheel")
    (dist / "harflow_mcp-0.0.1.tar.gz").write_bytes(b"sdist")
    (dist / ".gitignore").write_text("*")
    with pytest.raises(ValueError, match="unexpected distribution files"):
        release.validate_artifacts(tmp_path, dist)


def test_publisher_digest_mismatch_fails_before_execution(tmp_path):
    publisher = tmp_path / "mcp-publisher"
    publisher.write_bytes(b"untrusted publisher")
    with pytest.raises(ValueError, match="publisher checksum mismatch"):
        release.verify_publisher(publisher)

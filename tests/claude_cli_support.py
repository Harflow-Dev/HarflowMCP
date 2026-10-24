"""Shared setup for opt-in Claude CLI certification tests: version gating and an isolated environment."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from harflow_mcp.backends import claude_qualification as qualification


def require_claude(feature: str, monkeypatch: pytest.MonkeyPatch) -> str:
    """The installed version, which must be measured for `feature` or named by HARFLOW_QUALIFY_CLAUDE_VERSION.

    A candidate version is measured as if it had the newest measured row; support in the product is unchanged."""
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    observed = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    version = qualification.parse_version(observed)
    if version is not None and not qualification.supports(version, feature) and os.environ.get("HARFLOW_QUALIFY_CLAUDE_VERSION") == version:
        newest = max((v for v, row in qualification.MEASURED.items() if row.get(feature)), key=lambda v: tuple(map(int, v.split("."))))
        monkeypatch.setitem(qualification.MEASURED, version, dict(qualification.MEASURED[newest]))
    if version is None or not qualification.supports(version, feature):
        pytest.skip(f"Installed Claude {observed!r} is not in the measured set; set HARFLOW_QUALIFY_CLAUDE_VERSION to measure it")
    return version


def isolate(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, base_url: str) -> None:
    for key in tuple(os.environ):
        if key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) or key == "CLAUDECODE":
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "config"))
    monkeypatch.setenv("ANTHROPIC_API_KEY", "local-fake-key")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", base_url)
    monkeypatch.setenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")

"""Invalid state-root startup is isolated, deterministic, and side-effect free."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from harflow_mcp.wire_contracts import PRODUCT_VERSION


def _entrypoint(name: str) -> Path:
    path = Path(sys.executable).with_name(name)
    assert path.is_file(), f"installed entrypoint missing: {path}"
    return path


def _environment(tmp_path: Path, *, state_root: str) -> tuple[dict[str, str], Path]:
    home = tmp_path / "home"
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    environment = dict(
        os.environ,
        HOME=str(home),
        HARFLOW_STATE_ROOT=state_root,
        PATH="/usr/bin:/bin",
        CODEX_HOME=str(tmp_path / "codex-home"),
        XDG_CONFIG_HOME=str(tmp_path / "xdg-config"),
        CLAUDE_CONFIG_DIR=str(tmp_path / "claude-config"),
        OPENCODE_CONFIG_DIR=str(tmp_path / "opencode-config"),
    )
    environment.pop("HARFLOW_TASK_ID", None)
    environment.pop("PB_TASK_ID", None)
    return environment, cwd


def _run(tmp_path: Path, argv: list[str], *, state_root: str) -> subprocess.CompletedProcess[str]:
    environment, cwd = _environment(tmp_path, state_root=state_root)
    return subprocess.run(
        argv,
        cwd=cwd,
        env=environment,
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )


def _assert_no_state(tmp_path: Path, result: subprocess.CompletedProcess[str]) -> None:
    assert result.returncode == 2, result.stderr
    assert "Traceback" not in result.stderr
    assert not (tmp_path / "home").exists()
    assert not any((tmp_path / "cwd").iterdir())
    assert not list(tmp_path.rglob(".harflow"))
    assert not list(tmp_path.rglob(".polybridge"))


@pytest.mark.parametrize("state_root", ["", "relative-root"])
@pytest.mark.parametrize(
    ("name", "arguments", "format"),
    [
        ("harflow-ctl", ["list", "--json"], "harflow.ctl"),
        ("harflow-setup", ["--status", "--json"], "harflow.setup"),
        ("harflow-ctl", ["version-info", "--json"], "harflow.version-info"),
    ],
)
def test_invalid_state_root_json_entrypoints_fail_before_state_or_client_setup(
    tmp_path: Path, state_root: str, name: str, arguments: list[str], format: str
) -> None:
    result = _run(tmp_path, [str(_entrypoint(name)), *arguments], state_root=state_root)
    _assert_no_state(tmp_path, result)
    document = json.loads(result.stdout)
    assert document["product"] == "harflow"
    assert document["format"] == format
    assert document["v"] == 1
    assert document["product_version"] == PRODUCT_VERSION
    assert document["error"]["code"] == "usage"
    assert "state root" in document["error"]["message"].lower()
    assert "state_root" not in document


@pytest.mark.parametrize("state_root", ["", "relative-root"])
def test_invalid_state_root_plain_ctl_fails_with_stderr_only(tmp_path: Path, state_root: str) -> None:
    result = _run(tmp_path, [str(_entrypoint("harflow-ctl")), "list"], state_root=state_root)
    _assert_no_state(tmp_path, result)
    assert result.stdout == ""
    assert "state root" in result.stderr.lower()


@pytest.mark.parametrize("name", ["harflow-ctl", "harflow-setup", "harflow-server"])
def test_help_remains_available_with_invalid_state_root(tmp_path: Path, name: str) -> None:
    result = _run(tmp_path, [str(_entrypoint(name)), "--help"], state_root="relative-root")
    assert result.returncode == 0, result.stderr
    assert result.stdout or result.stderr
    assert "Traceback" not in result.stderr
    assert not (tmp_path / "home").exists()
    assert not any((tmp_path / "cwd").iterdir())
    assert not list(tmp_path.rglob(".harflow"))


def test_invalid_state_root_server_exits_before_mcp_or_store_initialization(tmp_path: Path) -> None:
    result = _run(tmp_path, [str(_entrypoint("harflow-server"))], state_root="relative-root")
    _assert_no_state(tmp_path, result)
    assert result.stdout == ""
    assert "state root" in result.stderr.lower()


def test_valid_custom_root_version_info_is_side_effect_free(tmp_path: Path) -> None:
    selected = tmp_path / "custom-state"
    result = _run(tmp_path, [str(_entrypoint("harflow-ctl")), "version-info", "--json"], state_root=str(selected))
    assert result.returncode == 0, result.stderr
    document = json.loads(result.stdout)
    assert document["state_root"] == str(selected)
    assert not selected.exists()
    assert not (tmp_path / "home").exists()
    assert not any((tmp_path / "cwd").iterdir())


def test_invalid_lazy_version_info_does_not_import_stateful_modules(tmp_path: Path) -> None:
    trap = tmp_path / "sitecustomize.py"
    marker = tmp_path / "imports"
    trap.write_text(
        "import os\n"
        "from pathlib import Path\n"
        "Path(os.environ['HARFLOW_IMPORT_TRAP_MARKER']).write_text('loaded')\n"
        "blocked = {'harflow_mcp.ctl','harflow_mcp.control','harflow_mcp.store','harflow_mcp.tasks','harflow_mcp.server','harflow_mcp.backends','harflow_mcp.clients'}\n"
        "class Trap:\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname in blocked: raise RuntimeError('stateful import during invalid version-info')\n"
        "        return None\n"
        "import sys\n"
        "sys.meta_path.insert(0, Trap())\n"
    )
    environment, cwd = _environment(tmp_path, state_root="relative-root")
    environment["PYTHONPATH"] = str(tmp_path)
    environment["HARFLOW_IMPORT_TRAP_MARKER"] = str(marker)
    result = subprocess.run(
        [str(_entrypoint("harflow-ctl")), "version-info", "--json"],
        cwd=cwd,
        env=environment,
        stdin=subprocess.DEVNULL,
        text=True,
        capture_output=True,
        check=False,
        timeout=20,
    )
    _assert_no_state(tmp_path, result)
    assert marker.read_text() == "loaded"
    document = json.loads(result.stdout)
    assert document["error"]["code"] == "usage"

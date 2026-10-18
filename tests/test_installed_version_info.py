"""The installed lazy CLI path must answer version-info without stateful imports."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path


def test_installed_version_info_success_and_usage_are_state_free(tmp_path: Path):
    entrypoint = Path(sys.executable).with_name("harflow-ctl")
    assert entrypoint.is_file(), f"installed entrypoint missing: {entrypoint}"
    trap = tmp_path / "sitecustomize.py"
    trap.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "Path(__import__('os').environ['HARFLOW_IMPORT_TRAP_MARKER']).write_text('loaded')\n"
        "blocked = {'harflow_mcp.ctl','harflow_mcp.control','harflow_mcp.store','harflow_mcp.tasks','harflow_mcp.server','harflow_mcp.backends','harflow_mcp.detached','harflow_mcp.takeover'}\n"
        "class Trap:\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname in blocked: raise RuntimeError('stateful import during version-info')\n"
        "        return None\n"
        "sys.meta_path.insert(0, Trap())\n"
    )
    home = tmp_path / "home"
    marker = tmp_path / "trap-loaded"
    env = dict(os.environ, HOME=str(home), PYTHONPATH=str(tmp_path), HARFLOW_IMPORT_TRAP_MARKER=str(marker), HARFLOW_TASK_ID="stale", PB_TASK_ID="stale")
    ok = subprocess.run([str(entrypoint), "version-info", "--json"], env=env, text=True, capture_output=True, check=False)
    assert ok.returncode == 0, ok.stderr
    assert marker.read_text() == "loaded"
    document = json.loads(ok.stdout)
    assert document["product"] == "harflow"
    assert document["format"] == "harflow.version-info"
    assert document["v"] == 1
    metadata = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert document["product_version"] == metadata["project"]["version"]
    assert Path(document["state_root"]).name == ".harflow"
    assert not home.exists()

    marker.unlink()
    bad = subprocess.run([str(entrypoint), "version-info", "--json", "extra"], env=env, text=True, capture_output=True, check=False)
    assert bad.returncode != 0
    assert marker.read_text() == "loaded"
    usage = json.loads(bad.stdout)
    assert usage["format"] == "harflow.version-info"
    assert usage["error"]["code"] == "usage"
    assert not home.exists()

"""Drives the real client CLIs against sandboxed config directories.

Opt-in, and deliberately *not* under `PB_INTEGRATION`, which means runs that spend money. These
spend nothing — but they depend on optional external binaries, and on those binaries' current flag
and output shapes, which is exactly what the default suite promises not to.

    PB_CLI_INTEGRATION=1 uv run pytest -m cli_integration

Everything the fake-runner tests assert about *policy* is asserted there. What can only be learned
here is whether the argv still means what it meant when it was measured.
"""

from __future__ import annotations

import json
import os
import shutil
import tomllib
from pathlib import Path

import pytest

from polybridge.clients import Registration, run_cli
from polybridge.clients.claude_code import ClaudeCodeClient
from polybridge.clients.codex import CodexClient
from polybridge.clients.opencode import OpencodeClient

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(
        not os.environ.get("PB_CLI_INTEGRATION"),
        reason="drives real client CLIs; opt in with PB_CLI_INTEGRATION=1",
    ),
]

SERVER = "/opt/polybridge/bin/polybridge-server"
REGISTRATION = Registration(key="polybridge", command=SERVER, path_env="/sandbox/bin:/usr/bin")


@pytest.fixture
def sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every client at a throwaway home, so a regression here cannot touch real config.

    HOME is redirected too, not just the client-specific variable: `run_cli` runs commands from the
    user's home directory, and a client that ignored its override would otherwise land in the real
    one.
    """
    home = tmp_path / "home"
    # Created rather than left to the CLIs: `codex mcp add` refuses an explicit CODEX_HOME that does
    # not exist (measured). It does create its *default* `~/.codex`, so this is a sandbox artefact.
    for directory in (home, home / "claude", home / "codex", home / "config"):
        directory.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home / "claude"))
    monkeypatch.setenv("CODEX_HOME", str(home / "codex"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home / "config"))
    return home


def needs(binary: str) -> None:
    if shutil.which(binary) is None:
        pytest.skip(f"`{binary}` is not installed")


def test_claude_code_writes_a_stdio_entry(sandbox: Path) -> None:
    needs("claude")

    result = ClaudeCodeClient().apply(REGISTRATION, run_cli)

    assert result.status == "applied", result
    config = json.loads((sandbox / "claude" / ".claude.json").read_text())
    assert config["mcpServers"]["polybridge"]["command"] == SERVER
    assert config["mcpServers"]["polybridge"]["env"]["PATH"] == REGISTRATION.path_env


def test_claude_code_replaces_rather_than_refusing_on_a_second_run(sandbox: Path) -> None:
    """The measured reason the replacement policy exists: `add` alone exits 1 the second time."""
    needs("claude")
    client = ClaudeCodeClient()
    assert client.apply(REGISTRATION, run_cli).status == "applied"
    moved = Registration(key="polybridge", command=SERVER, path_env="/moved:/usr/bin")

    result = client.apply(moved, run_cli)

    assert result.status == "applied", result
    assert "replaced" in result.detail
    servers = json.loads((sandbox / "claude" / ".claude.json").read_text())["mcpServers"]
    assert servers["polybridge"]["env"]["PATH"] == "/moved:/usr/bin"
    assert len(servers) == 1, "the remove-then-add should not leave a duplicate behind"


def test_codex_writes_the_entry_and_keeps_the_rest_of_the_file(sandbox: Path) -> None:
    needs("codex")
    codex_home = sandbox / "codex"
    (codex_home / "config.toml").write_text('# hand written\nmodel = "gpt-5"\n')

    result = CodexClient().apply(REGISTRATION, run_cli)

    assert result.status == "applied", result
    raw = (codex_home / "config.toml").read_text()
    assert "# hand written" in raw
    config = tomllib.loads(raw)
    assert config["mcp_servers"]["polybridge"]["command"] == SERVER
    assert config["mcp_servers"]["polybridge"]["env"]["PATH"] == REGISTRATION.path_env


def test_codex_overwrites_on_a_second_run(sandbox: Path) -> None:
    """The first add is asserted too: without it a silently-failing first run would pass as an
    overwrite when it was really an ordinary first write."""
    needs("codex")
    client = CodexClient()
    config = sandbox / "codex" / "config.toml"

    assert client.apply(REGISTRATION, run_cli).status == "applied"
    assert tomllib.loads(config.read_text())["mcp_servers"]["polybridge"]["env"]["PATH"] == (
        REGISTRATION.path_env
    )

    result = client.apply(
        Registration(key="polybridge", command=SERVER, path_env="/moved"), run_cli
    )

    assert result.status == "applied", result
    servers = tomllib.loads(config.read_text())["mcp_servers"]
    assert servers["polybridge"]["env"]["PATH"] == "/moved"
    assert len(servers) == 1, "it should have replaced the entry, not added a second one"


def test_opencode_writes_its_own_shape(sandbox: Path) -> None:
    """Its schema differs from everyone else's — which is why it writes the file, not us."""
    needs("opencode")

    result = OpencodeClient().apply(REGISTRATION, run_cli)

    assert result.status == "applied", result
    written = json.loads((sandbox / "config" / "opencode" / "opencode.jsonc").read_text())
    entry = written["mcp"]["polybridge"]
    assert entry["type"] == "local"
    assert entry["command"] == [SERVER]
    assert entry["environment"] == {"PATH": REGISTRATION.path_env}


def test_opencode_overwrites_on_a_second_run(sandbox: Path) -> None:
    needs("opencode")
    client = OpencodeClient()
    config = sandbox / "config" / "opencode" / "opencode.jsonc"

    assert client.apply(REGISTRATION, run_cli).status == "applied"
    assert json.loads(config.read_text())["mcp"]["polybridge"]["environment"] == {
        "PATH": REGISTRATION.path_env
    }

    result = client.apply(
        Registration(key="polybridge", command=SERVER, path_env="/moved"), run_cli
    )

    assert result.status == "applied", result
    servers = json.loads(config.read_text())["mcp"]
    assert servers["polybridge"]["environment"] == {"PATH": "/moved"}
    assert len(servers) == 1, "it should have replaced the entry, not added a second one"

"""What gets registered, how it is selected, and what the command line reports.

`install.sh` has claimed since day one that setup is "covered by tests"; until now it was not.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from polybridge import setup_client
from polybridge.clients import Result, SetupError

EVERYTHING = {"polybridge-server", "claude", "codex", "opencode", "vibe", "git"}


class FakeCli:
    """Stands in for every client CLI, so no test here can reach a real config.

    `shutil.which` being patched only changes what *looks* installed: the argv still names the bare
    binary, so without this a test that "applied" to Codex ran the real `codex mcp add` against the
    real `~/.codex/config.toml` — which is exactly what one did, from 2026-08-12 until this fixture.
    """

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.replies: dict[tuple[str, ...], tuple[int, str]] = {}

    def reply(self, prefix: tuple[str, ...], returncode: int, stdout: str) -> None:
        self.replies[prefix] = (returncode, stdout)

    def __call__(self, argv, **kwargs):
        argv = list(argv)
        self.calls.append(argv)
        returncode, stdout = next(
            (reply for prefix, reply in self.replies.items() if tuple(argv[: len(prefix)]) == prefix),
            (0, ""),
        )
        return subprocess.CompletedProcess(argv, returncode, stdout, "")


@pytest.fixture(autouse=True)
def fake_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeCli:
    """Every client CLI faked, and every config location a client reads pointed into tmp_path."""
    fake = FakeCli()
    fake.reply(("codex", "mcp", "list", "--json"), 0, "[]")
    monkeypatch.setattr("polybridge.clients.base.subprocess.run", fake)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home / "claude"))
    monkeypatch.setenv("CODEX_HOME", str(home / "codex"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home / "config"))
    monkeypatch.setenv("VIBE_HOME", str(home / "vibe"))
    return fake


@pytest.fixture
def which(monkeypatch: pytest.MonkeyPatch):
    """Control what looks installed. Patching `shutil.which` covers setup and every client."""

    def install(*names: str) -> None:
        available = set(names)
        monkeypatch.setattr(
            "shutil.which", lambda name: f"/fake/bin/{name}" if name in available else None
        )

    return install


def run(tmp_path: Path, *argv: str) -> int:
    """Run the command line against a desktop config inside tmp_path."""
    return setup_client.main(
        ["--desktop-config", str(tmp_path / "claude_desktop_config.json"), *argv]
    )


# --- what gets registered ---------------------------------------------------------------------


def test_path_env_leads_with_the_servers_own_directory() -> None:
    path_env = setup_client.build_path_env("/opt/tools/polybridge-server", "/usr/local/bin/claude")

    assert path_env.split(":")[0] == "/opt/tools"
    assert "/usr/local/bin" in path_env.split(":")


def test_path_env_keeps_symlinks_unresolved(tmp_path: Path) -> None:
    """Claude Code's binary points into a versioned directory; resolving it pins PATH to today's."""
    real = tmp_path / "versions" / "2.1.220"
    real.mkdir(parents=True)
    (real / "claude").write_text("")
    link = tmp_path / "bin"
    link.parent.mkdir(exist_ok=True)
    link.symlink_to(real)

    path_env = setup_client.build_path_env("/opt/bin/polybridge-server", str(link / "claude"))

    assert str(link) in path_env.split(":")
    assert str(real) not in path_env.split(":")


def test_path_env_drops_absent_dependencies_and_repeats() -> None:
    path_env = setup_client.build_path_env("/usr/bin/polybridge-server", None, "/usr/bin/git")

    assert path_env.split(":").count("/usr/bin") == 1


def test_missing_server_binary_is_an_error_that_says_how_to_fix_it(which) -> None:
    which()

    with pytest.raises(SetupError) as excinfo:
        setup_client.resolve_server_command()

    assert "uv tool install" in str(excinfo.value)
    assert "--no-cache" in str(excinfo.value)


def test_an_install_inside_a_virtualenv_is_recognised_as_ephemeral() -> None:
    assert setup_client.is_ephemeral_install("/repo/.venv/bin/polybridge-server")
    assert not setup_client.is_ephemeral_install("/Users/x/.local/bin/polybridge-server")


# --- selection ----------------------------------------------------------------------------------


def test_a_dry_run_changes_nothing_and_previews_every_client(
    tmp_path: Path, which, capsys
) -> None:
    which(*EVERYTHING)

    code = run(tmp_path, "--dry-run")

    out = capsys.readouterr().out
    assert code == 0
    assert not (tmp_path / "claude_desktop_config.json").exists()
    assert "would write" in out
    for binary in ("claude", "codex", "opencode", "vibe"):
        assert f"{binary} mcp add polybridge" in out


def test_clients_that_are_not_installed_are_skipped_not_failed(
    tmp_path: Path, which, capsys
) -> None:
    which("polybridge-server", "git")

    code = run(tmp_path)

    out = capsys.readouterr().out
    assert code == 0
    assert json.loads((tmp_path / "claude_desktop_config.json").read_text())["mcpServers"]
    assert out.count("skipped") == 4


def test_asking_for_a_client_that_is_not_installed_is_an_error(
    tmp_path: Path, which, capsys
) -> None:
    which("polybridge-server")

    code = setup_client.main(["--client", "codex"])

    assert code == 1
    assert "not on PATH" in capsys.readouterr().out


def test_an_unknown_client_name_is_rejected(tmp_path: Path, which, capsys) -> None:
    which(*EVERYTHING)

    code = run(tmp_path, "--client", "cursor")

    assert code == 1
    assert "unknown client" in capsys.readouterr().err


def test_desktop_config_is_rejected_when_the_desktop_client_is_not_selected(
    tmp_path: Path, which, capsys
) -> None:
    """It used to be `--config`, which said nothing about which client it meant."""
    which(*EVERYTHING)

    code = run(tmp_path, "--client", "codex", "--dry-run")

    assert code == 1
    assert "Claude desktop app, which is not selected" in capsys.readouterr().err


# --- reporting ------------------------------------------------------------------------------------


def test_post_apply_guidance_comes_from_the_clients_not_from_a_name_check(
    tmp_path: Path, which, capsys
) -> None:
    """Which clients need a restart is theirs to say; setup only prints what it is told."""
    which("polybridge-server", "codex", "git")

    run(tmp_path)

    out = capsys.readouterr().out
    assert "Claude desktop app: restart it" in out
    assert "Codex: no restart needed" in out


def test_an_empty_report_does_not_raise(capsys) -> None:
    setup_client._report([])

    assert capsys.readouterr().out == ""


def test_a_result_from_an_unregistered_client_is_still_printable(capsys) -> None:
    """Reporting must not be the thing that crashes when something upstream is wrong."""
    setup_client._report([Result("something-new", "failed", "why")])

    assert "something-new" in capsys.readouterr().out


def test_failures_print_the_command_that_was_run(capsys) -> None:
    setup_client._report(
        [Result("codex", "failed", "add command failed (exit 2)", steps=("codex mcp add x",))]
    )

    out = capsys.readouterr().out
    assert "ran: codex mcp add x" in out


def test_a_successful_report_does_not_repeat_the_command(capsys) -> None:
    setup_client._report([Result("codex", "applied", "add command succeeded", steps=("codex …",))])

    assert "ran:" not in capsys.readouterr().out


# --- actions: install (default), --status, --uninstall -------------------------------------------

DESKTOP = "claude_desktop_config.json"
ROW_KEYS = {"key", "available", "installed", "command", "current", "action", "error", "notes"}


def run_json(tmp_path: Path, capsys, *argv: str, desktop: bool = True) -> tuple[int, dict]:
    """`desktop=False` for a `--client` selection without the desktop app, which rightly rejects
    `--desktop-config`; HOME is inside tmp_path either way."""
    code = run(tmp_path, "--json", *argv) if desktop else setup_client.main(["--json", *argv])
    out = capsys.readouterr().out
    return code, json.loads(out)


def nothing_registered(fake_cli: FakeCli) -> None:
    """Each CLI's measured "there was nothing to remove" reply."""
    fake_cli.reply(("claude", "mcp", "remove"), 1, 'No MCP server named "polybridge" in user scope')
    fake_cli.reply(("codex", "mcp", "remove"), 0, "No MCP server named 'polybridge' found.")
    fake_cli.reply(
        ("vibe", "mcp", "remove"), 0, "MCP server `polybridge` is not configured in the user config."
    )


def rows(document: dict) -> dict[str, dict]:
    return {row["key"]: row for row in document["clients"]}


@pytest.mark.parametrize(
    "argv",
    [["--status", "--uninstall"], ["--status", "--dry-run"], ["--uninstall", "--dry-run"]],
    ids=["status+uninstall", "status+dry-run", "uninstall+dry-run"],
)
def test_conflicting_actions_are_rejected_before_anything_runs(
    tmp_path: Path, which, fake_cli, capsys, argv: list[str]
) -> None:
    which(*EVERYTHING)

    with pytest.raises(SystemExit) as excinfo:
        run(tmp_path, *argv)

    assert excinfo.value.code == 2
    assert fake_cli.calls == []
    assert not (tmp_path / DESKTOP).exists()


def test_dry_run_rejection_says_what_it_applies_to(tmp_path: Path, which, capsys) -> None:
    which(*EVERYTHING)

    with pytest.raises(SystemExit):
        run(tmp_path, "--status", "--dry-run")

    assert "--dry-run only applies to install" in capsys.readouterr().err


def test_status_does_not_need_the_server_binary(tmp_path: Path, which, capsys) -> None:
    which("claude", "codex", "opencode", "vibe")

    code, document = run_json(tmp_path, capsys, "--status")

    assert code == 0
    assert document["server_path"] is None
    assert all(row["current"] is None for row in document["clients"])


def test_uninstall_does_not_need_the_server_binary(
    tmp_path: Path, which, fake_cli, capsys
) -> None:
    which("claude", "codex", "opencode", "vibe")
    nothing_registered(fake_cli)

    code = run(tmp_path, "--uninstall")

    assert code == 0
    assert "not on PATH" not in capsys.readouterr().err


def test_install_still_requires_the_server_binary(tmp_path: Path, which, capsys) -> None:
    which("codex")

    assert run(tmp_path) == 1
    assert "uv tool install" in capsys.readouterr().err


def test_status_json_is_version_one_with_exactly_the_documented_fields(
    tmp_path: Path, which, capsys
) -> None:
    """The Mac app reads this. A field added, renamed or retyped here must bump `v`."""
    which(*EVERYTHING)

    code, document = run_json(tmp_path, capsys, "--status")

    assert code == 0
    assert set(document) == {"v", "server_path", "clients"}
    assert document["v"] == 1
    assert document["server_path"] == "/fake/bin/polybridge-server"
    assert [row["key"] for row in document["clients"]] == [
        "claude-desktop",
        "claude-code",
        "codex",
        "opencode",
        "vibe",
    ]
    for row in document["clients"]:
        assert set(row) == ROW_KEYS
        assert isinstance(row["key"], str)
        assert isinstance(row["available"], bool)
        assert row["installed"] in (True, False, None)
        assert row["command"] is None or isinstance(row["command"], str)
        assert row["current"] in (True, False, None)
        assert row["action"] is None, "--status acts on nothing"
        assert row["error"] is None or isinstance(row["error"], str)
        assert isinstance(row["notes"], list) and all(isinstance(n, str) for n in row["notes"])


def test_status_reports_an_install_as_current_and_a_stale_one_as_not(
    tmp_path: Path, which, capsys
) -> None:
    which(*EVERYTHING)
    assert run(tmp_path, "--client", "claude-desktop") == 0
    capsys.readouterr()

    _, document = run_json(tmp_path, capsys, "--status", "--client", "claude-desktop")
    row = rows(document)["claude-desktop"]
    assert (row["installed"], row["command"], row["current"]) == (
        True,
        "/fake/bin/polybridge-server",
        True,
    )

    config = json.loads((tmp_path / DESKTOP).read_text())
    config["mcpServers"]["polybridge"]["env"]["PATH"] = "/somewhere/else"
    (tmp_path / DESKTOP).write_text(json.dumps(config))

    _, document = run_json(tmp_path, capsys, "--status", "--client", "claude-desktop")
    assert rows(document)["claude-desktop"]["current"] is False


def test_status_exits_zero_with_every_client_absent(tmp_path: Path, which, capsys) -> None:
    which()

    code, document = run_json(tmp_path, capsys, "--status")

    assert code == 0
    by_key = rows(document)
    assert by_key["codex"]["available"] is False
    assert by_key["codex"]["installed"] is None
    assert by_key["claude-desktop"]["installed"] is False


def test_status_exits_one_when_an_inspect_errors(tmp_path: Path, which, capsys) -> None:
    which(*EVERYTHING)
    (tmp_path / DESKTOP).write_text("{not json")

    code, document = run_json(tmp_path, capsys, "--status")

    assert code == 1
    assert "not valid JSON" in rows(document)["claude-desktop"]["error"]


def test_status_runs_nothing_but_codex_s_listing(tmp_path: Path, which, fake_cli, capsys) -> None:
    """Read-only: no add, no remove, and never `claude mcp get`, which launches the server."""
    which(*EVERYTHING)

    run(tmp_path, "--status")

    assert fake_cli.calls == [["codex", "mcp", "list", "--json"]]


def test_status_table_is_readable(tmp_path: Path, which, capsys) -> None:
    which("polybridge-server", "codex", "git")

    code = run(tmp_path, "--status")

    out = capsys.readouterr().out
    assert code == 0
    assert "server:  /fake/bin/polybridge-server" in out
    assert "not installed" in out
    assert "unavailable" in out


def test_uninstall_removes_from_the_desktop_app_and_says_to_restart(
    tmp_path: Path, which, capsys
) -> None:
    which(*EVERYTHING)
    run(tmp_path, "--client", "claude-desktop")
    capsys.readouterr()

    code = run(tmp_path, "--uninstall", "--client", "claude-desktop")

    out = capsys.readouterr().out
    assert code == 0
    assert "removed" in out
    assert "Claude desktop app: restart it" in out
    assert json.loads((tmp_path / DESKTOP).read_text())["mcpServers"] == {}


def test_uninstall_treats_nothing_to_remove_as_success(
    tmp_path: Path, which, fake_cli, capsys
) -> None:
    which(*EVERYTHING)
    nothing_registered(fake_cli)

    code, document = run_json(tmp_path, capsys, "--uninstall")

    assert code == 0
    assert {row["key"]: row["action"] for row in document["clients"]} == {
        "claude-desktop": "not_installed",
        "claude-code": "not_installed",
        "codex": "not_installed",
        "opencode": "not_installed",
        "vibe": "not_installed",
    }
    assert document["server_path"] is None


def test_uninstall_leaves_opencode_to_the_user_and_still_exits_zero(
    tmp_path: Path, which, capsys
) -> None:
    which("opencode")
    config = tmp_path / "home" / "config" / "opencode" / "opencode.jsonc"
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({"mcp": {"polybridge": {"type": "local", "command": ["/x"]}}}))

    code, document = run_json(
        tmp_path, capsys, "--uninstall", "--client", "opencode", desktop=False
    )

    row = rows(document)["opencode"]
    assert code == 0
    assert row["action"] == "skipped"
    assert row["installed"] is True, "nothing was removed, and the row must say so"
    assert any("manually" in note and str(config) in note for note in row["notes"])


def test_uninstall_exits_one_on_a_remove_failure(tmp_path: Path, which, fake_cli, capsys) -> None:
    which("codex")
    fake_cli.reply(("codex", "mcp", "remove"), 1, "permission denied")

    code, document = run_json(
        tmp_path, capsys, "--uninstall", "--client", "codex", desktop=False
    )

    row = rows(document)["codex"]
    assert code == 1
    assert row["action"] == "failed"
    assert row["error"] == "remove command failed (exit 1)"
    assert "ran: codex mcp remove polybridge" in row["notes"]


def test_uninstall_exits_one_on_an_unknown_outcome(tmp_path: Path, which, fake_cli, capsys) -> None:
    which("codex")
    fake_cli.reply(("codex", "mcp", "remove"), 0, "something new")

    code = setup_client.main(["--uninstall", "--client", "codex"])

    assert code == 1


def test_uninstall_of_a_named_client_that_is_not_installed_is_an_error(
    tmp_path: Path, which, capsys
) -> None:
    which()

    assert setup_client.main(["--uninstall", "--client", "codex"]) == 1
    assert "not on PATH" in capsys.readouterr().out


def test_install_json_is_the_only_thing_on_stdout(tmp_path: Path, which, capsys) -> None:
    which(*EVERYTHING)

    code, document = run_json(tmp_path, capsys, "--client", "claude-desktop,codex")

    by_key = rows(document)
    assert code == 0
    assert document["server_path"] == "/fake/bin/polybridge-server"
    assert by_key["claude-desktop"]["action"] == "applied"
    assert by_key["claude-desktop"]["installed"] is True
    assert by_key["claude-desktop"]["current"] is True
    assert by_key["codex"]["action"] == "applied"


def test_install_dry_run_json_describes_the_unchanged_state(tmp_path: Path, which, capsys) -> None:
    which(*EVERYTHING)

    code, document = run_json(tmp_path, capsys, "--dry-run", "--client", "claude-desktop")

    row = rows(document)["claude-desktop"]
    assert code == 0
    assert row["action"] == "previewed"
    assert row["installed"] is False
    assert not (tmp_path / DESKTOP).exists()


def test_json_document_puts_a_failed_actions_detail_in_error() -> None:
    from polybridge.clients import Inspection

    document = setup_client.json_document(
        "/s",
        "/p",
        [Inspection("codex", None, error="list broke")],
        [Result("codex", "unknown", "timed out", steps=("codex mcp remove polybridge",))],
    )

    (row,) = document["clients"]
    assert row["error"] == "timed out"
    assert "inspect: list broke" in row["notes"]
    assert "ran: codex mcp remove polybridge" in row["notes"]


def test_the_report_keeps_its_columns_aligned_for_long_statuses(capsys) -> None:
    setup_client._report(
        [Result("codex", "not_installed", "nothing registered"), Result("vibe", "removed", "ok")]
    )

    lines = capsys.readouterr().out.splitlines()
    assert lines[0].index("nothing registered") == lines[1].index("ok")

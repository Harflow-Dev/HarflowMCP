"""What gets registered, how it is selected, and what the command line reports.

`install.sh` has claimed since day one that setup is "covered by tests"; until now it was not.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polybridge import setup_client
from polybridge.clients import Result, SetupError

EVERYTHING = {"polybridge-server", "claude", "codex", "opencode", "git"}


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
    for binary in ("claude", "codex", "opencode"):
        assert f"{binary} mcp add polybridge" in out


def test_clients_that_are_not_installed_are_skipped_not_failed(
    tmp_path: Path, which, capsys
) -> None:
    which("polybridge-server", "git")

    code = run(tmp_path)

    out = capsys.readouterr().out
    assert code == 0
    assert json.loads((tmp_path / "claude_desktop_config.json").read_text())["mcpServers"]
    assert out.count("skipped") == 3


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

"""The client seam: argv shapes, replacement policy, and outcomes that never overclaim.

Every CLI response scripted below matches something measured from the real binary — the exact
"already exists" and "No MCP server named" wordings especially, since the replacement policy turns
on them.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from polybridge import clients
from polybridge.clients import Registration, Result, RunResult, SetupError, run_cli
from polybridge.clients.base import CliClient
from polybridge.clients.claude_code import ClaudeCodeClient
from polybridge.clients.codex import CodexClient
from polybridge.clients.desktop import DesktopClient
from polybridge.clients.opencode import OpencodeClient

REGISTRATION = Registration(key="polybridge", command="/opt/bin/polybridge-server", path_env="/a:/b")

ALREADY_EXISTS = "MCP server polybridge already exists in user config"
NOT_FOUND = 'No MCP server named "polybridge" in user scope'

CLI_CLIENTS = [ClaudeCodeClient(), CodexClient(), OpencodeClient()]


def ok(output: str = "") -> tuple[int | None, str, bool]:
    return (0, output, False)


def fails(code: int, output: str) -> tuple[int | None, str, bool]:
    return (code, output, False)


def times_out() -> tuple[int | None, str, bool]:
    return (None, "", True)


class FakeRunner:
    """Replays scripted responses in order, recording what it was asked to run."""

    def __init__(self, *responses: tuple[int | None, str, bool]) -> None:
        self.responses = list(responses)
        self.calls: list[list[str]] = []

    def __call__(self, argv):
        argv = list(argv)
        self.calls.append(argv)
        if not self.responses:
            raise AssertionError(f"unexpected extra invocation: {argv}")
        returncode, output, timed_out = self.responses.pop(0)
        return RunResult(tuple(argv), returncode, output, timed_out)


# --- registry ------------------------------------------------------------------------------


def test_every_client_is_registered() -> None:
    assert sorted(clients.CLIENTS) == ["claude-code", "claude-desktop", "codex", "opencode"]
    for key, client in clients.CLIENTS.items():
        assert client.key == key


def test_desktop_comes_first_because_it_needs_a_restart() -> None:
    assert next(iter(clients.CLIENTS)) == "claude-desktop"


@pytest.mark.parametrize("client", CLI_CLIENTS, ids=lambda c: c.key)
def test_cli_argv_puts_the_command_after_a_separator(client: CliClient) -> None:
    """So the server path can never be parsed as an option by the client's own parser."""
    argv = client.add_argv(REGISTRATION)
    assert argv[0] == client.binary
    assert argv[1:4] == ["mcp", "add", "polybridge"]
    assert argv[-2:] == ["--", "/opt/bin/polybridge-server"]


def test_claude_code_registers_at_user_scope() -> None:
    argv = ClaudeCodeClient().add_argv(REGISTRATION)
    assert argv == [
        "claude",
        "mcp",
        "add",
        "polybridge",
        "-s",
        "user",
        "-e",
        "PATH=/a:/b",
        "--",
        "/opt/bin/polybridge-server",
    ]


@pytest.mark.parametrize("client", [CodexClient(), OpencodeClient()], ids=lambda c: c.key)
def test_codex_and_opencode_spell_env_the_same_way(client: CliClient) -> None:
    assert client.add_argv(REGISTRATION)[4:6] == ["--env", "PATH=/a:/b"]


# --- clients whose add overwrites ------------------------------------------------------------


@pytest.mark.parametrize("client", [CodexClient(), OpencodeClient()], ids=lambda c: c.key)
def test_overwriting_clients_need_exactly_one_invocation(client: CliClient) -> None:
    runner = FakeRunner(ok())
    result = client.apply(REGISTRATION, runner)

    assert result.status == "applied"
    assert len(runner.calls) == 1


def test_success_claims_only_that_the_command_exited_zero() -> None:
    """It says nothing about the client having loaded or being able to launch the server."""
    result = CodexClient().apply(REGISTRATION, FakeRunner(ok()))

    assert "add command succeeded" in result.detail
    for overclaim in ("registered", "connected", "loaded", "running"):
        assert overclaim not in result.detail.lower()


@pytest.mark.parametrize("client", CLI_CLIENTS, ids=lambda c: c.key)
def test_a_timeout_is_unknown_not_failed(client: CliClient) -> None:
    """A CLI can write the config and then hang; calling that `failed` would be a guess."""
    result = client.apply(REGISTRATION, FakeRunner(times_out()))

    assert result.status == "unknown"
    assert "may have changed" in result.detail


def test_a_plain_failure_reports_the_exit_code_and_output() -> None:
    result = CodexClient().apply(REGISTRATION, FakeRunner(fails(2, "boom")))

    assert result.status == "failed"
    assert "exit 2" in result.detail
    assert "boom" in result.diagnostics


# --- Claude Code's replacement policy --------------------------------------------------------


def test_claude_code_does_not_remove_anything_when_the_add_succeeds() -> None:
    runner = FakeRunner(ok())
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "applied"
    assert len(runner.calls) == 1


def test_claude_code_replaces_an_existing_entry() -> None:
    runner = FakeRunner(fails(1, ALREADY_EXISTS), ok(), ok())
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "applied"
    assert [call[2] for call in runner.calls] == ["add", "remove", "add"]
    assert "replaced" in result.detail


def test_claude_code_tolerates_remove_reporting_the_name_was_absent() -> None:
    """Exit 1 from `remove` is also how it says "there was nothing there" (measured)."""
    runner = FakeRunner(fails(1, ALREADY_EXISTS), fails(1, NOT_FOUND), ok())
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "applied"
    assert len(runner.calls) == 3
    # Nothing was observed to exist, so claiming a replacement would invent an entry.
    assert "already gone" in result.detail
    assert "replaced" not in result.detail


@pytest.mark.parametrize(
    "output",
    [
        "Error: a worktree for that branch already exists",
        "MCP server something-else already exists in user config",
        "already exists",
        # The one that matters: a *longer* name ending in ours. Matching "<key> already exists"
        # would delete the entry we were asked to protect on someone else's collision.
        "MCP server not-polybridge already exists in user config",
        "MCP server polybridge-staging already exists in user config",
    ],
    ids=["unrelated-resource", "different-server", "bare-phrase", "name-suffix", "name-prefix"],
)
def test_only_a_collision_naming_our_own_key_leads_to_a_removal(output: str) -> None:
    """A substring match on "already exists" would let an unrelated error trigger a delete."""
    runner = FakeRunner(fails(1, output))

    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "failed"
    assert len(runner.calls) == 1


@pytest.mark.parametrize(
    "output",
    [
        'No MCP server named "something-else" in user scope',
        'No MCP server named "not-polybridge" in user scope',
        "No MCP server named. see polybridge docs",
    ],
    ids=["different-server", "name-suffix", "stray-mention"],
)
def test_a_remove_failure_that_is_not_our_own_absence_is_not_tolerated(output: str) -> None:
    runner = FakeRunner(fails(1, ALREADY_EXISTS), fails(1, output))

    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "failed"
    assert "no replacement was attempted" in result.detail
    assert len(runner.calls) == 2


@pytest.mark.parametrize(
    ("third", "status"),
    [(ok(), "applied"), (times_out(), "unknown"), (fails(1, "boom"), "failed")],
    ids=["succeeded", "timed-out", "failed"],
)
def test_no_report_claims_a_removal_that_was_never_observed(
    third: tuple[int | None, str, bool], status: str
) -> None:
    """When `remove` says the name was absent, no branch may then speak of a previous entry."""
    runner = FakeRunner(fails(1, ALREADY_EXISTS), fails(1, NOT_FOUND), third)

    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == status
    assert "previous entry was removed" not in result.detail
    assert "already gone" in result.detail


def test_claude_code_stops_when_remove_fails_for_any_other_reason() -> None:
    """Permission errors and future incompatibilities must not read as licence to delete."""
    runner = FakeRunner(fails(1, ALREADY_EXISTS), fails(1, "EACCES: permission denied"))
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "failed"
    # Not "nothing was changed": a non-zero exit does not prove the command changed nothing.
    assert "no replacement was attempted" in result.detail
    assert len(runner.calls) == 2


def test_claude_code_says_so_loudly_when_the_replacement_add_fails() -> None:
    """The destructive window is the price of updating; it must never be reported quietly."""
    runner = FakeRunner(fails(1, ALREADY_EXISTS), ok(), fails(1, "unexpected"))
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "failed"
    assert "assume polybridge is NOT registered" in result.detail
    restore = "\n".join(result.diagnostics)
    assert "claude mcp add polybridge -s user" in restore
    assert "/opt/bin/polybridge-server" in restore


def test_claude_code_reports_unknown_if_the_replacement_add_times_out() -> None:
    runner = FakeRunner(fails(1, ALREADY_EXISTS), ok(), times_out())
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "unknown"
    assert any("restore" in line or "run:" in line for line in result.diagnostics)


def test_claude_code_reports_unknown_if_the_remove_times_out() -> None:
    runner = FakeRunner(fails(1, ALREADY_EXISTS), times_out())
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "unknown"
    assert len(runner.calls) == 2


def test_an_unrecognised_add_failure_is_not_treated_as_already_exists() -> None:
    runner = FakeRunner(fails(1, "some other problem"))
    result = ClaudeCodeClient().apply(REGISTRATION, runner)

    assert result.status == "failed"
    assert len(runner.calls) == 1


# --- availability and selection ---------------------------------------------------------------


def test_a_missing_binary_is_skipped_by_default_but_fails_when_asked_for(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("polybridge.clients.base.shutil.which", lambda _: None)
    codex = CodexClient()

    skipped = clients.register([codex], REGISTRATION, run=FakeRunner())
    failed = clients.register(
        [codex], REGISTRATION, run=FakeRunner(), named=frozenset({"codex"})
    )

    assert [result.status for result in skipped] == ["skipped"]
    assert [result.status for result in failed] == ["failed"]
    assert "not on PATH" in skipped[0].detail


def test_an_availability_check_that_crashes_is_a_failure_not_a_skip() -> None:
    """`skipped` means "not installed". A broken check is a fault, and must not exit zero."""

    class Broken(Exploding):
        def availability(self):
            raise self.exc

    results = clients.register(
        [Broken(RuntimeError("cannot stat")), OpencodeClient()],
        REGISTRATION,
        run=FakeRunner(ok()),
    )

    assert results[0].status == "failed"
    assert "cannot stat" in results[0].detail
    assert clients.exit_code(results) == 1


def test_a_dry_run_previews_a_client_that_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A preview changes nothing, so refusing to print one would only withhold information."""
    monkeypatch.setattr("polybridge.clients.base.shutil.which", lambda _: None)

    (result,) = clients.register(
        [CodexClient()], REGISTRATION, dry_run=True, named=frozenset({"codex"})
    )

    assert result.status == "previewed"
    assert "codex mcp add polybridge" in result.detail
    assert any("not on PATH" in line for line in result.diagnostics)


def test_one_clients_failure_does_not_stop_the_others(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("polybridge.clients.base.shutil.which", lambda binary: f"/bin/{binary}")
    runner = FakeRunner(fails(2, "nope"), ok())

    results = clients.register([CodexClient(), OpencodeClient()], REGISTRATION, run=runner)

    assert [result.status for result in results] == ["failed", "applied"]


class Exploding:
    """A client that raises rather than returning a Result — a bug, or an unenumerated error."""

    key = "codex"
    label = "Codex"
    post_apply_note = ""

    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def availability(self):
        return clients.Availability(True, where="/bin/codex")

    def preview(self, registration):
        raise self.exc

    def apply(self, registration, run):
        raise self.exc


@pytest.mark.parametrize(
    "exc",
    [UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"), RuntimeError("kaboom")],
    ids=["non-utf8-config", "bug"],
)
def test_an_exception_from_one_client_still_leaves_the_others_running(
    exc: Exception, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only enumerated exception types were caught before, which made the guarantee a comment."""
    monkeypatch.setattr("polybridge.clients.base.shutil.which", lambda binary: f"/bin/{binary}")

    results = clients.register(
        [Exploding(exc), OpencodeClient()], REGISTRATION, run=FakeRunner(ok())
    )

    assert [result.status for result in results] == ["failed", "applied"]
    assert results[0].detail


def test_an_exception_with_no_message_still_reports_something(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("polybridge.clients.base.shutil.which", lambda binary: f"/bin/{binary}")

    (result,) = clients.register([Exploding(RuntimeError())], REGISTRATION, run=FakeRunner())

    assert result.status == "failed"
    assert "RuntimeError" in result.detail


def test_parse_selection_accepts_repeats_commas_and_all() -> None:
    assert clients.parse_selection(["codex,opencode"]) == ["codex", "opencode"]
    assert clients.parse_selection(["codex", "codex"]) == ["codex"]
    assert clients.parse_selection(["all"]) == list(clients.CLIENTS)
    assert clients.parse_selection(None) == list(clients.CLIENTS)


def test_only_individually_named_clients_are_treated_as_asked_for() -> None:
    """`all` is the default by another name, so it makes nothing an error on its own."""
    assert clients.named_clients(None) == frozenset()
    assert clients.named_clients(["all"]) == frozenset()
    assert clients.named_clients(["codex"]) == frozenset({"codex"})
    assert clients.named_clients(["codex", "opencode"]) == frozenset({"codex", "opencode"})
    # Mixed: Codex *was* named, so its absence is still an error even though `all` came too.
    assert clients.named_clients(["codex,all"]) == frozenset({"codex"})


def test_a_client_named_alongside_all_is_still_an_error_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("polybridge.clients.base.shutil.which", lambda _: None)

    results = clients.register(
        [CodexClient(), OpencodeClient()],
        REGISTRATION,
        run=FakeRunner(),
        named=clients.named_clients(["codex,all"]),
    )

    assert [result.status for result in results] == ["failed", "skipped"]


def test_parse_selection_rejects_an_unknown_name() -> None:
    with pytest.raises(clients.UnknownClient) as excinfo:
        clients.parse_selection(["cursor"])

    assert "cursor" in str(excinfo.value)
    assert "codex" in str(excinfo.value)


def test_closing_notes_group_clients_that_need_the_same_thing_done() -> None:
    notes = clients.closing_notes(
        [
            Result("claude-desktop", "applied", ""),
            Result("claude-code", "applied", ""),
            Result("codex", "applied", ""),
            Result("opencode", "skipped", ""),
        ]
    )

    # Two groups, not four lines: clients needing the same thing said once, in registry order.
    assert len(notes) == 2
    assert notes[0].startswith("Claude desktop app:") and "restart" in notes[0]
    assert notes[1].startswith("Claude Code, Codex:") and "no restart" in notes[1]
    assert not any("opencode" in note for note in notes)


def test_nothing_is_promised_about_a_client_that_did_not_change() -> None:
    assert clients.closing_notes([Result("codex", "skipped", "")]) == []
    assert clients.closing_notes([Result("codex", "unknown", "")]) == []


def test_override_config_path_is_rejected_when_no_selected_client_owns_a_config(
    tmp_path: Path,
) -> None:
    with pytest.raises(SetupError) as excinfo:
        clients.override_config_path([CodexClient()], tmp_path / "x.json")

    assert "Claude desktop app" in str(excinfo.value)


def test_override_config_path_leaves_the_other_clients_alone(tmp_path: Path) -> None:
    chosen = clients.override_config_path(
        [DesktopClient(), CodexClient()], tmp_path / "desktop.json"
    )

    assert chosen[0].config_path == tmp_path / "desktop.json"
    assert chosen[1] == CodexClient()


def test_exit_code_is_non_zero_for_trouble_or_for_nothing_at_all() -> None:
    applied = Result("codex", "applied", "")
    assert clients.exit_code([applied]) == 0
    assert clients.exit_code([Result("codex", "previewed", "")]) == 0
    assert clients.exit_code([applied, Result("opencode", "failed", "")]) == 1
    assert clients.exit_code([applied, Result("opencode", "unknown", "")]) == 1
    assert clients.exit_code([Result("codex", "skipped", "")]) == 1


# --- run_cli, which is load-bearing in ways that are easy to "tidy" away --------------------------


def python(code: str) -> list[str]:
    """A portable stand-in for /bin/* helpers, which are not in fixed places on every layout."""
    return [sys.executable, "-c", code]


def test_run_cli_closes_stdin() -> None:
    """Without this, `codex exec` blocks forever and `opencode mcp add` can prompt."""
    result = run_cli(python("import sys; sys.exit(0 if sys.stdin.read() == '' else 1)"))

    assert result.ok, "stdin was readable, so a prompting CLI would hang here"


def test_run_cli_runs_from_the_users_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """So a project-local config in the installer's directory cannot capture the registration."""
    monkeypatch.setenv("HOME", str(tmp_path))

    result = run_cli(python("import os; print(os.getcwd())"))

    assert result.output.strip() == os.path.realpath(tmp_path)


def test_run_cli_reports_a_timeout_as_such(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("polybridge.clients.base.CLI_TIMEOUT_SECONDS", 0.2)

    result = run_cli(python("import time; time.sleep(30)"))

    assert result.timed_out
    assert result.returncode is None
    assert not result.ok


def test_run_cli_reports_a_binary_that_cannot_be_launched() -> None:
    result = run_cli(["/nonexistent/polybridge-probe"])

    assert not result.ok
    assert not result.timed_out
    assert "could not run" in result.output


def test_run_cli_captures_both_streams() -> None:
    result = run_cli(
        python("import sys; print('out'); print('err', file=sys.stderr); sys.exit(3)")
    )

    assert result.returncode == 3
    assert "out" in result.output
    assert "err" in result.output


def test_a_long_output_is_trimmed_for_reporting() -> None:
    result = RunResult(("x",), 1, "y" * 5000)

    assert len(result.tail) < 1000
    assert result.tail.startswith("…")


# --- the desktop app, which has no CLI to delegate to -------------------------------------------


def desktop(tmp_path: Path) -> tuple[DesktopClient, Path]:
    path = tmp_path / "claude_desktop_config.json"
    return DesktopClient(config_path=path), path


def test_desktop_writes_the_entry(tmp_path: Path) -> None:
    client, path = desktop(tmp_path)

    result = client.apply(REGISTRATION, FakeRunner())

    assert result.status == "applied"
    written = json.loads(path.read_text())
    assert written["mcpServers"]["polybridge"] == {
        "command": "/opt/bin/polybridge-server",
        "env": {"PATH": "/a:/b"},
    }


def test_desktop_keeps_other_servers_and_other_keys(tmp_path: Path) -> None:
    client, path = desktop(tmp_path)
    path.write_text(json.dumps({"theme": "dark", "mcpServers": {"argent": {"command": "argent"}}}))

    client.apply(REGISTRATION, FakeRunner())

    written = json.loads(path.read_text())
    assert written["theme"] == "dark"
    assert written["mcpServers"]["argent"] == {"command": "argent"}
    assert "polybridge" in written["mcpServers"]


def test_desktop_backs_up_before_overwriting(tmp_path: Path) -> None:
    client, path = desktop(tmp_path)
    path.write_text(json.dumps({"mcpServers": {}}))

    client.apply(REGISTRATION, FakeRunner())

    backups = list(tmp_path.glob("claude_desktop_config.json.bak-*"))
    assert len(backups) == 1
    assert json.loads(backups[0].read_text()) == {"mcpServers": {}}


def test_desktop_refuses_to_overwrite_a_config_it_cannot_parse(tmp_path: Path) -> None:
    client, path = desktop(tmp_path)
    path.write_text("{not json")

    with pytest.raises(SetupError):
        client.apply(REGISTRATION, FakeRunner())

    assert path.read_text() == "{not json"


def test_a_malformed_desktop_config_is_reported_not_raised_through_register(
    tmp_path: Path,
) -> None:
    client, path = desktop(tmp_path)
    path.write_text("{not json")

    (result,) = clients.register([client], REGISTRATION, run=FakeRunner())

    assert result.status == "failed"
    assert "not valid JSON" in result.detail


def test_a_preview_that_cannot_be_produced_is_a_failure_not_progress(tmp_path: Path) -> None:
    """Otherwise `--dry-run` exits zero on a config that a real run could never write."""
    client, path = desktop(tmp_path)
    path.write_text("{not json")

    results = clients.register([client], REGISTRATION, run=FakeRunner(), dry_run=True)

    assert [result.status for result in results] == ["failed"]
    assert clients.exit_code(results) == 1


def test_desktop_refuses_when_the_file_changed_underneath_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, path = desktop(tmp_path)
    path.write_text(json.dumps({"mcpServers": {}}))
    # Three reads: the baseline, the one `load_config` does, then the final comparison.
    original = path.read_text()
    reads = iter([original, original, '{"mcpServers": {"someone-else": {}}}'])
    monkeypatch.setattr("polybridge.clients.desktop.read_raw", lambda _: next(reads))

    result = client.apply(REGISTRATION, FakeRunner())

    assert result.status == "failed"
    assert "changed while it was being edited" in result.detail
    assert json.loads(path.read_text()) == {"mcpServers": {}}


@pytest.mark.skipif(
    hasattr(os, "geteuid") and os.geteuid() == 0, reason="root ignores the mode bits this relies on"
)
def test_a_desktop_config_that_cannot_be_written_is_reported_not_raised(tmp_path: Path) -> None:
    """A PermissionError is an OSError, but the point is that `register` survives it either way."""
    directory = tmp_path / "readonly"
    directory.mkdir()
    (directory / "claude_desktop_config.json").write_text("{}")
    directory.chmod(0o500)
    try:
        results = clients.register(
            [DesktopClient(config_path=directory / "claude_desktop_config.json")],
            REGISTRATION,
            run=FakeRunner(),
        )
    finally:
        directory.chmod(0o700)

    assert [result.status for result in results] == ["failed"]
    assert results[0].detail


def test_a_non_utf8_desktop_config_is_reported_not_raised(tmp_path: Path) -> None:
    client, path = desktop(tmp_path)
    path.write_bytes(b"\xff\xfe{}")

    (result,) = clients.register([client], REGISTRATION, run=FakeRunner())

    assert result.status == "failed"
    assert result.detail


def test_desktop_preview_writes_nothing(tmp_path: Path) -> None:
    client, path = desktop(tmp_path)

    result = client.preview(REGISTRATION)

    assert result.status == "previewed"
    assert not path.exists()
    assert any("polybridge" in line for line in result.diagnostics)

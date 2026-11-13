"""`harflow-ctl backend-models`: caller gate, budget, output bounds and config-only dispatch."""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

from harflow_mcp import backends, bounded_process, ctl, lineage, takeover, wire_contracts
from harflow_mcp.backends import model_listing as ml
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session

DATA = Path(__file__).parent / "data" / "backend_models"
MARKERS = ("HARFLOW_TASK_ID", "PB_TASK_ID", "HARFLOW_DEPTH", "PB_DEPTH")
SENTINEL_HARNESSES = ("opencode", "agy", "cursor-agent", "agent", "codex", "vibe", "claude")


def executable(directory: Path, name: str, body: str = "exit 0") -> Path:
    path = directory / name
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.fixture
def world(tmp_path, monkeypatch):
    home = tmp_path / "home"
    bin_dir = tmp_path / "bin"
    home.mkdir()
    bin_dir.mkdir()
    state = tmp_path / "state"  # deliberately missing
    for name in MARKERS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("PATH", str(bin_dir))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(state))
    monkeypatch.delenv("CODEX_HOME", raising=False)
    monkeypatch.delenv("VIBE_HOME", raising=False)
    return type("World", (), {"home": home, "bin": bin_dir, "state": state, "tmp": tmp_path})


def run(capsys, *argv):
    try:
        code = ctl.main(["backend-models", *argv])
    except SystemExit as exc:
        code = exc.code
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def document(out):
    lines = out.strip().splitlines()
    assert len(lines) == 1
    return json.loads(lines[0])


def prepared_root(world):
    layout = resolve_layout()
    with state_session(layout):
        layout.tasks_directory.mkdir(parents=True, exist_ok=True)
    return layout


# --- contract surface -----------------------------------------------------------------------------------------


def test_capability_is_appended_last():
    assert wire_contracts.CAPABILITIES[-1] == "backend-models.v1"
    assert len(wire_contracts.CAPABILITIES) == 35


def test_json_result_for_a_claude_install(world, capsys):
    executable(world.bin, "claude")
    code, out, err = run(capsys, "--backend", "claude", "--json")
    doc = document(out)
    assert code == 0 and err == ""
    assert (doc["product"], doc["format"], doc["v"]) == ("harflow", "harflow.ctl", 1)
    result = doc["result"]
    assert list(result) == ["backend", "state", "reason", "source", "scope", "advisory", "harness_version",
                            "source_observed_at", "observed_at", "truncated", "dropped_count", "models"]
    assert (result["backend"], result["state"], result["source"], result["scope"]) == ("claude", "available", "documented_aliases", "user")
    assert result["advisory"] is True and result["harness_version"] is None and result["truncated"] is False
    assert [m["id"] for m in result["models"]] == ["fable", "opus", "sonnet"]
    assert result["observed_at"].endswith("Z")


def test_plain_output_one_line_per_model(world, capsys):
    executable(world.bin, "claude")
    code, out, _ = run(capsys, "--backend", "claude")
    assert code == 0 and out.splitlines() == ["fable\tFable", "opus\tOpus", "sonnet\tSonnet"]


def test_plain_output_states_the_state_and_reason(world, capsys):
    assert run(capsys, "--backend", "claude")[1] == "not_installed\n"
    executable(world.bin, "agy", "exit 3")
    assert run(capsys, "--backend", "antigravity")[1] == "unavailable: exit_nonzero\n"


def test_unavailable_is_a_successful_result_with_an_empty_list(world, capsys):
    executable(world.bin, "agy", "exit 3")
    code, out, _ = run(capsys, "--backend", "antigravity", "--json")
    result = document(out)["result"]
    assert code == 0
    assert (result["state"], result["reason"], result["source"], result["models"]) == ("unavailable", "exit_nonzero", "cli_output", [])


def test_opencode_through_the_real_runner(world, capsys):
    fixture = DATA / "opencode-models-verbose.txt"
    executable(world.bin, "opencode", f'/bin/cat "{fixture}"')
    code, out, _ = run(capsys, "--backend", "opencode", "--json")
    result = document(out)["result"]
    assert code == 0 and result["state"] == "available" and len(result["models"]) == 6


def test_unknown_backend_is_a_usage_error(world, capsys):
    code, out, err = run(capsys, "--backend", "nope", "--json")
    assert code == 2
    doc = document(out)
    assert doc["error"]["code"] == "usage" and "invalid choice" in doc["error"]["message"]
    assert "invalid choice" in err
    code, out, err = run(capsys, "--backend", "nope")
    assert code == 2 and out == "" and "invalid choice" in err


def test_missing_backend_is_a_usage_error(world, capsys):
    code, out, _ = run(capsys, "--json")
    assert code == 2 and document(out)["error"]["code"] == "usage"


def test_a_missing_state_root_proceeds_and_is_not_created(world, capsys):
    executable(world.bin, "claude")
    assert not world.state.exists()
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    assert code == 0 and document(out)["result"]["state"] == "available"
    assert not world.state.exists()


def test_backends_command_never_lists_models(world, monkeypatch, capsys):
    def boom(self, context):
        raise AssertionError("backends must stay probe-free")

    for backend in backends.BACKENDS.values():
        monkeypatch.setattr(backend, "list_models", boom.__get__(backend))
    assert ctl.main(["backends", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["backends"]


# --- caller gate ----------------------------------------------------------------------------------------------


class Spies:
    def __init__(self, world, monkeypatch):
        self.calls = []
        self.marker = world.tmp / "marker"
        for name in SENTINEL_HARNESSES:
            executable(world.bin, name, f': > "{self.marker}"; exit 0')
        sentinel = world.tmp / "sentinel"
        sentinel.write_text("{}")
        monkeypatch.setenv("CODEX_HOME", str(world.tmp))
        monkeypatch.setenv("VIBE_HOME", str(world.tmp))

        def spy(label):
            def wrapper(*args, **kwargs):
                self.calls.append(label)
                raise AssertionError(f"{label} must not run")
            return wrapper

        for backend in backends.BACKENDS.values():
            monkeypatch.setattr(backend, "list_models", spy("list_models:" + backend.name))
        monkeypatch.setattr(bounded_process, "run", spy("bounded_process.run"))
        monkeypatch.setattr(subprocess, "Popen", spy("Popen"))
        monkeypatch.setattr(subprocess, "run", spy("subprocess.run"))
        monkeypatch.setattr(ml, "read_file", spy("read_file"))
        monkeypatch.setattr("shutil.which", spy("which"))

    def assert_nothing_ran(self):
        assert self.calls == [] and not self.marker.exists()


@pytest.mark.parametrize("marker", MARKERS)
def test_env_markers_refuse_before_any_lookup(world, monkeypatch, capsys, marker):
    spies = Spies(world, monkeypatch)
    monkeypatch.setenv(marker, "1")
    code, out, err = run(capsys, "--backend", "codex", "--json")
    error = document(out)["error"]
    assert code == 1
    assert error["code"] == "agent_caller"
    assert error["data"] == {"coverage": "unknown", "check": "direct_caller_required"}  # no detail for agent refusals
    assert error["message"] == "Backend model listing requires a verified direct caller." and "direct caller" in err
    spies.assert_nothing_ran()


def test_a_lineage_refusal_refuses_before_any_lookup(world, monkeypatch, capsys):
    prepared_root(world)
    spies = Spies(world, monkeypatch)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *a, **k: ("agent_caller", "it was called from task x"))
    code, out, _ = run(capsys, "--backend", "vibe", "--json")
    error = document(out)["error"]
    assert (code, error["code"]) == (1, "agent_caller")
    assert error["data"] == {"coverage": "unknown", "check": "direct_caller_required"}
    spies.assert_nothing_ran()


def test_an_undecidable_lineage_fails_closed(world, monkeypatch, capsys):
    prepared_root(world)
    spies = Spies(world, monkeypatch)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *a, **k: ("caller_undecidable", "ps is unavailable"))
    code, out, _ = run(capsys, "--backend", "cursor", "--json")
    error = document(out)["error"]
    assert (code, error["code"], error["data"]["detail"]) == (1, "caller_undecidable", "lineage_undecidable")
    spies.assert_nothing_ran()


def test_a_crashing_lineage_check_fails_closed(world, monkeypatch, capsys):
    prepared_root(world)
    spies = Spies(world, monkeypatch)

    def crash(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(takeover, "caller_refusal", crash)
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    error = document(out)["error"]
    assert (code, error["code"], error["data"]["detail"]) == (1, "caller_undecidable", "detection_failed")
    assert "boom" not in out
    spies.assert_nothing_ran()


def test_an_existing_marked_root_with_no_caller_proceeds(world, monkeypatch, capsys):
    prepared_root(world)
    executable(world.bin, "claude")
    monkeypatch.setattr(takeover, "caller_refusal", lambda *a, **k: None)
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    assert code == 0 and document(out)["result"]["state"] == "available"


def test_an_unmarked_nonempty_root_fails_closed(world, monkeypatch, capsys):
    spies = Spies(world, monkeypatch)
    world.state.mkdir()
    (world.state / "stray").write_text("x")
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    error = document(out)["error"]
    assert (code, error["code"]) == (1, "caller_undecidable")
    assert error["data"] == {"coverage": "unknown", "check": "direct_caller_required", "detail": "root_present_unmarked"}
    spies.assert_nothing_ran()
    assert sorted(path.name for path in world.state.iterdir()) == ["stray"]


@pytest.mark.parametrize("contents", [(), (".harflow-store.lock",), (".harflow-store.lock", "tasks/")])
def test_an_existing_unmarked_root_never_proceeds(world, monkeypatch, capsys, contents):
    spies = Spies(world, monkeypatch)
    world.state.mkdir()
    for entry in contents:
        if entry.endswith("/"):
            (world.state / entry).mkdir()
        else:
            (world.state / entry).write_text("")
    before = sorted(str(p.relative_to(world.state)) for p in world.state.rglob("*"))
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    error = document(out)["error"]
    assert (code, error["code"], error["data"]["detail"]) == (1, "caller_undecidable", "root_present_unmarked")
    spies.assert_nothing_ran()
    assert sorted(str(p.relative_to(world.state)) for p in world.state.rglob("*")) == before


def test_an_unlstatable_root_fails_closed(world, monkeypatch, capsys):
    spies = Spies(world, monkeypatch)
    world.state.mkdir()
    real = os.lstat

    def lstat(path, *args, **kwargs):
        if str(path) == str(world.state):
            raise PermissionError("denied")
        return real(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", lstat)
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    assert (code, document(out)["error"]["data"]["detail"]) == (1, "store_unavailable")
    spies.assert_nothing_ran()


def test_a_held_store_lock_fails_closed_within_the_timeout(world, monkeypatch, capsys):
    layout = prepared_root(world)
    holder = subprocess.Popen(
        [sys.executable, "-c",
         "import fcntl,os,sys,time\n"
         f"fd=os.open({str(layout.root / '.harflow-store.lock')!r}, os.O_RDWR)\n"
         "fcntl.flock(fd, fcntl.LOCK_EX)\nprint('ready', flush=True)\ntime.sleep(30)"],
        stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "ready"
        spies = Spies(world, monkeypatch)
        started = time.monotonic()
        code, out, _ = run(capsys, "--backend", "claude", "--json")
        elapsed = time.monotonic() - started
    finally:
        holder.kill()
        holder.wait()
    error = document(out)["error"]
    assert (code, error["code"], error["data"]["detail"]) == (1, "caller_undecidable", "lock_timeout")
    assert elapsed < 6
    spies.assert_nothing_ran()


def test_a_gate_that_uses_the_budget_up_starts_no_discovery(world, monkeypatch, capsys):
    prepared_root(world)
    spies = Spies(world, monkeypatch)
    real_budget = ml.Budget
    monkeypatch.setattr(ml, "Budget", lambda: real_budget(0.05))

    def slow_gate(*args, **kwargs):
        time.sleep(0.15)
        return None

    monkeypatch.setattr(takeover, "caller_refusal", slow_gate)
    code, out, _ = run(capsys, "--backend", "opencode", "--json")
    result = document(out)["result"]
    assert code == 0
    assert (result["state"], result["reason"], result["models"]) == ("unavailable", "timeout", [])
    spies.assert_nothing_ran()


def test_the_budget_is_shared_across_spawns(world, monkeypatch, capsys):
    executable(world.bin, "agy", "/bin/sleep 5")
    real_budget = ml.Budget
    monkeypatch.setattr(ml, "Budget", lambda: real_budget(0.4))
    started = time.monotonic()
    code, out, _ = run(capsys, "--backend", "antigravity", "--json")
    assert code == 0 and time.monotonic() - started < 3
    assert document(out)["result"]["reason"] == "timeout"


def test_a_source_that_raises_is_reported_unavailable(world, monkeypatch, capsys):
    def boom(self, context):
        raise ValueError("bug")

    monkeypatch.setattr(backends.BACKENDS["claude"], "list_models", boom.__get__(backends.BACKENDS["claude"]))
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    result = document(out)["result"]
    assert code == 0 and (result["state"], result["models"]) == ("unavailable", [])


# --- output bounds through the real serializer ----------------------------------------------------------------


def listing_for(rows):
    return ml.build_listing(rows, source="cli_output")


def serve(monkeypatch, rows):
    listing = listing_for(rows)
    backend = backends.BACKENDS["opencode"]
    monkeypatch.setattr(backend, "list_models", lambda context: listing)


def test_401_valid_rows_print_400_and_truncated(world, monkeypatch, capsys):
    serve(monkeypatch, [ml.RawModel(f"m{i}", "x") for i in range(401)])
    code, out, _ = run(capsys, "--backend", "opencode", "--json")
    result = document(out)["result"]
    assert code == 0 and len(result["models"]) == 400 and result["truncated"] is True and result["dropped_count"] == 0


def test_invalid_rows_do_not_count_toward_400(world, monkeypatch, capsys):
    serve(monkeypatch, [ml.RawModel(f"m{i}", "x") for i in range(400)] + [ml.RawModel("bad id", "x")] * 5)
    result = document(run(capsys, "--backend", "opencode", "--json")[1])["result"]
    assert len(result["models"]) == 400 and result["truncated"] is False and result["dropped_count"] == 5


def padded_rows(count, tail_label):
    rows = [ml.RawModel(f"m{index:04d}" + "i" * 94, "é" * 128) for index in range(count)]
    rows.append(ml.RawModel("tail", tail_label))
    return rows


def tail_for(escaped_bytes):
    """A label of at most 256 UTF-8 bytes whose JSON-escaped form is exactly `escaped_bytes` long."""
    for accents in range(0, 129):
        ascii_count = escaped_bytes - 6 * accents
        if ascii_count >= 0 and ascii_count + 2 * accents <= 256 and ascii_count + accents >= 1:
            return "a" * ascii_count + "é" * accents
    raise AssertionError("no label of that size")


def test_a_line_of_exactly_the_limit_is_kept_and_one_byte_more_removes_one_row(world, monkeypatch, capsys):
    limit = ml.LINE_LIMIT_BYTES
    count = 200
    while True:
        serve(monkeypatch, padded_rows(count, "x"))
        _, out, _ = run(capsys, "--backend", "opencode", "--json")
        length = len(out.rstrip("\n").encode())
        if length > limit:
            count -= 1
        elif limit - length > 900:
            count += 1
        else:
            need = limit - length
            break
    serve(monkeypatch, padded_rows(count, tail_for(1 + need)))
    _, out, _ = run(capsys, "--backend", "opencode", "--json")
    assert len(out.rstrip("\n").encode()) == limit
    kept = document(out)["result"]
    assert len(kept["models"]) == count + 1 and kept["truncated"] is False
    serve(monkeypatch, padded_rows(count, tail_for(2 + need)))
    _, out, _ = run(capsys, "--backend", "opencode", "--json")
    cut = document(out)["result"]
    assert len(out.rstrip("\n").encode()) <= limit
    assert len(cut["models"]) == count and cut["truncated"] is True and cut["models"][-1]["id"] != "tail"


def test_a_successful_listing_survives_a_temp_dir_cleanup_error(world, monkeypatch, capsys):
    import tempfile
    executable(world.bin, "claude")

    class Failing(tempfile.TemporaryDirectory):
        def cleanup(self):
            super().cleanup()
            raise OSError("cleanup failed")

    monkeypatch.setattr(tempfile, "TemporaryDirectory", Failing)
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    assert code == 0 and document(out)["result"]["state"] == "available"


def test_temp_dir_creation_failure_is_spawn_failed(world, monkeypatch, capsys):
    import tempfile
    executable(world.bin, "claude")

    def boom(*args, **kwargs):
        raise OSError("no space")

    monkeypatch.setattr(tempfile, "TemporaryDirectory", boom)
    code, out, _ = run(capsys, "--backend", "claude", "--json")
    result = document(out)["result"]
    assert code == 0 and (result["state"], result["reason"], result["source"]) == ("unavailable", "spawn_failed", None)

"""`Backend.list_models`: sources, bounds and failure states, with recorded outputs and fake binaries."""
from __future__ import annotations

import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

from harflow_mcp import backends, bounded_process
from harflow_mcp.backends import model_listing as ml
from harflow_mcp.backends.base import ExtraPathsUnsupported

DATA = Path(__file__).parent / "data" / "backend_models"
NOW = datetime(2026, 11, 13, 12, 0, 0, tzinfo=timezone.utc)


def data(name: str) -> bytes:
    return (DATA / name).read_bytes()


def executable(directory: Path, name: str, body: str = "exit 0") -> Path:
    path = directory / name
    path.write_text("#!/bin/sh\n" + body + "\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


class Recorder:
    """A runner returning canned results by the last argv element, recording every call."""

    def __init__(self, results=None, default=None):
        self.results = results or {}
        self.default = default
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((list(argv), kwargs))
        key = argv[-1]
        result = self.results.get(key, self.default)
        if result is None:
            raise AssertionError(f"unexpected spawn: {argv}")
        return result


def ok(stdout=b"", stderr=b"", returncode=0):
    return bounded_process.Completed(returncode, stdout, stderr, None)


def failed(cause):
    return bounded_process.Completed(None, b"", b"", cause)


@pytest.fixture
def home(tmp_path):
    path = tmp_path / "home"
    path.mkdir()
    return path


@pytest.fixture
def bin_dir(tmp_path):
    path = tmp_path / "bin"
    path.mkdir()
    return path


@pytest.fixture
def make_context(tmp_path, home, bin_dir):
    work = tmp_path / "work"
    work.mkdir()

    def build(runner=None, **environ):
        env = {"PATH": str(bin_dir), "HOME": str(home), **environ}
        return ml.ModelListContext(environ=env, cwd=str(work), now=lambda: NOW, runner=runner)

    return build


def listing(name, context):
    return backends.BACKENDS[name].list_models(context)


def ids(result):
    return [row.id for row in result.models]


# --- seam -----------------------------------------------------------------------------------------------------


def test_default_is_unsupported(make_context):
    result = ExtraPathsUnsupported().list_models(make_context())
    assert (result.state, result.reason, result.source, result.models) == ("unsupported", None, None, ())


def test_every_registered_backend_has_a_source():
    assert all(type(backend).list_models is not ExtraPathsUnsupported.list_models for backend in backends.BACKENDS.values())


@pytest.mark.parametrize("name,source", [("claude", "documented_aliases"), ("codex", "harness_cache"),
                                         ("opencode", "cli_output"), ("antigravity", "cli_output"),
                                         ("cursor", "cli_output"), ("vibe", "config_aliases")])
def test_not_installed_spawns_and_reads_nothing(name, source, make_context, monkeypatch, home):
    def boom(*args, **kwargs):
        raise AssertionError("must not run or read")

    monkeypatch.setattr(bounded_process, "run", boom)
    monkeypatch.setattr(ml, "read_file", boom)
    context = make_context(CODEX_HOME=str(home), VIBE_HOME=str(home))
    result = listing(name, context)
    assert (result.state, result.source, result.models, result.reason) == ("not_installed", source, (), None)


# --- claude ---------------------------------------------------------------------------------------------------


def test_claude_documented_aliases_without_a_subprocess(make_context, bin_dir):
    executable(bin_dir, "claude")
    result = listing("claude", make_context(Recorder()))
    assert (result.state, result.source, result.source_observed_at) == ("available", "documented_aliases", None)
    assert ids(result) == ["fable", "opus", "sonnet"]
    assert [row.default for row in result.models] == [None, None, None]
    assert [row.effort_levels for row in result.models] == [None, None, None]


# --- codex ----------------------------------------------------------------------------------------------------


@pytest.fixture
def codex_home(tmp_path, bin_dir):
    executable(bin_dir, "codex")
    path = tmp_path / "codex-home"
    path.mkdir()
    return path


def test_codex_cache_listed_rows_by_priority_with_effort_intersection(make_context, codex_home):
    (codex_home / "models_cache.json").write_bytes(data("codex-models-cache.json"))
    result = listing("codex", make_context(CODEX_HOME=str(codex_home)))
    assert (result.state, result.source, result.source_observed_at) == ("available", "harness_cache", "2026-11-13T05:47:45Z")
    # hidden row dropped without counting; ties keep cache order (astra before 6-luna)
    assert ids(result) == ["gpt-6.1-sol", "gpt-6-astra", "gpt-6-luna", "gpt-5.6-luna"]
    assert result.dropped_count == 0 and result.truncated is False
    by_id = {row.id: row for row in result.models}
    assert by_id["gpt-6.1-sol"].label == "GPT-6.1-Sol"
    assert by_id["gpt-6.1-sol"].effort_levels == ("low", "medium", "high", "xhigh")  # max/ultra are outside the engine vocabulary
    assert by_id["gpt-6-luna"].effort_levels == ("high",)
    assert by_id["gpt-5.6-luna"].effort_levels == ()
    assert all(row.default is None for row in result.models)


def test_codex_default_location_is_dot_codex_under_home(make_context, codex_home, home):
    (home / ".codex").mkdir()
    (home / ".codex" / "models_cache.json").write_bytes(data("codex-models-cache.json"))
    assert listing("codex", make_context()).state == "available"


def test_codex_missing_cache(make_context, codex_home):
    result = listing("codex", make_context(CODEX_HOME=str(codex_home)))
    assert (result.state, result.reason, result.models) == ("unavailable", "source_missing", ())


@pytest.mark.parametrize("value", ["", "relative/dir"])
def test_codex_home_empty_or_relative_is_source_missing(make_context, codex_home, value):
    result = listing("codex", make_context(CODEX_HOME=value))
    assert (result.state, result.reason) == ("unavailable", "source_missing")


def test_codex_invalid_fetched_at_is_null_but_listing_succeeds(make_context, codex_home):
    document = json.loads(data("codex-models-cache.json"))
    document["fetched_at"] = "yesterday"
    (codex_home / "models_cache.json").write_text(json.dumps(document))
    result = listing("codex", make_context(CODEX_HOME=str(codex_home)))
    assert result.state == "available" and result.source_observed_at is None


def test_codex_fetched_at_offset_is_normalised_to_utc(make_context, codex_home):
    document = json.loads(data("codex-models-cache.json"))
    document["fetched_at"] = "2026-11-13T07:00:00+02:00"
    (codex_home / "models_cache.json").write_text(json.dumps(document))
    assert listing("codex", make_context(CODEX_HOME=str(codex_home))).source_observed_at == "2026-11-13T05:00:00Z"


@pytest.mark.parametrize("mutate", [
    lambda d: d.pop("models"),
    lambda d: d.update(models={}),
    lambda d: d["models"][0].pop("visibility"),
    lambda d: d["models"][0].pop("slug"),
    lambda d: d["models"][0].update(priority="1"),
    lambda d: d["models"][0].update(priority=True),
    lambda d: d["models"][0].update(supported_reasoning_levels=[{"description": "x"}]),
    lambda d: d["models"][0].update(supported_reasoning_levels="low"),
    lambda d: d["models"].append(3),
])
def test_codex_shape_deviation_is_unparseable(make_context, codex_home, mutate):
    document = json.loads(data("codex-models-cache.json"))
    mutate(document)
    (codex_home / "models_cache.json").write_text(json.dumps(document))
    result = listing("codex", make_context(CODEX_HOME=str(codex_home)))
    assert (result.state, result.reason, result.models) == ("unavailable", "unparseable", ())


def test_codex_invalid_json_and_non_object_are_unparseable(make_context, codex_home):
    for content in (b"{", b"[]", b"\xff\xfe"):
        (codex_home / "models_cache.json").write_bytes(content)
        assert listing("codex", make_context(CODEX_HOME=str(codex_home))).reason == "unparseable"


def test_codex_fifo_is_unparseable_and_does_not_block(make_context, codex_home):
    os.mkfifo(codex_home / "models_cache.json")
    assert listing("codex", make_context(CODEX_HOME=str(codex_home))).reason == "unparseable"


def test_codex_oversized_cache_is_unparseable(make_context, codex_home):
    path = codex_home / "models_cache.json"
    path.write_bytes(b"{}")
    os.truncate(path, ml.FILE_CAP_BYTES + 1)
    assert listing("codex", make_context(CODEX_HOME=str(codex_home))).reason == "unparseable"


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file modes")
def test_codex_unreadable_cache_is_unparseable(make_context, codex_home):
    path = codex_home / "models_cache.json"
    path.write_bytes(data("codex-models-cache.json"))
    path.chmod(0)
    try:
        assert listing("codex", make_context(CODEX_HOME=str(codex_home))).reason == "unparseable"
    finally:
        path.chmod(0o600)


def test_codex_symlinked_cache_is_refused(make_context, codex_home, tmp_path):
    target = tmp_path / "elsewhere.json"
    target.write_bytes(data("codex-models-cache.json"))
    (codex_home / "models_cache.json").symlink_to(target)
    assert listing("codex", make_context(CODEX_HOME=str(codex_home))).reason == "unparseable"


def test_codex_never_spawns(make_context, codex_home):
    (codex_home / "models_cache.json").write_bytes(data("codex-models-cache.json"))
    listing("codex", make_context(Recorder(), CODEX_HOME=str(codex_home)))  # a spawn would raise in Recorder


# --- opencode -------------------------------------------------------------------------------------------------


@pytest.fixture
def opencode(bin_dir):
    executable(bin_dir, "opencode")


def test_opencode_verbose_output(make_context, opencode, bin_dir):
    recorder = Recorder(default=ok(data("opencode-models-verbose.txt")))
    result = listing("opencode", make_context(recorder))
    assert (result.state, result.source) == ("available", "cli_output")
    argv, kwargs = recorder.calls[0]
    assert argv == [str(bin_dir / "opencode"), "models", "--verbose"]  # the path the installed check found, not resolved
    assert kwargs["stdout_cap"] == 32 * 1024 * 1024 and kwargs["stderr_cap"] == 64 * 1024
    assert ids(result) == ["opencode/beam-free", "opencode/big-pickle", "anthropic/claude-opus-4-5", "openai/gpt-5",
                           "opencode-go/deepseek-v4-pro", "opencode-go/gpt-5.6-luna"]
    by_id = {row.id: row for row in result.models}
    assert by_id["opencode/beam-free"].label == "Beam Free"
    assert by_id["opencode/beam-free"].effort_levels == ("low", "medium", "high", "xhigh")  # max is outside the vocabulary
    assert by_id["opencode/big-pickle"].effort_levels is None  # no variants: the source says nothing
    assert by_id["openai/gpt-5"].effort_levels == ("low", "medium", "high")  # minimal is outside the vocabulary
    assert by_id["opencode-go/deepseek-v4-pro"].effort_levels == ("high",)
    assert all(row.default is None for row in result.models)


def test_opencode_variants_with_nothing_selectable_is_an_empty_list(make_context, opencode):
    text = "p/m\n" + json.dumps({"id": "m", "providerID": "p", "name": "M", "variants": {"none": {}, "max": {}}}, indent=2)
    result = listing("opencode", make_context(Recorder(default=ok(text.encode()))))
    assert result.models[0].effort_levels == ()


def test_opencode_header_mismatch_is_unparseable(make_context, opencode):
    text = data("opencode-models-verbose.txt").decode().replace('"providerID": "opencode"', '"providerID": "other"', 1)
    result = listing("opencode", make_context(Recorder(default=ok(text.encode()))))
    assert (result.state, result.reason, result.models) == ("unavailable", "unparseable", ())


@pytest.mark.parametrize("stdout", [b"", b"not json at all", b"a/b\n[1]", b"a/b\n{", b"\xff", b"nosplit\n{}"])
def test_opencode_unexpected_stdout_is_unparseable(make_context, opencode, stdout):
    assert listing("opencode", make_context(Recorder(default=ok(stdout)))).reason == "unparseable"


def test_opencode_missing_name_drops_the_row(make_context, opencode):
    text = "p/m\n" + json.dumps({"id": "m", "providerID": "p"}) + "\np/n\n" + json.dumps({"id": "n", "providerID": "p", "name": "N"})
    result = listing("opencode", make_context(Recorder(default=ok(text.encode()))))
    assert ids(result) == ["p/n"] and result.dropped_count == 1


@pytest.mark.parametrize("done,reason", [
    (failed("timeout"), "timeout"),
    (failed("output_ceiling"), "output_ceiling"),
    (failed("spawn_failed"), "spawn_failed"),
    (ok(b"", b"boom", 3), "exit_nonzero"),
])
def test_command_failures_map_to_reasons(make_context, opencode, done, reason):
    result = listing("opencode", make_context(Recorder(default=done)))
    assert (result.state, result.reason, result.source, result.models) == ("unavailable", reason, "cli_output", ())


# --- antigravity ----------------------------------------------------------------------------------------------


@pytest.fixture
def agy(bin_dir):
    executable(bin_dir, "agy")


def test_agy_models_tab_separated(make_context, agy, bin_dir):
    recorder = Recorder(default=ok(data("agy-models.out"), b"Fetching available models...\n"))
    result = listing("antigravity", make_context(recorder))
    assert recorder.calls[0][0] == [str(bin_dir / "agy"), "models"]
    assert ids(result) == ["gemini-3.8-flash-high", "gemini-3.8-flash-medium", "gemini-3.8-flash-low",
                           "claude-sonnet-4-6", "claude-opus-4-6-thinking", "gpt-oss-120b-medium"]
    assert result.models[3].label == "Claude Sonnet 4.6 (Thinking)"
    assert all(row.effort_levels is None and row.default is None for row in result.models)


@pytest.mark.parametrize("stdout", [b"", b"\n\n", b"no tab here\n", b"id\tlabel\nbroken line\n"])
def test_agy_unexpected_stdout_is_unparseable(make_context, agy, stdout):
    assert listing("antigravity", make_context(Recorder(default=ok(stdout)))).reason == "unparseable"


def test_agy_failure(make_context, agy):
    assert listing("antigravity", make_context(Recorder(default=ok(b"", b"x", 1)))).reason == "exit_nonzero"


# --- cursor ---------------------------------------------------------------------------------------------------

CURSOR_VERSION = b"2026.11.11-b912975\n"


@pytest.fixture
def cursor(bin_dir):
    executable(bin_dir, "cursor-agent")
    return bin_dir / "cursor-agent"


def cursor_runner(models, versions=CURSOR_VERSION):
    return Recorder({"--version": versions if isinstance(versions, bounded_process.Completed) else ok(versions),
                     "models": models})


def test_cursor_logged_in_listing_marks_the_default(make_context, cursor):
    recorder = cursor_runner(ok(data("cursor-models.out")))
    result = listing("cursor", make_context(recorder))
    assert recorder.calls[0][0] == [str(cursor), "--version"] and recorder.calls[1][0] == [str(cursor), "models"]
    assert (result.state, result.source) == ("available", "cli_output")
    assert ids(result) == ["auto", "gpt-5.3-codex-low", "gpt-5.3-codex-low-fast", "gpt-5.3-codex",
                           "gpt-5.3-codex-fast", "gpt-5.3-codex-high", "gpt-5.3-codex-high-fast"]
    assert result.models[0].label == "Auto"  # the "(current, default)" suffix is stripped
    assert [row.default for row in result.models] == [True] + [False] * 6
    assert all(row.effort_levels is None for row in result.models)


def test_cursor_without_a_default_marker_leaves_defaults_null(make_context, cursor):
    text = b"Available models\n\na - A\nb - B (current)\n\nTip: x\n"
    result = listing("cursor", make_context(cursor_runner(ok(text))))
    assert [(row.id, row.label, row.default) for row in result.models] == [("a", "A", None), ("b", "B", None)]


def test_cursor_logged_out_is_auth_required(make_context, cursor):
    err = data("cursor-models-loggedout.err")
    result = listing("cursor", make_context(cursor_runner(ok(b"", err, 1))))
    assert (result.state, result.reason, result.models) == ("unavailable", "auth_required", ())


def test_cursor_other_nonzero_exit_is_exit_nonzero(make_context, cursor):
    result = listing("cursor", make_context(cursor_runner(ok(b"", b"Error: something else\n", 1))))
    assert result.reason == "exit_nonzero"


def test_cursor_auth_text_with_exit_zero_is_not_a_signature(make_context, cursor):
    result = listing("cursor", make_context(cursor_runner(ok(b"", b"Error: Authentication required.\n", 0))))
    assert result.reason == "unparseable"


@pytest.mark.parametrize("stdout", [b"", b"Available models\n\n", b"nope\n", b"Available models\n\nno separator\n"])
def test_cursor_unexpected_stdout_is_unparseable(make_context, cursor, stdout):
    assert listing("cursor", make_context(cursor_runner(ok(stdout)))).reason == "unparseable"


def test_cursor_version_probe_timeout_is_unavailable_not_not_installed(make_context, cursor):
    recorder = cursor_runner(ok(b""), versions=failed("timeout"))
    result = listing("cursor", make_context(recorder))
    assert (result.state, result.reason) == ("unavailable", "timeout")
    assert [call[0][-1] for call in recorder.calls] == ["--version"]


@pytest.mark.parametrize("cause,reason", [("output_ceiling", "output_ceiling"), ("spawn_failed", "spawn_failed")])
def test_cursor_version_probe_failures(make_context, cursor, cause, reason):
    result = listing("cursor", make_context(cursor_runner(ok(b""), versions=failed(cause))))
    assert (result.state, result.reason) == ("unavailable", reason)


def test_cursor_wrong_version_line_is_not_installed(make_context, cursor):
    recorder = cursor_runner(ok(b""), versions=b"agent 1.2.3\n")
    result = listing("cursor", make_context(recorder))
    assert (result.state, result.reason) == ("not_installed", None)
    assert [call[0][-1] for call in recorder.calls] == ["--version"]


def test_cursor_falls_back_to_agent_when_the_first_candidate_fails_the_probe(make_context, bin_dir, cursor):
    executable(bin_dir, "agent")

    def runner(argv, **kwargs):
        if argv[-1] == "--version":
            return failed("timeout") if argv[0].endswith("cursor-agent") else ok(CURSOR_VERSION)
        assert argv[0] == str(bin_dir / "agent")
        return ok(data("cursor-models.out"))

    result = listing("cursor", make_context(runner))
    assert result.state == "available" and result.models[0].id == "auto"


def test_cursor_uses_agent_alone_when_it_verifies(make_context, bin_dir):
    executable(bin_dir, "agent")
    result = listing("cursor", make_context(cursor_runner(ok(data("cursor-models.out")))))
    assert result.state == "available"


def test_cursor_generic_agent_that_is_not_cursor_is_not_installed(make_context, bin_dir):
    executable(bin_dir, "agent")
    result = listing("cursor", make_context(cursor_runner(ok(b""), versions=b"something else\n")))
    assert result.state == "not_installed"


# --- vibe -----------------------------------------------------------------------------------------------------


@pytest.fixture
def vibe_home(tmp_path, bin_dir):
    executable(bin_dir, "vibe")
    path = tmp_path / "vibe-home"
    path.mkdir()
    return path


def test_vibe_config_aliases_with_a_matching_active_model(make_context, vibe_home):
    (vibe_home / "config.toml").write_bytes(data("vibe-config.toml"))
    result = listing("vibe", make_context(Recorder(), VIBE_HOME=str(vibe_home)))
    assert (result.state, result.source) == ("available", "config_aliases")
    assert [(row.id, row.label, row.default) for row in result.models] == [
        ("mistral-medium-3.5", "mistral-medium-3.5", False),
        ("devstral-small", "devstral-small-latest", True),
        ("local", "local", False),
    ]
    assert all(row.effort_levels is None for row in result.models)


def test_vibe_active_model_without_a_matching_alias_leaves_defaults_null(make_context, vibe_home):
    text = data("vibe-config.toml").decode().replace('active_model = "devstral-small"', 'active_model = "glm-5-3"')
    (vibe_home / "config.toml").write_text(text)
    result = listing("vibe", make_context(VIBE_HOME=str(vibe_home)))
    assert [row.default for row in result.models] == [None, None, None]


def test_vibe_default_location(make_context, vibe_home, home):
    (home / ".vibe").mkdir()
    (home / ".vibe" / "config.toml").write_bytes(data("vibe-config.toml"))
    assert len(listing("vibe", make_context()).models) == 3


def test_vibe_missing_config(make_context, vibe_home):
    result = listing("vibe", make_context(VIBE_HOME=str(vibe_home)))
    assert (result.state, result.reason) == ("unavailable", "source_missing")


@pytest.mark.parametrize("value", ["", "relative"])
def test_vibe_home_empty_or_relative_is_source_missing(make_context, vibe_home, value):
    assert listing("vibe", make_context(VIBE_HOME=value)).reason == "source_missing"


@pytest.mark.parametrize("content", [b"[[models]\n", b"\xff", b"models = 3\n", b"models = [1]\n"])
def test_vibe_invalid_config_is_unparseable(make_context, vibe_home, content):
    (vibe_home / "config.toml").write_bytes(content)
    assert listing("vibe", make_context(VIBE_HOME=str(vibe_home))).reason == "unparseable"


def test_vibe_config_without_models_is_an_empty_available_list(make_context, vibe_home):
    (vibe_home / "config.toml").write_text('active_model = "x"\n')
    result = listing("vibe", make_context(VIBE_HOME=str(vibe_home)))
    assert (result.state, result.models) == ("available", ())


def test_vibe_symlinked_config_is_followed_to_a_regular_file(make_context, vibe_home, tmp_path):
    target = tmp_path / "dotfiles-config.toml"
    target.write_bytes(data("vibe-config.toml"))
    (vibe_home / "config.toml").symlink_to(target)
    assert len(listing("vibe", make_context(VIBE_HOME=str(vibe_home))).models) == 3


def test_vibe_symlink_to_a_directory_is_unparseable(make_context, vibe_home, tmp_path):
    (vibe_home / "config.toml").symlink_to(tmp_path)
    assert listing("vibe", make_context(VIBE_HOME=str(vibe_home))).reason == "unparseable"


def test_vibe_fifo_and_oversized_config(make_context, vibe_home):
    os.mkfifo(vibe_home / "config.toml")
    assert listing("vibe", make_context(VIBE_HOME=str(vibe_home))).reason == "unparseable"
    (vibe_home / "config.toml").unlink()
    (vibe_home / "config.toml").write_bytes(b"")
    os.truncate(vibe_home / "config.toml", ml.FILE_CAP_BYTES + 1)
    assert listing("vibe", make_context(VIBE_HOME=str(vibe_home))).reason == "unparseable"


# --- bounds ---------------------------------------------------------------------------------------------------


def raw(identifier, label="L", **kwargs):
    return ml.RawModel(identifier, label, **kwargs)


@pytest.mark.parametrize("identifier", ["", "a b", "a\tb", "a\nb", "a\x00b", "a​b ", "x" * 257, "é" * 129, None, 3])
def test_invalid_ids_are_dropped_and_counted(identifier):
    result = ml.build_listing([raw(identifier), raw("ok")], source="cli_output")
    assert ids(result) == ["ok"] and result.dropped_count == 1


def test_id_of_exactly_256_bytes_is_kept():
    result = ml.build_listing([raw("x" * 256), raw("é" * 128)], source="cli_output")
    assert len(result.models) == 2 and result.dropped_count == 0


def test_label_rules():
    result = ml.build_listing([raw("a", "y" * 256), raw("b", "y" * 257), raw("c", "tab\there"), raw("d", ""), raw("e", None),
                               raw("f", "ünï")], source="cli_output")
    assert [(row.id, row.label[:3]) for row in result.models] == [("a", "yyy"), ("d", "d"), ("f", "ünï")]
    assert result.dropped_count == 3


def test_unencodable_text_drops_only_that_row():
    result = ml.build_listing([raw("a\ud800b"), raw("ok", "fine"), raw("ok2", "bad\udc00label")], source="cli_output")
    assert ids(result) == ["ok"] and result.dropped_count == 2


def test_symlinked_binary_is_not_resolved(make_context, bin_dir, tmp_path):
    real = executable(tmp_path, "real-agy")
    (bin_dir / "agy").symlink_to(real)
    recorder = Recorder(default=ok(b"a\tA\n"))
    listing("antigravity", make_context(recorder))
    assert recorder.calls[0][0][0] == str(bin_dir / "agy")


def test_duplicate_ids_keep_the_first_and_count_the_rest():
    result = ml.build_listing([raw("a", "one"), raw("a", "two")], source="cli_output")
    assert [(row.id, row.label) for row in result.models] == [("a", "one")] and result.dropped_count == 1


def test_cap_of_400_valid_rows_sets_truncated_without_counting_them():
    rows = [raw(f"m{i}") for i in range(401)]
    result = ml.build_listing(rows, source="cli_output")
    assert len(result.models) == 400 and result.truncated is True and result.dropped_count == 0
    assert result.models[-1].id == "m399"


def test_invalid_rows_do_not_count_toward_the_cap():
    rows = [raw(f"m{i}") if i % 2 else raw("bad id") for i in range(800)]
    result = ml.build_listing(rows, source="cli_output")
    assert len(result.models) == 400 and result.truncated is False and result.dropped_count == 400


def test_exactly_400_valid_rows_is_not_truncated():
    result = ml.build_listing([raw(f"m{i}") for i in range(400)], source="cli_output")
    assert len(result.models) == 400 and result.truncated is False


def test_intersect_levels_distinguishes_null_from_empty():
    levels = ("low", "medium", "high", "xhigh")
    assert ml.intersect_levels(None, levels) is None
    assert ml.intersect_levels([], levels) == ()
    assert ml.intersect_levels(["max"], levels) == ()
    assert ml.intersect_levels(["xhigh", "low", "bogus", 3], levels) == ("low", "xhigh")


def encode(result):
    return json.dumps({"result": result})


def test_fit_to_line_keeps_a_line_of_exactly_the_limit_whole():
    rows = [ml.ModelRow(f"m{i:03d}", "label", None, None) for i in range(20)]
    result = ml.result_document("codex", ml.ModelListing("available", None, "cli_output", None, tuple(rows)), NOW)
    exact = len(encode(result).encode())
    kept = ml.fit_to_line(result, encode, exact)
    assert len(kept["models"]) == 20 and kept["truncated"] is False
    assert len(encode(kept).encode()) == exact


def test_fit_to_line_one_byte_over_removes_exactly_the_last_row():
    rows = [ml.ModelRow(f"m{i:03d}", "label", None, None) for i in range(20)]
    result = ml.result_document("codex", ml.ModelListing("available", None, "cli_output", None, tuple(rows)), NOW)
    exact = len(encode(result).encode())
    cut = ml.fit_to_line(result, encode, exact - 1)
    assert [m["id"] for m in cut["models"]] == [f"m{i:03d}" for i in range(19)] and cut["truncated"] is True
    assert len(encode(cut).encode()) <= exact - 1


def test_fit_to_line_can_remove_every_row_and_preserves_prior_truncation():
    rows = [ml.ModelRow("a", "b", None, None)]
    result = ml.result_document("codex", ml.ModelListing("available", None, "cli_output", None, tuple(rows), True), NOW)
    assert ml.fit_to_line(result, encode, 10)["models"] == []
    assert ml.fit_to_line(result, encode)["truncated"] is True


def test_result_document_shape():
    row = ml.ModelRow("a", "A", True, ("low",))
    document = ml.result_document("opencode", ml.ModelListing("available", None, "cli_output", "2026-11-13T05:47:45Z", (row,), False, 2), NOW)
    assert list(document) == ["backend", "state", "reason", "source", "scope", "advisory", "harness_version",
                              "source_observed_at", "observed_at", "truncated", "dropped_count", "models"]
    assert document["observed_at"] == "2026-11-13T12:00:00Z" and document["scope"] == "user" and document["advisory"] is True
    assert document["harness_version"] is None and document["dropped_count"] == 2
    assert document["models"] == [{"id": "a", "label": "A", "default": True, "effort_levels": ["low"]}]


# --- real subprocess path -------------------------------------------------------------------------------------


def test_opencode_through_the_real_bounded_runner(make_context, bin_dir, tmp_path):
    fixture = DATA / "opencode-models-verbose.txt"
    executable(bin_dir, "opencode", f'/bin/cat "{fixture}"; echo ignored >&2')
    result = listing("opencode", make_context())
    assert result.state == "available" and len(result.models) == 6


def test_a_hung_harness_times_out_within_the_shared_budget(make_context, bin_dir, tmp_path):
    marker = tmp_path / "started"
    executable(bin_dir, "agy", f'/usr/bin/touch "{marker}"; exec /bin/sleep 60')
    context = make_context()
    context.budget = ml.Budget(0.5)
    result = listing("antigravity", context)
    assert (result.state, result.reason) == ("unavailable", "timeout")
    assert marker.exists()


def test_an_exhausted_budget_spawns_nothing(make_context, bin_dir, tmp_path):
    marker = tmp_path / "started"
    executable(bin_dir, "agy", f'/usr/bin/touch "{marker}"')
    context = make_context()
    context.budget = ml.Budget(0.0)
    assert listing("antigravity", context).reason == "timeout"
    assert not marker.exists()


def test_stdout_flood_is_an_output_ceiling(make_context, bin_dir):
    executable(bin_dir, "agy", f'exec {sys.executable} -c "import sys\nwhile True: sys.stdout.write(\'x\'*65536)"')
    assert listing("antigravity", make_context()).reason == "output_ceiling"


def test_harness_runs_in_the_given_cwd_with_the_given_environment(make_context, bin_dir, tmp_path):
    executable(bin_dir, "agy", 'printf "%s\\t%s\\n" "$HF_PROBE" "$(pwd -P)"')
    context = make_context(HF_PROBE="seen")
    result = listing("antigravity", context)
    assert result.models[0].id == "seen" and result.models[0].label == os.path.realpath(context.cwd)

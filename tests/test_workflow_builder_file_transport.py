"""Large Monitor builder payloads must travel as files, outside argv limits."""
import json
from unittest.mock import AsyncMock

import pytest

from polybridge import ctl, server


def test_cli_build_reads_large_definition_and_source_files(monkeypatch, tmp_path, capsys):
    definition = {"instructions": "canvas" * 100000}
    source = {"saved_definition": {"instructions": "baseline" * 100000}, "revision": 3}
    canvas = tmp_path / "canvas.json"
    baseline = tmp_path / "source.json"
    canvas.write_text(json.dumps(definition))
    baseline.write_text(json.dumps(source))
    build = AsyncMock(return_value={"workflow_run_id": "builder"})
    monkeypatch.setattr(server, "workflow_builder", build)
    args = ["workflow-build", "canvas", "--prompt", "refine", "--backend", "codex", "--definition", str(canvas), "--source-file", str(baseline), "--json"]
    assert sum(len(a.encode()) for a in args) < 8192
    assert ctl.main(args) == 0
    assert build.call_args.args[-2:] == (definition, source)
    assert json.loads(capsys.readouterr().out)["result"]["workflow_run_id"] == "builder"


def test_source_file_and_inline_source_are_mutually_exclusive(tmp_path):
    with pytest.raises(SystemExit):
        ctl.main(["workflow-build", "canvas", "--prompt", "refine", "--backend", "codex", "--source", "{}", "--source-file", str(tmp_path / "source.json")])


@pytest.mark.parametrize("invalid", [False, True])
def test_unreadable_source_file_never_starts_builder(monkeypatch, tmp_path, invalid, capsys):
    path = tmp_path / "source.json"
    if invalid:
        path.write_text("{bad-json")
    build = AsyncMock()
    monkeypatch.setattr(server, "workflow_builder", build)
    assert ctl.main(["workflow-build", "canvas", "--prompt", "refine", "--backend", "codex", "--source-file", str(path), "--json"]) != 0
    assert build.await_count == 0
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "workflow_error"

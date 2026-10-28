"""Full engine group-report lifecycle against installed Codex; localhost, no paid calls."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import uuid

import pytest

from fake_responses_api import LocalResponses
from harflow_mcp import shared_scratch, state_ownership, store
from harflow_mcp.backends import get
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.tasks import TaskRegistry
from test_execution_config import isolate_configuration_environment

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1")]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.asyncio
async def test_registry_group_report_start_resume_continue_peer_and_isolation(tmp_path, monkeypatch):
    executable = shutil.which("codex")
    if sys.platform != "darwin" or executable is None:
        pytest.skip("group report output requires measured macOS Codex")
    observed = subprocess.check_output([executable, "--version"], text=True).strip()
    if observed != "codex-cli 0.165.0":
        pytest.skip(f"group report output measured on codex-cli 0.165.0; found {observed}")
    paths = {name: (tmp_path / name).resolve() for name in
             ("repo", "outside", "home", "codex-home", "native-bin", "state")}
    for path in paths.values():
        path.mkdir()
    subprocess.run(["git", "init", "-q", str(paths["repo"])], check=True)
    (paths["repo"] / ".codex").mkdir()
    # Keep launcher/config qualification real, while excluding ambient script backends.
    for name, selected in [("codex", executable), *((name, shutil.which(name))
                           for name in ("git", "ps", "open", "touch", "cat"))]:
        if selected is None:
            pytest.skip("native fixture command unavailable: " + name)
        (paths["native-bin"] / name).symlink_to(selected)
    isolate_configuration_environment(monkeypatch, paths["home"])
    monkeypatch.setenv("CODEX_HOME", str(paths["codex-home"]))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(paths["state"]))
    monkeypatch.setenv("PATH", str(paths["native-bin"]))
    layout = resolve_layout(root=paths["state"])
    backend = get("codex")
    q = lambda path: shlex.quote(str(path))
    with state_ownership.state_session(layout), LocalResponses(tmp_path).serve() as api:
        config = paths["codex-home"] / "config.toml"
        config.write_text(
            'model_provider="local"\nmodel="gpt-6.1-sol"\n[model_providers.local]\n'
            'name="local"\nwire_api="responses"\nrequires_openai_auth=false\nbase_url=' +
            json.dumps(api.url + "/v1") + "\n")
        registry = TaskRegistry(log_dir=paths["state"] / "tasks", open_monitor=False)
        group = shared_scratch.directory(registry.log_dir, "group", "reviews")
        other = shared_scratch.directory(registry.log_dir, "group", "other-reviews")
        for directory in (group, other):
            directory.mkdir(parents=True, exist_ok=True)
        (group / "escape").symlink_to(paths["outside"], target_is_directory=True)
        protected = {
            "repo": paths["repo"] / "source.txt",
            "project_config": paths["repo"] / ".codex" / "denied.txt",
            "outside": paths["outside"] / "existing.txt",
            "user_config": config,
        }
        for label, path in protected.items():
            if path != config:
                path.write_text(label + " unchanged")
        baseline = {label: digest(path) for label, path in protected.items()}
        fresh = resumed = None
        for phase in ("fresh", "resume", "continuation", "peer", "other_group"):
            selected_group = other if phase == "other_group" else group
            excluded_group = group if phase == "other_group" else other
            excluded_report = excluded_group / "protected.md"
            excluded_report.write_text("other group unchanged")
            excluded_hash = digest(excluded_report)
            api.nonce = "engine_group_" + uuid.uuid4().hex
            prompt = f"{api.nonce}  Review caller prose\nwith preserved spacing."
            api.commands = [
                ("create", f"printf first > {q(selected_group / 'report.md')}"),
                ("update", f"printf second > {q(selected_group / 'report.md')}"),
                ("read", f"cat {q(selected_group / 'report.md')}"),
                *[(label, f"printf changed > {q(path)}") for label, path in protected.items()],
                ("other", f"printf changed > {q(excluded_report)}"),
                ("traversal", f"printf changed > {q(selected_group / '..' / 'traversal.txt')}"),
            ]
            if phase != "other_group":
                api.commands.append(("symlink", f"printf changed > {q(group / 'escape' / 'existing.txt')}"))
            if phase == "peer":
                api.commands.insert(0, ("peer_read", f"cat {q(group / 'report.md')}"))
            if phase == "resume":
                record = store.read(registry.log_dir, fresh.task_id)
                task = await registry.resume_record(record, prompt)
                resumed = task
            elif phase == "continuation":
                task = await registry.continue_task(resumed.task_id, prompt)
            else:
                task = await registry.start(prompt, paths["repo"], backend=backend,
                    freedom="read_only", network=False, group="other-reviews" if phase == "other_group" else "reviews",
                    group_report_output=True)
            try:
                await asyncio.wait_for(task.done.wait(), timeout=90)
            finally:
                if not task.done.is_set():
                    await registry.cancel(task.task_id)
            assert task.status == "completed", task.snapshot()
            if phase == "fresh":
                fresh = task
            if phase == "resume":
                assert task.session_id == fresh.session_id
            elif phase in ("continuation", "peer", "other_group"):
                assert task.session_id != fresh.session_id
            if phase != "continuation":
                assert task.prompt == prompt
            assert task.freedom == "read_only"
            assert task.enforcement["group_report_output"] is True
            assert task.enforcement["writable_roots"] == [str(selected_group)]
            assert task.scratch["group"] == str(selected_group)
            assert task.scratch["access"]["group"] == shared_scratch.WRITABLE
            assert task.scratch["access"]["chain"] == shared_scratch.SANDBOX_READ_ONLY
            assert task.scratch.get("task") is None
            saved = store.read(registry.log_dir, task.task_id)
            assert saved.enforcement["group_report_output"] is True
            assert saved.freedom == "read_only" and saved.group == task.group
            inputs = api.requests[-2]["input"]
            user_texts = [block.get("text") for item in inputs if item.get("role") == "user"
                          for block in item.get("content", []) if isinstance(block, dict)]
            assert task.prompt in user_texts  # actual provider delivery, not just stored metadata
            permission_blocks = [block.get("text", "") for item in inputs if item.get("role") == "developer"
                                 for block in item.get("content", []) if isinstance(block, dict)
                                 and "<permissions instructions>" in block.get("text", "")]
            assert any(f"The writable root is `{selected_group}`" in text for text in permission_blocks)
            results = api.results()
            for label in ("create", "update", "read"):
                assert results[label]["exit_code"] == 0, results
            for label in (*protected, "other", "traversal"):
                assert results[label]["exit_code"] != 0, results
                assert "operation not permitted" in results[label]["output"].lower(), results
            if phase != "other_group":
                assert results["symlink"]["exit_code"] != 0, results
            if phase == "peer":
                assert "second" in results["peer_read"]["output"], results
            assert (selected_group / "report.md").read_text() == "second"
            assert {label: digest(path) for label, path in protected.items()} == baseline
            assert digest(excluded_report) == excluded_hash
            assert not (selected_group.parent / "traversal.txt").exists()

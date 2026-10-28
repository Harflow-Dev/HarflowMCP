"""An agent writes its worktree, so Git is never run through anything it can repoint or configure."""

from __future__ import annotations

import asyncio
import os
import stat
from pathlib import Path

import pytest

from harflow_mcp import store, task_resources, worktrees
from harflow_mcp.backends import get

from restart_test_support import install_fake_claude
from test_task_resources import OWNER, launch_state, owned, registry, tasks_dir, wait_for, write_receipt  # noqa: F401
from test_worktrees import git, make_plan, record_for, seed_created, source  # noqa: F401


@pytest.fixture
def marker(tmp_path) -> tuple[Path, Path]:
    """`(script, marker)`: running the script leaves the marker behind."""
    script, marker = tmp_path / "evil.sh", tmp_path / "executed"
    script.write_text(f"#!/bin/sh\ntouch {marker}\n")
    script.chmod(script.stat().st_mode | stat.S_IXUSR)
    return script, marker


def evil_gitdir(tmp_path: Path, script: Path) -> Path:
    """A Git directory whose configuration runs `script` as fsmonitor and as a clean/smudge filter."""
    evil = tmp_path / "evil-gitdir"
    evil.mkdir()
    (evil / "HEAD").write_text("ref: refs/heads/main\n")
    (evil / "config").write_text(
        "[core]\n\trepositoryformatversion = 0\n\tfsmonitor = " + str(script) + "\n"
        '[filter "x"]\n\tclean = ' + str(script) + "\n\tsmudge = " + str(script) + "\n")
    return evil


def dirty_with_filter_attributes(path: Path) -> None:
    """A modified tracked file under a `.gitattributes` that routes everything through filter `x`."""
    (path / ".gitattributes").write_text("* filter=x\n")
    (path / "a.txt").write_text("changed so that status has to run the clean filter")


def remove_code(log_dir: Path, task_id: str) -> str:
    with pytest.raises(worktrees.WorktreeError) as refused:
        worktrees.remove(log_dir, task_id)
    return refused.value.code


# ---- a rewritten .git file -----------------------------------------------------------------------


def test_rewritten_git_file_is_refused_and_nothing_runs(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    (path / ".git").write_text(f"gitdir: {evil_gitdir(tmp_path, script)}\n")
    dirty_with_filter_attributes(path)
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_link_invalid"
    assert not ran.exists() and path.exists()


def test_git_file_pointing_at_another_worktree_is_refused(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    other = worktrees.create(make_plan(source, tmp_path, "b" * 32))
    path = Path(made["path"])
    (path / ".git").write_text((Path(other["path"]) / ".git").read_text())  # a real, but someone else's, Git directory
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_link_invalid"
    assert not ran.exists()


def test_git_symlink_is_refused(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    real = tmp_path / "real-git-file"
    (path / ".git").rename(real)
    (path / ".git").symlink_to(real)
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_link_invalid"


def test_broken_back_links_are_refused(source, tmp_path, owned, identities) -> None:
    made = seed_created(source, tmp_path, owned)
    gitdir = Path(next(line for line in (Path(made["path"]) / ".git").read_text().splitlines()).split(": ", 1)[1])
    original = (gitdir / "gitdir").read_text()
    (gitdir / "gitdir").write_text(str(tmp_path / "elsewhere" / ".git") + "\n")
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_link_invalid"
    (gitdir / "gitdir").write_text(original)
    (gitdir / "commondir").write_text(str(tmp_path) + "\n")
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_link_invalid"


# ---- executable configuration in the effective config --------------------------------------------


def test_filter_in_the_worktree_specific_config_is_never_executed(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    git(source, "config", "extensions.worktreeConfig", "true")
    git(path, "config", "--worktree", "filter.x.clean", str(script))
    git(path, "config", "--worktree", "core.fsmonitor", str(script))
    dirty_with_filter_attributes(path)
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_filters_unsupported"
    assert not ran.exists()


def test_filter_added_to_the_source_after_creation_is_refused_before_status(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    seed_created(source, tmp_path, owned)
    git(source, "config", "filter.x.clean", str(script))
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_filters_unsupported"
    assert not ran.exists()


def test_fsmonitor_in_the_shared_config_is_not_run_by_a_clean_removal(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    git(source, "config", "core.fsmonitor", str(script))
    git(source, "config", "core.hooksPath", str(source / ".git" / "hooks"))
    hook = source / ".git" / "hooks" / "post-checkout"
    hook.write_text(f"#!/bin/sh\ntouch {ran}\n")
    hook.chmod(0o755)
    result = worktrees.remove(owned.tasks_directory, "a" * 32)
    assert result["removed"] is True and not Path(made["path"]).exists()
    assert not ran.exists()


# ---- item 9: the usage lock is recognised from durable metadata ----------------------------------


@pytest.mark.asyncio
async def test_resume_serializes_with_removal_whatever_the_current_worktree_root(tmp_path, source, monkeypatch, identities) -> None:
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("reply done", source, backend=get("claude"), freedom="write_in_repo", max_turns=3, worktree={}, task_id="a" * 32)
    await wait_for(lambda: (r := store.read(log_dir, "a" * 32)) is not None and r.finished_at and r.output_drained)
    await wait_for(lambda: launch_state(tmp_path, "a" * 32) == "settled")
    record = store.read(log_dir, "a" * 32)
    path = Path(record.worktree["path"])
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "somewhere-else"))  # the configuration moved on
    original = worktrees.usage_lock
    monkeypatch.setattr(worktrees, "usage_lock", lambda p, timeout=10.0: original(p, timeout=0.3))
    with worktrees.usage_lock(path):  # what a removal in progress holds
        with pytest.raises(worktrees.WorktreeError) as busy:
            await reg.resume_record(record, "reply again")
        assert busy.value.code == "worktree_busy"
    assert launch_state(tmp_path, "a" * 32) == "settled"
    resumed = await reg.resume_record(record, "reply again")  # and it is not blocked once the lock is free
    assert resumed.task_id != "a" * 32


def test_lock_path_is_the_same_for_every_spelling_of_the_path(source, tmp_path, owned, identities) -> None:
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    link = tmp_path / "alias"
    link.symlink_to(path.parent)
    assert worktrees.lock_path(link / path.name) == worktrees.lock_path(path)


# ---- items 19 and 20 -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_plain_launch_inside_a_recorded_worktree_takes_its_usage_lock(tmp_path, source, monkeypatch, identities) -> None:
    install_fake_claude(tmp_path, monkeypatch)
    log_dir = tasks_dir(tmp_path)
    reg = registry(tmp_path)
    await reg.start("reply done", source, backend=get("claude"), freedom="write_in_repo", max_turns=3, worktree={}, task_id="a" * 32)
    await wait_for(lambda: (r := store.read(log_dir, "a" * 32)) is not None and r.finished_at and r.output_drained)
    path = Path(store.read(log_dir, "a" * 32).worktree["path"])
    monkeypatch.setenv("HARFLOW_WORKTREE_ROOT", str(tmp_path / "somewhere-else"))
    original = worktrees.usage_lock
    monkeypatch.setattr(worktrees, "usage_lock", lambda p, timeout=10.0: original(p, timeout=0.3))
    with worktrees.usage_lock(path):
        with pytest.raises(worktrees.WorktreeError) as busy:
            await reg.start("reply plain", path, backend=get("claude"), freedom="write_in_repo", task_id="b" * 32)
        assert busy.value.code == "worktree_busy"


def test_worktree_config_that_would_execute_is_refused_and_never_run(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    git(source, "config", "extensions.worktreeConfig", "true")
    git(path, "config", "--worktree", "core.fsmonitor", str(script))
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_filters_unsupported"
    assert not ran.exists() and path.exists()


# ---- round 2: containment, submodules and locks ---------------------------------------


def test_a_task_running_in_a_subdirectory_of_the_worktree_blocks_removal(source, tmp_path, owned, identities) -> None:
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    (path / "pkg").mkdir()
    live = record_for("b" * 32, str(path / "pkg"), status="running", exit_code=None, finished_at=None, output_drained=False,
                      pid=4242, start_time="Mon Jan  1 00:00:00 2001", markers=["x"], owner=OWNER)
    store.write(owned.tasks_directory, live)
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_in_use"
    store.write(owned.tasks_directory, record_for("b" * 32, str(path / "pkg")))
    write_receipt(tmp_path, "c" * 32, "granted", owner=OWNER, repo_path=str(path / "pkg"))
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_in_use"
    assert path.exists()


def with_submodule(source: Path, tmp_path: Path, path: Path) -> Path:
    """Add and commit an initialized submodule inside the worktree; returns the submodule directory."""
    inner = tmp_path / "inner"
    inner.mkdir()
    git(inner, "init", "-q")
    (inner / "lib.txt").write_text("lib")
    git(inner, "add", ".")
    git(inner, "-c", "user.email=t@example.com", "-c", "user.name=T", "commit", "-q", "-m", "lib")
    git(path, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(inner), "sub")
    git(path, "-c", "user.email=t@example.com", "-c", "user.name=T", "commit", "-q", "-m", "add submodule")
    return path / "sub"


def test_a_worktree_with_an_initialized_submodule_is_refused_and_keeps_its_changes(source, tmp_path, owned, identities) -> None:
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    sub = with_submodule(source, tmp_path, path)
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_has_submodules"
    (sub / "lib.txt").write_text("dirty change only inside the submodule")
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_has_submodules"
    assert (sub / "lib.txt").read_text() == "dirty change only inside the submodule" and path.exists()


def test_an_initialized_but_empty_submodule_with_its_git_dir_in_the_common_directory_is_refused(source, tmp_path, owned, identities) -> None:
    import shutil
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    sub = with_submodule(source, tmp_path, path)
    worktree_git = Path(worktrees.verify_link(path, source)[0])
    common = Path(worktrees.verify_link(path, source)[1])
    # Initialized, but the working directory is empty and the Git directory is under <common>/modules/<path>.
    (common / "modules").mkdir(exist_ok=True)
    shutil.move(str(worktree_git / "modules" / "sub"), str(common / "modules" / "sub"))
    shutil.rmtree(sub)
    sub.mkdir()
    assert not (worktree_git / "modules").exists() or not any((worktree_git / "modules").iterdir())
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_has_submodules"
    assert path.exists()


def test_a_malicious_submodule_configuration_is_never_executed(source, tmp_path, owned, identities, marker) -> None:
    script, ran = marker
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    sub = with_submodule(source, tmp_path, path)
    git(sub, "config", "core.fsmonitor", str(script))
    git(sub, "config", "filter.x.clean", str(script))
    (sub / "lib.txt").write_text("dirty")
    (sub / ".gitattributes").write_text("* filter=x\n")
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_has_submodules"
    assert not ran.exists() and path.exists()


def test_a_locked_worktree_is_not_removed(source, tmp_path, owned, identities) -> None:
    made = seed_created(source, tmp_path, owned)
    path = Path(made["path"])
    git(source, "worktree", "lock", str(path))
    assert remove_code(owned.tasks_directory, "a" * 32) == "worktree_locked"
    assert path.exists()
    git(source, "worktree", "unlock", str(path))
    assert worktrees.remove(owned.tasks_directory, "a" * 32)["removed"] is True


def test_removal_uses_a_single_force_after_the_full_proof(source, tmp_path, owned, identities, monkeypatch) -> None:
    seen = []
    real = worktrees.subprocess.run

    def spy(argv, *args, **kwargs):
        seen.append(list(argv))
        return real(argv, *args, **kwargs)

    seed_created(source, tmp_path, owned)
    monkeypatch.setattr(worktrees.subprocess, "run", spy)
    worktrees.remove(owned.tasks_directory, "a" * 32)
    removals = [argv for argv in seen if "worktree" in argv and "remove" in argv]
    assert len(removals) == 1 and removals[0].count("--force") == 1
    assert all("--ignore-submodules=none" not in argv for argv in seen)

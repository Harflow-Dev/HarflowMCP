"""`task-changes` capture: bounded, isolated Git observation stored once per settled task."""
from __future__ import annotations

import asyncio
import base64
import fcntl
import inspect
import json
import os
import stat
import subprocess
import time

import pytest

from task_changes_support import TASK, commit, entry, git, head, init_repo, pin_git, seed_task

from harflow_mcp import state_ownership, store, task_changes_capture as capture, task_changes_document as shape, task_supervisor
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def world(tmp_path, monkeypatch):
    pin_git(monkeypatch, tmp_path / "home")
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield type("World", (), {"tasks": layout.tasks_directory, "root": tmp_path.resolve(), "layout": layout})


def build(repositories, **options):
    document = capture.build_document(TASK, repositories, **options)
    shape.validate(document, TASK)
    return document


def scopes(document, index=0):
    return document["repositories"][index]["scopes"]


def rows(document, scope, index=0):
    return {capture_row_path(row): row for row in scopes(document, index)[scope]["rows"]}


def capture_row_path(row):
    return shape.row_path_bytes(row).decode("utf-8", "surrogateescape")


def text_of(row):
    return base64.b64decode(row["text_b64"]).decode() if row.get("text_b64") else None


@pytest.fixture
def repo(world):
    path = init_repo(world.root / "repo")
    base = commit(path, "base", f1="a\nb\n", f2="x\n")
    return path, base


def simple_entry(path, base, **options):
    return entry(path, base, head(path), **options)


# --- basics -----------------------------------------------------------------------------------


def test_committed_and_working_tree_scopes_with_provenance(repo):
    path, base = repo
    tip = commit(path, "second", f1="a\nb\nc\n", f2=None, f3="new\n")
    (path / "f1").write_text("a\nb\nc\nw\n")
    (path / "untracked").write_text("u")
    document = build([entry(path, base, tip, provenance="replaced after branch change")])
    repository = document["repositories"][0]
    assert document["capture"] == {"state": "available", "reason": None}
    assert (repository["base_commit"], repository["head_commit"], repository["base_provenance"]) == (base, tip, "replaced after branch change")
    assert repository["baseline_dirty"] is False and repository["attribution"] == shape.ATTRIBUTION
    committed = rows(document, "committed_range")
    assert {name: row["status"] for name, row in committed.items()} == {"f1": "modified", "f2": "deleted", "f3": "added"}
    assert "+c" in text_of(committed["f1"]) and (committed["f1"]["additions"], committed["f1"]["deletions"]) == (1, 0)
    working = rows(document, "working_tree")
    assert set(working) == {"f1", "untracked"} and "+w" in text_of(working["f1"])
    assert working["untracked"]["status"] == "untracked" and working["untracked"]["content"]["reason"] == "untracked"
    assert scopes(document)["working_tree"]["counts_and_patch_separate"] is True
    assert scopes(document)["committed_range"]["state"] == scopes(document)["working_tree"]["state"] == "available"


def test_empty_diff_is_available_with_zero_rows(repo):
    path, base = repo
    document = build([simple_entry(path, base)])
    for scope in scopes(document).values():
        assert (scope["state"], scope["row_count"], scope["rows"]) == ("available", 0, [])


@pytest.mark.parametrize("status,expected", [("", False), (" M f1\n", True), (None, "unknown")])
def test_baseline_dirty_is_derived_from_the_recorded_status(repo, status, expected):
    path, base = repo
    assert build([simple_entry(path, base, status=status)])["repositories"][0]["baseline_dirty"] == expected


def test_rename_is_a_deletion_and_an_addition(repo):
    path, base = repo
    git(path, "mv", "f2", "renamed")
    tip = commit(path, "rename")
    document = build([entry(path, base, tip)])
    assert {name: row["status"] for name, row in rows(document, "committed_range").items()} == {"f2": "deleted", "renamed": "added"}


def test_binary_files_carry_no_text(repo):
    path, base = repo
    tip = commit(path, "binary", blob=b"\x00\x01\x02binary")
    row = rows(build([entry(path, base, tip)]), "committed_range")["blob"]
    assert row["binary"] is True and row["additions"] is None and row["content"] == {"state": "binary", "reason": None, "bytes": 0}
    assert "text_b64" not in row


def test_sha256_repository(world):
    path = init_repo(world.root / "sha256", object_format="sha256")
    base = commit(path, "base", f="1\n")
    tip = commit(path, "next", f="1\n2\n")
    (path / "f").write_text("1\n2\n3\n")
    document = build([entry(path, base, tip)])
    assert "+2" in text_of(rows(document, "committed_range")["f"]) and "+3" in text_of(rows(document, "working_tree")["f"])


def test_unmerged_entries(world):
    path = init_repo(world.root / "conflict")
    base = commit(path, "base", f="base\n")
    git(path, "checkout", "-q", "-b", "other")
    commit(path, "other", f="one\n")
    git(path, "checkout", "-q", "main")
    tip = commit(path, "main", f="two\n")
    git(path, "merge", "other", check=False)
    row = rows(build([entry(path, base, tip)]), "working_tree")["f"]
    assert row["status"] == "unmerged" and row["content"] == {"state": "not_captured", "reason": "unmerged", "bytes": 0}


def test_base_missing_not_ancestor_and_unborn_head(repo):
    path, base = repo
    git(path, "checkout", "-q", "-b", "side")
    side = commit(path, "side", g="1\n")
    git(path, "checkout", "-q", "main")
    tip = commit(path, "main", h="1\n")
    for given, reason in ((None, "base_missing"), ("1" * 40, "base_missing"), (side, "base_not_ancestor")):
        scope = scopes(build([entry(path, given, tip)]))["committed_range"]
        assert (scope["state"], scope["reason"], scope["rows"]) == ("not_captured", reason, [])
    scope = scopes(build([entry(path, base, None)]))
    assert scope["committed_range"]["reason"] == scope["working_tree"]["reason"] == "head_missing"
    assert build([entry(path, base, "2" * 40)])["repositories"][0]["scopes"]["committed_range"]["reason"] == "head_missing"


def test_repository_that_is_gone_is_unavailable(world):
    document = build([{"repository_root": str(world.root / "gone"), "worktree": str(world.root / "gone"),
                       "git_common_dir": str(world.root / "gone" / ".git"), "base_commit": None, "head_commit": None}])
    assert document["repositories"][0]["state"] == "unavailable" and document["capture"]["state"] == "incomplete"


# --- isolation --------------------------------------------------------------------------------


def test_repository_configuration_cannot_run_anything(repo, world):
    path, base = repo
    tip = commit(path, "attrs", **{".gitattributes": "* filter=evil\n*.txt diff=evil\n"}, t="text\n")
    marks = world.root / "marks"
    marks.mkdir()
    for name, value in {"filter.evil.clean": f"touch {marks}/clean", "filter.evil.smudge": f"touch {marks}/smudge",
                        "filter.evil.process": f"touch {marks}/process", "filter.evil.required": "true",
                        "diff.external": f"touch {marks}/external", "diff.evil.textconv": f"touch {marks}/textconv",
                        "core.fsmonitor": f"touch {marks}/fsmonitor", "core.hooksPath": str(world.root / "hooks"),
                        "core.pager": f"touch {marks}/pager", "diff.evil.command": f"touch {marks}/command"}.items():
        git(path, "config", name, value)
    (path / "f1").write_text("changed\n")
    os.utime(path / "f2", (1_000_000_000, 1_000_000_000))  # stat-dirty only, so a refresh rehashes it
    (path / "t.txt").write_text("text\n")
    document = build([entry(path, base, tip)])
    assert list(marks.iterdir()) == []
    assert "changed" in text_of(rows(document, "working_tree")["f1"]) and "f2" not in rows(document, "working_tree")
    # The control: the repository's own Git does run its configured integrations.
    git(path, "diff", "HEAD", check=False)
    git(path, "status", check=False)
    assert list(marks.iterdir())


def test_replace_refs_are_ignored(repo):
    path, base = repo
    tip = commit(path, "change", f1="a\nb\nc\n")
    tree = git(path, "mktree", input=b"")
    other = git(path, "commit-tree", tree, "-m", "other")
    git(path, "replace", tip, other)
    document = build([entry(path, base, tip)])
    assert set(rows(document, "committed_range")) == {"f1"}


def test_symlinks_are_never_followed(repo, world):
    path, base = repo
    outside = world.root / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("SECRET-SENTINEL\n")
    os.symlink("f1", path / "link")
    tip = commit(path, "link")
    os.unlink(path / "link")
    os.symlink(outside / "secret.txt", path / "link")
    os.symlink(outside, path / "dirlink")
    document = build([entry(path, base, tip)])
    working = rows(document, "working_tree")
    assert set(working) == {"link", "dirlink"} and str(outside / "secret.txt") in text_of(working["link"])
    assert "SECRET-SENTINEL" not in json.dumps(document) and "SECRET-SENTINEL" not in "".join(
        text_of(row) or "" for row in working.values())


def test_submodule_rows_have_no_text_and_submodule_config_never_runs(world):
    sub = init_repo(world.root / "sub")
    sub_head = commit(sub, "sub", s="s\n")
    path = init_repo(world.root / "super")
    base = commit(path, "base", f="1\n")
    git(path, "-c", "protocol.file.allow=always", "submodule", "add", "-q", str(sub), "sm")
    tip = commit(path, "add submodule")
    marks = world.root / "marks"
    marks.mkdir()
    git(path / "sm", "config", "core.fsmonitor", f"touch {marks}/fsmonitor")
    git(path / "sm", "config", "core.hooksPath", str(world.root / "hooks"))
    commit(sub, "newer", s="2\n")
    git(path / "sm", "-c", "protocol.file.allow=always", "pull", "-q", check=False)
    (path / "sm" / "extra").write_text("dirty\n")
    for mark in marks.iterdir():  # the fixture's own Git runs the configured fsmonitor: the control
        mark.unlink()
    document = build([entry(path, base, tip)])
    row = rows(document, "committed_range")["sm"]
    assert row["status"] == "submodule" and row["content"] == {"state": "not_captured", "reason": "submodule", "bytes": 0}
    assert ".gitmodules" in rows(document, "committed_range")
    assert list(marks.iterdir()) == [] and sub_head


def test_repointed_git_file_is_unverified_but_committed_scope_still_captures(world, repo):
    path, base = repo
    git(path, "branch", "other")
    linked = world.root / "linked"
    git(path, "worktree", "add", "-q", str(linked), "-b", "linked")
    tip = commit(linked, "in linked", g="1\n")
    common = git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    good = build([entry(path, base, tip, worktree=linked, common=common)])
    assert scopes(good)["working_tree"]["state"] == "available"
    other = world.root / "other-linked"
    git(path, "worktree", "add", "-q", str(other), "other")
    for target in (str(world.root / "nowhere"), os.path.join(common, "worktrees", "other-linked")):
        (linked / ".git").write_text(f"gitdir: {target}\n")
        document = build([entry(path, base, tip, worktree=linked, common=common)])
        assert scopes(document)["working_tree"]["reason"] == "git_dir_unverified"
        assert scopes(document)["committed_range"]["state"] == "available" and "g" in rows(document, "committed_range")


def test_symlinked_git_directory_is_unverified(world, repo):
    path, base = repo
    real = world.root / "elsewhere"
    os.rename(path / ".git", real)
    os.symlink(real, path / ".git")
    document = build([simple_entry(path, base, common=str(path / ".git"))])
    assert scopes(document)["working_tree"]["reason"] == "git_dir_unverified"


def test_verified_git_dir_forms(world, repo):
    path, _ = repo
    common = git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    assert capture.verified_git_dir(str(path), common) == common
    assert capture.verified_git_dir(str(path), common + "/other") is None
    linked = world.root / "wt"
    git(path, "worktree", "add", "-q", str(linked), "-b", "wt")
    assert capture.verified_git_dir(str(linked), common) == os.path.join(common, "worktrees", "wt")
    assert capture.verified_git_dir(str(world.root / "missing"), common) is None


# --- index handling ---------------------------------------------------------------------------


def test_original_index_is_never_modified(repo):
    path, base = repo
    index = path / ".git" / "index"
    (path / "f1").write_text("a\nb\nchanged\n")
    os.utime(path / "f2", (1_000_000_000, 1_000_000_000))
    before = (index.read_bytes(), index.stat().st_mtime_ns)
    document = build([simple_entry(path, base)])
    assert (index.read_bytes(), index.stat().st_mtime_ns) == before
    assert set(rows(document, "working_tree")) == {"f1"}


def test_split_index_is_copied_with_its_shared_part(repo):
    path, base = repo
    git(path, "update-index", "--split-index")
    (path / "f1").write_text("a\nb\nsplit\n")
    git(path, "add", "f1")
    shared = sorted(p.name for p in (path / ".git").glob("sharedindex.*"))
    assert shared
    index = (path / ".git" / "index").read_bytes()
    assert capture._scan_index(index, 20, capture._Budget(5)).shared in shared
    document = build([simple_entry(path, base)])
    assert "split" in text_of(rows(document, "working_tree")["f1"])
    for name in shared:
        (path / ".git" / name).unlink()
    assert scopes(build([simple_entry(path, base)]))["working_tree"]["reason"] == "index_unsupported"


def test_shared_index_name_is_none_for_a_plain_index(repo):
    path, _ = repo
    scan = capture._scan_index((path / ".git" / "index").read_bytes(), 20, capture._Budget(5))
    assert scan.shared is None and not scan.unsafe and scan.flagged == []
    with pytest.raises(capture._IndexUnsupported):
        capture._scan_index(b"nonsense" * 10, 20, capture._Budget(5))


def test_index_too_large(repo, monkeypatch):
    path, base = repo
    monkeypatch.setattr(capture, "INDEX_LIMIT", 10)
    scope = scopes(build([simple_entry(path, base)]))["working_tree"]
    assert (scope["state"], scope["reason"]) == ("not_captured", "index_too_large")


def wrapper(world, body):
    script = world.root / "git-wrapper.sh"
    script.write_text("#!/bin/sh\n" + body + '\nexec git "$@"\n')
    script.chmod(0o755)
    return (str(script),)


def test_failed_refresh_reports_no_modified_entries(repo, world, monkeypatch):
    path, base = repo
    (path / "f1").write_text("changed\n")
    monkeypatch.setattr(capture, "GIT", wrapper(world, 'case "$*" in *update-index*) exit 1;; esac'))
    scope = scopes(build([simple_entry(path, base)]))["working_tree"]
    assert (scope["state"], scope["reason"], scope["rows"]) == ("incomplete", "index_refresh_failed", [])


# --- budgets and streams ----------------------------------------------------------------------


def test_deadline_in_a_command_marks_that_scope_incomplete(repo, world, monkeypatch):
    path, base = repo
    (path / "f1").write_text("changed\n")
    monkeypatch.setattr(capture, "GIT", wrapper(world, 'case "$*" in *diff-index*) sleep 5;; esac'))
    started = time.monotonic()
    document = build([simple_entry(path, base)], budget_seconds=1.0)
    assert time.monotonic() - started < 4
    assert (scopes(document)["working_tree"]["state"], scopes(document)["working_tree"]["reason"]) == ("incomplete", "capture_timeout")
    assert scopes(document)["committed_range"]["state"] == "available"
    assert document["repositories"][0]["state"] == "incomplete" and document["capture"] == {"state": "incomplete", "reason": "capture_timeout"}


def test_partial_patch_at_the_deadline_is_truncated_with_that_reason(repo, world, monkeypatch):
    path, base = repo
    tip = commit(path, "two", a="1\n", b="2\n")
    monkeypatch.setattr(capture, "GIT", (str(world.root / "slow.sh"),))
    script = world.root / "slow.sh"
    script.write_text('#!/bin/sh\ngit "$@"\ncase "$*" in *" -p "*) sleep 5;; esac\n')
    script.chmod(0o755)
    document = build([entry(path, base, tip)], budget_seconds=1.0)
    committed = scopes(document)["committed_range"]
    assert (committed["state"], committed["reason"]) == ("incomplete", "capture_timeout")
    last = committed["rows"][-1]["content"]
    assert last["state"] == "truncated" and last["reason"] == "capture_timeout"
    assert committed["rows"][0]["content"]["state"] == "available"


def test_exhausted_budget_leaves_repositories_unstarted(repo):
    path, base = repo
    document = build([simple_entry(path, base)], budget_seconds=0)
    repository = document["repositories"][0]
    assert (repository["state"], repository["reason"]) == ("incomplete", "budget_exhausted")
    assert all(scope["state"] == "not_captured" and scope["reason"] == "budget_exhausted" for scope in repository["scopes"].values())


def test_output_ceiling_is_distinct_from_the_deadline(repo, monkeypatch):
    path, base = repo
    tip = commit(path, "big", f1="a\nb\n" + "line\n" * 2000)
    monkeypatch.setattr(capture, "COMMAND_CEILING", 3000)
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert (committed["state"], committed["reason"]) == ("incomplete", "output_ceiling")
    row = committed["rows"][0]["content"]
    assert row["state"] == "truncated" and row["reason"] == "output_ceiling"
    monkeypatch.setattr(capture, "COMMAND_CEILING", 40)
    listing = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert (listing["state"], listing["reason"], listing["rows"]) == ("incomplete", "output_ceiling", [])


def test_retained_text_limits_do_not_make_a_scope_incomplete(repo, monkeypatch):
    path, base = repo
    line = ("x" * 99 + "\n")
    tip = commit(path, "big", one=line * 1000, two=line * 1000, three=line * 1000)
    monkeypatch.setattr(capture, "TASK_TEXT_LIMIT", 90 * 1024)
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert committed["state"] == "available" and committed["reason"] is None
    by_name = {capture_row_path(row): row["content"] for row in committed["rows"]}
    assert by_name["one"]["state"] == "truncated" and by_name["one"]["reason"] == "file_limit" and by_name["one"]["bytes"] == shape.MAX_TEXT_BYTES
    assert by_name["three"]["state"] == "truncated" and by_name["three"]["reason"] == "task_limit"
    assert by_name["two"]["state"] == "omitted_size"
    assert sum(content["bytes"] for content in by_name.values()) <= 90 * 1024


def test_a_megabyte_per_task_default(repo):
    path, base = repo
    files = {f"f{index}": ("y" * 99 + "\n") * 700 for index in range(40)}
    tip = commit(path, "many", **files)
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert sum(row["content"]["bytes"] for row in committed["rows"]) <= capture.TASK_TEXT_LIMIT
    assert {row["content"]["state"] for row in committed["rows"]} == {"truncated", "omitted_size"}


def test_rows_are_capped_at_five_hundred_including_untracked(repo):
    path, base = repo
    tip = commit(path, "wide", **{f"c{index:04d}": f"{index}\n" for index in range(520)})
    for index in range(10):
        (path / f"u{index}").write_text("u")
    document = build([entry(path, base, tip)])
    committed = scopes(document)["committed_range"]
    assert committed["row_count"] == 500 and committed["rows_truncated"] is True
    assert all(row["content"]["state"] == "available" for row in committed["rows"])
    assert [capture_row_path(row) for row in committed["rows"]] == sorted(capture_row_path(row) for row in committed["rows"])
    working = scopes(document)["working_tree"]
    assert working["row_count"] == 10 and working["rows_truncated"] is False


def test_patch_misalignment_keeps_counts_without_text(repo, world, monkeypatch):
    path, base = repo
    tip = commit(path, "two", a="1\n", b="2\n")
    script = world.root / "extra.sh"
    script.write_text('#!/bin/sh\ngit "$@"\ncase "$*" in *" -p "*) echo "diff --git a/zz b/zz";; esac\n')
    script.chmod(0o755)
    monkeypatch.setattr(capture, "GIT", (str(script),))
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert (committed["state"], committed["reason"]) == ("incomplete", "patch_alignment_failed")
    for row in committed["rows"]:
        assert row["additions"] == 1 and row["content"] == {"state": "not_captured", "reason": "patch_alignment_failed", "bytes": 0}


def test_mode_only_change_keeps_alignment(repo):
    path, base = repo
    os.chmod(path / "f1", 0o755)
    tip = commit(path, "mode", g="1\n")
    document = build([entry(path, base, tip)])
    assert rows(document, "committed_range")["f1"]["additions"] == 0
    assert "old mode" in text_of(rows(document, "committed_range")["f1"]) or "new mode" in text_of(rows(document, "committed_range")["f1"])
    assert text_of(rows(document, "committed_range")["g"]).startswith("diff --git a/g b/g")


def test_path_longer_than_the_cap_is_truncated_and_has_no_text(repo):
    path, base = repo
    long_name = "/".join(["d" * 200] * 25) + "/leaf"  # about 5000 bytes, never created on disk
    blob = git(path, "hash-object", "-w", "--stdin", input=b"content\n")
    tree_index = path / ".git" / "scratch-index"
    environment = {**os.environ, "GIT_INDEX_FILE": str(tree_index)}
    subprocess.run(["git", "read-tree", base], cwd=path, env=environment, check=True)
    subprocess.run(["git", "update-index", "--add", "--cacheinfo", f"100644,{blob},{long_name}"], cwd=path, env=environment, check=True)
    tree = subprocess.run(["git", "write-tree"], cwd=path, env=environment, check=True, capture_output=True, text=True).stdout.strip()
    tip = git(path, "commit-tree", tree, "-p", base, "-m", "long")
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    long_row = next(row for row in committed["rows"] if row["path_truncated"])
    assert len(shape.row_path_bytes(long_row)) <= shape.MAX_PATH_BYTES
    assert long_row["content"] == {"state": "not_captured", "reason": "path_too_long", "bytes": 0}


def test_non_utf8_path_is_stored_as_base64(repo):
    path, base = repo
    raw = b"bad-\xff-name"
    try:
        (path / os.fsdecode(raw)).write_text("1\n")
    except OSError:
        pytest.skip("the filesystem rejects non-UTF-8 names")
    tip = commit(path, "bad name")
    row = scopes(build([entry(path, base, tip)]))["committed_range"]["rows"][0]
    assert row["path"] is None and base64.b64decode(row["path_bytes_b64"]) == raw


def test_path_encoding_helpers():
    assert capture._encode_path("héllo".encode()) == ("héllo", None, False)
    assert capture._encode_path(b"a\xffb") == (None, base64.b64encode(b"a\xffb").decode(), False)
    text, encoded, truncated = capture._encode_path(("é" * 3000).encode())
    assert truncated and encoded is None and len(text.encode()) <= shape.MAX_PATH_BYTES
    assert capture._encode_path(b"\xff" * 5000)[1] is not None
    assert capture._trim_incomplete_utf8(bytearray("aé".encode()[:2])) == b"a"


def test_listing_parser_and_splitter_forms():
    raw = (b":100644 100644 " + b"1" * 40 + b" " + b"0" * 40 + b" M\0a b\0:000000 160000 " + b"0" * 40 + b" " + b"2" * 40 + b" A\0sub\0")
    parsed = capture._parse_raw(raw)
    assert [(e.path, e.status, e.forced) for e in parsed] == [(b"a b", "modified", None), (b"sub", "submodule", "submodule")]
    assert capture._parse_raw(raw[:-5]) is None
    assert [e.path for e in capture._parse_raw(raw[:-5], partial=True)] == [b"a b"]
    assert capture._parse_stat(b"1\t2\ta b\0-\t-\tsub\0") == {b"a b": (1, 2, False), b"sub": (None, None, True)}
    assert capture._parse_stat(b"1\t2\ta b") == {} and capture._parse_stat(b"x\0") is None
    budget = capture._Budget(5)
    splitter = capture._PatchSplitter([True, False, True], budget)
    patch = (b"diff --git a/x b/x\nnew file mode 100644\n--- /dev/null\n+++ b/x\n@@ -0,0 +1 @@\n+hi\n"
             b"diff --git a/y b/y\nBinary files a/y and b/y differ\n" b"* Unmerged path z\n")
    for index in range(0, len(patch), 7):  # arbitrary chunk boundaries
        splitter.feed(patch[index:index + 7])
    splitter.finish(True)
    assert [segment.size > 0 for segment in splitter.segments] == [True, True, True] and not splitter.bad
    assert bytes(splitter.segments[0].kept).endswith(b"+hi\n") and splitter.segments[1].kept == b""
    assert bytes(splitter.segments[2].header) == b"* Unmerged path z" and budget.text_left < capture.TASK_TEXT_LIMIT
    early = capture._PatchSplitter([True], capture._Budget(5))
    early.feed(b"stray\n")
    assert early.bad


# --- storage ----------------------------------------------------------------------------------


def published(world, repositories=None, **seed):
    repo_path = world.root / "stored"
    if not repo_path.exists():
        init_repo(repo_path)
        base = commit(repo_path, "base", f="1\n")
        tip = commit(repo_path, "next", f="1\n2\n")
        repositories = repositories or [entry(repo_path, base, tip)]
    seed_task(world.tasks, repositories, **seed)
    return world.tasks / f"{TASK}{shape.SUFFIX}"


def test_published_document_is_private_valid_and_leaves_the_record_alone(world):
    seeded = published(world)
    record_file = store.record_path(world.tasks, TASK)
    before = (record_file.read_bytes(), record_file.stat().st_mtime_ns)
    assert capture.capture_task_changes(world.tasks, TASK) == "published"
    assert (record_file.read_bytes(), record_file.stat().st_mtime_ns) == before
    assert stat.S_IMODE(seeded.stat().st_mode) == 0o600 and list(world.tasks.glob("*.tmp")) == []
    document = json.loads(seeded.read_bytes())
    shape.validate(document, TASK)
    assert document["format"] == "harflow.task-changes" and document["v"] == 1
    assert "+2" in text_of(rows(document, "committed_range")["f"])


def test_capture_never_writes_a_record(world, monkeypatch):
    published(world)

    def refuse(*args, **kwargs):
        raise AssertionError("capture must not write the record")
    monkeypatch.setattr(store, "write", refuse)
    monkeypatch.setattr(store, "write_landed", refuse)
    assert capture.capture_task_changes(world.tasks, TASK) == "published"


@pytest.mark.parametrize("seed", [{"exit_code": None}, {"status": "running", "exit_code": None}])
def test_nothing_to_capture_is_skipped(world, seed):
    published(world, **seed)
    assert capture.capture_task_changes(world.tasks, TASK) == "skipped"
    assert not (world.tasks / f"{TASK}{shape.SUFFIX}").exists()


@pytest.mark.parametrize("repositories", [None, []])
def test_task_without_repositories_is_skipped(world, repositories):
    seed_task(world.tasks, repositories)
    assert capture.capture_task_changes(world.tasks, TASK) == "skipped"


def test_capture_exception_publishes_a_capture_error_document(world, monkeypatch):
    seeded = published(world)

    def explode(*args, **kwargs):
        raise RuntimeError("boom")
    monkeypatch.setattr(capture, "build_document", explode)
    assert capture.capture_task_changes(world.tasks, TASK) == "failure_document"
    document = json.loads(seeded.read_bytes())
    shape.validate(document, TASK)
    assert document["capture"] == {"state": "unavailable", "reason": "capture_error"} and document["repositories"] == []
    assert not list(world.tasks.glob("*supervisor-error*"))


def test_oversized_document_is_replaced_by_capture_too_large(world, monkeypatch):
    seeded = published(world)
    monkeypatch.setattr(shape, "MAX_DOCUMENT_BYTES", 500)
    assert capture.capture_task_changes(world.tasks, TASK) == "failure_document"
    assert json.loads(seeded.read_bytes())["capture"] == {"state": "unavailable", "reason": "capture_too_large"}


def test_busy_record_lock_means_nothing_is_published(world, monkeypatch):
    seeded = published(world)
    monkeypatch.setattr(capture, "LOCK_RETRY_SECONDS", 0.05)
    holder = os.open(world.tasks / f"{TASK}.lock", os.O_CREAT | os.O_RDWR, 0o644)
    fcntl.flock(holder, fcntl.LOCK_EX)
    try:
        assert capture.capture_task_changes(world.tasks, TASK) == "not_published"
    finally:
        os.close(holder)
    assert not seeded.exists() and list(world.tasks.glob("*.tmp")) == []


def test_record_removed_or_replaced_during_capture_is_not_published(world, monkeypatch):
    seeded = published(world)
    real = capture.build_document

    def remove_record(*args, **kwargs):
        document = real(*args, **kwargs)
        store.record_path(world.tasks, TASK).unlink()
        return document
    monkeypatch.setattr(capture, "build_document", remove_record)
    assert capture.capture_task_changes(world.tasks, TASK) == "not_published"
    assert not seeded.exists()
    seed_task(world.tasks, published_entries(world))

    def replace_record(*args, **kwargs):
        document = real(*args, **kwargs)
        seed_task(world.tasks, published_entries(world), finished_at="2027-01-01T00:00:11+00:00")
        return document
    monkeypatch.setattr(capture, "build_document", replace_record)
    assert capture.capture_task_changes(world.tasks, TASK) == "not_published"
    assert not seeded.exists()


def published_entries(world):
    repo_path = world.root / "stored"
    return [entry(repo_path, git(repo_path, "rev-parse", "HEAD~1"), head(repo_path))]


def test_record_lock_is_held_while_the_document_is_written(world, monkeypatch):
    seeded = published(world)
    observed = []
    real = capture._write_private

    def probe(path, data, layout):
        contender = os.open(world.tasks / f"{TASK}.lock", os.O_RDWR)
        try:
            fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
            observed.append("acquired")
        except BlockingIOError:
            observed.append("blocked")
        finally:
            os.close(contender)
        real(path, data, layout)
    monkeypatch.setattr(capture, "_write_private", probe)
    assert capture.capture_task_changes(world.tasks, TASK) == "published"
    assert observed == ["blocked"] and seeded.exists()


def test_retention_deletes_the_document_and_an_orphaned_temporary_file(world):
    from harflow_mcp import retention
    seeded = published(world)
    assert capture.capture_task_changes(world.tasks, TASK) == "published"
    orphan = world.tasks / f"{TASK}{shape.SUFFIX}.0123456789abcdef.tmp"
    orphan.write_text("{")
    assert retention._delete_task_files_locked(world.tasks, TASK)
    assert not seeded.exists() and not orphan.exists()
    assert not store.record_path(world.tasks, TASK).exists()


# --- supervisor hook --------------------------------------------------------------------------


class _Task:
    task_id = TASK


def test_hook_contains_failures_and_propagates_ownership_loss(world, monkeypatch):
    def explode(*args):
        raise RuntimeError("boom")
    monkeypatch.setattr(task_supervisor.task_changes_capture, "capture_task_changes", explode)
    asyncio.run(task_supervisor._capture_changes(world.tasks, _Task()))
    assert not list(world.tasks.glob("*supervisor-error*"))

    def lost(*args):
        raise state_ownership.StateOwnershipError("lost")
    monkeypatch.setattr(task_supervisor.task_changes_capture, "capture_task_changes", lost)
    with pytest.raises(state_ownership.StateOwnershipError):
        asyncio.run(task_supervisor._capture_changes(world.tasks, _Task()))


def test_hook_runs_the_capture_off_the_event_loop(world):
    published(world)
    asyncio.run(task_supervisor._capture_changes(world.tasks, _Task()))
    assert (world.tasks / f"{TASK}{shape.SUFFIX}").exists()


def test_hook_follows_settlement_in_the_supervisor():
    source = inspect.getsource(task_supervisor.run)
    settled = source.index("state='settled'")
    hook = source.index("_capture_changes(")
    assert settled < hook
    assert "task.exit_code is not None and task.latest_record_landed" in source[settled:hook + 200]


# --- continuations ----------------------------------------------------------------------------


def test_continuations_are_launched_as_their_own_supervised_processes(world, monkeypatch):
    """A supervised registry never runs a task in the launching process: `_spawn` hands every launch,
    including `continue_task`'s, to `task_supervision.launch`, which execs a new `task_supervisor`
    owner. That owner's `run()` carries the capture hook, so a continuation is captured by its own
    supervisor after it settles; the parent's `registry._continuations` jobs only await the launch."""
    from harflow_mcp import disk_space, task_supervision
    from harflow_mcp.tasks import TaskRegistry
    launched = []

    async def fake_launch(registry, invocation, kwargs):
        launched.append(kwargs.get("continuation_of_task_id", "n/a"))
        return "child"
    monkeypatch.setattr(task_supervision, "launch", fake_launch)
    monkeypatch.setattr(disk_space, "check_launch_space", lambda *a, **k: None)
    registry = TaskRegistry(log_dir=world.tasks, execution_mode="supervised")
    assert asyncio.run(registry._spawn(object(), repo_path=world.root, continuation_of_task_id="parent")) == "child"
    assert launched == ["parent"]
    assert "execution_mode == \"supervised\"" in inspect.getsource(TaskRegistry._spawn)
    assert "_execute_prepared" not in inspect.getsource(TaskRegistry._spawn).split("supervised")[1].split("return await launch")[0]


# --- hostile indexes --------------------------------------------------------------------------


def forge_index(entries, *, flags_for=None):
    """A version 2 index with exactly these `(name, oid)` entries, bypassing Git's own path checks."""
    import struct
    body = b"DIRC" + struct.pack(">II", 2, len(entries))
    for name, oid in sorted(entries):
        fixed = struct.pack(">10I", 0, 0, 0, 0, 0, 0, 0o100644, 0, 0, 3) + bytes.fromhex(oid) + struct.pack(">H", min(len(name), 0xFFF))
        entry = fixed + name + b"\0"
        body += entry + b"\0" * (-len(entry) % 8)
    import hashlib
    return body + hashlib.sha1(body).digest()


@pytest.mark.parametrize("name", [b"../secret.txt", b".git/config", b".GIT/x", b"a/./b", b"a/../b", b"/etc/passwd", b"a//b",
                                  b".git ", b".git.", b"git~1/x", ".g‌it/x".encode(), b"a\\b", b".git::$INDEX_ALLOCATION/x", b""])
def test_unsafe_index_names_are_recognised(name):
    assert capture.unsafe_index_path(name)


@pytest.mark.parametrize("name", [b"a", b"dir/file.txt", b".github/workflows/x.yml", b".gitignore", b"foo.git/x", b"gitx", b"a b/c"])
def test_ordinary_index_names_are_accepted(name):
    assert not capture.unsafe_index_path(name)


@pytest.mark.parametrize("name", [b"../secret.txt", b".git/config", b"sub/../../secret.txt"])
def test_index_entry_outside_the_work_tree_is_never_read(repo, world, name):
    path, base = repo
    (world.root / "secret.txt").write_text("OUTSIDE-SENTINEL\n")
    blob = git(path, "hash-object", "-w", str(world.root / "secret.txt"))
    index = path / ".git" / "index"
    index.write_bytes(forge_index([(b"f1", git(path, "rev-parse", "HEAD:f1")), (name, blob)]))
    document = build([simple_entry(path, base)])
    scope = scopes(document)["working_tree"]
    assert (scope["state"], scope["reason"], scope["rows"]) == ("not_captured", "index_unsafe_path", [])
    assert "OUTSIDE-SENTINEL" not in json.dumps(document) and scopes(document)["committed_range"]["state"] == "available"


def test_unsafe_name_in_a_shared_index_is_refused(repo):
    import hashlib
    path, base = repo
    git(path, "update-index", "--split-index")
    git(path, "add", "-A")
    git_dir = path / ".git"
    shared = next(git_dir.glob("sharedindex.*"))
    forged = forge_index([(b"../x", git(path, "rev-parse", "HEAD:f1"))])
    digest = forged[-20:]
    index = bytearray((git_dir / "index").read_bytes())
    position = bytes(index).index(bytes.fromhex(shared.name.split(".")[1]))
    index[position:position + 20] = digest
    index[-20:] = hashlib.sha1(bytes(index[:-20])).digest()
    (git_dir / ("sharedindex." + digest.hex())).write_bytes(forged)
    (git_dir / "index").write_bytes(bytes(index))
    scope = scopes(build([simple_entry(path, base)]))["working_tree"]
    assert (scope["state"], scope["reason"]) == ("not_captured", "index_unsafe_path")


def test_leading_directory_symlink_leaks_nothing(world):
    path = init_repo(world.root / "link-repo")
    base = commit(path, "base", **{"d/secret.txt": "committed\n", "f": "1\n"})
    outside = world.root / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("LEAK-SENTINEL\n")
    import shutil
    shutil.rmtree(path / "d")
    os.symlink(outside, path / "d")
    document = build([simple_entry(path, base)])
    assert "LEAK-SENTINEL" not in json.dumps(document)
    assert rows(document, "working_tree")["d/secret.txt"]["status"] == "deleted"


def test_missing_index_is_not_captured(repo):
    path, base = repo
    (path / ".git" / "index").unlink()
    scope = scopes(build([simple_entry(path, base)]))["working_tree"]
    assert (scope["state"], scope["reason"], scope["rows"]) == ("not_captured", "index_missing", [])


def test_assume_unchanged_and_skip_worktree_cannot_hide_edits(repo):
    path, base = repo
    git(path, "update-index", "--assume-unchanged", "f1")
    git(path, "update-index", "--skip-worktree", "f2")
    stamp = (path / "f1").stat()
    (path / "f1").write_text("A\nB\n")  # same size; mtime restored below so stat data cannot give it away
    os.utime(path / "f1", ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    (path / "f2").write_text("changed\n")
    before = (path / ".git" / "index").read_bytes()
    document = build([simple_entry(path, base)])
    working = rows(document, "working_tree")
    assert set(working) == {"f1", "f2"} and "+A" in text_of(working["f1"]) and "+changed" in text_of(working["f2"])
    assert (path / ".git" / "index").read_bytes() == before


def test_skip_worktree_entries_without_files_stay_quiet(repo):
    path, base = repo
    git(path, "update-index", "--skip-worktree", "f2")
    (path / "f2").unlink()
    assert rows(build([simple_entry(path, base)]), "working_tree") == {}


def test_flags_in_a_split_index_are_cleared_too(repo):
    path, base = repo
    git(path, "update-index", "--split-index")
    git(path, "update-index", "--assume-unchanged", "f1")
    (path / "f1").write_text("split-edit\n")
    git(path, "update-index", "--split-index")
    document = build([simple_entry(path, base)])
    assert "+split-edit" in text_of(rows(document, "working_tree")["f1"])


# --- trust anchors ----------------------------------------------------------------------------


def seeded(world, repositories, **extra):
    return seed_task(world.tasks, repositories, **extra)


def test_dispatch_anchor_rejects_a_different_repository_identity(world, repo):
    path, base = repo
    other = init_repo(world.root / "other")
    commit(other, "x", f="1\n")
    tip = commit(path, "next", g="1\n")
    common = git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    seeded(world, [entry(path, base, tip)], worktree={"path": str(path), "source_repo_path": str(other)},
           git_baseline={"repository_root": str(path)})
    assert capture.capture_task_changes(world.tasks, TASK) == "published"
    document = json.loads((world.tasks / f"{TASK}{shape.SUFFIX}").read_bytes())
    assert scopes(document)["working_tree"]["reason"] == "repository_identity_changed"
    assert scopes(document)["committed_range"]["state"] == "available" and common


def test_dispatch_anchor_accepts_the_recorded_source_repository(world, repo):
    path, base = repo
    tip = commit(path, "next", g="1\n")
    seeded(world, [entry(path, base, tip)], git_baseline={"repository_root": str(path)})
    assert capture.capture_task_changes(world.tasks, TASK) == "published"
    document = json.loads((world.tasks / f"{TASK}{shape.SUFFIX}").read_bytes())
    assert scopes(document)["working_tree"]["state"] == "available"


def test_unanchored_repository_with_alternates_is_not_captured(repo, world):
    path, base = repo
    (path / ".git" / "objects" / "info").mkdir(exist_ok=True)
    (path / ".git" / "objects" / "info" / "alternates").write_text(str(world.root / "elsewhere") + "\n")
    repository = build([simple_entry(path, base)])["repositories"][0]
    assert (repository["state"], repository["reason"]) == ("not_captured", "alternates_present")
    assert all(scope["state"] == "not_captured" and scope["rows"] == [] for scope in repository["scopes"].values())
    anchored = build([simple_entry(path, base)], anchors={os.path.realpath(path): None})["repositories"][0]
    assert anchored["state"] == "available"


# --- document bounds --------------------------------------------------------------------------


def test_repository_strings_are_bounded_and_flagged(repo):
    path, base = repo
    long_value = "p" + "\x01" * 3000
    repository = build([{**simple_entry(path, base), "base_provenance": long_value}])["repositories"][0]
    assert shape.escaped_size(repository["base_provenance"]) <= shape.MAX_REPOSITORY_FIELD
    assert repository["fields_truncated"] is True
    assert build([simple_entry(path, base)])["repositories"][0]["fields_truncated"] is False


# --- typechange -------------------------------------------------------------------------------


def test_typechange_in_both_scopes_keeps_alignment(repo):
    path, base = repo
    os.unlink(path / "f2")
    os.symlink("f1", path / "f2")
    tip = commit(path, "file to symlink", other="1\n")
    committed = rows(build([entry(path, base, tip)]), "committed_range")
    assert committed["f2"]["status"] == "typechange" and "+f1" in text_of(committed["f2"]) and "-x" in text_of(committed["f2"])
    assert committed["other"]["content"]["state"] == "available"
    os.unlink(path / "f2")
    (path / "f2").write_text("regular again\n")
    document = build([entry(path, base, tip)])
    working = rows(document, "working_tree")
    assert working["f2"]["status"] == "typechange" and "regular again" in text_of(working["f2"])
    assert scopes(document)["working_tree"]["state"] == "available"
    assert text_of(working["f2"]).count("diff --git") == 2


# --- exclusions -------------------------------------------------------------------------------


def test_info_exclude_global_excludes_and_directories(repo, world, monkeypatch):
    path, base = repo
    (path / ".git" / "info" / "exclude").write_text("build/\n")
    (path / "build").mkdir()
    for index in range(600):
        (path / "build" / f"o{index}.o").write_text("x")
    (path / "f1").write_text("edit\n")
    (path / "noise.log").write_text("x")
    (path / "newdir").mkdir()
    for name in "abc":
        (path / "newdir" / name).write_text("x")
    global_config = world.root / "global.gitconfig"
    ignore = world.root / "global-ignore"
    ignore.write_text("*.log\n")
    global_config.write_text(f"[core]\n\texcludesFile = {ignore}\n")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))
    scope = scopes(build([simple_entry(path, base)], budget_seconds=30))["working_tree"]
    assert [capture_row_path(row) for row in scope["rows"]] == ["f1", "newdir/"]
    assert scope["rows_truncated"] is False and scope["state"] == "available"


def test_tracked_rows_are_never_evicted_by_untracked_ones(repo):
    path, base = repo
    (path / "zzz-tracked-edit").write_text("1\n")
    git(path, "add", "zzz-tracked-edit")
    tip = commit(path, "tracked")
    (path / "zzz-tracked-edit").write_text("2\n")
    for index in range(600):
        (path / f"a{index:04d}").write_text("u")
    scope = scopes(build([entry(path, base, tip)], budget_seconds=30))["working_tree"]
    names = [capture_row_path(row) for row in scope["rows"]]
    assert scope["row_count"] == 500 and scope["rows_truncated"] is True and "zzz-tracked-edit" in names, (scope["state"], scope["reason"], len(names))


def test_allow_listed_core_booleans_are_honoured(repo):
    path, base = repo
    os.chmod(path / "f1", 0o755)
    assert "f1" in rows(build([simple_entry(path, base)]), "working_tree")
    git(path, "config", "core.filemode", "false")
    git(path, "config", "core.fsmonitor", "touch /nonexistent/never")
    assert rows(build([simple_entry(path, base)]), "working_tree") == {}


# --- processes --------------------------------------------------------------------------------


def test_capture_commands_run_with_a_disposable_home(repo, world, monkeypatch):
    path, base = repo
    out = world.root / "env.txt"
    monkeypatch.setattr(capture, "GIT", wrapper(world, f'echo "$HOME $XDG_CONFIG_HOME" >> {out}'))
    build([simple_entry(path, base)])
    seen = {line for line in out.read_text().splitlines()}
    assert seen and all(str(world.root / "home") not in line and "harflow_mcp-changes-" in line for line in seen)


def test_a_process_that_survives_the_kill_is_abandoned(world, monkeypatch):
    import threading
    release = threading.Event()
    reaped = []
    reader, writer = os.pipe()

    class Stuck:
        pid = 0
        returncode = None
        stdout = os.fdopen(reader, "rb")

        def __init__(self, *args, **kwargs):
            pass

        def poll(self):
            return None

        def wait(self, timeout=None):
            if timeout is not None:
                raise subprocess.TimeoutExpired("git", timeout)
            reaped.append(True)
            release.wait(5)

        def kill(self):
            pass
    monkeypatch.setattr(capture.subprocess, "Popen", Stuck)
    monkeypatch.setattr(capture, "_kill", lambda process: None)
    monkeypatch.setattr(capture, "REAP_SECONDS", 0.1)
    started = time.monotonic()
    result = capture._run(["git"], {}, str(world.root), capture._Budget(0.2), lambda chunk: None)
    assert time.monotonic() - started < 3 and result.cause == "capture_timeout"
    time.sleep(0.1)
    assert reaped == [True]
    release.set()
    os.close(writer)


def test_a_command_finishing_at_the_deadline_edge_is_not_a_timeout(world, monkeypatch):
    monkeypatch.setattr(capture, "EXIT_GRACE_SECONDS", 1.0)
    result = capture._run(["sh", "-c", "exec 1>&-; sleep 0.2"], dict(os.environ), str(world.root), capture._Budget(0.1), lambda chunk: None)
    assert (result.cause, result.returncode) == (None, 0)


def test_failed_numstat_keeps_rows_and_text(repo, world, monkeypatch):
    path, base = repo
    tip = commit(path, "two", a="1\n", b="2\n")
    monkeypatch.setattr(capture, "GIT", wrapper(world, 'case "$*" in *--numstat*) exit 3;; esac'))
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert (committed["state"], committed["reason"]) == ("incomplete", "git_failed")
    assert [row["additions"] for row in committed["rows"]] == [None, None]
    assert all(row["content"]["state"] == "available" for row in committed["rows"])


def test_listing_ceiling_keeps_the_complete_records_it_read(repo, monkeypatch):
    path, base = repo
    tip = commit(path, "many", **{f"file{index:02d}": "x\n" for index in range(30)})
    monkeypatch.setattr(capture, "COMMAND_CEILING", 700)
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert (committed["state"], committed["reason"], committed["rows_truncated"]) == ("incomplete", "output_ceiling", True)
    assert 0 < committed["row_count"] < 30
    assert all(row["content"] == {"state": "not_captured", "reason": "output_ceiling", "bytes": 0} for row in committed["rows"])


# --- degraded repositories --------------------------------------------------------------------


def test_dangling_shared_index_link_is_unsupported_not_a_crash(repo):
    path, base = repo
    git(path, "update-index", "--split-index")
    git(path, "add", "-A")
    for shared in (path / ".git").glob("sharedindex.*"):
        shared.unlink()
        os.symlink(path / "nowhere", shared)
    scope = scopes(build([simple_entry(path, base)]))["working_tree"]
    assert scope["state"] == "not_captured" and scope["reason"] in {"index_unsupported", "index_unreadable"}


def test_unusable_head_in_a_worktree_admin_directory_does_not_stop_capture(world, repo):
    path, base = repo
    linked = world.root / "wt-head"
    git(path, "worktree", "add", "-q", str(linked), "-b", "wt-head")
    tip = commit(linked, "in linked", g="1\n")
    common = git(path, "rev-parse", "--path-format=absolute", "--git-common-dir")
    admin = os.path.join(common, "worktrees", "wt-head")
    os.unlink(os.path.join(admin, "HEAD"))
    os.mkfifo(os.path.join(admin, "HEAD"))
    document = build([entry(path, base, tip, worktree=linked, common=common)])
    assert scopes(document)["committed_range"]["state"] == "available" and "g" in rows(document, "committed_range")


def test_a_failing_scope_does_not_lose_the_completed_one(repo, monkeypatch):
    path, base = repo
    tip = commit(path, "next", g="1\n")

    def explode(*args, **kwargs):
        raise RuntimeError("boom")
    monkeypatch.setattr(capture, "_working_tree_scope", explode)
    document = build([entry(path, base, tip)])
    assert scopes(document)["committed_range"]["state"] == "available" and "g" in rows(document, "committed_range")
    assert (scopes(document)["working_tree"]["state"], scopes(document)["working_tree"]["reason"]) == ("unavailable", "capture_error")


# --- cancellation and races -------------------------------------------------------------------


def test_cancelled_hook_leaves_an_inert_swept_temporary_file(world, monkeypatch):
    from harflow_mcp import retention
    published(world)
    started, release = __import__("threading").Event(), __import__("threading").Event()
    orphan = world.tasks / f"{TASK}{shape.SUFFIX}.0123456789abcdef.tmp"

    def interrupted(log_dir, task_id):
        orphan.write_text("{")
        started.set()
        release.wait(5)
        return "published"
    monkeypatch.setattr(task_supervisor.task_changes_capture, "capture_task_changes", interrupted)

    async def scenario():
        job = asyncio.ensure_future(task_supervisor._capture_changes(world.tasks, _Task()))
        while not started.is_set():
            await asyncio.sleep(0.01)
        job.cancel()
        with pytest.raises(asyncio.CancelledError):
            await job
        release.set()
    asyncio.run(scenario())
    from harflow_mcp import task_changes_read
    assert task_changes_read.load_document(world.tasks, TASK).state == "missing"
    assert retention._delete_task_files_locked(world.tasks, TASK) and not orphan.exists()


def test_a_record_rewritten_during_capture_with_the_same_identity_still_publishes(world, monkeypatch):
    published(world)
    real = capture.build_document

    def rewrite(*args, **kwargs):
        document = real(*args, **kwargs)
        import dataclasses
        store.write(world.tasks, dataclasses.replace(store.read(world.tasks, TASK, include_prompt=False), title="resumed meanwhile"))
        return document
    monkeypatch.setattr(capture, "build_document", rewrite)
    assert capture.capture_task_changes(world.tasks, TASK) == "published"


# --- end to end -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_supervised_task_stores_its_changes_after_settling(tmp_path, monkeypatch):
    """The production path: a detached owner runs a fake agent in a real repository, settles, then captures."""
    from harflow_mcp import task_supervision
    from harflow_mcp.backends import get
    from harflow_mcp.tasks import TaskRegistry
    from restart_test_support import install_fake_claude

    pin_git(monkeypatch, tmp_path / "home")
    install_fake_claude(tmp_path, monkeypatch, session="changes-e2e")
    repo = init_repo(tmp_path / "work")
    commit(repo, "base", f="1\n")
    (repo / "f").write_text("1\n2\n")
    log_dir = tmp_path / "state" / "tasks"
    registry = TaskRegistry(log_dir=log_dir, execution_mode="supervised")
    task_id = "c" * 32
    await registry.start("die 0", repo, backend=get("claude"), freedom="read_only", task_id=task_id)
    document_path = log_dir / f"{task_id}{shape.SUFFIX}"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline and not document_path.exists():
        await asyncio.sleep(0.2)
    assert document_path.exists(), "the owner never stored a changes document"
    document = json.loads(document_path.read_bytes())
    shape.validate(document, task_id)
    record = store.read(log_dir, task_id, include_prompt=False)
    assert record.exit_code is not None and stat.S_IMODE(document_path.stat().st_mode) == 0o600
    working = rows(document, "working_tree")
    assert "+2" in text_of(working["f"]) and scopes(document)["working_tree"]["state"] == "available"
    launch = task_supervision.read_launch(log_dir, task_id)
    assert launch["state"] == "settled"


# --- cut-off listings -------------------------------------------------------------------------

_RAW = (b":100644 100644 " + b"1" * 40 + b" " + b"0" * 40 + b" M\0one\0:100644 100644 " + b"2" * 40 + b" " + b"0" * 40 + b" M\0two\0")


_FIRST = len(b":100644 100644 " + b"1" * 40 + b" " + b"0" * 40 + b" M\0one\0")
_META = len(b":100644 100644 " + b"2" * 40 + b" " + b"0" * 40 + b" M")


@pytest.mark.parametrize("cut", [_FIRST + 1, _FIRST + 20, _FIRST + _META, _FIRST + _META + 1, _FIRST + _META + 2, len(_RAW) - 1])
def test_a_cut_raw_listing_keeps_every_completed_record(cut):
    """Cut inside the metadata, right after its NUL, and inside the pathname: only the unfinished record goes."""
    entries = capture._parse_raw(_RAW[:cut], partial=True)
    assert entries is not None and [entry.path for entry in entries] == [b"one"]
    assert capture._parse_raw(_RAW[:cut]) is None


def test_a_whole_listing_is_unchanged_by_partial_parsing():
    assert [entry.path for entry in capture._parse_raw(_RAW, partial=True)] == [b"one", b"two"]
    assert capture._parse_raw(b"", partial=True) == [] and capture._parse_raw(b"junk\0", partial=True) is None


@pytest.mark.parametrize("keep", [30, 60, 83, 85, 95, 140])
def test_a_listing_cut_by_the_deadline_keeps_its_rows_and_the_cause(repo, world, monkeypatch, keep):
    path, base = repo
    tip = commit(path, "many", **{f"file{index}": "x\n" for index in range(6)})
    script = world.root / "cut.sh"
    script.write_text(f'#!/bin/sh\ncase "$*" in *" --raw "*) git "$@" | head -c {keep}; sleep 5; exit 0;; esac\nexec git "$@"\n')
    script.chmod(0o755)
    monkeypatch.setattr(capture, "GIT", (str(script),))
    committed = scopes(build([entry(path, base, tip)], budget_seconds=0.8))["committed_range"]
    assert (committed["state"], committed["reason"]) == ("incomplete", "capture_timeout")
    assert committed["rows_truncated"] is True
    assert all(row["content"]["reason"] == "capture_timeout" for row in committed["rows"])


@pytest.mark.parametrize("ceiling", [60, 83, 85, 95, 140, 200])
def test_a_listing_cut_by_the_ceiling_keeps_its_rows_and_the_cause(repo, monkeypatch, ceiling):
    path, base = repo
    tip = commit(path, "many", **{f"file{index}": "x\n" for index in range(6)})
    monkeypatch.setattr(capture, "COMMAND_CEILING", ceiling)
    committed = scopes(build([entry(path, base, tip)]))["committed_range"]
    assert (committed["state"], committed["reason"]) == ("incomplete", "output_ceiling")
    assert committed["rows_truncated"] is True and committed["row_count"] == len(committed["rows"])


def test_a_cut_untracked_listing_keeps_the_tracked_rows(repo, world, monkeypatch):
    path, base = repo
    (path / "f1").write_text("edit\n")
    for index in range(5):
        (path / f"untracked{index}").write_text("u")
    script = world.root / "flood-others.sh"
    script.write_text('#!/bin/sh\ncase "$*" in *" --others "*) git "$@"; head -c 5000 /dev/zero | tr "\\0" "a"; exit 0;; esac\nexec git "$@"\n')
    script.chmod(0o755)
    monkeypatch.setattr(capture, "GIT", (str(script),))
    monkeypatch.setattr(capture, "COMMAND_CEILING", 2000)
    scope = scopes(build([simple_entry(path, base)]))["working_tree"]
    assert (scope["state"], scope["reason"], scope["rows_truncated"]) == ("incomplete", "output_ceiling", True)
    names = [capture_row_path(row) for row in scope["rows"]]
    assert names == ["f1"] + [f"untracked{index}" for index in range(5)]


def test_numstat_and_listing_parsers_never_return_unfinished_records():
    assert capture._parse_stat(b"1\t2\ta\0-\t-\tb") == {b"a": (1, 2, False)}
    assert capture._parse_stat(b"1\t2\ta\0-\t-\tb\0") == {b"a": (1, 2, False), b"b": (None, None, True)}


# --- typechange to and from a submodule -------------------------------------------------------


def gitlink_commit(path, name, parent, target):
    index = path / ".git" / "scratch-index"
    environment = {**os.environ, "GIT_INDEX_FILE": str(index)}
    subprocess.run(["git", "read-tree", parent], cwd=path, env=environment, check=True)
    subprocess.run(["git", "update-index", "--add", "--cacheinfo", f"160000,{target},{name}"], cwd=path, env=environment, check=True)
    tree = subprocess.run(["git", "write-tree"], cwd=path, env=environment, check=True, capture_output=True, text=True).stdout.strip()
    return git(path, "commit-tree", tree, "-p", parent, "-m", "gitlink")


def test_file_to_gitlink_and_back_keep_the_patch_aligned(repo):
    path, base = repo
    linked = gitlink_commit(path, "f2", base, base)
    restored = commit_tree_from(path, linked, "f2", "x again\n")
    for tip, direction in ((linked, "file to gitlink"), (restored, "gitlink to file")):
        parent = base if tip == linked else linked
        committed = scopes(build([entry(path, parent, tip)]))["committed_range"]
        rows_by_name = {capture_row_path(row): row for row in committed["rows"]}
        assert committed["state"] == "available", direction
        assert rows_by_name["f2"]["content"]["reason"] == "submodule", direction
    both = rows(build([entry(path, base, restored)]), "committed_range")
    assert both["f2"]["status"] in {"modified", "typechange"}


def commit_tree_from(path, parent, name, content):
    blob = git(path, "hash-object", "-w", "--stdin", input=content.encode())
    index = path / ".git" / "scratch-index-2"
    environment = {**os.environ, "GIT_INDEX_FILE": str(index)}
    subprocess.run(["git", "read-tree", parent], cwd=path, env=environment, check=True)
    subprocess.run(["git", "update-index", "--add", "--cacheinfo", f"100644,{blob},{name}"], cwd=path, env=environment, check=True)
    tree = subprocess.run(["git", "write-tree"], cwd=path, env=environment, check=True, capture_output=True, text=True).stdout.strip()
    return git(path, "commit-tree", tree, "-p", parent, "-m", "file again")


def test_a_gitlink_typechange_still_has_text_for_its_neighbours(repo):
    path, base = repo
    linked = gitlink_commit(path, "f2", base, base)
    after = commit_tree_from(path, linked, "z-new", "text\n")
    committed = scopes(build([entry(path, base, after)]))["committed_range"]
    by_name = {capture_row_path(row): row for row in committed["rows"]}
    assert committed["state"] == "available" and "+text" in text_of(by_name["z-new"])


# --- flag accounting --------------------------------------------------------------------------


def test_absent_skip_worktree_files_do_not_count_toward_the_limit(repo, monkeypatch):
    path, base = repo
    names = [f"s{index}" for index in range(10)]
    tip = commit(path, "sparse", **{name: "x\n" for name in names})
    for name in names:
        git(path, "update-index", "--skip-worktree", name)
        (path / name).unlink()
    monkeypatch.setattr(capture, "FLAG_LIMIT", 3)
    scope = scopes(build([entry(path, base, tip)]))["working_tree"]
    assert (scope["state"], scope["reason"], scope["rows"]) == ("available", None, [])
    for name in names[:5]:
        git(path, "update-index", "--assume-unchanged", name)
    scope = scopes(build([entry(path, base, tip)]))["working_tree"]
    assert (scope["state"], scope["reason"]) == ("incomplete", "index_flags_present")


def test_directory_mode_entries_are_never_rewritten(repo):
    path, _ = repo
    data = (path / ".git" / "index").read_bytes()
    scan = capture._scan_index(data, 20, capture._Budget(5))
    forged = [capture._Flagged(12, b"dir", True, True, directory=True)]
    assert capture._clear_flags(data, 20, forged, lambda name: True) == data
    assert scan.flagged == []


def test_edits_in_the_same_instant_as_the_index_are_still_seen(repo):
    path, base = repo
    for index in range(6):
        name = f"racy{index}"
        (path / name).write_text("1\n")
        git(path, "add", name)
    tip = commit(path, "racy")
    for index in range(6):
        (path / f"racy{index}").write_text("2\n")  # same size, written moments after the index
    working = rows(build([entry(path, base, tip)]), "working_tree")
    assert {f"racy{index}" for index in range(6)} <= set(working)

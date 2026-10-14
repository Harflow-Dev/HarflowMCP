"""Bounded, best-effort observations; never evidence of exclusive task attribution."""
from __future__ import annotations

import os
import re
import subprocess
import stat
import tempfile
import time
from pathlib import Path
from typing import Any

MAX_OUTPUT = 262_144
BUDGET = 1.5


class GitProbe:
    """Disable repository executable integrations before inspecting working tree contents."""
    def __init__(self, path: Path):
        self.path = path
        self.deadline = time.monotonic() + BUDGET
        self.config = ["-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null",
                       "-c", "submodule.recurse=false", "-c", "diff.external="]

    def read(self, *args: str, environment: dict[str, str] | None = None) -> str | None:
        # Spool rather than buffering unbounded output in server memory; stop at the ceiling.
        env = {**os.environ, "GIT_NO_LAZY_FETCH": "1", "GIT_OPTIONAL_LOCKS": "0",
               "GIT_TERMINAL_PROMPT": "0", "GIT_PAGER": "cat", **(environment or {})}
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            proc = subprocess.Popen(["git", "--no-optional-locks", "-C", str(self.path), *self.config, *args],
                                    stdout=output, stderr=errors, stdin=subprocess.DEVNULL, env=env)
            try:
                while proc.poll() is None:
                    if time.monotonic() >= self.deadline or os.fstat(output.fileno()).st_size > MAX_OUTPUT or os.fstat(errors.fileno()).st_size > MAX_OUTPUT:
                        raise ValueError("Git observation exceeded time/output budget")
                    time.sleep(0.01)
                if os.fstat(output.fileno()).st_size > MAX_OUTPUT:
                    raise ValueError("Git observation exceeded output budget")
                if proc.returncode:
                    return None
                output.seek(0)
                return output.read(MAX_OUTPUT).decode("utf-8", errors="replace")
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()

    def status(self, worktree: str | None = None) -> str | None:
        # Read the original index and object database through a disposable Git directory with
        # no repository/global/system config. Enumerating filter names then overriding them is
        # unsafe: another process could add a new driver between enumeration and status.
        git_dir = _strip(self.read("rev-parse", "--absolute-git-dir"))
        common = _strip(self.read("rev-parse", "--path-format=absolute", "--git-common-dir"))
        worktree = worktree or _strip(self.read("rev-parse", "--show-toplevel"))
        object_format = _strip(self.read("rev-parse", "--show-object-format"))
        if not git_dir or not common or not worktree or object_format not in ("sha1", "sha256"):
            return None
        with tempfile.TemporaryDirectory(prefix="polybridge-git-observation-") as directory:
            isolated = Path(directory)
            (isolated / "objects").symlink_to(Path(common) / "objects", target_is_directory=True)
            (isolated / "refs").symlink_to(Path(common) / "refs", target_is_directory=True)
            for source, name in ((Path(git_dir) / "HEAD", "HEAD"), (Path(common) / "packed-refs", "packed-refs")):
                if source.exists():
                    descriptor = os.open(source, os.O_RDONLY | os.O_NONBLOCK)
                    with os.fdopen(descriptor, "rb") as metadata:
                        if not stat.S_ISREG(os.fstat(metadata.fileno()).st_mode):
                            raise ValueError("Git metadata is not a regular file")
                        data = metadata.read(MAX_OUTPUT + 1)
                    if len(data) > MAX_OUTPUT:
                        raise ValueError("Git metadata exceeds budget")
                    (isolated / name).write_bytes(data)
            config = "[core]\nrepositoryformatversion = 0\nbare = false\n"
            if object_format == "sha256":
                config = "[core]\nrepositoryformatversion = 1\nbare = false\n[extensions]\nobjectFormat = sha256\n"
            (isolated / "config").write_text(config)
            environment = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null",
                           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_COUNT": "0",
                           "GIT_CONFIG_PARAMETERS": "", "GIT_COMMON_DIR": directory,
                           "GIT_INDEX_FILE": str(Path(git_dir) / "index")}
            return self.read("--git-dir=" + directory, "--work-tree=" + worktree, "status",
                             "--porcelain=v1", "--untracked-files=normal", "--ignore-submodules=all",
                             environment=environment)


def baseline(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"head": None, "branch": None, "repository_root": None, "working_tree_status": None,
                              "provenance": "local Git observation with executable filters disabled", "error": None}
    try:
        probe = GitProbe(path)
        result["head"] = _strip(probe.read("rev-parse", "--verify", "HEAD"))
        result["branch"] = _strip(probe.read("symbolic-ref", "--quiet", "--short", "HEAD"))
        result["repository_root"] = _strip(probe.read("rev-parse", "--show-toplevel"))
        result["working_tree_status"] = probe.status(result["repository_root"])
        if result["working_tree_status"] is None:
            result["error"] = "working tree observation unavailable"
    except Exception:
        result["error"] = "Git observation unavailable or exceeded budget"
    return result


def _strip(value: str | None) -> str | None:
    return value.strip() if value is not None else None


def capture(path: Path, start: dict[str, Any] | None, evidence: dict[str, Any]) -> dict[str, Any]:
    end = baseline(path)
    result: dict[str, Any] = {"baseline": start, "terminal": end, "base_commit": (start or {}).get("head"),
        "head_commit": end["head"], "new_commits": None, "committed_diff_stat": None,
        "local_upstream": None, "pushed": None, "publication_evidence": None,
        "attribution": "Shared-repository observations do not establish task authorship or goal completion.",
        "external_writes": evidence.get("external_writes") or None,
        "last_build_test": evidence.get("last_build_test"),
        "evidence_provenance": "normalized structured tool calls/results; unavailable when absent or unsupported"}
    try:
        probe = GitProbe(path)
        base, head = result["base_commit"], result["head_commit"]
        if base and head and re.fullmatch(r"[0-9a-f]{40,64}", base) and re.fullmatch(r"[0-9a-f]{40,64}", head):
            commits = probe.read("rev-list", "--max-count=101", f"{base}..{head}")
            result["new_commits"] = commits.splitlines() if commits is not None else None
            result["new_commits_truncated"] = bool(commits and len(commits.splitlines()) > 100)
            if result["new_commits"] is not None:
                result["new_commits"] = result["new_commits"][:100]
            result["committed_diff_stat"] = probe.read("diff", "--no-ext-diff", "--no-textconv", "--stat", base, head, "--")
        upstream = _strip(probe.read("rev-parse", "--symbolic-full-name", "@{upstream}"))
        if upstream:
            counts = _strip(probe.read("rev-list", "--left-right", "--count", "HEAD...@{upstream}"))
            result["local_upstream"] = {"ref": upstream, "ahead_behind": counts,
                                        "provenance": "local tracking ref; no remote contacted"}
    except Exception:
        pass
    return result


def observe(evidence: dict[str, Any], kind: str, fields: dict[str, Any], repo_path: Path | None = None, repository_root: Path | None = None) -> None:
    """Retain only bounded correlated structured evidence, never infer success from narration."""
    pending = evidence.setdefault("pending", {})
    call_id = fields.get("call_id")
    if kind == "tool_call" and call_id:
        if len(pending) >= 100:
            pending.pop(next(iter(pending)))
        pending[call_id] = {key: fields.get(key) for key in ("command", "tool", "category", "path")}
    elif kind == "tool_result":
        call = pending.pop(call_id, None)
        if not call:
            return
        command = call.get("command")
        if isinstance(command, str) and re.search(r"(?:^|[\s;&|])(?:pytest|swift\s+(?:test|build)|xcodebuild|(?:uv\s+run\s+)?pytest|npm\s+(?:test|run\s+(?:test|build))|cargo\s+(?:test|build)|make\s+(?:test|build))\b", command):
            evidence["last_build_test"] = {"command": command[:8192], "exit_code": fields.get("exit_code"),
                                            "ok": fields.get("ok"), "call_id": call_id}
        path = call.get("path")
        if fields.get("ok") is True and call.get("category") in {"edit", "write"} and isinstance(path, str) and repo_path is not None:
            target = Path(path)
            if not target.is_absolute():
                target = repo_path / target
            target = target.resolve()
            if not target.is_relative_to((repository_root or repo_path).resolve()):
                writes = evidence.setdefault("external_writes", [])
                if len(writes) < 50:
                    writes.append({"path": str(target), "tool": call.get("tool"), "call_id": call_id,
                                   "provenance": "successful normalized edit result for path outside repository"})

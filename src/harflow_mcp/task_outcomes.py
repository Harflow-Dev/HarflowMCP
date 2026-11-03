"""Bounded, best-effort observations; never evidence of exclusive task attribution."""
from __future__ import annotations

import os
import hashlib
import shlex
import re
import subprocess
import stat
import tempfile
import time
from pathlib import Path
from typing import Any

MAX_OUTPUT = 262_144
BUDGET = 1.5
MAX_REPOSITORIES = 8
MAX_FIELD_BYTES = 8192
GIT_LOCAL_ENV = {
    "GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_PREFIX",
    "GIT_SHALLOW_FILE", "GIT_GRAFT_FILE", "GIT_NAMESPACE", "GIT_IMPLICIT_WORK_TREE",
    "GIT_CONFIG", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT",
    "GIT_CONFIG_SYSTEM", "GIT_CONFIG_GLOBAL", "GIT_NO_REPLACE_OBJECTS", "GIT_REPLACE_REF_BASE",
}


def _bounded(value: Any) -> tuple[str | None, bool]:
    if not isinstance(value, str):
        return None, False
    data = value.encode("utf-8")
    return data[:MAX_FIELD_BYTES].decode("utf-8", errors="ignore"), len(data) > MAX_FIELD_BYTES


def _command_match(command: str) -> bool:
    # Unsupported shell expansion/control grammar is unavailable, never guessed.
    if any(marker in command for marker in ("$", "`", "(", ")", "\n", "\r", "\\")):
        return False
    if re.search(r"['\"][^'\"]*[;&|][^'\"]*['\"]", command):
        return False
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|")
        lexer.whitespace_split = True
        words = list(lexer)
    except ValueError:
        return False
    starts = [0] + [i + 1 for i, word in enumerate(words) if word in {";", "&&", "||", "|", "&"}]
    for index in starts:
        args = words[index:]
        if args[:2] == ["uv", "run"]:
            args = args[2:]
        if not args:
            continue
        if args[0] in {"pytest", "xcodebuild"}:
            return True
        if args[0] in {"swift", "cargo", "make"} and args[1:2] in (["test"], ["build"]):
            return True
        if args[0] == "npm" and (args[1:2] == ["test"] or args[1:3] in (["run", "test"], ["run", "build"])):
            return True
    return False


class GitProbe:
    """Disable repository executable integrations before inspecting working tree contents."""
    def __init__(self, path: Path, budget: float | None = None):
        self.path = path
        self.started = time.monotonic()
        self.budget = BUDGET if budget is None else budget
        self.deadline = self.started + self.budget
        self.diagnostics: list[dict[str, Any]] = []
        self.config = ["-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null",
                       "-c", "submodule.recurse=false", "-c", "diff.external="]

    def summary(self) -> dict[str, Any]:
        return {"budget_ms": round(self.budget * 1000),
                "elapsed_ms": round((time.monotonic() - self.started) * 1000),
                "probes": self.diagnostics[:16]}

    def read(self, *args: str, environment: dict[str, str] | None = None) -> str | None:
        started = time.monotonic()
        # Only command names/options are retained, never repository paths or Git stderr.
        name = " ".join(arg for arg in args if not arg.startswith(("--git-dir=", "--work-tree=")))
        entry: dict[str, Any] = {"probe": name[:160], "status": "error", "elapsed_ms": 0}
        self.diagnostics.append(entry)
        if started >= self.deadline:
            entry["status"] = "skipped_budget_exhausted"
            return None
        env = {**{key: value for key, value in os.environ.items()
                  if key not in GIT_LOCAL_ENV and not key.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))},
               "GIT_NO_LAZY_FETCH": "1", "GIT_OPTIONAL_LOCKS": "0",
               "GIT_TERMINAL_PROMPT": "0", "GIT_PAGER": "cat", **(environment or {})}
        proc = None
        try:
            with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
                # Environment and temporary-file setup also consume the shared budget.
                if time.monotonic() >= self.deadline:
                    entry["status"] = "skipped_budget_exhausted"
                    return None
                proc = subprocess.Popen(["git", "--no-optional-locks", "-C", str(self.path), *self.config, *args],
                                        stdout=output, stderr=errors, stdin=subprocess.DEVNULL, env=env)
                try:
                    while True:
                        code = proc.poll()
                        if os.fstat(output.fileno()).st_size > MAX_OUTPUT or os.fstat(errors.fileno()).st_size > MAX_OUTPUT:
                            entry["status"] = "output_limit"
                            return None
                        if code is not None:
                            entry.update(status="success" if code == 0 else "git_failure", returncode=code)
                            if code:
                                return None
                            output.seek(0)
                            return output.read(MAX_OUTPUT).decode("utf-8", errors="replace")
                        remaining = self.deadline - time.monotonic()
                        if remaining <= 0:
                            entry["status"] = "time_limit"
                            return None
                        time.sleep(min(0.01, remaining))
                finally:
                    if proc.poll() is None:
                        proc.kill()
                    proc.wait()
                    entry["stdout_bytes"] = os.fstat(output.fileno()).st_size
                    entry["stderr_bytes"] = os.fstat(errors.fileno()).st_size
        except Exception as exc:
            entry["error_type"] = type(exc).__name__[:64]
            return None
        finally:
            entry["elapsed_ms"] = round((time.monotonic() - started) * 1000)

    def status(self, worktree: str | None = None, *, git_dir: str | None = None, common: str | None = None) -> str | None:
        # Read the original index and object database through a disposable Git directory with
        # no repository/global/system config. Enumerating filter names then overriding them is
        # unsafe: another process could add a new driver between enumeration and status.
        # A caller that has already verified where the Git directories are (a task worktree an
        # agent can repoint) passes `git_dir` and `common`, so nothing is discovered through the
        # tree being inspected.
        pinned = git_dir is not None and common is not None
        if not pinned:
            git_dir = _strip(self.read("rev-parse", "--absolute-git-dir"))
            common = _strip(self.read("rev-parse", "--path-format=absolute", "--git-common-dir"))
        worktree = worktree or _strip(self.read("rev-parse", "--show-toplevel"))
        object_format = _strip(self.read(*(("--git-dir=" + git_dir,) if pinned else ()), "rev-parse", "--show-object-format"))
        if not git_dir or not common or not worktree or object_format not in ("sha1", "sha256"):
            return None
        with tempfile.TemporaryDirectory(prefix="harflow_mcp-git-observation-") as directory:
            build_isolated_git_dir(Path(directory), git_dir, common, object_format)
            environment = {"GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_SYSTEM": "/dev/null",
                           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_COUNT": "0",
                           "GIT_CONFIG_PARAMETERS": "", "GIT_COMMON_DIR": directory,
                           "GIT_INDEX_FILE": str(Path(git_dir) / "index")}
            return self.read("--git-dir=" + directory, "--work-tree=" + worktree, "status",
                             "--porcelain=v1", "--untracked-files=normal", "--ignore-submodules=all",
                             environment=environment)


def build_isolated_git_dir(isolated: Path, git_dir: str, common: str, object_format: str) -> None:
    """Populate `isolated` (an existing empty directory) as a disposable Git directory.

    Objects and refs are linked from the common directory, `HEAD` (checked as a regular file) comes from
    `git_dir`, and the configuration is minimal, so no repository, global or system config, hook or
    filter driver applies. Callers add their own index and environment.
    """
    (isolated / "objects").symlink_to(Path(common) / "objects", target_is_directory=True)
    (isolated / "refs").symlink_to(Path(common) / "refs", target_is_directory=True)
    for source, name in ((Path(git_dir) / "HEAD", "HEAD"), (Path(common) / "packed-refs", "packed-refs")):
        if source.exists():
            descriptor = os.open(source, os.O_RDONLY | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as metadata:
                if not stat.S_ISREG(os.fstat(metadata.fileno()).st_mode):
                    raise ValueError("Git metadata is not a regular file")
                if name == "HEAD":
                    data = metadata.read(MAX_OUTPUT + 1)
                    if len(data) > MAX_OUTPUT:
                        raise ValueError("HEAD metadata exceeds input limit")
                    (isolated / name).write_bytes(data)
                else:
                    # Large packed ref databases are inputs, not retained output. Let
                    # Git read them under the subprocess time/output ceiling, just as
                    # it reads the refs and objects symlinked above.
                    (isolated / name).symlink_to(source)
    config = "[core]\nrepositoryformatversion = 0\nbare = false\n"
    if object_format == "sha256":
        config = "[core]\nrepositoryformatversion = 1\nbare = false\n[extensions]\nobjectFormat = sha256\n"
    (isolated / "config").write_text(config)


def empty_baseline() -> dict[str, Any]:
    return {"head": None, "branch": None, "repository_root": None, "working_tree_status": None,
            "provenance": "local Git observation with executable filters disabled", "error": None}


def interrupted_baseline(partial: dict[str, Any], reason: str, budget: float) -> dict[str, Any]:
    """Freeze known evidence before the background worker finishes; never await it indefinitely."""
    result = partial.copy()
    if isinstance(partial.get("diagnostics"), dict):
        result["diagnostics"] = partial["diagnostics"].copy()
        result["diagnostics"]["probes"] = [entry.copy() for entry in
            list(partial["diagnostics"].get("probes", []))[:16]]
    result["observation_status"] = "partial" if any(result.get(key) is not None for key in
        ("head", "branch", "repository_root", "working_tree_status")) else "unavailable"
    result["error"] = "Git observation interrupted: " + reason
    diagnostics = result.setdefault("diagnostics", {"phase": "baseline", "probes": []})
    diagnostics["outer_status"] = reason
    diagnostics["outer_budget_ms"] = round(budget * 1000)
    return result


def _baseline(path: Path, partial: dict[str, Any] | None = None, probe: GitProbe | None = None) -> dict[str, Any]:
    result = partial if partial is not None else empty_baseline()
    probe = probe or GitProbe(path)
    result["diagnostics"] = {"phase": "baseline", "budget_ms": round(BUDGET * 1000), "probes": probe.diagnostics}
    try:
        result["head"] = _strip(probe.read("rev-parse", "--verify", "HEAD"))
        result["branch"] = _strip(probe.read("symbolic-ref", "--quiet", "--short", "HEAD"))
        result["repository_root"] = _strip(probe.read("rev-parse", "--show-toplevel"))
        result["working_tree_status"] = probe.status(result["repository_root"])
    except Exception as exc:
        probe.diagnostics.append({"probe": "status_setup", "status": "error", "error_type": type(exc).__name__[:64], "reason": str(exc)[:160] if isinstance(exc, ValueError) else "metadata_setup_failed"})
    available = sum(result[key] is not None for key in ("head", "repository_root", "working_tree_status"))
    result["observation_status"] = "success" if available == 3 else "partial" if available else "unavailable"
    result["diagnostics"] = {"phase": "baseline", **probe.summary()}
    if result["working_tree_status"] is None:
        failures = [item for item in probe.diagnostics if item["status"] != "success"]
        reason = failures[-1]["status"] if failures else "unavailable"
        result["error"] = "working tree observation unavailable: " + reason

    return result


def validate_observe_repos(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) >= MAX_REPOSITORIES:
        raise ValueError("observe_repos must be a list of at most seven absolute paths")
    if any(not isinstance(item, str) or not Path(item).is_absolute() for item in value):
        raise ValueError("observe_repos must contain absolute paths")
    return list(dict.fromkeys(str(Path(item).resolve()) for item in value))


def baseline(path: Path, partial: dict[str, Any] | None = None, observe_repos: list[str] | None = None) -> dict[str, Any]:
    deadline = time.monotonic() + BUDGET
    result = _baseline(path, partial)
    repositories = []
    unavailable = []
    seen = {result.get("repository_root")}
    for candidate in validate_observe_repos(observe_repos):
        probe = GitProbe(Path(candidate), budget=max(0, deadline - time.monotonic()))
        entry = _baseline(Path(candidate), probe=probe)
        root = entry.get("repository_root")
        if not root:
            unavailable.append({"path": candidate, "reason": entry.get("error") or "repository identity unavailable"})
        if root and root not in seen:
            entry["baseline_provenance"] = "dispatch"
            repositories.append(entry)
            seen.add(root)
    if observe_repos is not None:
        result["repositories"] = repositories
        result["repository_discovery_unavailable"] = unavailable
    return result


def _strip(value: str | None) -> str | None:
    return value.strip() if value is not None else None


COMMIT_ATTRIBUTION = (
    "new_commits means newly reachable from the observed comparison base; imported history may be included. "
    "Task-created commits and authorship are unavailable."
)


def project_outcome(outcome: Any) -> Any:
    """Clarify range semantics on reads without rewriting persisted observations."""
    if not isinstance(outcome, dict):
        return outcome
    return {**outcome, "attribution": COMMIT_ATTRIBUTION}


def endpoint_projection(outcome: dict[str, Any]) -> dict[str, Any]:
    """Bounded endpoint provenance only; intermediate transitions are unobserved."""
    def bounded(value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        raw = value.encode("utf-8")
        if len(raw) <= 160:
            return value
        return raw[:146].decode("utf-8", "ignore") + "...[truncated]"

    result = {}
    for phase in ("baseline", "terminal"):
        value = outcome.get(phase)
        if isinstance(value, dict):
            result[phase] = {
                key: bounded(value.get(key))
                for key in ("head", "branch", "provenance")
            }
    return result


def _capture(path: Path, start: dict[str, Any] | None, evidence: dict[str, Any], deadline: float) -> dict[str, Any]:
    probe = GitProbe(path, budget=max(0, deadline - time.monotonic()))
    end = _baseline(path, probe=probe)
    result: dict[str, Any] = {"baseline": start, "terminal": end, "base_commit": (start or {}).get("head"),
        "head_commit": end["head"], "new_commits": None, "committed_diff_stat": None,
        "local_upstream": None, "pushed": None, "publication_evidence": None,
        "attribution": COMMIT_ATTRIBUTION,
        "external_writes": evidence.get("external_writes") or None,
        "last_build_test": evidence.get("last_build_test"),
        "evidence_provenance": "normalized structured tool calls/results; unavailable when absent or unsupported"}
    end["diagnostics"]["phase"] = "terminal"
    try:
        base, head = result["base_commit"], result["head_commit"]
        result["dispatch_head"] = base
        result["base_provenance"] = (start or {}).get("baseline_provenance", "dispatch")
        result["repository_root"] = end.get("repository_root")
        result["worktree"] = end.get("repository_root")
        result["git_common_dir"] = _strip(probe.read("rev-parse", "--path-format=absolute", "--git-common-dir"))
        branch_changed = base and head and (start or {}).get("branch") != end.get("branch")
        divergent = base and head and _strip(probe.read("merge-base", base, head)) != base
        if branch_changed or divergent:
            # A feature upstream may already contain a just-pushed commit. Prefer the
            # unambiguous recorded default; endpoint-only refs still cannot prove authorship.
            reference = None
            defaults = probe.read("for-each-ref", "--format=%(symref)", "refs/remotes/*/HEAD")
            refs = sorted(set((defaults or "").splitlines()))
            if len(refs) == 1:
                reference = _strip(probe.read("rev-parse", "--verify", refs[0]))
            if not reference:
                reference = _strip(probe.read("rev-parse", "--verify", "@{upstream}"))
            if reference:
                merged = _strip(probe.read("merge-base", head, reference))
                base = result["base_commit"] = merged
                result["base_provenance"] = ("merge-base with local upstream/default after branch change" if merged else
                    "unavailable: no merge-base with local upstream/default")
            else:
                base = result["base_commit"] = None
                result["base_provenance"] = "unavailable: changed branch/history without local upstream/default"

        if base and head and re.fullmatch(r"[0-9a-f]{40,64}", base) and re.fullmatch(r"[0-9a-f]{40,64}", head):
            commits = probe.read("rev-list", "--max-count=101", f"{base}..{head}")
            result["new_commits"] = commits.splitlines() if commits is not None else None
            result["new_commits_truncated"] = bool(commits and len(commits.splitlines()) > 100)
            if result["new_commits"] is not None:
                result["new_commits"] = result["new_commits"][:100]
            result["committed_diff_stat"] = probe.read("diff", "--no-ext-diff", "--no-textconv", "--stat", base, head, "--")
        result["local_remote_refs"] = None
        if head:
            refs = probe.read("for-each-ref", "--count=33", "--format=%(refname)", "--contains=" + head, "refs/remotes/")
            if refs is not None:
                result["local_remote_refs"] = {"containing_head": refs.splitlines()[:32], "truncated": len(refs.splitlines()) > 32,
                    "freshness": "unknown", "provenance": "local remote-tracking refs only; no remote contacted"}
        upstream = _strip(probe.read("rev-parse", "--symbolic-full-name", "@{upstream}"))
        if upstream:
            counts = _strip(probe.read("rev-list", "--left-right", "--count", "HEAD...@{upstream}"))
            result["local_upstream"] = {"ref": upstream, "ahead_behind": counts,
                                        "provenance": "local tracking ref; no remote contacted", "freshness": "unknown",
                                        "contains_head": _strip(probe.read("merge-base", "--is-ancestor", "HEAD", "@{upstream}")) == "" or None}
    except Exception as exc:
        probe.diagnostics.append({"probe": "capture_setup", "status": "error", "error_type": type(exc).__name__[:64]})
    result["diagnostics"] = {"phase": "capture", **probe.summary()}
    return result


def capture(path: Path, start: dict[str, Any] | None, evidence: dict[str, Any]) -> dict[str, Any]:
    deadline = time.monotonic() + 2 * BUDGET
    result = _capture(path, start, evidence, deadline)
    result["git_observation_version"] = 2
    repositories = []
    seen = set()
    if result.get("repository_root"):
        repositories.append({key: value for key, value in result.items() if key != "repositories"})
        seen.add(result["repository_root"])
    entries = list((start or {}).get("repositories", [])) + list(evidence.get("repositories", {}).values())
    omitted = False
    for entry in entries[:MAX_REPOSITORIES * 2]:
        root = entry.get("repository_root")
        if not root or root in seen:
            continue
        if len(repositories) >= MAX_REPOSITORIES:
            omitted = True
            continue
        seen.add(root)
        repositories.append(_capture(Path(root), entry, evidence, deadline))
    publications = evidence.get("publications", [])
    for entry in repositories:
        matching = [item for item in publications if item.get("repository_root") == entry.get("repository_root")]
        if matching:
            entry["publication_evidence"] = matching[-10:]
        refs = (entry.get("local_remote_refs") or {}).get("containing_head", [])
        entry["pushed"] = True if refs and entry.get("new_commits") else None
        if refs:
            entry["push_evidence"] = {"provenance": "HEAD reachable from cached local remote-tracking refs",
                "freshness": "unknown", "refs": refs, "head": entry.get("head_commit")}
    primary = next((item for item in repositories if item.get("repository_root") == result.get("repository_root")), None)
    if primary:
        result["pushed"] = primary["pushed"]
        result["publication_evidence"] = primary["publication_evidence"]
        if primary.get("push_evidence"):
            result["push_evidence"] = primary["push_evidence"]
    result["repositories"] = repositories
    result["repositories_truncated"] = bool(omitted or evidence.get("repositories_truncated"))
    result["repository_discovery_unavailable"] = (list((start or {}).get("repository_discovery_unavailable", [])) +
        list(evidence.get("repository_discovery_unavailable", [])))[:MAX_REPOSITORIES]
    return result


def _structured_git(command: Any, cwd: Path) -> tuple[Path, str | None] | None:
    if not isinstance(command, str) or len(command.encode()) > MAX_FIELD_BYTES:
        return None
    # Only one literal invocation can be correlated with the whole tool result.
    if any(marker in command for marker in ("$", "`", "\\", "\n", "\r", ";", "&", "|", "<", ">", "(", ")")):
        return None
    try:
        args = shlex.split(command)
    except ValueError:
        return None
    if not args or args[0] not in {"git", "gh"}:
        return None
    executable = args.pop(0)
    if executable == "gh":
        return (cwd, "gh") if args[:2] in (["pr", "create"], ["release", "create"]) else None
    while args[:1] == ["-C"] and len(args) > 2:
        target = Path(args[1])
        cwd = target if target.is_absolute() else cwd / target
        args = args[2:]
    if not args or args[0].startswith("-"):
        return None
    return cwd, "push" if args[0] == "push" and not any(arg in {"--dry-run", "-n"} for arg in args[1:]) else None


def _discover(evidence: dict[str, Any], candidate: Path) -> dict[str, Any] | None:
    repositories = evidence.setdefault("repositories", {})
    candidate = candidate.resolve()
    if candidate.is_file():
        candidate = candidate.parent
    # Candidates themselves are bounded before spawning Git, including nonrepositories.
    attempted = evidence.setdefault("repository_candidates", [])
    key = str(candidate)
    if key in attempted:
        return next((entry for entry in repositories.values() if candidate.is_relative_to(Path(entry["repository_root"]))), None)
    if len(attempted) >= MAX_REPOSITORIES * 4 or len(repositories) >= MAX_REPOSITORIES:
        evidence["repositories_truncated"] = True
        return None
    attempted.append(key)
    remaining = max(0, BUDGET - evidence.get("repository_discovery_elapsed", 0))
    started = time.monotonic()
    entry = _baseline(candidate, probe=GitProbe(candidate, budget=remaining))
    evidence["repository_discovery_elapsed"] = evidence.get("repository_discovery_elapsed", 0) + time.monotonic() - started
    root = entry.get("repository_root")
    if not root:
        unavailable = evidence.setdefault("repository_discovery_unavailable", [])
        if len(unavailable) < MAX_REPOSITORIES:
            unavailable.append({"path": key, "reason": entry.get("error") or "repository identity unavailable"})
    if root:
        entry["baseline_provenance"] = "first_seen_midrun; earlier changes unavailable"
        repositories.setdefault(root, entry)
        return repositories[root]
    return None


def observe(evidence: dict[str, Any], kind: str, fields: dict[str, Any], repo_path: Path | None = None, repository_root: Path | None = None) -> None:
    """Retain only bounded correlated structured evidence, never infer success from narration."""
    pending = evidence.setdefault("pending", {})
    raw_id = fields.get("call_id")
    call_id, id_truncated = _bounded(raw_id)
    if id_truncated:
        call_id = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()
    id_encoding = "sha256" if id_truncated else "verbatim"
    correlation_key = hashlib.sha256(raw_id.encode("utf-8")).hexdigest() if isinstance(raw_id, str) and raw_id else None
    if kind == "tool_call" and call_id:
        if len(pending) >= 100:
            pending.pop(next(iter(pending)))
        call = {}
        for key in ("command", "tool", "category", "path", "cwd"):
            value, truncated = _bounded(fields.get(key))
            call[key] = None if truncated and key in {"path", "category"} else value
            call[key + "_truncated"] = truncated
        if repo_path is not None:
            cwd = Path(call["cwd"]) if call.get("cwd") and not call["cwd_truncated"] else repo_path
            if not cwd.is_absolute():
                cwd = repo_path / cwd
            parsed = _structured_git(call.get("command"), cwd)
            candidate = parsed[0] if parsed else cwd if call.get("cwd") and not call["cwd_truncated"] else None
            if call.get("category") in {"edit", "write"} and call.get("path"):
                candidate = Path(call["path"])
                if not candidate.is_absolute():
                    candidate = cwd / candidate
                if not candidate.exists():
                    candidate = candidate.parent
            entry = _discover(evidence, candidate) if candidate is not None else None
            if parsed and parsed[1] and entry:
                call["publication"] = parsed[1]
                call["repository_root"] = entry["repository_root"]
                if parsed[1] == "push":
                    words = shlex.split(call["command"])
                    words = words[1:]
                    while words[:1] == ["-C"] and len(words) > 2:
                        words = words[2:]
                    options = words[1:]
                    positional = options if all(not word.startswith("-") for word in options) else []
                    call["remote"] = positional[0] if positional else None
                    call["ref"] = positional[1:9] if len(positional) > 1 else []
        pending[correlation_key] = call
    elif kind == "tool_result":
        call = pending.pop(correlation_key, None)
        if not call:
            return
        if call.get("publication"):
            publications = evidence.setdefault("publications", [])
            if len(publications) < 50:
                publications.append({"kind": call["publication"], "repository_root": call["repository_root"],
                    "command": call["command"], "call_id": call_id,
                    "result": _bounded(fields.get("output_tail") or fields.get("output") or fields.get("stdout"))[0],
                    "result_provenance": "bounded normalized result tail",
                    "target_provenance": "requested command arguments; server response targets unavailable",
                    "remote": call.get("remote"), "ref": call.get("ref"),
                    "exit_code": fields.get("exit_code") if type(fields.get("exit_code")) is int else None,
                    "success": fields.get("exit_code") == 0 if type(fields.get("exit_code")) is int else None,
                    "provenance": "correlated standalone command/result; remote freshness unknown"})
        command = call.get("command")
        if isinstance(command, str) and not call["command_truncated"] and _command_match(command):
            evidence["last_build_test"] = {"command": command, "command_truncated": call["command_truncated"],
                "exit_code": fields.get("exit_code") if type(fields.get("exit_code")) is int else None,
                "ok": fields.get("ok") if type(fields.get("ok")) is bool else None,
                "call_id": call_id, "call_id_encoding": id_encoding, "exit_code_scope": "entire tool invocation",
                "provenance": "correlated tool result; command contains a supported build/test invocation; individual segments may not have executed"}
        path = call.get("path")
        if fields.get("ok") is True and call.get("category") in {"edit", "write"} and isinstance(path, str) and repo_path is not None:
            target = Path(path)
            if not target.is_absolute():
                cwd = call.get("cwd")
                base_path = Path(cwd) if cwd and not call["cwd_truncated"] else repo_path
                if not base_path.is_absolute():
                    base_path = repo_path / base_path
                target = base_path / target
            target = target.resolve()
            resolved, truncated = _bounded(str(target))
            if not truncated and not target.is_relative_to((repository_root or repo_path).resolve()):
                writes = evidence.setdefault("external_writes", [])
                if len(writes) < 50:
                    writes.append({"path": resolved, "tool": call.get("tool"), "tool_truncated": call["tool_truncated"],
                                   "call_id": call_id, "call_id_encoding": id_encoding,
                                   "provenance": "successful normalized edit result for path outside repository"})

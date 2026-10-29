"""Git preparation Harflow runs before the agent starts.

`branch` and `base` create one non-forced branch from a fetched base. `fetch` refreshes
remote-tracking refs. Both use the same hook-disabled Git invocation. The agent's freedom is
unchanged: a `read_only` task may fetch because Harflow, not the agent, writes `.git`.
"""
from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .task_outcomes import GIT_LOCAL_ENV

COMMAND_TIMEOUT = 30.0
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_/-]*")
CONFIG = (
    "-c", "core.fsmonitor=false",
    "-c", "core.hooksPath=/dev/null",
    "-c", "submodule.recurse=false",
    "-c", "diff.external=",
    "-c", "filter.lfs.required=false",
    "-c", "filter.lfs.smudge=",
    "-c", "filter.lfs.clean=",
    "-c", "filter.lfs.process=",
)


class DispatchGitError(ValueError):
    """Dispatch-time Git preparation refused. The agent has not started."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.harflow_not_started = True


def literal_name(value: object, label: str) -> str:
    if type(value) is not str or not NAME.fullmatch(value) or any(
            part in ("", ".", "..") for part in value.split("/")):
        raise DispatchGitError(
            f"{label} must be a literal name of letters, digits, '_', '-' and '/', "
            "starting with a letter or digit, with no empty, '.' or '..' component, "
            "and no options, refspecs or revision expressions"
        )
    return value


def parse_base(value: object) -> tuple[str, str]:
    text = literal_name(value, "base")
    remote, separator, branch = text.partition("/")
    if not separator or not remote or not branch:
        raise DispatchGitError(
            "base must be <remote>/<branch>, for example origin/develop; "
            "the first component is the remote and the rest is the branch"
        )
    return literal_name(remote, "base remote"), literal_name(branch, "base branch")


def parse_fetch(value: object) -> tuple[str, ...] | None:
    """None omits the refresh. True selects the default remote. A list is literal remote names."""
    if value is None or value is False:
        return None
    if value is True:
        return ("<default>",)
    if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
        raise DispatchGitError("fetch must be true or a list of remote names")
    if not value:
        raise DispatchGitError("fetch must name at least one remote, or be true for the default remote")
    return tuple(literal_name(item, "fetch remote") for item in value)


def validate_request(
    *,
    freedom: str,
    branch: str | None = None,
    base: str | None = None,
    fetch: object = None,
    fetch_required: bool = False,
    worktree: bool = False,
    continuation: bool = False,
) -> dict[str, Any]:
    """Check names and freedom before any Git command. Does not touch the repository."""
    if type(fetch_required) is not bool:
        raise DispatchGitError("fetch_required must be a boolean")
    remotes = parse_fetch(fetch)
    if fetch_required and remotes is None:
        raise DispatchGitError("fetch_required needs fetch: true or a list of remote names")
    if (branch is None) != (base is None):
        raise DispatchGitError("branch and base are set together; pass both, for example branch='feature/x' and base='origin/develop'")
    if branch is not None:
        if continuation:
            raise DispatchGitError(
                "continuations reuse the branch prepared at dispatch and do not accept branch or base"
            )
        if freedom == "read_only":
            raise DispatchGitError(
                "branch preparation is unavailable at read_only; use write_in_repo or above, or pass fetch without branch"
            )
        if worktree:
            raise DispatchGitError(
                "branch preparation and worktree create different checkouts; pass one of them"
            )
        literal_name(branch, "branch")
        parse_base(base)
    return {"branch": branch, "base": base, "fetch": None if remotes is None else list(remotes), "fetch_required": fetch_required}


def preview(
    *,
    freedom: str,
    branch: str | None = None,
    base: str | None = None,
    fetch: object = None,
    fetch_required: bool = False,
    worktree: bool = False,
) -> dict[str, Any] | None:
    if branch is None and base is None and fetch is None and not fetch_required:
        return None
    requested = validate_request(
        freedom=freedom, branch=branch, base=base, fetch=fetch,
        fetch_required=fetch_required, worktree=worktree,
    )
    return {
        "status": "preview",
        **requested,
        "runs_before_agent": True,
        "agent_freedom_unchanged": True,
        "read_only_agent_unchanged": freedom == "read_only",
        "note": (
            "Harflow runs this before the agent starts. The agent's freedom is unchanged; "
            "only this preparation writes to .git."
        ),
    }


def node_kwargs(node: dict[str, Any], *, resume: bool) -> dict[str, Any]:
    """Launch kwargs for one workflow node. Resume refreshes refs and does not recreate the branch."""
    out: dict[str, Any] = {}
    if "fetch" in node:
        out["fetch"] = node["fetch"]
    if node.get("fetch_required"):
        out["fetch_required"] = True
    if not resume and node.get("branch"):
        out["branch"] = node["branch"]
        out["base"] = node["base"]
    return out


def validate_node(node: dict[str, Any], node_id: str) -> None:
    try:
        validate_request(
            freedom=node.get("freedom", "write_in_repo"),
            branch=node.get("branch"),
            base=node.get("base"),
            fetch=node.get("fetch"),
            fetch_required=node.get("fetch_required", False),
        )
    except DispatchGitError as exc:
        raise DispatchGitError(f"{node_id}: {exc}") from None


def prepared_branch(report: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(report, dict) or not report.get("branch"):
        return None, None
    return str(report["branch"]), report.get("base") if isinstance(report.get("base"), str) else None


def prepare(
    repo: Path,
    *,
    freedom: str,
    branch: str | None = None,
    base: str | None = None,
    fetch: object = None,
    fetch_required: bool = False,
    reuse_branch: bool = False,
    timeout: float = COMMAND_TIMEOUT,
) -> dict[str, Any] | None:
    """Fetch and, unless reusing, create `branch` from `base`. Raises before any checkout on refusal.

    A failed optional fetch is recorded and does not stop dispatch unless `fetch_required` or the
    remote is the one named by `base`. Branch creation never stashes, resets or forces.
    """
    if branch is None and base is None and fetch is None and not fetch_required and not reuse_branch:
        return None
    requested = validate_request(
        freedom=freedom, branch=branch, base=base, fetch=fetch, fetch_required=fetch_required,
    )
    started = datetime.now(timezone.utc)
    notices: list[str] = []
    fetches: list[dict[str, Any]] = []
    base_remote, base_branch = parse_base(base) if base else (None, None)
    optional = _resolve_remotes(repo, requested["fetch"], timeout)
    required = set(optional if fetch_required else [])
    # Reuse does not refetch the base: a network failure must not block a resume that
    # only needed the branch already checked out. An explicit fetch still runs.
    if base_remote and not reuse_branch:
        required.add(base_remote)
    ordered: list[str] = []
    for remote in (*optional, *([base_remote] if base_remote and not reuse_branch else [])):
        if remote not in ordered:
            ordered.append(remote)
    for remote in ordered:
        entry = _fetch_one(repo, remote, timeout)
        fetches.append(entry)
        if entry["ok"]:
            continue
        message = f"git fetch {remote} failed: {entry['error']}"
        if remote in required:
            raise DispatchGitError(
                message + "; dispatch refused and the agent was not started. "
                "Harflow did not stash, reset or force."
            )
        notices.append(message + "; the task will start and remote refs may be stale. Pass fetch_required to refuse dispatch instead.")
    created = None
    base_commit = None
    reused = False
    if branch:
        if reuse_branch:
            _require_current_branch(repo, branch, timeout)
            reused = True
            base_commit = _rev_parse(repo, "HEAD", timeout)
        else:
            _refuse_dirty_or_sequenced(repo, timeout)
            if _branch_exists(repo, branch, timeout):
                raise DispatchGitError(
                    f"branch {branch!r} already exists; choose another name or resume the task that prepared it. "
                    "Harflow did not switch, stash or reset."
                )
            assert base_remote and base_branch and base
            specific = _run(repo, ["fetch", base_remote, f"{base_branch}:refs/remotes/{base_remote}/{base_branch}"], timeout=timeout)
            if specific.returncode != 0:
                raise DispatchGitError(
                    f"could not fetch {base}: {_brief(specific.stderr)}; "
                    "dispatch refused and the agent was not started. Harflow did not create the branch."
                )
            switched = _run(repo, ["switch", "-c", branch, base], timeout=timeout)
            if switched.returncode != 0:
                raise DispatchGitError(
                    f"could not create {branch} from {base}: {_brief(switched.stderr)}; "
                    "Harflow did not force or reset."
                )
            created = True
            base_commit = _rev_parse(repo, "HEAD", timeout)
    if not fetches and created is None and not reused:
        return None
    return {
        "status": "prepared",
        "branch": branch,
        "base": base,
        "base_commit": base_commit,
        "reused": reused,
        "created": bool(created),
        "fetch": fetches,
        "fetch_required": fetch_required,
        "at": started.isoformat(),
        "notices": notices,
        "agent_freedom_unchanged": True,
        "read_only_agent_unchanged": freedom == "read_only",
    }


def _resolve_remotes(repo: Path, requested: list[str] | None, timeout: float) -> list[str]:
    if not requested:
        return []
    if requested == ["<default>"]:
        return [_default_remote(repo, timeout)]
    return list(requested)


def _default_remote(repo: Path, timeout: float) -> str:
    configured = _run(repo, ["config", "--get", "checkout.defaultRemote"], timeout=timeout)
    if configured.returncode == 0 and configured.stdout.strip():
        return literal_name(configured.stdout.strip(), "checkout.defaultRemote")
    listed = _run(repo, ["remote"], timeout=timeout)
    remotes = [line for line in listed.stdout.splitlines() if line]
    if len(remotes) == 1:
        return literal_name(remotes[0], "remote")
    if "origin" in remotes:
        return "origin"
    shown = ", ".join(remotes) if remotes else "none"
    raise DispatchGitError(
        f"fetch true needs a default remote; this repository has {shown}. "
        "Pass an explicit remote name, or set checkout.defaultRemote."
    )


def _fetch_one(repo: Path, remote: str, timeout: float) -> dict[str, Any]:
    at = datetime.now(timezone.utc).isoformat()
    # No leading '+' and no --force: a non-fast-forward update fails. No --tags; tagOpt is
    # cleared so repository config cannot widen this past Git's default auto-follow.
    result = _run(
        repo,
        ["fetch", "--prune", remote, f"refs/heads/*:refs/remotes/{remote}/*"],
        timeout=timeout,
        extra=("-c", f"remote.{remote}.tagOpt="),
    )
    error = None if result.returncode == 0 else _brief(result.stderr or result.stdout or "git fetch failed")
    return {"remote": remote, "ok": result.returncode == 0, "at": at, "error": error}


def _refuse_dirty_or_sequenced(repo: Path, timeout: float) -> None:
    status = _run(repo, ["status", "--porcelain=v1", "--untracked-files=normal"], timeout=timeout)
    if status.returncode != 0:
        raise DispatchGitError(
            f"branch preparation cannot inspect {repo}: git status failed; verify this is an accessible Git checkout and retry"
        )
    if status.stdout:
        raise DispatchGitError(
            f"branch preparation refused for dirty checkout {repo}; inspect git status and preserve or commit your changes before retrying. "
            "Harflow did not stash, reset or switch branches."
        )
    for name, label in (("MERGE_HEAD", "merge"), ("rebase-merge", "rebase"), ("rebase-apply", "rebase")):
        if _git_path_exists(repo, name, timeout):
            raise DispatchGitError(
                f"branch preparation refused because a {label} is in progress in {repo}. "
                "Finish or abort it first. Harflow did not stash, reset or switch."
            )


def _require_current_branch(repo: Path, branch: str, timeout: float) -> None:
    if not _branch_exists(repo, branch, timeout):
        raise DispatchGitError(
            f"prepared branch {branch!r} no longer exists; Harflow will not recreate it on resume or continuation"
        )
    head = _run(repo, ["rev-parse", "--abbrev-ref", "HEAD"], timeout=timeout)
    current = head.stdout.strip()
    if head.returncode != 0 or current != branch:
        raise DispatchGitError(
            f"expected to stay on prepared branch {branch!r}, but HEAD is {current or 'unknown'!r}. "
            "Harflow will not switch."
        )


def _branch_exists(repo: Path, branch: str, timeout: float) -> bool:
    result = _run(repo, ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], timeout=timeout)
    return result.returncode == 0


def _git_path_exists(repo: Path, name: str, timeout: float) -> bool:
    result = _run(repo, ["rev-parse", "--path-format=absolute", "--git-path", name], timeout=timeout)
    text = result.stdout.strip()
    return result.returncode == 0 and bool(text) and Path(text).exists()


def _rev_parse(repo: Path, rev: str, timeout: float) -> str | None:
    result = _run(repo, ["rev-parse", "--verify", rev], timeout=timeout)
    text = result.stdout.strip()
    if result.returncode != 0 or not re.fullmatch(r"[0-9a-f]{40,64}", text):
        return None
    return text


def _brief(text: str) -> str:
    cleaned = "".join(ch if ch.isprintable() else " " for ch in text).strip()
    return cleaned[:240] or "git failed"


def _run(
    repo: Path,
    args: list[str],
    *,
    timeout: float,
    extra: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    env = {
        key: value for key, value in os.environ.items()
        if key not in GIT_LOCAL_ENV and not key.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))
    }
    env.update({
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_PAGER": "cat",
        "GIT_OPTIONAL_LOCKS": "0",
    })
    command = ["git", "--no-optional-locks", "-C", str(repo), *CONFIG, *extra, *args]
    try:
        return subprocess.run(
            command, capture_output=True, text=True, timeout=timeout, env=env,
            stdin=subprocess.DEVNULL, check=False,
        )
    except subprocess.TimeoutExpired:
        failed: subprocess.CompletedProcess[str] = subprocess.CompletedProcess(command, 124, "", "timed out")
        return failed
    except OSError as exc:
        failed = subprocess.CompletedProcess(command, 127, "", type(exc).__name__)
        return failed

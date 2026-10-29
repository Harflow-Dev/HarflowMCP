"""Process-local Git transport configuration; never edits Git files or handles tokens."""
from __future__ import annotations

import functools
import os
import shlex
import shutil
import subprocess
from collections.abc import Mapping

REWRITE_ENV = "HARFLOW_GIT_HTTPS_REWRITE"
PREFIXES = ("git@github.com:", "ssh://git@ssh.github.com:443/")


def resolve(value: bool | None = None, *, environ: Mapping[str, str] | None = None) -> bool:
    if value is not None:
        if type(value) is not bool:
            raise ValueError("git_https_rewrite must be a boolean or null")
        return value
    env = os.environ if environ is None else environ
    raw = env.get(REWRITE_ENV, "1").lower()
    if raw not in {"1", "0", "true", "false", "yes", "no", "on", "off"}:
        raise ValueError(f"{REWRITE_ENV} must be a boolean (1/0, true/false, yes/no, on/off)")
    return raw in {"1", "true", "yes", "on"}


def inherited(value: bool | None, source: object | None) -> bool:
    if value is None and source is not None:
        enforcement = getattr(source, "enforcement", None)
        payload = enforcement.as_dict() if hasattr(enforcement, "as_dict") else enforcement
        if isinstance(payload, dict):
            git = payload.get("git_environment")
            if isinstance(git, dict):
                value = git.get("github_https_rewrite")
    return resolve(value)


def configure(environ: Mapping[str, str], enabled: bool) -> dict[str, str]:
    """Append Git's command-scope config, preserving the caller's complete environment.

    Malformed inherited config is refused without echoing its potentially secret values.
    SSH_AUTH_SOCK retains the existing inherited-environment behavior.
    """
    result = dict(environ)
    if not enabled:
        return result
    raw = result.get("GIT_CONFIG_COUNT", "0")
    if not raw.isascii() or not raw.isdecimal() or len(raw) > 6:
        raise ValueError("Cannot append HTTPS Git rewrite: invalid inherited GIT_CONFIG_COUNT")
    count = int(raw)
    if count > 1024:
        raise ValueError("Cannot append HTTPS Git rewrite: inherited Git configuration exceeds 1024 entries")
    if any(f"GIT_CONFIG_{kind}_{index}" not in result for index in range(count) for kind in ("KEY", "VALUE")):
        raise ValueError("Cannot append HTTPS Git rewrite: incomplete inherited Git configuration")
    for index, prefix in enumerate(PREFIXES, count):
        result[f"GIT_CONFIG_KEY_{index}"] = "url.https://github.com/.insteadOf"
        result[f"GIT_CONFIG_VALUE_{index}"] = prefix
    result["GIT_CONFIG_COUNT"] = str(count + len(PREFIXES))
    return result


def _gh_helper(value: str) -> bool:
    try:
        parts = shlex.split(value.removeprefix("!"))
    except ValueError:
        return False
    return len(parts) == 3 and os.path.basename(parts[0]) == "gh" and parts[1:] == ["auth", "git-credential"]


@functools.lru_cache(maxsize=1)
def auth_status() -> str:
    """Bounded server-process observation. Output is discarded, never persisted or logged.

    Authentication and a helper declaration do not prove effective credential-helper availability.
    This is called off the event loop by startup/launch and backend discovery.
    """
    if shutil.which("gh") is None:
        return "gh_missing"
    try:
        status = subprocess.run(["gh", "auth", "status", "--hostname", "github.com"], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, timeout=2, check=False)
        if status.returncode:
            return "gh_not_authenticated"
        helpers = subprocess.run(["git", "config", "--get-urlmatch", "credential.helper", "https://github.com"],
                                 capture_output=True, text=True, timeout=2, check=False)
        if helpers.returncode not in (0, 1):
            return "unknown"
        # Observe a URL-matched declaration only. A later generic reset can invalidate
        # this declaration in the effective credential chain. We never invoke helpers
        # or claim effective credential availability from this query.
        if helpers.returncode == 0 and _gh_helper(helpers.stdout.strip()):
            return "gh_authenticated_helper_declaration_observed"
        return "gh_authenticated_helper_not_observed"
    except (OSError, subprocess.TimeoutExpired, UnicodeError):
        return "unknown"


_REMEDIES = {
    "gh_missing": "Install GitHub CLI, run `gh auth login` and `gh auth setup-git`, then restart the Harflow server.",
    "gh_not_authenticated": "Run `gh auth login` and `gh auth setup-git`, then restart the Harflow server.",
    "gh_authenticated_helper_not_observed": "Run `gh auth setup-git`, then restart the Harflow server. The observation is cached for the server's lifetime.",
}
_SSH_NOTE = (
    "SSH URLs for github.com are fetched over HTTPS, so SSH agent setup does not affect them; "
    "git_https_rewrite=false restores SSH."
)


def remedy(enabled: bool, status: str) -> str | None:
    """What to do about this observation while the rewrite is on. None means no HTTPS remedy."""
    if not enabled:
        return None
    return _REMEDIES.get(status)


def disclosure(enabled: bool, status: str) -> dict[str, object]:
    payload: dict[str, object] = {"github_https_rewrite": enabled, "https_auth_observation": status,
                                  "reachability_verified": False}
    text = remedy(enabled, status)
    if text is not None:
        payload["remedy"] = text
    return payload


def notice(enabled: bool, status: str) -> str:
    transport = "GitHub SSH URLs configured to rewrite to HTTPS" if enabled else "GitHub HTTPS rewrite disabled"
    parts = [f"Git: {transport}; HTTPS authentication observation: {status}."]
    if enabled:
        parts.append(_SSH_NOTE)
    text = remedy(enabled, status)
    if text is not None:
        parts.append(text)
    parts.append("Network, sandbox and repository access are not verified.")
    return " ".join(parts)

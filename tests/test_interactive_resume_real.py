"""`interactive_resume_argv` checked against each installed CLI's own `--help`.

Opt-in (`PB_CLI_INTEGRATION=1 uv run pytest -m cli_integration`). Only `--help` is ever run: the
command under test opens an interactive UI (and codex's writes a trust entry to ~/.codex/config.toml
in an untrusted directory), so running it here is exactly what must never happen. What `--help` can
show is that every option the argv uses still exists, and that the positional shape is still the one
that was measured — which is what drifts when a CLI renames a flag.

The table below names, per backend, which help text to read and what it must contain. It is tied to
the argv itself: every option token the argv carries must be claimed by some entry, so the table
cannot silently fall behind a change to `interactive_resume_argv`.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from polybridge import backends

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(
        not os.environ.get("PB_CLI_INTEGRATION"),
        reason="reads real agent CLIs' --help; opt in with PB_CLI_INTEGRATION=1",
    ),
]

SESSION_ID = "0199a3f2-7c1e-7b8a-9d0e-123456789abc"
REPO = Path("/tmp/pb-interactive-help-check")

# (help argv suffix, [patterns that must match that help text], option tokens those patterns claim)
HELP_CHECKS: dict[str, list[tuple[list[str], list[str], set[str]]]] = {
    "claude": [(["--help"], [r"-r, --resume \[value\]"], {"--resume"})],
    "codex": [
        (["--help"], [r"-c, --config <key=value>", r"^\s+resume\s"], {"-c"}),
        (["resume", "--help"], [r"Usage: codex resume \[OPTIONS\] \[SESSION_ID\]"], set()),
    ],
    "opencode": [
        (["--help"], [r"opencode \[project\]", r"-s, --session"], {"-s"}),
    ],
    "vibe": [
        (["--help"], [r"--trust\b", r"--workdir DIR", r"--resume \[SESSION_ID\]"], {"--trust", "--workdir", "--resume"}),
    ],
    # Measured on agy 1.2.14: `agy --conversation <id>` resumes the same conversation (an unknown
    # id is not refused — it silently starts a new one — which the drainer reports).
    "antigravity": [
        (["--help"], [r"--conversation"], {"--conversation"}),
    ],
}


def _help(binary: str, suffix: list[str]) -> str:
    result = subprocess.run(
        [binary, *suffix],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        cwd="/tmp",
    )
    return result.stdout + result.stderr


@pytest.mark.parametrize("name", sorted(backends.BACKENDS))
def test_interactive_resume_argv_matches_the_cli_help(name: str) -> None:
    backend = backends.get(name)
    binary = shutil.which(backend.binary)
    if binary is None:
        pytest.skip(f"{backend.binary} is not installed")

    argv = backend.interactive_resume_argv(SESSION_ID, REPO)
    assert argv is not None and argv[0] == backend.binary
    assert argv[-1] == SESSION_ID

    claimed: set[str] = set()
    for suffix, patterns, tokens in HELP_CHECKS[name]:
        text = _help(binary, suffix)
        for pattern in patterns:
            assert re.search(pattern, text, re.M), (
                f"{name}: `{backend.binary} {' '.join(suffix)}` no longer matches {pattern!r}"
            )
        claimed |= tokens

    options = {token for token in argv[1:] if token.startswith("-")}
    assert options <= claimed, f"{name}: options {sorted(options - claimed)} are not checked here"

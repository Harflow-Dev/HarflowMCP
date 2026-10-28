"""Identity of the Cursor Agent CLI, shared by the client and the backend.

`agent` is a generic binary name. Availability and launch accept `cursor-agent` or
`agent` only when `--version` prints Cursor's version line. The path is the one
`shutil.which` returns. It is not resolved: that symlink points at a versioned
directory, and the wrapper realpaths itself.
"""

from __future__ import annotations

import re
import shutil
import subprocess

VERSION_RE = re.compile(r"^\d{4}\.\d{2}\.\d{2}-[0-9a-f]+\Z")
NAMES = ("cursor-agent", "agent")


def version_line(executable: str) -> str | None:
    """The first stdout line when it is Cursor's version, otherwise None."""
    try:
        result = subprocess.run(
            [executable, "--version"],
            capture_output=True,
            text=True,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    line = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
    return line if VERSION_RE.fullmatch(line) else None


def find_executable() -> str | None:
    """The first on-PATH Cursor executable, preferring `cursor-agent`."""
    for name in NAMES:
        path = shutil.which(name)
        if path and version_line(path):
            return path
    return None


def require_version(binary: str) -> str:
    """Return Cursor's version line or raise. Does not resolve symlinks."""
    from .backends.base import UnsupportedCapability

    path = shutil.which(binary)
    line = version_line(path) if path else None
    if line is None:
        raise UnsupportedCapability(
            f"{binary} is not the Cursor Agent CLI: `--version` must print a "
            "YYYY.MM.DD-<hex> line. `agent` alone is not enough; run `agent login` "
            "only after this version check passes, or install `cursor-agent`."
        )
    return line
